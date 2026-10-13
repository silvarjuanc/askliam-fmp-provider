"""Bridge for Stage 0: consumes existing /enrich provider unchanged.

Single authenticated HTTP request to the deployed provider, then maps returned
data to existing worksheet headers. Does NOT score or mark BROAD_SCAN_READY.
"""
from __future__ import annotations
import os
from datetime import datetime, timezone
from urllib.parse import urlsplit
import httpx
import pandas as pd
from stage0_fmp_session_quality import provider_eod_date_guard


def provider_failure_code(errors):
    """Return only allowlisted non-sensitive upstream error categories."""
    import re
    codes = set()
    for value in errors:
        raw = str(value).upper()
        match = re.search(r"FMP_HTTP_(400|401|402|403|404|408|429|500|502|503|504)", raw)
        if match:
            codes.add("FMP_HTTP_" + match.group(1))
        elif "FMP_BATCH_SYMBOL_MISSING" in raw or "FMP_PROFILE_SYMBOL_MISSING" in raw:
            codes.add("FMP_SYMBOL_NOT_FOUND")
        elif "FMP_HISTORICAL_EMPTY" in raw:
            codes.add("FMP_HISTORICAL_EMPTY")
        else:
            codes.add("FMP_PROVIDER_ERROR")
    return next(iter(codes)) if len(codes) == 1 else "FMP_ACQUISITION_INCOMPLETE"


# Only these codes can cross the provider/ingestor boundary.
KNOWN_SOURCE_CODES = {
    "NO_ASSET_DATA_OBTAINED", "FMP_ACQUISITION_INCOMPLETE",
    "FMP_QUOTES_MISSING", "FMP_HISTORICAL_EMPTY",
    "PROVIDER_RESPONSE_CONTRACT_INVALID",
    "REQUESTED_SYMBOL_NOT_IN_ELIGIBLE_FMP_UNIVERSE",
    "NO_SUPPORTED_FMP_UNIVERSE_ASSETS",
    "PROVIDER_AUTH_OR_TLS_NOT_CONFIGURED",
    "FMP_SYMBOL_NOT_FOUND", "FMP_PROVIDER_ERROR",
    *(f"FMP_HTTP_{n}" for n in (400,401,402,403,404,408,429,500,502,503,504)),
}


class ExistingFMPStage0Adapter:
    def __init__(self, spreadsheet, run_id, *, max_symbols=8, historical_budget=8, symbols=None):
        self.spreadsheet = spreadsheet
        self.run_id = run_id
        self.max_symbols = max(1, min(int(max_symbols), 159))
        self.requested_symbols = [str(x).strip().upper() for x in (symbols or [])]
        self.historical_budget = max(0, min(int(historical_budget), self.max_symbols))
        self.base_url = os.getenv("ASKLIAM_PROVIDER_URL", "https://askliam-fmp-provider.onrender.com").rstrip("/")
        self.token = os.getenv("ASKLIAM_SERVICE_TOKEN", "")
        if urlsplit(self.base_url).scheme != "https" or not self.token:
            raise RuntimeError("PROVIDER_AUTH_OR_TLS_NOT_CONFIGURED")

    def __call__(self):
        self.phase = "UNIVERSE"
        try:
            return self._acquire()
        except httpx.TimeoutException:
            raise RuntimeError("FMP_PROVIDER_TIMEOUT") from None
        except httpx.RequestError:
            raise RuntimeError("FMP_PROVIDER_TRANSPORT_ERROR") from None
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            if code in (400, 401, 403, 404, 408, 409, 429, 500, 502, 503, 504):
                raise RuntimeError(f"FMP_INTERNAL_HTTP_{code}") from None
            raise RuntimeError("FMP_INTERNAL_HTTP_ERROR") from None
        except RuntimeError as exc:
            if str(exc) in KNOWN_SOURCE_CODES:
                raise
            raise RuntimeError(f"ADAPTER_{self.phase}_RUNTIME_ERROR") from None
        except Exception:
            raise RuntimeError(f"ADAPTER_{self.phase}_UNEXPECTED_ERROR") from None

    def _acquire(self):
        self.phase = "UNIVERSE"
        raw = self.spreadsheet.worksheet("Universe").get_all_records(numericise_ignore=["all"])
        priority = []
        for item in raw:
            if str(item.get("Analysis Enabled", "")).upper() != "TRUE":
                continue
            cls = str(item.get("Asset Class", "")).lower()
            sym = str(item.get("Symbol", "")).strip().upper()
            if not sym or not (cls in ("equity", "index", "fixed income etf", "index / etf / basket", "etf")):
                continue
            priority.append((sym, item))

        # Stage 0 queue; existing workbook SNAPSHOT is the source of previous
        # provider-deferred state. Prioritize deferred/missing before other assets.
        self.phase = "SNAPSHOT"
        snapshots = self.spreadsheet.worksheet("Market_Data_Snapshot").get_all_records(numericise_ignore=["all"])
        deferred = {
            str(row.get("canonical_symbol", "")).upper()
            for row in snapshots
            if "DEFERRED" in str(row.get("enrichment_status", "")).upper()
            or str(row.get("mandatory_broad_pass", "")).upper() in ("FALSE", "0")
        }
        priority.sort(key=lambda pair: (pair[0] not in deferred, pair[0]))
        if self.requested_symbols:
            known = {sym: item for sym, item in priority}
            if any(sym not in known for sym in self.requested_symbols):
                raise RuntimeError("REQUESTED_SYMBOL_NOT_IN_ELIGIBLE_FMP_UNIVERSE")
            selected = [(sym, known[sym]) for sym in self.requested_symbols]
        else:
            selected = priority[:self.max_symbols]
        if not selected:
            raise RuntimeError("NO_SUPPORTED_FMP_UNIVERSE_ASSETS")
        symbols = [symbol for symbol, _ in selected]
        now = datetime.now(timezone.utc).isoformat()
        # FMP Free supports single-symbol /stable/quote but can reject
        # comma-separated symbols with HTTP 402. Call existing provider as
        # an unchanged black box, once per symbol, caching handled upstream.
        records = []
        quota = {}
        self.phase = "PROVIDER_REQUEST"
        with httpx.Client(timeout=90) as http:
            for index, symbol in enumerate(symbols):
                # Quota optimization: verify mandatory quote access FIRST.
                # If FMP Free returns 402, never waste a second call asking
                # for historical data that cannot make this asset usable.
                probe_response = http.post(
                    self.base_url + "/enrich",
                    headers={"X-Askliam-Token": self.token},
                    json={
                        "symbols": [symbol],
                        "include_profiles": False,
                        "include_historical": False,
                        "historical_budget": 0,
                    },
                )
                probe_response.raise_for_status()
                self.phase = "PROVIDER_DECODE"
                probe = probe_response.json()
                probe_rows = probe.get("results", [])
                if (
                    probe.get("version") != "V6.5.1"
                    or len(probe_rows) != 1
                    or probe_rows[0].get("canonical_symbol") != symbol
                ):
                    raise RuntimeError("PROVIDER_RESPONSE_CONTRACT_INVALID")
                probe_row = probe_rows[0]
                if probe_row.get("provider_errors"):
                    raise RuntimeError(
                        provider_failure_code(probe_row["provider_errors"])
                    )
                if (probe_row.get("fields") or {}).get("last_price") is None:
                    raise RuntimeError("FMP_QUOTES_MISSING")
                self.phase = "PROVIDER_REQUEST"

                # Stage0 stores prices/history, NOT fundamental factors.
                # Skip /profile until the separate fundamentals enrichment
                # actually consumes those fields; missing fundamentals
                # remain explicit and ranking_permitted remains FALSE.
                response = http.post(
                    self.base_url + "/enrich",
                    headers={"X-Askliam-Token": self.token},
                    json={
                        "symbols": [symbol],
                        "include_profiles": False,
                        "include_historical": index < self.historical_budget,
                        "historical_budget": 1 if index < self.historical_budget else 0,
                    },
                )
                response.raise_for_status()
                self.phase = "PROVIDER_DECODE"
                output = response.json()
                self.phase = "PROVIDER_REQUEST"
                result_rows = output.get("results", [])
                if (
                    output.get("version") != "V6.5.1"
                    or len(result_rows) != 1
                    or result_rows[0].get("canonical_symbol") != symbol
                ):
                    raise RuntimeError("PROVIDER_RESPONSE_CONTRACT_INVALID")
                records.extend(result_rows)
                quota = output.get("fmp_quota") or quota
        if len(records) != len(symbols):
            raise RuntimeError("PROVIDER_RESPONSE_CONTRACT_INVALID")
        self.phase = "TRANSFORM"
        quotes = []
        bars = []
        failed = []
        historical_counts = {}
        by_symbol = {str(row.get("canonical_symbol", "")).upper(): row for row in records}
        for symbol, item in selected:
            result = by_symbol.get(symbol)
            if result is None:
                failed.append(f"{symbol}:MISSING_PROVIDER_RESULT")
                continue
            fields = result.get("fields") or {}
            errors = result.get("provider_errors") or []
            success = result.get("enrichment_status") == "ACQUIRED_NOT_SCORED"
            urls = result.get("source_urls") or []
            source = ";".join(urls)
            # Mandatory fields not yet synthesized must remain missing. This
            # avoids false positives and mass DATA_WAIT.
            missing = [
                name for name in ("last_price", "return_1m", "return_3m", "volatility_20d",
                                  "momentum_score", "trend_score", "relative_strength_score",
                                  "volume_score", "macro_score", "fundamental_factor_count")
                if fields.get(name) is None
            ]
            if errors:
                failed.append(f"{symbol}:{','.join(map(str, errors))}")
            snapshot = {
                "ranking_run_id": self.run_id,
                "canonical_symbol": symbol,
                "asset_class": item.get("Asset Class", ""),
                "score_model": "EQUITY_V2" if "equity" in str(item.get("Asset Class", "")).lower() else "INDEX_V1",
                "data_timestamp": result.get("data_timestamp") or now,
                "analysis_price_source": "FMP_STABLE" if fields.get("last_price") is not None else "",
                "last_price": fields.get("last_price"),
                "return_1d": fields.get("return_1d"),
                "return_5d": fields.get("return_5d"),
                "return_1m": fields.get("return_1m"),
                "return_3m": fields.get("return_3m"),
                "return_6m": fields.get("return_6m"),
                "return_12m": fields.get("return_12m"),
                "volatility_20d": fields.get("volatility_20d"),
                "volatility_60d": fields.get("volatility_60d"),
                "drawdown_52w": fields.get("drawdown_pct"),
                "distance_20dma": fields.get("distance_ma20_pct"),
                "distance_50dma": fields.get("distance_ma50_pct"),
                "distance_200dma": fields.get("distance_ma200_pct"),
                "missing_mandatory_fields": "; ".join(missing),
                "missing_optional_fields": "NOT_EVALUATED",
                "source_summary": source,
                "data_completeness_pct": "",
                "mandatory_broad_pass": "FALSE",
                "broad_readiness": "ENRICHMENT_PENDING",
                "enrichment_status": "PROVIDER_DEFERRED" if (errors or not success) else "ENRICHMENT_PENDING",
                "notes": "Stage0 acquisition only; no canonical macro/fundamental/momentum scoring",
            }
            quotes.append(snapshot)
            historical_counts[symbol] = 0
            for candle in sorted(
                (b for b in (result.get("historical") or []) if isinstance(b, dict) and b.get("date")),
                key=lambda b: str(b["date"]),
            )[-300:]:
                if not isinstance(candle, dict) or not candle.get("date"):
                    continue
                when = str(candle["date"])[:10]
                # Same-day FMP bars may carry partial volume. No current
                # US daily bar enters the store before its 18:00 NY buffer.
                try:
                    if not provider_eod_date_guard(
                        when, datetime.fromisoformat(now),
                    ):
                        continue
                except ValueError:
                    continue
                row = {
                    "ranking_run_id": self.run_id,
                    "canonical_symbol": symbol,
                    "provider_symbol": symbol,
                    "asset_class": item.get("Asset Class", ""),
                    "source": "FMP",
                    "source_url_template": "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + symbol,
                    "trading_date": when,
                    "open": candle.get("open"),
                    "high": candle.get("high"),
                    "low": candle.get("low"),
                    "close": candle.get("close"),
                    "volume": candle.get("volume"),
                    "fetched_at_utc": now,
                    "upsert_key": f"{symbol}|{when}|FMP",
                    "quality_status": "EOD_SESSION_BUFFER_PASS_PROVIDER_UNCONFIRMED",
                    "notes": "Bar captured after 18:00 New York date buffer; this is not an official settlement certificate",
                }
                bars.append(row)
                historical_counts[symbol] += 1
        settings = [
            {"Setting": "Current Quota Used", "Configured Value": str(quota.get("used", "UNKNOWN")), "Runtime State": "OBSERVED_FROM_UPSTASH", "Version": "V6.5.1"},
            {"Setting": "Current Quota Remaining", "Configured Value": str(quota.get("remaining", "UNKNOWN")), "Runtime State": "OBSERVED_FROM_UPSTASH", "Version": "V6.5.1"},
            {"Setting": "Stage 0 Last Run ID", "Configured Value": self.run_id, "Runtime State": "COMMIT_CANDIDATE_READBACK_REQUIRED", "Version": "V6.5.1"},
            {"Setting": "Stage 0 Source Failures", "Configured Value": "NONE" if not failed else ";".join(failed)[:900], "Runtime State": "OBSERVED_PROVIDER_ERRORS", "Version": "V6.5.1"},
            {"Setting": "Stage 0 Updated Assets", "Configured Value": str(len(quotes)), "Runtime State": "ACQUIRED_PENDING_READBACK", "Version": "V6.5.1"},
        ]
        # Fail closed: deferred placeholders are NOT acquired market data.
        if not quotes:
            raise RuntimeError("NO_ASSET_DATA_OBTAINED")
        if failed:
            raise RuntimeError(provider_failure_code([error for row in records for error in (row.get("provider_errors") or [])]))
        if any(q.get("last_price") is None for q in quotes):
            raise RuntimeError("FMP_QUOTES_MISSING")
        if self.historical_budget and any(historical_counts.get(sym, 0) == 0 for sym in symbols[:self.historical_budget]):
            raise RuntimeError("FMP_HISTORICAL_EMPTY")
        return {
            "Raw_OHLCV": pd.DataFrame(bars),
            "Market_Data_Snapshot": pd.DataFrame(quotes),
            "FMP_Runtime_State": pd.DataFrame(settings),
        }
