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


class ExistingFMPStage0Adapter:
    def __init__(self, spreadsheet, run_id, *, max_symbols=8, historical_budget=8):
        self.spreadsheet = spreadsheet
        self.run_id = run_id
        self.max_symbols = max(1, min(int(max_symbols), 159))
        self.historical_budget = max(0, min(int(historical_budget), self.max_symbols))
        self.base_url = os.getenv("ASKLIAM_PROVIDER_URL", "https://askliam-fmp-provider.onrender.com").rstrip("/")
        self.token = os.getenv("ASKLIAM_SERVICE_TOKEN", "")
        if urlsplit(self.base_url).scheme != "https" or not self.token:
            raise RuntimeError("PROVIDER_AUTH_OR_TLS_NOT_CONFIGURED")

    def __call__(self):
        raw = self.spreadsheet.worksheet("Universe").get_all_records(
            expected_headers=[], numericise_ignore=["all"]
        )
        priority = []
        for item in raw:
            if str(item.get("Analysis Enabled", "")).upper() != "TRUE":
                continue
            cls = str(item.get("Asset Class", "")).lower()
            sym = str(item.get("Symbol", "")).strip().upper()
            if not sym or not ("equity" in cls or cls in ("etf", "index")):
                continue
            priority.append((sym, item))

        # Stage 0 queue; existing workbook SNAPSHOT is the source of previous
        # provider-deferred state. Prioritize deferred/missing before other assets.
        snapshots = self.spreadsheet.worksheet("Market_Data_Snapshot").get_all_records(
            expected_headers=[], numericise_ignore=["all"]
        )
        deferred = {
            str(row.get("canonical_symbol", "")).upper()
            for row in snapshots
            if "DEFERRED" in str(row.get("enrichment_status", "")).upper()
            or str(row.get("mandatory_broad_pass", "")).upper() in ("FALSE", "0")
        }
        priority.sort(key=lambda pair: (pair[0] not in deferred, pair[0]))
        selected = priority[:self.max_symbols]
        if not selected:
            raise RuntimeError("NO_SUPPORTED_FMP_UNIVERSE_ASSETS")
        symbols = [symbol for symbol, _ in selected]
        now = datetime.now(timezone.utc).isoformat()
        with httpx.Client(timeout=90) as http:
            response = http.post(
                self.base_url + "/enrich",
                headers={"X-Askliam-Token": self.token},
                json={
                    "symbols": symbols,
                    "include_profiles": True,
                    "include_historical": True,
                    "historical_budget": self.historical_budget,
                },
            )
            response.raise_for_status()
            output = response.json()
        records = output.get("results", [])
        if output.get("version") != "V6.5.1" or len(records) != len(symbols):
            raise RuntimeError("PROVIDER_RESPONSE_CONTRACT_INVALID")
        quotes = []
        bars = []
        failed = []
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
            for candle in (result.get("historical") or [])[-300:]:
                if not isinstance(candle, dict) or not candle.get("date"):
                    continue
                when = str(candle["date"])[:10]
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
                    "quality_status": "RECEIVED_UNVERIFIED",
                    "notes": "",
                }
                bars.append(row)
        quota = output.get("fmp_quota") or {}
        settings = [
            {"Setting": "Current Quota Used", "Configured Value": str(quota.get("used", "UNKNOWN")), "Runtime State": "OBSERVED_FROM_UPSTASH", "Version": "V6.5.1"},
            {"Setting": "Current Quota Remaining", "Configured Value": str(quota.get("remaining", "UNKNOWN")), "Runtime State": "OBSERVED_FROM_UPSTASH", "Version": "V6.5.1"},
            {"Setting": "Stage 0 Last Run ID", "Configured Value": self.run_id, "Runtime State": "PENDING_READBACK", "Version": "V6.5.1"},
            {"Setting": "Stage 0 Source Failures", "Configured Value": ";".join(failed)[:900], "Runtime State": "PROVIDER_ERRORS", "Version": "V6.5.1"},
            {"Setting": "Stage 0 Updated Assets", "Configured Value": str(len(quotes)), "Runtime State": "PROPOSED", "Version": "V6.5.1"},
        ]
        if not quotes:
            raise RuntimeError("NO_ASSET_DATA_OBTAINED")
        return {
            "Raw_OHLCV": pd.DataFrame(bars),
            "Market_Data_Snapshot": pd.DataFrame(quotes),
            "FMP_Runtime_State": pd.DataFrame(settings),
        }
