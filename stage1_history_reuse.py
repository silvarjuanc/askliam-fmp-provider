"""AskLiam V6.5.1 Stage1 REHYDRATION DIAGNOSTIC, read-only.

Reuses existing cross-run Raw_OHLCV by canonical symbol and source.
Never changes Stage0 checkpoints, Ranking, books, Lifecycle or AI Trades.
The output is technical evidence, NOT an AskLiam global score.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from math import isfinite
from typing import Mapping

from stage0_canonical_factors import canonical_technicals, relative_strength, volume_score
from stage0_model_readiness import validate_mandatory
from stage0_equity_session_gate import closed_us_equity_session

VERSION = "V6.5.1"
MIN_HISTORY = 253
HISTORY_CLASSES = {"EQUITY_V2", "INDEX_V1", "CRYPTO_V1"}
SOURCES = {
    "EQUITY_V2": {"FMP"},
    "INDEX_V1": {"FMP"},
    "CRYPTO_V1": {"COINBASE_EXCHANGE_SPOT_USD", "COINBASE_USD_SPOT", "COINBASE_SPOT_USD"},
}
SPOT_VALIDATED_STATUSES = {"INDEPENDENT_SPOT_OHLCV_VALIDATED"}
QUOTE_TTL_SECONDS = 900


def numeric(value):
    try:
        if value is None or isinstance(value, bool):
            return None
        number = float(value)
        return number if isfinite(number) else None
    except (ValueError, TypeError, OverflowError):
        return None


def parse_utc(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (ValueError, TypeError, OverflowError):
        return None


def validate_candles(symbol, model, rows, now):
    """Make no assumption that historical rows carry today's run_id.

    OHLC validation establishes structural consistency, NOT independent
    confirmation of exchange-reported prices or equity volume.
    """
    allowed = SOURCES.get(model) or set()
    accepted = defaultdict(dict)
    bad = []
    for row in rows:
        if str(row.get("canonical_symbol", "")).upper() != symbol:
            continue
        source = str(row.get("source", "")).upper()
        if source not in allowed:
            continue
        day = str(row.get("trading_date") or "")[:10]
        try:
            session = datetime.strptime(day, "%Y-%m-%d").date()
        except ValueError:
            bad.append("INVALID_DATE")
            continue
        if session > now.date():
            bad.append("FUTURE_CANDLE")
            continue
        if model in ("EQUITY_V2", "INDEX_V1") and not closed_us_equity_session(day, now):
            continue
        o, h, l, c = (numeric(row.get(key)) for key in ("open", "high", "low", "close"))
        v = numeric(row.get("volume"))
        if (None in (o, h, l, c) or min(o, h, l, c) <= 0
            or h < max(o, c, l) or l > min(o, c, h)
            or (v is not None and v < 0)):
            bad.append("BAD_OHLCV")
            continue
        payload = {"date": day, "open":o,"high":h,"low":l,"close":c,"volume":v,
                   "quality_status":row.get("quality_status", "")}
        previous = accepted[source].get(day)
        if previous and any(previous[k] != payload[k] for k in ("open","high","low","close","volume")):
            bad.append("SOURCE_DATE_CONFLICT")
            continue
        accepted[source][day] = payload
    ranked = sorted(accepted.items(), key=lambda kv: (len(kv[1]), max(kv[1],default="")), reverse=True)
    if not ranked:
        return {"status":"NO_VALID_HISTORY", "observations":0, "errors":sorted(set(bad))}
    source, by_day = ranked[0]
    bars = [by_day[day] for day in sorted(by_day)]
    latest = bars[-1]["date"]
    last_date = datetime.strptime(latest, "%Y-%m-%d").date()
    history_age_days = (now.date() - last_date).days
    history_fresh = history_age_days <= (2 if model == "CRYPTO_V1" else 4)
    if bad:
        return {"status":"HISTORY_CONFLICT_OR_INVALID", "source":source,
                "observations":len(bars), "last_session":latest,
                "errors": sorted(set(bad))}
    if len(bars) < MIN_HISTORY:
        return {"status":"HISTORY_TOO_SHORT", "source":source,
                "observations":len(bars), "last_session":latest, "errors":[]}
    # Minimum 253 close observations. A new day's intraday FMP EOD bar
    # is never included before 16:20 ET by closed_us_equity_session.
    try:
        tech = canonical_technicals([b["close"] for b in bars], sessions_per_year=(365 if model=="CRYPTO_V1" else 252))
    except (ValueError, TypeError, OverflowError):
        return {"status":"CANONICAL_CALCULATION_FAILED","source":source,
                "observations":len(bars),"last_session":latest,"errors":[]}
    if source.startswith("COINBASE"):
        validated = all(str(x.get("quality_status")) in SPOT_VALIDATED_STATUSES for x in bars[-MIN_HISTORY:])
        quality = "INDEPENDENT_SPOT_VALIDATED" if validated else "SPOT_SOURCE_QUALITY_PENDING"
    else:
        # FMP bars are received with RECEIVED_UNVERIFIED status. Structural
        # checks do not silently overwrite that original provenance label.
        validated = False
        quality = "FMP_STRUCTURAL_CHECK_PASS_SOURCE_UNVERIFIED"
    scores = {}
    last21 = bars[-21:]
    if all(x.get("volume") is not None and x["volume"] >= 0 for x in last21):
        mean = sum(x["volume"] for x in last21[:-1]) / 20
        volume = volume_score(last21[-1]["volume"],mean,source_verified=validated)
        if volume is not None:
            scores["volume_score"] = volume
    return {"status":"TECHNICAL_FACTORS_RECOMPUTED", "source":source,
            "history_age_days":history_age_days, "history_fresh":history_fresh,
            "source_quality_status":quality, "source_quality_verified":validated,
            "observations":len(bars),"last_session":latest,"errors":[],
            "factors":{**tech,**scores}}


def preview(run_id, symbols, snapshots, requirements, raw_rows, *,
            now=None):
    """Only selected assets; no writes, no trading, no global-score inference."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or not run_id.startswith("STAGE0-") or not run_id.endswith("-V6.5.1"):
        raise ValueError("RUN_OR_TIME_INVALID")
    if not symbols or len(symbols)>10 or len(set(symbols))!=len(symbols):
        raise ValueError("INVALID_SYMBOL_REQUEST")
    specs = {r.get("Model"):r for r in requirements}
    current = {r.get("canonical_symbol"):r for r in snapshots if r.get("ranking_run_id")==run_id}
    by_symbol = defaultdict(list)
    requested = set(symbols)
    for row in raw_rows:
        symbol = str(row.get("canonical_symbol","")).upper()
        if symbol in requested:
            by_symbol[symbol].append(row)
    results = []
    spy_history = None
    if "SPY" in requested:
        spy = current.get("SPY")
        if spy:
            spy_history = validate_candles("SPY","INDEX_V1",by_symbol["SPY"],now)
    for symbol in symbols:
        row = current.get(symbol)
        if not row:
            results.append({"symbol":symbol,"status":"NO_CURRENT_RUN_SNAPSHOT",
                            "ranking_permitted":False,"executable":False})
            continue
        model = row.get("score_model")
        spec = specs.get(model)
        if not spec:
            results.append({"symbol":symbol,"status":"MODEL_NOT_CONFIGURED",
                            "ranking_permitted":False,"executable":False})
            continue
        history = validate_candles(symbol,model,by_symbol[symbol],now) if model in HISTORY_CLASSES else {"status":"NON_OHLCV_MODEL"}
        projected = dict(row)
        factors = history.get("factors") or {}
        for key, value in factors.items():
            if (projected.get(key) in (None,"","UNKNOWN") and
                    key in ("return_1m","return_3m","volatility_20d",
                            "momentum_score","trend_score","volume_score")):
                projected[key] = value
        if (model == "EQUITY_V2" and spy_history and
            spy_history.get("status")=="TECHNICAL_FACTORS_RECOMPUTED" and
            history.get("status")=="TECHNICAL_FACTORS_RECOMPUTED" and
            spy_history.get("last_session")==history.get("last_session")):
            projected["relative_strength_score"] = relative_strength(
                factors["return_3m"], spy_history["factors"]["return_3m"])
        check = validate_mandatory(projected,spec)
        timestamp = parse_utc(row.get("data_timestamp"))
        quote_age = (now-timestamp).total_seconds() if timestamp else None
        fresh = quote_age is not None and 0<=quote_age<=QUOTE_TTL_SECONDS
        supported = (history.get("status")=="TECHNICAL_FACTORS_RECOMPUTED"
                     and history.get("history_fresh") is True
                     and history.get("source_quality_verified") is True)
        # Even an apparently complete field set cannot rank with stale price
        # or unverified source. This endpoint is PREVIEW-only in any case.
        results.append({
            "symbol":symbol, "model":model, "status":"READ_ONLY_STAGE1_PREVIEW",
            "history_status":history.get("status"),
            "history_source":history.get("source"),
            "history_observations":history.get("observations",0),
            "history_last_session":history.get("last_session"),
            "history_fresh":history.get("history_fresh", False),
            "source_quality_status":history.get("source_quality_status","UNKNOWN"),
            "canonical_technical_factors":{k:factors[k] for k in (
                "return_1m","return_3m","volatility_20d",
                "momentum_score","trend_score","volume_score"
            ) if k in factors},
            "quote_fresh_15m":fresh,
            "quote_age_seconds":round(quote_age,1) if quote_age is not None else None,
            "mandatory_coverage_pct":check["mandatory_coverage_pct"],
            "mandatory_missing":check["missing"],
            "potential_analytical_readiness":bool(check["mandatory_input_complete"] and fresh and supported),
            "ranking_permitted":False, "executable":False,
        })
    return {"version":VERSION,"run_id":run_id,"mode":"READ_ONLY_STAGE1_DIAGNOSTICS",
            "checked_symbols":len(results),"results":results,
            "ranking_permitted":False,"execution_permitted":False,
            "sheet_writes_performed":False}
