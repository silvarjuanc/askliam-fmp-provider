"""Stage0 V6.5.1 native crypto perpetual OHLCV *preparation*, never a writer.

This module intentionally has no Redis, Sheets or broker client. It converts
validated raw Hyperliquid candles to frames compatible with the existing
Stage0 UPSERT schema. No score or independent underlying spot price is claimed.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from typing import Any

import httpx
import pandas as pd

from stage0_crypto_probe import (
    COINGECKO_IDS, HYPERLIQUID_INFO, classify_response_status,
    hyperliquid_rows, positive_finite, finite_number,
)
from stage0_crypto_history_probe import (
    DAY_MS, WINDOW_DAYS, FUNDING_WINDOW_DAYS, MIN_DAILY_CANDLES,
    validate_daily_candles, validate_funding_history,
)

SOURCE = "HYPERLIQUID_NATIVE_PERP"
SOURCE_PRICE = "HYPERLIQUID_NATIVE_PERP_REFERENCE_ONLY"
SOURCE_URL = "https://api.hyperliquid.xyz/info"
CRYPTO_MISSING = [
    "last_price (independent spot)",
    "return_1m (independent spot)",
    "return_3m (independent spot)",
    "volatility_20d (independent spot)",
    "momentum_score",
    "volume_score",
    "trend_score",
]


def _utc(now: datetime) -> datetime:
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("NAIVE_CLOCK_NOT_ALLOWED")
    return now.astimezone(timezone.utc)


def frames_from_native_perp(
    symbol: str, *,
    candles: Any,
    context_payload: Any,
    funding_payload: Any,
    now: datetime,
    run_id: str,
) -> tuple[dict[str, pd.DataFrame], dict]:
    """Offline strict semantic validation, then pure frame construction.

    Reject partial historical chunks, disjoint daily sessions, malformed
    OHLCV, stale last bar, mismatched asset, and unavailable funding/context.
    The labels on returned columns make explicit that OHLCV are *perpetuals*.
    """
    symbol = str(symbol).strip().upper()
    now = _utc(now)
    if symbol not in COINGECKO_IDS:
        raise ValueError("CRYPTO_SYMBOL_NOT_IN_CANONICAL_MAPPING")
    if not isinstance(run_id, str) or not run_id.startswith("STAGE0-") or not run_id.endswith("-V6.5.1"):
        raise ValueError("INVALID_STAGE0_RUN_ID")
    ctx = hyperliquid_rows(context_payload, [symbol])[symbol]
    if ctx.get("status") != "PERP_CONTEXT_OBSERVED":
        raise ValueError("NATIVE_PERP_NOT_LISTED")
    if positive_finite(ctx.get("mark_price_usd")) is None:
        raise ValueError("NATIVE_PERP_MARK_PRICE_INVALID")
    funding_now = finite_number(ctx.get("funding_rate"))
    oi_base = positive_finite(ctx.get("open_interest_base_units"))
    if funding_now is None or oi_base is None:
        raise ValueError("NATIVE_PERP_DERIVATIVES_CONTEXT_INCOMPLETE")
    historical = validate_daily_candles(candles, symbol, now=now)
    if historical.get("status") != "DERIVATIVE_HISTORY_VALIDATED":
        raise ValueError("NATIVE_PERP_DAILY_HISTORY_NOT_VALIDATED")
    n_complete = historical["complete_candles"]
    n_contiguous = historical["consecutive_candles"]
    if n_complete != n_contiguous or not MIN_DAILY_CANDLES <= n_complete <= WINDOW_DAYS:
        raise ValueError("NATIVE_PERP_HISTORY_DISCONTINUOUS")
    funding = validate_funding_history(funding_payload, symbol, now=now)
    if funding.get("status") != "REALIZED_FUNDING_OBSERVED":
        raise ValueError("NATIVE_PERP_FUNDING_NOT_VALIDATED")

    today = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    today_ms = int(today.timestamp() * 1000)
    fetched = now.isoformat()
    raw_rows = []
    for candle in candles:
        t = int(candle["t"])
        if t >= today_ms:
            continue  # current, unfinished UTC day cannot enter historical sheet
        day = datetime.fromtimestamp(t / 1000, timezone.utc).date().isoformat()
        raw_rows.append({
            "ranking_run_id": run_id,
            "canonical_symbol": symbol,
            "provider_symbol": symbol,
            "asset_class": "Crypto",
            "source": SOURCE,
            "source_url_template": (
                SOURCE_URL + "?type=candleSnapshot&coin=" + symbol + "&interval=1d"
            ),
            "trading_date": day,
            "open": positive_finite(candle["o"]),
            "high": positive_finite(candle["h"]),
            "low": positive_finite(candle["l"]),
            "close": positive_finite(candle["c"]),
            # Hyperliquid candle.v is BASE ASSET units, not USD nor spot volume.
            "volume": finite_number(candle["v"]),
            "fetched_at_utc": fetched,
            "upsert_key": symbol + "|" + day + "|" + SOURCE,
            "quality_status": "DERIVATIVE_SOURCE_VALIDATED_NOT_SPOT",
            "notes": "native perpetual OHLCV; base-asset volume, not USD/spot",
        })
    raw_rows.sort(key=lambda item: item["trading_date"])
    if len(raw_rows) != n_complete or len({
        (row["canonical_symbol"], row["trading_date"], row["source"])
        for row in raw_rows
    }) != len(raw_rows):
        raise ValueError("NATIVE_PERP_RAW_UPSERT_KEY_INCONSISTENT")

    summary = {
        "perpetual_return_1m_pct": historical.get("perpetual_return_1m_pct"),
        "perpetual_return_3m_pct": historical.get("perpetual_return_3m_pct"),
        "perpetual_volatility_20d_annual_pct": historical.get(
            "perpetual_volatility_20d_annual_pct"
        ),
    }
    snapshot = {
        "ranking_run_id": run_id,
        "canonical_symbol": symbol,
        "asset_class": "Crypto",
        "score_model": "CRYPTO_V1",
        "data_timestamp": fetched,
        "analysis_price_source": SOURCE_PRICE,
        # Keep spot-dependent analytical fields BLANK (not futures proxies).
        "last_price": "",
        "return_1m": "",
        "return_3m": "",
        "volatility_20d": "",
        "funding_rate": funding_now,
        "open_interest": oi_base,
        "mandatory_broad_pass": "FALSE",
        "broad_readiness": "INCOMPLETE_DERIVATIVE_ONLY",
        "missing_mandatory_fields": "; ".join(CRYPTO_MISSING),
        "source_summary": (
            SOURCE + " candleSnapshot 1d " + str(n_complete)
            + " complete days; fundingHistory "
            + str(funding["funding_observations"]) + " observations"
        ),
        "enrichment_status": "ACQUIRED_PERPETUAL_NOT_SPOT",
        "notes": (
            "Native-perp price/history are NOT independent spot market data. "
            "Perpetual-only reference metrics: "
            + json.dumps(summary, separators=(",", ":"), sort_keys=True)
            + ". Open interest in base asset units. "
            "Funding rate from current derivatives context; do not conflate "
            "with historical mean. No momentum/trend/volume scores computed."
        ),
    }
    canonical = [
        [row["trading_date"], row["open"], row["high"], row["low"],
         row["close"], row["volume"]]
        for row in raw_rows
    ]
    digest = hashlib.sha256(
        json.dumps(canonical, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()
    frames = {
        "Raw_OHLCV": pd.DataFrame(raw_rows),
        "Market_Data_Snapshot": pd.DataFrame([snapshot]),
    }
    report = {
        "canonical_symbol": symbol,
        "source": SOURCE,
        "history_kind": "HYPERLIQUID_NATIVE_PERPETUAL",
        "daily_rows_prepared": len(raw_rows),
        "first_date_utc": raw_rows[0]["trading_date"],
        "last_date_utc": raw_rows[-1]["trading_date"],
        "funding_observations_validated": funding["funding_observations"],
        "snapshot_rows_prepared": 1,
        "upsert_unique_keys": len(raw_rows),
        "ohlcv_sha256": digest,
        "preview_first": {
            k: raw_rows[0][k] for k in ("canonical_symbol", "trading_date",
                                     "source", "open", "high", "low", "close", "volume")
        },
        "preview_last": {
            k: raw_rows[-1][k] for k in ("canonical_symbol", "trading_date",
                                      "source", "open", "high", "low", "close", "volume")
        },
        "independent_spot_acquired": False,
        "mandatory_broad_pass": False,
        "ranking_permitted": False,
        "execution_permitted": False,
        "sheet_writes_performed": False,
        "checkpoint_advanced": False,
    }
    return frames, report


async def preview_native_perp_stage0(
    symbol: str, *, client: httpx.AsyncClient, now: datetime | None = None,
) -> dict:
    """Read-only remote preflight, maximum 3 Hyperliquid Info calls.

    Does not call the Stage0 writer or Redis checkpoint. NEVER returns a
    COMMITTED status, even when frame conversion succeeds.
    """
    symbol = str(symbol).strip().upper()
    if symbol not in COINGECKO_IDS:
        raise ValueError("CRYPTO_SYMBOL_NOT_IN_CANONICAL_MAPPING")
    now = _utc(now or datetime.now(timezone.utc))
    run_id = "STAGE0-" + now.strftime("%Y%m%d") + "-V6.5.1"
    base = {
        "version": "V6.5.1",
        "symbol": symbol,
        "run_id_preview_only": run_id,
        "status": "READ_ONLY_CRYPTO_STAGE0_PREPARE",
        "data_timestamp_utc": now.isoformat(),
        "source": SOURCE,
        "source_url_template": SOURCE_URL,
        "requests_performed": 0,
        "sheet_writes_performed": False,
        "checkpoint_advanced": False,
        "ranking_permitted": False,
        "execution_permitted": False,
        "fmp_requests_performed": False,
    }
    async def post(payload):
        try:
            response = await client.post(HYPERLIQUID_INFO, json=payload, timeout=18.0)
            code = classify_response_status(response)
            return (code, response.json() if code == "AVAILABLE" else None)
        except (httpx.RequestError, httpx.TimeoutException):
            return ("NETWORK_ERROR", None)
        except (ValueError, TypeError, KeyError, OverflowError):
            return ("PROVIDER_RESPONSE_INVALID", None)

    ctx_code, context = await post({"type":"metaAndAssetCtxs"})
    base["requests_performed"] = 1
    if ctx_code != "AVAILABLE":
        return {**base, "status": "SOURCE_UNAVAILABLE",
                "reason": "CONTEXT_" + ctx_code}
    try:
        ctx = hyperliquid_rows(context, [symbol])[symbol]
        if ctx.get("status") != "PERP_CONTEXT_OBSERVED":
            raise ValueError("NATIVE_PERP_NOT_LISTED")
    except (ValueError, TypeError, KeyError, OverflowError):
        return {**base, "status": "SOURCE_UNAVAILABLE",
                "reason": "CONTEXT_MISMATCH"}

    today = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    start_ms = int((today - timedelta(days=WINDOW_DAYS)).timestamp()*1000)
    end_ms = int(today.timestamp()*1000)
    candles_code, candles = await post({
        "type":"candleSnapshot",
        "req":{"coin":symbol,"interval":"1d",
               "startTime":start_ms,"endTime":end_ms},
    })
    base["requests_performed"] = 2
    if candles_code != "AVAILABLE":
        return {**base, "status":"SOURCE_UNAVAILABLE",
                "reason":"CANDLES_" + candles_code}

    try:
        summary = validate_daily_candles(candles, symbol, now=now)
        if summary["status"] != "DERIVATIVE_HISTORY_VALIDATED":
            return {**base, "status": "PREPARATION_BLOCKED",
                    "reason": summary["status"],
                    "daily_rows_available": summary["complete_candles"]}
    except (ValueError, TypeError, KeyError, OverflowError):
        return {**base, "status":"PREPARATION_BLOCKED",
                "reason":"CANDLE_CONTRACT_INVALID"}

    fund_code, funding = await post({
        "type":"fundingHistory","coin":symbol,
        "startTime":int((now - timedelta(days=FUNDING_WINDOW_DAYS)).timestamp()*1000),
        "endTime":int(now.timestamp()*1000),
    })
    base["requests_performed"] = 3
    if fund_code != "AVAILABLE":
        return {**base,"status":"SOURCE_UNAVAILABLE",
                "reason":"FUNDING_" + fund_code}
    try:
        frames, report = frames_from_native_perp(
            symbol, candles=candles, context_payload=context,
            funding_payload=funding, now=now, run_id=run_id,
        )
        # Confirm compatibility with the real Stage0 tab contract.
        from stage0_ingestor import TABS
        for tab, frame in frames.items():
            if tab not in TABS or not set(TABS[tab]).issubset(frame.columns):
                raise ValueError("STAGE0_KEY_SCHEMA_MISMATCH")
    except (ValueError, TypeError, KeyError, OverflowError):
        return {**base, "status":"PREPARATION_BLOCKED",
                "reason":"FRAME_VALIDATION_FAILED"}
    return {
        **base,
        "status": "PREPARED_PREVIEW_NO_WRITES",
        "prepared": report,
        "reader_only": True,
    }
