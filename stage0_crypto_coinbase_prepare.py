"""V6.5.1 Coinbase spot + Hyperliquid context -> strictly validated Stage0 frames.

Pure transformation only. Does not read/write Sheets, Redis, broker or ranking.
Spot bars remain Coinbase bars; Hyperliquid contributes ONLY funding/OI context.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from math import isfinite
from typing import Any

import pandas as pd

from stage0_crypto_coinbase_spot import SOURCE as SPOT_SOURCE, ENDPOINT, validate_candles
from stage0_crypto_probe import COINGECKO_IDS, hyperliquid_rows
from stage0_canonical_factors import canonical_technicals

SOURCE_URL = ENDPOINT + "/products/{symbol}-USD/candles"
DERIVATIVES_URL = "https://api.hyperliquid.xyz/info"
PRICE_SOURCE = SPOT_SOURCE
MANDATORY = (
    "last_price", "return_1m", "return_3m", "volatility_20d",
    "momentum_score", "volume_score", "open_interest", "funding_rate", "trend_score",
)


def positive_number(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("INVALID_NUMBER")
    try:
        n = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("INVALID_NUMBER") from None
    if not isfinite(n) or n <= 0:
        raise ValueError("INVALID_NUMBER")
    return n


def validated_ticker(ticker: Any, now: datetime) -> tuple[float, str]:
    """The daily close is NOT the fresh spot last_price."""
    if not isinstance(ticker, dict) or not isinstance(ticker.get("time"), str):
        raise ValueError("COINBASE_TICKER_INVALID")
    try:
        stamp = datetime.fromisoformat(ticker["time"].replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("COINBASE_TICKER_TIME_INVALID") from None
    if stamp.tzinfo is None or now.tzinfo is None:
        raise ValueError("COINBASE_TICKER_TIME_INVALID")
    stamp = stamp.astimezone(timezone.utc)
    age = (now.astimezone(timezone.utc) - stamp).total_seconds()
    if age < 0 or age > 900:
        raise ValueError("COINBASE_TICKER_STALE")
    return positive_number(ticker.get("price")), stamp.isoformat()


def prepare_coinbase_spot_frames(
    symbol: str, *, candles: Any, ticker: Any, hyperliquid_context: Any,
    now: datetime, run_id: str,
) -> tuple[dict[str, pd.DataFrame], dict]:
    symbol = str(symbol).strip().upper()
    if symbol not in COINGECKO_IDS or now.tzinfo is None:
        raise ValueError("SPOT_CONTEXT_INVALID")
    now = now.astimezone(timezone.utc)
    expected_run = "STAGE0-" + now.strftime("%Y%m%d") + "-V6.5.1"
    if run_id != expected_run:
        raise ValueError("SPOT_RUN_ID_INVALID")

    proof = validate_candles(candles, symbol, now)
    price, quote_time = validated_ticker(ticker, now)
    ctx = hyperliquid_rows(hyperliquid_context, [symbol])[symbol]
    if ctx["status"] != "PERP_CONTEXT_OBSERVED":
        raise ValueError("HYPERLIQUID_PERP_NOT_LISTED")
    funding = ctx.get("funding_rate")
    oi_base = ctx.get("open_interest_base_units")
    if (
        funding is None or not isfinite(float(funding))
        or oi_base is None or not isfinite(float(oi_base)) or oi_base <= 0
    ):
        raise ValueError("DERIVATIVES_CONTEXT_INCOMPLETE")

    today = now.date().isoformat()
    clean = []
    for item in candles:
        from datetime import datetime as DateTime
        day = DateTime.fromtimestamp(int(item[0]), timezone.utc).date().isoformat()
        if day >= today:
            continue
        clean.append({
            "trading_date": day, "open": positive_number(item[3]),
            "high": positive_number(item[2]), "low": positive_number(item[1]),
            "close": positive_number(item[4]), "volume": positive_number(item[5]),
        })
    clean.sort(key=lambda x: x["trading_date"])
    if len(clean) != proof["daily_bars"] or clean[-1]["trading_date"] != proof["last_complete_utc_date"]:
        raise ValueError("SPOT_HISTORY_MISMATCH")

    factors = canonical_technicals([x["close"] for x in clean])
    factors["volume_score"] = proof["canonical_technicals"]["volume_score"]
    analytical = {
        "last_price": price,
        "return_1m": factors["return_1m"],
        "return_3m": factors["return_3m"],
        "volatility_20d": factors["volatility_20d"],
        "momentum_score": factors["momentum_score"],
        "volume_score": factors["volume_score"],
        "open_interest": float(oi_base),
        "funding_rate": float(funding),
        "trend_score": factors["trend_score"],
    }
    if set(analytical) != set(MANDATORY) or any(
        not isinstance(v, (int, float)) or not isfinite(v) for v in analytical.values()
    ):
        raise ValueError("CRYPTO_MANDATORY_INCOMPLETE")
    fetched = now.isoformat()
    bar_url = SOURCE_URL.format(symbol=symbol)
    raw = []
    for bar in clean:
        raw.append({
            "ranking_run_id": run_id, "canonical_symbol": symbol,
            "provider_symbol": symbol + "-USD", "asset_class": "Crypto",
            "source": SPOT_SOURCE, "source_url_template": bar_url,
            **bar,
            "fetched_at_utc": fetched,
            "upsert_key": symbol + "|" + bar["trading_date"] + "|" + SPOT_SOURCE,
            "quality_status": "INDEPENDENT_SPOT_OHLCV_VALIDATED",
            "notes": "Coinbase Exchange direct USD spot daily OHLCV; volume in BASE asset units; completed UTC bars only",
        })
    canonical = [
        [r["trading_date"], r["open"], r["high"], r["low"], r["close"], r["volume"]]
        for r in raw
    ]
    digest = hashlib.sha256(json.dumps(
        canonical, separators=(",", ":"), allow_nan=False
    ).encode()).hexdigest()
    snapshot = {
        "ranking_run_id": run_id, "canonical_symbol": symbol,
        "asset_class": "Crypto", "score_model": "CRYPTO_V1",
        "data_timestamp": fetched, "analysis_price_source": PRICE_SOURCE,
        **analytical,
        "return_1d": factors["return_1d"], "return_5d": factors["return_5d"],
        "return_6m": factors["return_6m"], "return_12m": factors["return_12m"],
        "volatility_60d": factors["volatility_60d"],
        "distance_20dma": factors["distance_20dma"],
        "distance_50dma": factors["distance_50dma"],
        "distance_200dma": factors["distance_200dma"],
        "data_completeness_pct": 100,
        "mandatory_broad_pass": "TRUE",
        "broad_readiness": "BROAD_INPUTS_VALIDATED_NOT_SCORED",
        "missing_mandatory_fields": "NONE",
        "missing_optional_fields": "relative_strength_score; oi_change; catalyst_score; macro_score",
        "source_summary": (
            bar_url + " (USD spot OHLCV); "
            + ENDPOINT + "/products/" + symbol + "-USD/ticker (spot quote); "
            + DERIVATIVES_URL + " (native perpetual funding and OI)"
        ),
        "enrichment_status": "ACQUIRED_NOT_SCORED",
        "notes": (
            "Spot OHLCV/returns/momentum/trend/volume from Coinbase USD pair. "
            "Spot last_price from Coinbase ticker observed " + quote_time + ". "
            "Funding rate and open interest ONLY from native Hyperliquid perpetual. "
            "Open interest measured in base asset units; funding is current rate. "
            "No overall ranking or trading decisions computed."
        ),
    }
    return {
        "Raw_OHLCV": pd.DataFrame(raw),
        "Market_Data_Snapshot": pd.DataFrame([snapshot]),
    }, {
        "source": SPOT_SOURCE, "symbol": symbol, "daily_rows": len(raw),
        "run_id": run_id, "spot_quote_time_utc": quote_time,
        "ohlcv_sha256": digest, "mandatory_fields_present": len(MANDATORY),
        "mandatory_broad_pass": True, "ranking_permitted": False,
        "execution_permitted": False, "sheet_writes_performed": False,
    }
