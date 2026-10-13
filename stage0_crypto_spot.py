"""AskLiam V6.5.1 independent crypto SPOT analytics, without any writes.

CoinGecko daily market_chart returns *point samples* (00:00 UTC), NOT OHLC.
'Total_volumes' is USD rolling 24h trading volume at the sampling time,
NOT an exchange session volume.  Never back-fill OHLC with invented values.
No Hyperliquid-derived prices are used for analytical spot factors.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import statistics
from typing import Any

import pandas as pd

from stage0_crypto_probe import COINGECKO_IDS

VERSION = "V6.5.1"
SPOT_SOURCE = "COINGECKO_SPOT_USD_DAILY_POINT"
PRICE_SOURCE = "COINGECKO_SPOT_USD"
CHART_URL = "https://api.coingecko.com/api/v3/coins/{id}/market_chart"
MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
MIN_HISTORY = 253  # Minimum for 252-session optional return/12M.
MAX_HISTORY = 365
SPOT_QUOTE_TTL_SECONDS = 900
BROAD_MANDATORY = (
    "last_price", "return_1m", "return_3m", "volatility_20d",
    "momentum_score", "volume_score", "open_interest", "funding_rate",
    "trend_score",
)
OPTIONAL = (
    "return_1d", "return_5d", "return_6m", "return_12m",
    "volatility_60d", "drawdown_52w", "distance_20dma",
    "distance_50dma", "distance_200dma", "relative_strength_score",
    "oi_change", "catalyst_score", "macro_score",
)


def _number(value: Any, *, positive: bool = False) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        n = float(value)
    except (ValueError, OverflowError, TypeError):
        return None
    if not math.isfinite(n) or (positive and n <= 0):
        return None
    return n


def _timestamp_ms(value: Any) -> datetime:
    if isinstance(value, bool):
        raise ValueError("INVALID_SPOT_TIMESTAMP")
    try:
        n = int(value)
    except (ValueError, TypeError, OverflowError):
        raise ValueError("INVALID_SPOT_TIMESTAMP") from None
    if n < 1_000_000_000_000 or n > 4_102_444_800_000:
        raise ValueError("INVALID_SPOT_TIMESTAMP")
    return datetime.fromtimestamp(n / 1000, timezone.utc)


def _utc(now: datetime) -> datetime:
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("NAIVE_TIMESTAMP")
    return now.astimezone(timezone.utc)


def validate_coin_gecko_chart(
    symbol: str, payload: dict, *, now: datetime, min_history: int = MIN_HISTORY
) -> list[dict]:
    """Require 253-365 consecutive complete daily 00:00 UTC SPOT samples.

    Alignment is by exact UTC date.  Duplicate dates, nonmidnight points,
    stale tails and unavailable comparable USD 24h volume all fail closed.
    """
    now = _utc(now)
    if symbol not in COINGECKO_IDS or not isinstance(payload, dict):
        raise ValueError("INVALID_CANONICAL_SPOT_SYMBOL_OR_DATA")
    prices = payload.get("prices")
    volumes = payload.get("total_volumes")
    if not isinstance(prices, list) or not isinstance(volumes, list):
        raise ValueError("SPOT_PRICE_VOLUME_SERIES_MISSING")

    def extract(series, positive):
        result = {}
        for pair in series:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                raise ValueError("SPOT_SAMPLE_FORMAT")
            ts = _timestamp_ms(pair[0])
            if ts.date() >= now.date():
                continue  # excludes current UTC daily point or intraday samples.
            # CoinGecko 365-day series should supply one daily 00:00 UTC point.
            if ts.hour != 0 or ts.minute != 0 or ts.second != 0:
                raise ValueError("SPOT_SAMPLE_GRANULARITY_NOT_DAILY")
            day = ts.date().isoformat()
            value = _number(pair[1], positive=positive)
            if value is None:
                raise ValueError("SPOT_SAMPLE_INVALID_VALUE")
            if day in result:
                raise ValueError("SPOT_DUPLICATE_DAILY_SAMPLE")
            result[day] = value
        return result

    px = extract(prices, True)
    vol = extract(volumes, True)
    if set(px) != set(vol):
        raise ValueError("SPOT_PRICE_VOLUME_DATE_MISMATCH")
    days = sorted(px)
    if len(days) < min_history or len(days) > MAX_HISTORY:
        raise ValueError("SPOT_HISTORY_LENGTH_INSUFFICIENT")
    # Only the latest contiguous suffix will be used; forbids hiding gaps.
    for a, b in zip(days, days[1:]):
        if (datetime.fromisoformat(b) - datetime.fromisoformat(a)).days != 1:
            raise ValueError("SPOT_DAILY_GAP")
    if (now.date() - datetime.fromisoformat(days[-1]).date()).days > 2:
        raise ValueError("SPOT_DAILY_HISTORY_STALE")
    return [{"day": day, "close": px[day], "usd_volume_24h": vol[day]}
            for day in days]


def validate_spot_quote(symbol: str, payload: Any, *, now: datetime) -> dict:
    now = _utc(now)
    if symbol not in COINGECKO_IDS or not isinstance(payload, list):
        raise ValueError("INVALID_SPOT_QUOTE")
    matches = [x for x in payload
               if isinstance(x, dict) and x.get("id") == COINGECKO_IDS[symbol]
               and str(x.get("symbol", "")).upper() == symbol]
    if len(matches) != 1:
        raise ValueError("SPOT_QUOTE_IDENTITY_MISMATCH")
    row = matches[0]
    price = _number(row.get("current_price"), positive=True)
    ts_string = row.get("last_updated")
    if price is None or not isinstance(ts_string, str):
        raise ValueError("SPOT_QUOTE_MISSING")
    try:
        quoted_at = datetime.fromisoformat(ts_string.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("SPOT_QUOTE_TIMESTAMP_INVALID") from None
    if quoted_at.tzinfo is None:
        raise ValueError("SPOT_QUOTE_TIMESTAMP_INVALID")
    quoted_at = quoted_at.astimezone(timezone.utc)
    seconds = (now - quoted_at).total_seconds()
    if seconds < -60 or seconds > SPOT_QUOTE_TTL_SECONDS:
        raise ValueError("SPOT_QUOTE_STALE")
    return {"price_usd": price, "quoted_at_utc": quoted_at.isoformat()}


def _returns(prices, n):
    return 100 * (prices[-1] / prices[-1 - n] - 1) if len(prices) > n else None


def calculate_canonical_spot_technicals(points: list[dict]) -> dict:
    """Exactly the formulas specified in canonical Master Prompt section 47."""
    p = [float(x["close"]) for x in points]
    v = [float(x["usd_volume_24h"]) for x in points]
    if len(p) < MIN_HISTORY or any(not math.isfinite(x) or x <= 0 for x in p + v):
        raise ValueError("SPOT_TECHNICAL_INPUT_INSUFFICIENT")
    returns = {
        "return_1d": _returns(p, 1),
        "return_5d": _returns(p, 5),
        "return_1m": _returns(p, 21),
        "return_3m": _returns(p, 63),
        "return_6m": _returns(p, 126),
        "return_12m": _returns(p, 252),
    }
    daily_log = [math.log(p[i] / p[i-1]) for i in range(1, len(p))]
    volatility = lambda n: statistics.stdev(daily_log[-n:]) * math.sqrt(252) * 100
    sma = lambda n: sum(p[-n:]) / n
    distances = {
        "distance_20dma": 100*(p[-1]/sma(20)-1),
        "distance_50dma": 100*(p[-1]/sma(50)-1),
        "distance_200dma": 100*(p[-1]/sma(200)-1),
    }
    momentum = (
        .45*returns["return_1m"]
        + .35*returns["return_3m"]
        + .20*returns["return_6m"]
    )
    trend = (distances["distance_50dma"] + distances["distance_200dma"]) / 2
    avg_volume = statistics.mean(v[-21:-1])
    if avg_volume <= 0:
        raise ValueError("SPOT_VOLUME_REFERENCE_INVALID")
    ratio = v[-1]/avg_volume
    # max drawdown of the observed most recent 252 points.
    window = p[-252:]
    max_seen = window[0]
    min_dd = 0.
    for close in window:
        max_seen = max(max_seen, close)
        min_dd = min(min_dd, (close/max_seen-1)*100)
    return {
        **returns,
        "volatility_20d": volatility(20),
        "volatility_60d": volatility(60),
        "drawdown_52w": min_dd,
        **distances,
        "momentum_score": max(0., min(100., 50 + 50*math.tanh(momentum/15))),
        "trend_score": max(0., min(100., 50 + 50*math.tanh(trend/12))),
        "volume_score": max(0., min(100., 50 + 35*(ratio - 1))),
    }


def make_spot_frames(
    symbol: str, *, chart: dict, quotes: list, now: datetime, run_id: str,
) -> tuple[dict[str, pd.DataFrame], dict]:
    """Prepare pure point-observation history and spot-only snapshot patch."""
    now = _utc(now)
    if symbol not in COINGECKO_IDS:
        raise ValueError("SPOT_SYMBOL_UNKNOWN")
    if run_id != "STAGE0-" + now.strftime("%Y%m%d") + "-V6.5.1":
        raise ValueError("STAGE0_RUN_ID_MISMATCH")
    points = validate_coin_gecko_chart(symbol, chart, now=now)
    quote = validate_spot_quote(symbol, quotes, now=now)
    derived = calculate_canonical_spot_technicals(points)
    fetched = now.isoformat()
    source_url = CHART_URL.format(id=COINGECKO_IDS[symbol]) + "?vs_currency=usd&days=365"
    raw = [{
        "ranking_run_id": run_id,
        "canonical_symbol": symbol,
        "provider_symbol": COINGECKO_IDS[symbol],
        "asset_class": "Crypto",
        "source": SPOT_SOURCE,
        "source_url_template": source_url,
        "trading_date": x["day"],
        "open": "", "high": "", "low": "",
        "close": x["close"],
        "volume": x["usd_volume_24h"],
        "fetched_at_utc": fetched,
        "upsert_key": symbol + "|" + x["day"] + "|" + SPOT_SOURCE,
        "quality_status": "INDEPENDENT_SPOT_DAILY_POINT_NOT_OHLC",
        "notes": "CoinGecko UTC00 spot price sample, NOT OHLC; volume=rolling 24h USD snapshot, NOT daily exchange volume",
    } for x in points]
    snapshot = {
        "ranking_run_id": run_id,
        "canonical_symbol": symbol,
        "asset_class": "Crypto",
        "score_model": "CRYPTO_V1",
        "data_timestamp": quote["quoted_at_utc"],
        "analysis_price_source": PRICE_SOURCE,
        "last_price": quote["price_usd"],
        **derived,
    }
    summary = (
        source_url + "; " + MARKETS_URL + "?vs_currency=usd&ids="
        + COINGECKO_IDS[symbol]
        + " (spot quote); validated 00:00 UTC price + trailing-24h USD volume, "
        + str(len(raw)) + " point observations"
    )
    snapshot["source_summary"] = summary
    snapshot["notes"] = (
        "Spot 00UTC closes and live CoinGecko quote are independent of Hyperliquid. "
        "Price history is daily spot point series not OHLC. USD rolling-24h "
        "volume sampled at UTC 00:00 is a comparable volume proxy, "
        "not exchange-session executed volume. Technical score formulas: "
        "Master Prompt V6.5.1 s47. Relative strength and macro remain unfilled."
    )
    canonical = [[r["trading_date"],r["close"],r["volume"]] for r in raw]
    digest = hashlib.sha256(
        json.dumps(canonical,separators=(",",":"),allow_nan=False).encode()
    ).hexdigest()
    return {
        "Raw_OHLCV": pd.DataFrame(raw),
        "Market_Data_Snapshot": pd.DataFrame([snapshot]),
    }, {
        "version": VERSION,
        "symbol": symbol,
        "source": SPOT_SOURCE,
        "data_points": len(raw),
        "first_date": raw[0]["trading_date"],
        "last_date": raw[-1]["trading_date"],
        "sha256": digest,
        "quote_timestamp": quote["quoted_at_utc"],
        "ranking_permitted": False,
        "execution_permitted": False,
        "sheet_writes_performed": False,
    }


def merge_derivative_and_spot_snapshot(
    previous: dict, proposed: dict, *, now: datetime,
) -> dict:
    """Only fill spot fields onto a SAME-DAY verified Hyperliquid row.

    Mandatory pass means *Stage0 data sufficiency*, never scoring permission.
    """
    now = _utc(now)
    if (
        not isinstance(previous, dict)
        or previous.get("ranking_run_id") != proposed.get("ranking_run_id")
        or previous.get("canonical_symbol") != proposed.get("canonical_symbol")
        or previous.get("score_model") != "CRYPTO_V1"
        or previous.get("analysis_price_source")
           != "HYPERLIQUID_NATIVE_PERP_REFERENCE_ONLY"
        or previous.get("last_price") not in ("", None)
        or previous.get("enrichment_status") != "ACQUIRED_PERPETUAL_NOT_SPOT"
    ):
        raise ValueError("SPOT_SNAPSHOT_COLLISION")
    if _number(previous.get("open_interest"), positive=True) is None or (
        _number(previous.get("funding_rate")) is None
    ):
        raise ValueError("SPOT_MERGE_DERIVATIVES_MISSING")
    try:
        derivative_at = datetime.fromisoformat(
            str(previous["data_timestamp"]).replace("Z", "+00:00")
        )
    except (ValueError, TypeError, KeyError):
        raise ValueError("SPOT_MERGE_DERIVATIVES_TIMESTAMP") from None
    if derivative_at.tzinfo is None or not (
        -60 <= (now - derivative_at.astimezone(timezone.utc)).total_seconds()
        <= 900
    ):
        raise ValueError("SPOT_MERGE_DERIVATIVES_STALE")
    combined = dict(proposed)
    combined["open_interest"] = _number(previous["open_interest"], positive=True)
    combined["funding_rate"] = _number(previous["funding_rate"])
    available = [field for field in BROAD_MANDATORY
                 if _number(combined.get(field)) is not None]
    missing = [field for field in BROAD_MANDATORY if field not in available]
    combined["data_completeness_pct"] = round(100*len(available)/len(BROAD_MANDATORY),2)
    combined["mandatory_broad_pass"] = "TRUE" if not missing else "FALSE"
    combined["broad_readiness"] = (
        "BROAD_SCAN_READY" if not missing else "INCOMPLETE_MANDATORY"
    )
    combined["missing_mandatory_fields"] = "; ".join(missing)
    combined["missing_optional_fields"] = "; ".join(
        x for x in OPTIONAL if _number(combined.get(x)) is None
    )
    combined["enrichment_status"] = (
        "INDEPENDENT_SPOT_DERIVATIVE_CONTEXT_VALIDATED"
        if not missing else "INCOMPLETE_MANDATORY"
    )
    combined["source_summary"] += "; " + str(previous.get("source_summary") or "")
    combined["notes"] += (
        " Hyperliquid derivative context separately validated at "
        + str(previous["data_timestamp"])
        + "; open_interest in BASE ASSET units. Broad readiness is NOT a rank."
    )
    return combined
