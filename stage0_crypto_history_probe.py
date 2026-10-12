"""AskLiam V6.5.1: bounded read-only Hyperliquid CRYPTO_V1 history probe.

The instrument is a native PERPETUAL, not an independent spot price.
No Sheet, checkpoint, score, portfolio, or trading changes are possible.
Uses three read-only Info POSTs: metaAndAssetCtxs, candleSnapshot,
fundingHistory. The results are validity diagnostics, not rank permission.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
from typing import Any

import httpx

from stage0_crypto_probe import (
    COINGECKO_IDS, HYPERLIQUID_INFO, hyperliquid_rows,
    classify_response_status, finite_number, positive_finite,
)

DAY_MS = 86_400_000
WINDOW_DAYS = 150
FUNDING_WINDOW_DAYS = 14
MIN_DAILY_CANDLES = 64  # 63-day lookback requires 64 closes.
MIN_RECENT_HOURS = 48


def _timestamp_ms(value):
    if isinstance(value, bool):
        raise ValueError("INVALID_TIMESTAMP")
    try:
        timestamp = int(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("INVALID_TIMESTAMP") from None
    if timestamp < 1_000_000_000_000 or timestamp > 4_102_444_800_000:
        raise ValueError("INVALID_TIMESTAMP")
    return timestamp


def _nonnegative(value):
    number = finite_number(value)
    if number is None or number < 0:
        raise ValueError("INVALID_NONNEGATIVE_NUMBER")
    return number


def validate_daily_candles(payload: Any, symbol: str, *, now: datetime) -> dict:
    """Summarize complete, consecutive UTC days; never include live candle.

    Missing intervals or stale bars are explicit and never interpolated.
    """
    if not isinstance(payload, list) or len(payload) > 5000:
        raise ValueError("CANDLE_RESPONSE_CONTRACT_INVALID")
    today_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    today_ms = int(today_start.timestamp() * 1000)
    seen = {}
    for row in payload:
        if not isinstance(row, dict) or row.get("s") != symbol or row.get("i") != "1d":
            raise ValueError("CANDLE_INSTRUMENT_MISMATCH")
        t = _timestamp_ms(row.get("t"))
        close_time = _timestamp_ms(row.get("T"))
        if t % DAY_MS != 0 or close_time < t or close_time >= t + DAY_MS:
            raise ValueError("CANDLE_TIME_INVALID")
        # A provider may include the current, incomplete UTC daily candle.
        if t >= today_ms:
            continue
        if t in seen:
            raise ValueError("CANDLE_DUPLICATE_DAY")
        o = positive_finite(row.get("o"))
        h = positive_finite(row.get("h"))
        l = positive_finite(row.get("l"))
        c = positive_finite(row.get("c"))
        v = _nonnegative(row.get("v"))
        if any(n is None for n in (o, h, l, c)):
            raise ValueError("CANDLE_PRICE_INVALID")
        if not (l <= min(o, c) and h >= max(o, c) and h >= l):
            raise ValueError("CANDLE_OHLC_INCONSISTENT")
        seen[t] = (c, v)
    if not seen:
        return {
            "status": "NO_COMPLETE_DAILY_CANDLES",
            "complete_candles": 0,
            "consecutive_candles": 0,
        }
    times = sorted(seen)
    suffix = [times[-1]]
    for t in reversed(times[:-1]):
        if suffix[-1] - t != DAY_MS:
            break
        suffix.append(t)
    suffix.reverse()
    closes = [seen[t][0] for t in suffix]
    last_ts = times[-1]
    last_complete_day = datetime.fromtimestamp(last_ts / 1000, timezone.utc)
    last_close_utc = last_complete_day + timedelta(days=1)
    age_hours = (now - last_close_utc).total_seconds() / 3600
    enough = len(suffix) >= MIN_DAILY_CANDLES
    fresh = 0 <= age_hours <= MIN_RECENT_HOURS
    metrics = {}
    if enough and fresh:
        metrics["perpetual_return_1m_pct"] = 100 * (closes[-1] / closes[-22] - 1)
        metrics["perpetual_return_3m_pct"] = 100 * (closes[-1] / closes[-64] - 1)
        logs = [
            math.log(closes[i] / closes[i - 1])
            for i in range(len(closes) - 20, len(closes))
        ]
        avg = sum(logs) / len(logs)
        stdev = math.sqrt(sum((x - avg) ** 2 for x in logs) / (len(logs) - 1))
        metrics["perpetual_volatility_20d_annual_pct"] = stdev * math.sqrt(365) * 100
    return {
        "status": (
            "DERIVATIVE_HISTORY_VALIDATED" if enough and fresh
            else "DERIVATIVE_HISTORY_STALE" if not fresh
            else "DERIVATIVE_HISTORY_INSUFFICIENT"
        ),
        "complete_candles": len(times),
        "consecutive_candles": len(suffix),
        "earliest_contiguous_day_utc": datetime.fromtimestamp(
            suffix[0] / 1000, timezone.utc
        ).date().isoformat(),
        "last_complete_day_utc": last_complete_day.date().isoformat(),
        "last_close_age_hours": round(age_hours, 2),
        "last_perpetual_daily_close_usd": closes[-1],
        "history_kind": "HYPERLIQUID_NATIVE_PERPETUAL",
        "base_volume_last_day": seen[last_ts][1],
        **metrics,
    }


def validate_funding_history(payload: Any, symbol: str, *, now: datetime) -> dict:
    """Realized funding; do not conflate a historical average and current rate."""
    if not isinstance(payload, list) or len(payload) > 5000:
        raise ValueError("FUNDING_RESPONSE_CONTRACT_INVALID")
    cutoff = int((now - timedelta(days=FUNDING_WINDOW_DAYS)).timestamp() * 1000)
    end = int(now.timestamp() * 1000)
    rates = {}
    for row in payload:
        if not isinstance(row, dict) or row.get("coin") != symbol:
            raise ValueError("FUNDING_INSTRUMENT_MISMATCH")
        stamp = _timestamp_ms(row.get("time"))
        if stamp < cutoff or stamp > end:
            continue
        if stamp in rates:
            raise ValueError("DUPLICATE_FUNDING_TIMESTAMP")
        rate = finite_number(row.get("fundingRate"))
        if rate is None or abs(rate) > 1.0:
            raise ValueError("INVALID_FUNDING_RATE")
        rates[stamp] = rate
    if not rates:
        return {"status": "FUNDING_HISTORY_EMPTY", "funding_observations": 0}
    times = sorted(rates)
    last = times[-1]
    return {
        "status": "REALIZED_FUNDING_OBSERVED",
        "funding_observations": len(rates),
        "last_rate": rates[last],
        "mean_observed_rate": sum(rates.values()) / len(rates),
        "last_funding_time_utc": datetime.fromtimestamp(
            last / 1000, timezone.utc
        ).isoformat(),
        "funding_kind": "HYPERLIQUID_NATIVE_PERPETUAL",
    }


async def probe_hyperliquid_history(
    symbol: str, *, client: httpx.AsyncClient, now: datetime | None = None,
) -> dict:
    """One-symbol, three-call ceiling, no retries, side-effect-free."""
    symbol = str(symbol).strip().upper()
    if symbol not in COINGECKO_IDS:
        raise ValueError("CRYPTO_SYMBOL_NOT_IN_CANONICAL_MAPPING")
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("NAIVE_CLOCK_NOT_ALLOWED")
    now = now.astimezone(timezone.utc)
    calls = 0

    async def post(body):
        nonlocal calls
        calls += 1
        try:
            response = await client.post(HYPERLIQUID_INFO, json=body, timeout=18.0)
            status = classify_response_status(response)
            if status != "AVAILABLE":
                return status, None
            return "AVAILABLE", response.json()
        except (httpx.RequestError, httpx.TimeoutException):
            return "NETWORK_ERROR", None
        except (ValueError, TypeError, KeyError, OverflowError):
            return "RESPONSE_CONTRACT_INVALID", None

    base = {
        "version": "V6.5.1",
        "status": "READ_ONLY_HYPERLIQUID_HISTORY_PROBE",
        "symbol": symbol,
        "model": "CRYPTO_V1",
        "perpetual_not_spot": True,
        "data_timestamp_utc": now.isoformat(),
        "source": HYPERLIQUID_INFO,
        "ranking_permitted": False,
        "execution_permitted": False,
        "mandatory_broad_pass": False,
        "sheets_written": False,
        "checkpoint_advanced": False,
        "fmp_requests_performed": False,
    }
    context_status, context_payload = await post({"type": "metaAndAssetCtxs"})
    if context_status != "AVAILABLE":
        return {**base, "context_status": context_status, "requests_performed": calls}
    try:
        context = hyperliquid_rows(context_payload, [symbol])[symbol]
    except (ValueError, KeyError, TypeError, OverflowError):
        return {**base, "context_status": "RESPONSE_CONTRACT_INVALID",
                "requests_performed": calls}
    if context.get("status") != "PERP_CONTEXT_OBSERVED":
        return {**base, "context_status": context["status"], "requests_performed": calls}
    base["context_status"] = "AVAILABLE"
    base["perpetual_context"] = context

    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    start_ms = int((day_start - timedelta(days=WINDOW_DAYS)).timestamp() * 1000)
    end_ms = int(day_start.timestamp() * 1000)
    candle_status, candles = await post({
        "type": "candleSnapshot",
        "req": {"coin": symbol, "interval": "1d",
                "startTime": start_ms, "endTime": end_ms},
    })
    if candle_status == "AVAILABLE":
        try:
            history = validate_daily_candles(candles, symbol, now=now)
        except (ValueError, TypeError, KeyError, OverflowError):
            history = {"status": "RESPONSE_CONTRACT_INVALID"}
    else:
        history = {"status": candle_status}
    base["perpetual_daily_history"] = history

    # Do not hammer API when upstream is rate-limited or unavailable.
    if candle_status in ("HTTP_429", "HTTP_403", "NETWORK_ERROR"):
        return {**base, "funding_history": {"status": "NOT_REQUESTED_AFTER_SOURCE_FAILURE"},
                "requests_performed": calls}

    funding_status, funding_rows = await post({
        "type": "fundingHistory", "coin": symbol,
        "startTime": int((now - timedelta(days=FUNDING_WINDOW_DAYS)).timestamp()*1000),
        "endTime": int(now.timestamp()*1000),
    })
    if funding_status == "AVAILABLE":
        try:
            funding = validate_funding_history(funding_rows, symbol, now=now)
        except (ValueError, TypeError, KeyError, OverflowError):
            funding = {"status": "RESPONSE_CONTRACT_INVALID"}
    else:
        funding = {"status": funding_status}
    return {**base, "funding_history": funding, "requests_performed": calls}
