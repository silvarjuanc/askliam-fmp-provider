"""AskLiam V6.5.1 CRYPTO_V1 independent, read-only source readiness.

Quotes: CoinGecko IDs, never a fuzzy symbol search. Derivatives context:
Hyperliquid first-perp-DEX metaAndAssetCtxs, exact universe names only.
No Google Sheets writes, no Redis checkpoint changes, no ranking/execution.
Historical coverage, funding history, OI history and scoring deliberately
remain unverified until dedicated adapters validate them.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import math
import os
from typing import Any

import httpx

COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"
HYPERLIQUID_INFO = "https://api.hyperliquid.xyz/info"
COINGECKO_IDS = {
    "ADA": "cardano",
    "BTC": "bitcoin",
    "DOGE": "dogecoin",
    "ETH": "ethereum",
    "LINK": "chainlink",
    "SOL": "solana",
    "SUI": "sui",
    "XRP": "ripple",
}
QUOTE_MAX_AGE_SECONDS = 15 * 60


def positive_finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def parse_source_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        when = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return when.astimezone(timezone.utc) if when.tzinfo else None


def classify_response_status(response: httpx.Response) -> str:
    """Only expose HTTP status digits; never provider response body or keys."""
    if response.status_code != 200:
        return "HTTP_" + str(response.status_code)
    return "AVAILABLE"


def coingecko_rows(payload: Any, symbols: list[str], now: datetime) -> dict:
    if not isinstance(payload, list):
        raise ValueError("CG_RESPONSE_CONTRACT_INVALID")
    id_map = {}
    for item in payload:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            raise ValueError("CG_RESPONSE_CONTRACT_INVALID")
        identifier = item["id"]
        if identifier in id_map:
            raise ValueError("CG_DUPLICATE_ID")
        id_map[identifier] = item
    output = {}
    for symbol in symbols:
        asset_id = COINGECKO_IDS.get(symbol)
        if asset_id is None:
            output[symbol] = {"status": "SYMBOL_NOT_MAPPED"}
            continue
        item = id_map.get(asset_id)
        if item is None:
            output[symbol] = {"status": "COIN_ID_NOT_RETURNED", "coin_id": asset_id}
            continue
        # Both ID and ticker must agree to avoid ambiguous cryptocurrency assets.
        if str(item.get("symbol") or "").strip().upper() != symbol:
            output[symbol] = {"status": "ID_SYMBOL_MISMATCH", "coin_id": asset_id}
            continue
        price = positive_finite(item.get("current_price"))
        stamp = parse_source_time(item.get("last_updated"))
        age = (now - stamp).total_seconds() if stamp is not None else None
        fresh = age is not None and 0 <= age <= QUOTE_MAX_AGE_SECONDS
        output[symbol] = {
            "status": (
                "QUOTE_FRESH" if price is not None and fresh
                else "QUOTE_STALE_OR_INVALID"
            ),
            "coin_id": asset_id,
            "price_usd": price,
            "last_updated_utc": stamp.isoformat() if stamp else None,
            "age_seconds": round(age) if age is not None else None,
            "total_volume_usd_24h": finite_number(item.get("total_volume")),
            "market_cap_usd": finite_number(item.get("market_cap")),
        }
    return output


def hyperliquid_rows(payload: Any, symbols: list[str]) -> dict:
    """Return only exact-named perpetuals; no spot/HIP-3 proxy assumptions."""
    if (not isinstance(payload, list) or len(payload) != 2
        or not isinstance(payload[0], dict)
        or not isinstance(payload[1], list)):
        raise ValueError("HL_RESPONSE_CONTRACT_INVALID")
    universe = payload[0].get("universe")
    contexts = payload[1]
    if not isinstance(universe, list) or len(universe) != len(contexts):
        raise ValueError("HL_CONTEXT_LENGTH_MISMATCH")
    mapping = {}
    for market, context in zip(universe, contexts):
        if not isinstance(market, dict) or not isinstance(context, dict):
            raise ValueError("HL_RESPONSE_CONTRACT_INVALID")
        name = market.get("name")
        if not isinstance(name, str):
            raise ValueError("HL_RESPONSE_CONTRACT_INVALID")
        if name in mapping:
            raise ValueError("HL_DUPLICATE_MARKET")
        mapping[name] = context
    output = {}
    for symbol in symbols:
        row = mapping.get(symbol)
        if row is None:
            output[symbol] = {"status": "PERP_NOT_LISTED"}
            continue
        output[symbol] = {
            "status": "PERP_CONTEXT_OBSERVED",
            "mark_price_usd": positive_finite(row.get("markPx")),
            "oracle_price_usd": positive_finite(row.get("oraclePx")),
            "funding_rate": finite_number(row.get("funding")),
            "open_interest_base_units": finite_number(row.get("openInterest")),
            "day_notional_volume_usd": finite_number(row.get("dayNtlVlm")),
        }
    return output


async def probe_crypto_sources(
    symbols: list[str], *, client: httpx.AsyncClient, now: datetime | None = None,
    coingecko_key: str | None = None
) -> dict:
    """Two bounded network reads, independent provider errors.

    One CoinGecko markets GET for the cohort and one Hyperliquid Info POST.
    No historical data acquisition or ready-to-score assertion.
    """
    now = now or datetime.now(timezone.utc)
    symbols = sorted(set(str(s).strip().upper() for s in symbols))
    if not symbols or len(symbols) > 8 or any(s not in COINGECKO_IDS for s in symbols):
        raise ValueError("CRYPTO_UNIVERSE_REQUIRES_EXACT_MAPPING")

    headers = {"accept": "application/json"}
    if coingecko_key:
        headers["x-cg-demo-api-key"] = coingecko_key

    async def fetch_gecko():
        try:
            resp = await client.get(
                COINGECKO_MARKETS,
                params={"vs_currency": "usd",
                        "ids": ",".join(COINGECKO_IDS[s] for s in symbols),
                        "per_page": 8, "page": 1},
                headers=headers, timeout=15.0,
            )
            status = classify_response_status(resp)
            if status != "AVAILABLE":
                return status, {}
            return "AVAILABLE", coingecko_rows(resp.json(), symbols, now)
        except (httpx.RequestError, httpx.TimeoutException):
            return "NETWORK_ERROR", {}
        except (ValueError, TypeError, KeyError, OverflowError):
            return "RESPONSE_CONTRACT_INVALID", {}

    async def fetch_hyperliquid():
        try:
            resp = await client.post(
                HYPERLIQUID_INFO,
                json={"type": "metaAndAssetCtxs"},
                timeout=15.0,
            )
            status = classify_response_status(resp)
            if status != "AVAILABLE":
                return status, {}
            return "AVAILABLE", hyperliquid_rows(resp.json(), symbols)
        except (httpx.RequestError, httpx.TimeoutException):
            return "NETWORK_ERROR", {}
        except (ValueError, TypeError, KeyError, OverflowError):
            return "RESPONSE_CONTRACT_INVALID", {}

    (cg_status, quotes), (hl_status, perps) = await asyncio.gather(
        fetch_gecko(), fetch_hyperliquid()
    )
    assets = []
    for symbol in symbols:
        assets.append({
            "canonical_symbol": symbol,
            "model": "CRYPTO_V1",
            "coingecko_id": COINGECKO_IDS[symbol],
            "underlying_spot_quote": quotes.get(symbol, {"status": cg_status}),
            "hyperliquid_perpetual_context": perps.get(symbol, {"status": hl_status}),
            "source_history_validated": False,
            "mandatory_broad_pass": False,
        })
    return {
        "version": "V6.5.1",
        "status": "READ_ONLY_CRYPTO_SOURCE_PROBE",
        "universe_crypto_count": len(symbols),
        "coingecko_status": cg_status,
        "hyperliquid_status": hl_status,
        "data_timestamp_utc": now.isoformat(),
        "sources": {
            "underlying": "https://api.coingecko.com/api/v3/coins/markets",
            "derivatives": "https://api.hyperliquid.xyz/info",
        },
        "assets": assets,
        "sheets_written": False,
        "checkpoint_advanced": False,
        "fmp_requests_performed": False,
        "ranking_permitted": False,
        "execution_permitted": False,
    }
