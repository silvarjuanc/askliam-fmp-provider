"""Read-only independent crypto spot adapter, no checkpoint or Sheets write."""
from datetime import datetime, timezone
import os

import httpx

from stage0_crypto_probe import COINGECKO_IDS
from stage0_canonical_factors import canonical_technicals
from stage0_crypto_spot_history import (
    SpotDataError, validate_spot_series, raw_spot_indicators, readiness_preview,
)

BASE = "https://api.coingecko.com/api/v3"

async def independent_spot_preview(symbol: str, client: httpx.AsyncClient, now=None):
    symbol = str(symbol).upper()
    if symbol not in COINGECKO_IDS:
        raise SpotDataError("SPOT_UNKNOWN_SYMBOL")
    now = now or datetime.now(timezone.utc)
    coin_id = COINGECKO_IDS[symbol]
    headers={"accept":"application/json"}
    key=os.getenv("COINGECKO_DEMO_API_KEY", "").strip()
    if key:
        headers["x-cg-demo-api-key"]=key
    try:
        response = await client.get(
            BASE + "/coins/" + coin_id + "/market_chart",
            params={"vs_currency":"usd","days":"365"},
            headers=headers,
            timeout=30,
        )
    except httpx.HTTPError:
        raise SpotDataError("SPOT_TRANSPORT_FAILURE") from None
    if response.status_code == 429:
        raise SpotDataError("SPOT_RATE_LIMIT_BACKOFF")
    if response.status_code in (401,402,403):
        raise SpotDataError("SPOT_ENTITLEMENT_FAILURE")
    if response.status_code != 200:
        raise SpotDataError("SPOT_HTTP_FAILURE")
    try: payload=response.json()
    except ValueError: raise SpotDataError("SPOT_INVALID_JSON") from None
    history=validate_spot_series(payload, symbol, now)
    result=readiness_preview(history, raw_spot_indicators(history))
    result["canonical_technicals"] = canonical_technicals(
        [r["spot_price"] for r in history["rows"]]
    )
    result["canonical_volume_score"] = None  # historical 24h volume not a validated session volume
    result["mandatory_broad_pass"] = False
    result["ranking_permitted"] = False
    return result
