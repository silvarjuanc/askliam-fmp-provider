"""Independent exchange spot OHLCV probe for V6.5.1. Read only.

Coinbase Exchange daily BTC-USD etc spot candles: [unix seconds,
low, high, open, close, volume BASE]. No futures/perpetuals, no synthetic
pair conversions. Quarantines incomplete current UTC day and historical
gaps. Source candle count may be less than requested for illiquid pairs.
"""
from datetime import datetime, timedelta, timezone
from math import isfinite
import httpx

from stage0_crypto_probe import COINGECKO_IDS
from stage0_canonical_factors import canonical_technicals, volume_score

ENDPOINT = "https://api.exchange.coinbase.com"
SOURCE = "COINBASE_EXCHANGE_SPOT_USD"
MIN_BARS = 253
MAX_BARS = 300

class CoinbaseSpotError(ValueError):
    pass

def _number(value):
    if isinstance(value, bool):
        raise CoinbaseSpotError("COINBASE_INVALID_NUMERIC")
    try:
        num = float(value)
    except (TypeError, ValueError, OverflowError):
        raise CoinbaseSpotError("COINBASE_INVALID_NUMERIC") from None
    if not isfinite(num) or num <= 0:
        raise CoinbaseSpotError("COINBASE_INVALID_NUMERIC")
    return num

def validate_candles(payload, symbol: str, now: datetime, min_bars=MIN_BARS):
    if now.tzinfo is None or symbol not in COINGECKO_IDS:
        raise CoinbaseSpotError("COINBASE_INVALID_CONTEXT")
    if not isinstance(payload, list):
        raise CoinbaseSpotError("COINBASE_INVALID_SCHEMA")
    today = now.astimezone(timezone.utc).date()
    bars = []
    seen = set()
    for entry in payload:
        if not isinstance(entry, list) or len(entry) != 6:
            raise CoinbaseSpotError("COINBASE_INVALID_CANDLE")
        ts = entry[0]
        if isinstance(ts, bool):
            raise CoinbaseSpotError("COINBASE_INVALID_TIMESTAMP")
        try:
            ts = int(ts)
        except (TypeError, ValueError, OverflowError):
            raise CoinbaseSpotError("COINBASE_INVALID_TIMESTAMP") from None
        if ts < 1500000000 or ts > 4102444800 or ts % 86400:
            raise CoinbaseSpotError("COINBASE_INVALID_DAILY_TIMESTAMP")
        date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        if date > today:
            raise CoinbaseSpotError("COINBASE_FUTURE_CANDLE")
        if date == today:
            continue
        if date in seen:
            raise CoinbaseSpotError("COINBASE_DUPLICATE_DAILY_CANDLE")
        seen.add(date)
        low, high, opn, close, volume = [_number(x) for x in entry[1:]]
        if low > high or not (low <= opn <= high and low <= close <= high):
            raise CoinbaseSpotError("COINBASE_INCONSISTENT_OHLC")
        bars.append({"trading_date":date.isoformat(),"open":opn,"high":high,
                     "low":low,"close":close,"volume_base":volume})
    bars.sort(key=lambda x:x["trading_date"])
    if len(bars) < min_bars or len(bars) > MAX_BARS:
        raise CoinbaseSpotError("COINBASE_HISTORY_COVERAGE")
    first = datetime.fromisoformat(bars[0]["trading_date"]).date()
    last = datetime.fromisoformat(bars[-1]["trading_date"]).date()
    if (last - first).days != len(bars)-1:
        raise CoinbaseSpotError("COINBASE_DAILY_GAP")
    if (today - last).days > 2:
        raise CoinbaseSpotError("COINBASE_STALE_HISTORY")
    prices = [bar["close"] for bar in bars]
    volumes = [bar["volume_base"] for bar in bars]
    factors = canonical_technicals(prices, sessions_per_year=365)
    avg_previous_20 = sum(volumes[-21:-1])/20
    factors["volume_score"] = volume_score(
        volumes[-1], avg_previous_20, source_verified=True,
    )
    if factors["volume_score"] is None:
        raise CoinbaseSpotError("COINBASE_UNRELIABLE_VOLUME")
    return {
        "version":"V6.5.1","status":"SPOT_OHLCV_VALIDATED_READ_ONLY",
        "symbol":symbol,"source":SOURCE,
        "provider_symbol":symbol+"-USD",
        "source_url_template":ENDPOINT+"/products/"+symbol+"-USD/candles",
        "history_kind":"INDEPENDENT_USD_SPOT_EXCHANGE_CANDLES",
        "daily_bars":len(bars),"last_complete_utc_date":bars[-1]["trading_date"],
        "canonical_technicals":factors,
        "mandatory_broad_pass":False,
        "ranking_permitted":False,"execution_permitted":False,
        "sheet_writes_performed":False,
    }

async def probe_spot(symbol: str, client: httpx.AsyncClient, *, now=None):
    now = now or datetime.now(timezone.utc)
    symbol = str(symbol).strip().upper()
    if symbol not in COINGECKO_IDS:
        raise CoinbaseSpotError("COINBASE_UNSUPPORTED_ASSET")
    if now.tzinfo is None:
        raise CoinbaseSpotError("COINBASE_NAIVE_CLOCK")
    cutoff = now.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    beginning = cutoff - timedelta(days=299)
    try:
        response = await client.get(
            ENDPOINT+"/products/"+symbol+"-USD/candles",
            params={
                "granularity":"86400",
                "start":beginning.isoformat().replace("+00:00","Z"),
                "end":cutoff.isoformat().replace("+00:00","Z"),
            },
            headers={"Accept":"application/json"},
            timeout=30,
        )
    except httpx.HTTPError:
        raise CoinbaseSpotError("COINBASE_TRANSPORT_ERROR") from None
    if response.status_code == 429:
        raise CoinbaseSpotError("COINBASE_RATE_LIMIT")
    if response.status_code in (401,403):
        raise CoinbaseSpotError("COINBASE_ACCESS_DENIED")
    if response.status_code == 404:
        raise CoinbaseSpotError("COINBASE_PAIR_NOT_LISTED")
    if response.status_code != 200:
        raise CoinbaseSpotError("COINBASE_HTTP_ERROR") 
    try:
        payload = response.json()
    except ValueError:
        raise CoinbaseSpotError("COINBASE_INVALID_JSON") from None
    return validate_candles(payload, symbol, now)
