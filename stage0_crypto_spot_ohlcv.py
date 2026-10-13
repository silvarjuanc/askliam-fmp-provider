"""Independent spot OHLCV candidate from CryptoCompare CCCAGG, READ ONLY.

Never treat synthetic BTC-converted pairs as direct USD markets; parameter
tryConversion=false is mandatory. No writes, scoring permissions or trades.
"""
from datetime import datetime,timezone
from math import isfinite

import httpx
from stage0_crypto_probe import COINGECKO_IDS

SOURCE="CRYPTOCOMPARE_CCCAGG_SPOT_USD"
URL="https://min-api.cryptocompare.com/data/v2/histoday"

class OHLCError(ValueError): pass

def validate_ohlc(payload,symbol,now,min_sessions=253):
    if symbol not in COINGECKO_IDS or now.tzinfo is None:
        raise OHLCError("INVALID_SYMBOL_OR_CLOCK")
    if not isinstance(payload,dict) or payload.get("Response")!="Success":
        raise OHLCError("OHLC_SOURCE_RESPONSE_NOT_SUCCESS")
    rows=payload.get("Data",{}).get("Data")
    if not isinstance(rows,list): raise OHLCError("OHLC_SOURCE_EMPTY")
    out=[]; last=-1
    today=now.astimezone(timezone.utc).date()
    for row in rows:
        if not isinstance(row,dict) or isinstance(row.get("time"),bool):
            raise OHLCError("OHLC_ROW_MALFORMED")
        try: stamp=int(row["time"])
        except (TypeError,ValueError,KeyError): raise OHLCError("OHLC_BAD_TIME") from None
        if stamp<=last: raise OHLCError("OHLC_TIME_DUPLICATE")
        last=stamp
        day=datetime.fromtimestamp(stamp,tz=timezone.utc).date()
        if day>today: raise OHLCError("OHLC_FUTURE_BAR")
        if day==today: continue
        try: values={k:float(row[k]) for k in ("open","high","low","close","volumeto")}
        except (ValueError,TypeError,KeyError): raise OHLCError("OHLC_BAD_NUMERIC") from None
        o,h,l,close=values["open"],values["high"],values["low"],values["close"]
        if (not all(isfinite(v) for v in values.values()) or
            min(o,h,l,close)<=0 or values["volumeto"]<=0 or
            l>min(o,close) or h<max(o,close) or l>h):
            raise OHLCError("OHLC_INVALID_CANDLE")
        out.append({"trading_date":day.isoformat(),
                    "open":o,"high":h,"low":l,"close":close,
                    "volume_quote_usd":values["volumeto"]})
    if len(out)<min_sessions: raise OHLCError("OHLC_INSUFFICIENT_SESSIONS")
    for i in range(1,len(out)):
        previous=datetime.fromisoformat(out[i-1]["trading_date"]).date()
        current=datetime.fromisoformat(out[i]["trading_date"]).date()
        if (current-previous).days!=1: raise OHLCError("OHLC_DATE_GAP")
    if (today-datetime.fromisoformat(out[-1]["trading_date"]).date()).days>2:
        raise OHLCError("OHLC_STALE")
    return {"version":"V6.5.1","symbol":symbol,"source":SOURCE,
            "history_kind":"INDEPENDENT_AGGREGATED_SPOT_USD_OHLCV",
            "source_url_template":URL+"?fsym="+symbol+"&tsym=USD&limit=300&e=CCCAGG",
            "daily_bars":len(out),"rows":out,"ranking_permitted":False,
            "execution_permitted":False,"sheet_writes_performed":False}

async def probe_ohlc(symbol,client,now=None):
    now=now or datetime.now(timezone.utc)
    symbol=str(symbol).upper()
    if symbol not in COINGECKO_IDS: raise OHLCError("OHLC_SYMBOL_UNSUPPORTED")
    try:
        resp=await client.get(URL,params={"fsym":symbol,"tsym":"USD",
            "limit":"300","aggregate":"1","e":"CCCAGG",
            "tryConversion":"false"},timeout=30)
    except httpx.HTTPError: raise OHLCError("OHLC_SOURCE_NETWORK_ERROR") from None
    if resp.status_code==429: raise OHLCError("OHLC_SOURCE_RATE_LIMIT")
    if resp.status_code!=200: raise OHLCError("OHLC_SOURCE_HTTP_ERROR")
    try: payload=resp.json()
    except ValueError: raise OHLCError("OHLC_SOURCE_INVALID_JSON") from None
    return validate_ohlc(payload,symbol,now)
