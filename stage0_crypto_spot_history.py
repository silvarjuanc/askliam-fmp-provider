"""Read-only independent spot series validator. Never invent OHLC or rank."""
from datetime import datetime, timezone
from math import isfinite, sqrt
from statistics import stdev

from stage0_crypto_probe import COINGECKO_IDS

class SpotDataError(ValueError):
    pass

def _num(x, *, positive=True):
    if isinstance(x, bool):
        raise SpotDataError("SPOT_INVALID_NUMBER")
    try: v=float(x)
    except (ValueError, TypeError, OverflowError): raise SpotDataError("SPOT_INVALID_NUMBER") from None
    if not isfinite(v) or (v<=0 if positive else v<0):
        raise SpotDataError("SPOT_INVALID_NUMBER")
    return v

def validate_spot_series(payload, symbol, now, min_days=253):
    if now.tzinfo is None or symbol not in COINGECKO_IDS:
        raise SpotDataError("SPOT_INVALID_CONTEXT")
    if not isinstance(payload, dict):
        raise SpotDataError("SPOT_INVALID_PAYLOAD")
    def parse(name, positive):
        data=payload.get(name)
        if not isinstance(data,list) or not data:
            raise SpotDataError("SPOT_SERIES_MISSING")
        result={}; previous=-1
        for row in data:
            if not isinstance(row,list) or len(row)!=2 or isinstance(row[0],bool):
                raise SpotDataError("SPOT_ROW_INVALID")
            try: ts=int(row[0])
            except (TypeError,ValueError): raise SpotDataError("SPOT_TIMESTAMP_INVALID") from None
            if ts<=previous or ts<1500000000000 or ts>4102444800000:
                raise SpotDataError("SPOT_TIMESTAMP_INVALID")
            previous=ts
            date=datetime.fromtimestamp(ts/1000,tz=timezone.utc).date()
            if date>now.astimezone(timezone.utc).date():
                raise SpotDataError("SPOT_FUTURE_DATE")
            if date==now.astimezone(timezone.utc).date():
                continue
            result[date]=_num(row[1],positive=positive)
        return result
    prices=parse("prices",True); volumes=parse("total_volumes",False)
    if set(prices)!=set(volumes):
        raise SpotDataError("SPOT_DATE_MISMATCH")
    days=sorted(prices)
    if len(days)<min_days:
        raise SpotDataError("SPOT_INSUFFICIENT_HISTORY")
    if (now.astimezone(timezone.utc).date()-days[-1]).days>2:
        raise SpotDataError("SPOT_STALE")
    if any((b-a).days!=1 for a,b in zip(days,days[1:])):
        raise SpotDataError("SPOT_DAILY_GAP")
    return {"symbol":symbol,"coin_id":COINGECKO_IDS[symbol],
        "source":"COINGECKO_MARKET_CHART_DAILY_PRICE_SAMPLE",
        "history_kind":"INDEPENDENT_SPOT_PRICE_SAMPLES_NOT_OHLCV",
        "last_complete_day":days[-1].isoformat(),
        "daily_observations":len(days),
        "rows":[{"trading_date":d.isoformat(),"spot_price":prices[d],
                 "total_volume_usd":volumes[d]} for d in days]}

def raw_spot_indicators(history):
    rows=history["rows"]
    if len(rows)<253: raise SpotDataError("SPOT_INSUFFICIENT_HISTORY")
    p=[_num(x["spot_price"]) for x in rows]
    v=[_num(x["total_volume_usd"],positive=False) for x in rows]
    result={"last_price":p[-1]}
    for name,n in (("return_1d",1),("return_5d",5),("return_1m",21),
                   ("return_3m",63),("return_6m",126),("return_12m",252)):
        result[name]=p[-1]/p[-1-n]-1
    for n in (20,60):
        returns=[p[i]/p[i-1]-1 for i in range(len(p)-n,len(p))]
        result[f"volatility_{n}d"]=stdev(returns)*sqrt(365)
    for n in (20,50,200):
        result[f"distance_{n}dma"]=p[-1]/(sum(p[-n:])/n)-1
    result["volume_20d_usd"]=sum(v[-20:])/20
    result["volume_ratio_20d"]=(sum(v[-20:])/sum(v[-40:-20])
                                 if sum(v[-40:-20])>0 else None)
    result["momentum_raw_63d"]=result["return_3m"]
    result["trend_raw_above_200dma"]=result["distance_200dma"]>0
    return result

def readiness_preview(history, indicators):
    return {"version":"V6.5.1","symbol":history["symbol"],
        "spot_history_verified":True,"source":history["source"],
        "sample_count":history["daily_observations"],
        "raw_indicators":indicators,
        "missing_canonical_fields":["momentum_score","volume_score","trend_score",
                                    "funding_rate","open_interest"],
        "mandatory_broad_pass":False,"ranking_permitted":False,
        "execution_permitted":False,"sheet_writes_performed":False}
