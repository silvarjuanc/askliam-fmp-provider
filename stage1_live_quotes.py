"""Read-only V6.5.1 live quote adapter (independent underlying pricing).

Separates the last CLOSED daily candle from the CURRENT ticker, and keeps
spot Coinbase independent from Hyperliquid perpetual execution venue.
Never writes Stage0/Ranking/Lifecycle/AI Trades and never sends orders.
"""
from __future__ import annotations
import asyncio
from datetime import datetime,timezone

from stage1_history_reuse import numeric,parse_utc,QUOTE_TTL_SECONDS

VERSION="V6.5.1"
COINBASE_TICKERS={"BTC":"BTC-USD","ETH":"ETH-USD"}
COINBASE_BASE="https://api.exchange.coinbase.com/products/"


def fmp_source_time(value):
    """FMP stable /quote timestamp is Unix seconds, NOT fetch-time ISO.

    Some FMP endpoints emit milliseconds. Neither missing nor invalid
    source timestamps may inherit a fresh HTTP retrieval timestamp.
    """
    from math import isfinite
    if isinstance(value,bool) or value is None:
        return None
    try:
        n=float(value)
        if not isfinite(n) or n<=0:
            return None
        if n>1e11:
            n/=1000
        observed=datetime.fromtimestamp(n,tz=timezone.utc)
        if observed.year<2000:
            return None
        return observed.isoformat()
    except (ValueError,TypeError,OverflowError,OSError):
        return None



def accept_quote(symbol,source,price,observed_at,now,source_url):
    p=numeric(price)
    ts=parse_utc(observed_at)
    age=(now-ts).total_seconds() if ts else None
    verified=(p is not None and p>0 and age is not None
              and 0<=age<=QUOTE_TTL_SECONDS)
    return {
        "symbol":symbol,"source":source,"source_url":source_url,
        "price":p if verified else None,
        "observed_at_utc":ts.isoformat() if ts else None,
        "quote_age_seconds":round(age,1) if age is not None else None,
        "quote_fresh_15m":bool(verified),
        "status":"LIVE_QUOTE_VERIFIED" if verified else "LIVE_QUOTE_UNVERIFIED",
        "execution_permitted":False,
    }


async def acquire_live_quotes(symbols, *,client,fmp_get,now=None):
    now=now or datetime.now(timezone.utc)
    if now.tzinfo is None or not 1<=len(symbols)<=10 or len(set(symbols))!=len(symbols):
        raise ValueError("INVALID_QUOTE_REQUEST")
    async def one(symbol):
        if symbol in COINBASE_TICKERS:
            url=COINBASE_BASE+COINBASE_TICKERS[symbol]+"/ticker"
            try:
                response=await client.get(url,timeout=12)
                response.raise_for_status()
                body=response.json()
                if not isinstance(body,dict):
                    raise ValueError("INVALID_COINBASE_TICKER")
                return accept_quote(symbol,"COINBASE_EXCHANGE_SPOT_USD",
                                    body.get("price"),body.get("time"),now,url)
            except Exception:
                return {"symbol":symbol,"status":"COINBASE_SPOT_UNAVAILABLE",
                        "quote_fresh_15m":False,"execution_permitted":False}
        try:
            body=await fmp_get("quote",symbol)
            if body.get("status")!="READY":
                return {"symbol":symbol,"status":"FMP_QUOTE_DEFERRED",
                        "provider_reason":body.get("error","UNKNOWN"),
                        "quote_fresh_15m":False,"execution_permitted":False}
            d=body.get("data")
            if isinstance(d,list):
                rows=[x for x in d if isinstance(x,dict)
                      and str(x.get("symbol","")).upper()==symbol]
                d=rows[0] if len(rows)==1 else {}
            if not isinstance(d,dict) or str(d.get("symbol","")).upper()!=symbol:
                return {"symbol":symbol,"status":"FMP_SYMBOL_MISMATCH",
                        "quote_fresh_15m":False,"execution_permitted":False}
            return accept_quote(symbol,"FMP_STABLE",d.get("price"),
                                fmp_source_time(d.get("timestamp")),now,
                                body.get("source_url_template",
                                         "https://financialmodelingprep.com/stable/quote?symbol="+symbol))
        except Exception:
            return {"symbol":symbol,"status":"FMP_QUOTE_UNAVAILABLE",
                    "quote_fresh_15m":False,"execution_permitted":False}
    return {"version":VERSION,"asof_utc":now.isoformat(),
            "quotes":await asyncio.gather(*(one(s) for s in symbols)),
            "sheet_writes_performed":False,"ranking_permitted":False,
            "execution_permitted":False}


def enrich_readiness(preview_report,quotes):
    if preview_report.get("version")!=VERSION or quotes.get("version")!=VERSION:
        raise ValueError("WRONG_STAGE1_VERSION")
    mapped={q["symbol"]:q for q in quotes["quotes"]}
    out=dict(preview_report)
    enriched=[]
    for original in preview_report["results"]:
        row=dict(original)
        sym=row.get("symbol")
        quote=mapped.get(sym)
        if quote is None:
            raise ValueError("LIVE_QUOTE_SYMBOL_MISSING")
        row["live_quote"]=quote
        if quote.get("quote_fresh_15m") is True:
            row["quote_fresh_15m"]=True
            row["quote_age_seconds"]=quote.get("quote_age_seconds")
        else:
            row["quote_fresh_15m"]=False
        # An independent, current ticker may fix the STALE quote gate.
        # This does not fix missing fundamentals or source-quality failures.
        ready=(row.get("history_status")=="TECHNICAL_FACTORS_RECOMPUTED"
               and row.get("history_fresh") is True
               and row.get("source_quality_status")=="INDEPENDENT_SPOT_VALIDATED"
               and not row.get("mandatory_missing")
               and quote.get("quote_fresh_15m") is True)
        row["live_broad_input_ready"]=bool(ready)
        row["ranking_permitted"]=False
        row["executable"]=False
        enriched.append(row)
    out["results"]=enriched
    out["live_broad_input_ready_count"]=sum(
        x["live_broad_input_ready"] for x in enriched)
    out["live_quote_overlay_mode"]="LIVE_SEPARATE_FROM_CLOSED_HISTORY"
    out["ranking_permitted"]=False
    out["execution_permitted"]=False
    out["sheet_writes_performed"]=False
    return out
