"""V6.5.1 FMP EOD technical transformations, no macro/fundamental invention.

Only completed US daily candles from approved provider may feed momentum,
trend, volume and return/volatility indicators. A quote is NOT a daily close.
"""
from __future__ import annotations

from datetime import datetime,timezone
from math import isfinite

from stage0_canonical_factors import canonical_technicals,volume_score
from stage0_equity_session_gate import partition_confirmed_bars

TECHNICAL_FIELDS=(
    "return_1d","return_5d","return_1m","return_3m","return_6m",
    "return_12m","volatility_20d","volatility_60d","drawdown_52w",
    "distance_20dma","distance_50dma","distance_200dma",
    "momentum_score","trend_score","volume_score",
)


def verified_completed_candles(raw,observed_at:datetime):
    if observed_at.tzinfo is None:
        raise ValueError("NAIVE_OBSERVED_AT")
    if not isinstance(raw,list):
        return [],[]
    accepted,dropped=partition_confirmed_bars(raw,observed_at)
    unique={}
    for bar in accepted:
        if not isinstance(bar,dict):
            return [],dropped
        day=str(bar.get("date",""))[:10]
        if day in unique:
            # Two different records for one trading date are ambiguous.
            return [],dropped
        try:
            p=[float(bar[k]) for k in ("open","high","low","close")]
            v=float(bar["volume"])
        except (ValueError,TypeError,KeyError):
            return [],dropped
        if (not all(isfinite(x) and x>0 for x in p)
            or not isfinite(v) or v<0 or p[1]<max(p[0],p[2],p[3])
            or p[2]>min(p[0],p[1],p[3])):
            return [],dropped
        unique[day]=bar
    return [unique[k] for k in sorted(unique)],dropped


def closed_session_factors(raw, observed_at):
    candles,dropped=verified_completed_candles(raw,observed_at)
    out={
        "values":{},
        "completed_candles":candles,
        "in_session_rejected":len(dropped),
        "closed_session":candles[-1]["date"] if candles else None,
        "technical_status":"INSUFFICIENT_COMPLETED_EOD",
    }
    if len(candles)<253:
        return out
    try:
        canon=canonical_technicals([c["close"] for c in candles])
    except (ValueError,TypeError,OverflowError):
        return out
    # Daily volume score uses LAST COMPLETED EOD volume vs preceding 20
    # completed sessions. Never substitute current partial-session quote volume.
    previous=[float(x["volume"]) for x in candles[-21:-1]]
    avg=sum(previous)/len(previous) if len(previous)==20 else 0
    volume=volume_score(candles[-1]["volume"],avg,source_verified=True)
    if volume is None:
        return out
    values=dict(canon)
    values["volume_score"]=volume
    closes=[float(c["close"]) for c in candles]
    peak=closes[0];drawdown=0.
    for close in closes:
        peak=max(peak,close)
        drawdown=min(drawdown,100*(close/peak-1))
    values["drawdown_52w"]=drawdown
    out["values"]=values
    out["technical_status"]="CALCULATED_FROM_FMP_COMPLETED_EOD_NOT_SCORED"
    return out
