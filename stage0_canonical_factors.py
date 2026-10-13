"""Canonical V6.5.1 factors from the Google Drive Master Prompt.

Return/volatility/distance stored as percentage points. No score is inferred
from unreliable or unverified volume observations; no macro scores invented.
"""
from math import isfinite, log, sqrt, tanh
from statistics import stdev

def _clamp(n):
    return max(0.0,min(100.0,n))

def canonical_technicals(closes, *, sessions_per_year=252):
    p=[float(x) for x in closes]
    if len(p)<253 or any(not isfinite(x) or x<=0 for x in p):
        raise ValueError("CANONICAL_HISTORY_INCOMPLETE")
    r={}
    for label,n in (("return_1d",1),("return_5d",5),
                    ("return_1m",21),("return_3m",63),
                    ("return_6m",126),("return_12m",252)):
        r[label]=(p[-1]/p[-1-n]-1)*100
    for n in (20,60):
        logs=[log(p[i]/p[i-1]) for i in range(len(p)-n,len(p))]
        r[f"volatility_{n}d"]=stdev(logs)*sqrt(sessions_per_year)*100
    for n in (20,50,200):
        avg=sum(p[-n:])/n
        r[f"distance_{n}dma"]=(p[-1]/avg-1)*100
    raw=.45*r["return_1m"]+.35*r["return_3m"]+.20*r["return_6m"]
    r["momentum_score"]=_clamp(50+50*tanh(raw/15))
    trend=(r["distance_50dma"]+r["distance_200dma"])/2
    r["trend_score"]=_clamp(50+50*tanh(trend/12))
    return r

def relative_strength(asset_return_3m,benchmark_return_3m):
    if not all(isfinite(float(v)) for v in (asset_return_3m,benchmark_return_3m)):
        raise ValueError("BENCHMARK_NOT_VERIFIED")
    return _clamp(50+50*tanh((float(asset_return_3m)-float(benchmark_return_3m))/15))

def volume_score(current_volume,previous_20_average,*,source_verified):
    if source_verified is not True: return None
    try:
        current=float(current_volume)
        past=float(previous_20_average)
    except (ValueError,TypeError): return None
    if not isfinite(current) or not isfinite(past) or past<=0 or current<0:
        return None
    return _clamp(50+35*(current/past-1))

def normalize_score(factors,risk_penalty,*,mandatory_complete):
    """Only normalize actual supplied per-factor earned point values."""
    if mandatory_complete is not True: return None
    maxima={"factor_1":20,"factor_2":20,"factor_3":20,
            "catalyst":15,"momentum":15,"macro_fit":10}
    earned=0.0;available=0.0
    for key,maximum in maxima.items():
        value=factors.get(key)
        if value is None: continue
        value=float(value)
        if not isfinite(value) or not 0<=value<=maximum:
            raise ValueError("SCORE_FACTOR_INVALID")
        earned+=value
        available+=maximum
    risk=float(risk_penalty)
    if not isfinite(risk) or not 0<=risk<=20 or available<=0:
        raise ValueError("RISK_OR_WEIGHT_INVALID")
    return _clamp(earned/(available/100)-risk)
