"""Model-grounded financial ratio evidence. NEVER a synthetic score.

Source: FMP /stable/ratios-ttm. One earned fundamental factor per
independent category with a real finite ratio and source provenance.
No price or corporate facts are invented; missing reports remain blank.
"""
from __future__ import annotations
import math
import json

FIELDS={
    "profitability":"netProfitMarginTTM",
    "valuation":"priceToEarningsRatioTTM",
    "cashflow":"freeCashFlowOperatingCashFlowRatioTTM",
    "balance_sheet":"debtToEquityRatioTTM",
}
URL_ROOT="https://financialmodelingprep.com/stable/ratios-ttm?symbol="


def extract_ttm(symbol, upstream, fetched_utc):
    if not isinstance(upstream, (dict,list)):
        return {"fundamental_factor_count":None,"fundamental_ratios":{},
                "status":"FMP_TTM_MISSING"}
    records=upstream if isinstance(upstream,list) else [upstream]
    matches=[x for x in records if isinstance(x,dict)
             and str(x.get("symbol","")).upper()==symbol]
    if len(matches)!=1:
        return {"fundamental_factor_count":None,"fundamental_ratios":{},
                "status":"FMP_TTM_SYMBOL_UNVERIFIED"}
    rec=matches[0]
    evidence={}
    for bucket,key in FIELDS.items():
        raw=rec.get(key)
        if isinstance(raw,bool) or raw is None:
            continue
        try:
            val=float(raw)
        except (ValueError,TypeError,OverflowError):
            continue
        if not math.isfinite(val) or (bucket=="valuation" and val<=0):
            continue
        evidence[bucket]={"metric":key,"value":val}
    return {
        "fundamental_factor_count":len(evidence) if evidence else None,
        "fundamental_ratios":evidence,
        "status":"FMP_TTM_OBSERVED_UNVERIFIED_FILINGS" if evidence else "FMP_TTM_NO_USABLE_FACTORS",
        "source_url":URL_ROOT+symbol,
        "fetched_utc":fetched_utc,
    }


def serialized_evidence(evidence):
    if not evidence.get("fundamental_factor_count"):
        return None
    return json.dumps({
        "source":"FMP_RATIOS_TTM",
        "source_url":evidence["source_url"],
        "fetched_utc":evidence["fetched_utc"],
        "quality_status":evidence["status"],
        "ratios":evidence["fundamental_ratios"],
    },sort_keys=True,separators=(",",":"),allow_nan=False)
