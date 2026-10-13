"""AskLiam V6.5.1 Stage1 strict, read-only analytical-sufficiency gate.

The output is never an actual committed ranking.  It independently compares
the real workbook's Data_Requirements, Universe, Market_Data_Snapshot and
Ranking_Staging.  No source or synthetic score can be silently imputed.

SCORED requires an explicitly documented risk penalty and documented factors
from a current valid analytical source. Execution availability NEVER ranks.
"""
from __future__ import annotations

from datetime import datetime, timezone
import math
import re

from stage0_crypto_spot import PRICE_SOURCE

VERSION = "V6.5.1"
FACTOR_LIMITS = {
    "Factor 1 /20": 20,
    "Factor 2 /20": 20,
    "Factor 3 /20": 20,
    "Catalyst /15": 15,
    "Momentum /15": 15,
    "Macro Fit /10": 10,
}
RISK_LIMIT = 20
MODEL_TO_CLASS = {
    "EQUITY_V2": {"Equity", "Equity-like ETF"},
    "INDEX_V1": {"Index / ETF / Basket", "Index", "ETF"},
    "CRYPTO_V1": {"Crypto"},
    "FX_V1": {"FX", "FX / Macro"},
    "COMMODITY_V1": {"Commodity"},
    "RATES_V1": {"Rates / Fixed Income"},
    "ALT_MARKET_V1": {"Pre-IPO / Private / Synthetic / Alternative"},
}


def _valid_number(value):
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def parse_required(expression: str) -> list[str]:
    if not isinstance(expression, str):
        raise ValueError("INVALID_REQUIREMENT")
    fields = [x.strip() for x in expression.split(";") if x.strip()]
    if not fields or len(set(fields)) != len(fields):
        raise ValueError("INVALID_OR_DUPLICATE_REQUIREMENT")
    return fields


def field_present(row: dict, field: str) -> bool:
    """Support explicit alternates and numeric comparator only; never guess."""
    if not isinstance(row, dict):
        return False
    if ">=" in field:
        name, limit = (x.strip() for x in field.split(">=", 1))
        threshold = _valid_number(limit)
        return (
            threshold is not None
            and _valid_number(row.get(name)) is not None
            and _valid_number(row.get(name)) >= threshold
        )
    if field == "breadth_score_or_proxy":
        return any(_valid_number(row.get(name)) is not None
                   for name in ("breadth_score","breadth_score_or_proxy"))
    if field == "supply_demand_score_or_proxy":
        return any(_valid_number(row.get(name)) is not None
                   for name in ("supply_demand_score","supply_demand_score_or_proxy"))
    if field == "yield_or_reference_price":
        return any(_valid_number(row.get(name)) is not None
                   for name in ("yield_or_reference_price","last_price"))
    if field == "return_1m_or_yield_change":
        return any(_valid_number(row.get(name)) is not None
                   for name in ("return_1m_or_yield_change","return_1m","yield_change_1m"))
    return _valid_number(row.get(field)) is not None


def _parse_clock(raw):
    if not isinstance(raw, str) or not raw:
        return None
    try:
        t = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return t.astimezone(timezone.utc) if t.tzinfo else None


def audit_asset(model_row, snapshot, *, expected_run_id, now):
    if now.tzinfo is None:
        raise ValueError("NAIVE_NOW")
    model = model_row["Model"]
    threshold = _valid_number(model_row["Broad Min %"])
    if threshold is None or not 0 < threshold <= 100:
        raise ValueError("INVALID_MODEL_THRESHOLD")
    mandatory = parse_required(model_row["Broad Mandatory Data"])
    if not snapshot or snapshot.get("ranking_run_id") != expected_run_id:
        return {
            "model": model,
            "status": "PROVIDER_DEFERRED",
            "completeness_pct": 0.,
            "missing_fields": mandatory,
            "reason": "NO_CURRENT_RUN_SNAPSHOT",
        }
    if snapshot.get("score_model") != model:
        return {
            "model": model, "status": "HALTED",
            "completeness_pct": 0., "missing_fields": mandatory,
            "reason": "SCORE_MODEL_MISMATCH",
        }
    # Provider/access/quota faults are not genuine analytical DATA_WAIT.
    if "DEFERRED" in str(snapshot.get("enrichment_status", "")).upper():
        return {
            "model": model,
            "status": "PROVIDER_DEFERRED",
            "completeness_pct": 0.,
            "missing_fields": mandatory,
            "reason": "PROVIDER_RUNTIME_OR_ENTITLEMENT_UNAVAILABLE",
        }
    if model == "CRYPTO_V1" and snapshot.get("analysis_price_source") != PRICE_SOURCE:
        return {
            "model": model,
            "status": "PROVIDER_DEFERRED",
            "completeness_pct": 0.,
            "missing_fields": mandatory,
            "reason": "INDEPENDENT_SPOT_SOURCE_MISSING",
        }
    missing = [field for field in mandatory if not field_present(snapshot,field)]
    coverage = round(100*(len(mandatory)-len(missing))/len(mandatory),2)
    freshness = True
    if "last_price" in mandatory:
        timestamp = _parse_clock(snapshot.get("data_timestamp"))
        if timestamp is None:
            freshness = False
        elif model == "CRYPTO_V1":
            age = (now.astimezone(timezone.utc)-timestamp).total_seconds()
            freshness = -60 <= age <= 900
        else:
            # The latest settled official session may be >15 min old; no
            # generic asset-class freshness is inferred from one timestamp.
            freshness = timestamp <= now.astimezone(timezone.utc)
    declared_ready = (
        str(snapshot.get("mandatory_broad_pass","")).upper() == "TRUE"
        and snapshot.get("broad_readiness") == "BROAD_SCAN_READY"
    )
    if not freshness:
        return {
            "model": model, "status": "PROVIDER_DEFERRED",
            "completeness_pct": coverage, "missing_fields": missing,
            "reason": "ANALYTICAL_PRICE_STALE_OR_UNVERIFIED",
        }
    if missing or coverage < threshold or not declared_ready:
        return {
            "model": model,
            "status": "DATA_WAIT" if not missing and declared_ready else "STAGE0_PARTIAL",
            "completeness_pct": coverage, "missing_fields": missing,
            "reason": "MANDATORY_OR_PROVIDER_VALIDATION_INCOMPLETE",
        }
    return {
        "model": model, "status": "BROAD_SCAN_READY",
        "completeness_pct": coverage, "missing_fields": [], "reason": "PASS",
    }


def compute_score_from_evidence(row: dict):
    """Strict 3x20 + optional catalyst/momentum/macro weights, - risk/20.

    No unverified zero penalty, fabricated fundamentals or trading proxy.
    At least three mandatory factor-20 values required. Risk is required.
    Missing optional components renormalize *available* score weights.
    The source summary must be non-empty.
    """
    if not isinstance(row, dict):
        return {"status": "UNSCORED", "reason": "NO_FACTOR_EVIDENCE"}
    if not str(row.get("Analysis Source Summary","")).strip():
        return {"status": "UNSCORED", "reason": "FACTOR_PROVENANCE_MISSING"}
    values = {}
    for name, limit in FACTOR_LIMITS.items():
        v = _valid_number(row.get(name))
        if v is None:
            if name in ("Factor 1 /20", "Factor 2 /20", "Factor 3 /20"):
                return {"status": "UNSCORED", "reason": "PRIMARY_FACTOR_MISSING"}
            continue
        if not 0 <= v <= limit:
            return {"status": "UNSCORED", "reason": "INVALID_FACTOR_RANGE"}
        values[name] = v
    risk = _valid_number(row.get("Risk Penalty"))
    if risk is None or not 0 <= risk <= RISK_LIMIT:
        return {"status": "UNSCORED", "reason": "VERIFIED_RISK_PENALTY_MISSING"}
    available_weight = sum(FACTOR_LIMITS[k] for k in values)
    if available_weight < 60:
        return {"status": "UNSCORED", "reason": "INSUFFICIENT_AVAILABLE_FACTOR_WEIGHT"}
    before_risk = sum(values.values()) / available_weight * 100
    score = max(0., min(100., before_risk-risk))
    return {
        "status": "SCORE_CANDIDATE_NOT_COMMITTED",
        "score": round(score,4),
        "available_weight_pct": available_weight,
        "renormalized": available_weight < 100,
        "risk_penalty":risk,
    }


def evaluate_readiness(
    *, universe, requirements, snapshots, ranking_staging, run_id, now,
):
    """Pure, read-only proposal. Returns at most 30 score-ready candidates."""
    if now.tzinfo is None:
        raise ValueError("NAIVE_NOW")
    models = {str(r.get("Model","")).strip():r for r in requirements}
    if not models or len(models)!=len(requirements):
        raise ValueError("REQUIREMENT_SCHEMA_OR_DUPLICATE")
    symbols={}
    for row in universe:
        symbol=str(row.get("Symbol","")).strip().upper()
        if not symbol:
            continue
        if str(row.get("Analysis Enabled","")).upper() == "TRUE" and (
            str(row.get("Ranking Enabled","")).upper() == "TRUE"
        ):
            if symbol in symbols:
                raise ValueError("DUPLICATE_UNIVERSE_SYMBOL")
            symbols[symbol]=row
    snap={}
    for row in snapshots:
        symbol=str(row.get("canonical_symbol","")).strip().upper()
        if row.get("ranking_run_id") != run_id or symbol not in symbols:
            continue
        if symbol in snap:
            raise ValueError("DUPLICATE_CURRENT_SNAPSHOT")
        snap[symbol]=row
    factors={}
    for row in ranking_staging:
        symbol=str(row.get("Symbol","")).strip().upper()
        if symbol in symbols and symbol not in factors:
            factors[symbol]=row
    results={}
    scored=[]
    for symbol,row in sorted(symbols.items()):
        # Fail closed on ambiguous score models rather than guess from asset class.
        model = str(snap.get(symbol,{}).get("score_model") or
                    factors.get(symbol,{}).get("Score Model") or "")
        if model not in models:
            results[symbol]={"status":"PROVIDER_DEFERRED",
                             "reason":"CURRENT_MODEL_OR_SOURCE_UNKNOWN"}
            continue
        audited=audit_asset(models[model],snap.get(symbol),expected_run_id=run_id,now=now)
        results[symbol]=audited
        if audited["status"]!="BROAD_SCAN_READY":
            continue
        src=factors.get(symbol,{})
        if src.get("Run ID") != run_id or src.get("Score Model") != model:
            audited["scoring"]={"status":"UNSCORED","reason":"FACTOR_RUN_ID_NOT_VERIFIED"}
            continue
        candidate=compute_score_from_evidence(src)
        audited["scoring"]=candidate
        if candidate["status"]=="SCORE_CANDIDATE_NOT_COMMITTED":
            scored.append({"symbol":symbol,"model":model,"score":candidate["score"],
                           "factor_availability_pct":candidate["available_weight_pct"]})
    scored.sort(key=lambda x:(-x["score"],x["symbol"]))
    groups={}
    for item in results.values():
        groups[item["status"]]=groups.get(item["status"],0)+1
    return {
        "version": VERSION, "run_id": run_id,
        "eligible_universe":len(symbols),
        "current_run_snapshots":len(snap),
        "by_status":groups,
        "score_candidates_count":len(scored),
        "top_30_preview_only":scored[:30],
        "scored_committed":False,
        "ranking_committed":False,
        "deep_dive_completed":False,
        "lifecycle_updates":0,
        "execution_permitted":False,
        "ranking_permitted":False,
        "reader_only":True,
        "per_asset":results,
    }


def stage1_preview_sync(run_id: str):
    """Read four separate live workbook tabs; performs no writes."""
    if not re.fullmatch(r"STAGE0-\d{8}-V6\.5\.1",run_id):
        raise ValueError("INVALID_STAGE0_RUN_ID")
    from stage0_ingestor import Stage0_Daily_Ingestor
    reader=Stage0_Daily_Ingestor(providers={"read_only":lambda:{}})
    book=reader._open()
    def read(name):
        return book.worksheet(name).get_all_records(numericise_ignore=["all"])
    return evaluate_readiness(
        universe=read("Universe"),
        requirements=read("Data_Requirements"),
        snapshots=read("Market_Data_Snapshot"),
        ranking_staging=read("Ranking_Staging"),
        run_id=run_id,now=datetime.now(timezone.utc),
    )
