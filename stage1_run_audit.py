"""AskLiam V6.5.1 — source-grounded, read-only daily Stage0 reconciliation.

Counts today's exact run id separately from Ranking eligibility, never labels
an unattempted asset DATA_WAIT, and does not pretend to query live Redis.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone

from stage0_model_readiness import ReadinessError, validate_mandatory
from stage1_history_reuse import parse_utc

VERSION = "V6.5.1"
QUOTE_TTL_SECONDS = 900
FMP_CLASSES = {"Equity", "Index", "Index / ETF / Basket"}


def _enabled(value):
    return str(value).strip().upper() == "TRUE"


def _canonical(row, key):
    return str(row.get(key, "")).strip().upper()


def _strict_run_id(day):
    if isinstance(day, str):
        day = date.fromisoformat(day)
    if not isinstance(day, date):
        raise ValueError("BAD_RUN_DATE")
    return f"STAGE0-{day:%Y%m%d}-{VERSION}"


def audit_daily(universe, snapshots, specs, *, day, now=None):
    """No scores, no live Redis inference, no persistence."""
    run_id = _strict_run_id(day)
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("NAIVE_CLOCK")
    now = now.astimezone(timezone.utc)

    canonical_universe = {}
    for row in universe:
        sym = _canonical(row, "Symbol")
        if not sym:
            continue
        if sym in canonical_universe:
            raise ValueError("DUPLICATE_UNIVERSE_SYMBOL")
        canonical_universe[sym] = row
    eligible = {sym:row for sym,row in canonical_universe.items()
                if _enabled(row.get("Analysis Enabled"))
                and _enabled(row.get("Ranking Enabled"))}

    by_model = {str(row.get("Model", "")).strip():row for row in specs}
    matched = {}
    for row in snapshots:
        if str(row.get("ranking_run_id", "")).strip() != run_id:
            continue
        sym = _canonical(row, "canonical_symbol")
        if not sym or sym in matched:
            raise ValueError("DUPLICATE_OR_EMPTY_TODAY_SNAPSHOT")
        matched[sym] = row

    acquired = sorted(set(matched).intersection(eligible))
    excluded = sorted(set(matched).difference(eligible))
    no_commit = sorted(set(eligible).difference(matched))

    classes = Counter()
    for sym in no_commit:
        route = "FMP" if str(eligible[sym].get("Asset Class","")).strip() in FMP_CLASSES else "OTHER"
        classes[route] += 1

    states = Counter()
    missing_rules = Counter()
    details = []
    for sym in acquired:
        snapshot = matched[sym]
        model = str(snapshot.get("score_model", "")).strip()
        spec = by_model.get(model)
        if not spec:
            mandatory = False
            missing = ["MODEL_REQUIREMENTS_UNAVAILABLE"]
            coverage = None
        else:
            try:
                result = validate_mandatory(snapshot, spec)
                mandatory = bool(result["mandatory_input_complete"])
                missing = list(result["missing"])
                coverage = result["mandatory_coverage_pct"]
            except (ReadinessError, ValueError, TypeError):
                mandatory = False
                missing = ["REQUIREMENT_CONTRACT_INVALID"]
                coverage = None
        for name in missing:
            missing_rules[name] += 1
        observed = parse_utc(snapshot.get("data_timestamp"))
        quote_age = (now-observed).total_seconds() if observed else None
        quote_fresh = quote_age is not None and 0<=quote_age<=QUOTE_TTL_SECONDS
        if not mandatory:
            state = "ACQUIRED_INCOMPLETE"
        elif not quote_fresh:
            state = "MANDATORY_COMPLETE_QUOTE_STALE"
        else:
            state = "MANDATORY_COMPLETE_QUOTE_FRESH"
        states[state] += 1
        details.append({
            "canonical_symbol":sym, "score_model":model,
            "state":state, "mandatory_missing":missing,
            "mandatory_coverage_pct":coverage, "quote_fresh":quote_fresh,
            "quote_observed_at_utc":observed.isoformat() if observed else None,
            "source_quality_verified":False,  # not inferred from worksheet
            "ranking_permitted":False,
        })

    acquired_sources = Counter(
        str(matched[sym].get("analysis_price_source","UNKNOWN")).strip() or "UNKNOWN"
        for sym in matched
    )
    return {
        "version":VERSION, "run_id":run_id,
        "asof_utc":now.isoformat(),
        "total_universe":len(canonical_universe),
        "ranking_eligible":len(eligible),
        "stage0_snapshots":len(matched),
        "stage0_eligible_acquired":len(acquired),
        "stage0_acquired_excluded_ranking":len(excluded),
        "excluded_symbols":excluded,
        "eligible_without_current_snapshot":len(no_commit),
        "missing_snapshot_by_route":dict(classes),
        "mandatory_complete_count":sum(
            states.get(x,0) for x in (
                "MANDATORY_COMPLETE_QUOTE_STALE","MANDATORY_COMPLETE_QUOTE_FRESH")),
        "eligible_quote_fresh_and_mandatory_count":states.get(
            "MANDATORY_COMPLETE_QUOTE_FRESH",0),
        "readiness_counts":dict(states),
        "frequent_missing_mandatory_rules":dict(missing_rules),
        "acquired_sources":dict(acquired_sources),
        "details":details,
        "provider_attempt_proven":False,
        "redis_live_checked":False,
        "redis_pending":"UNVERIFIED",
        "fmp_quota":"UNVERIFIED",
        "full_pipeline_committed":False,
        "ranking_permitted":False,
        "execution_permitted":False,
        "sheet_writes_performed":False,
    }
