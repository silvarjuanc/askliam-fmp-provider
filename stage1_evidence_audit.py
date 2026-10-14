"""V6.5.1 evidence bridge: audited Stage0 -> Stage1 candidates, strictly read-only.

It does not calculate non-canonical score weights or create Ranking, Lifecycle,
Run_Manifest, AI Trades, orders, or portfolio state. Only candidate eligibility
is certified when BOTH the workbook snapshot and Redis checkpoint agree.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timezone
from math import isfinite
from typing import Mapping, Sequence

from stage0_model_readiness import ReadinessError, validate_mandatory
from stage0_canonical_factors import relative_strength

VERSION = "V6.5.1"
COINBASE = "COINBASE_EXCHANGE_SPOT_USD"
RUN_PATTERN = re.compile(r"^STAGE0-(\d{8})-V6\.5\.1$")
QUOTE_PATTERN = re.compile(
    r"Spot last_price from Coinbase ticker observed (\S+)"
)
REQUIRED_SCORE_FACTORS = (
    "factor_1", "factor_2", "factor_3", "catalyst", "momentum", "macro_fit",
)


class Stage1EvidenceError(ValueError):
    pass


def parse_run_id(run_id: str) -> date:
    match = RUN_PATTERN.fullmatch(str(run_id))
    if not match:
        raise Stage1EvidenceError("INVALID_RUN_ID")
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        raise Stage1EvidenceError("INVALID_RUN_DATE") from None


def parse_utc(raw: object) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        value = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return value.astimezone(timezone.utc) if value.tzinfo is not None else None


def fresh_ticker_status(snapshot: Mapping, now: datetime) -> dict:
    match = QUOTE_PATTERN.search(str(snapshot.get("notes") or ""))
    value = parse_utc(match.group(1).rstrip(".") if match else None)
    if value is None or now.tzinfo is None:
        return {"fresh": False, "quote_timestamp_utc": None, "age_seconds": None}
    age = (now.astimezone(timezone.utc) - value).total_seconds()
    return {
        "fresh": 0 <= age <= 900,
        "quote_timestamp_utc": value.isoformat(),
        "age_seconds": int(age),
    }


def audit_stage1_candidates(
    *, universe: Sequence[Mapping], snapshots: Sequence[Mapping],
    requirements: Sequence[Mapping], run_id: str,
    spot_checkpoint: Mapping | None, pending_reconciliation: bool,
    now: datetime,
) -> dict:
    day = parse_run_id(run_id)
    if now.tzinfo is None or day > now.astimezone(timezone.utc).date():
        raise Stage1EvidenceError("INVALID_AUDIT_CLOCK")
    date_utc = day.isoformat()
    models = {}
    for model in requirements:
        key = str(model.get("Model") or "").strip()
        if not key or key in models:
            raise Stage1EvidenceError("DUPLICATE_OR_MISSING_MODEL")
        models[key] = model

    eligible = {}
    for entry in universe:
        symbol = str(entry.get("Symbol") or "").strip().upper()
        if not symbol:
            continue
        if str(entry.get("Analysis Enabled") or "").strip().upper() != "TRUE":
            continue
        if str(entry.get("Ranking Enabled") or "").strip().upper() != "TRUE":
            continue
        if symbol in eligible:
            raise Stage1EvidenceError("DUPLICATE_ELIGIBLE_SYMBOL")
        eligible[symbol] = entry

    current = {}
    for row in snapshots:
        if str(row.get("ranking_run_id") or "").strip() != run_id:
            continue
        symbol = str(row.get("canonical_symbol") or "").strip().upper()
        if not symbol or symbol in current:
            raise Stage1EvidenceError("DUPLICATE_OR_INVALID_CURRENT_SNAPSHOT")
        current[symbol] = row

    if spot_checkpoint is None:
        completed_spot = set()
    else:
        symbols = spot_checkpoint.get("completed_symbols")
        if (
            spot_checkpoint.get("version") != VERSION
            or spot_checkpoint.get("utc_date") != date_utc
            or spot_checkpoint.get("provider") != COINBASE
            or spot_checkpoint.get("status") != "COMMITTED_VERIFY_PASS"
            or not isinstance(symbols, list)
            or len(set(symbols)) != len(symbols)
            or any(s not in ("BTC", "ETH") for s in symbols)
        ):
            raise Stage1EvidenceError("UNTRUSTED_SPOT_CHECKPOINT")
        completed_spot = set(symbols)

    # No fabricated negative "DATA_WAIT": non-acquired universe assets may
    # still be pending a permitted source, authentication or quota.
    btc_row = current.get("BTC")
    crypto_results = []
    for symbol in ("BTC", "ETH"):
        if symbol not in eligible:
            continue
        row = current.get(symbol)
        if not row:
            continue
        ready = {
            "symbol": symbol,
            "run_id": run_id,
            "model": "CRYPTO_V1",
            "score_status": "NOT_SCORED",
            "global_score": None,
            "ranking_publication_permitted": False,
            "execution_permitted": False,
        }
        ts = parse_utc(row.get("data_timestamp"))
        from_correct_day = ts is not None and ts.date() == day and ts <= now.astimezone(timezone.utc)
        provenance = (
            str(row.get("analysis_price_source") or "") == COINBASE
            and str(row.get("score_model") or "") == "CRYPTO_V1"
            and str(row.get("asset_class") or "") == "Crypto"
            and "https://api.exchange.coinbase.com/products/" + symbol + "-USD/candles"
            in str(row.get("source_summary") or "")
            and from_correct_day
        )
        checkpoint_verified = symbol in completed_spot and pending_reconciliation is False
        if not provenance or not checkpoint_verified:
            ready.update({
                "ingestion_verified": False, "mandatory_pass": False,
                "missing_mandatory_fields": ["PROVENANCE_OR_CHECKPOINT_UNVERIFIED"],
                "score_blockers": ["SOURCE_OR_REDIS_CHECKPOINT_NOT_VERIFIED"],
            })
            crypto_results.append(ready)
            continue
        spec = models.get("CRYPTO_V1")
        if spec is None:
            raise Stage1EvidenceError("CRYPTO_REQUIREMENTS_UNAVAILABLE")
        try:
            coverage = validate_mandatory(row, spec)
        except ReadinessError:
            raise Stage1EvidenceError("CRYPTO_REQUIREMENTS_INVALID") from None
        complete = (
            coverage["mandatory_input_complete"] is True
            and str(row.get("mandatory_broad_pass")) == "TRUE"
            and str(row.get("enrichment_status")) == "ACQUIRED_NOT_SCORED"
        )
        fresh = fresh_ticker_status(row, now)
        ready.update({
            "ingestion_verified": True,
            "source": COINBASE,
            "data_timestamp_utc": ts.isoformat(),
            "checkpoint_verified": True,
            "mandatory_pass": bool(complete),
            "mandatory_fields_present": coverage["present"],
            "mandatory_fields_required": coverage["total"],
            "missing_mandatory_fields": coverage["missing"],
            "broad_inputs_ready": bool(complete),
            "spot_quote_fresh_for_live_signals": fresh["fresh"],
            "spot_quote_timestamp_utc": fresh["quote_timestamp_utc"],
            "spot_quote_age_seconds": fresh["age_seconds"],
            "score_blockers": (
                ["FACTOR_POINT_EVIDENCE_NOT_PROVIDED", "RISK_PENALTY_NOT_VALIDATED"]
                if complete else ["CRYPTO_MANDATORY_OR_STATUS_INCOMPLETE"]
            ),
        })
        if symbol == "ETH" and btc_row and complete:
            try:
                benchmark = float(btc_row["return_3m"])
                asset = float(row["return_3m"])
                if (
                    isfinite(benchmark) and isfinite(asset)
                    and str(btc_row.get("analysis_price_source")) == COINBASE
                    and "BTC" in completed_spot
                    and parse_utc(btc_row.get("data_timestamp")) is not None
                ):
                    ready["relative_strength_vs_BTC_preview"] = round(
                        relative_strength(asset, benchmark), 8
                    )
                    ready["relative_strength_is_a_score_factor"] = False
            except (KeyError, ValueError, TypeError, OverflowError):
                pass
        crypto_results.append(ready)

    complete_assets = [r["symbol"] for r in crypto_results if r.get("broad_inputs_ready")]
    confirmed_ingested = [r["symbol"] for r in crypto_results if r.get("ingestion_verified")]
    not_acquired = len(set(eligible) - set(current))
    return {
        "version": VERSION,
        "status": "STAGE1_EVIDENCE_ONLY_NOT_RANKED",
        "run_id": run_id,
        "as_of_utc": now.astimezone(timezone.utc).isoformat(),
        "eligible_universe_count": len(eligible),
        "current_run_snapshot_count": len(current),
        "current_run_eligible_snapshot_count": len(set(eligible) & set(current)),
        "current_run_without_snapshot_count": not_acquired,
        "current_run_without_snapshot_status": "PROVIDER_DEFERRED_OR_NOT_YET_ATTEMPTED",
        "spot_verified_assets": confirmed_ingested,
        "crypto_mandatory_ready_assets": complete_assets,
        "candidate_details": crypto_results,
        "factor_point_normalization": "NOT_EXECUTED_NO_AUDITED_FACTOR_EVIDENCE",
        "risk_penalty": "NOT_ESTIMATED",
        "scored_assets": 0,
        "top30_published": False,
        "ranked": False,
        "sheet_writes_performed": False,
        "run_manifest_written": False,
        "portfolio_mutated": False,
        "execution_permitted": False,
        "note": (
            "Historical Stage0 readback and Redis source checkpoint prove acquired "
            "data, not mapped 20/20/20/15/15/10 factor scores or risk penalty. "
            "A fresh spot ticker is separately needed for a live signal."
        ),
    }
