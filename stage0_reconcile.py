"""AskLiam V6.5.1 read-only reconciliation evidence for abandoned Stage0 journals.

DO NOT infer that PREPARED alone means no Google Sheets writes.
Never silently clear Redis pending after process interruption.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from stage0_batch import pending_key, state_key, LOCK_SECONDS
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

RECOVERY_GRACE_SECONDS = 60


def inspect_worksheet_footprint(symbols, run_id, *, reader_factory=None):
    """Independent authenticated read-back of all approved Stage0 sheets."""
    if reader_factory is None:
        reader_factory = lambda: Stage0_Daily_Ingestor(providers={"reconcile_readonly": lambda: {}})
    ingestor = reader_factory()
    captured = ingestor._capture(ingestor._open())
    result = {}
    for symbol in symbols:
        raw = [
            key for key in captured["Raw_OHLCV"]["rows"]
            if key[0] == symbol
        ]
        snapshots = [
            key for key in captured["Market_Data_Snapshot"]["rows"]
            if key == (run_id, symbol)
        ]
        result[symbol] = {
            "raw_history_any_run": len(raw),
            "snapshot_matching_pending_run": len(snapshots),
        }
    return result


async def inspect_abandoned_prewrite(redis, *, footprint_reader=None,
                                    now=None):
    """Conservative, side-effect-free feasibility assessment.

    Only qualify a PREPARED journal when its intended symbols have never
    appeared in the historical sheet, do not appear in the current run
    snapshot, the day checkpoint did not advance for them, and the original
    Redis lease must have expired.
    """
    now = now or datetime.now(timezone.utc)
    raw = await redis.call("GET", pending_key(None))
    if not raw:
        return {
            "version": "V6.5.1", "status": "NO_PENDING_JOURNAL",
            "safe_to_abort_prewrite": False,
            "pending": None, "ranking_permitted": False,
        }
    blockers = []
    try:
        journal = json.loads(raw)
    except (ValueError, TypeError):
        return {
            "version": "V6.5.1", "status": "RECONCILIATION_BLOCKED",
            "safe_to_abort_prewrite": False,
            "blockers": ["CORRUPT_JOURNAL"],
            "ranking_permitted": False,
        }
    if not isinstance(journal, dict):
        raise Stage0Error("RECONCILIATION_REQUIRED", "Invalid pending journal")
    symbols = journal.get("symbols")
    run_id = journal.get("run_id")
    stamp = journal.get("created_at")
    stage = journal.get("status")
    if (not isinstance(symbols, list) or not (1 <= len(symbols) <= 5)
        or any(not isinstance(s, str) or not s or len(s) > 40 for s in symbols)
        or len(set(symbols)) != len(symbols)):
        blockers.append("INVALID_PENDING_SYMBOLS")
        symbols = []
    if stage != "PREPARED":
        blockers.append("NOT_PREPARED")
    if not isinstance(run_id, str):
        blockers.append("INVALID_RUN_ID")
    try:
        created = datetime.fromisoformat(stamp)
        if created.tzinfo is None:
            raise ValueError("timestamp must have timezone")
        created = created.astimezone(timezone.utc)
    except (ValueError, TypeError):
        blockers.append("INVALID_CREATION_TIMESTAMP")
        created = None
    day = created.date().isoformat() if created else None
    if day and run_id != ("STAGE0-" + day.replace("-", "") + "-V6.5.1"):
        blockers.append("RUN_DAY_MISMATCH")
    age = (now - created).total_seconds() if created else 0
    if age < LOCK_SECONDS + RECOVERY_GRACE_SECONDS:
        blockers.append("WAIT_ORIGINAL_LEASE_EXPIRY")
    if not day:
        blockers.append("UNKNOWN_CHECKPOINT_DATE")
    checkpoint = None
    checkpoint_raw = None
    if day:
        checkpoint_raw = await redis.call("GET", state_key(day))
        try:
            checkpoint = json.loads(checkpoint_raw) if checkpoint_raw else {}
        except (ValueError, TypeError):
            blockers.append("CORRUPT_DAY_CHECKPOINT")
            checkpoint = {}
    if isinstance(checkpoint, dict):
        completed = set(checkpoint.get("completed_symbols") or [])
        if completed.intersection(symbols):
            blockers.append("SYMBOL_ALREADY_CHECKPOINTED")
    else:
        blockers.append("INVALID_DAY_CHECKPOINT")

    footprint = {}
    # Even if metadata is malformed, never show a green light.
    if symbols and isinstance(run_id, str) and run_id:
        if footprint_reader is None:
            footprint = await asyncio.to_thread(
                inspect_worksheet_footprint, symbols, run_id,
            )
        else:
            footprint = await asyncio.to_thread(footprint_reader, symbols, run_id)
        for symbol, counts in footprint.items():
            if counts["raw_history_any_run"]:
                blockers.append("RAW_ROWS_PRESENT_" + symbol)
            if counts["snapshot_matching_pending_run"]:
                blockers.append("SNAPSHOT_PRESENT_" + symbol)

    return {
        "version": "V6.5.1",
        "status": ("SAFE_PREWRITE_ABORT_CANDIDATE" if not blockers
                   else "RECONCILIATION_BLOCKED"),
        "safe_to_abort_prewrite": not blockers,
        "pending": {
            "status": stage, "run_id": run_id,
            "symbols": symbols, "created_at": stamp,
        },
        "pending_age_seconds": max(0, int(age)),
        "footprint": footprint,
        "blockers": blockers,
        "checkpoint_completed_symbols": len(
            checkpoint.get("completed_symbols") or []
        ) if isinstance(checkpoint, dict) else None,
        "checkpoint_raw": checkpoint_raw,  # internal compare value only
        "journal_raw": raw,  # internal compare value only
        "ranking_permitted": False,
    }


def public_inspection(report):
    """Strip exact Redis compare values; they are not user-facing."""
    return {k: v for k, v in report.items()
            if k not in ("journal_raw", "checkpoint_raw")}
