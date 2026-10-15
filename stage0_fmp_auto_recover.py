"""V6.5.1 fail-closed recovery for FMP Stage0 interrupted Sheets writes.

Only journals created with the current coordinator's immutable full-workbook
SHA256 witnesses qualify. Legacy and partial writes remain untouched.
This module never issues a Sheets write or re-fetches market data.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from stage0_batch import (
    LOCK_SECONDS, Stage0BatchCoordinator, LOCK_KEY, pending_key, state_key,
    safe_cursor,
)
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error, TABS

MIN_RECOVERY_AGE_SECONDS = 2 * LOCK_SECONDS + 60

# Lock owner + exact journal + unchanged prior checkpoint, atomically.
_ABORT_CAS = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
if (redis.call('GET', KEYS[3]) or '') ~= ARGV[3] then return 0 end
return redis.call('DEL', KEYS[2])
"""
_FINISH_CAS = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
if (redis.call('GET', KEYS[3]) or '') ~= ARGV[3] then return 0 end
redis.call('SET', KEYS[3], ARGV[4])
redis.call('DEL', KEYS[2])
return 1
"""


def _safe_journal(raw, now):
    """Parse only v6.5.1 FMP single-symbol witnesses; no guessing."""
    try:
        j = json.loads(raw)
        stamp = datetime.fromisoformat(j["created_at"]).astimezone(timezone.utc)
        day = stamp.date().isoformat()
        symbols = j["symbols"]
        if (j.get("version") != "V6.5.1"
            or j.get("status") not in ("PREPARED", "WRITE_INTENT")
            or not isinstance(symbols, list) or len(symbols) != 1
            or not isinstance(symbols[0], str) or not symbols[0]
            or j.get("run_id") != "STAGE0-" + day.replace("-", "") + "-V6.5.1"
            or not isinstance(j.get("previous_checkpoint_raw"), str)):
            return None, "LEGACY_OR_INVALID_JOURNAL"
        before = j.get("before_fingerprint")
        after = j.get("expected_fingerprint")
        if (not isinstance(before, str) or len(before) != 64
            or any(c not in "0123456789abcdef" for c in before)):
            return None, "NO_DURABLE_BEFORE_WITNESS"
        if j["status"] == "WRITE_INTENT" and (
            not isinstance(after, str) or len(after) != 64
            or any(c not in "0123456789abcdef" for c in after)
        ):
            return None, "NO_DURABLE_EXPECTED_WITNESS"
        if (now - stamp).total_seconds() < MIN_RECOVERY_AGE_SECONDS:
            return None, "ORIGINAL_WRITER_GRACE_WINDOW"
        return (j, day), None
    except (ValueError, TypeError, KeyError, AttributeError):
        return None, "LEGACY_OR_INVALID_JOURNAL"


def _readback_state():
    # This independent capture reads ONLY the three authorized Stage0 tabs.
    reader = Stage0_Daily_Ingestor(providers={"readonly": lambda: {}})
    captured = reader._capture(reader._open())
    return reader._fingerprint(captured), {
        tab: len(captured[tab]["rows"]) for tab in TABS
    }


async def recover_verified_journal(redis, *, now=None, reader=None):
    """Idempotent recovery; never delete ambiguous journals.

    A matching pre-write workbook fingerprint proves no data changed: abort.
    A matching expected post-write fingerprint proves EVERY planned value:
    atomically promote the FMP checkpoint and remove pending.
    Any partial/mismatched readback, legacy journal, or concurrent change
    leaves Redis and Sheets untouched.
    """
    now = now or datetime.now(timezone.utc)
    original = await redis.call("GET", pending_key(None))
    if not original:
        return {"version": "V6.5.1", "status": "NO_PENDING_JOURNAL",
                "changed": False, "ranking_permitted": False}
    parsed, error = _safe_journal(original, now)
    if error:
        return {"version": "V6.5.1", "status": "RECOVERY_DEFERRED",
                "reason": error, "changed": False, "ranking_permitted": False}
    j, day = parsed
    batch = Stage0BatchCoordinator(redis)
    try:
        token = await batch.acquire()
    except Stage0Error as exc:
        return {"version": "V6.5.1", "status": "RECOVERY_DEFERRED",
                "reason": exc.status, "changed": False, "ranking_permitted": False}
    try:
        current = await redis.call("GET", pending_key(day))
        if current != original:
            return {"version": "V6.5.1", "status": "RECOVERY_DEFERRED",
                    "reason": "JOURNAL_CHANGED", "changed": False,
                    "ranking_permitted": False}
        checkpoint_raw = await redis.call("GET", state_key(day))
        if (checkpoint_raw or "") != j["previous_checkpoint_raw"]:
            return {"version": "V6.5.1", "status": "RECOVERY_DEFERRED",
                    "reason": "CHECKPOINT_CHANGED", "changed": False,
                    "ranking_permitted": False}
        # The sole source of proof is freshly authenticated Google Sheets.
        digest, sizes = await asyncio.to_thread(reader or _readback_state)
        before = j["before_fingerprint"]
        expected = j.get("expected_fingerprint")
        keys = [LOCK_KEY, pending_key(day), state_key(day)]
        base_args = [token, original, j["previous_checkpoint_raw"]]
        if digest == before:
            n = await redis.eval(_ABORT_CAS, keys, base_args)
            return {"version": "V6.5.1",
                    "status": ("RECOVERED_VERIFIED_NO_WRITE" if int(n or 0) == 1
                               else "RECOVERY_CAS_FAILED"),
                    "symbol": j["symbols"][0], "changed": int(n or 0) == 1,
                    "checkpoint_advanced": False, "ranking_permitted": False}
        if j["status"] == "WRITE_INTENT" and digest == expected:
            try:
                previous = (json.loads(j["previous_checkpoint_raw"])
                            if j["previous_checkpoint_raw"] else {})
                if not isinstance(previous, dict):
                    raise ValueError("Invalid checkpoint")
                if j["symbols"][0] in (previous.get("completed_symbols") or []):
                    raise ValueError("Already completed")
                completed = safe_cursor(previous, j["symbols"])
            except (ValueError, TypeError, KeyError):
                return {"version": "V6.5.1", "status": "RECOVERY_DEFERRED",
                        "reason": "INVALID_PRIOR_CHECKPOINT", "changed": False,
                        "ranking_permitted": False}
            cp = {
                "version": "V6.5.1", "utc_date": day,
                "last_verified_run_id": j["run_id"],
                "completed_symbols": completed,
                "last_verified_symbols": j["symbols"],
                "readback": {tab: {"verified_rows": count, "status": "PASS"}
                             for tab, count in sizes.items()},
                "verified_at_utc": datetime.now(timezone.utc).isoformat(),
                "status": "COMMITTED_VERIFY_PASS",
                "recovery_method": "EXACT_FULL_SHEETS_SHA256",
            }
            n = await redis.eval(_FINISH_CAS, keys, [
                *base_args, json.dumps(cp, separators=(",", ":"))
            ])
            return {"version": "V6.5.1",
                    "status": ("RECOVERED_COMMITTED_VERIFY_PASS" if int(n or 0) == 1
                               else "RECOVERY_CAS_FAILED"),
                    "symbol": j["symbols"][0], "changed": int(n or 0) == 1,
                    "checkpoint_advanced": int(n or 0) == 1,
                    "ranking_permitted": False}
        return {"version": "V6.5.1", "status": "RECOVERY_DEFERRED",
                "reason": "AMBIGUOUS_PARTIAL_WRITE",
                "symbol": j["symbols"][0], "changed": False,
                "ranking_permitted": False}
    except Stage0Error as exc:
        return {"version": "V6.5.1", "status": "RECOVERY_DEFERRED",
                "reason": exc.status, "changed": False,
                "ranking_permitted": False}
    finally:
        await batch.release(token)
