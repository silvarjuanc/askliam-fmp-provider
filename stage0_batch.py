"""AskLiam V6.5.1 Stage 0 transactional batch coordinator.

Redis is the authoritative durable journal. Google Sheets remains the immutable
historical store of record, restricted to the three Stage0 tabs.
No ranking, trading or lifecycle writes occur here.
"""
from __future__ import annotations

import json
import os
import secrets
from datetime import datetime, timezone

from stage0_fmp_adapter import ExistingFMPStage0Adapter
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

LOCK_KEY = "askliam:v651:stage0:exclusive"
LOCK_SECONDS = 900
MIN_QUOTA_RESERVE = 40
MAX_BATCH = 5

_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""
_COMMIT_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2])
redis.call('DEL', KEYS[3])
return 1
"""


def utc_day():
    return datetime.now(timezone.utc).date().isoformat()


def state_key(day):
    return "askliam:v651:stage0:checkpoint:" + day


def pending_key(day):
    # One global unresolved journal blocks runs across UTC day boundaries.
    return "askliam:v651:stage0:pending"


def safe_cursor(previous, symbols):
    """Use canonical symbols as durable completed set; never advance by index."""
    old = set(previous.get("completed_symbols") or [])
    return sorted(old.union(symbols))


class Stage0BatchCoordinator:
    def __init__(self, redis_state, *, spreadsheet_id: str | None = None):
        if redis_state is None:
            raise Stage0Error("STORAGE_UNAVAILABLE", "Upstash Redis required")
        self.redis = redis_state
        self.spreadsheet_id = spreadsheet_id or os.getenv("ASKLIAM_SPREADSHEET_ID")

    async def status(self):
        day = utc_day()
        raw = await self.redis.call("GET", state_key(day))
        pending_raw = await self.redis.call("GET", pending_key(day))
        quota = await self.redis.quota()
        return {
            "version": "V6.5.1",
            "utc_date": day,
            "quota": quota,
            "checkpoint": json.loads(raw) if raw else {
                "completed_symbols": [], "last_verified_run_id": None,
            },
            "pending_reconciliation": bool(pending_raw),
            "pending": json.loads(pending_raw) if pending_raw else None,
        }

    async def acquire(self):
        token = secrets.token_urlsafe(32)
        result = await self.redis.call(
            "SET", LOCK_KEY, token, "NX", "EX", LOCK_SECONDS
        )
        if str(result).upper() != "OK":
            raise Stage0Error("CONCURRENT_RUN_BLOCKED", "Stage0 lease already held")
        return token

    async def release(self, token):
        await self.redis.eval(_RELEASE_LUA, [LOCK_KEY], [token])

    async def prepare(self, symbols, token):
        if not symbols or len(symbols) > MAX_BATCH:
            raise Stage0Error("INVALID_BATCH", "Select between 1 and 5 canonical symbols")
        if len(set(symbols)) != len(symbols):
            raise Stage0Error("INVALID_BATCH", "Duplicate canonical symbols")
        state = await self.status()
        if state["pending_reconciliation"]:
            raise Stage0Error(
                "RECONCILIATION_REQUIRED",
                "Unverified previous batch exists; no new writes allowed",
            )
        completed = set(state["checkpoint"].get("completed_symbols") or [])
        if any(symbol in completed for symbol in symbols):
            raise Stage0Error(
                "ALREADY_VERIFIED_TODAY",
                "At least one symbol already has a verified daily checkpoint",
            )
        quota = state["quota"]
        if quota["blocked"] or quota["remaining"] < max(MIN_QUOTA_RESERVE, 3 * len(symbols)):
            raise Stage0Error("FMP_QUOTA_GUARD", "Quota reserve breached, wait for UTC reset")
        day = state["utc_date"]
        # Reuse the same run ID per UTC day to update rather than append
        # snapshots for the same asset on retry.
        run_id = "STAGE0-" + day.replace("-", "") + "-V6.5.1"
        journal = {
            "status": "PREPARED",
            "run_id": run_id,
            "symbols": symbols,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        pending_written = await self.redis.call(
            "SET", pending_key(day),
            json.dumps(journal, separators=(",", ":")), "NX",
        )
        if str(pending_written).upper() != "OK":
            raise Stage0Error("RECONCILIATION_REQUIRED", "Another pending journal exists")
        return day, run_id, state["checkpoint"]

    def run_blocking(self, symbols, run_id):
        ingestor = Stage0_Daily_Ingestor(
            providers={"fmp": lambda: adapter()},
            spreadsheet_id=self.spreadsheet_id,
        )
        ingestor.run_id = run_id
        book = ingestor._open()
        adapter = ExistingFMPStage0Adapter(
            book, run_id,
            max_symbols=len(symbols),
            historical_budget=len(symbols),
            symbols=symbols,
        )
        return ingestor.run()

    async def finish(self, day, run_id, previous, symbols, result, token):
        if result.get("commit_status") != "COMMITTED" or result.get("verify_status") != "PASS":
            raise Stage0Error("VERIFICATION_FAILED", "Read-back commit proof missing")
        if utc_day() != day:
            raise Stage0Error(
                "UTC_DAY_CHANGED",
                "Read-back passed but UTC day changed; manual journal reconciliation required",
            )
        completed = safe_cursor(previous, symbols)
        checkpoint = {
            "version": "V6.5.1",
            "utc_date": day,
            "last_verified_run_id": run_id,
            "completed_symbols": completed,
            "last_verified_symbols": symbols,
            "readback": result["tabs"],
            "verified_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "COMMITTED_VERIFY_PASS",
        }
        committed = await self.redis.eval(
            _COMMIT_LUA,
            [LOCK_KEY, state_key(day), pending_key(day)],
            [token, json.dumps(checkpoint, separators=(",", ":"))],
        )
        if int(committed or 0) != 1:
            raise Stage0Error(
                "CHECKPOINT_NOT_COMMITTED",
                "Lease lost before durable checkpoint write; manual reconciliation required",
            )
        result = dict(result)
        result["cursor_advance_permitted"] = True
        result["checkpoint_status"] = "COMMITTED_VERIFY_PASS"
        result["completed_symbols_today"] = len(completed)
        return result
