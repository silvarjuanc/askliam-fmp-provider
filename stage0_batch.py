"""AskLiam V6.5.1 Stage 0 transactional batch coordinator.

Redis is the authoritative durable journal. Google Sheets remains the immutable
historical store of record, restricted to the three Stage0 tabs.
No ranking, trading or lifecycle writes occur here.
"""
from __future__ import annotations

import asyncio
import json
import os
import secrets
from datetime import datetime, timezone

from stage0_fmp_adapter import ExistingFMPStage0Adapter
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error
from stage0_fmp_cooldown import (FMP_402_KEY, FMP_402_REASON, COOLDOWN_HASH_TTL_SECONDS,
                                active_cooldowns, cooldown_expiry)

LOCK_KEY = "askliam:v651:stage0:exclusive"
LOCK_SECONDS = 900
MIN_QUOTA_RESERVE = 40
MAX_BATCH = 5
TRANSIENT_HOLD_SECONDS = 1800

def transient_hold_key(day):
    return "askliam:v651:stage0:transient_prewrite:" + day


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
            "deferred_today": await self.deferred_today(),
            "transient_prewrite_today": await self.transient_prewrite_today(),
            "fmp_quote_402_cooldowns": await self.fmp_402_cooldowns(),
        }

    async def deferred_today(self):
        data = await self.redis.call(
            "HGETALL", "askliam:v651:stage0:deferred:" + utc_day()
        )
        if isinstance(data, list):
            return dict(zip(data[::2], data[1::2]))
        return data if isinstance(data, dict) else {}

    async def transient_prewrite_today(self):
        """Temporary source/API retry holds, never provider-deferred or scored."""
        raw = await self.redis.call("HGETALL", transient_hold_key(utc_day()))
        if isinstance(raw, list):
            return dict(zip(raw[::2], raw[1::2]))
        return raw if isinstance(raw, dict) else {}

    async def mark_transient_prewrite(self, symbols, safe_code, token):
        """Atomic PREWRITE-only journal cleanup + short retry hold.

        Caller proves Stage0_Daily_Ingestor.write_attempted is false.
        This is NOT usable for Sheets writes, postwrite reads, or uncertain
        status codes. Atomic Redis CAS also enforces original exclusive lease.
        """
        if len(symbols) != 1 or safe_code not in (
            "SHEET_READ_FAILED", "AUTHENTICATION_FAILED",
        ):
            raise Stage0Error("RECONCILIATION_REQUIRED","Unapproved prewrite error")
        day = utc_day()
        raw = await self.redis.call("GET", pending_key(day))
        if not isinstance(raw, str):
            raise Stage0Error("RECONCILIATION_REQUIRED","PREPARED journal missing")
        try:
            j = json.loads(raw)
        except (ValueError,TypeError):
            raise Stage0Error("RECONCILIATION_REQUIRED","Invalid journal") from None
        if (not isinstance(j,dict) or j.get("status")!="PREPARED"
            or j.get("symbols")!=symbols
            or j.get("run_id") != ("STAGE0-" + day.replace("-","") + "-V6.5.1")):
            raise Stage0Error("RECONCILIATION_REQUIRED","Unexpected journal")
        script = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[3], ARGV[3], ARGV[4])
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[5]))
redis.call('DEL', KEYS[2])
return 1
"""
        result = await self.redis.eval(
            script,[LOCK_KEY,pending_key(day),transient_hold_key(day)],
            [token,raw,symbols[0],safe_code,TRANSIENT_HOLD_SECONDS],
        )
        if int(result or 0)!=1:
            raise Stage0Error("RECONCILIATION_REQUIRED","Prewrite retry hold CAS failed")

    async def fmp_402_cooldowns(self):
        """Persist FMP quote entitlements across UTC day rollover (7d fields)."""
        data = await self.redis.call("HGETALL", FMP_402_KEY)
        try:
            return active_cooldowns(data)
        except (TypeError, ValueError, OverflowError):
            raise Stage0Error("STORAGE_UNAVAILABLE", "INVALID_FMP_COOLDOWN_LEDGER") from None

    async def seed_402_from_today(self, token):
        """Explicit one-time migration of today's confirmed FMP 402 entries.

        Requires an active exclusive lease. Cannot write market worksheets,
        checkpoints, or the global pending journal. Entries with generic
        error codes are never guessed to be FMP_HTTP_402.
        """
        day = utc_day()
        records = await self.deferred_today()
        targets = sorted(symbol for symbol, reason in records.items()
                         if reason == FMP_402_REASON)
        if not targets:
            return {"eligible_today": 0, "new_cooldowns": 0,
                    "already_active": 0, "cooldown_days": 7}
        expiry = cooldown_expiry()
        script = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('EXISTS', KEYS[2]) ~= 0 then return -2 end
if redis.call('HGET', KEYS[3], ARGV[2]) ~= ARGV[3] then return 0 end
local previous = tonumber(redis.call('HGET', KEYS[4], ARGV[2]) or '0')
if previous > tonumber(ARGV[4]) then return 2 end
redis.call('HSET', KEYS[4], ARGV[2], ARGV[5])
redis.call('EXPIRE', KEYS[4], tonumber(ARGV[6]))
return 1
"""
        from time import time
        epoch = int(time())
        inserted = 0
        already = 0
        for symbol in targets:
            code = await self.redis.eval(
                script,
                [LOCK_KEY, pending_key(day),
                 "askliam:v651:stage0:deferred:" + day, FMP_402_KEY],
                [token, symbol, FMP_402_REASON, epoch,
                 expiry, COOLDOWN_HASH_TTL_SECONDS],
            )
            code = int(code or 0)
            if code == 1:
                inserted += 1
            elif code == 2:
                already += 1
            else:
                raise Stage0Error("RECONCILIATION_REQUIRED",
                                  "Deferred history changed; cooldown seed halted")
        return {"eligible_today": len(targets), "new_cooldowns": inserted,
                "already_active": already, "cooldown_days": 7}

    async def mark_deferred_prewrite(self, symbols, reason, token):
        # Safe ONLY for source validation errors raised before any Sheet writes.
        # Atomically record the defer and remove this run's PREPARED journal
        # while verifying exclusive lease ownership and matching run symbols.
        import hashlib
        day = utc_day()
        stored = await self.redis.call("GET", pending_key(day))
        if not stored:
            raise Stage0Error("RECONCILIATION_REQUIRED", "Pending journal disappeared")
        try:
            journal = json.loads(stored)
        except (ValueError, TypeError):
            raise Stage0Error("RECONCILIATION_REQUIRED", "Pending journal unreadable") from None
        if journal.get("symbols") != symbols:
            raise Stage0Error("RECONCILIATION_REQUIRED", "Pending symbols changed")
        script = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[3], ARGV[3], ARGV[4])
if ARGV[4] == 'fmp: FMP_HTTP_402' then
    redis.call('HSET', KEYS[4], ARGV[3], ARGV[5])
    redis.call('EXPIRE', KEYS[4], tonumber(ARGV[6]))
end
redis.call('DEL', KEYS[2])
return 1
"""
        expiry = cooldown_expiry()
        for symbol in symbols:
            result = await self.redis.eval(
                script,
                [LOCK_KEY, pending_key(day),
                 "askliam:v651:stage0:deferred:" + day, FMP_402_KEY],
                [token, stored, symbol, reason, expiry, COOLDOWN_HASH_TTL_SECONDS],
            )
            if int(result or 0) != 1:
                raise Stage0Error(
                    "RECONCILIATION_REQUIRED",
                    "Could not confirm provider failure before write",
                )

    async def abort_prewrite(self, symbols, token):
        """Discard a PREPARED journal only when provider acquisition failed
        before entering the write plan, under our original exclusive lease.
        Never invoke this for write, verification, or Redis-commit failures.
        """
        raw = await self.redis.call("GET", pending_key(utc_day()))
        if not raw:
            raise Stage0Error("RECONCILIATION_REQUIRED", "PREPARED journal missing")
        try:
            saved = json.loads(raw)
        except (ValueError, TypeError):
            raise Stage0Error("RECONCILIATION_REQUIRED", "PREPARED journal invalid") from None
        if saved.get("status") != "PREPARED" or saved.get("symbols") != symbols:
            raise Stage0Error("RECONCILIATION_REQUIRED", "PREPARED journal mismatch")
        script = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
return redis.call('DEL', KEYS[2])
"""
        confirmed = await self.redis.eval(
            script, [LOCK_KEY, pending_key(utc_day())], [token, raw],
        )
        if int(confirmed or 0) != 1:
            raise Stage0Error("RECONCILIATION_REQUIRED", "Failed to release prewrite journal")

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
        # Capture the exact prior checkpoint for recovery CAS. Never assume
        # that it stayed unchanged across a server restart or lease expiry.
        previous_raw = await self.redis.call("GET", state_key(day))
        journal = {
            "status": "PREPARED",
            "version": "V6.5.1",
            "run_id": run_id,
            "symbols": symbols,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "previous_checkpoint_raw": previous_raw or "",
        }
        pending_written = await self.redis.call(
            "SET", pending_key(day),
            json.dumps(journal, separators=(",", ":")), "NX",
        )
        if str(pending_written).upper() != "OK":
            raise Stage0Error("RECONCILIATION_REQUIRED", "Another pending journal exists")
        return day, run_id, state["checkpoint"]

    async def record_write_evidence(self, day, run_id, symbols, token, phase, fingerprint):
        """CAS-write one SHA256 witness while the original Stage0 lease is held.

        CAPTURED: exact STATE_BEFORE digest, PREPARED only.
        WRITE_INTENT: exact expected full readback digest, prior to any write.
        Redis refusal halts the worker before it can mutate Google Sheets.
        """
        if (phase not in ("CAPTURED", "WRITE_INTENT")
            or not isinstance(fingerprint, str) or len(fingerprint) != 64
            or any(c not in "0123456789abcdef" for c in fingerprint)):
            raise Stage0Error("RECONCILIATION_REQUIRED", "INVALID_EVIDENCE_DIGEST")
        raw = await self.redis.call("GET", pending_key(day))
        if not isinstance(raw, str):
            raise Stage0Error("RECONCILIATION_REQUIRED", "PENDING_JOURNAL_MISSING")
        try:
            j = json.loads(raw)
        except (ValueError, TypeError):
            raise Stage0Error("RECONCILIATION_REQUIRED", "PENDING_JOURNAL_INVALID") from None
        expected_status = "PREPARED"
        if (j.get("run_id") != run_id or j.get("symbols") != symbols
            or j.get("status") != expected_status
            or j.get("version") != "V6.5.1"):
            raise Stage0Error("RECONCILIATION_REQUIRED", "PENDING_JOURNAL_CHANGED")
        if phase == "CAPTURED":
            if j.get("before_fingerprint"):
                raise Stage0Error("RECONCILIATION_REQUIRED", "DUPLICATE_BASELINE_EVIDENCE")
            j["before_fingerprint"] = fingerprint
        else:
            if not j.get("before_fingerprint"):
                raise Stage0Error("RECONCILIATION_REQUIRED", "BASELINE_EVIDENCE_MISSING")
            if j.get("expected_fingerprint"):
                raise Stage0Error("RECONCILIATION_REQUIRED", "DUPLICATE_WRITE_INTENT")
            j["expected_fingerprint"] = fingerprint
            j["status"] = "WRITE_INTENT"
        # Only advance journal metadata after Redis lease+exact value CAS.
        script = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
redis.call('SET', KEYS[2], ARGV[3])
return 1
"""
        ok = await self.redis.eval(
            script, [LOCK_KEY, pending_key(day)],
            [token, raw, json.dumps(j, separators=(",", ":"))],
        )
        if int(ok or 0) != 1:
            raise Stage0Error("RECONCILIATION_REQUIRED", "JOURNAL_EVIDENCE_CAS_FAILED")

    def run_blocking(self, symbols, run_id, day=None, token=None, loop=None):
        ingestor = Stage0_Daily_Ingestor(
            providers={"fmp": lambda: adapter()},
            spreadsheet_id=self.spreadsheet_id,
        )
        ingestor.run_id = run_id
        if day is not None or token is not None or loop is not None:
            if day is None or token is None or loop is None:
                raise Stage0Error("CONFIGURATION_FAILED", "INCOMPLETE_JOURNAL_WITNESS")
            def journal_hook(phase, fingerprint):
                witness = asyncio.run_coroutine_threadsafe(
                    self.record_write_evidence(day, run_id, symbols, token, phase, fingerprint),
                    loop,
                )
                try:
                    witness.result(timeout=30)
                except Stage0Error:
                    raise
                except Exception:
                    # Never start Sheets mutation when Redis witness is uncertain.
                    raise Stage0Error("RECONCILIATION_REQUIRED", "JOURNAL_EVIDENCE_UNAVAILABLE") from None
            ingestor.journal_stage_hook = journal_hook
        try:
            book = ingestor._open()
            adapter = ExistingFMPStage0Adapter(
                book, run_id,
                max_symbols=len(symbols),
                historical_budget=len(symbols),
                symbols=symbols,
            )
            return ingestor.run()
        except Stage0Error as exc:
            # NO Sheet mutation could occur before write_attempted becomes true.
            # Only known transient Google API failures are eligible for an
            # atomic PREPARED abort.  A post-write failure remains blocked.
            if (
                not ingestor.write_attempted
                and exc.status in ("SHEET_READ_FAILED","AUTHENTICATION_FAILED")
                and any(marker in exc.detail for marker in (
                    "APIError","Timeout","TransportError"
                ))
            ):
                raise Stage0Error("SAFE_PREWRITE_RETRYABLE",exc.status) from None
            raise

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
