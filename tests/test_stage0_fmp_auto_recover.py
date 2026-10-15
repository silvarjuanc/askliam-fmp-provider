"""No-network V6.5.1 journal recovery regression tests.

Run: python -m unittest discover -s tests -p "test_stage0_fmp_auto_recover.py"
"""
import asyncio
import hashlib
import json
import unittest
from datetime import datetime, timedelta, timezone

from stage0_fmp_auto_recover import (
    MIN_RECOVERY_AGE_SECONDS, recover_verified_journal,
)


class FakeRedis:
    def __init__(self, pending=None, checkpoint=None):
        self.keys = {}
        if pending is not None:
            self.keys["askliam:v651:stage0:pending"] = pending
        if checkpoint is not None:
            day = json.loads(pending)["created_at"][:10]
            self.keys["askliam:v651:stage0:checkpoint:" + day] = checkpoint
        self.eval_count = 0

    async def call(self, op, *args):
        if op == "GET":
            return self.keys.get(args[0])
        if op == "SET":
            key, value = args[0], args[1]
            if "NX" in args and key in self.keys:
                return None
            self.keys[key] = value
            return "OK"
        raise AssertionError("UNEXPECTED_COMMAND")

    async def eval(self, script, keys, args):
        self.eval_count += 1
        if "ARGV[4]" in script and "redis.call('SET', KEYS[3]" in script:
            operation = "FINISH"
        elif "ARGV[3]" in script:
            operation = "ABORT"
        else:
            operation = "RELEASE"
        if operation == "RELEASE":
            if self.keys.get(keys[0]) == args[0]:
                self.keys.pop(keys[0], None)
                return 1
            return 0
        lock, pending, checkpoint = keys
        if (self.keys.get(lock) != args[0]
            or self.keys.get(pending) != args[1]
            or (self.keys.get(checkpoint) or "") != args[2]):
            return 0
        if operation == "FINISH":
            self.keys[checkpoint] = args[3]
        self.keys.pop(pending, None)
        return 1


def digest(s):
    return hashlib.sha256(s.encode()).hexdigest()


def context(stage="WRITE_INTENT", *, older=True):
    now = datetime.now(timezone.utc)
    stamp = now - timedelta(seconds=MIN_RECOVERY_AGE_SECONDS + 60 if older else 1)
    day = stamp.date().isoformat()
    previous = json.dumps({"completed_symbols": ["AAPL"]})
    pending = json.dumps({
        "version": "V6.5.1",
        "status": stage,
        "symbols": ["NVDA"],
        "run_id": "STAGE0-" + day.replace("-", "") + "-V6.5.1",
        "created_at": stamp.isoformat(),
        "previous_checkpoint_raw": previous,
        "before_fingerprint": digest("before"),
        "expected_fingerprint": digest("after") if stage == "WRITE_INTENT" else None,
    })
    return FakeRedis(pending, previous), now, day


class AutoRecoveryTests(unittest.TestCase):
    def recover(self, fake, now, observed):
        return asyncio.run(recover_verified_journal(
            fake, now=now, reader=lambda: (digest(observed),
                {"Raw_OHLCV": 300, "Market_Data_Snapshot": 1,
                 "FMP_Runtime_State": 20}),
        ))

    def test_empty_journal(self):
        fake = FakeRedis()
        result = self.recover(fake, datetime.now(timezone.utc), "before")
        self.assertEqual(result["status"], "NO_PENDING_JOURNAL")
        self.assertFalse(result["changed"])

    def test_safe_prewrite_abort_with_legacy_history(self):
        fake, now, day = context("PREPARED")
        result = self.recover(fake, now, "before")
        self.assertEqual(result["status"], "RECOVERED_VERIFIED_NO_WRITE")
        self.assertFalse(result["checkpoint_advanced"])
        self.assertNotIn("askliam:v651:stage0:pending", fake.keys)
        self.assertEqual(json.loads(fake.keys["askliam:v651:stage0:checkpoint:" + day])
                         ["completed_symbols"], ["AAPL"])

    def test_postwrite_readback_finalizes_checkpoint(self):
        fake, now, day = context()
        result = self.recover(fake, now, "after")
        self.assertEqual(result["status"], "RECOVERED_COMMITTED_VERIFY_PASS")
        self.assertTrue(result["checkpoint_advanced"])
        self.assertNotIn("askliam:v651:stage0:pending", fake.keys)
        checkpoint = json.loads(fake.keys["askliam:v651:stage0:checkpoint:" + day])
        self.assertEqual(checkpoint["completed_symbols"], ["AAPL", "NVDA"])
        self.assertEqual(checkpoint["recovery_method"], "EXACT_FULL_SHEETS_SHA256")

    def test_partial_write_is_never_cleared(self):
        fake, now, _ = context()
        original = fake.keys["askliam:v651:stage0:pending"]
        result = self.recover(fake, now, "partially-written")
        self.assertEqual(result["reason"], "AMBIGUOUS_PARTIAL_WRITE")
        self.assertEqual(fake.keys["askliam:v651:stage0:pending"], original)

    def test_fresh_writer_is_never_interrupted(self):
        fake, now, _ = context(older=False)
        result = self.recover(fake, now, "after")
        self.assertEqual(result["reason"], "ORIGINAL_WRITER_GRACE_WINDOW")
        self.assertTrue(fake.keys.get("askliam:v651:stage0:pending"))

    def test_old_unwitnessed_journal_is_not_deleted(self):
        fake, now, _ = context()
        raw = json.loads(fake.keys["askliam:v651:stage0:pending"])
        del raw["before_fingerprint"]
        fake.keys["askliam:v651:stage0:pending"] = json.dumps(raw)
        result = self.recover(fake, now, "after")
        self.assertEqual(result["reason"], "NO_DURABLE_BEFORE_WITNESS")
        self.assertTrue(fake.keys.get("askliam:v651:stage0:pending"))

    def test_checkpoint_change_blocks_recovery(self):
        fake, now, day = context()
        fake.keys["askliam:v651:stage0:checkpoint:" + day] = json.dumps(
            {"completed_symbols": ["META"]})
        result = self.recover(fake, now, "after")
        self.assertEqual(result["reason"], "CHECKPOINT_CHANGED")
        self.assertTrue(fake.keys.get("askliam:v651:stage0:pending"))


if __name__ == "__main__":
    unittest.main()
