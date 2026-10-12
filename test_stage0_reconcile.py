"""Offline reconciliation invariants. No network or production secrets."""
import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone

from stage0_reconcile import inspect_abandoned_prewrite, public_inspection


DAY = "2026-10-12"
RUN = "STAGE0-20261012-V6.5.1"
WHEN = "2026-10-12T13:32:15+00:00"
NOW = datetime(2026, 10, 12, 14, 32, 0, tzinfo=timezone.utc)


class FakeRedis:
    def __init__(self, *, symbols=None, checkpoint=None, stage="PREPARED"):
        self.journal = json.dumps({
            "status": stage, "run_id": RUN,
            "symbols": symbols or ["TER"], "created_at": WHEN,
        })
        self.checkpoint = json.dumps(
            checkpoint if checkpoint is not None else {
                "completed_symbols": ["AAPL", "SPY"],
            }
        )
    async def call(self, command, key):
        assert command == "GET"
        if key.endswith(":pending"):
            return self.journal
        if key.endswith(":checkpoint:" + DAY):
            return self.checkpoint
        raise AssertionError(key)


def footprint(raw=0, snapshot=0):
    return lambda symbols, run_id: {
        symbols[0]: {
            "raw_history_any_run": raw,
            "snapshot_matching_pending_run": snapshot,
        }
    }


class ReconcileTests(unittest.TestCase):
    def test_ter_absent_can_abort_after_lease_expiry(self):
        result = asyncio.run(inspect_abandoned_prewrite(
            FakeRedis(), footprint_reader=footprint(), now=NOW))
        self.assertTrue(result["safe_to_abort_prewrite"])
        self.assertEqual(result["pending"]["symbols"], ["TER"])
        self.assertNotIn("journal_raw", public_inspection(result))

    def test_any_historical_row_must_block(self):
        result = asyncio.run(inspect_abandoned_prewrite(
            FakeRedis(), footprint_reader=footprint(raw=1), now=NOW))
        self.assertFalse(result["safe_to_abort_prewrite"])
        self.assertIn("RAW_ROWS_PRESENT_TER", result["blockers"])

    def test_snapshot_matching_run_must_block(self):
        result = asyncio.run(inspect_abandoned_prewrite(
            FakeRedis(), footprint_reader=footprint(snapshot=1), now=NOW))
        self.assertFalse(result["safe_to_abort_prewrite"])

    def test_checkpoint_previously_advanced_must_block(self):
        result = asyncio.run(inspect_abandoned_prewrite(
            FakeRedis(checkpoint={"completed_symbols":["TER"]}),
            footprint_reader=footprint(), now=NOW))
        self.assertFalse(result["safe_to_abort_prewrite"])
        self.assertIn("SYMBOL_ALREADY_CHECKPOINTED", result["blockers"])

    def test_lease_not_expired_must_block(self):
        now=datetime.fromisoformat(WHEN)+timedelta(minutes=3)
        result = asyncio.run(inspect_abandoned_prewrite(
            FakeRedis(), footprint_reader=footprint(), now=now))
        self.assertFalse(result["safe_to_abort_prewrite"])
        self.assertIn("WAIT_ORIGINAL_LEASE_EXPIRY", result["blockers"])

    def test_no_journal(self):
        redis = FakeRedis()
        redis.journal = None
        result = asyncio.run(inspect_abandoned_prewrite(
            redis, footprint_reader=footprint(), now=NOW))
        self.assertEqual(result["status"], "NO_PENDING_JOURNAL")


if __name__ == "__main__":
    unittest.main()
