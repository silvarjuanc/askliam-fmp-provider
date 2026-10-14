"""Offline fail-closed Coinbase Stage0 transaction tests; no real Redis/Sheets."""
import json
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

from stage0_crypto_coinbase_commit import (
    confirm_spot, spot_commit_one, spot_checkpoint_key, spot_write_guard,
)
from stage0_batch import LOCK_KEY, pending_key
from stage0_ingestor import Stage0Error


class FakeRedis:
    def __init__(self):
        self.store = {}
        self.reads = []

    async def call(self, command, *args):
        self.reads.append((command, *args))
        if command == "GET":
            return self.store.get(args[0])
        if command == "SET":
            key, value = args[:2]
            if "NX" in args and key in self.store:
                return None
            self.store[key] = value
            return "OK"
        raise AssertionError("Unexpected Redis mutation: " + command)

    async def eval(self, script, keys, args):
        if len(keys) == 1:  # exclusive lease conditional release
            if self.store.get(keys[0]) == args[0]:
                self.store.pop(keys[0], None)
                return 1
            return 0
        if len(keys) == 3:  # checked checkpoint commit
            lock, journal, checkpoint = keys
            if self.store.get(lock) != args[0] or self.store.get(journal) != args[1]:
                return 0
            prior = self.store.get(checkpoint)
            if args[2] == "1" and prior != args[3]:
                return 0
            if args[2] != "1" and prior is not None:
                return 0
            self.store[checkpoint] = args[4]
            self.store.pop(journal, None)
            return 1
        raise AssertionError("Unknown atomic mutation")


def configured():
    return {
        "ASKLIAM_COINBASE_SPOT_WRITE_ENABLED": "true",
        "ASKLIAM_COINBASE_SPOT_WRITE_SYMBOL": "BTC",
        "ASKLIAM_COINBASE_SPOT_WRITE_UTC_DAY": datetime.now(timezone.utc).date().isoformat(),
        "ASKLIAM_SPREADSHEET_ID": "fake-sheet",
        "GOOGLE_SERVICE_ACCOUNT_JSON": "fake-json",
    }


def offline_frames():
    import pandas as pd
    return {
        "Raw_OHLCV": pd.DataFrame([{
            "canonical_symbol": "BTC",
            "source": "COINBASE_EXCHANGE_SPOT_USD",
            "trading_date": "2026-10-13",
        }]),
        "Market_Data_Snapshot": pd.DataFrame([{
            "canonical_symbol": "BTC", "last_price": 50000,
        }]),
    }, {"ohlcv_sha256": "safe_test_digest", "daily_rows": 299, "mandatory_fields_present": 9}


def proof():
    return {
        "commit_status": "COMMITTED", "verify_status": "PASS",
        "final_status": "COMMITTED",
        "tabs": {
            "Raw_OHLCV": {"status": "PASS"},
            "Market_Data_Snapshot": {"status": "PASS"},
            "FMP_Runtime_State": {"status": "PASS"},
        },
    }


class GuardTests(unittest.TestCase):
    def test_disabled_flag_blocks(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(Stage0Error, "SPOT_WRITE_DISABLED"):
                spot_write_guard("BTC", confirm_spot("BTC"))

    def test_other_symbol_blocked_before_write(self):
        with patch.dict(os.environ, configured()):
            with self.assertRaisesRegex(Stage0Error, "SPOT_WRITE_SCOPE_DENIED"):
                spot_write_guard("ETH", confirm_spot("ETH"))

    def test_wrong_confirmation_blocked(self):
        with patch.dict(os.environ, configured()):
            with self.assertRaisesRegex(Stage0Error, "EXPLICIT_CONFIRMATION_REQUIRED"):
                spot_write_guard("BTC", "INVALID_CONFIRMATION")


class CommitTests(unittest.IsolatedAsyncioTestCase):
    async def test_readback_commit_creates_separate_spot_checkpoint(self):
        redis = FakeRedis()
        writer = Mock(return_value=proof())
        with patch.dict(os.environ, configured()), patch(
            "stage0_crypto_coinbase_commit.acquire_spot_frames",
            new=AsyncMock(return_value=offline_frames()),
        ), patch(
            "stage0_crypto_coinbase_commit._blocking_preflight",
            return_value={},
        ):
            r = await spot_commit_one(
                "BTC", confirm_spot("BTC"), redis=redis, client=None, writer=writer,
            )
        self.assertEqual(r["status"], "COMMITTED_VERIFY_PASS")
        self.assertEqual(r["daily_bars"], 299)
        self.assertFalse(r["ranking_permitted"])
        self.assertFalse(r["execution_permitted"])
        day = datetime.now(timezone.utc).date().isoformat()
        cp = json.loads(redis.store[spot_checkpoint_key(day)])
        self.assertEqual(cp["completed_symbols"], ["BTC"])
        self.assertNotIn(pending_key(day), redis.store)
        self.assertNotIn(LOCK_KEY, redis.store)
        self.assertTrue(writer.called)

    async def test_pending_journal_refuses_acquisition(self):
        redis = FakeRedis()
        day = datetime.now(timezone.utc).date().isoformat()
        redis.store[pending_key(day)] = "prior-pending-write"
        fetch = AsyncMock()
        with patch.dict(os.environ, configured()), patch(
            "stage0_crypto_coinbase_commit.acquire_spot_frames", new=fetch,
        ):
            with self.assertRaisesRegex(Stage0Error, "RECONCILIATION_REQUIRED"):
                await spot_commit_one(
                    "BTC", confirm_spot("BTC"), redis=redis, client=None,
                )
        fetch.assert_not_awaited()
        self.assertEqual(redis.store[pending_key(day)], "prior-pending-write")

    async def test_unverified_readback_leaves_global_journal(self):
        redis = FakeRedis()
        incomplete = {**proof(), "verify_status": "NOT_VERIFIED"}
        with patch.dict(os.environ, configured()), patch(
            "stage0_crypto_coinbase_commit.acquire_spot_frames",
            new=AsyncMock(return_value=offline_frames()),
        ), patch(
            "stage0_crypto_coinbase_commit._blocking_preflight",
            return_value={},
        ):
            with self.assertRaisesRegex(Stage0Error, "VERIFICATION_FAILED"):
                await spot_commit_one(
                    "BTC", confirm_spot("BTC"), redis=redis, client=None,
                    writer=lambda *args: incomplete,
                )
        day = datetime.now(timezone.utc).date().isoformat()
        self.assertIn(pending_key(day), redis.store)
        self.assertNotIn(spot_checkpoint_key(day), redis.store)
        self.assertNotIn(LOCK_KEY, redis.store)


if __name__ == "__main__":
    unittest.main()
