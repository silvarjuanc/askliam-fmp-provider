"""Offline read-only crypto checkpoint tests. Never accesses Upstash/Sheets."""
from __future__ import annotations

import json
import unittest

from stage0_crypto_checkpoint_read import inspect_crypto_checkpoint
from stage0_crypto_commit import crypto_checkpoint_key
from stage0_batch import pending_key
from stage0_ingestor import Stage0Error

DAY = "2026-10-13"


class RedisReader:
    def __init__(self, values=None):
        self.values = values or {}
        self.calls = []
    async def call(self, *args):
        self.calls.append(args)
        if args[0] != "GET":
            raise AssertionError("Checkpoint inspection attempted to modify Redis")
        return self.values.get(args[1])


def good_checkpoint():
    return json.dumps({
        "version": "V6.5.1",
        "utc_date": DAY,
        "completed_symbols": ["BTC"],
        "provider": "HYPERLIQUID_NATIVE_PERP",
        "last_verified_run_id": "STAGE0-20261013-V6.5.1",
        "last_verified_symbol": "BTC",
        "last_verified_ohlcv_sha256": "f" * 64,
        "last_verified_at_utc": "2026-10-13T08:51:22+00:00",
        "status": "COMMITTED_VERIFY_PASS",
    })


class InspectCheckpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_verified_btc_does_only_two_redis_gets(self):
        redis = RedisReader({crypto_checkpoint_key(DAY): good_checkpoint()})
        out = await inspect_crypto_checkpoint(redis, day=DAY, symbol="BTC")
        self.assertEqual(out["checkpoint_status"], "COMMITTED_VERIFY_PASS")
        self.assertTrue(out["checkpoint_found"])
        self.assertTrue(out["symbol_checkpoint_verified"])
        self.assertFalse(out["pending_reconciliation"])
        self.assertEqual(out["last_verified_run_id"],"STAGE0-20261013-V6.5.1")
        self.assertFalse(out["sheet_writes_performed"])
        self.assertFalse(out["checkpoint_advanced"])
        self.assertFalse(out["ranking_permitted"])
        self.assertFalse(out["execution_permitted"])
        self.assertEqual(redis.calls, [
            ("GET", crypto_checkpoint_key(DAY)),
            ("GET", pending_key(DAY)),
        ])

    async def test_global_pending_forces_false_even_if_btc_completed(self):
        redis = RedisReader({
            crypto_checkpoint_key(DAY):good_checkpoint(),
            pending_key(DAY):'{"status":"PREPARED"}',
        })
        out = await inspect_crypto_checkpoint(redis, day=DAY, symbol="BTC")
        self.assertTrue(out["pending_reconciliation"])
        self.assertTrue(out["symbol_completed"])
        self.assertFalse(out["symbol_checkpoint_verified"])

    async def test_missing_checkpoint_does_not_imply_success(self):
        out = await inspect_crypto_checkpoint(RedisReader(), day=DAY)
        self.assertFalse(out["checkpoint_found"])
        self.assertFalse(out["symbol_checkpoint_verified"])

    async def test_mismatched_checkpoint_rejected(self):
        cp = json.loads(good_checkpoint())
        cp["provider"] = "FMP"
        r=RedisReader({crypto_checkpoint_key(DAY):json.dumps(cp)})
        with self.assertRaisesRegex(Stage0Error,"CHECKPOINT_INVALID"):
            await inspect_crypto_checkpoint(r, day=DAY)

    async def test_unknown_symbol_and_invalid_day_no_redis_calls(self):
        redis=RedisReader()
        for sym,day in (("WRONG",DAY),("BTC","2026-10-1"),("BTC","2099-01-01")):
            with self.assertRaises(Stage0Error):
                await inspect_crypto_checkpoint(redis, symbol=sym, day=day)
        self.assertEqual(redis.calls,[])


if __name__ == "__main__":
    unittest.main()
