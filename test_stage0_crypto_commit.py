"""No-network no-Sheets tests for opt-in V6.5.1 crypto guarded commits."""
from datetime import datetime, timezone
from unittest.mock import patch
import asyncio
import json
import unittest

from stage0_crypto_commit import (
    commit_one_native_perp, crypto_checkpoint_key, confirmation_for,
    feature_guard, GuardedCryptoIngestor,
)
from stage0_batch import LOCK_KEY, pending_key
from stage0_ingestor import Stage0Error
from test_stage0_crypto_ingest_prepare import prepare, RUN
from test_stage0_crypto_sheet_preflight import captured_state

NOW = datetime(2026,10,12,15,0,tzinfo=timezone.utc)
DAY = "2026-10-12"


class FakeRedis:
    def __init__(self):
        self.kv={}
        self.called=[]
        self.eval_calls=[]
    async def call(self,*args):
        self.called.append(args)
        cmd=args[0]
        if cmd=="GET":
            return self.kv.get(args[1])
        if cmd=="SET":
            key,val=args[1:3]
            if "NX" in args[3:] and key in self.kv:
                return None
            self.kv[key]=val
            return "OK"
        raise AssertionError("Unexpected command: "+str(args))
    async def eval(self,script,keys,args):
        self.eval_calls.append((script,keys,args))
        if len(keys)==1:  # shared exclusive lock release
            if self.kv.get(keys[0])==args[0]:
                self.kv.pop(keys[0],None)
                return 1
            return 0
        if len(keys)==3:
            lock,pending,checkpoint=keys
            if self.kv.get(lock)!=args[0] or self.kv.get(pending)!=args[1]:
                return 0
            current=self.kv.get(checkpoint)
            if args[2]=="1":
                if current!=args[3]:
                    return 0
            elif current is not None:
                return 0
            self.kv[checkpoint]=args[4]
            self.kv.pop(pending,None)
            return 1
        raise AssertionError("Unexpected Lua keys")


class CommitGuards(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_feature_never_touches_redis(self):
        redis=FakeRedis()
        with patch.dict("os.environ", {"ASKLIAM_CRYPTO_WRITE_ENABLED":"false"}):
            with self.assertRaisesRegex(Stage0Error,"CRYPTO_WRITE_DISABLED"):
                await commit_one_native_perp(
                    "BTC",confirmation_for("BTC"),redis=redis,client=None,now=NOW
                )
        self.assertEqual(redis.called, [])

    async def test_wrong_confirmation_no_provider_or_journal(self):
        redis=FakeRedis()
        with patch.dict("os.environ", {"ASKLIAM_CRYPTO_WRITE_ENABLED":"true", "ASKLIAM_CRYPTO_WRITE_SYMBOL":"BTC", "ASKLIAM_CRYPTO_WRITE_UTC_DAY":"2026-10-12"}):
            with self.assertRaisesRegex(Stage0Error,"EXPLICIT_CONFIRMATION_REQUIRED"):
                await commit_one_native_perp(
                    "BTC","WRONG",redis=redis,client=None,now=NOW
                )
        self.assertEqual(redis.called, [])

    async def test_crypto_scope_rejects_other_symbol_before_network(self):
        redis=FakeRedis()
        with patch.dict("os.environ", {
            "ASKLIAM_CRYPTO_WRITE_ENABLED":"true",
            "ASKLIAM_CRYPTO_WRITE_SYMBOL":"BTC",
            "ASKLIAM_CRYPTO_WRITE_UTC_DAY":"2026-10-12",
        }):
            with self.assertRaisesRegex(Stage0Error,"CRYPTO_WRITE_SCOPE_DENIED"):
                await commit_one_native_perp(
                    "ETH",confirmation_for("ETH"),redis=redis,
                    client=None,now=NOW
                )
        self.assertEqual(redis.called, [])

    async def test_crypto_scope_expires_at_utc_day(self):
        redis=FakeRedis()
        with patch.dict("os.environ", {
            "ASKLIAM_CRYPTO_WRITE_ENABLED":"true",
            "ASKLIAM_CRYPTO_WRITE_SYMBOL":"BTC",
            "ASKLIAM_CRYPTO_WRITE_UTC_DAY":"2026-10-11",
        }):
            with self.assertRaisesRegex(Stage0Error,"CRYPTO_WRITE_DAY_EXPIRED"):
                await commit_one_native_perp(
                    "BTC",confirmation_for("BTC"),redis=redis,
                    client=None,now=NOW
                )
        self.assertEqual(redis.called, [])

    async def test_one_committed_then_repeated_is_blocked(self):
        redis=FakeRedis()
        frames,report=prepare()
        writes=[]
        async def mock_acquire(symbol,*,client,now,run_id):
            self.assertEqual(symbol,"BTC")
            self.assertEqual(run_id,RUN)
            return frames, report
        def mock_writer(f,s,r):
            writes.append((s,r,len(f["Raw_OHLCV"])))
            return {
                "commit_status":"COMMITTED","verify_status":"PASS",
                "final_status":"COMMITTED",
                "tabs":{"Raw_OHLCV":{"status":"PASS"},
                        "Market_Data_Snapshot":{"status":"PASS"},
                        "FMP_Runtime_State":{"status":"PASS"}},
            }
        with patch.dict("os.environ", {"ASKLIAM_CRYPTO_WRITE_ENABLED":"true", "ASKLIAM_CRYPTO_WRITE_SYMBOL":"BTC", "ASKLIAM_CRYPTO_WRITE_UTC_DAY":"2026-10-12"}):
            with patch("stage0_crypto_commit.acquire_native_perp_frames",
                       new=mock_acquire):
                out=await commit_one_native_perp(
                    "BTC",confirmation_for("BTC"),redis=redis,client=None,
                    now=NOW,writer=mock_writer
                )
                self.assertEqual(out["status"],"COMMITTED_VERIFY_PASS")
                self.assertTrue(out["crypto_checkpoint_updated"])
                self.assertFalse(out["ranking_permitted"])
                self.assertFalse(out["execution_permitted"])
                self.assertFalse(out["fmp_requests_performed"])
                self.assertEqual(writes,[("BTC",RUN,150)])
                self.assertNotIn(pending_key(DAY),redis.kv)
                checkpoint=json.loads(redis.kv[crypto_checkpoint_key(DAY)])
                self.assertEqual(checkpoint["completed_symbols"],["BTC"])
                with self.assertRaisesRegex(Stage0Error,"ALREADY_COMMITTED_TODAY"):
                    await commit_one_native_perp(
                        "BTC",confirmation_for("BTC"),redis=redis,client=None,
                        now=NOW,writer=mock_writer
                    )
        self.assertEqual(len(writes),1)
        self.assertNotIn(LOCK_KEY,redis.kv)

    async def test_sheet_write_failure_retains_global_journal(self):
        redis=FakeRedis()
        frames,report=prepare()
        async def mock_acquire(symbol,*,client,now,run_id):
            return frames,report
        def broken_writer(f,s,r):
            raise Stage0Error(
                "PARTIAL_COMMIT_UNVERIFIED","Simulated lost Sheets reply"
            )
        with patch.dict("os.environ", {"ASKLIAM_CRYPTO_WRITE_ENABLED":"true", "ASKLIAM_CRYPTO_WRITE_SYMBOL":"BTC", "ASKLIAM_CRYPTO_WRITE_UTC_DAY":"2026-10-12"}):
            with patch("stage0_crypto_commit.acquire_native_perp_frames",
                       new=mock_acquire):
                with self.assertRaisesRegex(Stage0Error,"PARTIAL_COMMIT_UNVERIFIED"):
                    await commit_one_native_perp(
                        "BTC",confirmation_for("BTC"),redis=redis,client=None,
                        now=NOW,writer=broken_writer
                    )
        self.assertIn(pending_key(DAY),redis.kv)
        self.assertNotIn(crypto_checkpoint_key(DAY),redis.kv)
        self.assertNotIn(LOCK_KEY,redis.kv)

    async def test_unverified_writer_result_never_checkpointed(self):
        redis=FakeRedis()
        frames,report=prepare()
        async def mock_acquire(symbol,*,client,now,run_id):
            return frames,report
        with patch.dict("os.environ", {"ASKLIAM_CRYPTO_WRITE_ENABLED":"true", "ASKLIAM_CRYPTO_WRITE_SYMBOL":"BTC", "ASKLIAM_CRYPTO_WRITE_UTC_DAY":"2026-10-12"}):
            with patch("stage0_crypto_commit.acquire_native_perp_frames",
                       new=mock_acquire):
                with self.assertRaisesRegex(Stage0Error,"VERIFICATION_FAILED"):
                    await commit_one_native_perp(
                        "BTC",confirmation_for("BTC"),redis=redis,client=None,
                        now=NOW,
                        writer=lambda f,s,r:{
                            "commit_status":"COMMITTED","verify_status":"FAIL",
                            "final_status":"COMMITTED"
                        }
                    )
        self.assertIn(pending_key(DAY),redis.kv)
        self.assertNotIn(crypto_checkpoint_key(DAY),redis.kv)

    async def test_preexisting_global_pending_blocks_before_source(self):
        redis=FakeRedis()
        redis.kv[pending_key(DAY)] = json.dumps({"status":"PREPARED"})
        with patch.dict("os.environ", {"ASKLIAM_CRYPTO_WRITE_ENABLED":"true", "ASKLIAM_CRYPTO_WRITE_SYMBOL":"BTC", "ASKLIAM_CRYPTO_WRITE_UTC_DAY":"2026-10-12"}):
            with self.assertRaisesRegex(Stage0Error,"RECONCILIATION_REQUIRED"):
                await commit_one_native_perp(
                    "BTC",confirmation_for("BTC"),redis=redis,client=None,now=NOW
                )
        self.assertNotIn(LOCK_KEY,redis.kv)


class WriterSafety(unittest.TestCase):
    def test_preexisting_independent_spot_snapshot_rejected(self):
        frames,report=prepare()
        writer=GuardedCryptoIngestor(frames,"BTC",RUN,spreadsheet_id="offline-only")
        before=captured_state(collision=True)
        with self.assertRaisesRegex(Stage0Error,"SNAPSHOT_COLLISION"):
            writer._plan(before,{
                "Raw_OHLCV":frames["Raw_OHLCV"],
                "Market_Data_Snapshot":frames["Market_Data_Snapshot"],
            })

    def test_no_collision_allows_validated_crypto_frames(self):
        frames,report=prepare()
        writer=GuardedCryptoIngestor(frames,"BTC",RUN,spreadsheet_id="offline-only")
        before=captured_state(fmp=True)
        plans,expected=writer._plan(before,{
            "Raw_OHLCV":frames["Raw_OHLCV"],
            "Market_Data_Snapshot":frames["Market_Data_Snapshot"],
            "FMP_Runtime_State": __import__("pandas").DataFrame(),
        })
        self.assertEqual(len(plans["Raw_OHLCV"]["inserts"]),150)
        self.assertEqual(len(plans["Market_Data_Snapshot"]["inserts"]),1)
        self.assertEqual(len(plans["FMP_Runtime_State"]["inserts"]),0)


if __name__=="__main__":
    unittest.main()
