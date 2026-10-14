"""Offline V6.5.1 Stage0 runtime evidence bridge tests."""
import json
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from stage0_runtime_evidence import sync_stage0_evidence
from stage0_crypto_coinbase_commit import spot_checkpoint_key
from stage0_batch import state_key
from stage0_ingestor import Stage0Error

DAY=datetime.now(timezone.utc).date().isoformat()
class FakeRedis:
    def __init__(self,*,cp=None,fmp=None,pending=None,used=12):
        self.state={spot_checkpoint_key(DAY):json.dumps(cp) if cp is not None else None,
                    state_key(DAY):json.dumps(fmp) if fmp is not None else None,
                    "askliam:v651:stage0:pending":pending}
        self.lock=None
        self.used=used
    async def call(self,*args):
        if args[0]=="SET":
            if self.lock is not None: return None
            self.lock=args[2]
            return "OK"
        if args[0]=="GET":
            return self.state.get(args[1])
        raise AssertionError(args)
    async def quota(self):
        return {"utc_date":DAY, "used":self.used,"remaining":240-self.used,
                "blocked":False,"reason":None}
    async def eval(self,code,keys,args):
        if len(keys)==1 and self.lock==args[0]:
            self.lock=None
            return 1
        return 0


def checkpoint(symbols):
    return {"version":"V6.5.1","utc_date":DAY,
            "status":"COMMITTED_VERIFY_PASS","provider":"COINBASE_EXCHANGE_SPOT_USD",
            "completed_symbols":symbols}


class MirrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_verified_spot_mirrored_without_ranking(self):
        redis=FakeRedis(cp=checkpoint(["BTC","ETH"]))
        def sheet(proof):
            self.assertEqual(proof["spot_verified"],["BTC","ETH"])
            self.assertEqual(proof["used"],12)
            return {"sync_status":"COMMITTED_VERIFY_PASS",
                    "spot_verified":proof["spot_verified"],
                    "snapshot_count":2,"evidence_rows":8,
                    "ranking_permitted":False,"execution_permitted":False}
        with patch("stage0_runtime_evidence._sheet_evidence_sync",new=sheet):
            result=await sync_stage0_evidence(redis)
        self.assertEqual(result["sync_status"],"COMMITTED_VERIFY_PASS")
        self.assertEqual(result["spot_verified"],["BTC","ETH"])
        self.assertEqual(result["fmp_remaining"],228)
        self.assertIsNone(redis.lock)

    async def test_fmp_checkpoint_is_mirrored_without_promoting_scores(self):
        fmp={"version":"V6.5.1","utc_date":DAY,
             "status":"COMMITTED_VERIFY_PASS",
             "completed_symbols":["SPY","TSLA"]}
        redis=FakeRedis(cp=checkpoint(["BTC","ETH"]),fmp=fmp,used=8)
        def sheet(proof):
            self.assertEqual(proof["spot_verified"],["BTC","ETH"])
            self.assertEqual(proof["fmp_verified"],["SPY","TSLA"])
            return {"sync_status":"COMMITTED_VERIFY_PASS","snapshot_count":4,
                    "spot_verified":proof["spot_verified"],
                    "fmp_verified":proof["fmp_verified"],
                    "unique_acquired":4,"evidence_rows":12,
                    "ranking_permitted":False,"execution_permitted":False}
        with patch("stage0_runtime_evidence._sheet_evidence_sync",new=sheet):
            out=await sync_stage0_evidence(redis)
        self.assertEqual(out["fmp_verified"],["SPY","TSLA"])
        self.assertEqual(out["unique_acquired"],4)
        self.assertEqual(out["fmp_used"],8)
        self.assertEqual(out["fmp_remaining"],232)
        self.assertFalse(out["ranking_permitted"])

    async def test_bad_fmp_checkpoint_blocks_sheet_reconciliation(self):
        fmp={"version":"V6.5.1","utc_date":DAY,
             "status":"COMMITTED_VERIFY_PASS",
             "completed_symbols":["SPY","SPY"]}
        redis=FakeRedis(cp=checkpoint(["BTC"]),fmp=fmp)
        with patch("stage0_runtime_evidence._sheet_evidence_sync") as writer:
            with self.assertRaisesRegex(Stage0Error,"RECONCILIATION_REQUIRED"):
                await sync_stage0_evidence(redis)
        writer.assert_not_called()

    async def test_pending_global_journal_blocks_sheet_writes(self):
        redis=FakeRedis(cp=checkpoint(["BTC"]),pending="prepared")
        with patch("stage0_runtime_evidence._sheet_evidence_sync") as writer:
            with self.assertRaisesRegex(Stage0Error,"RECONCILIATION_REQUIRED"):
                await sync_stage0_evidence(redis)
        writer.assert_not_called()
        self.assertIsNone(redis.lock)

    async def test_corrupt_spot_checkpoint_fails_closed(self):
        redis=FakeRedis(cp=checkpoint(["BTC","BTC"]))
        with patch("stage0_runtime_evidence._sheet_evidence_sync") as writer:
            with self.assertRaisesRegex(Stage0Error,"RECONCILIATION_REQUIRED"):
                await sync_stage0_evidence(redis)
        writer.assert_not_called()
