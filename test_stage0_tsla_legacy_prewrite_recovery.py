import asyncio
import json
import unittest
from datetime import datetime,timezone,timedelta
from unittest.mock import patch

from stage0_tsla_legacy_prewrite_recovery import (
    old_history_is_immutable,recover_tsla_prewrite,DAY,SYMBOL,RUN_ID,
)
from stage0_ingestor import Stage0Error

UTC=timezone.utc
CREATED=datetime(2026,10,15,12,12,13,tzinfo=UTC)
NOW=datetime(2026,10,15,12,29,30,tzinfo=UTC)

def old_rows():
    return [
        dict(canonical_symbol=SYMBOL,source="FMP",
             ranking_run_id=("STAGE0-20261012-V6.5.1" if i<299
                             else "STAGE0-20261014-V6.5.1"),
             trading_date=("2026-10-12" if i==299 else "2026-10-13")
                          if i>=299 else "2025-08-%03d"%i,
             close="305.11",quality_status="RECEIVED_UNVERIFIED",
             fetched_at_utc="2026-10-12T13:14:40+00:00")
        for i in range(301)
    ]

class FakeRedis:
    def __init__(self):
        self.journal=json.dumps(dict(status="PREPARED",run_id=RUN_ID,
                         symbols=["TSLA"],created_at=CREATED.isoformat()))
        self.cp=json.dumps(dict(version="V6.5.1",utc_date=DAY,
                            completed_symbols=["SPY","GE"]))
        self.operations=[]
    async def call(self,*args):
        self.operations.append(args[0])
        if args[0]!="GET":
            raise AssertionError("Unexpected mutation")
        if args[1].endswith(":pending"):return self.journal
        if args[1].endswith(":checkpoint:"+DAY):return self.cp
        raise AssertionError(args)
    async def eval(self,script,keys,vals):
        self.operations.append("EVAL")
        if vals==["lock",self.journal,self.cp]:
            self.journal=None
            return 1
        return 0

class FakeCoordinator:
    def __init__(self,redis):self.redis=redis
    async def acquire(self):return "lock"
    async def release(self,token):self.redis.operations.append("RELEASE")

class RecoveryTest(unittest.TestCase):
    def test_301_old_bars_safe(self):
        self.assertTrue(old_history_is_immutable(old_rows(),[],CREATED))
    def test_snapshot_current_blocks(self):
        self.assertFalse(old_history_is_immutable(old_rows(),[{"run_id":RUN_ID}],CREATED))
    def test_today_row_or_timestamp_blocks(self):
        rows=old_rows();rows[100]["ranking_run_id"]=RUN_ID
        self.assertFalse(old_history_is_immutable(rows,[],CREATED))
        rows=old_rows();rows[100]["fetched_at_utc"]=CREATED.isoformat()
        self.assertFalse(old_history_is_immutable(rows,[],CREATED))
    def test_stale_prior_date_invalid(self):
        rows=old_rows();rows[0]["ranking_run_id"]="STAGE0-20261013-V6.5.1"
        self.assertFalse(old_history_is_immutable(rows,[],CREATED))
    def test_atomic_cas_once_no_market_sheet_writes(self):
        fake=FakeRedis()
        with patch("stage0_tsla_legacy_prewrite_recovery.Stage0BatchCoordinator",FakeCoordinator):
            result=asyncio.run(recover_tsla_prewrite(fake,now=NOW,
                                            sheet_reader=lambda:(old_rows(),[])))
        self.assertEqual(result["status"],"TSLA_PREWRITE_ABORTED_VERIFIED")
        self.assertFalse(result["sheet_writes_performed"])
        self.assertFalse(result["checkpoint_advanced"])
        self.assertEqual(fake.operations.count("EVAL"),1)
        self.assertIsNone(fake.journal)
    def test_recent_lease_blocks_before_sheet_reads(self):
        fake=FakeRedis()
        def explode():raise AssertionError("Should not read sheets")
        with patch("stage0_tsla_legacy_prewrite_recovery.Stage0BatchCoordinator",FakeCoordinator):
            with self.assertRaisesRegex(Stage0Error,"Lease grace"):
                asyncio.run(recover_tsla_prewrite(fake,now=CREATED+timedelta(minutes=4),
                                               sheet_reader=explode))
        self.assertNotIn("EVAL",fake.operations)
    def test_scope_wrong_symbol_blocks(self):
        fake=FakeRedis()
        fake.journal=fake.journal.replace('"TSLA"','"CVX"')
        with patch("stage0_tsla_legacy_prewrite_recovery.Stage0BatchCoordinator",FakeCoordinator):
            with self.assertRaises(Stage0Error):
                asyncio.run(recover_tsla_prewrite(fake,now=NOW,
                                               sheet_reader=lambda:(old_rows(),[])))
        self.assertNotIn("EVAL",fake.operations)
    def test_corrupt_snapshot_not_atomic_deleted(self):
        fake=FakeRedis()
        with patch("stage0_tsla_legacy_prewrite_recovery.Stage0BatchCoordinator",FakeCoordinator):
            with self.assertRaises(Stage0Error):
                asyncio.run(recover_tsla_prewrite(fake,now=NOW,
                    sheet_reader=lambda:(old_rows(),[{"run_id":RUN_ID}])))
        self.assertNotIn("EVAL",fake.operations)
    def test_wrong_day_blocks(self):
        fake=FakeRedis()
        with patch("stage0_tsla_legacy_prewrite_recovery.Stage0BatchCoordinator",FakeCoordinator):
            with self.assertRaises(Stage0Error):
                asyncio.run(recover_tsla_prewrite(fake,
                    now=datetime(2026,10,16,13,tzinfo=UTC),
                    sheet_reader=lambda:(old_rows(),[])))
        self.assertNotIn("EVAL",fake.operations)

if __name__=="__main__":unittest.main()
