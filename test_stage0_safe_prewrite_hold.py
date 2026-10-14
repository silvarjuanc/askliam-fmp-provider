import asyncio
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from stage0_ingestor import Stage0Error
from stage0_batch import Stage0BatchCoordinator, transient_hold_key, TRANSIENT_HOLD_SECONDS
from stage0_daily import plan_from_universe
from stage0_fmp_preclose_drain import drain_fmp_preclose

DAY="2026-10-14"
NOW=datetime(2026,10,14,13,0,tzinfo=timezone.utc)

class TestPrewriteProvenance(unittest.TestCase):
    def fake_stage0(self, attempted, error):
        fake=Mock()
        fake.write_attempted=attempted
        fake._open.return_value=object()
        fake.run.side_effect=error
        return fake

    def test_transient_initial_read_error_eligible_for_prewrite_abort(self):
        fake=self.fake_stage0(False, Stage0Error("SHEET_READ_FAILED","Raw_OHLCV: APIError"))
        batch=Stage0BatchCoordinator(object(), spreadsheet_id="sheet-id")
        with patch("stage0_batch.Stage0_Daily_Ingestor",return_value=fake),patch(
            "stage0_batch.ExistingFMPStage0Adapter",return_value=object()):
            with self.assertRaises(Stage0Error) as ctx:
                batch.run_blocking(["JPM"],"STAGE0-20261014-V6.5.1")
        self.assertEqual(ctx.exception.status,"SAFE_PREWRITE_RETRYABLE")
        self.assertEqual(ctx.exception.detail,"SHEET_READ_FAILED")

    def test_postwrite_auth_error_still_halts_without_journal_abort(self):
        fake=self.fake_stage0(True, Stage0Error("SHEET_READ_FAILED","Raw_OHLCV: APIError"))
        batch=Stage0BatchCoordinator(object(), spreadsheet_id="sheet-id")
        with patch("stage0_batch.Stage0_Daily_Ingestor",return_value=fake),patch(
            "stage0_batch.ExistingFMPStage0Adapter",return_value=object()):
            with self.assertRaises(Stage0Error) as ctx:
                batch.run_blocking(["JPM"],"STAGE0-20261014-V6.5.1")
        self.assertEqual(ctx.exception.status,"SHEET_READ_FAILED")

    def test_prewrite_schema_mismatch_never_auto_skips(self):
        fake=self.fake_stage0(False, Stage0Error("SCHEMA_FAILED","Bad header"))
        batch=Stage0BatchCoordinator(object(), spreadsheet_id="sheet-id")
        with patch("stage0_batch.Stage0_Daily_Ingestor",return_value=fake),patch(
            "stage0_batch.ExistingFMPStage0Adapter",return_value=object()):
            with self.assertRaises(Stage0Error) as ctx:
                batch.run_blocking(["JPM"],"STAGE0-20261014-V6.5.1")
        self.assertEqual(ctx.exception.status,"SCHEMA_FAILED")

    def test_atomically_mark_transient_and_clear_only_original_prepared(self):
        class FakeRedis:
            def __init__(self):
                self.j=json.dumps({"status":"PREPARED","run_id":"STAGE0-20261014-V6.5.1",
                                   "symbols":["JPM"]})
                self.calls=[]
            async def call(self,*args):
                self.calls.append(("call",args))
                return self.j
            async def eval(self,script,keys,args):
                self.calls.append(("eval",keys,args))
                assert keys[0].endswith(":exclusive")
                assert keys[1].endswith(":pending")
                assert keys[2] == transient_hold_key(DAY)
                assert args[0]=="lease" and args[1]==self.j
                assert args[-1]==TRANSIENT_HOLD_SECONDS
                return 1
        redis=FakeRedis()
        batch=Stage0BatchCoordinator(redis,spreadsheet_id="sid")
        with patch("stage0_batch.utc_day",return_value=DAY):
            asyncio.run(batch.mark_transient_prewrite(["JPM"],"SHEET_READ_FAILED","lease"))
        self.assertEqual(len(redis.calls),2)
        self.assertEqual(redis.calls[1][0],"eval")

    def test_fail_closed_on_unknown_error_code(self):
        batch=Stage0BatchCoordinator(object(),spreadsheet_id="sid")
        with self.assertRaises(Stage0Error):
            asyncio.run(batch.mark_transient_prewrite(["JPM"],"VERIFICATION_FAILED","lease"))

    def test_transient_hold_is_not_deferred_or_completed(self):
        entries=[{"symbol":"AAPL","route":"FMP_CANDIDATE"},
                 {"symbol":"JPM","route":"FMP_CANDIDATE"},
                 {"symbol":"MSFT","route":"FMP_CANDIDATE"}]
        state={"utc_date":DAY,"checkpoint":{"completed_symbols":["AAPL"]},
               "deferred_today":{},"transient_prewrite_today":{"JPM":"SHEET_READ_FAILED"},
               "quota":{"remaining":100},"pending_reconciliation":False}
        result=plan_from_universe(entries,state)
        self.assertEqual(result["next_symbols"],["MSFT"])
        self.assertEqual(result["fmp_transient_prewrite_held_symbols"],["JPM"])
        self.assertEqual(result["fmp_deferred_today"],0)
        self.assertEqual(result["fmp_completed_today"],1)

    def test_drain_safe_prewrite_hold_advances_after_verifying_redis(self):
        queue=["JPM","MSFT"]
        held={}
        completed=[]
        async def status():return {
            "version":"V6.5.1","utc_date":DAY,"pending_reconciliation":False,
            "quota":{"remaining":200,"blocked":False},
            "transient_prewrite_today":held.copy(),
            "checkpoint":{"completed_symbols":completed[:]},
        }
        async def plan():return {"version":"V6.5.1","utc_date":DAY,
            "pending_reconciliation":False,"next_symbols":queue[:1]}
        async def step():
            symbol=queue.pop(0)
            if symbol=="JPM":
                held[symbol]="SHEET_READ_FAILED"
                return {"status":"SOURCE_RETRYABLE","symbol":symbol,
                        "checkpoint_advanced":False,"temporarily_held":True}
            completed.append(symbol)
            return {"daily_symbol":symbol,"commit_status":"COMMITTED",
                    "verify_status":"PASS","checkpoint_status":"COMMITTED_VERIFY_PASS"}
        result=asyncio.run(drain_fmp_preclose(
            status_fn=status,plan_fn=plan,step_fn=step,
            now_fn=lambda:NOW,max_steps=4,
        ))
        self.assertEqual(result["transient_prewrite_held"],["JPM"])
        self.assertEqual(result["committed"],["MSFT"])
        self.assertEqual(result["status"],"FMP_QUEUE_EMPTY")

if __name__=="__main__":unittest.main()
