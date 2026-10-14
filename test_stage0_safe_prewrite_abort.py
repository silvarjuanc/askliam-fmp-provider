import asyncio
import unittest
from datetime import datetime,timezone
from unittest.mock import patch

import stage0_batch
from stage0_batch import Stage0BatchCoordinator
from stage0_ingestor import Stage0Error
from stage0_fmp_preclose_drain import drain_fmp_preclose

NOW=datetime(2026,10,14,13,0,tzinfo=timezone.utc)

class FakeIngestor:
    write_started=False
    def __init__(self,**kwargs):pass
    def _open(self):return object()
    def run(self):raise Stage0Error("SHEET_READ_FAILED","no writes")
class FakeIngestorPostwrite(FakeIngestor):
    write_started=True
    def run(self):raise Stage0Error("PARTIAL_COMMIT_UNVERIFIED","readback uncertain")

class SafePrewriteTests(unittest.TestCase):
    def test_prewrite_failure_distinguished_without_hiding(self):
        batch=Stage0BatchCoordinator(object(),spreadsheet_id="TEST_ID")
        with patch.object(stage0_batch,"Stage0_Daily_Ingestor",FakeIngestor), \
             patch.object(stage0_batch,"ExistingFMPStage0Adapter",return_value=object()):
            with self.assertRaises(Stage0Error) as caught:
                batch.run_blocking(["AAOI"],"STAGE0-20261014-V6.5.1")
        self.assertEqual(caught.exception.status,"SAFE_PREWRITE_ABORTABLE")
    def test_partial_commit_never_autoabortable(self):
        batch=Stage0BatchCoordinator(object(),spreadsheet_id="TEST_ID")
        with patch.object(stage0_batch,"Stage0_Daily_Ingestor",FakeIngestorPostwrite), \
             patch.object(stage0_batch,"ExistingFMPStage0Adapter",return_value=object()):
            with self.assertRaises(Stage0Error) as caught:
                batch.run_blocking(["AAOI"],"STAGE0-20261014-V6.5.1")
        self.assertEqual(caught.exception.status,"PARTIAL_COMMIT_UNVERIFIED")

    def test_operational_hold_validated_and_queue_continues(self):
        done=[]
        held={}
        queue=["AAOI","CVX"]
        async def status():
            return {"version":"V6.5.1","utc_date":"2026-10-14",
                    "pending_reconciliation":False,
                    "quota":{"remaining":180,"blocked":False},
                    "checkpoint":{"completed_symbols":done[:]},
                    "deferred_today":dict(held)}
        async def plan():
            return {"version":"V6.5.1","utc_date":"2026-10-14",
                    "pending_reconciliation":False,"next_symbols":queue[:1]}
        async def step():
            s=queue.pop(0)
            if s=="AAOI":
                held[s]="STAGE0_PREWRITE_RUNTIME_RETRYABLE"
                return {"status":"OPERATIONALLY_HELD","symbol":s,
                        "checkpoint_advanced":False,"sheet_writes_performed":False}
            done.append(s)
            return {"daily_symbol":s,"commit_status":"COMMITTED",
                    "verify_status":"PASS","checkpoint_status":"COMMITTED_VERIFY_PASS"}
        out=asyncio.run(drain_fmp_preclose(status_fn=status,plan_fn=plan,
                         step_fn=step,now_fn=lambda:NOW,max_steps=5))
        self.assertEqual(out["deferred"],["AAOI"])
        self.assertEqual(out["committed"],["CVX"])
        self.assertFalse(out["ranking_permitted"])

    def test_unverified_held_state_cannot_be_skipped(self):
        async def status():
            return {"version":"V6.5.1","utc_date":"2026-10-14",
                    "pending_reconciliation":False,
                    "quota":{"remaining":180,"blocked":False},
                    "checkpoint":{"completed_symbols":[]},"deferred_today":{}}
        async def plan():
            return {"version":"V6.5.1","utc_date":"2026-10-14",
                    "pending_reconciliation":False,"next_symbols":["AAOI"]}
        async def step():
            return {"status":"OPERATIONALLY_HELD","symbol":"AAOI",
                    "checkpoint_advanced":False}
        from stage0_fmp_preclose_drain import DrainHalt
        with self.assertRaises(DrainHalt):
            asyncio.run(drain_fmp_preclose(status_fn=status,plan_fn=plan,
                       step_fn=step,now_fn=lambda:NOW,max_steps=2))

if __name__=="__main__":unittest.main()
