import unittest
from datetime import datetime,timezone
from stage0_fmp_preclose_drain import preclose, drain_fmp_preclose, DrainHalt
from unittest.mock import patch
import asyncio

UTC=timezone.utc
T=datetime(2026,10,14,12,0,tzinfo=UTC)
LATE=datetime(2026,10,14,20,0,tzinfo=UTC)

class TestPrecloseDrain(unittest.TestCase):
    def test_utc_et_cutoff(self):
        self.assertTrue(preclose(T))
        self.assertTrue(preclose(datetime(2026,12,14,14,0,tzinfo=UTC)))
        self.assertFalse(preclose(LATE))
        self.assertFalse(preclose(datetime(2026,12,14,21,0,tzinfo=UTC)))
        with self.assertRaisesRegex(DrainHalt,"NAIVE_CLOCK"):
            preclose(datetime(2026,10,14,12,0))

    def test_verified_checkpoints_continue_until_queue_empty(self):
        saved=[]
        queue=["NVDA","MSFT"]
        def state():
            return dict(version="V6.5.1",utc_date="2026-10-14",
                pending_reconciliation=False,
                quota=dict(remaining=200,blocked=False),
                checkpoint=dict(completed_symbols=saved[:]))
        async def status(): return state()
        async def plan():
            return dict(version="V6.5.1",utc_date="2026-10-14",
                pending_reconciliation=False,next_symbols=queue[:1])
        async def step():
            sym=queue.pop(0)
            saved.append(sym)
            return dict(daily_symbol=sym,commit_status="COMMITTED",
                        verify_status="PASS",checkpoint_status="COMMITTED_VERIFY_PASS")
        out=asyncio.run(drain_fmp_preclose(
            status_fn=status,plan_fn=plan,step_fn=step,now_fn=lambda:T,
            max_steps=5))
        self.assertEqual(out["committed"],["NVDA","MSFT"])
        self.assertEqual(out["status"],"FMP_QUEUE_EMPTY")
        self.assertFalse(out["ranking_permitted"])
        self.assertFalse(out["trades_written"])

    def test_fmp_402_defer_advances_queue_not_checkpoint(self):
        queue=["AAOI","SPY"];saved=[]
        async def status(): return dict(version="V6.5.1",utc_date="2026-10-14",
            pending_reconciliation=False,quota=dict(remaining=200,blocked=False),
            checkpoint=dict(completed_symbols=saved[:]))
        async def plan(): return dict(version="V6.5.1",utc_date="2026-10-14",
            pending_reconciliation=False,next_symbols=queue[:1])
        async def step():
            sym=queue.pop(0)
            if sym=="AAOI":return dict(status="PROVIDER_DEFERRED",symbol=sym,checkpoint_advanced=False)
            saved.append(sym);return dict(daily_symbol=sym,commit_status="COMMITTED",
                verify_status="PASS",checkpoint_status="COMMITTED_VERIFY_PASS")
        out=asyncio.run(drain_fmp_preclose(status_fn=status,plan_fn=plan,
                        step_fn=step,now_fn=lambda:T,max_steps=4))
        self.assertEqual(out["deferred"],["AAOI"])
        self.assertEqual(out["committed"],["SPY"])

    def test_retryable_stops_without_repeating_same_symbol(self):
        count=[0]
        async def status():return dict(version="V6.5.1",utc_date="2026-10-14",
            pending_reconciliation=False,quota=dict(remaining=200,blocked=False),
            checkpoint=dict(completed_symbols=[]))
        async def plan():return dict(version="V6.5.1",utc_date="2026-10-14",
            pending_reconciliation=False,next_symbols=["NVDA"])
        async def step():
            count[0]+=1
            return dict(status="SOURCE_RETRYABLE",symbol="NVDA",checkpoint_advanced=False)
        out=asyncio.run(drain_fmp_preclose(status_fn=status,plan_fn=plan,
                          step_fn=step,now_fn=lambda:T))
        self.assertEqual(out["status"],"RETRYABLE_SOURCE_NVDA")
        self.assertEqual(count[0],1)

    def test_no_commits_on_pending_journal(self):
        async def status():return dict(version="V6.5.1",utc_date="2026-10-14",
                                        pending_reconciliation=True)
        with self.assertRaisesRegex(DrainHalt,"RECONCILIATION_REQUIRED"):
            asyncio.run(drain_fmp_preclose(status_fn=status,
                plan_fn=None,step_fn=None,now_fn=lambda:T))

    def test_no_network_when_too_late(self):
        out=asyncio.run(drain_fmp_preclose(
            status_fn=None,plan_fn=None,step_fn=None,now_fn=lambda:LATE))
        self.assertEqual(out["status"],"NOT_PRE_CUTOFF")

    def test_fail_closed_unverified_readback(self):
        async def status():return dict(version="V6.5.1",utc_date="2026-10-14",
            pending_reconciliation=False,quota=dict(remaining=200,blocked=False),
            checkpoint=dict(completed_symbols=[]))
        async def plan():return dict(version="V6.5.1",utc_date="2026-10-14",
            pending_reconciliation=False,next_symbols=["TSLA"])
        async def step():return dict(daily_symbol="TSLA",commit_status="COMMITTED",
                                  verify_status="NOT_CHECKED",checkpoint_status="UNKNOWN")
        with self.assertRaisesRegex(DrainHalt,"COMMIT_OR_READBACK_NOT_VERIFIED"):
            asyncio.run(drain_fmp_preclose(status_fn=status,
                plan_fn=plan,step_fn=step,now_fn=lambda:T))

if __name__=="__main__":unittest.main()
