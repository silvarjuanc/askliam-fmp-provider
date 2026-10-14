import unittest
from datetime import datetime, timezone
from stage0_equity_preclose_bootstrap import (
    bootstrap_priority_equities, precutoff_gate, BootstrapHalt,
)
UTC=timezone.utc
NOW=datetime(2026,10,14,11,0,tzinfo=UTC)
DAY="2026-10-14"
VERSION="V6.5.1"


class Harness:
    def __init__(self):
        self.done=set()
        self.calls=[]
        self.pending=False
        self.steponce=None
    async def status(self):
        self.calls.append("status")
        return {"version":VERSION,"utc_date":DAY,
                "checkpoint":{"completed_symbols":list(self.done)},
                "pending_reconciliation":self.pending}
    async def plan(self):
        self.calls.append("plan")
        for symbol in ("SPY","TSLA","AAPL"):
            if symbol not in self.done:
                return {"version":VERSION,"utc_date":DAY,
                        "pending_reconciliation":False,"next_symbols":[symbol]}
        return {"version":VERSION,"utc_date":DAY,
                "pending_reconciliation":False,"next_symbols":[]}
    async def step(self):
        self.calls.append("step")
        symbol=next(s for s in ("SPY","TSLA","AAPL") if s not in self.done)
        if self.steponce:
            return self.steponce
        self.done.add(symbol)
        return {"daily_symbol":symbol,"commit_status":"COMMITTED",
                "verify_status":"PASS","checkpoint_status":"COMMITTED_VERIFY_PASS"}


class Tests(unittest.IsolatedAsyncioTestCase):
    async def test_three_safe_sheet_commits_and_readbacks(self):
        h=Harness()
        r=await bootstrap_priority_equities(
            status_fn=h.status,plan_fn=h.plan,step_fn=h.step,now=NOW)
        self.assertEqual(r["symbols_committed"],["SPY","TSLA","AAPL"])
        self.assertEqual(r["status"],"STAGE0_PRIORITY_COMMITTED_VERIFY_PASS")
        self.assertFalse(r["ranking_permitted"])
        self.assertFalse(r["trades_written"])
        self.assertEqual(h.calls.count("step"),3)

    async def test_idempotent_preverified(self):
        h=Harness();h.done={"SPY","TSLA"}
        r=await bootstrap_priority_equities(status_fn=h.status,plan_fn=h.plan,
                                             step_fn=h.step,now=NOW)
        self.assertEqual(r["symbols_committed"],["AAPL"])
        self.assertEqual(r["already_verified"],["SPY","TSLA"])

    async def test_pending_redis_journal_blocks(self):
        h=Harness();h.pending=True
        with self.assertRaisesRegex(BootstrapHalt,"RECONCILIATION_REQUIRED"):
            await bootstrap_priority_equities(status_fn=h.status,
                      plan_fn=h.plan,step_fn=h.step,now=NOW)
        self.assertNotIn("step",h.calls)

    async def test_weak_sheet_confirmation_never_promotes(self):
        h=Harness();h.steponce={"daily_symbol":"SPY","commit_status":"COMMITTED",
                                 "verify_status":"NOT_VERIFIED",
                                 "checkpoint_status":"COMMITTED_VERIFY_PASS"}
        with self.assertRaisesRegex(BootstrapHalt,"SHEET_COMMIT_NOT_VERIFIED"):
            await bootstrap_priority_equities(status_fn=h.status,
                      plan_fn=h.plan,step_fn=h.step,now=NOW)

    async def test_queue_mismatch_cannot_write(self):
        h=Harness()
        async def bad_plan():
            return {"version":VERSION,"utc_date":DAY,
                    "pending_reconciliation":False,"next_symbols":["AAOI"]}
        with self.assertRaisesRegex(BootstrapHalt,"PRIORITY_QUEUE_ORDER_NOT_VERIFIED"):
            await bootstrap_priority_equities(status_fn=h.status,
                      plan_fn=bad_plan,step_fn=h.step,now=NOW)
        self.assertNotIn("step",h.calls)

    async def test_strict_cutoff_prevents_network(self):
        h=Harness()
        late=datetime(2026,10,14,20,0,tzinfo=UTC)
        self.assertFalse(precutoff_gate(late))
        r=await bootstrap_priority_equities(status_fn=h.status,
                                             plan_fn=h.plan,step_fn=h.step,now=late)
        self.assertEqual(r["status"],"NOT_PRE_CUTOFF")
        self.assertEqual(h.calls,[])


if __name__=="__main__":
    unittest.main()
