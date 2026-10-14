"""Explicit one-day V6.5.1 FMP Stage0 bootstrap for three verified equities.

Uses the *existing* Stage0 planner and authenticated-service internal transaction
with global Redis lease, PREPARED journal and mandatory Google Sheets readback.
No bypass of write gates, no fake scoring, and no Layer 2/portfolio mutation.
"""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

VERSION="V6.5.1"
PRIORITY=("SPY","TSLA","AAPL")
NY=ZoneInfo("America/New_York")


class BootstrapHalt(ValueError):
    pass


def precutoff_gate(now):
    if now.tzinfo is None: raise BootstrapHalt("INVALID_TIMEZONE")
    local=now.astimezone(NY)
    return local.weekday()<5 and (local.hour,local.minute)<(15,30)


async def bootstrap_priority_equities(*, status_fn, plan_fn, step_fn, now):
    if not precutoff_gate(now):
        return {"version":VERSION,"status":"NOT_PRE_CUTOFF",
                "sheets_committed":[],"execution_permitted":False}
    today=now.astimezone(timezone.utc).date().isoformat()
    committed=[]
    skipped=[]
    for symbol in PRIORITY:
        state=await status_fn()
        if state.get("version")!=VERSION or state.get("utc_date")!=today:
            raise BootstrapHalt("CHECKPOINT_DATE_OR_VERSION_INVALID")
        if state.get("pending_reconciliation"):
            raise BootstrapHalt("RECONCILIATION_REQUIRED")
        completed=set((state.get("checkpoint") or {}).get("completed_symbols") or [])
        if symbol in completed:
            skipped.append(symbol)
            continue
        planned=await plan_fn()
        nxt=planned.get("next_symbols") or []
        if planned.get("version")!=VERSION or planned.get("utc_date")!=today:
            raise BootstrapHalt("PLAN_VERSION_OR_DAY_INVALID")
        if planned.get("pending_reconciliation"):
            raise BootstrapHalt("RECONCILIATION_REQUIRED")
        if not nxt or nxt[0]!=symbol:
            raise BootstrapHalt("PRIORITY_QUEUE_ORDER_NOT_VERIFIED")
        step=await step_fn()
        if step.get("status")=="PROVIDER_DEFERRED":
            # The server has already marked the safe pre-write deferral
            # and removed its own PREPARED journal atomically.
            raise BootstrapHalt("SOURCE_PROVIDER_DEFERRED")
        if step.get("status")=="SOURCE_RETRYABLE":
            raise BootstrapHalt("SOURCE_RETRYABLE")
        if not (
            step.get("daily_symbol")==symbol
            and step.get("commit_status")=="COMMITTED"
            and step.get("verify_status")=="PASS"
            and step.get("checkpoint_status")=="COMMITTED_VERIFY_PASS"
        ):
            raise BootstrapHalt("SHEET_COMMIT_NOT_VERIFIED")
        confirmed=await status_fn()
        if (confirmed.get("pending_reconciliation") or
            symbol not in ((confirmed.get("checkpoint") or {}).get("completed_symbols") or []) or
            confirmed.get("utc_date")!=today):
            raise BootstrapHalt("REDIS_POSTWRITE_CHECKPOINT_NOT_VERIFIED")
        committed.append(symbol)
    return {"version":VERSION,"status":"STAGE0_PRIORITY_COMMITTED_VERIFY_PASS",
            "utc_day":today,"symbols_committed":committed,
            "already_verified":skipped,"sheets_commit_readback":True,
            "ranking_permitted":False,"execution_permitted":False,
            "run_manifest_written":False,"trades_written":False}
