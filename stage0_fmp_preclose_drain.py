"""AskLiam V6.5.1 FMP priority queue: cautious, bounded, pre-close Stage0 drain.

Invokes existing Stage0 daily transactional step, which owns the Redis journal,
exclusive lease, Google Sheets UPSERT and postwrite readback. This wrapper
cannot advance checkpoints itself, mutate ranking or submit trades.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

VERSION = "V6.5.1"
NY = ZoneInfo("America/New_York")
MAX_ASSETS = 96
MAX_WALL_SECONDS = 55 * 60
QUOTA_RESERVE = 40


class DrainHalt(ValueError):
    pass


def preclose(now: datetime) -> bool:
    if now.tzinfo is None:
        raise DrainHalt("NAIVE_CLOCK")
    t = now.astimezone(NY)
    # Keep enough time for final verification. No acquisition after 15:30 NY.
    return t.weekday() < 5 and (t.hour, t.minute) < (15, 30)


async def drain_fmp_preclose(*, status_fn, plan_fn, step_fn,
                             now_fn=lambda: datetime.now(timezone.utc),
                             monotonic=time.monotonic,
                             max_steps=MAX_ASSETS, max_wall_seconds=MAX_WALL_SECONDS,
                             on_step=None):
    now = now_fn()
    if not preclose(now):
        return {"version": VERSION, "status": "NOT_PRE_CUTOFF",
                "committed": [], "deferred": [], "ranking_permitted": False}
    day = now.astimezone(timezone.utc).date().isoformat()
    if not isinstance(max_steps, int) or not 1 <= max_steps <= MAX_ASSETS:
        raise DrainHalt("INVALID_MAX_STEPS")
    if not 0 < max_wall_seconds <= MAX_WALL_SECONDS:
        raise DrainHalt("INVALID_WALL_BUDGET")
    began = monotonic()
    good = []
    skipped = []
    reason = "BUDGET_EXHAUSTED"
    for _ in range(max_steps):
        if not preclose(now_fn()) or monotonic() - began >= max_wall_seconds:
            reason = "PRECLOSE_CUTOFF_OR_TIME_BUDGET"
            break
        state = await status_fn()
        if state.get("version") != VERSION or state.get("utc_date") != day:
            raise DrainHalt("STATE_DATE_OR_VERSION")
        if state.get("pending_reconciliation"):
            raise DrainHalt("RECONCILIATION_REQUIRED")
        quota = state.get("quota") or {}
        if quota.get("blocked"):
            reason = "FMP_DAY_BLOCKED"
            break
        try:
            remaining = int(quota.get("remaining"))
        except (TypeError, ValueError):
            raise DrainHalt("QUOTA_NOT_VERIFIED") from None
        if remaining <= QUOTA_RESERVE:
            reason = "FMP_QUOTA_RESERVE"
            break
        plan = await plan_fn()
        if (plan.get("version") != VERSION or plan.get("utc_date") != day
                or plan.get("pending_reconciliation")):
            raise DrainHalt("PLAN_NOT_VERIFIED")
        queue = plan.get("next_symbols") or []
        if not queue:
            reason = "FMP_QUEUE_EMPTY"
            break
        symbol = str(queue[0])
        if symbol in good or symbol in skipped:
            raise DrainHalt("QUEUE_NOT_ADVANCING")
        step = await step_fn()
        if step.get("status") == "PROVIDER_DEFERRED":
            if step.get("symbol") != symbol or step.get("checkpoint_advanced") is not False:
                raise DrainHalt("DEFERRED_NOT_VERIFIED")
            skipped.append(symbol)
            if on_step:
                on_step("DEFERRED", symbol)
            continue
        if step.get("status") == "SOURCE_RETRYABLE":
            if step.get("symbol") != symbol or step.get("checkpoint_advanced") is not False:
                raise DrainHalt("RETRYABLE_NOT_VERIFIED")
            reason = "RETRYABLE_SOURCE_" + symbol
            break
        if step.get("status") == "FMP_QUEUE_EMPTY":
            reason = "FMP_QUEUE_EMPTY"
            break
        if not (
            step.get("daily_symbol") == symbol
            and step.get("commit_status") == "COMMITTED"
            and step.get("verify_status") == "PASS"
            and step.get("checkpoint_status") == "COMMITTED_VERIFY_PASS"
        ):
            raise DrainHalt("COMMIT_OR_READBACK_NOT_VERIFIED")
        check = await status_fn()
        if (
            check.get("utc_date") != day
            or check.get("pending_reconciliation")
            or symbol not in (check.get("checkpoint") or {}).get("completed_symbols", [])
        ):
            raise DrainHalt("REDIS_CHECKPOINT_NOT_VERIFIED")
        good.append(symbol)
        if on_step:
            on_step("VERIFIED", symbol)
    return {
        "version": VERSION,
        "utc_day": day,
        "status": reason,
        "committed": good,
        "deferred": skipped,
        "verified_count": len(good),
        "deferred_count": len(skipped),
        "ranking_permitted": False,
        "execution_permitted": False,
        "run_manifest_written": False,
        "trades_written": False,
    }
