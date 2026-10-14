"""AskLiam V6.5.1 STRICT one-day legacy-history PREPARED journal recovery.

Unlike generic prewrite inspection, permit prior immutable OHLCV only when
EVERY record predates failed journal and no same-run write footprint exists.
No market data or checkpoint is changed. Atomic Redis compare-and-delete only.
"""
from __future__ import annotations
import asyncio
import json
from datetime import datetime,timezone

from stage0_batch import LOCK_KEY,LOCK_SECONDS,pending_key,state_key,Stage0BatchCoordinator
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error
from stage0_reconcile import RECOVERY_GRACE_SECONDS

DAY="2026-10-14"
SYMBOL="GE"
RUN_ID="STAGE0-20261014-V6.5.1"
LUA="""
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
if (redis.call('GET', KEYS[3]) or '') ~= ARGV[3] then return 0 end
return redis.call('DEL', KEYS[2])
"""


def read_sheet_footprint():
    probe=Stage0_Daily_Ingestor(providers={"reconcile_readonly":lambda:{}})
    actual=probe._capture(probe._open())
    history=[row for key,row in actual["Raw_OHLCV"]["rows"].items()
             if key[0]==SYMBOL]
    snapshots=[row for key,row in actual["Market_Data_Snapshot"]["rows"].items()
               if key==(RUN_ID,SYMBOL)]
    return history,snapshots


def old_history_is_immutable(history,snapshots,created):
    if snapshots or len(history)!=300:
        return False
    unique=set()
    for row in history:
        if (str(row.get("canonical_symbol","")).upper()!=SYMBOL
            or str(row.get("source","")).upper()!="FMP"
            or row.get("ranking_run_id")==RUN_ID
            or not str(row.get("ranking_run_id","")).startswith("STAGE0-20261012")
            or not row.get("trading_date")
            or not row.get("close")
            or str(row.get("quality_status",""))!="RECEIVED_UNVERIFIED"):
            return False
        key=(row.get("canonical_symbol"),row.get("trading_date"),row.get("source"))
        if key in unique:
            return False
        unique.add(key)
        try:
            stamp=datetime.fromisoformat(str(row["fetched_at_utc"]).replace("Z","+00:00"))
            if stamp.tzinfo is None or not stamp.astimezone(timezone.utc)<created:
                return False
        except (ValueError,TypeError,KeyError):
            return False
    return True


async def recover_ge_prewrite(redis,*,now=None,sheet_reader=None):
    now=now or datetime.now(timezone.utc)
    if now.tzinfo is None or now.astimezone(timezone.utc).date().isoformat()!=DAY:
        raise Stage0Error("RECOVERY_DATE_INVALID","Exact UTC day required")
    batch=Stage0BatchCoordinator(redis)
    lease=None
    try:
        lease=await batch.acquire()
        pending=await redis.call("GET",pending_key(DAY))
        if not isinstance(pending,str):
            raise Stage0Error("RECOVERY_NOT_REQUIRED","No verified PREPARED journal")
        try:
            journal=json.loads(pending)
            created=datetime.fromisoformat(journal["created_at"].replace("Z","+00:00"))
        except (ValueError,TypeError,KeyError,AttributeError):
            raise Stage0Error("RECOVERY_JOURNAL_INVALID","Invalid journal payload") from None
        if (not isinstance(journal,dict)
            or journal.get("status")!="PREPARED"
            or journal.get("symbols")!=[SYMBOL]
            or journal.get("run_id")!=RUN_ID
            or created.tzinfo is None or created.astimezone(timezone.utc).date().isoformat()!=DAY):
            raise Stage0Error("RECOVERY_SCOPE_MISMATCH","Wrong symbol, day or stage")
        if (now.astimezone(timezone.utc)-created.astimezone(timezone.utc)).total_seconds() < (
            LOCK_SECONDS+RECOVERY_GRACE_SECONDS
        ):
            raise Stage0Error("RECOVERY_LEASE_NOT_EXPIRED","Lease grace not elapsed")
        checkpoint_raw=await redis.call("GET",state_key(DAY))
        try:
            cp=json.loads(checkpoint_raw) if checkpoint_raw else {}
        except (ValueError,TypeError):
            raise Stage0Error("RECOVERY_CHECKPOINT_INVALID","Invalid checkpoint") from None
        if (not isinstance(cp,dict)
            or (cp and (cp.get("version")!="V6.5.1" or cp.get("utc_date")!=DAY))
            or SYMBOL in (cp.get("completed_symbols") or [])):
            raise Stage0Error("RECOVERY_CHECKPOINT_CHANGED","Checkpoint not safe")
        reader=sheet_reader or read_sheet_footprint
        history,snapshots=await asyncio.to_thread(reader)
        if not old_history_is_immutable(history,snapshots,created):
            raise Stage0Error("RECOVERY_SHEET_FOOTPRINT_INVALID",
                              "Old GE bars changed or current-run data present")
        success=await redis.eval(
            LUA,[LOCK_KEY,pending_key(DAY),state_key(DAY)],
            [lease,pending,checkpoint_raw or ""],
        )
        if int(success or 0)!=1:
            raise Stage0Error("RECOVERY_COMPARE_AND_DELETE_FAILED","Concurrent mutation")
        if await redis.call("GET",pending_key(DAY)):
            raise Stage0Error("RECOVERY_READBACK_FAILED","Pending journal remains")
        if (await redis.call("GET",state_key(DAY)) or "")!=(checkpoint_raw or ""):
            raise Stage0Error("RECOVERY_READBACK_FAILED","Checkpoint changed")
        return {"version":"V6.5.1","status":"GE_PREWRITE_ABORTED_VERIFIED",
                "symbol":SYMBOL,"run_id":RUN_ID,
                "verified_historical_rows":len(history),
                "sheet_writes_performed":False,"checkpoint_advanced":False,
                "ranking_permitted":False,"execution_permitted":False}
    finally:
        if lease is not None:
            await batch.release(lease)
