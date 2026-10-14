"""V6.5.1: exact-day JPM post-write reconciliation after failed Sheets readback.

Promotion is allowed ONLY after independent FMP historical replay, complete
Sheet footprint validation, expired lease, intact checkpoint and atomic Redis
CAS.  Never writes Google Sheets or touches Ranking/Lifecycle/trades.
"""
from __future__ import annotations

import asyncio
import json
import math
from datetime import datetime, timezone

from stage0_batch import (
    Stage0BatchCoordinator, LOCK_KEY, LOCK_SECONDS, pending_key, state_key,
    safe_cursor,
)
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error
from stage0_reconcile import RECOVERY_GRACE_SECONDS

VERSION = "V6.5.1"
DAY = "2026-10-14"
SYMBOL = "JPM"
RUN = "STAGE0-20261014-V6.5.1"
NEW_DATES = frozenset(("2026-10-12", "2026-10-13"))
FIELDS = ("open", "high", "low", "close", "volume")
_ATOMIC_COMMIT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
if redis.call('GET', KEYS[3]) ~= ARGV[3] then return 0 end
redis.call('SET', KEYS[3], ARGV[4])
redis.call('DEL', KEYS[2])
return 1
"""


def _utc_stamp(value):
    try:
        item = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        raise Stage0Error("JPM_RECONCILIATION_TIMESTAMP_INVALID", "Invalid ISO source time") from None
    if item.tzinfo is None:
        raise Stage0Error("JPM_RECONCILIATION_TIMESTAMP_INVALID", "Naive ISO source time")
    return item.astimezone(timezone.utc)


def inspect_jpm_saved(saved, journal_created, fmp_history):
    """Check every persisted JPM record plus independent provider replay."""
    raw = saved["Raw_OHLCV"]["rows"]
    rows = [row for key, row in raw.items() if key[0] == SYMBOL]
    if len(rows) != 302:
        raise Stage0Error("JPM_POSTWRITE_FOOTPRINT_INVALID", "Unexpected JPM row count")
    old = [r for r in rows if r.get("ranking_run_id") != RUN]
    new = [r for r in rows if r.get("ranking_run_id") == RUN]
    if len(old) != 300 or len(new) != 2:
        raise Stage0Error("JPM_POSTWRITE_FOOTPRINT_INVALID", "Incorrect old/new row split")
    dates = {r.get("trading_date") for r in new}
    if dates != NEW_DATES:
        raise Stage0Error("JPM_POSTWRITE_FOOTPRINT_INVALID", "New session dates mismatch")
    for row in old:
        if (row.get("ranking_run_id") != "STAGE0-20261012-V6.5.1"
            or row.get("source") != "FMP"
            or _utc_stamp(row.get("fetched_at_utc")) >= journal_created):
            raise Stage0Error("JPM_POSTWRITE_FOOTPRINT_INVALID", "Legacy row mutated")
    samples = (fmp_history if isinstance(fmp_history, list)
               else fmp_history.get("historical") if isinstance(fmp_history, dict) else None)
    if not isinstance(samples, list):
        raise Stage0Error("JPM_PROVIDER_REPLAY_INVALID", "Invalid history response")
    replay = {}
    for item in samples:
        if not isinstance(item, dict):
            continue
        d = str(item.get("date", ""))[:10]
        if d in NEW_DATES and d in replay:
            raise Stage0Error("JPM_PROVIDER_REPLAY_INVALID", "Duplicated provider date")
        if d in NEW_DATES:
            replay[d] = item
    if set(replay) != NEW_DATES:
        raise Stage0Error("JPM_PROVIDER_REPLAY_INCOMPLETE", "Provider is missing candles")
    for row in new:
        day = row["trading_date"]
        if (row.get("canonical_symbol") != SYMBOL
            or row.get("provider_symbol") != SYMBOL
            or row.get("source") != "FMP"
            or row.get("upsert_key") != f"{SYMBOL}|{day}|FMP"
            or row.get("source_url_template")
                 != "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=JPM"
            or row.get("quality_status") != "RECEIVED_UNVERIFIED"
            or _utc_stamp(row.get("fetched_at_utc")) < journal_created):
            raise Stage0Error("JPM_POSTWRITE_FOOTPRINT_INVALID", "New row provenance invalid")
        for fld in FIELDS:
            observed = row.get(fld, "")
            expected = Stage0_Daily_Ingestor._scalar(replay[day].get(fld))
            if not expected or observed != expected:
                raise Stage0Error("JPM_PROVIDER_REPLAY_MISMATCH", "FMP candle mismatch")
            try:
                if not math.isfinite(float(observed)) or float(observed) < 0:
                    raise ValueError("Nonfinite")
            except (ValueError,TypeError):
                raise Stage0Error("JPM_POSTWRITE_FOOTPRINT_INVALID", "Invalid OHLCV") from None
        low, high = float(row["low"]), float(row["high"])
        opn, close = float(row["open"]), float(row["close"])
        if low <= 0 or low > min(opn, close) or high < max(opn, close):
            raise Stage0Error("JPM_POSTWRITE_FOOTPRINT_INVALID", "Invalid OHLC ranges")
    snapshots = saved["Market_Data_Snapshot"]["rows"]
    snap = snapshots.get((RUN, SYMBOL))
    if snap is None:
        raise Stage0Error("JPM_POSTWRITE_SNAPSHOT_MISSING", "Current-run snapshot absent")
    if (snap.get("asset_class") != "Equity"
        or snap.get("score_model") != "EQUITY_V2"
        or snap.get("analysis_price_source") != "FMP_STABLE"
        or snap.get("mandatory_broad_pass") != "FALSE"
        or snap.get("enrichment_status") != "ENRICHMENT_PENDING"
        or snap.get("broad_readiness") != "ENRICHMENT_PENDING"
        or "stable/quote?symbol=JPM" not in snap.get("source_summary", "")
        or "stable/historical-price-eod/full?symbol=JPM" not in snap.get("source_summary", "")
        or _utc_stamp(snap.get("data_timestamp")) < journal_created
        or _utc_stamp(snap.get("data_timestamp")).date().isoformat() != DAY):
        raise Stage0Error("JPM_POSTWRITE_SNAPSHOT_INVALID", "Snapshot source or status")
    try:
        px=float(snap.get("last_price", ""))
        if not math.isfinite(px) or px <= 0:
            raise ValueError("Price")
    except (ValueError,TypeError):
        raise Stage0Error("JPM_POSTWRITE_SNAPSHOT_INVALID", "Missing positive price") from None
    runtime = saved["FMP_Runtime_State"]["rows"]
    entry = runtime.get(("Stage 0 Last Run ID",))
    if entry is None or entry.get("Configured Value") != RUN:
        raise Stage0Error("JPM_POSTWRITE_RUNTIME_INVALID", "Run ID not written")
    return {"jpm_rows":302,"legacy_rows":300,"new_dates":sorted(NEW_DATES),
            "snapshot_verified":True, "fmp_replay_verified":True,
            "readback_rows":{tab:len(saved[tab]["rows"]) for tab in (
                "Raw_OHLCV","Market_Data_Snapshot","FMP_Runtime_State",
            )}}


def read_sheets_for_jpm():
    reader=Stage0_Daily_Ingestor(providers={"read_only":lambda:{}})
    return reader._capture(reader._open())


async def reconcile_jpm_postwrite(*,redis,provider,now=None,
                                   sheet_reader=None):
    now=now or datetime.now(timezone.utc)
    if now.tzinfo is None or now.astimezone(timezone.utc).date().isoformat()!=DAY:
        raise Stage0Error("JPM_RECOVERY_DATE_INVALID", "Exact UTC day required")
    batch=Stage0BatchCoordinator(redis)
    lease=await batch.acquire()
    try:
        raw=await redis.call("GET",pending_key(DAY))
        if not isinstance(raw,str):
            raise Stage0Error("JPM_RECOVERY_NOT_REQUIRED", "Pending journal absent")
        try:
            journal=json.loads(raw)
            created=_utc_stamp(journal["created_at"])
        except (ValueError,TypeError,KeyError):
            raise Stage0Error("JPM_JOURNAL_INVALID", "Journal malformed") from None
        if (not isinstance(journal,dict) or journal.get("status")!="PREPARED"
            or journal.get("symbols")!=[SYMBOL] or journal.get("run_id")!=RUN
            or created.date().isoformat()!=DAY):
            raise Stage0Error("JPM_RECOVERY_SCOPE_MISMATCH", "Not the JPM PREPARED journal")
        if (now.astimezone(timezone.utc)-created).total_seconds() < (
            LOCK_SECONDS+RECOVERY_GRACE_SECONDS
        ):
            raise Stage0Error("JPM_LEASE_STILL_ACTIVE", "Wait for original lease expiry")
        checkpoint_raw=await redis.call("GET",state_key(DAY))
        if not isinstance(checkpoint_raw,str):
            raise Stage0Error("JPM_CHECKPOINT_INVALID", "Missing day checkpoint")
        try:
            prior=json.loads(checkpoint_raw)
        except (ValueError,TypeError):
            raise Stage0Error("JPM_CHECKPOINT_INVALID", "Checkpoint not JSON") from None
        if (not isinstance(prior,dict) or prior.get("version")!=VERSION
            or prior.get("utc_date")!=DAY or prior.get("status")!="COMMITTED_VERIFY_PASS"
            or SYMBOL in (prior.get("completed_symbols") or [])
            or "INTC" not in (prior.get("completed_symbols") or [])):
            raise Stage0Error("JPM_CHECKPOINT_INVALID", "Prior cursor inconsistent")
        history=await provider.get("historical",SYMBOL)
        if history.get("status")!="READY":
            raise Stage0Error("JPM_PROVIDER_REPLAY_UNAVAILABLE", "FMP history unavailable")
        saved=await asyncio.to_thread(sheet_reader or read_sheets_for_jpm)
        proof=inspect_jpm_saved(saved,created,history.get("data"))
        next_cp={
            "version": VERSION, "utc_date": DAY,
            "last_verified_run_id":RUN,
            "completed_symbols":safe_cursor(prior,[SYMBOL]),
            "last_verified_symbols":[SYMBOL],
            "readback":{tab:{"verified_rows":count,"status":"PASS"}
                        for tab,count in proof["readback_rows"].items()},
            "verified_at_utc":datetime.now(timezone.utc).isoformat(),
            "status":"COMMITTED_VERIFY_PASS",
            "reconciliation_evidence":{
                "type":"POSTWRITE_INDEPENDENT_SHEETS_AND_FMP_REPLAY",
                "symbol":SYMBOL,"old_bars":300,"new_bars":2,
                "mandatory_broad_pass":False,
            },
        }
        if datetime.now(timezone.utc).date().isoformat()!=DAY:
            raise Stage0Error("JPM_RECOVERY_DATE_CHANGED", "UTC day changed")
        packed=json.dumps(next_cp,separators=(",",":"),sort_keys=True)
        applied=await redis.eval(
            _ATOMIC_COMMIT,[LOCK_KEY,pending_key(DAY),state_key(DAY)],
            [lease,raw,checkpoint_raw,packed],
        )
        if int(applied or 0)!=1:
            raise Stage0Error("JPM_ATOMIC_COMMIT_CONFLICT","Redis state changed")
        if await redis.call("GET",pending_key(DAY)):
            raise Stage0Error("JPM_COMMIT_READBACK_FAILED","Journal remains")
        readback=await redis.call("GET",state_key(DAY))
        if readback!=packed:
            raise Stage0Error("JPM_COMMIT_READBACK_FAILED","Checkpoint mismatched")
        return {"version":VERSION,"status":"COMMITTED_VERIFY_PASS",
                "symbol":SYMBOL,"fmp_replay_verified":True,
                "sheet_snapshot_verified":True,"new_ohlcv_bars_verified":2,
                "historical_ohlcv_bars_preserved":300,
                "checkpoint_advanced":True,
                "pending_journal_cleared":True,
                "sheet_writes_performed":False,
                "ranking_permitted":False,"execution_permitted":False}
    finally:
        await batch.release(lease)
