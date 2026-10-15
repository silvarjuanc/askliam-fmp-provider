"""V6.5.1 Stockholm pre-analysis driver with Redis phase de-duplication.

The external Render wake cron is token-authenticated but holds NO
Google/FMP/broker secrets. It only wakes the existing web service;
the web service executes existing authenticated writer routes. All
trading, portfolio, manifest, scoring remain outside this driver.
"""
from __future__ import annotations
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from scripts.stage0_daily_scheduler import VERSION, BASE_URL, run, schedule_gate

STOCKHOLM=ZoneInfo("Europe/Stockholm")
LOG=logging.getLogger("uvicorn.error")
PHASE_HOURS=(9,12,14,15)
CONFIG_FLAGS=("ASKLIAM_STAGE0_ENABLED", "ASKLIAM_STAGE0_AUTO_ENABLED", "ASKLIAM_CRYPTO_AUTO_ENABLED")
CRON_STATUS_TTL=72*3600


def phase_for(now: datetime):
    if now.tzinfo is None:
        raise ValueError("NAIVE_CLOCK")
    local=now.astimezone(STOCKHOLM)
    return local.strftime("%Y-%m-%d:%H") if schedule_gate(now) else None


def preflight(now):
    if not phase_for(now):
        return "NOT_SCHEDULED_STOCKHOLM_PREANALYSIS"
    if os.environ.get("ASKLIAM_AUTONOMOUS_STAGE0_ENABLED","false").lower()!="true":
        return "STOCKHOLM_ORCHESTRATOR_DISABLED"
    if any(os.environ.get(k,"false").lower()!="true" for k in CONFIG_FLAGS):
        return "STAGE0_GATE_DISABLED"
    if os.environ.get("ASKLIAM_CRYPTO_WRITE_ENABLED","false").lower()=="true":
        return "MANUAL_CRYPTO_WRITER_CONFLICT"
    if not os.environ.get("ASKLIAM_SERVICE_TOKEN"):
        return "SERVICE_TOKEN_NOT_CONFIGURED"
    if not os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or not os.environ.get("ASKLIAM_SPREADSHEET_ID"):
        return "SHEETS_CREDENTIALS_NOT_CONFIGURED"
    return "READY"


def attempt_key(phase):
    return "askliam:v651:stockholm:orchestrator:attempt:"+phase


def result_key(phase):
    return "askliam:v651:stockholm:orchestrator:result:"+phase


def run_phase_sync(now):
    import httpx
    token=os.environ["ASKLIAM_SERVICE_TOKEN"]
    with httpx.Client(base_url=BASE_URL,follow_redirects=False) as client:
        return run(client,token,now=now)


async def run_phase(redis, phase, now):
    try:
        # Work is delegated to the dedicated runner with ALL transactional
        # source/readback checks; the wake request never writes to Sheets.
        output=await asyncio.to_thread(run_phase_sync, now)
        status=str(output.get("status","UNKNOWN"))
        summary={
            "version":VERSION,"phase":phase,"status":status,
            "halt_reason":output.get("halt_reason"),
            "fmp_verified":len(output.get("fmp_verified") or []),
            "fmp_deferred":len(output.get("fmp_deferred") or []),
            "crypto_verified":len(output.get("crypto_verified") or []),
            "timestamp_utc":datetime.now(timezone.utc).isoformat(),
            "execution_permitted":False,
        }
        try:
            await redis.call("SET",result_key(phase),
                             json.dumps(summary,separators=(",",":")),"EX",CRON_STATUS_TTL)
        except Exception:
            LOG.error("STOCKHOLM_PHASE_RESULT_REDIS_UNAVAILABLE phase=%s",phase)
        LOG.info("STOCKHOLM_PHASE_FINISHED phase=%s status=%s fmp_verified=%s crypto_verified=%s",
                 phase,status,summary["fmp_verified"],summary["crypto_verified"])
    except Exception as exc:
        LOG.error("STOCKHOLM_PHASE_HALTED phase=%s error_class=%s",phase,type(exc).__name__)
        try:
            await redis.call("SET",result_key(phase),json.dumps({
                "version":VERSION,"phase":phase,"status":"HALTED",
                "error_class":type(exc).__name__,
                "timestamp_utc":datetime.now(timezone.utc).isoformat(),
            }),"EX",CRON_STATUS_TTL)
        except Exception:
            pass


async def accepted_wake(redis, now, *, create_task=asyncio.create_task):
    """Idempotent per-phase gate, must only be invoked after authenticated HTTP."""
    status=preflight(now)
    if status!="READY":
        return {"version":VERSION,"status":status,"accepted":False}
    if redis is None:
        return {"version":VERSION,"status":"UPSTASH_UNAVAILABLE","accepted":False}
    phase=phase_for(now)
    claimed=await redis.call("SET",attempt_key(phase),"STARTED","NX","EX",CRON_STATUS_TTL)
    if claimed!="OK":
        return {"version":VERSION,"status":"PHASE_ALREADY_CLAIMED",
                "phase":phase,"accepted":False}
    create_task(run_phase(redis,phase,now))
    LOG.info("STOCKHOLM_PHASE_ACCEPTED phase=%s",phase)
    return {"version":VERSION,"status":"PHASE_ACCEPTED","phase":phase,
            "accepted":True,"ranking_permitted":False,"execution_permitted":False}
