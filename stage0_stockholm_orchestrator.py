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
# Upstream gateway failures alone may permit a single new phase attempt.
# The actual ingestion always rechecks pending Redis journal/cursor/Sheets.
RETRYABLE_HALTS={
    "FMP_STEP_HALTED_HTTP_502",
    "FMP_STEP_HALTED_HTTP_503",
    "FMP_STEP_HALTED_HTTP_504",
    "FMP_STEP_HALTED_HTTP_TRANSPORT_UNAVAILABLE",
}
RETRY_ONE_AFTER_VERIFIED_HALT_LUA = """
local claim = redis.call('GET', KEYS[1])
if claim ~= 'STARTED' then return 0 end
local payload = redis.call('GET', KEYS[2])
if not payload then return 0 end
local ok, result = pcall(cjson.decode, payload)
if not ok or type(result) ~= 'table' then return 0 end
if result.version ~= ARGV[1] or result.phase ~= ARGV[2] then return 0 end
if result.status ~= 'HALTED' then return 0 end
if result.halt_reason ~= ARGV[3] and result.halt_reason ~= ARGV[4]
   and result.halt_reason ~= ARGV[5] and result.halt_reason ~= ARGV[6] then
    return 0
end
local prior_time = result.timestamp_utc or ''
if type(prior_time) ~= 'string' or prior_time == '' then return 0 end
redis.call('SET', KEYS[1], 'RETRY_1', 'EX', tonumber(ARGV[7]))
-- Clear only the phase-result diagnostic, never any source journal,
-- checkpoint, quota reservation or Sheets record.
redis.call('DEL', KEYS[2])
return 1
"""


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
        import re
        reason=summary.get("halt_reason") or "NONE"
        reason=reason if re.fullmatch(r"[A-Z0-9_]{1,96}",str(reason)) else "REASON_REDACTED"
        LOG.info("STOCKHOLM_PHASE_FINISHED phase=%s status=%s reason=%s fmp_verified=%s crypto_verified=%s",
                 phase,status,reason,summary["fmp_verified"],summary["crypto_verified"])
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
    retry=False
    if claimed!="OK":
        # The previous result must be explicitly HALTED with a retryable
        # gateway/transport error. An uncertain in-flight execution without
        # a persisted result is NEVER retried automatically.
        raw=await redis.call("GET",result_key(phase))
        prior=None
        if raw:
            try:
                prior=json.loads(raw)
            except (ValueError,TypeError):
                prior={"status":"RESULT_UNREADABLE"}
        if (isinstance(prior,dict) and prior.get("version")==VERSION
            and prior.get("phase")==phase
            and prior.get("status")=="HALTED"
            and prior.get("halt_reason") in RETRYABLE_HALTS):
            # Atomic single retry; two overlapping wake requests cannot
            # both claim it. Stage0's own transactional preflight remains
            # authoritative for every subsequent source write.
            code=await redis.eval(
                RETRY_ONE_AFTER_VERIFIED_HALT_LUA,
                [attempt_key(phase),result_key(phase)],
                [VERSION,phase,*sorted(RETRYABLE_HALTS),CRON_STATUS_TTL]
            )
            retry=code==1
        if not retry:
            return {"version":VERSION,"status":"PHASE_ALREADY_CLAIMED",
                    "phase":phase,"accepted":False,"prior_result":prior}
    create_task(run_phase(redis,phase,now))
    LOG.info("STOCKHOLM_PHASE_%s phase=%s",
             "SAFE_RETRY_ACCEPTED" if retry else "ACCEPTED",phase)
    return {"version":VERSION,
            "status":"PHASE_SAFE_RETRY_ACCEPTED" if retry else "PHASE_ACCEPTED",
            "phase":phase,"accepted":True,
            "ranking_permitted":False,"execution_permitted":False}
