"""AskLiam V6.5.1 FMP provider. Python 3.11+; never logs credentials."""
from __future__ import annotations
import asyncio
import hashlib
import json
import math
import os
import hmac
import re
from datetime import timedelta
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

VERSION = "V6.5.1"
ROOT = "https://financialmodelingprep.com"
CAP = 240

TTL = {"quote": 900, "profile": 604800, "historical": 86400}

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def day_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()

# Render Free has no durable local filesystem. Upstash is authoritative.
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9^][A-Z0-9^._:/-]{0,39}$")
ROOT = os.environ.get("FMP_BASE_URL", ROOT).rstrip("/")
# Never allow environment configuration to raise this cap.
CAP = 240
RESERVE_LUA = """
local quota, blocked = KEYS[1], KEYS[2]
if redis.call('EXISTS', blocked) == 1 then return -1 end
if tonumber(redis.call('GET', quota) or '0') >= tonumber(ARGV[1]) then return -1 end
local n = redis.call('INCR', quota)
if n == 1 then redis.call('EXPIREAT', quota, tonumber(ARGV[2])) end
return n
"""
BLOCK_LUA = "redis.call('SET', KEYS[1], ARGV[1], 'EXAT', ARGV[2]); return 1"

def next_utc_midnight():
    from datetime import timedelta
    day = datetime.now(timezone.utc).date() + timedelta(days=1)
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())

class StorageError(Exception):
    pass

class RedisState:
    def __init__(self, client):
        self.client = client
        self.url = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
        self.token = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
        if not self.url.startswith("https://") or not self.token:
            raise StorageError("UPSTASH_NOT_CONFIGURED")

    async def call(self, *args):
        try:
            response = await self.client.post(
                self.url,
                json=list(args),
                headers={"Authorization": "Bearer " + self.token},
                timeout=15,
            )
            response.raise_for_status()
            value = response.json()
            if value.get("error"):
                raise StorageError("UPSTASH_COMMAND_ERROR")
            return value.get("result")
        except (httpx.HTTPError, ValueError, TypeError):
            raise StorageError("UPSTASH_UNAVAILABLE") from None

    async def eval(self, script, keys, args):
        return await self.call("EVAL", script, len(keys), *keys, *args)

    async def cache_get(self, key):
        raw = await self.call("GET", key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            raise StorageError("CACHE_CORRUPT") from None

    async def cache_put(self, key, payload, ttl):
        await self.call("SET", key, json.dumps(payload, separators=(",", ":"), allow_nan=False), "EX", ttl)

    async def reserve(self):
        day = day_utc()
        result = await self.eval(RESERVE_LUA, [
            "askliam:v651:fmp:used:" + day,
            "askliam:v651:fmp:blocked:" + day,
        ], [CAP, next_utc_midnight()])
        return isinstance(result, int) and result > 0

    async def block(self, reason, day):
        from datetime import timedelta
        end = datetime.fromisoformat(day).replace(tzinfo=timezone.utc) + timedelta(days=1)
        await self.eval(BLOCK_LUA, ["askliam:v651:fmp:blocked:" + day], [reason, int(end.timestamp())])

    async def quota(self):
        day = day_utc()
        used, block = await asyncio.gather(
            self.call("GET", "askliam:v651:fmp:used:" + day),
            self.call("GET", "askliam:v651:fmp:blocked:" + day),
        )
        count = int(used or 0)
        return {"utc_date": day, "used": count, "remaining": max(0, CAP - count), "blocked": bool(block or count >= CAP), "reason": block}

    async def cursor(self):
        return await self.call("GET", "askliam:v651:continuation_cursor")

    async def set_cursor(self, value):
        await self.call("SET", "askliam:v651:continuation_cursor", value)

def key_for(kind: str, symbols: str) -> str:
    return "askliam:v651:fmp:cache:" + hashlib.sha256(f"{VERSION}:stable:{kind}:{symbols}".encode()).hexdigest()

def finite(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (ValueError, TypeError, OverflowError):
        return None


def technicals(data: Any):
    bars = data if isinstance(data, list) else (data.get("historical", []) if isinstance(data, dict) else [])
    bars = sorted((b for b in bars if isinstance(b, dict) and b.get("date") and finite(b.get("close")) is not None and finite(b.get("close")) > 0), key=lambda b: b["date"])
    closes = [float(b["close"]) for b in bars]
    if not closes:
        return {}
    def ret(n):
        return 100 * (closes[-1] / closes[-n-1] - 1) if len(closes) > n else None
    def ma(n):
        return sum(closes[-n:]) / n if len(closes) >= n else None
    def vol(n):
        if len(closes) <= n:
            return None
        r = [math.log(closes[i]/closes[i-1]) for i in range(len(closes)-n, len(closes))]
        avg = sum(r)/len(r)
        return 100 * math.sqrt(sum((x-avg)**2 for x in r)/(len(r)-1)) * math.sqrt(252) if len(r) > 1 else None
    peak = closes[0]
    dd = 0.0
    for x in closes:
        peak = max(peak, x)
        dd = min(dd, 100 * (x/peak - 1))
    out = {"observations": len(closes), "return_1d": ret(1), "return_5d": ret(5), "return_1m": ret(21), "return_3m": ret(63), "return_6m": ret(126), "return_12m": ret(252), "volatility_20d": vol(20), "volatility_60d": vol(60), "drawdown_pct": dd}
    for n in (20, 50, 200):
        m = ma(n)
        out[f"ma{n}"] = m
        out[f"distance_ma{n}_pct"] = 100*(closes[-1]/m-1) if m else None
    return {k:v for k,v in out.items() if v is not None}


class Query(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=500)
    include_profiles: bool = True
    include_historical: bool = False
    historical_budget: int = Field(default=0, ge=0, le=240)



class Provider:
    def __init__(self):
        self.client: httpx.AsyncClient | None = None
        self.state: RedisState | None = None
        self.semaphore = asyncio.Semaphore(4)
        self.lock = asyncio.Lock()
        self.inflight: dict[str, asyncio.Task] = {}
        self.cache_hits = 0
        self.network_calls = 0
        self.local_blocked_days: set[str] = set()

    async def start(self):
        self.client = httpx.AsyncClient(http2=True, timeout=20, limits=httpx.Limits(max_connections=10, max_keepalive_connections=5), follow_redirects=False)
        try:
            self.state = RedisState(self.client)
        except StorageError:
            self.state = None

    async def stop(self):
        if self.client:
            await self.client.aclose()

    async def get(self, kind: Literal["quote", "profile", "historical"], symbols: str):
        if not self.state:
            return {"status": "PROVIDER_DEFERRED", "error": "PERSISTENT_STATE_UNAVAILABLE"}
        key = key_for(kind, symbols)
        try:
            cache = await self.state.cache_get(key)
        except StorageError:
            return {"status": "PROVIDER_DEFERRED", "error": "REDIS_UNAVAILABLE"}
        if cache is not None:
            self.cache_hits += 1
            return {"status": "READY", "cache_hit": True, **cache}
        if not os.environ.get("FMP_API_KEY"):
            return {"status": "PROVIDER_DEFERRED", "error": "MISSING_FMP_API_KEY"}
        async with self.lock:
            task = self.inflight.get(key)
            if task is None:
                task = asyncio.create_task(self._network(kind, symbols, key))
                self.inflight[key] = task
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                async with self.lock:
                    if self.inflight.get(key) is task:
                        self.inflight.pop(key, None)

    async def _network(self, kind, symbols, key):
        async with self.semaphore:
            if not self.state or day_utc() in self.local_blocked_days:
                return {"status": "PROVIDER_DEFERRED", "error": "PROVIDER_BLOCKED"}
            try:
                cache = await self.state.cache_get(key)
                if cache is not None:
                    self.cache_hits += 1
                    return {"status": "READY", "cache_hit": True, **cache}
                reservation_day = day_utc()
                permitted = await self.state.reserve()  # Atomic, BEFORE network.
            except StorageError:
                return {"status": "PROVIDER_DEFERRED", "error": "REDIS_RESERVATION_FAILED"}
            if not permitted:
                return {"status": "PROVIDER_DEFERRED", "error": "FMP_QUOTA_EXHAUSTED"}
            self.network_calls += 1
            if kind == "quote":
                endpoint = "/stable/batch-quote" if "," in symbols else "/stable/quote"
                query_params = {"symbols" if "," in symbols else "symbol": symbols}
            elif kind == "profile":
                endpoint = "/stable/profile"
                query_params = {"symbol": symbols}
            else:
                endpoint = "/stable/historical-price-eod/full"
                query_params = {"symbol": symbols}
            safe_params = dict(query_params)
            query_params["apikey"] = os.environ.get("FMP_API_KEY")
            try:
                response = await self.client.get(ROOT + endpoint, params=query_params)
            except httpx.RequestError:
                return {"status": "PROVIDER_DEFERRED", "error": "FMP_NETWORK_ERROR"}
            if response.status_code == 429:
                self.local_blocked_days.add(reservation_day)
                try:
                    await self.state.block("HTTP_429", reservation_day)
                except StorageError:
                    pass  # Redis outage still causes quota reservation to fail closed.
                return {"status": "PROVIDER_DEFERRED", "error": "FMP_HTTP_429"}
            if response.status_code >= 400:
                return {"status": "PROVIDER_DEFERRED", "error": "FMP_HTTP_403_KEY_OR_PLAN" if response.status_code == 403 else f"FMP_HTTP_{response.status_code}"}
            try:
                data = response.json()
            except ValueError:
                return {"status": "PROVIDER_DEFERRED", "error": "INVALID_JSON"}
            if not data or (isinstance(data, dict) and any(x in data for x in ("Error Message", "error", "Error"))):
                return {"status": "PROVIDER_DEFERRED", "error": "FMP_EMPTY_OR_ERROR"}
            info = {"data": data, "source": "FMP", "data_timestamp": now_utc(),
                    "source_url_template": ROOT + endpoint + "?" + "&".join(f"{k}={v}" for k, v in safe_params.items())}
            try:
                await self.state.cache_put(key, info, TTL[kind])
            except StorageError:
                return {"status": "PROVIDER_DEFERRED", "error": "REDIS_CACHE_WRITE_FAILED"}
            return {"status": "READY", "cache_hit": False, **info}

    async def batch(self, query: Query):
        symbols = list(dict.fromkeys(s.strip().upper() for s in query.symbols if s.strip()))
        if any(not SYMBOL_PATTERN.fullmatch(s) for s in symbols):
            raise HTTPException(422, "INVALID_SYMBOL")
        batches = [symbols[i:i + 20] for i in range(0, len(symbols), 20)]
        quotes = await asyncio.gather(*(self.get("quote", ",".join(batch)) for batch in batches))
        profiles = await asyncio.gather(*(self.get("profile", symbol) for symbol in symbols)) if query.include_profiles else []
        def split(results):
            output = {}
            for group, response in zip(batches, results):
                if response["status"] != "READY":
                    for name in group: output[name] = response
                    continue
                data = response["data"]
                records = data if isinstance(data, list) else [data]
                mapping = {str(x.get("symbol", "")).upper(): x for x in records if isinstance(x, dict)}
                for name in group:
                    output[name] = ({**response, "data": mapping[name]} if name in mapping
                        else {"status": "PROVIDER_DEFERRED", "error": "FMP_BATCH_SYMBOL_MISSING"})
            return output
        qmap = split(quotes)
        pmap = {symbol: response for symbol, response in zip(symbols, profiles)} if query.include_profiles else {}
        history = {}
        if query.include_historical and query.historical_budget:
            selected = symbols[:query.historical_budget]
            history = dict(zip(selected, await asyncio.gather(*(self.get("historical", symbol) for symbol in selected))))
        rows = []
        for symbol in symbols:
            q, p, h = qmap.get(symbol), pmap.get(symbol), history.get(symbol)
            fields = {}
            if q and q["status"] == "READY":
                v = q["data"]
                fields.update({"last_price": finite(v.get("price")), "volume": finite(v.get("volume")), "market_cap": finite(v.get("marketCap"))})
            errors_profile = None
            if p and p["status"] == "READY":
                raw_profile = p["data"]
                # FMP Stable /profile returns a list even for a single symbol.
                # Select only the matching asset and never assume a dict.
                profiles = raw_profile if isinstance(raw_profile, list) else [raw_profile]
                v = next(
                    (item for item in profiles
                     if isinstance(item, dict)
                     and str(item.get("symbol", "")).upper() == symbol),
                    None,
                )
                if v is None:
                    errors_profile = "FMP_PROFILE_SYMBOL_MISSING"
                else:
                    fields.update({
                        "sector": v.get("sector"),
                        "industry": v.get("industry"),
                        "currency": v.get("currency"),
                        "exchange": v.get("exchangeShortName"),
                    })
                    if fields.get("market_cap") is None:
                        fields["market_cap"] = finite(v.get("marketCap") or v.get("mktCap"))
            if h and h["status"] == "READY":
                fields.update(technicals(h["data"]))
            errors = [v["error"] for v in (q, p, h) if v and v.get("error")]
            if p and p["status"] == "READY" and errors_profile:
                errors.append(errors_profile)
            rows.append({"canonical_symbol": symbol, "enrichment_status": "PROVIDER_DEFERRED" if errors else "ACQUIRED_NOT_SCORED",
                         "source": "FMP", "data_timestamp": now_utc(),
                         "source_urls": list(dict.fromkeys(v["source_url_template"] for v in (q,p,h) if v and v.get("source_url_template"))),
                         "fields": {k:v for k,v in fields.items() if v is not None}, "provider_errors": errors,
                         "historical": h["data"] if h and h["status"] == "READY" else None})
        try:
            quota = await self.state.quota() if self.state else None
        except (StorageError, ValueError):
            quota = None
        return {"version": VERSION, "results": rows, "fmp_quota": quota,
                "cache_hits_this_process": self.cache_hits, "network_calls_this_process": self.network_calls}


provider = Provider()

async def _explicit_one_shot_coinbase_stage0():
    """Run only the operator-authorized BTC/ETH bootstrap on its exact UTC day.

    Never runs during normal startups, on /health, or on schedule. Individual
    Redis checkpoints make restart attempts idempotent. This is data-only.
    """
    import logging
    from stage0_crypto_coinbase_commit import (
        spot_preflight, spot_commit_one, confirm_spot,
    )
    from stage0_crypto_commit import crypto_checkpoint_key
    from stage0_ingestor import Stage0Error
    logger = logging.getLogger("uvicorn.error")
    today = datetime.now(timezone.utc).date().isoformat()
    if os.getenv("ASKLIAM_COINBASE_SPOT_STARTUP_ONCE_DATE", "") != today:
        return
    symbols = os.getenv("ASKLIAM_COINBASE_SPOT_STARTUP_ONCE_SYMBOLS", "")
    if symbols != "BTC,ETH" or provider.state is None or provider.client is None:
        logger.error("COINBASE_ONESHOT_BLOCKED: invalid scope or unavailable state")
        return
    if (os.getenv("ASKLIAM_COINBASE_SPOT_WRITE_ENABLED", "").lower() != "true"
        or os.getenv("ASKLIAM_COINBASE_SPOT_WRITE_SYMBOL") != symbols
        or os.getenv("ASKLIAM_COINBASE_SPOT_WRITE_UTC_DAY") != today):
        logger.error("COINBASE_ONESHOT_BLOCKED: scoped write permission absent")
        return
    for symbol in symbols.split(","):
        try:
            if datetime.now(timezone.utc).date().isoformat() != today:
                logger.error("COINBASE_ONESHOT_HALTED: UTC_DAY_CHANGED")
                return
            # Skip an already readback-verified spot import. Read-only Redis.
            from stage0_crypto_coinbase_commit import spot_checkpoint_key
            stored = await provider.state.call("GET", spot_checkpoint_key(today))
            if stored:
                state = json.loads(stored)
                if (state.get("version") != VERSION or state.get("utc_date") != today
                    or state.get("status") != "COMMITTED_VERIFY_PASS"):
                    logger.error("COINBASE_ONESHOT_HALTED: CHECKPOINT_INVALID")
                    return
                if symbol in state.get("completed_symbols", []):
                    logger.info("COINBASE_ONESHOT_ALREADY_VERIFIED symbol=%s", symbol)
                    continue
            # No writing unless the provider payload and production sheet
            # diff both pass a separate read-only preflight.
            check = await spot_preflight(
                symbol, redis=provider.state, client=provider.client,
            )
            logger.info(
                "COINBASE_ONESHOT_PREFLIGHT_PASS symbol=%s raw_inserts=%s "
                "raw_updates=%s snapshot_inserts=%s snapshot_updates=%s",
                symbol, check["plans"]["Raw_OHLCV"]["inserts"],
                check["plans"]["Raw_OHLCV"]["updates"],
                check["plans"]["Market_Data_Snapshot"]["inserts"],
                check["plans"]["Market_Data_Snapshot"]["updates"],
            )
            done = await spot_commit_one(
                symbol, confirm_spot(symbol), redis=provider.state,
                client=provider.client,
            )
            logger.info(
                "COINBASE_ONESHOT_COMMIT_PASS symbol=%s status=%s "
                "bars=%s run_id=%s",
                symbol, done["status"], done["daily_bars"], done["run_id"],
            )
        except Stage0Error as exc:
            logger.error(
                "COINBASE_ONESHOT_HALTED symbol=%s code=%s detail=%s",
                symbol, exc.status, exc.detail,
            )
            # A partial write may have happened. Stop until reconciled.
            return
        except Exception as exc:
            logger.error(
                "COINBASE_ONESHOT_HALTED symbol=%s error_class=%s",
                symbol, type(exc).__name__,
            )
            return


async def _explicit_coinbase_reconcile_then_bootstrap():
    """Fail-closed exact-day journal repair BEFORE the normal spot bootstrap."""
    import logging
    from stage0_crypto_coinbase_reconcile import reconcile_after_readback_error
    from stage0_ingestor import Stage0Error
    log = logging.getLogger("uvicorn.error")
    if (os.getenv("ASKLIAM_COINBASE_SPOT_RECOVER_DATE") != "2026-10-14"
        or os.getenv("ASKLIAM_COINBASE_SPOT_RECOVER_SYMBOL") != "ETH"):
        log.error("COINBASE_POSTWRITE_RECOVERY_BLOCKED invalid scope")
        return
    try:
        repaired = await reconcile_after_readback_error(
            redis=provider.state, client=provider.client,
        )
    except Stage0Error as exc:
        log.error("COINBASE_POSTWRITE_RECOVERY_HALTED code=%s detail=%s",
                  exc.status, exc.detail)
        return
    except Exception as exc:
        log.error("COINBASE_POSTWRITE_RECOVERY_HALTED error_class=%s",
                  type(exc).__name__)
        return
    log.info(
        "COINBASE_POSTWRITE_RECOVERY_PASS status=%s symbols=%s bars=%s",
        repaired["status"], ",".join(repaired["symbols_verified_today"]),
        repaired["daily_bars_verified"],
    )
    await _explicit_one_shot_coinbase_stage0()


async def _mirror_runtime_evidence_once():
    """Operator-scoped one-day evidence bridge for the connected Sheet reader."""
    import logging
    from stage0_runtime_evidence import sync_stage0_evidence
    log = logging.getLogger("uvicorn.error")
    try:
        if provider.state is None:
            log.error("STAGE0_EVIDENCE_SYNC_HALTED code=REDIS_UNAVAILABLE")
            return
        result = await sync_stage0_evidence(provider.state)
        log.info(
            "STAGE0_EVIDENCE_SYNC_PASS day=%s fmp_used=%s fmp_remaining=%s "
            "spot_verified=%s readback=%s",
            result["utc_day"], result["fmp_used"], result["fmp_remaining"],
            ",".join(result["spot_verified"]) or "NONE", result["sync_status"],
        )
    except Exception as err:
        status = getattr(err,"status",type(err).__name__)
        log.error("STAGE0_EVIDENCE_SYNC_HALTED code=%s", status)


async def _fmp_preclose_entitlement_probe_once():
    """Operator-controlled source diagnostics only. Never logs keys or prices."""
    import logging
    from stage0_fmp_entitlement_probe import probe_preclose_entitlements
    log = logging.getLogger("uvicorn.error")
    try:
        result = await probe_preclose_entitlements(provider)
        if result["status"] != "SOURCE_ENTITLEMENT_AUDIT_COMPLETED":
            log.warning("FMP_PRECLOSE_PROBE_HALTED status=%s", result["status"])
            return
        for item in result["symbols"]:
            for domain, detail in item["domains"].items():
                log.info(
                    "FMP_PRECLOSE_DOMAIN symbol=%s domain=%s status=%s reason=%s cache_hit=%s",
                    item["symbol"], domain, detail["status"],
                    detail.get("reason", "NONE"),
                    detail.get("cache_hit", False),
                )
        log.info(
            "FMP_PRECLOSE_AUDIT_PASS used_before=%s used_after=%s "
            "remaining_after=%s sheet_writes=0",
            result["quota_used_before"], result["quota_used_after"],
            result["quota_remaining_after"],
        )
    except Exception as exc:
        # Never include raw provider response or any secret-bearing details.
        log.error("FMP_PRECLOSE_PROBE_HALTED exception=%s", type(exc).__name__)


async def _run_fmp_equity_preclose_bootstrap_once():
    """Date-scoped priority FMP Stage0 writer: pre-existing transactional API only."""
    import logging
    from stage0_batch import Stage0BatchCoordinator
    from stage0_equity_preclose_bootstrap import bootstrap_priority_equities
    log = logging.getLogger("uvicorn.error")
    try:
        if os.getenv("ASKLIAM_STAGE0_ENABLED", "").lower() != "true":
            log.warning("FMP_EQUITY_BOOTSTRAP_HALTED code=STAGE0_DISABLED")
            return
        if provider.state is None:
            log.warning("FMP_EQUITY_BOOTSTRAP_HALTED code=UPSTASH_UNAVAILABLE")
            return
        batch = Stage0BatchCoordinator(provider.state)
        result = await bootstrap_priority_equities(
            status_fn=batch.status,
            plan_fn=stage0_daily_plan,
            step_fn=stage0_daily_step,
            now=datetime.now(timezone.utc),
        )
        log.info(
            "FMP_EQUITY_BOOTSTRAP_RESULT status=%s committed=%s already=%s "
            "sheet_readback=%s trades_written=0",
            result["status"], ",".join(result.get("symbols_committed", [])) or "NONE",
            ",".join(result.get("already_verified", [])) or "NONE",
            result.get("sheets_commit_readback", False),
        )
    except Exception as exc:
        from stage0_equity_preclose_bootstrap import BootstrapHalt
        if isinstance(exc, BootstrapHalt):
            log.error("FMP_EQUITY_BOOTSTRAP_HALTED code=%s", str(exc))
        else:
            log.error("FMP_EQUITY_BOOTSTRAP_HALTED class=%s", type(exc).__name__)


async def _run_fmp_preclose_drain_once():
    """Operator-scoped one-day FMP Stage0 ingestion, no ranking/trades."""
    import logging
    from stage0_batch import Stage0BatchCoordinator
    from stage0_fmp_preclose_drain import drain_fmp_preclose, DrainHalt
    logger = logging.getLogger("uvicorn.error")
    if os.getenv("ASKLIAM_STAGE0_ENABLED", "").lower() != "true":
        logger.error("FMP_DRAIN_HALTED reason=STAGE0_DISABLED")
        return
    if provider.state is None:
        logger.error("FMP_DRAIN_HALTED reason=REDIS_UNAVAILABLE")
        return
    batch = Stage0BatchCoordinator(provider.state)
    try:
        def step_log(status, symbol):
            logger.info("FMP_DRAIN_STEP status=%s symbol=%s", status, symbol)
        report = await drain_fmp_preclose(
            status_fn=batch.status, plan_fn=stage0_daily_plan,
            step_fn=stage0_daily_step, on_step=step_log,
        )
        logger.info(
            "FMP_DRAIN_SUMMARY status=%s committed=%s deferred=%s count=%s "
            "readback_required=true ranking_permitted=false",
            report["status"], ",".join(report["committed"]) or "NONE",
            ",".join(report["deferred"]) or "NONE",report.get("verified_count",0),
        )
    except DrainHalt as err:
        logger.error("FMP_DRAIN_HALTED reason=%s", str(err))
    except HTTPException as err:
        details = err.detail
        if isinstance(details, dict):
            code = str(details.get("stage0_status") or "HTTP_ERROR")
        else:
            code = "HTTP_ERROR"
        # Never log user/sheet provider payloads or secret-bearing descriptions.
        allowed = {
            "RECONCILIATION_REQUIRED", "SHEET_PREFLIGHT_UNAVAILABLE",
            "SHEET_WRITE_FAILED", "SHEET_READ_FAILED", "SOURCE_RETRYABLE",
            "FMP_QUOTA_GUARD", "CHECKPOINT_NOT_COMMITTED",
            "CONCURRENT_RUN_BLOCKED", "STAGE0_DISABLED",
            "HALTED", "VERIFICATION_FAILED", "STORAGE_UNAVAILABLE",
            "SCHEMA_FAILED", "PROVIDER_DEFERRED", "DATA_VALIDATION_FAILED",
        }
        code = code if code in allowed else "UNCLASSIFIED_HTTP_ERROR"
        logger.error("FMP_DRAIN_HALTED http=%s stage0_status=%s", err.status_code, code)
    except Exception as err:
        logger.error("FMP_DRAIN_HALTED error_class=%s", type(err).__name__)


async def _guarded_stage0_prewrite_recovery_once():
    """One-day opt-in: abort ONLY proved unwritten abandoned FMP PREPARED journal."""
    import logging
    from stage0_reconcile import inspect_abandoned_prewrite, public_inspection
    log = logging.getLogger("uvicorn.error")
    if provider.state is None:
        log.error("STAGE0_RECOVERY_AUDIT_HALTED reason=REDIS_UNAVAILABLE")
        return
    try:
        report = await inspect_abandoned_prewrite(provider.state)
        public = public_inspection(report)
        pending = public.get("pending") or {}
        symbols = pending.get("symbols") or []
        log.info(
            "STAGE0_RECOVERY_AUDIT status=%s symbol=%s journal_stage=%s "
            "age_seconds=%s blockers=%s safe=%s",
            public.get("status"), ",".join(symbols) or "NONE",
            pending.get("status"), public.get("pending_age_seconds"),
            ",".join(public.get("blockers") or []) or "NONE",
            public.get("safe_to_abort_prewrite"),
        )
        if not report.get("safe_to_abort_prewrite"):
            return
        if (len(symbols)!=1
                or str(pending.get("created_at",""))[:10] != day_utc()):
            log.error("STAGE0_RECOVERY_HALTED reason=INVALID_EXACT_DAY_SCOPE")
            return
        from stage0_fmp_preclose_drain import preclose
        if not preclose(datetime.now(timezone.utc)):
            log.error("STAGE0_RECOVERY_HALTED reason=PRECLOSE_GATE_CLOSED")
            return
        decision = Stage0PrewriteRecoveryRequest(
            symbol=symbols[0], run_id=pending["run_id"],
            confirmation="ABORT_PREWRITE_ONLY_NO_SHEET_ROWS",
        )
        result = await stage0_reconciliation_abort_prewrite(decision)
        if result.get("status") != "ABANDONED_PREWRITE_JOURNAL_ABORTED":
            log.error("STAGE0_RECOVERY_HALTED reason=CAS_NOT_VERIFIED")
            return
        log.info("STAGE0_RECOVERY_SAFE_ABORT_PASS symbol=%s",symbols[0])
        # The same idempotent and transactional daily writer resumes only
        # AFTER an entirely separate Google Sheets/Redis safety readback.
        await _run_fmp_preclose_drain_once()
    except HTTPException as exc:
        log.error("STAGE0_RECOVERY_HALTED http=%s",exc.status_code)
    except Exception as exc:
        log.error("STAGE0_RECOVERY_AUDIT_HALTED class=%s",type(exc).__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await provider.start()
    one_shot = None
    if os.getenv("ASKLIAM_COINBASE_SPOT_RECOVER_DATE", "") == day_utc():
        one_shot = asyncio.create_task(_explicit_coinbase_reconcile_then_bootstrap())
    elif os.getenv("ASKLIAM_COINBASE_SPOT_STARTUP_ONCE_DATE", "") == day_utc():
        one_shot = asyncio.create_task(_explicit_one_shot_coinbase_stage0())
    evidence_task = None
    if os.getenv("ASKLIAM_RUNTIME_EVIDENCE_ONCE_DAY", "") == day_utc():
        evidence_task = asyncio.create_task(_mirror_runtime_evidence_once())
    fmp_probe_task = None
    if os.getenv("ASKLIAM_FMP_PRECLOSE_PROBE_ONCE_DAY", "") == day_utc():
        fmp_probe_task = asyncio.create_task(_fmp_preclose_entitlement_probe_once())
    equity_task = None
    if os.getenv("ASKLIAM_FMP_EQUITY_BOOTSTRAP_ONCE_DAY", "") == day_utc():
        equity_task = asyncio.create_task(_run_fmp_equity_preclose_bootstrap_once())
    fmp_drain_task = None
    if os.getenv("ASKLIAM_FMP_PRECLOSE_DRAIN_ONCE_DAY", "") == day_utc():
        fmp_drain_task = asyncio.create_task(_run_fmp_preclose_drain_once())
    safe_recovery_task = None
    if os.getenv("ASKLIAM_STAGE0_SAFE_RECOVER_THEN_DRAIN_ONCE_DAY", "") == day_utc():
        safe_recovery_task = asyncio.create_task(_guarded_stage0_prewrite_recovery_once())
    try:
        yield
    finally:
        if safe_recovery_task is not None and not safe_recovery_task.done():
            safe_recovery_task.cancel()
            try:
                await safe_recovery_task
            except asyncio.CancelledError:
                pass
        if fmp_drain_task is not None and not fmp_drain_task.done():
            fmp_drain_task.cancel()
            try:
                await fmp_drain_task
            except asyncio.CancelledError:
                pass
        if equity_task is not None and not equity_task.done():
            equity_task.cancel()
            try:
                await equity_task
            except asyncio.CancelledError:
                pass
        if fmp_probe_task is not None and not fmp_probe_task.done():
            fmp_probe_task.cancel()
            try:
                await fmp_probe_task
            except asyncio.CancelledError:
                pass
        if evidence_task is not None and not evidence_task.done():
            evidence_task.cancel()
            try:
                await evidence_task
            except asyncio.CancelledError:
                pass
        if one_shot is not None and not one_shot.done():
            one_shot.cancel()
            try:
                await one_shot
            except asyncio.CancelledError:
                pass
        await provider.stop()

app = FastAPI(title="AskLiam FMP Enrichment Provider", version=VERSION, lifespan=lifespan)


def require_token(x_askliam_token: str | None = Header(default=None)):
    expected = os.environ.get("ASKLIAM_SERVICE_TOKEN")
    if not expected:
        raise HTTPException(status_code=503, detail="ASKLIAM_SERVICE_TOKEN not configured")
    import secrets
    if not x_askliam_token or not secrets.compare_digest(x_askliam_token, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/health")
async def health():
    return {"version": VERSION, "service": "askliam-fmp-provider",
            "storage": "UPSTASH_REDIS_REST", "redis_configured": provider.state is not None,
            "fmp_configured": bool(os.environ.get("FMP_API_KEY")),
            "status": "CONFIGURED_NOT_VALIDATED" if provider.state else "PENDING_EXTERNAL_RUNTIME"}

@app.get("/quota", dependencies=[Depends(require_token)])
async def quota():
    if not provider.state: raise HTTPException(503, "REDIS_NOT_CONFIGURED")
    try: return {"version": VERSION, **(await provider.state.quota())}
    except (StorageError, ValueError): raise HTTPException(503, "REDIS_UNAVAILABLE") from None

@app.get("/cursor", dependencies=[Depends(require_token)])
async def get_cursor():
    if not provider.state: raise HTTPException(503, "REDIS_NOT_CONFIGURED")
    try: return {"version": VERSION, "continuation_cursor": await provider.state.cursor()}
    except StorageError: raise HTTPException(503, "REDIS_UNAVAILABLE") from None

class Cursor(BaseModel):
    value: str = Field(min_length=1, max_length=250)

@app.put("/cursor", dependencies=[Depends(require_token)])
async def set_cursor(payload: Cursor):
    if not provider.state: raise HTTPException(503, "REDIS_NOT_CONFIGURED")
    try: await provider.state.set_cursor(payload.value)
    except StorageError: raise HTTPException(503, "REDIS_UNAVAILABLE") from None
    return {"version": VERSION, "cursor_persisted": True}

@app.post("/enrich", dependencies=[Depends(require_token)])
async def enrich(query: Query):
    return await provider.batch(query)


# Stage 0 is explicitly invoked and NEVER runs on a timer or on /health.
# The service token authenticates callers, while Google Service Account
# credentials separately authenticate all workbook reads and writes.
class Stage0RunRequest(BaseModel):
    # Explicit symbols only: unsupervised full-universe scans are not enabled.
    symbols: list[str] = Field(min_length=1, max_length=5)
    max_symbols: int = Field(default=5, ge=1, le=5)
    historical_budget: int = Field(default=5, ge=0, le=5)

@app.get("/stage0/reconciliation/inspect", dependencies=[Depends(require_token)])
async def stage0_reconciliation_inspect():
    """Independent read-only Redis/Sheets audit; never clears pending journal."""
    from stage0_reconcile import inspect_abandoned_prewrite, public_inspection
    from stage0_ingestor import Stage0Error
    if provider.state is None:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE")
    try:
        result = await inspect_abandoned_prewrite(provider.state)
        return public_inspection(result)
    except Stage0Error as exc:
        raise HTTPException(503, {"status": exc.status, "reason": exc.detail}) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(503, "RECONCILIATION_INSPECTION_UNAVAILABLE") from None


class Stage0PrewriteRecoveryRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=40)
    run_id: str = Field(min_length=10, max_length=100)
    confirmation: Literal["ABORT_PREWRITE_ONLY_NO_SHEET_ROWS"]


@app.post("/stage0/reconciliation/abort-prewrite", dependencies=[Depends(require_token)])
async def stage0_reconciliation_abort_prewrite(request: Stage0PrewriteRecoveryRequest):
    """Explicit and guarded Redis journal recovery; no Sheet modifications.

    PREPARED is not sufficient: verify no symbol footprint across sheets,
    expiry of old lease, preserved checkpoint, and exact Redis compare-and-del.
    Never promote checkpoint or write market data.
    """
    from stage0_batch import Stage0BatchCoordinator, LOCK_KEY, pending_key, state_key
    from stage0_reconcile import inspect_abandoned_prewrite
    from stage0_ingestor import Stage0Error
    if provider.state is None:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE")
    if not SYMBOL_PATTERN.fullmatch(request.symbol):
        raise HTTPException(422, "INVALID_SYMBOL")
    batch = Stage0BatchCoordinator(provider.state)
    token = None
    try:
        token = await batch.acquire()
        report = await inspect_abandoned_prewrite(provider.state)
        journal = report.get("pending") or {}
        if (
            not report["safe_to_abort_prewrite"]
            or journal.get("symbols") != [request.symbol]
            or journal.get("run_id") != request.run_id
        ):
            raise Stage0Error("RECONCILIATION_REQUIRED", "Footprint or journal validation blocked recovery")
        raw = report["journal_raw"]
        saved_checkpoint = report["checkpoint_raw"] or ""
        day = journal["created_at"][:10]
        script = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
if (redis.call('GET', KEYS[3]) or '') ~= ARGV[3] then return 0 end
return redis.call('DEL', KEYS[2])
"""
        result = await provider.state.eval(
            script,
            [LOCK_KEY, pending_key(day), state_key(day)],
            [token, raw, saved_checkpoint],
        )
        if int(result or 0) != 1:
            raise Stage0Error("RECONCILIATION_REQUIRED", "Concurrent Redis state change; journal preserved")
        return {
            "version": VERSION,
            "status": "ABANDONED_PREWRITE_JOURNAL_ABORTED",
            "symbol": request.symbol,
            "run_id": request.run_id,
            "sheet_writes_performed": False,
            "checkpoint_advanced": False,
            "ranking_permitted": False,
        }
    except Stage0Error as exc:
        raise HTTPException(409, {"status": exc.status, "reason": exc.detail}) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(503, "RECONCILIATION_ABORT_FAILED") from None
    finally:
        if token is not None:
            try:
                await batch.release(token)
            except Exception:
                pass


@app.get("/stage0/batch/status", dependencies=[Depends(require_token)])
async def stage0_batch_status():
    from stage0_batch import Stage0BatchCoordinator
    from stage0_ingestor import Stage0Error
    try:
        return await Stage0BatchCoordinator(provider.state).status()
    except Stage0Error as exc:
        raise HTTPException(503, {"status": exc.status, "detail": exc.detail}) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None

@app.post("/stage0/run", dependencies=[Depends(require_token)])
async def stage0_run(request: Stage0RunRequest):
    if os.getenv("ASKLIAM_STAGE0_ENABLED", "").lower() != "true":
        raise HTTPException(503, "STAGE0_DISABLED")
    if not os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or not os.getenv("ASKLIAM_SPREADSHEET_ID"):
        raise HTTPException(503, "STAGE0_GOOGLE_CREDENTIALS_NOT_CONFIGURED")
    symbols = [s.strip().upper() for s in request.symbols]
    if (
        len(symbols) > request.max_symbols
        or request.historical_budget != len(symbols)
        or len(symbols) != len(set(symbols))
        or any(not SYMBOL_PATTERN.fullmatch(s) for s in symbols)
    ):
        raise HTTPException(422, "STAGE0_REQUIRES_EXPLICIT_VALID_SYMBOLS_WITH_HISTORICAL")
    from stage0_batch import Stage0BatchCoordinator
    from stage0_ingestor import Stage0Error
    batch = Stage0BatchCoordinator(provider.state)
    token = None
    try:
        token = await batch.acquire()
        day, run_id, checkpoint = await batch.prepare(symbols, token)
        result = await asyncio.to_thread(batch.run_blocking, symbols, run_id)
        # No cursor promotion until Redis has acknowledged the durable
        # checkpoint, with lock ownership checked atomically in Lua.
        return await batch.finish(day, run_id, checkpoint, symbols, result, token)
    except Stage0Error as exc:
        raise HTTPException(
            409 if exc.status in ("CONCURRENT_RUN_BLOCKED", "RECONCILIATION_REQUIRED", "ALREADY_VERIFIED_TODAY", "INVALID_BATCH")
            else 503,
            {
                "stage0_status": exc.status, "reason": exc.detail,
                "ranking_permitted": False,
                "checkpoint_advanced": False,
            },
        ) from None
    except Exception:
        raise HTTPException(
            503,
            {"stage0_status": "HALTED", "checkpoint_advanced": False,
             "ranking_permitted": False},
        ) from None
    finally:
        if token is not None:
            try:
                await batch.release(token)
            except Exception:
                pass

class Stage0DiagnosticRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=40)

@app.post("/stage0/daily/diagnose", dependencies=[Depends(require_token)])
async def stage0_daily_diagnose(request: Stage0DiagnosticRequest):
    """Read-only workbook adapter diagnostic; FMP network quota may be used."""
    symbol = request.symbol.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise HTTPException(422, "INVALID_SYMBOL")
    from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error
    from stage0_fmp_adapter import ExistingFMPStage0Adapter
    if not provider.state:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE")
    try:
        quota_status = await provider.state.quota()
        if quota_status["blocked"] or quota_status["remaining"] < 40:
            raise HTTPException(503, "FMP_QUOTA_GUARD")
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None

    def probe():
        diagnostic = Stage0_Daily_Ingestor(providers={"probe": lambda: {}})
        book = diagnostic._open()
        adapter = ExistingFMPStage0Adapter(
            book, diagnostic.run_id,
            symbols=[symbol], max_symbols=1, historical_budget=1,
        )
        try:
            frames = adapter()
        except RuntimeError as exc:
            import re
            message = str(exc)
            code = message if re.fullmatch(r"[A-Z][A-Z0-9_]{2,90}", message) else "ADAPTER_UNEXPECTED_ERROR"
            return {"version": VERSION, "symbol": symbol, "status": "NOT_ACQUIRED",
                    "diagnostic_code": code, "wrote_sheets": False}
        return {"version": VERSION, "symbol": symbol, "status": "ACQUIRED_READ_ONLY",
                "quotes": len(frames["Market_Data_Snapshot"]),
                "historical_rows": len(frames["Raw_OHLCV"]),
                "wrote_sheets": False}
    try:
        return await asyncio.to_thread(probe)
    except Stage0Error as exc:
        raise HTTPException(503, {"status": exc.status, "detail": exc.detail}) from None
    except Exception:
        raise HTTPException(503, "DIAGNOSTIC_UNAVAILABLE") from None

def _safe_domain_error(value):
    """Reduce upstream failures to known identifiers, never raw provider text."""
    code = str(value or "").upper()
    known = {
        "FMP_HTTP_400", "FMP_HTTP_401", "FMP_HTTP_402",
        "FMP_HTTP_403_KEY_OR_PLAN", "FMP_HTTP_404",
        "FMP_HTTP_408", "FMP_HTTP_429", "FMP_HTTP_500",
        "FMP_HTTP_502", "FMP_HTTP_503", "FMP_HTTP_504",
        "FMP_QUOTA_EXHAUSTED", "FMP_NETWORK_ERROR",
        "FMP_EMPTY_OR_ERROR", "INVALID_JSON",
        "PROVIDER_BLOCKED", "PERSISTENT_STATE_UNAVAILABLE",
        "REDIS_UNAVAILABLE", "REDIS_RESERVATION_FAILED",
        "REDIS_CACHE_WRITE_FAILED", "MISSING_FMP_API_KEY",
    }
    return code if code in known else "PROVIDER_UNKNOWN_ERROR"


@app.post("/stage0/daily/source-check", dependencies=[Depends(require_token)])
async def stage0_daily_source_check(request: Stage0DiagnosticRequest):
    """FMP quote/profile/history entitlement check, no Sheet writes.

    Each missing-cache domain can use a separate FMP quota reservation.
    """
    symbol = request.symbol.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise HTTPException(422, "INVALID_SYMBOL")
    if provider.state is None:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE")
    try:
        quota_before = await provider.state.quota()
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    if quota_before["blocked"] or quota_before["remaining"] < 43:
        raise HTTPException(503, "FMP_QUOTA_GUARD")
    domains = {}
    for kind in ("quote", "profile", "historical"):
        # Separate single-symbol reads avoid the plan's batch-quote endpoint.
        try:
            result = await provider.get(kind, symbol)
        except (StorageError, ValueError):
            raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
        state = result.get("status")
        if state == "READY":
            domains[kind] = {
                "status": "AVAILABLE",
                "cache_hit": bool(result.get("cache_hit", False)),
            }
        else:
            error = _safe_domain_error(result.get("error"))
            domains[kind] = {"status": "NOT_AVAILABLE", "reason": error}
            if error in ("FMP_HTTP_429", "FMP_QUOTA_EXHAUSTED",
                         "REDIS_UNAVAILABLE", "REDIS_RESERVATION_FAILED",
                         "PROVIDER_BLOCKED"):
                break
    try:
        quota_after = await provider.state.quota()
    except StorageError:
        quota_after = None
    return {
        "version": VERSION, "symbol": symbol,
        "status": "SOURCE_DOMAINS_CHECKED",
        "domains": domains,
        "quota": quota_after,
        "wrote_sheets": False,
        "checkpoint_advanced": False,
        "ranking_permitted": False,
    }


class FMP402BackfillRequest(BaseModel):
    confirmation: Literal["SEED_CONFIRMED_FMP_HTTP402_ONLY"]


@app.post("/stage0/fmp/402/backfill-today", dependencies=[Depends(require_token)])
async def stage0_backfill_fmp_402(request: FMP402BackfillRequest):
    """Opt-in daily Redis-to-Redis migration. No FMP calls or Sheet writes.

    Requires exact user confirmation to prevent automatic mutation of the
    future request queue. Backfills only explicit, historical FMP_HTTP_402
    reasons from the current UTC date.
    """
    from stage0_batch import Stage0BatchCoordinator
    from stage0_ingestor import Stage0Error
    if provider.state is None:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE")
    batch = Stage0BatchCoordinator(provider.state)
    token = None
    try:
        token = await batch.acquire()
        status = await batch.status()
        if status["pending_reconciliation"]:
            raise Stage0Error("RECONCILIATION_REQUIRED",
                              "Pending journal prevents FMP cooldown migration")
        result = await batch.seed_402_from_today(token)
        return {
            "version": VERSION,
            "status": "FMP_402_COOLDOWNS_SEEDED",
            "source": "CONFIRMED_TODAY_DEFERRED_REDIS",
            **result,
            "sheet_writes_performed": False,
            "fmp_requests_performed": False,
            "checkpoint_advanced": False,
            "ranking_permitted": False,
        }
    except Stage0Error as exc:
        raise HTTPException(
            409 if exc.status in ("RECONCILIATION_REQUIRED", "CONCURRENT_RUN_BLOCKED") else 503,
            {"stage0_status": exc.status, "reason": exc.detail},
        ) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    finally:
        if token is not None:
            try:
                await batch.release(token)
            except Exception:
                pass


@app.get("/stage0/crypto/checkpoint", dependencies=[Depends(require_token)])
async def stage0_crypto_checkpoint(day: str, symbol: str = "BTC"):
    """Authenticated Redis-only read of crypto daily checkpoint; no mutation."""
    from stage0_crypto_checkpoint_read import inspect_crypto_checkpoint
    from stage0_ingestor import Stage0Error
    if provider.state is None:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE")
    try:
        status = await inspect_crypto_checkpoint(
            provider.state, day=day, symbol=symbol
        )
        # Non-secret runtime configuration, read-only: lets operators verify
        # that the opt-in Sheets writer has actually been disabled.
        status["crypto_write_enabled"] = (
            os.getenv("ASKLIAM_CRYPTO_WRITE_ENABLED", "").lower() == "true"
        )
        # Native-perp and Coinbase spot checkpoints are independent. Expose a
        # strictly validated spot completed set to the scheduled Stage0 driver
        # so it skips BTC/ETH already fully enriched by Coinbase today.
        from stage0_crypto_coinbase_commit import spot_checkpoint_key
        from stage0_crypto_coinbase_prepare import PRICE_SOURCE
        from stage0_crypto_probe import COINGECKO_IDS
        raw_spot_cp = await provider.state.call("GET", spot_checkpoint_key(day))
        if raw_spot_cp:
            try:
                spot_cp = json.loads(raw_spot_cp)
                spot_completed = spot_cp["completed_symbols"]
                if (
                    spot_cp.get("version") != VERSION
                    or spot_cp.get("utc_date") != day
                    or spot_cp.get("provider") != PRICE_SOURCE
                    or spot_cp.get("status") != "COMMITTED_VERIFY_PASS"
                    or not isinstance(spot_completed, list)
                    or len(set(spot_completed)) != len(spot_completed)
                    or any(s not in COINGECKO_IDS for s in spot_completed)
                ):
                    raise ValueError("SPOT_CHECKPOINT_INVALID")
            except (ValueError, KeyError, TypeError):
                raise Stage0Error(
                    "RECONCILIATION_REQUIRED", "Coinbase checkpoint invalid"
                ) from None
            status["spot_completed_symbols"] = sorted(spot_completed)
        else:
            status["spot_completed_symbols"] = []
        return status
    except Stage0Error as exc:
        code = 422 if exc.status in (
            "CRYPTO_SYMBOL_NOT_MAPPED", "INVALID_DATE"
        ) else 409
        raise HTTPException(code, {"stage0_status": exc.status}) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None


class CryptoAutomaticStepRequest(BaseModel):
    """Authenticated, environment-gated single crypto Stage0 step."""
    symbol: str = Field(min_length=2, max_length=12)


@app.get("/stage0/auto/config", dependencies=[Depends(require_token)])
async def stage0_auto_config():
    """No secrets. Used by the scheduled runner to fail closed."""
    return {
        "version": VERSION,
        "stage0_auto_enabled": os.getenv("ASKLIAM_STAGE0_AUTO_ENABLED", "").lower() == "true",
        "crypto_auto_enabled": os.getenv("ASKLIAM_CRYPTO_AUTO_ENABLED", "").lower() == "true",
        "fmp_stage0_enabled": os.getenv("ASKLIAM_STAGE0_ENABLED", "").lower() == "true",
        "manual_crypto_write_enabled": os.getenv("ASKLIAM_CRYPTO_WRITE_ENABLED", "").lower() == "true",
        "redis_configured": provider.state is not None,
        "sheets_configured": bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") and os.getenv("ASKLIAM_SPREADSHEET_ID")),
        "ranking_permitted": False,
        "execution_permitted": False,
    }


@app.post("/stage0/auto/crypto-step", dependencies=[Depends(require_token)])
async def stage0_auto_crypto_step(request: CryptoAutomaticStepRequest):
    """Only the scheduled driver may request server-side gated ingestion.

    This route NEVER bypasses Stage0 Redis's global pending journal, shared
    exclusive lease, Google Sheets state check, or independent readback.
    """
    from stage0_crypto_commit import (
        commit_one_native_perp, confirmation_for, automation_guard,
    )
    from stage0_crypto_probe import COINGECKO_IDS
    from stage0_daily import read_universe_cached
    from stage0_ingestor import Stage0Error
    symbol = request.symbol.strip().upper()
    try:
        automation_guard(symbol, confirmation_for(symbol))
        if provider.state is None or provider.client is None:
            raise HTTPException(503, "SOURCE_UNAVAILABLE")
        universe = await read_universe_cached(provider.state)
        eligible = {entry["symbol"] for entry in universe
                    if entry.get("route") == "CRYPTO_PROVIDER_PENDING"}
        if symbol not in COINGECKO_IDS or symbol not in eligible:
            raise HTTPException(422, "CRYPTO_SYMBOL_NOT_ENABLED")
        return await commit_one_native_perp(
            symbol, confirmation_for(symbol), redis=provider.state,
            client=provider.client, automation_mode=True,
        )
    except HTTPException:
        raise
    except Stage0Error as exc:
        code = 403 if exc.status in (
            "CRYPTO_AUTO_DISABLED", "MANUAL_CRYPTO_WRITE_STILL_ENABLED",
            "EXPLICIT_CONFIRMATION_REQUIRED",
        ) else 409 if exc.status in (
            "RECONCILIATION_REQUIRED", "ALREADY_COMMITTED_TODAY",
            "CONCURRENT_RUN_BLOCKED", "CHECKPOINT_NOT_COMMITTED", "SNAPSHOT_COLLISION", "ALREADY_SPOT_ACQUIRED_TODAY",
        ) else 503
        raise HTTPException(code, {
            "stage0_status": exc.status,
            "checkpoint_advanced": False,
            "ranking_permitted": False, "execution_permitted": False,
        }) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(503, {
            "stage0_status": "AUTO_CRYPTO_COMMIT_HALTED",
            "checkpoint_advanced": False, "ranking_permitted": False,
            "execution_permitted": False,
        }) from None


class CryptoSingleCommitRequest(BaseModel):
    symbol: str = Field(min_length=2, max_length=12)
    confirmation: str = Field(min_length=8, max_length=100)


@app.post("/stage0/crypto/commit-one", dependencies=[Depends(require_token)])
async def stage0_crypto_commit_one(request: CryptoSingleCommitRequest):
    """Explicit opt-in single-symbol Sheets commit; DISABLED BY DEFAULT.

    Must enable ASKLIAM_CRYPTO_WRITE_ENABLED=true intentionally on Render.
    No ranking, no orders, no FMP quota and no FMP checkpoint pollution.
    Partial/uncertain Sheets writes retain the global Redis pending journal.
    """
    from stage0_crypto_commit import (
        commit_one_native_perp, feature_guard,
    )
    from stage0_crypto_probe import COINGECKO_IDS
    from stage0_daily import read_universe_cached
    from stage0_ingestor import Stage0Error
    symbol = request.symbol.strip().upper()
    try:
        feature_guard(symbol, request.confirmation)
        if provider.state is None or provider.client is None:
            raise HTTPException(503, "SOURCE_UNAVAILABLE")
        universe = await read_universe_cached(provider.state)
        candidates = {
            row["symbol"] for row in universe
            if row.get("route") == "CRYPTO_PROVIDER_PENDING"
        }
        if symbol not in candidates or symbol not in COINGECKO_IDS:
            raise HTTPException(422, "CRYPTO_SYMBOL_NOT_ENABLED")
        return await commit_one_native_perp(
            symbol, request.confirmation, redis=provider.state,
            client=provider.client,
        )
    except HTTPException:
        raise
    except Stage0Error as exc:
        status = (
            403 if exc.status in (
                "CRYPTO_WRITE_DISABLED", "EXPLICIT_CONFIRMATION_REQUIRED"
            ) else
            409 if exc.status in (
                "RECONCILIATION_REQUIRED", "CONCURRENT_RUN_BLOCKED",
                "ALREADY_COMMITTED_TODAY", "SNAPSHOT_COLLISION", "ALREADY_SPOT_ACQUIRED_TODAY",
                "CHECKPOINT_NOT_COMMITTED"
            ) else 503
        )
        raise HTTPException(
            status, {
                "stage0_status": exc.status,
                "checkpoint_advanced": False,
                "ranking_permitted": False,
                "execution_permitted": False,
            }
        ) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(
            503, {
                "stage0_status": "CRYPTO_COMMIT_HALTED",
                "checkpoint_advanced": False,
                "ranking_permitted": False,
                "execution_permitted": False,
            }
        ) from None


@app.get("/stage0/crypto/sheet-dry-run", dependencies=[Depends(require_token)])
async def stage0_crypto_sheet_dry_run(symbol: str):
    """Read-only live Hyperliquid acquisition + Google Sheets UPSERT diff.

    No sheets commit, no Redis journal write, no score/trade action.
    Reject if a global pending reconciliation exists before making reads.
    """
    from stage0_batch import Stage0BatchCoordinator
    from stage0_crypto_probe import COINGECKO_IDS
    from stage0_crypto_ingest_prepare import preview_native_perp_stage0
    from stage0_crypto_sheet_preflight import read_only_sheet_preflight
    from stage0_daily import read_universe_cached
    from stage0_ingestor import Stage0Error
    if provider.state is None or provider.client is None:
        raise HTTPException(503, "SOURCE_PROBE_UNAVAILABLE")
    symbol = str(symbol).strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol) or symbol not in COINGECKO_IDS:
        raise HTTPException(422, "CRYPTO_SYMBOL_NOT_MAPPED")
    try:
        state = await Stage0BatchCoordinator(provider.state).status()
        if state["pending_reconciliation"]:
            raise HTTPException(409, "RECONCILIATION_REQUIRED")
        universe = await read_universe_cached(provider.state)
        eligible = {
            entry["symbol"] for entry in universe
            if entry.get("route") == "CRYPTO_PROVIDER_PENDING"
        }
        if symbol not in eligible:
            raise HTTPException(422, "CRYPTO_SYMBOL_NOT_ENABLED")
        return await preview_native_perp_stage0(
            symbol, client=provider.client,
            sheet_preflight=read_only_sheet_preflight,
        )
    except HTTPException:
        raise
    except Stage0Error as exc:
        raise HTTPException(503, {"stage0_status": exc.status}) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(503, "CRYPTO_SHEET_DRY_RUN_UNAVAILABLE") from None


@app.get("/stage0/crypto/ingest-preview", dependencies=[Depends(require_token)])
async def stage0_crypto_ingest_preview(symbol: str):
    """STRICTLY READ ONLY: prove native-perp OHLCV can become Stage0 frames.

    Does not call writer, Redis journal/checkpoint, score engine or broker.
    Not equivalent to an approved transactional import.
    """
    from stage0_crypto_ingest_prepare import preview_native_perp_stage0
    from stage0_crypto_probe import COINGECKO_IDS
    from stage0_daily import read_universe_cached
    from stage0_ingestor import Stage0Error
    if provider.state is None or provider.client is None:
        raise HTTPException(503, "SOURCE_PROBE_UNAVAILABLE")
    symbol = str(symbol).strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol) or symbol not in COINGECKO_IDS:
        raise HTTPException(422, "CRYPTO_SYMBOL_NOT_MAPPED")
    try:
        universe = await read_universe_cached(provider.state)
        crypto_symbols = {
            item["symbol"] for item in universe
            if item.get("route") == "CRYPTO_PROVIDER_PENDING"
        }
        if symbol not in crypto_symbols:
            raise HTTPException(422, "CRYPTO_SYMBOL_NOT_ENABLED")
        return await preview_native_perp_stage0(
            symbol, client=provider.client,
        )
    except HTTPException:
        raise
    except Stage0Error as exc:
        raise HTTPException(503, {"stage0_status": exc.status}) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(503, "CRYPTO_STAGE0_PREVIEW_UNAVAILABLE") from None


@app.get("/stage0/crypto/history-probe", dependencies=[Depends(require_token)])
async def stage0_crypto_history_probe(symbol: str):
    """Explicit one-asset read-only Hyperliquid native perpetual history test.

    No CoinGecko, no market data persistence, no analytical ranking, and no
    execution. Futures/perpetual prices must not masquerade as spot prices.
    """
    from stage0_crypto_history_probe import probe_hyperliquid_history
    from stage0_crypto_probe import COINGECKO_IDS
    from stage0_daily import read_universe_cached
    from stage0_ingestor import Stage0Error
    if provider.state is None or provider.client is None:
        raise HTTPException(503, "SOURCE_PROBE_UNAVAILABLE")
    sym = symbol.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(sym) or sym not in COINGECKO_IDS:
        raise HTTPException(422, "CRYPTO_SYMBOL_NOT_MAPPED")
    try:
        universe = await read_universe_cached(provider.state)
        crypto_symbols = {
            item["symbol"] for item in universe
            if item.get("route") == "CRYPTO_PROVIDER_PENDING"
        }
        if sym not in crypto_symbols:
            raise HTTPException(422, "CRYPTO_SYMBOL_NOT_ENABLED")
        return await probe_hyperliquid_history(
            sym, client=provider.client,
        )
    except HTTPException:
        raise
    except Stage0Error as exc:
        raise HTTPException(503, {"stage0_status": exc.status}) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(503, "CRYPTO_HISTORY_PROBE_UNAVAILABLE") from None


@app.get("/stage0/crypto/independent-spot-preview", dependencies=[Depends(require_token)])
async def stage0_independent_spot_preview(symbol: str):
    """Read-only CoinGecko spot-price-sample check. Never creates OHLC or scores."""
    from stage0_crypto_spot_adapter import independent_spot_preview
    from stage0_crypto_spot_history import SpotDataError
    if provider.client is None:
        raise HTTPException(503, "SOURCE_UNAVAILABLE")
    try:
        return await independent_spot_preview(symbol, provider.client)
    except SpotDataError as exc:
        code = 429 if str(exc) == "SPOT_RATE_LIMIT_BACKOFF" else 422 if str(exc) in (
            "SPOT_UNKNOWN_SYMBOL", "SPOT_INSUFFICIENT_HISTORY",
            "SPOT_DAILY_GAP", "SPOT_DATE_MISMATCH", "SPOT_STALE",
        ) else 503
        raise HTTPException(code, {
            "status": str(exc), "mandatory_broad_pass": False,
            "ranking_permitted": False, "execution_permitted": False,
        }) from None


@app.get("/stage0/crypto/coinbase-spot-preview", dependencies=[Depends(require_token)])
async def stage0_coinbase_spot_preview(symbol: str):
    """Validated exchange spot OHLCV source preview, read only."""
    from stage0_crypto_coinbase_spot import probe_spot, CoinbaseSpotError
    if provider.client is None:
        raise HTTPException(503, {"stage0_status":"SOURCE_UNAVAILABLE"})
    try:
        return await probe_spot(symbol, provider.client)
    except CoinbaseSpotError as exc:
        status = 429 if str(exc) == "COINBASE_RATE_LIMIT" else 503
        raise HTTPException(status, {
            "stage0_status":str(exc),
            "ranking_permitted":False,
            "execution_permitted":False,
            "sheet_writes_performed":False,
        }) from None


@app.get("/stage0/crypto/spot-ohlcv-preview", dependencies=[Depends(require_token)])
async def stage0_spot_ohlcv_preview(symbol: str):
    """Independent daily spot OHLCV quality probe; no sheets or scoring writes."""
    from stage0_crypto_spot_ohlcv import probe_ohlc, OHLCError
    from stage0_canonical_factors import canonical_technicals
    if provider.client is None:
        raise HTTPException(503, "SOURCE_UNAVAILABLE")
    try:
        result = await probe_ohlc(symbol, provider.client)
        tech = canonical_technicals([r["close"] for r in result["rows"]])
        return {
            "version": VERSION, "symbol": result["symbol"],
            "source": result["source"], "daily_bars": result["daily_bars"],
            "last_complete_utc_date": result["rows"][-1]["trading_date"],
            "canonical_technicals": tech,
            "ranking_permitted": False, "execution_permitted": False,
            "sheet_writes_performed": False, "status": "OHLCV_PREVIEW_ONLY",
        }
    except OHLCError as exc:
        raise HTTPException(503, {
            "stage0_status": str(exc), "ranking_permitted": False,
            "execution_permitted": False,
        }) from None


@app.get("/stage0/crypto/source-probe", dependencies=[Depends(require_token)])
async def stage0_crypto_source_probe():
    """Read-only provider preview of Universe CRYPTO_V1; does not persist prices."""
    from stage0_crypto_probe import probe_crypto_sources
    from stage0_daily import read_universe_cached
    from stage0_ingestor import Stage0Error
    if provider.state is None or provider.client is None:
        raise HTTPException(503, "SOURCE_PROBE_UNAVAILABLE")
    try:
        universe = await read_universe_cached(provider.state)
        symbols = [
            item["symbol"] for item in universe
            if item.get("route") == "CRYPTO_PROVIDER_PENDING"
        ]
        return await probe_crypto_sources(
            symbols, client=provider.client,
            coingecko_key=os.getenv("COINGECKO_DEMO_API_KEY") or None,
        )
    except Stage0Error as exc:
        raise HTTPException(503, {"stage0_status": exc.status}) from None
    except ValueError:
        raise HTTPException(503, "CRYPTO_UNIVERSE_MAPPING_INVALID") from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(503, "CRYPTO_SOURCE_PROBE_UNAVAILABLE") from None


@app.get("/stage0/daily/plan", dependencies=[Depends(require_token)])
async def stage0_daily_plan():
    from stage0_batch import Stage0BatchCoordinator
    from stage0_daily import read_universe_cached, plan_from_universe
    from stage0_ingestor import Stage0Error
    try:
        status = await Stage0BatchCoordinator(provider.state).status()
        universe = await read_universe_cached(provider.state)
        return plan_from_universe(universe, status)
    except Stage0Error as exc:
        raise HTTPException(503, {"stage0_status": exc.status, "reason": exc.detail}) from None
    except Exception:
        raise HTTPException(503, "DAILY_PLAN_UNAVAILABLE") from None

@app.post("/stage0/daily/step", dependencies=[Depends(require_token)])
async def stage0_daily_step():
    if os.getenv("ASKLIAM_STAGE0_ENABLED", "").lower() != "true":
        raise HTTPException(503, "STAGE0_DISABLED")
    from stage0_batch import Stage0BatchCoordinator
    from stage0_daily import read_universe_cached, plan_from_universe
    from stage0_ingestor import Stage0Error
    batch = Stage0BatchCoordinator(provider.state)
    token = None
    try:
        token = await batch.acquire()
        status = await batch.status()
        if status["pending_reconciliation"]:
            raise Stage0Error("RECONCILIATION_REQUIRED", "Unresolved journal")
        universe = await read_universe_cached(provider.state)
        plan = plan_from_universe(universe, status)
        if not plan["next_symbols"]:
            return {"version": VERSION, "status": "FMP_QUEUE_EMPTY", "daily_plan": plan, "ranking_permitted": False}
        symbol = plan["next_symbols"][0]
        day, run_id, previous = await batch.prepare([symbol], token)
        try:
            result = await asyncio.to_thread(batch.run_blocking, [symbol], run_id)
        except Stage0Error as exc:
            if exc.status == "PROVIDER_DEFERRED":
                code = exc.detail.rsplit(": ", 1)[-1]
                deferred_errors = {
                    "FMP_HTTP_401", "FMP_HTTP_402", "FMP_HTTP_403",
                    "FMP_HTTP_404", "FMP_SYMBOL_NOT_FOUND",
                    "FMP_QUOTES_MISSING", "FMP_HISTORICAL_EMPTY",
                    "FMP_ACQUISITION_INCOMPLETE", "FMP_PROVIDER_ERROR",
                    "NO_ASSET_DATA_OBTAINED",
                }
                if code in deferred_errors:
                    await batch.mark_deferred_prewrite([symbol], exc.detail, token)
                    return {"version": VERSION, "status": "PROVIDER_DEFERRED",
                            "symbol": symbol, "reason": exc.detail,
                            "checkpoint_advanced": False, "ranking_permitted": False}
                await batch.abort_prewrite([symbol], token)
                return {"version": VERSION, "status": "SOURCE_RETRYABLE",
                        "symbol": symbol, "reason": exc.detail,
                        "checkpoint_advanced": False, "ranking_permitted": False}
            raise
        result = await batch.finish(day, run_id, previous, [symbol], result, token)
        result["daily_symbol"] = symbol
        return result
    except Stage0Error as exc:
        raise HTTPException(
            409 if exc.status in ("CONCURRENT_RUN_BLOCKED", "RECONCILIATION_REQUIRED") else 503,
            {"stage0_status": exc.status, "reason": exc.detail,
             "checkpoint_advanced": False, "ranking_permitted": False},
        ) from None
    except Exception:
        raise HTTPException(503, {"stage0_status": "HALTED",
                                  "checkpoint_advanced": False, "ranking_permitted": False}) from None
    finally:
        if token is not None:
            try:
                await batch.release(token)
            except Exception:
                pass

@app.get("/stage0/check", dependencies=[Depends(require_token)])
async def stage0_check():
    """Authenticated, read-only Stage 0 Google Sheets access diagnostic."""
    configured = {
        "google_credentials_present": bool(os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")),
        "spreadsheet_id_present": bool(os.environ.get("ASKLIAM_SPREADSHEET_ID")),
        "ingestion_enabled": os.getenv("ASKLIAM_STAGE0_ENABLED", "").lower() == "true",
    }
    if not configured["google_credentials_present"] or not configured["spreadsheet_id_present"]:
        return {"version": VERSION, "status": "NOT_CONFIGURED", **configured}
    def verify():
        from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error
        from stage0_ingestor import TABS
        # No providers are called; this endpoint performs no writes.
        probe = Stage0_Daily_Ingestor(providers={"read_only": lambda: {}})
        captured = probe._capture(probe._open())
        return {
            "version": VERSION,
            "status": "PASS",
            **configured,
            "tabs": {name: {"readable": True, "rows": len(captured[name]["rows"])}
                     for name in TABS},
        }
    from stage0_ingestor import Stage0Error
    try:
        return await asyncio.to_thread(verify)
    except Stage0Error as exc:
        raise HTTPException(503, {
            "version": VERSION, "status": "CHECK_FAILED",
            "code": exc.status, "reason": exc.detail,
        }) from None
    except Exception as exc:
        raise HTTPException(503, {
            "version": VERSION, "status": "CHECK_FAILED",
            "code": type(exc).__name__,
        }) from None

# Coinbase spot ingestion is separated from existing native-perp checkpoints.
# All writes disabled unless explicitly scoped in Render and authenticated.
class CoinbaseSpotCommitRequest(BaseModel):
    symbol: str = Field(min_length=2, max_length=12)
    confirmation: str = Field(min_length=8, max_length=100)


@app.get("/stage0/crypto/coinbase-spot-ingest-preflight", dependencies=[Depends(require_token)])
async def coinbase_spot_ingest_preflight(symbol: str):
    """Real source + Sheets diff, but NO writes, checkpoint or ranking."""
    from stage0_crypto_coinbase_commit import spot_preflight
    from stage0_ingestor import Stage0Error
    if provider.state is None or provider.client is None:
        raise HTTPException(503, "SOURCE_UNAVAILABLE")
    try:
        return await spot_preflight(symbol.strip().upper(), redis=provider.state, client=provider.client)
    except Stage0Error as exc:
        raise HTTPException(409 if exc.status in (
            "RECONCILIATION_REQUIRED", "SPOT_SNAPSHOT_COLLISION"
        ) else 503, {
            "stage0_status": exc.status,
            "sheet_writes_performed": False, "checkpoint_advanced": False,
            "ranking_permitted": False, "execution_permitted": False,
        }) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(503, "COINBASE_SPOT_PREFLIGHT_HALTED") from None


@app.post("/stage0/crypto/coinbase-spot-commit-one", dependencies=[Depends(require_token)])
async def coinbase_spot_commit_one(request: CoinbaseSpotCommitRequest):
    """Explicit, scoped opt-in transaction. Never ranks or places orders."""
    from stage0_crypto_coinbase_commit import spot_commit_one
    from stage0_ingestor import Stage0Error
    if provider.state is None or provider.client is None:
        raise HTTPException(503, "SOURCE_UNAVAILABLE")
    try:
        return await spot_commit_one(
            request.symbol, request.confirmation,
            redis=provider.state, client=provider.client,
        )
    except Stage0Error as exc:
        http = 403 if exc.status in (
            "SPOT_WRITE_DISABLED", "SPOT_WRITE_SCOPE_DENIED",
            "EXPLICIT_CONFIRMATION_REQUIRED"
        ) else 409 if exc.status in (
            "RECONCILIATION_REQUIRED", "SPOT_SNAPSHOT_COLLISION",
            "ALREADY_COMMITTED_TODAY", "CONCURRENT_RUN_BLOCKED",
            "CHECKPOINT_NOT_COMMITTED",
        ) else 503
        raise HTTPException(http, {
            "stage0_status": exc.status, "checkpoint_advanced": False,
            "ranking_permitted": False, "execution_permitted": False,
        }) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(503, {
            "stage0_status": "SPOT_COMMIT_HALTED",
            "ranking_permitted": False, "execution_permitted": False,
        }) from None


@app.get("/stage0/crypto/coinbase-spot-checkpoint", dependencies=[Depends(require_token)])
async def coinbase_spot_checkpoint(day: str, symbol: str):
    """Independent Redis-only spot checkpoint reader; no provider calls."""
    from stage0_crypto_coinbase_commit import spot_checkpoint_key
    from stage0_crypto_coinbase_prepare import PRICE_SOURCE
    from stage0_crypto_probe import COINGECKO_IDS
    from stage0_batch import pending_key
    from datetime import date
    if provider.state is None:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE")
    try:
        parsed = date.fromisoformat(day)
        canonical = symbol.strip().upper()
        if (
            day != parsed.isoformat() or parsed > datetime.now(timezone.utc).date()
            or canonical not in COINGECKO_IDS
        ):
            raise ValueError("Invalid day or symbol")
    except (ValueError, TypeError):
        raise HTTPException(422, "INVALID_DATE_OR_SYMBOL") from None
    try:
        raw = await provider.state.call("GET", spot_checkpoint_key(day))
        pending = await provider.state.call("GET", pending_key(day))
        if raw:
            cp = json.loads(raw)
            if (
                cp.get("version") != VERSION or cp.get("utc_date") != day
                or cp.get("provider") != PRICE_SOURCE
                or cp.get("status") != "COMMITTED_VERIFY_PASS"
                or not isinstance(cp.get("completed_symbols"), list)
            ):
                raise ValueError("Invalid stored spot checkpoint")
            completed = cp["completed_symbols"]
        else:
            completed, cp = [], {}
        return {
            "version": VERSION, "utc_date": day, "symbol": canonical,
            "status": cp.get("status", "NOT_FOUND"),
            "symbol_completed": canonical in completed,
            "symbol_checkpoint_verified": canonical in completed and not bool(pending),
            "pending_reconciliation": bool(pending),
            "last_verified_run_id": cp.get("last_verified_run_id"),
            "sheet_writes_performed": False, "checkpoint_advanced": False,
            "ranking_permitted": False, "execution_permitted": False,
        }
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(409, "SPOT_CHECKPOINT_INVALID") from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None


@app.post("/stage0/auto/runtime-evidence-sync", dependencies=[Depends(require_token)])
async def sync_live_stage0_evidence():
    """Authorized non-secret mirror into FMP_Runtime_State only.

    It does not change Ranking/Run_Manifest/Lifecycle/AI Trades.
    """
    from stage0_runtime_evidence import sync_stage0_evidence
    from stage0_ingestor import Stage0Error
    if provider.state is None:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE")
    try:
        return await sync_stage0_evidence(provider.state)
    except Stage0Error as exc:
        http=409 if exc.status in (
            "RECONCILIATION_REQUIRED", "CONCURRENT_RUN_BLOCKED"
        ) else 503
        raise HTTPException(http, {
            "stage0_status":exc.status, "ranking_permitted":False,
            "execution_permitted":False,
        }) from None
    except StorageError:
        raise HTTPException(503, "UPSTASH_UNAVAILABLE") from None
    except Exception:
        raise HTTPException(503, "STAGE0_EVIDENCE_SYNC_HALTED") from None
