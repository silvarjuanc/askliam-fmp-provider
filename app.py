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
            if p and p["status"] == "READY":
                v = p["data"]
                fields.update({"sector": v.get("sector"), "industry": v.get("industry"), "currency": v.get("currency"), "exchange": v.get("exchangeShortName")})
                if fields.get("market_cap") is None: fields["market_cap"] = finite(v.get("mktCap"))
            if h and h["status"] == "READY":
                fields.update(technicals(h["data"]))
            errors = [v["error"] for v in (q, p, h) if v and v.get("error")]
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    await provider.start()
    yield
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
