"""AskLiam V6.5.1 FMP provider. Python 3.11+; never logs credentials."""
from __future__ import annotations
import asyncio
import hashlib
import json
import math
import os
import sqlite3
import time
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
DATA_DIR = Path(os.environ.get("ASKLIAM_DATA_DIR", str(Path.home() / ".askliam" / "v6.5.1")))
DB = DATA_DIR / "fmp_quota_v6_5_1.sqlite3"
TTL = {"quote": 900, "profile": 604800, "historical": 86400}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def day_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def db_conn():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB, timeout=30, isolation_level=None)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def init_db():
    with db_conn() as c:
        c.execute("CREATE TABLE IF NOT EXISTS quota(day TEXT PRIMARY KEY, used INTEGER NOT NULL, blocked INTEGER NOT NULL DEFAULT 0, reason TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS cache(key TEXT PRIMARY KEY, payload TEXT NOT NULL, source TEXT NOT NULL, fetched INTEGER NOT NULL, expires INTEGER NOT NULL)")


def quota_state():
    with db_conn() as c:
        row = c.execute("SELECT used,blocked,reason FROM quota WHERE day=?", (day_utc(),)).fetchone()
    used, blocked, reason = row if row else (0, 0, None)
    return {"utc_date": day_utc(), "used": used, "remaining": max(0, CAP - used), "blocked": bool(blocked or used >= CAP), "reason": reason}


def reserve() -> bool:
    # Atomic across independent processes sharing the SAME persistent SQLite volume.
    with db_conn() as c:
        c.execute("BEGIN IMMEDIATE")
        try:
            day = day_utc()
            c.execute("INSERT OR IGNORE INTO quota(day,used,blocked) VALUES (?,0,0)", (day,))
            used, blocked = c.execute("SELECT used,blocked FROM quota WHERE day=?", (day,)).fetchone()
            if blocked or used >= CAP:
                c.execute("COMMIT")
                return False
            c.execute("UPDATE quota SET used=used+1 WHERE day=?", (day,))
            c.execute("COMMIT")
            return True
        except BaseException:
            c.execute("ROLLBACK")
            raise


def block_day(reason: str):
    with db_conn() as c:
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute("INSERT INTO quota(day,used,blocked,reason) VALUES (?,0,1,?) ON CONFLICT(day) DO UPDATE SET blocked=1,reason=excluded.reason", (day_utc(), reason))
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise


def key_for(kind: str, symbol: str) -> str:
    # No API key in cache key, URL, logs or response.
    return hashlib.sha256(f"{VERSION}:{kind}:{symbol}".encode()).hexdigest()


def cache_get(kind: str, symbol: str):
    with db_conn() as c:
        row = c.execute("SELECT payload,source,fetched,expires FROM cache WHERE key=? AND expires>?", (key_for(kind, symbol), int(time.time()))).fetchone()
    if not row:
        return None
    return {"data": json.loads(row[0]), "source": row[1], "data_timestamp": datetime.fromtimestamp(row[2], timezone.utc).isoformat(), "expires_at": datetime.fromtimestamp(row[3], timezone.utc).isoformat(), "cache_hit": True}


def cache_put(kind: str, symbol: str, payload: Any):
    ts = int(time.time())
    with db_conn() as c:
        c.execute("INSERT INTO cache(key,payload,source,fetched,expires) VALUES (?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,source=excluded.source,fetched=excluded.fetched,expires=excluded.expires", (key_for(kind, symbol), json.dumps(payload, allow_nan=False), "FMP", ts, ts + TTL[kind]))


def finite(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (ValueError, TypeError, OverflowError):
        return None


def technicals(data: Any):
    bars = data.get("historical", []) if isinstance(data, dict) else []
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
        self.semaphore = asyncio.Semaphore(4)
        self.lock = asyncio.Lock()
        self.inflight: dict[str, asyncio.Task] = {}
        self.cache_hits = 0
        self.network_calls = 0

    async def start(self):
        self.client = httpx.AsyncClient(base_url=ROOT, http2=True, timeout=20, limits=httpx.Limits(max_connections=8, max_keepalive_connections=4), follow_redirects=False)

    async def stop(self):
        if self.client:
            await self.client.aclose()

    async def get(self, kind: Literal["quote", "profile", "historical"], symbol: str):
        symbol = symbol.upper().strip()
        cached = cache_get(kind, symbol)
        if cached is not None:
            self.cache_hits += 1
            return {"status": "READY", **cached}
        if not os.environ.get("FMP_API_KEY"):
            return {"status": "PROVIDER_DEFERRED", "error": "MISSING_FMP_API_KEY"}
        k = key_for(kind, symbol)
        async with self.lock:
            task = self.inflight.get(k)
            if task is None:
                task = asyncio.create_task(self._network(kind, symbol))
                self.inflight[k] = task
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                async with self.lock:
                    if self.inflight.get(k) is task:
                        self.inflight.pop(k, None)

    async def _network(self, kind, symbol):
        async with self.semaphore:
            cached = cache_get(kind, symbol)
            if cached is not None:
                self.cache_hits += 1
                return {"status": "READY", **cached}
            if quota_state()["blocked"] or not reserve():
                return {"status": "PROVIDER_DEFERRED", "error": "FMP_QUOTA_EXHAUSTED"}
            self.network_calls += 1
            path = {"quote": "/api/v3/quote/", "profile": "/api/v3/profile/", "historical": "/api/v3/historical-price-full/"}[kind] + symbol
            try:
                response = await self.client.get(path, params={"apikey": os.environ["FMP_API_KEY"]})
            except httpx.RequestError:
                return {"status": "PROVIDER_DEFERRED", "error": "FMP_NETWORK_ERROR"}
            if response.status_code == 429:
                block_day("HTTP_429")
                return {"status": "PROVIDER_DEFERRED", "error": "FMP_HTTP_429"}
            if response.status_code >= 400:
                return {"status": "PROVIDER_DEFERRED", "error": f"FMP_HTTP_{response.status_code}"}
            try:
                data = response.json()
            except ValueError:
                return {"status": "PROVIDER_DEFERRED", "error": "INVALID_JSON"}
            if isinstance(data, dict) and ("Error Message" in data or "error" in data):
                return {"status": "PROVIDER_DEFERRED", "error": "FMP_PROVIDER_ERROR"}
            if kind in ("quote", "profile"):
                data = next((x for x in data if isinstance(x, dict) and str(x.get("symbol", "")).upper() == symbol), None) if isinstance(data, list) else data
            if not data:
                return {"status": "DATA_WAIT", "error": "EMPTY_VALID_RESPONSE"}
            cache_put(kind, symbol, data)
            return {"status": "READY", "data": data, "source": "FMP", "data_timestamp": now_utc(), "cache_hit": False}

    async def batch(self, query: Query):
        symbols = list(dict.fromkeys(s.strip().upper() for s in query.symbols if s.strip()))
        # No fixed 40-symbol limit. Per-symbol fallback is used only for this
        # account-compatible endpoint; quota is reserved before every call.
        async def one(symbol, index):
            q = await self.get("quote", symbol)
            p = await self.get("profile", symbol) if query.include_profiles else None
            h = await self.get("historical", symbol) if query.include_historical and index < query.historical_budget else None
            fields = {}
            if q["status"] == "READY":
                d = q["data"]
                fields.update({"underlying_price": finite(d.get("price")), "volume": finite(d.get("volume")), "market_cap": finite(d.get("marketCap")), "currency": d.get("currency")})
            if p and p["status"] == "READY":
                d = p["data"]
                fields.update({"sector": d.get("sector"), "industry": d.get("industry"), "exchange": d.get("exchangeShortName"), "market_cap": fields.get("market_cap") or finite(d.get("mktCap"))})
            if h and h["status"] == "READY":
                fields.update(technicals(h["data"]))
            fields = {k:v for k,v in fields.items() if v is not None}
            statuses = [x["status"] for x in (q,p,h) if x is not None]
            status = "PROVIDER_DEFERRED" if "PROVIDER_DEFERRED" in statuses else ("DATA_WAIT" if "DATA_WAIT" in statuses else "ACQUIRED_NOT_SCORED")
            return {"canonical_symbol": symbol, "enrichment_status": status, "fields": fields, "source": "FMP", "data_timestamp": now_utc(), "missing_fields": [], "request_errors": [x.get("error") for x in (q,p,h) if x and x.get("error")], "historical": h["data"] if h and h["status"] == "READY" else None}
        rows = await asyncio.gather(*(one(s,i) for i,s in enumerate(symbols)))
        return {"version": VERSION, "results": rows, "fmp_quota": quota_state(), "cache_hits_this_process": self.cache_hits, "network_calls_this_process": self.network_calls}


provider = Provider()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
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
def health():
    return {"version": VERSION, "service": "askliam-fmp-provider", "status": "running", "fmp_configured": bool(os.environ.get("FMP_API_KEY")), "persistent_db_path": str(DB)}

@app.get("/quota", dependencies=[Depends(require_token)])
def quota():
    return {"version": VERSION, **quota_state()}

@app.post("/enrich", dependencies=[Depends(require_token)])
async def enrich(query: Query):
    return await provider.batch(query)
