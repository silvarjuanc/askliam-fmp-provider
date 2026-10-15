"""V6.5.1: disabled-by-default Coinbase spot Stage0 transaction.

Independent spot OHLCV and fresh quote from Coinbase; derivative context from
Hyperliquid only for funding/OI. Shared Redis lease/pending journal with FMP and
native-perp writes. Separate source-specific verified checkpoint.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd

from stage0_batch import Stage0BatchCoordinator, LOCK_KEY, pending_key
from stage0_crypto_coinbase_spot import ENDPOINT
from stage0_crypto_coinbase_prepare import prepare_coinbase_spot_frames, PRICE_SOURCE
from stage0_crypto_commit import COMMIT_LUA
from stage0_crypto_ingest_prepare import SOURCE_PRICE
from stage0_crypto_probe import COINGECKO_IDS, HYPERLIQUID_INFO
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

SPOT_CHECKPOINT_PREFIX = "askliam:v651:stage0:crypto:spot:checkpoint:"
SPOT_CONFIRM_PREFIX = "COMMIT_ONE_COINBASE_SPOT_"
SPOT_CONFIRM_SUFFIX = "_STAGE0_V651"


def spot_checkpoint_key(day: str) -> str:
    return SPOT_CHECKPOINT_PREFIX + day


def confirm_spot(symbol: str) -> str:
    return SPOT_CONFIRM_PREFIX + symbol + SPOT_CONFIRM_SUFFIX


def spot_automation_guard(symbol: str, confirmation: str) -> None:
    """Dedicated cron-only BTC/ETH spot transaction gate (not manual mode).

    The service's authenticated /auto route is the only invoker. Source,
    journal, readback and checkpoint rules are unchanged.
    """
    if os.getenv("ASKLIAM_COINBASE_SPOT_AUTO_ENABLED","false").lower()!="true":
        raise Stage0Error("SPOT_AUTO_DISABLED","Automatic independent spot source disabled")
    if os.getenv("ASKLIAM_STAGE0_AUTO_ENABLED","false").lower()!="true" or (
        os.getenv("ASKLIAM_CRYPTO_AUTO_ENABLED","false").lower()!="true"
    ):
        raise Stage0Error("CRYPTO_AUTO_DISABLED","Automation not enabled")
    if os.getenv("ASKLIAM_CRYPTO_WRITE_ENABLED","false").lower()=="true" or (
        os.getenv("ASKLIAM_COINBASE_SPOT_WRITE_ENABLED","false").lower()=="true"
    ):
        raise Stage0Error("MANUAL_CRYPTO_WRITE_STILL_ENABLED","Manual writer must be disabled")
    if symbol not in ("BTC","ETH") or confirmation!=confirm_spot(symbol):
        raise Stage0Error("EXPLICIT_CONFIRMATION_REQUIRED","Unsupported canonical spot scope")
    if not os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or not os.getenv("ASKLIAM_SPREADSHEET_ID"):
        raise Stage0Error("SPOT_SHEETS_NOT_CONFIGURED","Sheets configuration missing")


def spot_write_guard(symbol: str, confirmation: str) -> None:
    if os.getenv("ASKLIAM_COINBASE_SPOT_WRITE_ENABLED", "").lower() != "true":
        raise Stage0Error("SPOT_WRITE_DISABLED", "Explicit feature flag required")
    if symbol not in COINGECKO_IDS or confirmation != confirm_spot(symbol):
        raise Stage0Error("EXPLICIT_CONFIRMATION_REQUIRED", "Invalid scoped confirmation")
    # Deliberately bounded UTC-day cohort, used for an explicitly authorized
    # one-off startup batch; never a wildcard. Manual mode remains 1-symbol.
    scope = os.getenv("ASKLIAM_COINBASE_SPOT_WRITE_SYMBOL", "").strip().upper().split(",")
    scope = [x.strip() for x in scope]
    if (
        symbol not in scope or len(scope) > 2
        or any(s not in ("BTC", "ETH") for s in scope)
        or len(set(scope)) != len(scope)
        or os.getenv("ASKLIAM_COINBASE_SPOT_WRITE_UTC_DAY", "").strip()
        != datetime.now(timezone.utc).date().isoformat()
    ):
        raise Stage0Error("SPOT_WRITE_SCOPE_DENIED", "Symbol/day scope not permitted")
    if not os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or not os.getenv("ASKLIAM_SPREADSHEET_ID"):
        raise Stage0Error("SPOT_SHEETS_NOT_CONFIGURED", "Missing Sheets configuration")


async def acquire_spot_frames(symbol, client: httpx.AsyncClient, now: datetime, run_id: str):
    """Three bounded source requests, no retries, no FMP usage."""
    async def get_coinbase(path: str, params=None):
        try:
            response = await client.get(
                ENDPOINT + path, params=params, headers={"Accept": "application/json"},
                timeout=30,
            )
        except httpx.HTTPError:
            raise Stage0Error("PROVIDER_DEFERRED", "COINBASE_TRANSPORT_ERROR") from None
        if response.status_code != 200:
            raise Stage0Error(
                "PROVIDER_DEFERRED",
                "COINBASE_HTTP_" + str(response.status_code),
            )
        try:
            return response.json()
        except ValueError:
            raise Stage0Error("PROVIDER_DEFERRED", "COINBASE_INVALID_JSON") from None

    cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
    beginning = cutoff - timedelta(days=299)
    candles = await get_coinbase("/products/" + symbol + "-USD/candles", {
        "granularity": "86400",
        "start": beginning.isoformat().replace("+00:00", "Z"),
        "end": cutoff.isoformat().replace("+00:00", "Z"),
    })
    ticker = await get_coinbase("/products/" + symbol + "-USD/ticker")
    try:
        response = await client.post(
            HYPERLIQUID_INFO, json={"type": "metaAndAssetCtxs"}, timeout=18,
        )
    except httpx.HTTPError:
        raise Stage0Error("PROVIDER_DEFERRED", "HYPERLIQUID_TRANSPORT_ERROR") from None
    if response.status_code != 200:
        raise Stage0Error(
            "PROVIDER_DEFERRED", "HYPERLIQUID_HTTP_" + str(response.status_code),
        )
    try:
        derivatives = response.json()
        return prepare_coinbase_spot_frames(
            symbol, candles=candles, ticker=ticker,
            hyperliquid_context=derivatives, now=now, run_id=run_id,
        )
    except (ValueError, KeyError, TypeError, OverflowError):
        raise Stage0Error("PROVIDER_DEFERRED", "CRYPTO_SPOT_VALIDATION_FAILED") from None


class GuardedCoinbaseSpotIngestor(Stage0_Daily_Ingestor):
    def __init__(self, frames, symbol, run_id):
        self._symbol = symbol
        self._run_id = run_id
        super().__init__(providers={"coinbase_spot_plus_hl_context": lambda: frames})
        self.run_id = run_id

    def _plan(self, before, frames):
        raw, snap = frames["Raw_OHLCV"], frames["Market_Data_Snapshot"]
        if (
            raw.empty or len(snap) != 1
            or set(raw["canonical_symbol"]) != {self._symbol}
            or set(raw["source"]) != {PRICE_SOURCE}
            or snap.iloc[0]["ranking_run_id"] != self._run_id
            or snap.iloc[0]["canonical_symbol"] != self._symbol
            or snap.iloc[0]["analysis_price_source"] != PRICE_SOURCE
        ):
            raise Stage0Error("SPOT_SCHEMA_FAILED", "Unscoped crypto frames")
        current = before["Market_Data_Snapshot"]["rows"].get((self._run_id, self._symbol))
        if current is not None:
            # Preserve existing perp funding/OI, but never silently replace
            # an independent price from another provider or prior spot import.
            if (
                current.get("analysis_price_source", "")
                not in ("", SOURCE_PRICE)
                or current.get("last_price")
            ):
                raise Stage0Error("SPOT_SNAPSHOT_COLLISION", "Existing spot snapshot requires reconciliation")

        plans, expected = super()._plan(before, frames)
        for kind in ("updates", "inserts"):
            for change in plans["Raw_OHLCV"][kind]:
                if change.key[0] != self._symbol or change.key[2] != PRICE_SOURCE:
                    raise Stage0Error("SPOT_FOREIGN_ROW", "Unrelated OHLCV would change")
        if any(plans["FMP_Runtime_State"][kind] for kind in ("updates", "inserts")):
            raise Stage0Error("SPOT_FOREIGN_TAB", "FMP state would change")
        return plans, expected


def _blocking_preflight(frames, symbol, run_id):
    """Read-only plan with production Sheets state; no speculative mutation."""
    reader = GuardedCoinbaseSpotIngestor(frames, symbol, run_id)
    before = reader._capture(reader._open())
    plans, _ = reader._plan(before, {
        **frames, "FMP_Runtime_State": pd.DataFrame(),
    })
    return {
        tab: {kind: len(plans[tab][kind]) for kind in ("updates", "inserts")}
        for tab in plans
    }


def _blocking_write(frames, symbol, run_id):
    return GuardedCoinbaseSpotIngestor(frames, symbol, run_id).run()


async def spot_preflight(symbol, *, redis, client):
    """Source and production Sheet preflight; no Redis or Sheet mutation."""
    if symbol not in COINGECKO_IDS:
        raise Stage0Error("SPOT_UNKNOWN_SYMBOL", "Unknown canonical symbol")
    today = datetime.now(timezone.utc)
    day = today.date().isoformat()
    if await redis.call("GET", pending_key(day)):
        raise Stage0Error("RECONCILIATION_REQUIRED", "Global Stage0 journal pending")
    run_id = "STAGE0-" + today.strftime("%Y%m%d") + "-V6.5.1"
    frames, report = await acquire_spot_frames(symbol, client, today, run_id)
    changes = await asyncio.to_thread(_blocking_preflight, frames, symbol, run_id)
    return {
        "version": "V6.5.1", "status": "SPOT_SHEET_DRY_RUN_PREPARED",
        "symbol": symbol, "run_id": run_id,
        "prepared": report, "plans": changes,
        "sheet_writes_performed": False, "checkpoint_advanced": False,
        "ranking_permitted": False, "execution_permitted": False,
    }


async def spot_commit_one(symbol, confirmation, *, redis, client, writer=_blocking_write,
                          automation_mode: bool = False):
    symbol = str(symbol).strip().upper()
    if automation_mode:
        spot_automation_guard(symbol, confirmation)
    else:
        spot_write_guard(symbol, confirmation)
    now = datetime.now(timezone.utc)
    day = now.date().isoformat()
    run_id = "STAGE0-" + now.strftime("%Y%m%d") + "-V6.5.1"
    batch = Stage0BatchCoordinator(redis)
    lease = await batch.acquire()
    try:
        if await redis.call("GET", pending_key(day)):
            raise Stage0Error("RECONCILIATION_REQUIRED", "Global pending journal")
        original = await redis.call("GET", spot_checkpoint_key(day))
        if original:
            try:
                checkpoint = json.loads(original)
                completed = checkpoint["completed_symbols"]
                if (
                    checkpoint.get("utc_date") != day
                    or checkpoint.get("provider") != PRICE_SOURCE
                    or not isinstance(completed, list)
                    or any(s not in COINGECKO_IDS for s in completed)
                ):
                    raise ValueError("Invalid stored checkpoint")
            except (ValueError, KeyError, TypeError):
                raise Stage0Error("RECONCILIATION_REQUIRED", "Spot checkpoint invalid") from None
        else:
            checkpoint = {
                "version": "V6.5.1", "utc_date": day,
                "provider": PRICE_SOURCE, "completed_symbols": [],
            }
        if symbol in checkpoint["completed_symbols"]:
            raise Stage0Error("ALREADY_COMMITTED_TODAY", "Spot checkpoint already verified")
        frames, report = await acquire_spot_frames(symbol, client, now, run_id)
        # Fail before writing a pending journal if existing snapshot is incompatible.
        await asyncio.to_thread(_blocking_preflight, frames, symbol, run_id)
        if datetime.now(timezone.utc).date().isoformat() != day:
            raise Stage0Error("UTC_DAY_CHANGED", "Acquisition crossed UTC midnight")

        prepared = {
            "status": "PREPARED", "version": "V6.5.1",
            "provider": PRICE_SOURCE, "symbols": [symbol],
            "run_id": run_id, "ohlcv_sha256": report["ohlcv_sha256"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        journal = json.dumps(prepared, sort_keys=True, separators=(",", ":"))
        recorded = await redis.call("SET", pending_key(day), journal, "NX")
        if str(recorded).upper() != "OK":
            raise Stage0Error("RECONCILIATION_REQUIRED", "Concurrent journal exists")
        # From this point preserve pending journal on ANY failure.
        result = await asyncio.to_thread(writer, frames, symbol, run_id)
        if (
            result.get("commit_status") != "COMMITTED"
            or result.get("verify_status") != "PASS"
            or result.get("final_status") != "COMMITTED"
        ):
            raise Stage0Error("VERIFICATION_FAILED", "Independent read-back missing")
        if datetime.now(timezone.utc).date().isoformat() != day:
            raise Stage0Error("UTC_DAY_CHANGED", "Verified after day rollover")
        checkpoint["completed_symbols"] = sorted(set(checkpoint["completed_symbols"]) | {symbol})
        checkpoint["last_verified_run_id"] = run_id
        checkpoint["last_verified_symbol"] = symbol
        checkpoint["last_verified_ohlcv_sha256"] = report["ohlcv_sha256"]
        checkpoint["last_verified_at_utc"] = datetime.now(timezone.utc).isoformat()
        checkpoint["status"] = "COMMITTED_VERIFY_PASS"
        updated = json.dumps(checkpoint, sort_keys=True, separators=(",", ":"))
        success = await redis.eval(
            COMMIT_LUA, [LOCK_KEY, pending_key(day), spot_checkpoint_key(day)],
            [lease, journal, "1" if original else "0", original or "", updated],
        )
        if int(success or 0) != 1:
            raise Stage0Error("CHECKPOINT_NOT_COMMITTED", "Redis compare-and-swap failed")
        return {
            "version": "V6.5.1", "status": "COMMITTED_VERIFY_PASS",
            "source": PRICE_SOURCE, "symbol": symbol, "run_id": run_id,
            "daily_bars": report["daily_rows"], "ohlcv_sha256": report["ohlcv_sha256"],
            "mandatory_fields_present": report["mandatory_fields_present"],
            "sheet_readback": result["tabs"],
            "spot_checkpoint_updated": True,
            "sheet_writes_performed": True, "pending_reconciliation": False,
            "ranking_permitted": False, "execution_permitted": False,
            "fmp_requests_performed": False,
        }
    finally:
        try:
            await batch.release(lease)
        except Exception:
            pass
