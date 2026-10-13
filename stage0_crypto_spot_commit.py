"""AskLiam V6.5.1 transactional CoinGecko independent-SPOT Stage0 writer.

Separate Redis checkpoint from Hyperliquid-native-perpetual checkpoint.
Global pending journal + shared exclusive lease + readback-before-CAS are
the same non-atomic-Sheets protections as existing Stage0 writers.

Disabled unless ASKLIAM_STAGE0_AUTO_ENABLED, ASKLIAM_CRYPTO_AUTO_ENABLED
and ASKLIAM_CRYPTO_SPOT_ENABLED are all explicitly 'true'.  The manual
crypto write flag must be false.  No trading, scoring or Lifecycle writes.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
import pandas as pd

from stage0_batch import LOCK_KEY, Stage0BatchCoordinator, pending_key
from stage0_crypto_commit import (
    COMMIT_LUA, crypto_checkpoint_key, automation_guard,
)
from stage0_crypto_probe import COINGECKO_IDS
from stage0_crypto_spot import (
    CHART_URL, MARKETS_URL, SPOT_SOURCE, make_spot_frames,
    merge_derivative_and_spot_snapshot,
)
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

SPOT_CHECKPOINT_ROOT = "askliam:v651:stage0:crypto:spot:checkpoint:"
CHART_CACHE_ROOT = "askliam:v651:coingecko:history:usd:"
QUOTE_CACHE_ROOT = "askliam:v651:coingecko:quotes:usd:"
CHART_TTL = 86400
QUOTE_TTL = 900


def spot_checkpoint_key(day: str) -> str:
    return SPOT_CHECKPOINT_ROOT + day


def spot_automation_guard(symbol: str):
    if os.getenv("ASKLIAM_CRYPTO_SPOT_ENABLED", "").lower() != "true":
        raise Stage0Error("SPOT_WRITES_DISABLED", "Spot Stage0 is not enabled")
    from stage0_crypto_commit import confirmation_for
    automation_guard(symbol, confirmation_for(symbol))


class GuardedSpotIngestor(Stage0_Daily_Ingestor):
    def __init__(self, frames, symbol, run_id, *, observed_at, spreadsheet_id=None):
        self._symbol = symbol
        self._observed_at = observed_at
        self._expected_run_id = run_id
        super().__init__(
            providers={"coingecko_spot": lambda: frames},
            spreadsheet_id=spreadsheet_id,
        )
        self.run_id = run_id

    def _plan(self, before, frames):
        raw = frames["Raw_OHLCV"]
        snap = frames["Market_Data_Snapshot"]
        if len(snap) != 1 or raw.empty or (
            not all(raw["source"] == SPOT_SOURCE)
        ) or (
            not all(raw["canonical_symbol"] == self._symbol)
        ) or (
            snap.iloc[0]["canonical_symbol"] != self._symbol
            or snap.iloc[0]["ranking_run_id"] != self._expected_run_id
        ):
            raise Stage0Error("SPOT_FRAME_INVALID", "Unexpected source/symbol/snapshot")
        previous = before["Market_Data_Snapshot"]["rows"].get(
            (self._expected_run_id, self._symbol)
        )
        if previous is None:
            raise Stage0Error(
                "DERIVATIVE_SNAPSHOT_NOT_VERIFIED",
                "This run requires a validated native perpetual snapshot first",
            )
        try:
            combined = merge_derivative_and_spot_snapshot(
                previous, snap.iloc[0].to_dict(), now=self._observed_at,
            )
        except ValueError as exc:
            raise Stage0Error("SPOT_SNAPSHOT_COLLISION", str(exc)) from None
        cloned = dict(frames)
        cloned["Market_Data_Snapshot"] = pd.DataFrame([combined])
        plans, expected = super()._plan(before, cloned)
        for kind in ("inserts", "updates"):
            for change in plans["Raw_OHLCV"][kind]:
                if change.key[0] != self._symbol or change.key[2] != SPOT_SOURCE:
                    raise Stage0Error("SPOT_CROSS_SOURCE_WRITE", "Unapproved series")
            for change in plans["Market_Data_Snapshot"][kind]:
                if change.key != (self._expected_run_id, self._symbol):
                    raise Stage0Error("SPOT_SNAPSHOT_CROSS_SYMBOL", "Foreign snapshot")
            if plans["FMP_Runtime_State"][kind]:
                raise Stage0Error("SPOT_FMP_STATE_MODIFICATION", "FMP state not writable")
        if combined["mandatory_broad_pass"] != "TRUE":
            raise Stage0Error(
                "SPOT_MANDATORY_INCOMPLETE",
                "Cannot promote incomplete spot + derivative context",
            )
        return plans, expected


def preflight_spot_frames_sync(frames, symbol, run_id, observed_at):
    """Fail predictable Sheet schema/ownership/collision errors BEFORE journal.

    The actual writer will recapture STATE_BEFORE again, check fingerprint,
    execute transactional UPSERT, and independently READ-BACK VERIFY.
    """
    reader=GuardedSpotIngestor(
        frames,symbol,run_id,observed_at=observed_at,
    )
    before=reader._capture(reader._open())
    plans,_expected=reader._plan(before,{
        **frames,"FMP_Runtime_State":pd.DataFrame(),
    })
    return {
        tab:{"inserts":len(plan["inserts"]),"updates":len(plan["updates"])}
        for tab,plan in plans.items()
    }


def write_spot_frames_sync(frames, symbol, run_id, observed_at):
    return GuardedSpotIngestor(
        frames, symbol, run_id, observed_at=observed_at,
    ).run()


async def acquire_coingecko_spot(symbol, *, client, redis, now, run_id):
    """Two rate-limit-aware remote reads maximum; cache-first, no FMP calls."""
    coin_id = COINGECKO_IDS[symbol]
    day = now.date().isoformat()
    headers = {"accept": "application/json"}
    api_key = os.getenv("COINGECKO_DEMO_API_KEY", "")
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    async def retrieve(cache_key, url, params, ttl):
        cached = await redis.cache_get(cache_key)
        if cached is not None:
            if not isinstance(cached, (list, dict)):
                raise Stage0Error("SPOT_CACHE_INVALID", "CoinGecko cache format")
            return cached
        try:
            resp = await client.get(
                url, params=params, headers=headers, timeout=30.0,
            )
            if resp.status_code != 200:
                code = resp.status_code
                if code in (401, 402, 403, 408, 429, 500, 502, 503, 504):
                    raise Stage0Error("PROVIDER_DEFERRED", "COINGECKO_HTTP_" + str(code))
                raise Stage0Error("PROVIDER_DEFERRED", "COINGECKO_OTHER_HTTP")
            data = resp.json()
            if not isinstance(data, (dict, list)):
                raise Stage0Error("PROVIDER_DEFERRED", "COINGECKO_RESPONSE_SHAPE")
        except (httpx.HTTPError, httpx.TimeoutException):
            raise Stage0Error("PROVIDER_DEFERRED", "COINGECKO_NETWORK_ERROR") from None
        except (ValueError, KeyError, TypeError):
            raise Stage0Error("PROVIDER_DEFERRED", "COINGECKO_JSON_INVALID") from None
        await redis.cache_put(cache_key, data, ttl)
        return data

    chart = await retrieve(
        CHART_CACHE_ROOT + coin_id + ":" + day,
        CHART_URL.format(id=quote(coin_id, safe="")),
        {"vs_currency": "usd", "days": 365},
        CHART_TTL,
    )
    quotes = await retrieve(
        QUOTE_CACHE_ROOT + coin_id,
        MARKETS_URL,
        {"vs_currency": "usd", "ids": coin_id},
        QUOTE_TTL,
    )
    try:
        return make_spot_frames(
            symbol, chart=chart, quotes=quotes, now=now, run_id=run_id,
        )
    except (ValueError, TypeError, KeyError, OverflowError):
        raise Stage0Error("PROVIDER_DEFERRED", "COINGECKO_SPOT_VALIDATION_FAILED") from None


async def commit_coingecko_spot_one(
    symbol: str, *, redis, client, writer=write_spot_frames_sync, now=None,
) -> dict:
    symbol = str(symbol).strip().upper()
    spot_automation_guard(symbol)
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise Stage0Error("INVALID_TIMESTAMP", "Timezone-aware clock required")
    now = now.astimezone(timezone.utc)
    day = now.date().isoformat()
    run_id = "STAGE0-" + now.strftime("%Y%m%d") + "-V6.5.1"
    batch = Stage0BatchCoordinator(redis)
    token = await batch.acquire()
    try:
        if await redis.call("GET", pending_key(day)):
            raise Stage0Error("RECONCILIATION_REQUIRED", "Global pending journal")
        perp_raw = await redis.call("GET", crypto_checkpoint_key(day))
        if not perp_raw:
            raise Stage0Error("DERIVATIVE_NOT_ACQUIRED", "Native perp Stage0 required")
        try:
            perp_cp = json.loads(perp_raw)
            if perp_cp.get("utc_date") != day or (
                symbol not in perp_cp["completed_symbols"]
            ) or perp_cp.get("status") != "COMMITTED_VERIFY_PASS":
                raise ValueError()
        except (TypeError, ValueError, KeyError):
            raise Stage0Error(
                "DERIVATIVE_CHECKPOINT_INVALID", "Previous Stage0 invalid"
            ) from None
        stored = await redis.call("GET", spot_checkpoint_key(day))
        if stored:
            try:
                checkpoint = json.loads(stored)
                done = checkpoint["completed_symbols"]
                if (
                    checkpoint.get("utc_date") != day
                    or checkpoint.get("version") != "V6.5.1"
                    or not isinstance(done, list)
                    or any(x not in COINGECKO_IDS for x in done)
                ):
                    raise ValueError()
            except (ValueError, TypeError, KeyError):
                raise Stage0Error("SPOT_CHECKPOINT_INVALID", "Manual review") from None
        else:
            checkpoint = {
                "version": "V6.5.1", "utc_date": day,
                "completed_symbols": [], "provider": SPOT_SOURCE,
            }
        if symbol in checkpoint["completed_symbols"]:
            raise Stage0Error("ALREADY_COMMITTED_TODAY", "Spot completed")
        frames, report = await acquire_coingecko_spot(
            symbol, client=client, redis=redis, now=now, run_id=run_id,
        )
        # No journal until the current workbook confirms same-run derivative
        # fields/ownership, the dataset shape, and no foreign-sheet mutations.
        await asyncio.to_thread(
            preflight_spot_frames_sync, frames, symbol, run_id, now,
        )
        if datetime.now(timezone.utc).date().isoformat() != day:
            raise Stage0Error("UTC_DAY_CHANGED", "Spot acquisition crossed day")
        journal = {
            "version": "V6.5.1", "status": "PREPARED",
            "provider": SPOT_SOURCE, "run_id": run_id,
            "symbols": [symbol], "spot_sha256": report["sha256"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        journal_raw = json.dumps(journal, separators=(",", ":"), sort_keys=True)
        acquired = await redis.call(
            "SET", pending_key(day), journal_raw, "NX",
        )
        if str(acquired).upper() != "OK":
            raise Stage0Error("RECONCILIATION_REQUIRED", "Journal collision")
        # Any error from now on is potentially a partial Sheet write.
        result = await asyncio.to_thread(
            writer, frames, symbol, run_id, now,
        )
        if (
            result.get("commit_status") != "COMMITTED"
            or result.get("verify_status") != "PASS"
            or result.get("final_status") != "COMMITTED"
        ):
            raise Stage0Error("SPOT_READBACK_FAILED", "Manual reconciliation")
        if datetime.now(timezone.utc).date().isoformat() != day:
            raise Stage0Error("UTC_DAY_CHANGED", "Spot writer crossed UTC day")
        checkpoint["completed_symbols"] = sorted(
            set(checkpoint["completed_symbols"]) | {symbol}
        )
        checkpoint["last_verified_run_id"] = run_id
        checkpoint["last_verified_symbol"] = symbol
        checkpoint["last_verified_spot_sha256"] = report["sha256"]
        checkpoint["verified_at_utc"] = datetime.now(timezone.utc).isoformat()
        checkpoint["status"] = "COMMITTED_VERIFY_PASS"
        next_checkpoint = json.dumps(
            checkpoint, separators=(",", ":"), sort_keys=True,
        )
        cas = await redis.eval(
            COMMIT_LUA,
            [LOCK_KEY, pending_key(day), spot_checkpoint_key(day)],
            [token, journal_raw, "1" if stored else "0",
             stored or "", next_checkpoint],
        )
        if int(cas or 0) != 1:
            raise Stage0Error("CHECKPOINT_NOT_COMMITTED", "Spot CAS failed")
        return {
            "version": "V6.5.1",
            "symbol": symbol,
            "run_id": run_id,
            "status": "COMMITTED_VERIFY_PASS",
            "daily_point_rows": report["data_points"],
            "spot_sha256": report["sha256"],
            "sheet_readback": result["tabs"],
            "spot_checkpoint_updated": True,
            "ranking_permitted": False,
            "execution_permitted": False,
            "fmp_requests_performed": False,
            "pending_reconciliation": False,
        }
    finally:
        try:
            await batch.release(token)
        except Exception:
            pass
