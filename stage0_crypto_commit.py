"""AskLiam V6.5.1: opt-in, fail-closed ONE-SYMBOL crypto Stage0 commit.

Deployment ALONE cannot write. The route also requires a disabled-by-default
feature flag and a symbol-scoped explicit confirmation from the user.

A durable global Redis PREPARED journal is acquired *before* ANY Sheet write.
The shared FMP Stage0 exclusive lease serializes operations but FMP quota
counters/checkpoints are NEVER touched. Crypto uses its own daily checkpoint.

CRITICAL: Sheets isn't transactional. If interrupted after PREPARED, preserve
the pending journal and halt subsequent writes for manual reconciliation.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd

from stage0_batch import Stage0BatchCoordinator, LOCK_KEY, pending_key
from stage0_crypto_probe import (
    COINGECKO_IDS, HYPERLIQUID_INFO, classify_response_status,
)
from stage0_crypto_history_probe import WINDOW_DAYS, FUNDING_WINDOW_DAYS
from stage0_crypto_ingest_prepare import (
    SOURCE, frames_from_native_perp,
)
from stage0_crypto_sheet_preflight import compare_prepared_crypto_frames
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

CRYPTO_CHECKPOINT_ROOT = "askliam:v651:stage0:crypto:checkpoint:"
CONFIRM_PREFIX = "COMMIT_ONE_HYPERLIQUID_PERP_"
CONFIRM_SUFFIX = "_STAGE0_V651"
COMMIT_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
local current = redis.call('GET', KEYS[3])
if ARGV[3] == '1' then
    if current ~= ARGV[4] then return 0 end
else
    if current then return 0 end
end
redis.call('SET', KEYS[3], ARGV[5])
redis.call('DEL', KEYS[2])
return 1
"""


def crypto_checkpoint_key(day: str) -> str:
    return CRYPTO_CHECKPOINT_ROOT + day


def confirmation_for(symbol: str) -> str:
    return CONFIRM_PREFIX + symbol + CONFIRM_SUFFIX


def automation_guard(symbol: str, confirmation: str):
    """Separate server-side opt-in; never grants manual endpoint permissions.

    Only the authenticated internal automation route can select this mode.
    An explicitly enabled manual writer must first be closed to avoid mixing
    operator-controlled first import and unattended daily ingestion.
    """
    if os.getenv("ASKLIAM_STAGE0_AUTO_ENABLED", "").lower() != "true" or (
        os.getenv("ASKLIAM_CRYPTO_AUTO_ENABLED", "").lower() != "true"
    ):
        raise Stage0Error("CRYPTO_AUTO_DISABLED", "Automatic crypto writes not enabled")
    if os.getenv("ASKLIAM_CRYPTO_WRITE_ENABLED", "").lower() == "true":
        raise Stage0Error("MANUAL_CRYPTO_WRITE_STILL_ENABLED", "Close manual writer before automation")
    if symbol not in COINGECKO_IDS or confirmation != confirmation_for(symbol):
        raise Stage0Error("EXPLICIT_CONFIRMATION_REQUIRED", "Canonical crypto scope mismatch")
    if not os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or not os.getenv("ASKLIAM_SPREADSHEET_ID"):
        raise Stage0Error("CONFIGURATION_FAILED", "Missing Sheets credentials or workbook")


def feature_guard(symbol: str, confirmation: str):
    """Call before any network/sheet/Redis operation."""
    if os.getenv("ASKLIAM_CRYPTO_WRITE_ENABLED", "").lower() != "true":
        raise Stage0Error(
            "CRYPTO_WRITE_DISABLED", "Opt-in feature flag remains disabled"
        )
    if symbol not in COINGECKO_IDS or confirmation != confirmation_for(symbol):
        raise Stage0Error(
            "EXPLICIT_CONFIRMATION_REQUIRED", "Symbol-scoped confirmation missing"
        )
    allowed_symbol = os.getenv("ASKLIAM_CRYPTO_WRITE_SYMBOL", "").strip().upper()
    allowed_day = os.getenv("ASKLIAM_CRYPTO_WRITE_UTC_DAY", "").strip()
    if not allowed_symbol or not allowed_day:
        raise Stage0Error(
            "CRYPTO_WRITE_SCOPE_MISSING", "BTC-only UTC-day scope not configured"
        )
    if symbol != allowed_symbol:
        raise Stage0Error(
            "CRYPTO_WRITE_SCOPE_DENIED", "The requested crypto symbol is not authorized"
        )
    if datetime.now(timezone.utc).date().isoformat() != allowed_day:
        raise Stage0Error(
            "CRYPTO_WRITE_DAY_EXPIRED", "Single-day write permission has expired"
        )


async def acquire_native_perp_frames(
    symbol: str, *, client: httpx.AsyncClient, now: datetime, run_id: str,
) -> tuple[dict[str, pd.DataFrame], dict]:
    """Exactly 3 successful Hyperliquid Info reads; no retries or FMP."""
    if symbol not in COINGECKO_IDS:
        raise Stage0Error("INVALID_SYMBOL", "Symbol not in crypto cohort")

    async def fetch(body, kind):
        try:
            result = await client.post(
                HYPERLIQUID_INFO, json=body, timeout=18.0,
            )
            code = classify_response_status(result)
            if code != "AVAILABLE":
                raise Stage0Error("PROVIDER_DEFERRED", kind + ": " + code)
            return result.json()
        except (httpx.RequestError, httpx.TimeoutException):
            raise Stage0Error("PROVIDER_DEFERRED", kind + ": NETWORK_ERROR") from None
        except (ValueError, TypeError, KeyError, OverflowError):
            raise Stage0Error("PROVIDER_DEFERRED", kind + ": RESPONSE_INVALID") from None

    context = await fetch({"type": "metaAndAssetCtxs"}, "CONTEXT")
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    candles = await fetch({
        "type": "candleSnapshot",
        "req": {
            "coin": symbol, "interval": "1d",
            "startTime": int((day_start - timedelta(days=WINDOW_DAYS)).timestamp()*1000),
            "endTime": int(day_start.timestamp()*1000),
        },
    }, "CANDLES")
    funding = await fetch({
        "type": "fundingHistory", "coin": symbol,
        "startTime": int((now - timedelta(days=FUNDING_WINDOW_DAYS)).timestamp()*1000),
        "endTime": int(now.timestamp()*1000),
    }, "FUNDING")
    try:
        return frames_from_native_perp(
            symbol, candles=candles, context_payload=context,
            funding_payload=funding, now=now, run_id=run_id,
        )
    except (ValueError, TypeError, KeyError, OverflowError):
        raise Stage0Error(
            "PROVIDER_DEFERRED", "CRYPTO_VALIDATION_FAILED"
        ) from None


class GuardedCryptoIngestor(Stage0_Daily_Ingestor):
    """Apply collision/source guards to the SAME snapshot used by UPSERT."""
    def __init__(self, frames, symbol, run_id, *, spreadsheet_id=None):
        self._crypto_symbol = symbol
        self._crypto_run_id = run_id
        super().__init__(
            providers={"hyperliquid_native_perp": lambda: frames},
            spreadsheet_id=spreadsheet_id,
        )
        self.run_id = run_id

    def _plan(self, before, frames):
        subset = {
            "Raw_OHLCV": frames["Raw_OHLCV"],
            "Market_Data_Snapshot": frames["Market_Data_Snapshot"],
        }
        safety = compare_prepared_crypto_frames(
            subset, symbol=self._crypto_symbol,
            run_id=self._crypto_run_id, captured=before,
        )
        if safety["status"] != "SHEET_DRY_RUN_PREPARED":
            raise Stage0Error(
                "SNAPSHOT_COLLISION", "Preexisting independent quote cannot be overwritten"
            )
        return super()._plan(before, frames)


def write_crypto_frames_sync(frames, symbol, run_id):
    """Google Sheets writer with existing independent read-back protection."""
    writer = GuardedCryptoIngestor(frames, symbol, run_id)
    return writer.run()


async def commit_one_native_perp(
    symbol: str, confirmation: str, *,
    redis, client: httpx.AsyncClient, now: datetime | None = None,
    writer=write_crypto_frames_sync,
    automation_mode: bool = False,
) -> dict:
    symbol = str(symbol).strip().upper()
    if automation_mode:
        automation_guard(symbol, confirmation)
    else:
        feature_guard(symbol, confirmation)
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise Stage0Error("INVALID_TIMESTAMP", "UTC aware timestamp required")
    now = now.astimezone(timezone.utc)
    day = now.date().isoformat()
    run_id = "STAGE0-" + now.strftime("%Y%m%d") + "-V6.5.1"
    batch = Stage0BatchCoordinator(redis)
    token = await batch.acquire()
    journal_raw = None
    try:
        if await redis.call("GET", pending_key(day)):
            raise Stage0Error("RECONCILIATION_REQUIRED", "Global pending journal")
        # A spot-first same-day ingestion already includes freshly observed
        # funding and OI. Never overwrite its CRYPTO_V1 snapshot with the
        # incomplete derivative-only snapshot (even via a sparse UPSERT).
        from stage0_crypto_coinbase_commit import spot_checkpoint_key
        spot_raw = await redis.call("GET", spot_checkpoint_key(day))
        if spot_raw:
            try:
                spot_cp = json.loads(spot_raw)
                if (
                    spot_cp.get("version") != "V6.5.1"
                    or spot_cp.get("utc_date") != day
                    or spot_cp.get("status") != "COMMITTED_VERIFY_PASS"
                    or not isinstance(spot_cp.get("completed_symbols"), list)
                ):
                    raise ValueError("Invalid Coinbase spot checkpoint")
            except (ValueError, TypeError, AttributeError):
                raise Stage0Error(
                    "RECONCILIATION_REQUIRED", "Invalid Coinbase spot checkpoint"
                ) from None
            if symbol in spot_cp["completed_symbols"]:
                raise Stage0Error(
                    "ALREADY_SPOT_ACQUIRED_TODAY",
                    "Spot CRYPTO_V1 snapshot already verified; derivative-only write blocked",
                )
        stored = await redis.call("GET", crypto_checkpoint_key(day))
        if stored:
            try:
                checkpoint = json.loads(stored)
                if checkpoint.get("utc_date") != day:
                    raise ValueError("Checkpoint date mismatch")
                completed = checkpoint.get("completed_symbols")
                if not isinstance(completed, list) or any(
                    v not in COINGECKO_IDS for v in completed
                ):
                    raise ValueError("Checkpoint contract mismatch")
            except (ValueError, TypeError, AttributeError):
                raise Stage0Error(
                    "RECONCILIATION_REQUIRED", "Invalid crypto checkpoint"
                ) from None
        else:
            checkpoint = {
                "version": "V6.5.1", "utc_date": day,
                "completed_symbols": [], "provider": SOURCE,
            }
        if symbol in checkpoint["completed_symbols"]:
            raise Stage0Error(
                "ALREADY_COMMITTED_TODAY", "Crypto daily checkpoint already contains asset"
            )
        # Source acquisition/validation is a pure read BEFORE pending journal.
        frames, report = await acquire_native_perp_frames(
            symbol, client=client, now=now, run_id=run_id,
        )
        if datetime.now(timezone.utc).date().isoformat() != day:
            raise Stage0Error("UTC_DAY_CHANGED", "Date changed during acquisition")
        journal = {
            "status": "PREPARED", "version": "V6.5.1", "provider": SOURCE,
            "run_id": run_id, "symbols": [symbol],
            "ohlcv_sha256": report["ohlcv_sha256"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        journal_raw = json.dumps(journal, separators=(",", ":"), sort_keys=True)
        prepared = await redis.call(
            "SET", pending_key(day), journal_raw, "NX",
        )
        if str(prepared).upper() != "OK":
            raise Stage0Error(
                "RECONCILIATION_REQUIRED", "Failed to set exclusive durable journal"
            )
        # From this point ANY error is potentially a partial Sheet commit.
        # Never auto-delete pending journal; manual reconciliation required.
        result = await asyncio.to_thread(writer, frames, symbol, run_id)
        if (
            result.get("commit_status") != "COMMITTED"
            or result.get("verify_status") != "PASS"
            or result.get("final_status") != "COMMITTED"
        ):
            raise Stage0Error(
                "VERIFICATION_FAILED", "Google Sheets independent read-back absent"
            )
        if datetime.now(timezone.utc).date().isoformat() != day:
            raise Stage0Error(
                "UTC_DAY_CHANGED", "Sheets verified after rollover; reconciliation needed"
            )
        checkpoint["completed_symbols"] = sorted(
            set(checkpoint["completed_symbols"]) | {symbol}
        )
        checkpoint["last_verified_run_id"] = run_id
        checkpoint["last_verified_symbol"] = symbol
        checkpoint["last_verified_ohlcv_sha256"] = report["ohlcv_sha256"]
        checkpoint["last_verified_at_utc"] = datetime.now(timezone.utc).isoformat()
        checkpoint["status"] = "COMMITTED_VERIFY_PASS"
        next_checkpoint = json.dumps(
            checkpoint, separators=(",", ":"), sort_keys=True,
        )
        result_code = await redis.eval(
            COMMIT_LUA,
            [LOCK_KEY, pending_key(day), crypto_checkpoint_key(day)],
            [token, journal_raw, "1" if stored else "0",
             stored or "", next_checkpoint],
        )
        if int(result_code or 0) != 1:
            raise Stage0Error(
                "CHECKPOINT_NOT_COMMITTED", "Redis CAS failed; manual reconciliation"
            )
        return {
            "version": "V6.5.1",
            "status": "COMMITTED_VERIFY_PASS",
            "canonical_symbol": symbol,
            "source": SOURCE,
            "run_id": run_id,
            "daily_bars_expected": report["daily_rows_prepared"],
            "ohlcv_sha256": report["ohlcv_sha256"],
            "sheet_readback": result["tabs"],
            "crypto_checkpoint_updated": True,
            "sheet_writes_performed": True,
            "fmp_requests_performed": False,
            "ranking_permitted": False,
            "execution_permitted": False,
            "pending_reconciliation": False,
        }
    finally:
        try:
            await batch.release(token)
        except Exception:
            # A retained pending journal still blocks unsafe subsequent writes.
            pass
