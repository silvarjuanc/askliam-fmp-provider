"""V6.5.1 Coinbase post-write reconciliation. Never clear a journal blindly.

Explicit, exact-day ETH-only recovery of a PREPARED spot journal after a
Google Sheets readback API error. Verifies independent Coinbase OHLCV against
every persisted row, snapshot nine mandatory fields and original digest before
one Redis atomic journal->verified-checkpoint transition. No Sheet writes.
"""
from __future__ import annotations
import asyncio
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from math import isfinite

import httpx

from stage0_batch import Stage0BatchCoordinator, LOCK_KEY, pending_key
from stage0_crypto_coinbase_commit import spot_checkpoint_key
from stage0_crypto_coinbase_spot import ENDPOINT, SOURCE, validate_candles
from stage0_crypto_commit import COMMIT_LUA
from stage0_crypto_coinbase_prepare import MANDATORY
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

RECOVERY_SYMBOL = "ETH"
VERSION = "V6.5.1"


def validated_saved_spot(saved: dict, payload, *, symbol: str, run_id: str,
                         today: datetime, journal_digest: str) -> dict:
    """Strict independent replay-vs-Sheets comparison, without side effects."""
    if symbol != RECOVERY_SYMBOL or today.tzinfo is None:
        raise Stage0Error("RECONCILIATION_REQUIRED", "Invalid recovery scope")
    verified = validate_candles(payload, symbol, today)
    if verified["daily_bars"] != 299:
        raise Stage0Error("RECONCILIATION_REQUIRED", "Spot 299-day window incomplete")
    expected_end = (today.astimezone(timezone.utc).date() - timedelta(days=1)).isoformat()
    if verified["last_complete_utc_date"] != expected_end:
        raise Stage0Error("RECONCILIATION_REQUIRED", "Spot history not current")
    from datetime import datetime as DateTime
    candles = []
    for item in payload:
        day = DateTime.fromtimestamp(int(item[0]), timezone.utc).date().isoformat()
        if day >= today.date().isoformat():
            continue
        candles.append([day, float(item[3]), float(item[2]),
                        float(item[1]), float(item[4]), float(item[5])])
    candles.sort(key=lambda a: a[0])
    digest = hashlib.sha256(json.dumps(
        candles, separators=(",", ":"), allow_nan=False
    ).encode()).hexdigest()
    if digest != journal_digest:
        raise Stage0Error("RECONCILIATION_REQUIRED", "Original provider SHA mismatch")

    historical = saved["Raw_OHLCV"]["rows"]
    for date, opn, high, low, close, volume in candles:
        row = historical.get((symbol, date, SOURCE))
        if not row:
            raise Stage0Error("RECONCILIATION_REQUIRED", "Missing saved Coinbase candle")
        expected = {
            "open": opn, "high": high, "low": low,
            "close": close, "volume": volume,
        }
        for key, val in expected.items():
            if row.get(key, "") != Stage0_Daily_Ingestor._scalar(val):
                raise Stage0Error("RECONCILIATION_REQUIRED", "Candle/value mismatch")
        if any((
            row.get("canonical_symbol") != symbol,
            row.get("source") != SOURCE,
            row.get("provider_symbol") != symbol + "-USD",
            row.get("ranking_run_id") != run_id,
            row.get("quality_status") != "INDEPENDENT_SPOT_OHLCV_VALIDATED",
            row.get("upsert_key") != symbol + "|" + date + "|" + SOURCE,
            row.get("source_url_template")
                != ENDPOINT + "/products/" + symbol + "-USD/candles",
        )):
            raise Stage0Error("RECONCILIATION_REQUIRED", "Candle provenance mismatch")
    observed = [k for k in historical if k[0] == symbol and k[2] == SOURCE]
    if len(observed) != len(candles):
        raise Stage0Error("RECONCILIATION_REQUIRED", "Unexpected saved Coinbase candle count")
    snapshot = saved["Market_Data_Snapshot"]["rows"].get((run_id, symbol))
    if not snapshot:
        raise Stage0Error("RECONCILIATION_REQUIRED", "Snapshot absent")
    if any((
        snapshot.get("asset_class") != "Crypto",
        snapshot.get("score_model") != "CRYPTO_V1",
        snapshot.get("analysis_price_source") != SOURCE,
        snapshot.get("mandatory_broad_pass") != "TRUE",
        snapshot.get("data_completeness_pct") != "100",
        snapshot.get("missing_mandatory_fields") != "NONE",
        snapshot.get("enrichment_status") != "ACQUIRED_NOT_SCORED",
        snapshot.get("broad_readiness") != "BROAD_INPUTS_VALIDATED_NOT_SCORED",
    )):
        raise Stage0Error("RECONCILIATION_REQUIRED", "Snapshot semantic mismatch")
    stamp = datetime.fromisoformat(snapshot.get("data_timestamp", ""))
    if stamp.tzinfo is None or stamp.astimezone(timezone.utc).date() != today.date():
        raise Stage0Error("RECONCILIATION_REQUIRED", "Snapshot timestamp invalid")
    for field in MANDATORY:
        try:
            val = float(snapshot.get(field, ""))
        except (TypeError, ValueError, OverflowError):
            raise Stage0Error("RECONCILIATION_REQUIRED", "Missing mandatory field") from None
        if not isfinite(val):
            raise Stage0Error("RECONCILIATION_REQUIRED", "Non-finite mandatory field")
        if field in ("last_price", "open_interest") and val <= 0:
            raise Stage0Error("RECONCILIATION_REQUIRED", "Invalid mandatory positive field")
    return {
        "version": VERSION, "symbol": symbol, "source": SOURCE,
        "daily_bars_verified": 299, "snapshot_verified": True,
        "ohlcv_sha256": digest, "stage0_fields_verified": len(MANDATORY),
    }


def readback_all_sheets():
    reader = Stage0_Daily_Ingestor(providers={"readback_only": lambda: {}})
    return reader._capture(reader._open())


async def reconcile_after_readback_error(*, redis, client: httpx.AsyncClient) -> dict:
    now = datetime.now(timezone.utc)
    day = now.date().isoformat()
    if (
        os.getenv("ASKLIAM_COINBASE_SPOT_RECOVER_DATE", "") != day
        or os.getenv("ASKLIAM_COINBASE_SPOT_RECOVER_SYMBOL", "") != RECOVERY_SYMBOL
        or day != "2026-10-14"
    ):
        raise Stage0Error("RECOVERY_DISABLED", "No exact-day recovery permission")
    batch = Stage0BatchCoordinator(redis)
    token = await batch.acquire()
    try:
        raw = await redis.call("GET", pending_key(day))
        if not raw:
            raise Stage0Error("RECONCILIATION_REQUIRED", "Original pending journal missing")
        try:
            journal = json.loads(raw)
            digest = journal["ohlcv_sha256"]
            if (
                journal.get("version") != VERSION
                or journal.get("status") != "PREPARED"
                or journal.get("provider") != SOURCE
                or journal.get("symbols") != [RECOVERY_SYMBOL]
                or journal.get("run_id") != "STAGE0-20261014-V6.5.1"
                or not isinstance(digest, str) or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)
            ):
                raise ValueError("Invalid journal")
        except (ValueError, KeyError, TypeError, AttributeError):
            raise Stage0Error("RECONCILIATION_REQUIRED", "Pending journal is not ETH Coinbase") from None
        original = await redis.call("GET", spot_checkpoint_key(day))
        if not original:
            raise Stage0Error("RECONCILIATION_REQUIRED", "BTC checkpoint unexpectedly missing")
        try:
            cp = json.loads(original)
            if (cp.get("version") != VERSION or cp.get("utc_date") != day
                or cp.get("provider") != SOURCE
                or cp.get("status") != "COMMITTED_VERIFY_PASS"
                or cp.get("completed_symbols") != ["BTC"]):
                raise ValueError("Unexpected checkpoint")
        except (ValueError, TypeError, AttributeError):
            raise Stage0Error("RECONCILIATION_REQUIRED", "BTC checkpoint corrupted") from None
        cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            reply = await client.get(
                ENDPOINT + "/products/ETH-USD/candles",
                params={
                    "granularity": "86400",
                    "start": (cutoff - timedelta(days=299)).isoformat().replace("+00:00", "Z"),
                    "end": cutoff.isoformat().replace("+00:00", "Z"),
                }, timeout=30,
            )
        except httpx.HTTPError:
            raise Stage0Error("PROVIDER_DEFERRED", "Coinbase reconciliation transport") from None
        if reply.status_code != 200:
            raise Stage0Error("PROVIDER_DEFERRED", "Coinbase reconciliation HTTP non-200")
        try:
            payload = reply.json()
        except (TypeError, ValueError):
            raise Stage0Error("PROVIDER_DEFERRED", "Coinbase reconciliation invalid JSON") from None

        # Explicit bounded Google API error retry, always read only.
        last_error = None
        for delay in (0, 6, 18, 35):
            if delay:
                await asyncio.sleep(delay)
            if datetime.now(timezone.utc).date().isoformat() != day:
                raise Stage0Error("RECONCILIATION_REQUIRED", "Day crossed during reconciliation")
            try:
                saved = await asyncio.to_thread(readback_all_sheets)
                evidence = validated_saved_spot(
                    saved, payload, symbol=RECOVERY_SYMBOL,
                    run_id=journal["run_id"], today=now,
                    journal_digest=digest,
                )
                break
            except Stage0Error:
                raise
            except Exception as err:
                last_error = type(err).__name__
        else:
            raise Stage0Error(
                "RECONCILIATION_REQUIRED",
                "Readback transient API failure: " + str(last_error)
            )
        if datetime.now(timezone.utc).date().isoformat() != day:
            raise Stage0Error("RECONCILIATION_REQUIRED", "UTC day changed before commit")
        cp["completed_symbols"] = ["BTC", "ETH"]
        cp["last_verified_run_id"] = journal["run_id"]
        cp["last_verified_symbol"] = RECOVERY_SYMBOL
        cp["last_verified_ohlcv_sha256"] = digest
        cp["last_verified_at_utc"] = datetime.now(timezone.utc).isoformat()
        cp["reconciliation_evidence"] = {
            "type": "POST_WRITE_INDEPENDENT_READBACK",
            "verified_source": SOURCE,
            "daily_bars_verified": evidence["daily_bars_verified"],
            "mandatory_fields_verified": len(MANDATORY),
            "journal_digest_replayed_and_matched": True,
        }
        updated = json.dumps(cp, sort_keys=True, separators=(",", ":"))
        code = await redis.eval(
            COMMIT_LUA,
            [LOCK_KEY, pending_key(day), spot_checkpoint_key(day)],
            [token, raw, "1", original, updated],
        )
        if int(code or 0) != 1:
            raise Stage0Error("RECONCILIATION_REQUIRED", "Atomic checkpoint CAS unsuccessful")
        return {
            "version": VERSION, "status": "COMMITTED_VERIFY_PASS",
            "symbols_verified_today": cp["completed_symbols"],
            "reconciled_symbol": RECOVERY_SYMBOL,
            "source": SOURCE,
            "daily_bars_verified": 299,
            "journal_cleared_atomically": True,
            "checkpoint_committed": True,
            "ranking_permitted": False, "execution_permitted": False,
        }
    finally:
        try:
            await batch.release(token)
        except Exception:
            pass
