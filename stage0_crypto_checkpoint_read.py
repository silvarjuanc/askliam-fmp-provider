"""AskLiam V6.5.1: read-only crypto Stage0 checkpoint inspection.

No new market acquisition, no Sheet write, no Redis SET/EVAL and no
execution/ranking. Reports the separate crypto journal without conflating
it with the FMP daily checkpoint.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone

from stage0_batch import pending_key
from stage0_crypto_commit import crypto_checkpoint_key
from stage0_crypto_probe import COINGECKO_IDS
from stage0_ingestor import Stage0Error


async def inspect_crypto_checkpoint(redis, *, day: str, symbol: str = "BTC") -> dict:
    symbol = str(symbol).strip().upper()
    if symbol not in COINGECKO_IDS:
        raise Stage0Error("CRYPTO_SYMBOL_NOT_MAPPED", "Unknown crypto symbol")
    try:
        parsed = date.fromisoformat(day)
    except (TypeError, ValueError):
        raise Stage0Error("INVALID_DATE", "Expected YYYY-MM-DD") from None
    if day != parsed.isoformat() or parsed > datetime.now(timezone.utc).date():
        raise Stage0Error("INVALID_DATE", "UTC day invalid or in the future")

    # All reads. The pending journal is GLOBAL across UTC day boundaries.
    raw = await redis.call("GET", crypto_checkpoint_key(day))
    pending = await redis.call("GET", pending_key(day))
    state = {
        "version": "V6.5.1",
        "utc_date": day,
        "symbol": symbol,
        "reader_only": True,
        "sheet_writes_performed": False,
        "checkpoint_advanced": False,
        "ranking_permitted": False,
        "execution_permitted": False,
        "pending_reconciliation": bool(pending),
        "checkpoint_found": bool(raw),
        "symbol_completed": False,
        "checkpoint_status": "NOT_FOUND",
        "completed_symbols": [],
        "last_verified_run_id": None,
        "last_verified_symbol": None,
        "last_verified_ohlcv_sha256": None,
        "last_verified_at_utc": None,
        "symbol_checkpoint_verified": False,
    }
    if not raw:
        return state
    try:
        cp = json.loads(raw)
        completed = cp["completed_symbols"]
        if (
            not isinstance(cp, dict)
            or cp.get("version") != "V6.5.1"
            or cp.get("utc_date") != day
            or cp.get("provider") != "HYPERLIQUID_NATIVE_PERP"
            or not isinstance(completed, list)
            or not all(isinstance(sym, str) and sym in COINGECKO_IDS for sym in completed)
            or len(completed) != len(set(completed))
            or cp.get("status") != "COMMITTED_VERIFY_PASS"
        ):
            raise ValueError("Untrusted crypto checkpoint")
    except (ValueError, KeyError, TypeError, AttributeError):
        raise Stage0Error(
            "CHECKPOINT_INVALID", "Crypto checkpoint requires manual review"
        ) from None

    state.update({
        "checkpoint_status": cp["status"],
        "completed_symbols": completed,
        "symbol_completed": symbol in completed,
        "last_verified_run_id": cp.get("last_verified_run_id"),
        "last_verified_symbol": cp.get("last_verified_symbol"),
        "last_verified_ohlcv_sha256": cp.get("last_verified_ohlcv_sha256"),
        "last_verified_at_utc": cp.get("last_verified_at_utc"),
        "symbol_checkpoint_verified": symbol in completed and not bool(pending),
    })
    return state
