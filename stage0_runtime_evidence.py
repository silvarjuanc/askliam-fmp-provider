"""V6.5.1 Stage0 status mirror: public-free, authenticated, non-secret Sheet evidence.

A verified Stage0 data acquisition is NOT a completed portfolio run.
Writes ONLY FMP_Runtime_State (one of three approved Stage0 tabs).
Never touches Ranking, Run_Manifest, AI Trades, tokens, orders or portfolio.
"""
from __future__ import annotations
import asyncio
import json
from datetime import datetime, timezone
from math import isfinite

from stage0_batch import Stage0BatchCoordinator, pending_key
from stage0_crypto_coinbase_commit import spot_checkpoint_key
from stage0_crypto_coinbase_prepare import PRICE_SOURCE, MANDATORY
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

VERSION = "V6.5.1"
LABELS = (
    "Live Evidence UTC Day",
    "Live Evidence Captured UTC",
    "Live Redis FMP Used",
    "Live Redis FMP Remaining",
    "Live Coinbase Spot Verified",
    "Live Stage0 Pending Reconciliation",
    "Live Broad Mandatory Acquired",
    "Live Full Pipeline Status",
)


def _sheet_evidence_sync(proof):
    reader = Stage0_Daily_Ingestor(providers={"readonly": lambda: {}})
    book = reader._open()
    snaps = book.worksheet("Market_Data_Snapshot").get_all_values()
    if not snaps or len(snaps) < 2:
        raise Stage0Error("SHEET_READ_FAILED", "Snapshot tab is unexpectedly empty")
    headers = snaps[0]
    if len(headers) != len(set(headers)):
        raise Stage0Error("SCHEMA_FAILED", "Duplicate snapshot columns")
    target = {}
    run_id = "STAGE0-" + proof["day"].replace("-", "") + "-V6.5.1"
    for row in snaps[1:]:
        entry = dict(zip(headers, row + [""] * (len(headers)-len(row))))
        if entry.get("ranking_run_id") == run_id:
            symbol = entry.get("canonical_symbol")
            if symbol in target:
                raise Stage0Error("DUPLICATE_KEY", "Current day snapshot duplicate")
            target[symbol] = entry
    verified = proof["spot_verified"]
    for symbol in verified:
        row = target.get(symbol)
        if not row:
            raise Stage0Error("VERIFICATION_FAILED", "Verified spot snapshot missing")
        if (
            row.get("analysis_price_source") != PRICE_SOURCE
            or row.get("mandatory_broad_pass") != "TRUE"
            or row.get("missing_mandatory_fields") != "NONE"
            or row.get("data_completeness_pct") != "100"
        ):
            raise Stage0Error("VERIFICATION_FAILED", "Spot snapshot semantics mismatch")
        for field in MANDATORY:
            try:
                value = float(row[field])
            except (ValueError, TypeError, KeyError):
                raise Stage0Error("VERIFICATION_FAILED", "Mandatory snapshot field missing") from None
            if not isfinite(value):
                raise Stage0Error("VERIFICATION_FAILED", "Snapshot factor not finite")

    values = {
        "Live Evidence UTC Day": proof["day"],
        "Live Evidence Captured UTC": proof["timestamp"],
        "Live Redis FMP Used": str(proof["used"]),
        "Live Redis FMP Remaining": str(proof["remaining"]),
        "Live Coinbase Spot Verified": ",".join(verified) or "NONE",
        "Live Stage0 Pending Reconciliation": "TRUE" if proof["pending"] else "FALSE",
        "Live Broad Mandatory Acquired": ",".join(verified) or "NONE",
        "Live Full Pipeline Status": "NOT_VERIFIED_STAGE0_ACQUISITION_ONLY",
    }
    ws = book.worksheet("FMP_Runtime_State")
    matrix = ws.get_all_values()
    expected_headers = [
        "Setting", "Configured Value", "Runtime State", "Secret?", "Version", "Notes",
    ]
    if not matrix or matrix[0][:6] != expected_headers:
        raise Stage0Error("SCHEMA_FAILED", "FMP_Runtime_State schema drift")
    positions = {}
    for n, row in enumerate(matrix[1:], start=2):
        if not row or not row[0]:
            continue
        if row[0] in positions:
            raise Stage0Error("DUPLICATE_KEY", "Duplicate runtime setting")
        positions[row[0]] = n
    from gspread.utils import rowcol_to_a1
    updates, appends = [], []
    for setting, value in values.items():
        cells = [
            setting, value, "OBSERVED_FROM_UPSTASH_AND_SHEETS",
            "NO", VERSION,
            "Evidence snapshot; per-asset Stage0 is not full portfolio or Ranking commit.",
        ]
        rowno = positions.get(setting)
        if rowno:
            if (matrix[rowno-1] + [""]*6)[:6] != cells:
                updates.append({"range": rowcol_to_a1(rowno,1) + ":" + rowcol_to_a1(rowno,6),
                                "values":[cells]})
        else:
            appends.append(cells)
    if updates:
        ws.batch_update(updates, value_input_option="RAW")
    if appends:
        ws.append_rows(appends, value_input_option="RAW", insert_data_option="INSERT_ROWS")
    observed = ws.get_all_values()
    saved = {}
    for row in observed[1:]:
        if row and row[0] in values:
            if row[0] in saved:
                raise Stage0Error("VERIFICATION_FAILED", "Duplicate persisted setting")
            saved[row[0]] = row[1] if len(row)>1 else ""
    if saved != values:
        raise Stage0Error("VERIFICATION_FAILED", "Runtime evidence read-back mismatch")
    return {"sync_status":"COMMITTED_VERIFY_PASS","snapshot_count":len(target),
            "spot_verified":verified,"evidence_rows":len(values),
            "ranking_permitted":False,"execution_permitted":False}


async def sync_stage0_evidence(redis):
    now = datetime.now(timezone.utc)
    day = now.date().isoformat()
    batch = Stage0BatchCoordinator(redis)
    lease = await batch.acquire()
    try:
        pending = bool(await redis.call("GET", pending_key(day)))
        if pending:
            raise Stage0Error("RECONCILIATION_REQUIRED", "Pending global Stage0 journal")
        raw = await redis.call("GET", spot_checkpoint_key(day))
        spot = []
        if raw:
            try:
                cp = json.loads(raw)
                spot = cp["completed_symbols"]
                if (cp.get("utc_date")!=day or cp.get("version")!=VERSION
                    or cp.get("provider")!=PRICE_SOURCE
                    or cp.get("status")!="COMMITTED_VERIFY_PASS"
                    or not isinstance(spot,list) or len(set(spot))!=len(spot)
                    or any(x not in ("BTC","ETH") for x in spot)):
                    raise ValueError("Invalid spot proof")
            except (ValueError, TypeError, KeyError):
                raise Stage0Error("RECONCILIATION_REQUIRED","Invalid spot checkpoint") from None
        quota = await redis.quota()
        if quota.get("utc_date")!=day:
            raise Stage0Error("VALIDATION_FAILED","Quota day mismatch")
        used,remain=quota.get("used"),quota.get("remaining")
        if (not isinstance(used,int) or not isinstance(remain,int)
            or used<0 or remain<0 or used+remain!=240):
            raise Stage0Error("VALIDATION_FAILED","Redis quota malformed")
        evidence={"day":day,"timestamp":now.isoformat(),
                  "spot_verified":sorted(spot), "used":used, "remaining":remain,
                  "pending":pending}
        written=await asyncio.to_thread(_sheet_evidence_sync,evidence)
        return {"version":VERSION,**written,
                "utc_day":day,"fmp_used":used,"fmp_remaining":remain,
                "spot_verified":sorted(spot),"pending_reconciliation":pending}
    finally:
        await batch.release(lease)
