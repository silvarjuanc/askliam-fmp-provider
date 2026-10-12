"""AskLiam V6.5.1 crypto Stage0 sheet-aware DRY RUN (read-only).

Read existing production worksheets and calculate exact INSERT/UPDATE counts
using the existing Stage0 planner. No _commit/_verify/write/Redis mutation.
Checks the single-row-per-asset-per-run snapshot for unrelated provider data.
"""
from __future__ import annotations

import pandas as pd

from stage0_crypto_ingest_prepare import SOURCE, SOURCE_PRICE
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error


def compare_prepared_crypto_frames(
    frames: dict,
    *,
    symbol: str,
    run_id: str,
    captured: dict,
) -> dict:
    """Pure comparison of prepared frames against captured sheet state."""
    if set(frames) != {"Raw_OHLCV", "Market_Data_Snapshot"}:
        raise Stage0Error("SCHEMA_FAILED", "Unexpected crypto Stage0 tab set")
    raw, snap = frames["Raw_OHLCV"], frames["Market_Data_Snapshot"]
    if (
        not isinstance(raw, pd.DataFrame)
        or not isinstance(snap, pd.DataFrame)
        or raw.empty
        or len(snap) != 1
        or not all(raw["canonical_symbol"] == symbol)
        or not all(raw["source"] == SOURCE)
        or snap.iloc[0]["ranking_run_id"] != run_id
        or snap.iloc[0]["canonical_symbol"] != symbol
        or snap.iloc[0]["analysis_price_source"] != SOURCE_PRICE
        or snap.iloc[0]["last_price"] != ""
        or snap.iloc[0]["mandatory_broad_pass"] != "FALSE"
    ):
        raise Stage0Error("SCHEMA_FAILED", "Crypto/perpetual frame safety mismatch")

    current_snapshot = captured["Market_Data_Snapshot"]["rows"].get(
        (run_id, symbol)
    )
    if current_snapshot is not None:
        # A one-row snapshot for the same asset/run cannot be overwritten with
        # derivative-only fields if it already contains independent spot data.
        if not (
            current_snapshot.get("analysis_price_source") == SOURCE_PRICE
            and current_snapshot.get("enrichment_status")
            == "ACQUIRED_PERPETUAL_NOT_SPOT"
            and not current_snapshot.get("last_price")
        ):
            return {
                "status": "BLOCKED_EXISTING_NONPERP_SNAPSHOT",
                "snapshot_collision": True,
                "sheet_writes_performed": False,
                "checkpoint_advanced": False,
                "ranking_permitted": False,
                "execution_permitted": False,
            }

    ingestor = Stage0_Daily_Ingestor(
        providers={"no_write_test": lambda: {}},
        spreadsheet_id="read-only-preflight",
    )
    all_frames = {
        "Raw_OHLCV": raw,
        "Market_Data_Snapshot": snap,
        "FMP_Runtime_State": pd.DataFrame(),
    }
    plans, expected = ingestor._plan(captured, all_frames)

    # Defensive checks: every planned Raw_OHLCV delta is on the exact provider
    # source for this symbol; FMP rows can never be changed by this adapter.
    for kind in ("updates", "inserts"):
        for change in plans["Raw_OHLCV"][kind]:
            if len(change.key) != 3 or (
                change.key[0] != symbol or change.key[2] != SOURCE
            ):
                raise Stage0Error("SCHEMA_FAILED", "Foreign historical change")
    if any(plans["FMP_Runtime_State"][kind] for kind in ("updates", "inserts")):
        raise Stage0Error("UNAUTHORIZED_TAB", "Crypto cannot update FMP state")

    planned = {
        tab: {
            "inserts": len(plan["inserts"]),
            "updates": len(plan["updates"]),
            "existing_rows_before": len(captured[tab]["rows"]),
            "expected_rows_after": len(expected[tab]),
        }
        for tab, plan in plans.items()
    }
    return {
        "status": "SHEET_DRY_RUN_PREPARED",
        "source": SOURCE,
        "canonical_symbol": symbol,
        "run_id": run_id,
        "approved_tabs_only": True,
        "snapshot_collision": False,
        "plans": planned,
        "state_before_sha256": ingestor._fingerprint(captured),
        "readback_verification_performed": False,
        "sheet_writes_performed": False,
        "checkpoint_advanced": False,
        "ranking_permitted": False,
        "execution_permitted": False,
    }


def read_only_sheet_preflight(
    frames: dict, _report: dict, symbol: str, run_id: str,
) -> dict:
    """Production reader; capture three approved tabs, never commit."""
    ingestor = Stage0_Daily_Ingestor(
        providers={"read_only_comparison": lambda: {}},
    )
    before = ingestor._capture(ingestor._open())
    return compare_prepared_crypto_frames(
        frames, symbol=symbol, run_id=run_id, captured=before
    )
