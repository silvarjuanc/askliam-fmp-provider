"""Offline V6.5.1 Stage1 evidence-only audit: NEVER produces a fabricated score."""
import unittest
from datetime import datetime, timedelta, timezone

from stage1_evidence_audit import (
    audit_stage1_candidates, Stage1EvidenceError, COINBASE, fresh_ticker_status,
)

NOW = datetime(2026, 10, 14, 9, 0, tzinfo=timezone.utc)
RUN_ID = "STAGE0-20261014-V6.5.1"
REQUIREMENTS = [{
    "Model": "CRYPTO_V1",
    "Broad Min %": "70",
    "Broad Mandatory Data": (
        "last_price; return_1m; return_3m; volatility_20d; momentum_score; "
        "volume_score; open_interest; funding_rate; trend_score"
    ),
}]
UNIVERSE = [
    {"Symbol": s, "Analysis Enabled": "TRUE", "Ranking Enabled": "TRUE"}
    for s in ("BTC", "ETH", "AAPL")
]
CHECKPOINT = {
    "version": "V6.5.1", "utc_date": "2026-10-14",
    "provider": COINBASE, "status": "COMMITTED_VERIFY_PASS",
    "completed_symbols": ["BTC", "ETH"],
}


def snapshot(symbol, price, ret_3m):
    source_url = "https://api.exchange.coinbase.com/products/" + symbol + "-USD/candles"
    return {
        "ranking_run_id": RUN_ID, "canonical_symbol": symbol,
        "asset_class": "Crypto", "score_model": "CRYPTO_V1",
        "data_timestamp": "2026-10-14T08:04:15+00:00",
        "analysis_price_source": COINBASE,
        "source_summary": source_url,
        "enrichment_status": "ACQUIRED_NOT_SCORED",
        "mandatory_broad_pass": "TRUE", "last_price": str(price),
        "return_1m": "1.2", "return_3m": str(ret_3m),
        "volatility_20d": "23.1", "momentum_score": "50.1",
        "volume_score": "40.3", "open_interest": "1245",
        "funding_rate": "0.00001", "trend_score": "55.3",
        "notes": (
            "Spot last_price from Coinbase ticker observed "
            "2026-10-14T08:04:10+00:00. Funding rate only from perp."
        ),
    }


def audit(**overrides):
    params = dict(
        universe=UNIVERSE,
        snapshots=[snapshot("BTC", 82000, 2.1), snapshot("ETH", 2500, 1.3)],
        requirements=REQUIREMENTS, run_id=RUN_ID,
        spot_checkpoint=CHECKPOINT, pending_reconciliation=False,
        now=NOW,
    )
    params.update(overrides)
    return audit_stage1_candidates(**params)


class Stage1AuditTests(unittest.TestCase):
    def test_two_acquired_no_invented_score_and_other_not_mass_datawait(self):
        result = audit()
        self.assertEqual(result["eligible_universe_count"], 3)
        self.assertEqual(result["current_run_eligible_snapshot_count"], 2)
        self.assertEqual(result["current_run_without_snapshot_count"], 1)
        self.assertEqual(result["spot_verified_assets"], ["BTC", "ETH"])
        self.assertEqual(result["crypto_mandatory_ready_assets"], ["BTC", "ETH"])
        self.assertEqual(result["scored_assets"], 0)
        self.assertFalse(result["sheet_writes_performed"])
        self.assertFalse(result["execution_permitted"])
        for row in result["candidate_details"]:
            self.assertEqual(row["mandatory_fields_present"], 9)
            self.assertIsNone(row["global_score"])
            self.assertFalse(row["spot_quote_fresh_for_live_signals"])
            self.assertIn("RISK_PENALTY_NOT_VALIDATED", row["score_blockers"])
        self.assertIn("relative_strength_vs_BTC_preview", result["candidate_details"][1])
        self.assertFalse(result["candidate_details"][1]["relative_strength_is_a_score_factor"])

    def test_fresh_quote_is_distinct_from_acquired_history(self):
        row = snapshot("BTC", 80000, 1)
        self.assertFalse(fresh_ticker_status(row, NOW)["fresh"])
        self.assertTrue(fresh_ticker_status(row, datetime(
            2026, 10, 14, 8, 15, tzinfo=timezone.utc
        ))["fresh"])

    def test_no_checkpoint_never_proves_ingestion(self):
        out = audit(spot_checkpoint=None)
        self.assertEqual(out["spot_verified_assets"], [])
        self.assertEqual(out["crypto_mandatory_ready_assets"], [])
        self.assertEqual(out["scored_assets"], 0)

    def test_pending_journal_blocks_verified_gate(self):
        out = audit(pending_reconciliation=True)
        self.assertEqual(out["spot_verified_assets"], [])
        self.assertEqual(out["scored_assets"], 0)

    def test_invalid_checksum_day_fails_closed(self):
        with self.assertRaisesRegex(Stage1EvidenceError, "UNTRUSTED_SPOT_CHECKPOINT"):
            audit(spot_checkpoint={**CHECKPOINT, "utc_date":"2026-10-13"})

    def test_duplicate_current_snapshot_rejected(self):
        r = snapshot("ETH", 2000, 0.2)
        with self.assertRaisesRegex(Stage1EvidenceError, "DUPLICATE"):
            audit(snapshots=[snapshot("BTC", 82000, 2.1), r, dict(r)])

    def test_missing_mandatory_not_scored(self):
        r = snapshot("BTC", 82000, 2)
        r["trend_score"] = ""
        out = audit(snapshots=[r])
        self.assertEqual(out["spot_verified_assets"], ["BTC"])
        self.assertFalse(out["candidate_details"][0]["mandatory_pass"])
        self.assertEqual(out["candidate_details"][0]["missing_mandatory_fields"], ["trend_score"])

    def test_wrong_source_never_inherits_checkpoint(self):
        r = snapshot("BTC", 82000, 2)
        r["analysis_price_source"] = "HYPERLIQUID_NATIVE_PERP"
        out = audit(snapshots=[r])
        self.assertFalse(out["candidate_details"][0]["ingestion_verified"])

    def test_unverified_future_snapshot(self):
        r = snapshot("BTC", 82000, 2)
        r["data_timestamp"] = "2026-10-14T12:00:00+00:00"
        out = audit(snapshots=[r])
        self.assertFalse(out["candidate_details"][0]["ingestion_verified"])

    def test_absent_factor_points_always_no_score(self):
        out = audit()
        self.assertTrue(all(
            entry["score_status"]=="NOT_SCORED" for entry in out["candidate_details"]
        ))


if __name__=="__main__":
    unittest.main()
