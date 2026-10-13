"""Readiness audit and score math cannot create false SCORED or trades."""
import unittest
from datetime import datetime, timezone
from stage1_readiness import (
    parse_required, field_present, audit_asset,
    compute_score_from_evidence, evaluate_readiness,
)
NOW=datetime(2026,10,13,20,30,tzinfo=timezone.utc)
RUN="STAGE0-20261013-V6.5.1"
REQ={
    "Model":"CRYPTO_V1","Broad Min %":"70",
    "Broad Mandatory Data":(
        "last_price; return_1m; return_3m; volatility_20d; momentum_score; "
        "volume_score; open_interest; funding_rate; trend_score"
    ),
}
SPOT={
    "ranking_run_id":RUN,
    "canonical_symbol":"BTC",
    "score_model":"CRYPTO_V1",
    "analysis_price_source":"COINGECKO_SPOT_USD",
    "data_timestamp":"2026-10-13T20:25:00Z",
    "last_price":"123","return_1m":"2","return_3m":"4",
    "volatility_20d":"48","momentum_score":"55","volume_score":"62",
    "open_interest":"112","funding_rate":"0.00002","trend_score":"55",
    "mandatory_broad_pass":"TRUE","broad_readiness":"BROAD_SCAN_READY",
}

class ModelReadinessTests(unittest.TestCase):
    def test_crypto_all_mandatory_passing_requires_independent_quote(self):
        result=audit_asset(REQ,SPOT,expected_run_id=RUN,now=NOW)
        self.assertEqual(result["status"],"BROAD_SCAN_READY")
        self.assertEqual(result["completeness_pct"],100)
        perp=dict(SPOT,analysis_price_source="HYPERLIQUID_NATIVE_PERP_REFERENCE_ONLY")
        self.assertEqual(
            audit_asset(REQ,perp,expected_run_id=RUN,now=NOW)["status"],
            "PROVIDER_DEFERRED",
        )

    def test_incomplete_crypto_or_stale_quote_never_broad_ready(self):
        incomplete=dict(SPOT, momentum_score="")
        r=audit_asset(REQ,incomplete,expected_run_id=RUN,now=NOW)
        self.assertEqual(r["status"],"STAGE0_PARTIAL")
        self.assertEqual(r["missing_fields"],["momentum_score"])
        stale=dict(SPOT,data_timestamp="2026-10-13T18:00:00Z")
        self.assertEqual(audit_asset(
            REQ,stale,expected_run_id=RUN,now=NOW)["status"],"PROVIDER_DEFERRED")

    def test_missing_run_snapshot_is_provider_deferred_not_global_data_wait(self):
        r=audit_asset(REQ,dict(SPOT,ranking_run_id="OLD"),
                      expected_run_id=RUN,now=NOW)
        self.assertEqual(r["status"],"PROVIDER_DEFERRED")

    def test_numerical_and_alias_required_fields(self):
        self.assertTrue(field_present({"fundamental_factor_count":"2"},
                                      "fundamental_factor_count>=1"))
        self.assertFalse(field_present({"fundamental_factor_count":"0"},
                                       "fundamental_factor_count>=1"))
        self.assertTrue(field_present({"breadth_score":"22"},"breadth_score_or_proxy"))
        self.assertFalse(field_present({"breadth_score":""},"breadth_score_or_proxy"))
        with self.assertRaises(ValueError):
            parse_required("last_price; last_price")

    def test_scorer_never_imputes_risk_or_primary_factors(self):
        factors={
            "Factor 1 /20":"10","Factor 2 /20":"12","Factor 3 /20":"18",
            "Catalyst /15":"9","Momentum /15":"7.5","Macro Fit /10":"",
            "Risk Penalty":"5","Analysis Source Summary":"primary verified",
        }
        r=compute_score_from_evidence(factors)
        self.assertEqual(r["status"],"SCORE_CANDIDATE_NOT_COMMITTED")
        self.assertEqual(r["available_weight_pct"],90)
        self.assertAlmostEqual(r["score"],((10+12+18+9+7.5)/90)*100-5,places=4)
        factors["Risk Penalty"]=""
        self.assertEqual(compute_score_from_evidence(factors)["status"],"UNSCORED")
        factors["Risk Penalty"]="0"
        factors["Factor 2 /20"]=""
        self.assertEqual(compute_score_from_evidence(factors)["status"],"UNSCORED")

    def test_read_only_evaluate_no_score_without_factor_run_source(self):
        universe=[
            {"Symbol":"BTC","Analysis Enabled":"TRUE","Ranking Enabled":"TRUE"},
            {"Symbol":"ETH","Analysis Enabled":"TRUE","Ranking Enabled":"TRUE"},
            {"Symbol":"NOT_ELIGIBLE","Analysis Enabled":"TRUE","Ranking Enabled":"FALSE"},
        ]
        snapshots=[SPOT]
        staging=[{
            "Symbol":"BTC","Run ID":"RUN-OLD","Score Model":"CRYPTO_V1",
            "Factor 1 /20":"20","Factor 2 /20":"20","Factor 3 /20":"20",
            "Risk Penalty":"0","Analysis Source Summary":"verification",
        }]
        r=evaluate_readiness(
            universe=universe,requirements=[REQ],snapshots=snapshots,
            ranking_staging=staging,run_id=RUN,now=NOW,
        )
        self.assertEqual(r["eligible_universe"],2)
        self.assertEqual(r["by_status"]["BROAD_SCAN_READY"],1)
        self.assertEqual(r["score_candidates_count"],0)
        self.assertFalse(r["ranking_committed"])
        self.assertFalse(r["ranking_permitted"])
        self.assertEqual(r["per_asset"]["ETH"]["status"],"PROVIDER_DEFERRED")


if __name__=="__main__":
    unittest.main()
