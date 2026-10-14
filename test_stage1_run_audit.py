"""Offline tests for exact-day, independent Stage0 and Ranking accounting."""
import unittest
from datetime import datetime, timezone
from stage1_run_audit import audit_daily

RUN = "STAGE0-20261014-V6.5.1"
ASOF = datetime(2026,10,14,13,45,tzinfo=timezone.utc)
SPECS = [
    {"Model":"EQUITY_V2","Broad Min %":"70",
     "Broad Mandatory Data":"last_price; momentum_score; fundamental_factor_count>=1"},
    {"Model":"CRYPTO_V1","Broad Min %":"70",
     "Broad Mandatory Data":"last_price; funding_rate; open_interest"},
]


def payload():
    universe = [
        {"Symbol":f"EQ{i}","Asset Class":"Equity",
         "Analysis Enabled":"TRUE","Ranking Enabled":"TRUE"}
        for i in range(96)]
    universe += [
        {"Symbol":f"ALT{i}","Asset Class":"Crypto",
         "Analysis Enabled":"TRUE","Ranking Enabled":"TRUE"}
        for i in range(63)]
    universe += [
        {"Symbol":"JPM","Asset Class":"Equity",
         "Analysis Enabled":"TRUE","Ranking Enabled":"FALSE"}]
    snapshots=[]
    for i in range(19):
        snapshots.append({
            "ranking_run_id":RUN,"canonical_symbol":f"EQ{i}",
            "score_model":"EQUITY_V2","last_price":"150",
            "data_timestamp":"2026-10-14T12:30:00Z",
            "analysis_price_source":"FMP_STABLE"})
    for i in range(2):
        snapshots.append({
            "ranking_run_id":RUN,"canonical_symbol":f"ALT{i}",
            "score_model":"CRYPTO_V1",
            "last_price":"120", "funding_rate":"0", "open_interest":"90",
            "data_timestamp":"2026-10-14T08:04:00Z",
            "analysis_price_source":"COINBASE_EXCHANGE_SPOT_USD"})
    snapshots.append({
        "ranking_run_id":RUN,"canonical_symbol":"JPM",
        "score_model":"EQUITY_V2","last_price":"200",
        "data_timestamp":"2026-10-14T12:40:00Z"})
    return universe,snapshots


class DailyAuditTests(unittest.TestCase):
    def test_full_20261014_counts_and_no_false_provider_claims(self):
        universe,snapshots=payload()
        result=audit_daily(universe,snapshots,SPECS,day="2026-10-14",now=ASOF)
        self.assertEqual(result["ranking_eligible"],159)
        self.assertEqual(result["stage0_snapshots"],22)
        self.assertEqual(result["stage0_eligible_acquired"],21)
        self.assertEqual(result["excluded_symbols"],["JPM"])
        self.assertEqual(result["eligible_without_current_snapshot"],138)
        self.assertEqual(result["missing_snapshot_by_route"],{"FMP":77,"OTHER":61})
        self.assertEqual(result["mandatory_complete_count"],2)
        self.assertEqual(result["eligible_quote_fresh_and_mandatory_count"],0)
        self.assertEqual(result["readiness_counts"]["ACQUIRED_INCOMPLETE"],19)
        self.assertEqual(result["readiness_counts"]["MANDATORY_COMPLETE_QUOTE_STALE"],2)
        self.assertFalse(result["provider_attempt_proven"])
        self.assertEqual(result["redis_pending"],"UNVERIFIED")
        self.assertFalse(result["ranking_permitted"])
        self.assertFalse(result["sheet_writes_performed"])

    def test_previous_run_cannot_enter_today_counts(self):
        universe,snapshots=payload()
        snapshots.append({"ranking_run_id":"STAGE0-20261013-V6.5.1",
                          "canonical_symbol":"EQ20"})
        result=audit_daily(universe,snapshots,SPECS,day="2026-10-14",now=ASOF)
        self.assertEqual(result["stage0_snapshots"],22)

    def test_duplicate_today_symbol_is_a_hard_error(self):
        universe,snapshots=payload()
        snapshots.append(dict(snapshots[0]))
        with self.assertRaisesRegex(ValueError,"DUPLICATE"):
            audit_daily(universe,snapshots,SPECS,day="2026-10-14",now=ASOF)

    def test_noneligible_snapshot_does_not_inflate_ranking(self):
        universe,snapshots=payload()
        result=audit_daily(universe,snapshots,SPECS,day="2026-10-14",now=ASOF)
        self.assertEqual(result["stage0_acquired_excluded_ranking"],1)
        self.assertNotIn("JPM",[r["canonical_symbol"] for r in result["details"]])

    def test_actual_new_quote_is_fresh_only_with_real_evidence(self):
        universe,snapshots=payload()
        snapshots[19]["data_timestamp"]="2026-10-14T13:44:00Z"
        result=audit_daily(universe,snapshots,SPECS,day="2026-10-14",now=ASOF)
        self.assertEqual(result["eligible_quote_fresh_and_mandatory_count"],1)
        self.assertFalse(result["full_pipeline_committed"])

if __name__=="__main__":
    unittest.main()
