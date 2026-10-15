"""Mandatory inputs are model-specific; acquisition alone is never scoring."""
import unittest
from stage0_fmp_adapter import apply_model_requirements

MODEL_SPECS={
  "INDEX_V1":{"Model":"INDEX_V1","Broad Min %":"70",
    "Broad Mandatory Data":"last_price; return_1m; return_3m; volatility_20d; momentum_score; trend_score; macro_score; breadth_score_or_proxy"},
  "EQUITY_V2":{"Model":"EQUITY_V2","Broad Min %":"70",
    "Broad Mandatory Data":"last_price; return_1m; return_3m; volatility_20d; momentum_score; trend_score; relative_strength_score; volume_score; macro_score; fundamental_factor_count>=1"},
}

class ModelGateTests(unittest.TestCase):
    def setUp(self):
        self.base={"last_price":"770","return_1m":"2","return_3m":"6",
                   "volatility_20d":"18","momentum_score":"61",
                   "trend_score":"58"}
    def test_spy_index_missing_breadth_and_macro_not_fundamentals(self):
        s=apply_model_requirements(dict(self.base,score_model="INDEX_V1"),MODEL_SPECS)
        self.assertEqual(s["missing_mandatory_fields"],"macro_score; breadth_score_or_proxy")
        self.assertEqual(s["data_completeness_pct"],75)
        self.assertNotIn("fundamental_factor_count",s["missing_mandatory_fields"])
        self.assertEqual(s["mandatory_broad_pass"],"FALSE")
    def test_equity_cannot_skip_fundamental_minimum(self):
        s=apply_model_requirements(dict(self.base,score_model="EQUITY_V2"),MODEL_SPECS)
        self.assertIn("fundamental_factor_count>=1",s["missing_mandatory_fields"])
        self.assertIn("relative_strength_score",s["missing_mandatory_fields"])
    def test_all_fields_present_does_not_promote_provenance_gate(self):
        row=dict(self.base,score_model="INDEX_V1",macro_score="55",breadth_score="56")
        s=apply_model_requirements(row,MODEL_SPECS)
        self.assertEqual(s["data_completeness_pct"],100)
        self.assertEqual(s["missing_mandatory_fields"],"NONE")
        self.assertEqual(s["mandatory_broad_pass"],"FALSE")
    def test_unknown_model_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError,"DATA_REQUIREMENTS_MISSING_MODEL"):
            apply_model_requirements({"score_model":"UNKNOWN"},MODEL_SPECS)

if __name__=="__main__":
    unittest.main()
