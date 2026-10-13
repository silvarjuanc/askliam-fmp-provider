import unittest
from stage0_model_readiness import validate_mandatory, ReadinessError
SPEC={"Model":"CRYPTO_V1","Broad Min %":"70","Broad Mandatory Data":"last_price; return_1m; return_3m; volatility_20d; momentum_score; volume_score; open_interest; funding_rate; trend_score"}
class ReadinessTests(unittest.TestCase):
 def test_only_derivatives_22_percent(self):
  x=validate_mandatory({"score_model":"CRYPTO_V1","open_interest":100,"funding_rate":0},SPEC)
  self.assertEqual(x["present"],2)
  self.assertEqual(x["mandatory_coverage_pct"],22.22)
  self.assertFalse(x["broad_scan_ready"])
 def test_full_fields_do_not_activate_ranking(self):
  row={"score_model":"CRYPTO_V1"}
  for key in SPEC["Broad Mandatory Data"].split("; "): row[key]=1
  x=validate_mandatory(row,SPEC)
  self.assertTrue(x["mandatory_input_complete"])
  self.assertFalse(x["broad_scan_ready"])
  self.assertFalse(x["ranking_permitted"])
 def test_wrong_model(self):
  with self.assertRaises(ReadinessError):
   validate_mandatory({"score_model":"FX_V1"},SPEC)
if __name__=="__main__": unittest.main()
