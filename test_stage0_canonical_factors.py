import unittest
from stage0_canonical_factors import canonical_technicals,volume_score,normalize_score,relative_strength

class CanonicalFactorsTests(unittest.TestCase):
 def test_canonical_technical_scores_are_bounded(self):
  a=canonical_technicals([100+i for i in range(300)])
  for key in ("momentum_score","trend_score"):
   self.assertGreaterEqual(a[key],0)
   self.assertLessEqual(a[key],100)
  self.assertIn("return_12m",a)
  self.assertIn("volatility_20d",a)
 def test_no_volume_without_proof(self):
  self.assertIsNone(volume_score(110,100,source_verified=False))
  self.assertAlmostEqual(volume_score(110,100,source_verified=True),53.5)
 def test_score_normalization_without_faking_missing(self):
  factors={"factor_1":10,"factor_2":10,"factor_3":10}
  self.assertIsNone(normalize_score(factors,2,mandatory_complete=False))
  self.assertAlmostEqual(normalize_score(factors,2,mandatory_complete=True),48)
 def test_benchmark_strength(self):
  self.assertAlmostEqual(relative_strength(5,5),50)
if __name__=="__main__": unittest.main()
