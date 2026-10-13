import unittest
from stage0_ranking_preview import preview_broad

RUN="STAGE0-20261013-V6.5.1"
SPEC=[{"Model":"CRYPTO_V1","Broad Min %":"70","Broad Mandatory Data":"last_price; momentum_score; trend_score"}]
UNI=[{"Symbol":"BTC","Analysis Enabled":"TRUE","Ranking Enabled":"TRUE"},
     {"Symbol":"ETH","Analysis Enabled":"TRUE","Ranking Enabled":"TRUE"}]

class PreviewTests(unittest.TestCase):
 def test_unknown_snapshots_cannot_be_ranked(self):
  x=preview_broad(UNI,[],SPEC,{},RUN)
  self.assertEqual(x["scored_count"],0)
  self.assertFalse(x["publication_permitted"])
 def test_valid_one_scored_second_data_wait(self):
  snap=[{"ranking_run_id":RUN,"canonical_symbol":"BTC",
         "score_model":"CRYPTO_V1","last_price":100,
         "momentum_score":70,"trend_score":60}]
  evidence={"BTC":{"source_quality_verified":True,"risk_penalty":2,
                   "factor_points":{"factor_1":10,"factor_2":10,"factor_3":10}}}
  x=preview_broad(UNI,snap,SPEC,evidence,RUN)
  self.assertEqual(x["scored_count"],1)
  self.assertEqual(x["top30_count"],1)
  self.assertEqual(x["rows"][0]["global_score"],48)
  self.assertEqual(x["rows"][1]["score_status"],"DATA_WAIT")
  self.assertFalse(x["sheet_writes_performed"])
 def test_no_quality_proof_means_no_rank(self):
  snap=[{"ranking_run_id":RUN,"canonical_symbol":"BTC",
         "score_model":"CRYPTO_V1","last_price":100,
         "momentum_score":70,"trend_score":60}]
  x=preview_broad(UNI,snap,SPEC,{"BTC":{"risk_penalty":0}},RUN)
  self.assertEqual(x["scored_count"],0)

if __name__=="__main__": unittest.main()
