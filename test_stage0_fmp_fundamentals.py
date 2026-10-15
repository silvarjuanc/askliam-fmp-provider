"""Financial fundamentals are independent from quote/historical gate and cached."""
import unittest
from stage0_fmp_fundamental_factors import extract_ttm,serialized_evidence
from app import Provider,Query

class RatioExtraction(unittest.TestCase):
    def test_independent_groups_are_real_not_arbitrary_count(self):
        x=extract_ttm("AAPL",[{"symbol":"AAPL","netProfitMarginTTM":-0.11,
            "priceToEarningsRatioTTM":32,"freeCashFlowOperatingCashFlowRatioTTM":0.8,
            "debtToEquityRatioTTM":0.3}],"2026-10-15T08:20:00Z")
        self.assertEqual(x["fundamental_factor_count"],4)
        self.assertIn("FMP_RATIOS_TTM",serialized_evidence(x))
        self.assertEqual(x["fundamental_ratios"]["valuation"]["value"],32)
    def test_missing_or_wrong_company_never_earns_factor(self):
        self.assertIsNone(extract_ttm("AAPL",[{"symbol":"MSFT","netProfitMarginTTM":0.1}],"x")["fundamental_factor_count"])
        self.assertIsNone(extract_ttm("AAPL",[],"x")["fundamental_factor_count"])
        self.assertIsNone(extract_ttm("AAPL",[{"symbol":"AAPL","priceToEarningsRatioTTM":-2}],"x")["fundamental_factor_count"])

class DummyState:
    def __init__(self,remaining=150):self.remaining=remaining
    async def quota(self):return {"remaining":self.remaining,"used":240-self.remaining,"blocked":False}

class FundamentalsProvider(unittest.IsolatedAsyncioTestCase):
    async def test_ratio_gate_is_data_not_a_score(self):
        engine=Provider()
        engine.state=DummyState()
        calls=[]
        async def mock_get(kind,symbol):
            calls.append(kind)
            if kind=="quote":
                return {"status":"READY","data":[{"symbol":"AAPL","price":100}],
                        "source_url_template":"https://financialmodelingprep.com/stable/quote?symbol=AAPL"}
            if kind=="fundamental":
                return {"status":"READY","data":[{"symbol":"AAPL","netProfitMarginTTM":0.2}],
                        "data_timestamp":"2026-10-15T08:20:00Z",
                        "source_url_template":"https://financialmodelingprep.com/stable/ratios-ttm?symbol=AAPL"}
            raise AssertionError(kind)
        engine.get=mock_get
        data=await engine.batch(Query(symbols=["AAPL"],include_profiles=False,
                                       include_historical=False,include_fundamentals=True))
        record=data["results"][0]
        self.assertEqual(calls,["quote","fundamental"])
        self.assertEqual(record["fields"]["fundamental_factor_count"],1)
        self.assertNotIn("fundamental_score",record["fields"])
        self.assertEqual(record["enrichment_status"],"ACQUIRED_NOT_SCORED")
        self.assertIn("ratios-ttm",record["source_urls"][1])

    async def test_no_entitlement_does_not_cancel_the_price(self):
        engine=Provider()
        engine.state=DummyState()
        async def mock_get(kind,symbol):
            if kind=="quote":
                return {"status":"READY","data":[{"symbol":"AAPL","price":100}]}
            return {"status":"PROVIDER_DEFERRED","error":"FMP_HTTP_402"}
        engine.get=mock_get
        data=await engine.batch(Query(symbols=["AAPL"],include_profiles=False,
                                       include_historical=False,include_fundamentals=True))
        record=data["results"][0]
        self.assertNotIn("fundamental_factor_count",record["fields"])
        self.assertEqual(record["fundamental_provider_error"],"FMP_HTTP_402")
        self.assertEqual(record["provider_errors"],[])
        self.assertEqual(record["enrichment_status"],"ACQUIRED_NOT_SCORED")

    async def test_protected_reserve_disables_fundamental_attempt(self):
        engine=Provider()
        engine.state=DummyState(remaining=40)
        calls=[]
        async def mock_get(kind,symbol):
            calls.append(kind)
            return {"status":"READY","data":[{"symbol":"AAPL","price":100}]}
        engine.get=mock_get
        data=await engine.batch(Query(symbols=["AAPL"],include_profiles=False,
                                       include_historical=False,include_fundamentals=True))
        self.assertEqual(calls,["quote"])
        self.assertEqual(data["results"][0]["fundamental_provider_error"],"FMP_FUNDAMENTAL_QUOTA_RESERVE")

if __name__=="__main__":
    unittest.main()
