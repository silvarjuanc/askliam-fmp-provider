import unittest
from datetime import datetime,timedelta,timezone
from stage1_live_quotes import accept_quote,acquire_live_quotes,enrich_readiness,fmp_source_time

NOW=datetime(2026,10,15,14,0,tzinfo=timezone.utc)

class Response:
    def __init__(self,body):self.body=body
    def raise_for_status(self):pass
    def json(self):return self.body

class Client:
    def __init__(self,ticker):self.ticker=ticker
    async def get(self,url,timeout):return Response(self.ticker)

async def fmp_get(kind,symbol):
    return {"status":"READY","data":{"symbol":symbol,"price":200.25,
                                      "timestamp":int(NOW.timestamp())},
            "data_timestamp":NOW.isoformat(),
            "source_url_template":"https://financialmodelingprep.com/stable/quote?symbol="+symbol}


class LiveQuoteTests(unittest.IsolatedAsyncioTestCase):
    def test_source_timestamp_matters_not_time_of_fetch(self):
        old=accept_quote("BTC","COINBASE_EXCHANGE_SPOT_USD","99",
             (NOW-timedelta(minutes=25)).isoformat(),NOW,
             "https://api.exchange.coinbase.com/products/BTC-USD/ticker")
        self.assertFalse(old["quote_fresh_15m"])
        self.assertIsNone(old["price"])
        fresh=accept_quote("BTC","COINBASE_EXCHANGE_SPOT_USD","99",
             (NOW-timedelta(minutes=2)).isoformat(),NOW,
             "https://api.exchange.coinbase.com/products/BTC-USD/ticker")
        self.assertTrue(fresh["quote_fresh_15m"])

    async def test_live_spot_and_equity_separate_sources(self):
        ticker={"price":"86100","time":NOW.isoformat()}
        result=await acquire_live_quotes(["BTC","AAPL"],client=Client(ticker),
                                         fmp_get=fmp_get,now=NOW)
        self.assertEqual(result["quotes"][0]["source"],"COINBASE_EXCHANGE_SPOT_USD")
        self.assertEqual(result["quotes"][1]["source"],"FMP_STABLE")
        self.assertTrue(all(x["quote_fresh_15m"] for x in result["quotes"]))
        self.assertFalse(result["execution_permitted"])
        self.assertFalse(result["sheet_writes_performed"])

    def test_only_validated_history_with_mandatory_complete_can_be_ready(self):
        report={"version":"V6.5.1","results":[{
            "symbol":"BTC","history_status":"TECHNICAL_FACTORS_RECOMPUTED",
            "history_fresh":True,"source_quality_status":"INDEPENDENT_SPOT_VALIDATED",
            "mandatory_missing":[],"quote_fresh_15m":False,
        },{
            "symbol":"SPY","history_status":"TECHNICAL_FACTORS_RECOMPUTED",
            "history_fresh":True,
            "source_quality_status":"FMP_STRUCTURAL_CHECK_PASS_SOURCE_UNVERIFIED",
            "mandatory_missing":["macro_score"],"quote_fresh_15m":False,
        }]}
        quotes={"version":"V6.5.1","quotes":[
            accept_quote("BTC","COINBASE_EXCHANGE_SPOT_USD",100,
                         NOW.isoformat(),NOW,"https://api.exchange.coinbase.com"),
            accept_quote("SPY","FMP_STABLE",100,
                         NOW.isoformat(),NOW,"https://financialmodelingprep.com"),
        ]}
        result=enrich_readiness(report,quotes)
        self.assertEqual(result["live_broad_input_ready_count"],1)
        self.assertFalse(result["results"][1]["live_broad_input_ready"])
        self.assertFalse(result["ranking_permitted"])
        self.assertFalse(result["sheet_writes_performed"])

    def test_fmp_source_timestamp_is_not_fetch_timestamp(self):
        from datetime import timedelta
        source=(NOW-timedelta(minutes=33))
        observed=fmp_source_time(int(source.timestamp()))
        self.assertEqual(observed,source.isoformat())
        q=accept_quote("AAPL","FMP_STABLE",202,observed,NOW,
            "https://financialmodelingprep.com/stable/quote?symbol=AAPL")
        self.assertFalse(q["quote_fresh_15m"])
        self.assertIsNone(q["price"])
        self.assertIsNone(fmp_source_time(None))
        self.assertIsNone(fmp_source_time("bad"))
        self.assertEqual(fmp_source_time(int(NOW.timestamp()*1000)),
                         NOW.isoformat())

    async def test_fmp_without_source_time_is_not_fresh(self):
        async def source_missing_timestamp(kind,symbol):
            return {"status":"READY","data":{"symbol":symbol,"price":250},
                    "data_timestamp":NOW.isoformat()}
        out=await acquire_live_quotes(["AAPL"],client=Client({}),
                                      fmp_get=source_missing_timestamp,now=NOW)
        self.assertFalse(out["quotes"][0]["quote_fresh_15m"])
        self.assertEqual(out["quotes"][0]["status"],"LIVE_QUOTE_UNVERIFIED")

    def test_stale_coinbase_does_not_promote(self):
        result=accept_quote("ETH","COINBASE_EXCHANGE_SPOT_USD",200,
                (NOW-timedelta(minutes=17)).isoformat(),NOW,
                "https://api.exchange.coinbase.com")
        self.assertFalse(result["quote_fresh_15m"])

if __name__=="__main__":
    unittest.main()
