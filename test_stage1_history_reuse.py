import unittest
from datetime import date, datetime, timedelta, timezone
from stage1_history_reuse import validate_candles, preview

ASOF=datetime(2026,10,14,15,30,tzinfo=timezone.utc)
RUN="STAGE0-20261014-V6.5.1"

def history(symbol="SPY", model="INDEX_V1", source="FMP", validated=False):
    rows=[]
    days=[]
    d=date(2026,10,13)
    while len(days)<270:
        if model=="CRYPTO_V1" or d.weekday()<5:
            days.append(d)
        d-=timedelta(days=1)
    for d in reversed(days):
        x=100+len(rows)*.25
        rows.append({
                "ranking_run_id":"STAGE0-20261012-V6.5.1",
                "canonical_symbol":symbol, "source":source,
                "trading_date":d.isoformat(),
                "open":str(x), "high":str(x+2), "low":str(x-2),
                "close":str(x+1), "volume":"1500000",
                "quality_status":("INDEPENDENT_SPOT_OHLCV_VALIDATED"
                    if validated else "RECEIVED_UNVERIFIED"),
            })
    return rows


class Stage1HistoryReuseTests(unittest.TestCase):
    def test_cross_run_history_computes_canonical_but_unverified_fmp_stays_blocked(self):
        rows=history()
        output=validate_candles("SPY","INDEX_V1",rows,ASOF)
        self.assertEqual(output["status"],"TECHNICAL_FACTORS_RECOMPUTED")
        self.assertEqual(output["observations"],270)
        self.assertIn("momentum_score",output["factors"])
        self.assertIn("trend_score",output["factors"])
        self.assertFalse(output["source_quality_verified"])
        self.assertNotIn("volume_score",output["factors"])

    def test_coinbase_verified_volume_score_and_365d_vol(self):
        rows=history("BTC","CRYPTO_V1","COINBASE_EXCHANGE_SPOT_USD",True)
        output=validate_candles("BTC","CRYPTO_V1",rows,ASOF)
        self.assertTrue(output["source_quality_verified"])
        self.assertIn("volume_score",output["factors"])

    def test_cross_source_no_perp_as_spot(self):
        rows=history("BTC","CRYPTO_V1","HYPERLIQUID_NATIVE_PERP",True)
        output=validate_candles("BTC","CRYPTO_V1",rows,ASOF)
        self.assertEqual(output["status"],"NO_VALID_HISTORY")

    def test_bad_high_low_halts_historical_promotion(self):
        rows=history()
        rows[-1]["high"]="0.01"
        output=validate_candles("SPY","INDEX_V1",rows,ASOF)
        self.assertEqual(output["status"],"HISTORY_CONFLICT_OR_INVALID")
        self.assertIn("BAD_OHLCV",output["errors"])

    def test_stale_quote_no_broad_promotion_and_missing_macro(self):
        spec={"Model":"INDEX_V1","Broad Min %":"70",
              "Broad Mandatory Data":"last_price; return_1m; return_3m; volatility_20d; momentum_score; trend_score; macro_score; breadth_score_or_proxy"}
        snapshots=[{"ranking_run_id":RUN,"canonical_symbol":"SPY",
                    "score_model":"INDEX_V1","last_price":"100",
                    "data_timestamp":"2026-10-14T08:00:00Z"}]
        output=preview(RUN,["SPY"],snapshots,[spec],history(),now=ASOF)
        record=output["results"][0]
        self.assertFalse(record["quote_fresh_15m"])
        self.assertIn("macro_score",record["mandatory_missing"])
        self.assertIn("breadth_score_or_proxy",record["mandatory_missing"])
        self.assertFalse(record["potential_analytical_readiness"])
        self.assertFalse(output["ranking_permitted"])
        self.assertFalse(output["execution_permitted"])

    def test_same_run_snapshot_required(self):
        spec={"Model":"INDEX_V1","Broad Min %":"70","Broad Mandatory Data":"last_price"}
        output=preview(RUN,["SPY"],[],[spec],history(),now=ASOF)
        self.assertEqual(output["results"][0]["status"],"NO_CURRENT_RUN_SNAPSHOT")

    def test_previous_and_spot_sources_do_not_mix(self):
        rows=history("BTC","CRYPTO_V1","COINBASE_EXCHANGE_SPOT_USD",True)
        rows+=history("BTC","CRYPTO_V1","HYPERLIQUID_NATIVE_PERP",True)
        output=validate_candles("BTC","CRYPTO_V1",rows,ASOF)
        self.assertTrue(output["source_quality_verified"])
        self.assertEqual(output["source"],"COINBASE_EXCHANGE_SPOT_USD")


if __name__=="__main__":
    unittest.main()
