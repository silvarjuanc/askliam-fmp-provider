import unittest
from datetime import date,datetime,timedelta,timezone
from unittest.mock import patch
import stage0_fmp_adapter as adapter_module
from stage0_fmp_eod_technicals import closed_session_factors,verified_completed_candles
from stage0_canonical_factors import canonical_technicals
from stage0_fmp_adapter import ExistingFMPStage0Adapter
from test_stage0_quote_gate import FakeBook, ClientMock

UTC=timezone.utc
NOW=datetime(2026,10,14,12,0,tzinfo=UTC)
def candles(n=260):
    start=date(2025,1,1)
    return [{
       "date":(start+timedelta(days=i)).isoformat(),
       "open":100+i*.2,
       "high":102+i*.2,
       "low":99+i*.2,
       "close":100.5+i*.2,
       "volume":100000+i*100,
    } for i in range(n)]

class EODTest(unittest.TestCase):
    def test_260_complete_produces_canonical_momentum_trend_volume(self):
        out=closed_session_factors(candles(),NOW)
        self.assertEqual(out["technical_status"],"CALCULATED_FROM_FMP_COMPLETED_EOD_NOT_SCORED")
        expected=canonical_technicals([x["close"] for x in candles()])
        self.assertAlmostEqual(out["values"]["momentum_score"],expected["momentum_score"])
        self.assertAlmostEqual(out["values"]["trend_score"],expected["trend_score"])
        self.assertIsNotNone(out["values"]["volume_score"])
        self.assertNotIn("macro_score",out["values"])
        self.assertNotIn("fundamental_factor_count",out["values"])
    def test_incomplete_current_daily_bar_does_not_affect_technicals(self):
        old=candles()
        latest=dict(old[-1],date="2026-10-14",close=0.01,
                    open=0.01,high=0.01,low=0.01,volume=12)
        before=closed_session_factors(old,NOW)
        after=closed_session_factors(old+[latest],NOW)
        self.assertEqual(after["in_session_rejected"],1)
        self.assertEqual(before["values"],after["values"])
    def test_duplicate_eod_rejected_not_used(self):
        raw=candles()
        bad=raw+[dict(raw[-1],close=999)]
        result=closed_session_factors(bad,NOW)
        self.assertEqual(result["values"],{})
        self.assertEqual(result["technical_status"],"INSUFFICIENT_COMPLETED_EOD")
    def test_short_eod_no_fabricated_fields(self):
        out=closed_session_factors(candles(200),NOW)
        self.assertEqual(out["values"],{})
    def test_ohlcv_fails_closed_on_inconsistent_bar(self):
        raw=candles()
        raw[120]["high"]=1
        out=closed_session_factors(raw,NOW)
        self.assertEqual(out["values"],{})
    def test_fmp_adapter_emits_technicals_without_claiming_macro_score(self):
        raw=candles()
        provider=lambda use_hist:{
           "version":"V6.5.1","fmp_quota":{"remaining":150},
           "results":[{"canonical_symbol":"BE",
              "fields":{"last_price":205.21,
                        "return_3m":9999,"momentum_score":100},
              "provider_errors":[],"enrichment_status":"ACQUIRED_NOT_SCORED",
              "historical":raw if use_hist else None,"source_urls":[]}]
        }
        client=ClientMock(responses=[provider(False),provider(True)])
        with patch.dict("os.environ",{"ASKLIAM_SERVICE_TOKEN":"offline-only"}), \
             patch.object(adapter_module.httpx,"Client",return_value=client):
            adapter=ExistingFMPStage0Adapter(FakeBook(),"STAGE0-20261014-V6.5.1",
                 symbols=["BE"],max_symbols=1,historical_budget=1)
            frames=adapter()
        snap=frames["Market_Data_Snapshot"].iloc[0]
        self.assertEqual(len(frames["Raw_OHLCV"]),260)
        self.assertLess(snap["momentum_score"],100)
        self.assertNotEqual(snap["return_3m"],9999)
        self.assertEqual(snap["mandatory_broad_pass"],"FALSE")
        self.assertIn("macro_score",snap["missing_mandatory_fields"])
        self.assertIn("fundamental_factor_count",snap["missing_mandatory_fields"])
        self.assertNotIn("momentum_score",snap["missing_mandatory_fields"])
        self.assertIn("last_closed_session=",snap["notes"])
if __name__=="__main__":unittest.main()
