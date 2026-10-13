import unittest
from datetime import datetime,timedelta,timezone
from stage0_crypto_spot_ohlcv import validate_ohlc,OHLCError
NOW=datetime(2026,10,13,13,tzinfo=timezone.utc)
START=datetime(2025,12,17,tzinfo=timezone.utc)

def fixture(days=300,bad=False):
 rows=[]
 for i in range(days):
  day=START+timedelta(days=i)
  rows.append({"time":int(day.timestamp()),"open":100+i,"high":103+i,
   "low":99+i,"close":102+i,"volumeto":100000+i})
 if bad: rows[100]["high"]=1
 return {"Response":"Success","Data":{"Data":rows}}

class SpotOHLCVTests(unittest.TestCase):
 def test_300_complete_daily_spot_bars(self):
  p=validate_ohlc(fixture(),"BTC",NOW)
  self.assertEqual(p["daily_bars"],300)
  self.assertFalse(p["ranking_permitted"])
  self.assertEqual(p["source"],"CRYPTOCOMPARE_CCCAGG_SPOT_USD")
 def test_no_broken_candle(self):
  with self.assertRaisesRegex(OHLCError,"OHLC_INVALID_CANDLE"):
   validate_ohlc(fixture(bad=True),"BTC",NOW)
 def test_no_missing_sessions(self):
  p=fixture()
  p["Data"]["Data"].pop(101)
  with self.assertRaisesRegex(OHLCError,"OHLC_DATE_GAP"):
   validate_ohlc(p,"ETH",NOW)
if __name__=="__main__": unittest.main()
