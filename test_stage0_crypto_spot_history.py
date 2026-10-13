"""Offline checks for independent daily spot observations."""
import unittest
from datetime import datetime, timedelta, timezone
from stage0_crypto_spot_history import (
 SpotDataError, validate_spot_series, raw_spot_indicators, readiness_preview,
)
NOW=datetime(2026,10,13,12,tzinfo=timezone.utc)
START=datetime(2025,10,13,tzinfo=timezone.utc)
def fixture(days=365, gap=None, mismatch=False):
 p=[];v=[]
 for i in range(days):
  if i==gap: continue
  stamp=int((START+timedelta(days=i)).timestamp()*1000)
  p.append([stamp,100+i])
  if not mismatch or i!=50: v.append([stamp,100000+i])
 return {"prices":p,"total_volumes":v}
class SpotTests(unittest.TestCase):
 def test_valid_not_scored(self):
  h=validate_spot_series(fixture(),"BTC",NOW)
  self.assertEqual(h["daily_observations"],365)
  self.assertNotIn("open",h["rows"][0])
  f=raw_spot_indicators(h)
  self.assertEqual(f["last_price"],464)
  self.assertFalse(readiness_preview(h,f)["ranking_permitted"])
 def test_gap(self):
  with self.assertRaisesRegex(SpotDataError,"SPOT_DAILY_GAP"):
   validate_spot_series(fixture(gap=90),"ETH",NOW)
 def test_mismatch(self):
  with self.assertRaisesRegex(SpotDataError,"SPOT_DATE_MISMATCH"):
   validate_spot_series(fixture(mismatch=True),"ADA",NOW)
 def test_insufficient(self):
  with self.assertRaisesRegex(SpotDataError,"SPOT_INSUFFICIENT_HISTORY"):
   validate_spot_series(fixture(days=200),"SOL",NOW)
 def test_stale(self):
  with self.assertRaisesRegex(SpotDataError,"SPOT_STALE"):
   validate_spot_series(fixture(days=290),"SUI",NOW)
 def test_current_day_ignored(self):
  data=fixture()
  stamp=int(NOW.timestamp()*1000)
  data["prices"].append([stamp,99999])
  data["total_volumes"].append([stamp,99999])
  self.assertEqual(validate_spot_series(data,"XRP",NOW)["rows"][-1]["spot_price"],464)
if __name__=="__main__": unittest.main()
