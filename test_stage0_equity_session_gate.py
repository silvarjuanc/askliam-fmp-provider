import unittest
from datetime import datetime,timezone
from stage0_equity_session_gate import closed_us_equity_session,partition_confirmed_bars

def utc(h,m=0):
 return datetime(2026,10,12,h,m,tzinfo=timezone.utc)

class USSessionTests(unittest.TestCase):
 def test_october_mid_session_rejected(self):
  self.assertFalse(closed_us_equity_session("2026-10-12",utc(18)))
 def test_october_after_ny_close_and_settlement(self):
  self.assertTrue(closed_us_equity_session("2026-10-12",utc(21)))
 def test_prior_historical_preserved(self):
  self.assertTrue(closed_us_equity_session("2026-10-09",utc(18)))
 def test_partition_prevents_partial_volume_commit(self):
  yes,no=partition_confirmed_bars(
   [{"date":"2026-10-09","volume":99},{"date":"2026-10-12","volume":5}],utc(18))
  self.assertEqual(len(yes),1)
  self.assertEqual(len(no),1)
  self.assertEqual(no[0]["reason"],"CURRENT_SESSION_UNCONFIRMED")
if __name__=="__main__": unittest.main()
