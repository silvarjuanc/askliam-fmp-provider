"""Offline Coinbase Exchange spot OHLCV source-contract tests."""
import unittest
from datetime import datetime, timedelta, timezone
from stage0_crypto_coinbase_spot import validate_candles, CoinbaseSpotError

NOW=datetime(2026,10,14,12,tzinfo=timezone.utc)
START=NOW.replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(days=299)
def fixture(n=299):
    rows=[]
    for i in range(n):
        when=int((START+timedelta(days=i)).timestamp())
        p=100+i
        rows.append([when,p-2,p+3,p,p+1,1000+i])
    return list(reversed(rows))

class CoinbaseSpotTests(unittest.TestCase):
    def test_complete_spot_ohlcv(self):
        r=validate_candles(fixture(),"BTC",NOW)
        self.assertEqual(r["daily_bars"],299)
        self.assertEqual(r["last_complete_utc_date"],"2026-10-13")
        self.assertFalse(r["mandatory_broad_pass"])
        self.assertFalse(r["ranking_permitted"])
        self.assertFalse(r["sheet_writes_performed"])
        self.assertIsNotNone(r["canonical_technicals"]["volume_score"])
    def test_duplicate_bar_fails_closed(self):
        rows=fixture()
        rows.append(rows[0])
        with self.assertRaisesRegex(CoinbaseSpotError,"DUPLICATE"):
            validate_candles(rows,"BTC",NOW)
    def test_gap_fails_closed(self):
        rows=fixture()
        del rows[100]
        with self.assertRaisesRegex(CoinbaseSpotError,"DAILY_GAP"):
            validate_candles(rows,"ETH",NOW)
    def test_incomplete_current_day_skipped(self):
        rows=fixture()
        partial=int(NOW.replace(hour=0,minute=0,second=0,microsecond=0).timestamp())
        rows.insert(0,[partial,2,4,3,3.5,2])
        r=validate_candles(rows,"BTC",NOW)
        self.assertEqual(r["daily_bars"],299)
    def test_bad_ohlc_fails_closed(self):
        rows=fixture()
        rows[20][2]=1
        with self.assertRaisesRegex(CoinbaseSpotError,"INCONSISTENT_OHLC"):
            validate_candles(rows,"BTC",NOW)
    def test_not_enough_history(self):
        rows=fixture(200)
        with self.assertRaisesRegex(CoinbaseSpotError,"HISTORY_COVERAGE"):
            validate_candles(rows,"BTC",NOW)
if __name__=="__main__": unittest.main()
