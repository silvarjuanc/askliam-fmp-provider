import unittest
from datetime import datetime,timezone,timedelta
from app import technicals
from stage0_canonical_factors import canonical_technicals,volume_score
from stage0_equity_session_gate import partition_confirmed_bars

class TestFmpClosedCanonicalTechnicals(unittest.TestCase):
    @staticmethod
    def history(n):
        start=datetime(2025,1,1,tzinfo=timezone.utc)
        return [{"date":(start+timedelta(days=i)).date().isoformat(),
                 "close":100.0 + 0.6*i,"volume":10000+i*70} for i in range(n)]

    def test_all_score_transforms_use_canonical_functions(self):
        bars=self.history(300)
        actual=technicals(bars)
        canonical=canonical_technicals([x["close"] for x in bars])
        self.assertAlmostEqual(actual["momentum_score"],canonical["momentum_score"])
        self.assertAlmostEqual(actual["trend_score"],canonical["trend_score"])
        last=bars[-1]["volume"]
        avg=sum(x["volume"] for x in bars[-21:-1])/20
        self.assertAlmostEqual(actual["volume_score"],volume_score(
            last,avg,source_verified=True))
        self.assertNotIn("macro_score",actual)
        self.assertNotIn("fundamental_score",actual)

    def test_insufficient_history_cannot_gain_canonical_momentum(self):
        out=technicals(self.history(100))
        self.assertNotIn("momentum_score",out)
        self.assertNotIn("trend_score",out)

    def test_intraday_partial_candle_must_be_rejected_before_factor_transform(self):
        bars=self.history(300)
        now=datetime(2026,10,14,15,0,tzinfo=timezone.utc)
        # Last current day bar is an intraday, fabricated outlier in test only.
        partial={"date":"2026-10-14","close":999999.0,"volume":1}
        kept,rejected=partition_confirmed_bars(bars+[partial],now)
        self.assertEqual(len(rejected),1)
        self.assertEqual(len(kept),300)
        self.assertAlmostEqual(
            technicals(kept)["momentum_score"],
            canonical_technicals([b["close"] for b in bars])["momentum_score"]
        )

    def test_missing_volume_never_forces_volume_score(self):
        bars=self.history(300)
        bars[-1]["volume"]=None
        out=technicals(bars)
        self.assertNotIn("volume_score",out)
        self.assertIn("momentum_score",out)

if __name__=="__main__":unittest.main()
