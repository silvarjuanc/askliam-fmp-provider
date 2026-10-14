import unittest
from datetime import datetime,timezone
from stage0_fmp_technical_enrichment import derived_fmp_eod_factors,TechnicalEvidenceError

OBS=datetime(2026,10,14,12,0,tzinfo=timezone.utc)
def bars(n=300):
    from datetime import timedelta
    first=datetime(2025,1,1,tzinfo=timezone.utc)
    return [dict(date=(first+timedelta(days=i)).date().isoformat(),
                 open=100+i,high=101+i,low=99+i,close=100+i,volume=1000+i) for i in range(n)]

class FMPTechnicalTests(unittest.TestCase):
    def test_300_valid_bars_generate_real_technical_factors(self):
        out=derived_fmp_eod_factors(bars(),OBS)
        self.assertEqual(out["validated_eod_rows"],300)
        self.assertEqual(out["last_eod_date"],"2025-10-27")
        self.assertIn("return_1m",out)
        self.assertIn("volatility_20d",out)
        self.assertIn("momentum_score",out)
        self.assertIn("trend_score",out)
        self.assertIn("volume_score",out)
        self.assertNotIn("macro_score",out)
        self.assertNotIn("fundamental_factor_count",out)
        self.assertFalse(out["source_independently_crosschecked"])

    def test_benchmark_only_with_matching_session(self):
        out=derived_fmp_eod_factors(bars(),OBS,benchmark_return_3m=4,
                                      benchmark_last_eod="2025-10-27")
        self.assertIn("relative_strength_score",out)
        o2=derived_fmp_eod_factors(bars(),OBS,benchmark_return_3m=4,
                                      benchmark_last_eod="2025-10-26")
        self.assertNotIn("relative_strength_score",o2)

    def test_partial_current_eod_dropped(self):
        from datetime import timedelta
        b=bars()
        b.append(dict(date="2026-10-14",open=800,high=801,low=799,close=800,volume=1))
        out=derived_fmp_eod_factors(b,OBS)
        self.assertEqual(out["excluded_partial_eod_rows"],1)

    def test_broken_ohlc_fails_closed(self):
        b=bars()
        b[99]["low"]=999
        with self.assertRaisesRegex(TechnicalEvidenceError,"INVALID_OHLC"):
            derived_fmp_eod_factors(b,OBS)

    def test_short_history_no_synthetic_factor(self):
        with self.assertRaisesRegex(TechnicalEvidenceError,"UNDER_253"):
            derived_fmp_eod_factors(bars(200),OBS)

    def test_duplicate_days_fail_closed(self):
        b=bars()
        b.append(dict(b[14]))
        with self.assertRaisesRegex(TechnicalEvidenceError,"DUPLICATE_EOD_SESSION"):
            derived_fmp_eod_factors(b,OBS)

    def test_missing_volume_must_not_generate_volume_score(self):
        b=bars()
        b[-1]["volume"]=None
        out=derived_fmp_eod_factors(b,OBS)
        self.assertNotIn("volume_score",out)

if __name__=="__main__":unittest.main()
