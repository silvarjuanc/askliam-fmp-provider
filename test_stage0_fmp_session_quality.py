"""Read-only audit catches pre-close FMP partial bar risk."""
import unittest
from datetime import datetime,timezone
from stage0_fmp_session_quality import (
    provider_eod_date_guard, classify_stored_fmp_bar,
    audit_fmp_history_quality,
)

class FMPClosedBarGuardTests(unittest.TestCase):
    def test_early_us_open_bar_must_not_count_as_final(self):
        self.assertFalse(provider_eod_date_guard(
            "2026-10-12",datetime(2026,10,12,14,0,tzinfo=timezone.utc)
        ))
        self.assertFalse(provider_eod_date_guard(
            "2026-10-13",datetime(2026,10,13,20,0,tzinfo=timezone.utc)
        ))
        self.assertTrue(provider_eod_date_guard(
            "2026-10-12",datetime(2026,10,13,10,0,tzinfo=timezone.utc)
        ))

    def test_flags_existing_session_bar_fetched_before_close(self):
        r={
            "canonical_symbol":"AAPL","source":"FMP",
            "trading_date":"2026-10-12",
            "fetched_at_utc":"2026-10-12T09:30:35+00:00",
            "upsert_key":"AAPL|2026-10-12|FMP",
        }
        self.assertEqual(classify_stored_fmp_bar(r)["state"],
                         "ACQUIRED_BEFORE_EOD_BUFFER")
        audit=audit_fmp_history_quality([r])
        self.assertEqual(audit["premature_or_invalid_rows"],1)
        self.assertFalse(audit["ranking_permitted"])
        self.assertFalse(audit["sheet_writes_performed"])

    def test_does_not_claim_officially_final_after_buffer(self):
        r={
            "canonical_symbol":"AAPL","source":"FMP",
            "trading_date":"2026-10-12",
            "fetched_at_utc":"2026-10-13T09:30:35Z",
            "upsert_key":"AAPL|2026-10-12|FMP",
        }
        self.assertEqual(classify_stored_fmp_bar(r)["state"],
                         "ACQUIRED_AFTER_EOD_BUFFER_UNCONFIRMED")
        self.assertEqual(audit_fmp_history_quality([r])["premature_or_invalid_rows"],0)


if __name__=="__main__":
    unittest.main()
