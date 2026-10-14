"""Coverage routing tests; no network or credentials required."""
import unittest
from stage0_daily import classify, plan_from_universe

class CoverageTests(unittest.TestCase):
    def test_routes(self):
        cases = {
            "Equity": "FMP_CANDIDATE",
            "Index": "FMP_CANDIDATE",
            "Index / ETF / Basket": "FMP_CANDIDATE",
            "Fixed Income ETF": "FMP_CANDIDATE",
            "Crypto": "CRYPTO_PROVIDER_PENDING",
            "Commodity": "COMMODITY_PROVIDER_PENDING",
            "FX": "FX_MACRO_PROVIDER_PENDING",
            "Rates / Fixed Income": "OFFICIAL_RATES_PROVIDER_PENDING",
            "Synthetic / Private / Thematic": "ALTERNATIVE_PROVIDER_PENDING",
        }
        for asset_class, expected in cases.items():
            with self.subTest(asset_class=asset_class):
                self.assertEqual(classify(asset_class), expected)

    def test_no_repeated_verified_or_deferred(self):
        entries = [
            {"symbol": "AAPL", "route": "FMP_CANDIDATE"},
            {"symbol": "AAOI", "route": "FMP_CANDIDATE"},
            {"symbol": "AMD", "route": "FMP_CANDIDATE"},
            {"symbol": "BTC", "route": "CRYPTO_PROVIDER_PENDING"},
        ]
        status = {"utc_date": "2026-10-12",
                  "checkpoint": {"completed_symbols": ["AAPL"]},
                  "deferred_today": {"AAOI": "FMP_HTTP_402"},
                  "quota": {"remaining": 200},
                  "pending_reconciliation": False}
        result = plan_from_universe(entries, status)
        self.assertEqual(result["next_symbols"], ["AMD"])
        self.assertEqual(result["universe_enabled_count"], 4)
        self.assertEqual(result["fmp_completed_today"], 1)
        self.assertEqual(result["fmp_deferred_today"], 1)
        self.assertFalse(result["ranking_permitted"])

    def test_precutoff_prioritizes_benchmark_and_user_critical_equities(self):
        entries = [
            {"symbol": "ZM", "route": "FMP_CANDIDATE"},
            {"symbol": "TSLA", "route": "FMP_CANDIDATE"},
            {"symbol": "SPY", "route": "FMP_CANDIDATE"},
            {"symbol": "AAOI", "route": "FMP_CANDIDATE"},
            {"symbol": "NVDA", "route": "FMP_CANDIDATE"},
            {"symbol": "AAPL", "route": "FMP_CANDIDATE"},
        ]
        status = {"utc_date": "2026-10-14",
                  "checkpoint": {"completed_symbols": []},
                  "deferred_today": {},
                  "fmp_quote_402_cooldowns": {},
                  "quota": {"remaining": 240},
                  "pending_reconciliation": False}
        result = plan_from_universe(entries, status, batch_size=5)
        self.assertEqual(result["next_symbols"], [
            "SPY", "TSLA", "AAPL", "NVDA", "AAOI"
        ])
        status["fmp_quote_402_cooldowns"] = {"TSLA": "2026-10-19T00:00:00Z"}
        result = plan_from_universe(entries, status, batch_size=5)
        self.assertEqual(result["next_symbols"], [
            "SPY", "AAPL", "NVDA", "AAOI", "ZM"
        ])
        self.assertEqual(result["fmp_http402_skipped_symbols"], ["TSLA"])

    def test_ge_explicit_one_day_quarantine_does_not_mark_provider_deferred(self):
        from unittest.mock import patch
        items=[{"symbol":"GE","route":"FMP_CANDIDATE"},
               {"symbol":"COST","route":"FMP_CANDIDATE"},
               {"symbol":"CVX","route":"FMP_CANDIDATE"},
               {"symbol":"HIMS","route":"FMP_CANDIDATE"}]
        state={"utc_date":"2026-10-14",
               "checkpoint":{"completed_symbols":["COST","CVX"]},
               "deferred_today":{},
               "fmp_quote_402_cooldowns":{},
               "quota":{"remaining":130},
               "pending_reconciliation":False}
        with patch.dict("os.environ",{"ASKLIAM_STAGE0_QUARANTINE_GE_UTC_DAY":"2026-10-14"}):
            plan=plan_from_universe(items,state)
            self.assertEqual(plan["next_symbols"],["HIMS"])
            self.assertEqual(plan["fmp_operationally_held_symbols"],["GE"])
            self.assertEqual(plan["fmp_deferred_today"],0)
            self.assertEqual(plan["fmp_completed_today"],2)
            state["utc_date"]="2026-10-15"
            tomorrow=plan_from_universe(items,state)
            self.assertEqual(tomorrow["fmp_operationally_held_symbols"],[])
            self.assertEqual(tomorrow["next_symbols"],["GE"])

if __name__ == "__main__":
    unittest.main()
