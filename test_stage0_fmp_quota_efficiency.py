"""V6.5.1 offline quota-budget regression: no duplicate quote per equity."""
import os
import unittest
from unittest.mock import patch
from stage0_fmp_adapter import ExistingFMPStage0Adapter


class FakeBook:
    def worksheet(self, tab):
        return self
    def get_all_records(self, numericise_ignore=None):
        # Override in test instance using __class__ subclass.
        return []


class FakeSheet(FakeBook):
    def worksheet(self, tab):
        self.tab = tab
        return self
    def get_all_records(self, numericise_ignore=None):
        if self.tab == "Universe":
            return [
                {"Symbol": sym, "Asset Class": "Equity", "Analysis Enabled": "TRUE"}
                for sym in ("AAPL", "TSLA")
            ]
        return []


class FakeResponse:
    def __init__(self, symbol, budget):
        self.symbol = symbol
        self.budget = budget
    def raise_for_status(self):
        return None
    def json(self):
        sym = self.symbol
        return {
            "version": "V6.5.1", "fmp_quota": {"used": 2, "remaining": 238},
            "results": [{
                "canonical_symbol": sym, "enrichment_status": "ACQUIRED_NOT_SCORED",
                "fields": {"last_price": 420.0},
                "provider_errors": [],
                "source_urls": ["https://financialmodelingprep.com/stable/quote?symbol=" + sym],
                "historical": ([{
                    "date": "2026-10-13", "open": 415.0, "high": 425.0,
                    "low": 410.0, "close": 420.0, "volume": 1000,
                }] if self.budget else []),
            }],
        }


class FakeHTTP:
    def __init__(self):
        self.posts = []
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def post(self, url, headers, json):
        self.posts.append((url, json))
        return FakeResponse(json["symbols"][0], json["include_historical"])


class FMPQuotaEfficiencyTests(unittest.TestCase):
    def test_two_instruments_one_adapter_call_each(self):
        http = FakeHTTP()
        with patch.dict(os.environ, {"ASKLIAM_SERVICE_TOKEN": "test-secret"}), patch(
            "stage0_fmp_adapter.httpx.Client", return_value=http
        ):
            frames = ExistingFMPStage0Adapter(
                FakeSheet(), "STAGE0-20261014-V6.5.1",
                symbols=["AAPL", "TSLA"], historical_budget=2,
            )()
        self.assertEqual(len(http.posts), 2)  # prior design did 4
        self.assertEqual([x[1]["symbols"] for x in http.posts], [["AAPL"], ["TSLA"]])
        self.assertTrue(all(x[1]["include_historical"] for x in http.posts))
        self.assertEqual(len(frames["Raw_OHLCV"]), 2)
        self.assertEqual(len(frames["Market_Data_Snapshot"]), 2)

    def test_unavailable_quote_never_commits_placeholder(self):
        class DeniedHTTP(FakeHTTP):
            def post(self, url, headers, json):
                response = FakeResponse(json["symbols"][0], True)
                def denied():
                    item = response.json()
                    item["results"][0]["fields"] = {}
                    item["results"][0]["provider_errors"] = ["FMP_HTTP_402"]
                    return item
                response.json = denied
                return response
        with patch.dict(os.environ, {"ASKLIAM_SERVICE_TOKEN": "test-secret"}), patch(
            "stage0_fmp_adapter.httpx.Client", return_value=DeniedHTTP()
        ):
            with self.assertRaisesRegex(RuntimeError, "FMP_HTTP_402"):
                ExistingFMPStage0Adapter(
                    FakeSheet(), "STAGE0-20261014-V6.5.1",
                    symbols=["AAPL"], historical_budget=1,
                )()


if __name__ == "__main__":
    unittest.main()
