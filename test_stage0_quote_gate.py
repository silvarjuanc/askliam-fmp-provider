"""Offline regression: Stage0 quote gate minimizes FMP calls."""
import os
import unittest
from unittest.mock import patch
import stage0_fmp_adapter as adapter_module
from stage0_fmp_adapter import ExistingFMPStage0Adapter


class FakeSheet:
    def __init__(self, rows):
        self.rows = rows
    def get_all_records(self, **kwargs):
        return self.rows


class FakeBook:
    def worksheet(self, name):
        if name == "Universe":
            return FakeSheet([{"Analysis Enabled": "TRUE",
                               "Symbol": "BE", "Asset Class": "Equity"}])
        if name == "Market_Data_Snapshot":
            return FakeSheet([])
        raise AssertionError(name)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload
    def raise_for_status(self):
        pass
    def json(self):
        return self.payload


def response(*, errors=None, with_historical=False):
    return {
        "version": "V6.5.1",
        "fmp_quota": {"remaining": 176},
        "results": [{
            "canonical_symbol": "BE",
            "fields": {"last_price": 25.5} if not errors else {},
            "provider_errors": errors or [],
            "enrichment_status":
                "PROVIDER_DEFERRED" if errors else "ACQUIRED_NOT_SCORED",
            "historical": [
                {"date": "2026-10-09", "open": 24, "high": 26,
                 "low": 23, "close": 25.5, "volume": 1000000}
            ] if with_historical else [],
            "source_urls": [],
        }],
    }


class ClientMock:
    def __init__(self, *args, responses=None, **kwargs):
        self.responses = list(responses or [])
        self.payloads = []
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def post(self, url, *, headers, json):
        self.payloads.append(json)
        if not self.responses:
            raise AssertionError("Extra call")
        return FakeResponse(self.responses.pop(0))


class QuoteGateTests(unittest.TestCase):
    def setUp(self):
        self.secret = patch.dict(os.environ,
                                 {"ASKLIAM_SERVICE_TOKEN": "offline-test-only"})
        self.secret.start()
        self.adapter = ExistingFMPStage0Adapter(FakeBook(), "TEST-V6.5.1",
                             symbols=["BE"], max_symbols=1, historical_budget=1)
    def tearDown(self):
        self.secret.stop()

    def test_402_stops_before_history(self):
        client = ClientMock(responses=[response(errors=["FMP_HTTP_402"])])
        with patch.object(adapter_module.httpx, "Client", return_value=client):
            with self.assertRaisesRegex(RuntimeError, "FMP_HTTP_402"):
                self.adapter()
        self.assertEqual(len(client.payloads), 1)
        self.assertFalse(client.payloads[0]["include_historical"])
        self.assertFalse(client.payloads[0]["include_profiles"])

    def test_usable_quote_fetches_history_without_profile(self):
        client = ClientMock(responses=[
            response(), response(with_historical=True),
        ])
        with patch.object(adapter_module.httpx, "Client", return_value=client):
            frames = self.adapter()
        self.assertEqual(len(client.payloads), 2)
        self.assertTrue(client.payloads[1]["include_historical"])
        self.assertTrue(all(not x["include_profiles"] for x in client.payloads))
        self.assertEqual(len(frames["Raw_OHLCV"]), 1)
        self.assertEqual(len(frames["Market_Data_Snapshot"]), 1)
        self.assertEqual(frames["Market_Data_Snapshot"].iloc[0]["mandatory_broad_pass"], "FALSE")


if __name__ == "__main__":
    unittest.main()
