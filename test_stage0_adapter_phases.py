"""Offline phase-classification regression tests for the Stage0 adapter."""
import os
import unittest
from unittest.mock import patch

import httpx
from stage0_fmp_adapter import ExistingFMPStage0Adapter


class AdapterFailureTests(unittest.TestCase):
    def setUp(self):
        self.token = patch.dict(os.environ, {"ASKLIAM_SERVICE_TOKEN": "offline-test-token"})
        self.token.start()
        self.adapter = ExistingFMPStage0Adapter(
            spreadsheet=object(), run_id="TEST-V6.5.1",
            symbols=["BE"], max_symbols=1, historical_budget=1,
        )

    def tearDown(self):
        self.token.stop()

    def test_unclassified_runtime_has_no_secret_leak(self):
        def fail():
            self.adapter.phase = "PROVIDER_REQUEST"
            raise RuntimeError("token=do-not-log")
        self.adapter._acquire = fail
        with self.assertRaisesRegex(RuntimeError, "ADAPTER_PROVIDER_REQUEST_RUNTIME_ERROR"):
            self.adapter()

    def test_http_transport_failure(self):
        def fail():
            self.adapter.phase = "PROVIDER_REQUEST"
            raise httpx.ConnectError("network not reachable")
        self.adapter._acquire = fail
        with self.assertRaisesRegex(RuntimeError, "FMP_PROVIDER_TRANSPORT_ERROR"):
            self.adapter()

    def test_known_source_error_preserved(self):
        def fail():
            self.adapter.phase = "TRANSFORM"
            raise RuntimeError("FMP_HTTP_402")
        self.adapter._acquire = fail
        with self.assertRaisesRegex(RuntimeError, "FMP_HTTP_402"):
            self.adapter()

    def test_universe_failure_phase(self):
        def fail():
            self.adapter.phase = "UNIVERSE"
            raise RuntimeError("Unexpected state")
        self.adapter._acquire = fail
        with self.assertRaisesRegex(RuntimeError, "ADAPTER_UNIVERSE_RUNTIME_ERROR"):
            self.adapter()


if __name__ == "__main__":
    unittest.main()
