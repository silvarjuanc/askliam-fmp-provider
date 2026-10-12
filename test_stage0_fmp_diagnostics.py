"""No-network tests for safe FMP diagnostic classification."""
import unittest
from stage0_fmp_adapter import provider_failure_code

class ProviderDiagnosticsTests(unittest.TestCase):
    def test_http_402(self):
        self.assertEqual(provider_failure_code(["FMP_HTTP_402"]), "FMP_HTTP_402")
    def test_http_429(self):
        self.assertEqual(provider_failure_code(["FMP_HTTP_429"]), "FMP_HTTP_429")
    def test_missing_symbol(self):
        self.assertEqual(provider_failure_code(["FMP_BATCH_SYMBOL_MISSING"]), "FMP_SYMBOL_NOT_FOUND")
    def test_never_expose_provider_details(self):
        self.assertEqual(provider_failure_code(["token=confidential"]), "FMP_PROVIDER_ERROR")
    def test_mixed_errors(self):
        self.assertEqual(provider_failure_code(["FMP_HTTP_402","FMP_HTTP_503"]), "FMP_ACQUISITION_INCOMPLETE")

if __name__ == "__main__":
    unittest.main()
