"""Test server-only unattended crypto writer guard without network."""
import os
import unittest
from unittest.mock import patch

from stage0_crypto_commit import automation_guard, confirmation_for, feature_guard
from stage0_ingestor import Stage0Error


class AutomationGuardTests(unittest.TestCase):
    def setUp(self):
        self.auto_env = {
            "ASKLIAM_STAGE0_AUTO_ENABLED": "true",
            "ASKLIAM_CRYPTO_AUTO_ENABLED": "true",
            "ASKLIAM_CRYPTO_WRITE_ENABLED": "false",
            "GOOGLE_SERVICE_ACCOUNT_JSON": "synthetic-test-value",
            "ASKLIAM_SPREADSHEET_ID": "synthetic-test-sheet",
        }

    def test_auto_never_inherits_manual_write_toggle(self):
        env = dict(self.auto_env, ASKLIAM_STAGE0_AUTO_ENABLED="false")
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(Stage0Error, "CRYPTO_AUTO_DISABLED"):
                automation_guard("SUI", confirmation_for("SUI"))
            with self.assertRaisesRegex(Stage0Error, "CRYPTO_WRITE_DISABLED"):
                feature_guard("SUI", confirmation_for("SUI"))

    def test_auto_requires_both_flags_and_sheets(self):
        with patch.dict(os.environ, dict(self.auto_env, ASKLIAM_CRYPTO_AUTO_ENABLED="false"), clear=True):
            with self.assertRaisesRegex(Stage0Error, "CRYPTO_AUTO_DISABLED"):
                automation_guard("SOL", confirmation_for("SOL"))
        with patch.dict(os.environ, dict(self.auto_env, GOOGLE_SERVICE_ACCOUNT_JSON=""), clear=True):
            with self.assertRaisesRegex(Stage0Error, "CONFIGURATION_FAILED"):
                automation_guard("SOL", confirmation_for("SOL"))

    def test_active_manual_scope_blocks_automation(self):
        with patch.dict(os.environ, dict(self.auto_env, ASKLIAM_CRYPTO_WRITE_ENABLED="true"), clear=True):
            with self.assertRaisesRegex(Stage0Error, "MANUAL_CRYPTO_WRITE_STILL_ENABLED"):
                automation_guard("SOL", confirmation_for("SOL"))

    def test_successful_auto_scope_cannot_be_used_for_manual_path(self):
        with patch.dict(os.environ, self.auto_env, clear=True):
            automation_guard("ADA", confirmation_for("ADA"))
            with self.assertRaisesRegex(Stage0Error, "CRYPTO_WRITE_DISABLED"):
                feature_guard("ADA", confirmation_for("ADA"))

    def test_noncanonical_confirmation_rejected(self):
        with patch.dict(os.environ, self.auto_env, clear=True):
            with self.assertRaisesRegex(Stage0Error, "EXPLICIT_CONFIRMATION_REQUIRED"):
                automation_guard("ADA", confirmation_for("BTC"))


if __name__ == "__main__":
    unittest.main()
