"""Non-network unit tests for Stage0 durable cursor rules."""
import unittest
from stage0_batch import safe_cursor, state_key, pending_key, MAX_BATCH, MIN_QUOTA_RESERVE

class Stage0JournalTests(unittest.TestCase):
    def test_completed_union_deduplicated(self):
        self.assertEqual(safe_cursor({"completed_symbols": ["MSFT", "AAPL"]}, ["AAPL", "NVDA"]),
                         ["AAPL", "MSFT", "NVDA"])

    def test_checkpoint_namespaced_by_day(self):
        self.assertNotEqual(state_key("2026-10-12"), state_key("2026-10-13"))

    def test_pending_global(self):
        self.assertEqual(pending_key("2026-10-12"), pending_key("2026-10-13"))

    def test_guardrails(self):
        self.assertEqual(MAX_BATCH, 5)
        self.assertGreaterEqual(MIN_QUOTA_RESERVE, 40)

if __name__ == "__main__":
    unittest.main()
