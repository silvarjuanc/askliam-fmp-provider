"""Regression: a Render restart must not be a Stage0 ingestion trigger.

No networking, Sheets, Upstash or trading calls.
"""
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import app as server


class StartupGuards(unittest.IsolatedAsyncioTestCase):
    async def test_date_flags_alone_cannot_start_maintenance_jobs(self):
        day = datetime.now(timezone.utc).date().isoformat()
        flags = {
            "ASKLIAM_STARTUP_ONESHOTS_ENABLED": "false",
            "ASKLIAM_COINBASE_SPOT_RECOVER_DATE": day,
            "ASKLIAM_COINBASE_SPOT_STARTUP_ONCE_DATE": day,
            "ASKLIAM_RUNTIME_EVIDENCE_ONCE_DAY": day,
            "ASKLIAM_FMP_PRECLOSE_PROBE_ONCE_DAY": day,
            "ASKLIAM_FMP_EQUITY_BOOTSTRAP_ONCE_DAY": day,
            "ASKLIAM_FMP_PRECLOSE_DRAIN_ONCE_DAY": day,
            "ASKLIAM_STAGE0_JOURNAL_INSPECT_ONCE_DAY": day,
            "ASKLIAM_STAGE0_GE_PREWRITE_RECOVER_ONCE_DAY": day,
            "ASKLIAM_FMP_JPM_POSTWRITE_RECOVER_UTC_DAY": day,
        }
        with patch.dict(os.environ, flags), \
             patch.object(server.provider, "start", new_callable=AsyncMock) as started, \
             patch.object(server.provider, "stop", new_callable=AsyncMock) as stopped, \
             patch.object(server.asyncio, "create_task") as spawned:
            async with server.app.router.lifespan_context(server.app):
                self.assertEqual(spawned.call_count, 0)
            started.assert_awaited_once()
            stopped.assert_awaited_once()

    async def test_default_without_override_is_disabled(self):
        day = datetime.now(timezone.utc).date().isoformat()
        with patch.dict(os.environ, {"ASKLIAM_FMP_PRECLOSE_DRAIN_ONCE_DAY": day}, clear=True), \
             patch.object(server.provider, "start", new_callable=AsyncMock), \
             patch.object(server.provider, "stop", new_callable=AsyncMock), \
             patch.object(server.asyncio, "create_task") as spawned:
            async with server.app.router.lifespan_context(server.app):
                self.assertEqual(spawned.call_count, 0)


if __name__ == "__main__":
    unittest.main()
