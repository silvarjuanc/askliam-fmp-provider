"""No-network tests for persisted FMP 402 cooling-off and daily queue routing."""
import asyncio
import time
import unittest

from stage0_fmp_cooldown import (
    active_cooldowns, cooldown_expiry, COOLDOWN_SECONDS, FMP_402_REASON
)
from stage0_daily import plan_from_universe
from stage0_batch import Stage0BatchCoordinator


class CooldownParsingTests(unittest.TestCase):
    def test_expiry_is_seven_days(self):
        self.assertEqual(cooldown_expiry(now_epoch=1000), 1000 + 7 * 86400)
        self.assertEqual(COOLDOWN_SECONDS, 604800)

    def test_upstash_list_and_expired_entries(self):
        data = ["AAOI", "10000", "AMD", "1000"]
        self.assertEqual(set(active_cooldowns(data, now_epoch=9999)), {"AAOI"})

    def test_malformed_expiry_fails_closed(self):
        with self.assertRaises(ValueError):
            active_cooldowns(["AAOI", "bad"], now_epoch=1)

    def test_already_expired_cooldown_not_active(self):
        self.assertEqual(active_cooldowns({"TER": "100"}, now_epoch=100), {})

    def test_2026_next_day_queue_ignores_only_http402_cooldown(self):
        entries = [
            {"symbol":"AAOI", "route":"FMP_CANDIDATE"},
            {"symbol":"AMD", "route":"FMP_CANDIDATE"},
            {"symbol":"BE", "route":"FMP_CANDIDATE"},
            {"symbol":"BTC", "route":"CRYPTO_PROVIDER_PENDING"},
        ]
        status={
            "utc_date":"2026-10-13", "checkpoint":{"completed_symbols":[]},
            "deferred_today":{},
            "fmp_quote_402_cooldowns":{"AAOI":"2026-10-19T13:00:00+00:00"},
            "quota":{"remaining":240},
            "pending_reconciliation":False,
        }
        plan=plan_from_universe(entries,status)
        self.assertEqual(plan["next_symbols"], ["AMD"])
        self.assertEqual(plan["fmp_unattempted_today_including_cooldowns"], 3)
        self.assertEqual(plan["fmp_http402_skipped_today"], 1)
        self.assertEqual(plan["fmp_remaining_today"], 2)
        self.assertEqual(plan["unsupported_today"],1)


class FakeState:
    def __init__(self):
        self.calls=[]
    async def call(self, *args):
        self.calls.append(args)
        if args[0]=="HGETALL" and ":deferred:" in args[1]:
            return ["AAOI",FMP_402_REASON,"ASML","fmp: FMP_ACQUISITION_INCOMPLETE",
                    "BIL",FMP_402_REASON]
        raise AssertionError(args)
    async def eval(self,script, keys,args):
        self.calls.append(("EVAL",keys,args))
        assert args[2]==FMP_402_REASON
        return 1


class SeedTests(unittest.TestCase):
    def test_seed_only_confirmed_today_402(self):
        state=FakeState()
        batch=Stage0BatchCoordinator(state)
        result=asyncio.run(batch.seed_402_from_today("test-lease"))
        self.assertEqual(result["eligible_today"], 2)
        self.assertEqual(result["new_cooldowns"], 2)
        self.assertEqual(result["already_active"], 0)
        migrated=[call[2][1] for call in state.calls if call[0]=="EVAL"]
        self.assertEqual(migrated,["AAOI","BIL"])
        self.assertNotIn("ASML",migrated)


if __name__=="__main__":
    unittest.main()
