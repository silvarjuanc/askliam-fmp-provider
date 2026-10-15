import os
import unittest
from datetime import datetime,timezone
from unittest.mock import patch
from stage0_stockholm_orchestrator import accepted_wake,phase_for,preflight
from scripts.stockholm_wake_cron import fire

ENVS={
 "ASKLIAM_AUTONOMOUS_STAGE0_ENABLED":"true",
 "ASKLIAM_STAGE0_ENABLED":"true",
 "ASKLIAM_STAGE0_AUTO_ENABLED":"true",
 "ASKLIAM_CRYPTO_AUTO_ENABLED":"true",
 "ASKLIAM_CRYPTO_WRITE_ENABLED":"false",
 "ASKLIAM_SERVICE_TOKEN":"test-token-do-not-use",
 "GOOGLE_SERVICE_ACCOUNT_JSON":"test-nonsecret-fixture",
 "ASKLIAM_SPREADSHEET_ID":"test-book",
}
SUMMER=datetime(2026,10,15,7,0,tzinfo=timezone.utc)
WINTER=datetime(2026,12,15,8,0,tzinfo=timezone.utc)


class Redis:
    def __init__(self):
        self.claims=set()
    async def call(self,*args):
        if args[0]=="GET":
            return None
        if args[0]=="SET" and "NX" in args:
            if args[1] in self.claims:
                return None
            self.claims.add(args[1])
            return "OK"
        raise ValueError("BAD_FAKE_COMMAND")


class StockholmWakeTests(unittest.IsolatedAsyncioTestCase):
    def test_seasonal_local_hours_and_outside_cutoff(self):
        self.assertEqual(phase_for(SUMMER),"2026-10-15:09")
        self.assertEqual(phase_for(WINTER),"2026-12-15:09")
        self.assertIsNone(phase_for(datetime(2026,10,15,14,5,tzinfo=timezone.utc)))
        self.assertIsNone(phase_for(datetime(2026,10,17,7,0,tzinfo=timezone.utc)))

    async def test_authorized_phase_once_only(self):
        redis=Redis()
        tasks=[]
        def cancel_background(coroutine):
            tasks.append(coroutine)
            coroutine.close()
        with patch.dict(os.environ,ENVS,clear=True):
            x=await accepted_wake(redis,SUMMER,create_task=cancel_background)
            y=await accepted_wake(redis,SUMMER,create_task=cancel_background)
        self.assertEqual(x["status"],"PHASE_ACCEPTED")
        self.assertEqual(y["status"],"PHASE_ALREADY_CLAIMED")
        self.assertEqual(len(tasks),1)
        self.assertFalse(x["execution_permitted"])

    async def test_conflicting_manual_writer_blocks_claim(self):
        env={**ENVS,"ASKLIAM_CRYPTO_WRITE_ENABLED":"true"}
        with patch.dict(os.environ,env,clear=True):
            x=await accepted_wake(Redis(),SUMMER)
        self.assertEqual(x["status"],"MANUAL_CRYPTO_WRITER_CONFLICT")
        self.assertFalse(x["accepted"])

    async def test_disarm_and_missing_credentials(self):
        with patch.dict(os.environ,{},clear=True):
            self.assertEqual(preflight(SUMMER),"STOCKHOLM_ORCHESTRATOR_DISABLED")
        env={**ENVS}
        del env["ASKLIAM_SERVICE_TOKEN"]
        with patch.dict(os.environ,env,clear=True):
            self.assertEqual(preflight(SUMMER),"SERVICE_TOKEN_NOT_CONFIGURED")

    def test_wake_client_cannot_target_another_host(self):
        out=fire("https://example.com/wake","safe-token")
        self.assertEqual(out["status"],"UNTRUSTED_WAKE_DESTINATION")

if __name__=="__main__":
    unittest.main()
