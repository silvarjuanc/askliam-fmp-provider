import os
import unittest
from datetime import datetime,timezone
from unittest.mock import patch
from stage0_stockholm_orchestrator import accepted_wake,attempt_key,result_key

NOW=datetime(2026,10,15,7,30,tzinfo=timezone.utc)
PHASE="2026-10-15:09"
ENVS={"ASKLIAM_AUTONOMOUS_STAGE0_ENABLED":"true","ASKLIAM_STAGE0_ENABLED":"true",
      "ASKLIAM_STAGE0_AUTO_ENABLED":"true","ASKLIAM_CRYPTO_AUTO_ENABLED":"true",
      "ASKLIAM_CRYPTO_WRITE_ENABLED":"false","ASKLIAM_SERVICE_TOKEN":"fixture",
      "GOOGLE_SERVICE_ACCOUNT_JSON":"fixture","ASKLIAM_SPREADSHEET_ID":"fixture"}

class MockRedis:
    def __init__(self,reason="FMP_STEP_HALTED_HTTP_502"):
        self.claimed=False
        self.reason=reason
        self.retried=False
        self.result=None
    async def call(self,*args):
        if args[0]=="SET":
            if self.claimed:return None
            self.claimed=True
            return "OK"
        if args[0]=="GET":
            return self.result if args[1]==result_key(PHASE) else None
        raise AssertionError("unexpected command")
    async def eval(self,script,keys,args):
        if self.retried or self.reason not in args:return 0
        self.retried=True
        self.result=None
        return 1

class PhaseRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_gateway_recovery_only(self):
        import json
        db=MockRedis()
        created=[]
        def register(coro):
            created.append(coro)
            coro.close()
        with patch.dict(os.environ,ENVS,clear=True):
            first=await accepted_wake(db,NOW,create_task=register)
            self.assertEqual(first["status"],"PHASE_ACCEPTED")
            db.result=json.dumps({"version":"V6.5.1","phase":PHASE,"status":"HALTED",
                "halt_reason":"FMP_STEP_HALTED_HTTP_502",
                "timestamp_utc":"2026-10-15T07:25:00Z"})
            second=await accepted_wake(db,NOW,create_task=register)
            third=await accepted_wake(db,NOW,create_task=register)
        self.assertEqual(second["status"],"PHASE_SAFE_RETRY_ACCEPTED")
        self.assertEqual(third["status"],"PHASE_ALREADY_CLAIMED")
        self.assertEqual(len(created),2)

    async def test_uncertain_result_never_repeats(self):
        import json
        db=MockRedis("RECONCILIATION_REQUIRED")
        db.claimed=True
        db.result=json.dumps({"version":"V6.5.1","phase":PHASE,"status":"HALTED",
            "halt_reason":"RECONCILIATION_REQUIRED","timestamp_utc":"2026-10-15T07:25:00Z"})
        with patch.dict(os.environ,ENVS,clear=True):
            r=await accepted_wake(db,NOW)
        self.assertEqual(r["status"],"PHASE_ALREADY_CLAIMED")
        self.assertFalse(db.retried)

if __name__=="__main__": unittest.main()
