import unittest
from datetime import datetime,timezone
from unittest.mock import AsyncMock,Mock,patch
from stage0_crypto_coinbase_commit import (spot_automation_guard,confirm_spot,spot_commit_one)
from stage0_ingestor import Stage0Error
from scripts.stage0_daily_scheduler import run,VERSION,CRYPTO,SchedulerHalt

UTC=timezone.utc
AT=datetime(2026,10,15,10,0,tzinfo=UTC)
ENABLE={
 "ASKLIAM_COINBASE_SPOT_AUTO_ENABLED":"true",
 "ASKLIAM_STAGE0_AUTO_ENABLED":"true",
 "ASKLIAM_CRYPTO_AUTO_ENABLED":"true",
 "ASKLIAM_CRYPTO_WRITE_ENABLED":"false",
 "ASKLIAM_COINBASE_SPOT_WRITE_ENABLED":"false",
 "ASKLIAM_SPREADSHEET_ID":"mocked",
 "GOOGLE_SERVICE_ACCOUNT_JSON":"mocked",
}
class CryptoAutoSpotTests(unittest.TestCase):
    def test_auto_opt_in_requires_specific_gate(self):
        with patch.dict("os.environ",{},clear=True):
            with self.assertRaisesRegex(Stage0Error,"SPOT_AUTO_DISABLED"):
                spot_automation_guard("BTC",confirm_spot("BTC"))
        with patch.dict("os.environ",ENABLE,clear=True):
            spot_automation_guard("BTC",confirm_spot("BTC"))
            with self.assertRaisesRegex(Stage0Error,"EXPLICIT_CONFIRMATION_REQUIRED"):
                spot_automation_guard("ADA",confirm_spot("ADA"))
        conflict={**ENABLE,"ASKLIAM_COINBASE_SPOT_WRITE_ENABLED":"true"}
        with patch.dict("os.environ",conflict,clear=True):
            with self.assertRaisesRegex(Stage0Error,"MANUAL_CRYPTO_WRITE_STILL_ENABLED"):
                spot_automation_guard("BTC",confirm_spot("BTC"))

    def test_spot_before_fmp_and_no_duplicate_perp(self):
        completed=set()
        ordered=[]
        def fake(client,method,path,token,*,json_body=None):
            ordered.append((method,path))
            if path=="/stage0/auto/config":
                return {"version":VERSION,"stage0_auto_enabled":True,
                        "crypto_auto_enabled":True,"fmp_stage0_enabled":True,
                        "coinbase_spot_auto_enabled":True,
                        "redis_configured":True,"sheets_configured":True,
                        "manual_crypto_write_enabled":False}
            if path=="/stage0/batch/status":
                return {"version":VERSION,"utc_date":"2026-10-15",
                        "pending_reconciliation":False,
                        "quota":{"remaining":40,"blocked":False},
                        "checkpoint":{"completed_symbols":[]}}
            if path.startswith("/stage0/crypto/coinbase-spot-checkpoint?"):
                symbol=path.split("symbol=")[1]
                return {"version":VERSION,"utc_date":"2026-10-15",
                        "symbol_checkpoint_verified":symbol in completed,
                        "last_verified_run_id":"STAGE0-20261015-V6.5.1" if symbol in completed else None,
                        "pending_reconciliation":False}
            if path.startswith("/stage0/crypto/checkpoint"):
                return {"version":VERSION,"utc_date":"2026-10-15",
                        "completed_symbols":[],"spot_completed_symbols":sorted(completed),
                        "pending_reconciliation":False}
            if path=="/stage0/auto/coinbase-spot-step":
                sym=json_body["symbol"]
                completed.add(sym)
                return {"version":VERSION,"symbol":sym,"status":"COMMITTED_VERIFY_PASS",
                        "run_id":"STAGE0-20261015-V6.5.1",
                        "spot_checkpoint_updated":True,"mandatory_fields_present":9}
            if path=="/stage0/auto/crypto-step":
                self.assertNotIn(json_body["symbol"],("BTC","ETH"))
                raise SchedulerHalt("PROVIDER_DEFERRED")
            if path=="/stage0/auto/runtime-evidence-sync":
                return {"version":VERSION,"sync_status":"COMMITTED_VERIFY_PASS",
                        "spot_verified":sorted(completed)}
            raise AssertionError(path)
        with patch("scripts.stage0_daily_scheduler.request",new=fake):
            out=run(object(),"token",now=AT,sleep=lambda _:None)
        self.assertEqual(out["status"],"STAGE0_VERIFIED_OR_PROVIDER_DEFERRED")
        self.assertEqual(set(out["crypto_spot_verified"]),{"BTC","ETH"})
        self.assertEqual(len([x for x in ordered if x[1]=="/stage0/auto/crypto-step"]),6)
        self.assertFalse(out["ranking_permitted"])

if __name__=="__main__":unittest.main()
