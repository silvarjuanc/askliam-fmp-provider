"""Guarded spot writer tests: no FMP/perp clobber and atomic Redis guards."""
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
import pandas as pd

from stage0_crypto_spot import make_spot_frames
from stage0_crypto_spot_commit import (
    GuardedSpotIngestor, spot_automation_guard, commit_coingecko_spot_one,
    spot_checkpoint_key,
)
from stage0_crypto_commit import crypto_checkpoint_key
from stage0_batch import pending_key
from stage0_ingestor import Stage0Error
from test_stage0_crypto_sheet_preflight import captured_state
from test_stage0_crypto_spot import fixtures, NOW, RUN


def valid_env():
    return {
        "ASKLIAM_STAGE0_AUTO_ENABLED":"true",
        "ASKLIAM_CRYPTO_AUTO_ENABLED":"true",
        "ASKLIAM_CRYPTO_SPOT_ENABLED":"true",
        "ASKLIAM_CRYPTO_WRITE_ENABLED":"false",
        "GOOGLE_SERVICE_ACCOUNT_JSON":"unit-test-only",
        "ASKLIAM_SPREADSHEET_ID":"unit-test-only",
    }


def spot_frames():
    chart,quotes,_=fixtures()
    return make_spot_frames("BTC",chart=chart,quotes=quotes,
                            now=NOW,run_id=RUN)[0]


def good_state():
    state=captured_state(fmp=True,derivative=True)
    old_key=list(state["Market_Data_Snapshot"]["rows"])[0]
    record=state["Market_Data_Snapshot"]["rows"].pop(old_key)
    record.update({
        "ranking_run_id":RUN,
        "score_model":"CRYPTO_V1",
        "data_timestamp":"2026-10-13T20:24:00Z",
        "funding_rate":"0.00001",
        "open_interest":"2000",
        "source_summary":"https://api.hyperliquid.xyz/info",
    })
    state["Market_Data_Snapshot"]["rows"][(RUN,"BTC")]=record
    state["Market_Data_Snapshot"]["locations"]={(RUN,"BTC"):2}
    return state


class SpotWriterGuardsTests(unittest.TestCase):
    def test_no_write_without_opt_in(self):
        with patch.dict(os.environ,dict(valid_env(),ASKLIAM_CRYPTO_SPOT_ENABLED="false"),clear=True):
            with self.assertRaisesRegex(Stage0Error,"SPOT_WRITES_DISABLED"):
                spot_automation_guard("BTC")
        with patch.dict(os.environ,valid_env(),clear=True):
            spot_automation_guard("BTC")

    def test_planner_does_not_touch_perp_or_fmp_state(self):
        writer=GuardedSpotIngestor(spot_frames(),"BTC",RUN,
                                 observed_at=NOW,spreadsheet_id="fake")
        frames={**spot_frames(),"FMP_Runtime_State":pd.DataFrame()}
        plans,expected=writer._plan(good_state(),frames)
        self.assertEqual(len(plans["Raw_OHLCV"]["inserts"]),300)
        self.assertEqual(len(plans["Raw_OHLCV"]["updates"]),0)
        self.assertEqual(len(plans["Market_Data_Snapshot"]["updates"]),1)
        self.assertEqual(len(plans["FMP_Runtime_State"]["updates"]),0)
        self.assertEqual(len(plans["FMP_Runtime_State"]["inserts"]),0)
        self.assertIn(("BTC","2026-10-11","FMP"),expected["Raw_OHLCV"])
        self.assertEqual(expected["Market_Data_Snapshot"][(RUN,"BTC")]["mandatory_broad_pass"],"TRUE")

    def test_planner_blocks_foreign_spot_snapshot_collision(self):
        state=good_state()
        state["Market_Data_Snapshot"]["rows"][(RUN,"BTC")]["analysis_price_source"]="FMP_STABLE"
        writer=GuardedSpotIngestor(spot_frames(),"BTC",RUN,
                                  observed_at=NOW,spreadsheet_id="fake")
        with self.assertRaisesRegex(Stage0Error,"SPOT_SNAPSHOT_COLLISION"):
            writer._plan(state,{**spot_frames(),"FMP_Runtime_State":pd.DataFrame()})


class FakeRedis:
    def __init__(self,prepared):
        self.values=dict(prepared)
        self.writes=[]
    async def call(self,*args):
        op=args[0]
        if op=="SET" and args[-1]=="NX":
            if args[1] in self.values:
                return None
            self.values[args[1]]=args[2]
            self.writes.append(args)
            return "OK"
        if op=="GET":
            return self.values.get(args[1])
        raise AssertionError("Unexpected Redis call "+repr(args))
    async def eval(self,script,keys,args):
        self.values[keys[2]]=args[-1]
        self.values.pop(keys[1],None)
        return 1


class SpotCommitSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_daily_spot_checkpoint_blocks_before_network(self):
        import json
        day="2026-10-13"
        data={
            crypto_checkpoint_key(day):json.dumps({
                "utc_date":day,"status":"COMMITTED_VERIFY_PASS",
                "completed_symbols":["BTC"]
            }),
            spot_checkpoint_key(day):json.dumps({
                "version":"V6.5.1","utc_date":day,
                "completed_symbols":["BTC"],"provider":"COINGECKO_SPOT_USD_DAILY_POINT"
            })
        }
        redis=FakeRedis(data)
        class Coordinator:
            async def acquire(self):return "fake-token"
            async def release(self,token):return None
        with patch.dict(os.environ,valid_env(),clear=True):
            with patch("stage0_crypto_spot_commit.Stage0BatchCoordinator",return_value=Coordinator()):
                with patch("stage0_crypto_spot_commit.acquire_coingecko_spot",side_effect=AssertionError("NETWORK")):
                    with self.assertRaisesRegex(Stage0Error,"ALREADY_COMMITTED_TODAY"):
                        await commit_coingecko_spot_one("BTC",redis=redis,client=object(),now=NOW)
        self.assertEqual(redis.writes,[])
    async def test_unverified_writer_leaves_durable_reconciliation_journal(self):
        import json
        day="2026-10-13"
        redis=FakeRedis({
            crypto_checkpoint_key(day):json.dumps({
                "utc_date":day,"status":"COMMITTED_VERIFY_PASS",
                "completed_symbols":["BTC"]
            })
        })
        class Coordinator:
            async def acquire(self):return "fake-token"
            async def release(self,token):return None
        frames=spot_frames()
        prepared=(frames,{"sha256":"f"*64,"data_points":300})
        with patch.dict(os.environ,valid_env(),clear=True):
            with patch("stage0_crypto_spot_commit.Stage0BatchCoordinator",return_value=Coordinator()):
                with patch("stage0_crypto_spot_commit.acquire_coingecko_spot",return_value=prepared):
                    def failed_writer(*args):
                        raise RuntimeError("MOCK_SHEETS_PARTIAL")
                    with patch("stage0_crypto_spot_commit.preflight_spot_frames_sync",return_value={}):
                        with self.assertRaisesRegex(RuntimeError,"MOCK_SHEETS_PARTIAL"):
                            await commit_coingecko_spot_one(
                                "BTC",redis=redis,client=object(),now=NOW,writer=failed_writer,
                            )
        self.assertIn(pending_key(day),redis.values)
        self.assertNotIn(spot_checkpoint_key(day),redis.values)


if __name__ == "__main__":
    unittest.main()
