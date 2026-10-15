import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from stage0_jpm_postwrite_reconcile import (
    inspect_jpm_saved, reconcile_jpm_postwrite, RUN, DAY, SYMBOL,
)
from stage0_ingestor import Stage0Error

UTC=timezone.utc
# Historical reconciliation fixture is pinned to 2026-10-14; never let
# the wall clock of a future CI run invalidate the fixture itself.
class PinnedUtcDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is not None else NOW.replace(tzinfo=None)

CREATED=datetime(2026,10,14,12,42,53,tzinfo=UTC)
NOW=datetime(2026,10,14,13,9,tzinfo=UTC)
CANDLES=[
    {"date":"2026-10-12","open":334.3,"high":336.34,"low":333.55,"close":334.3,"volume":5680400},
    {"date":"2026-10-13","open":339.97,"high":344.36,"low":331.17,"close":331.43,"volume":13758631},
]

def sheets():
    rows={}
    for n in range(300):
        d=(datetime(2025,8,1,tzinfo=UTC)+timedelta(days=n)).date().isoformat()
        rows[(SYMBOL,d,"FMP")]=dict(canonical_symbol=SYMBOL,source="FMP",
            ranking_run_id="STAGE0-20261012-V6.5.1",
            trading_date=d,fetched_at_utc="2026-10-12T13:20:30+00:00")
    for bar in CANDLES:
        d=bar["date"]
        rows[(SYMBOL,d,"FMP")]={**{k:str(v) for k,v in bar.items() if k!="date"},
            "canonical_symbol":SYMBOL,"provider_symbol":SYMBOL,
            "ranking_run_id":RUN,"trading_date":d,"source":"FMP",
            "upsert_key":f"JPM|{d}|FMP",
            "source_url_template":"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=JPM",
            "quality_status":"RECEIVED_UNVERIFIED",
            "fetched_at_utc":"2026-10-14T12:43:01+00:00"}
    snap={
        "asset_class":"Equity","score_model":"EQUITY_V2",
        "analysis_price_source":"FMP_STABLE",
        "mandatory_broad_pass":"FALSE",
        "enrichment_status":"ENRICHMENT_PENDING",
        "broad_readiness":"ENRICHMENT_PENDING",
        "data_timestamp":"2026-10-14T12:43:02+00:00",
        "last_price":"331.43",
        "source_summary":(
            "https://financialmodelingprep.com/stable/quote?symbol=JPM;"
            "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=JPM"
        ),
    }
    return {
        "Raw_OHLCV":{"rows":rows},
        "Market_Data_Snapshot":{"rows":{(RUN,SYMBOL):snap}},
        "FMP_Runtime_State":{"rows":{("Stage 0 Last Run ID",):{"Configured Value":RUN}}},
    }

class FakeProvider:
    async def get(self,kind,symbol):
        assert kind=="historical" and symbol==SYMBOL
        return {"status":"READY","data":CANDLES}

class FakeRedis:
    def __init__(self):
        self.raw=json.dumps(dict(status="PREPARED",symbols=[SYMBOL],run_id=RUN,
                                 created_at=CREATED.isoformat()))
        self.cp=json.dumps(dict(version="V6.5.1",utc_date=DAY,
                      status="COMMITTED_VERIFY_PASS",completed_symbols=["INTC"]))
        self.calls=[]
    async def call(self,*a):
        self.calls.append(a[0])
        assert a[0]=="GET"
        if a[1].endswith(":pending"):return self.raw
        if a[1].endswith(":checkpoint:"+DAY):return self.cp
        raise AssertionError(a)
    async def eval(self,script,keys,args):
        self.calls.append("EVAL")
        assert args[:3]==["lease",self.raw,self.cp]
        self.cp=args[3]
        self.raw=None
        return 1

class CoordinatorStub:
    def __init__(self,r):self.r=r
    async def acquire(self):return "lease"
    async def release(self,t):self.r.calls.append("RELEASE")

class TestJpmPostwrite(unittest.TestCase):
    def test_pure_independent_complete_replay(self):
        result=inspect_jpm_saved(sheets(),CREATED,CANDLES)
        self.assertEqual(result["jpm_rows"],302)
        self.assertEqual(result["new_dates"],["2026-10-12","2026-10-13"])
        self.assertTrue(result["fmp_replay_verified"])

    def test_provider_candle_mismatch_blocks(self):
        candles=[dict(x) for x in CANDLES]
        candles[0]["close"]=313
        with self.assertRaises(Stage0Error):
            inspect_jpm_saved(sheets(),CREATED,candles)

    def test_missing_snapshot_blocks(self):
        s=sheets();s["Market_Data_Snapshot"]["rows"].clear()
        with self.assertRaises(Stage0Error):
            inspect_jpm_saved(s,CREATED,CANDLES)

    def test_legacy_row_mutation_blocks(self):
        s=sheets();key=next(iter(s["Raw_OHLCV"]["rows"]))
        s["Raw_OHLCV"]["rows"][key]["fetched_at_utc"]=CREATED.isoformat()
        with self.assertRaises(Stage0Error):
            inspect_jpm_saved(s,CREATED,CANDLES)

    def test_missing_new_candle_blocks(self):
        s=sheets();s["Raw_OHLCV"]["rows"].pop(("JPM","2026-10-13","FMP"))
        with self.assertRaises(Stage0Error):
            inspect_jpm_saved(s,CREATED,CANDLES)

    def test_success_advances_checkpoint_only_after_replay_and_readback(self):
        redis=FakeRedis()
        with (patch("stage0_jpm_postwrite_reconcile.Stage0BatchCoordinator",CoordinatorStub),
              patch("stage0_jpm_postwrite_reconcile.datetime",PinnedUtcDatetime)):
            result=asyncio.run(reconcile_jpm_postwrite(
                redis=redis,provider=FakeProvider(),now=NOW,sheet_reader=sheets))
        self.assertEqual(result["status"],"COMMITTED_VERIFY_PASS")
        self.assertTrue(result["pending_journal_cleared"])
        self.assertFalse(result["sheet_writes_performed"])
        self.assertEqual(redis.calls.count("EVAL"),1)
        self.assertIsNone(redis.raw)
        self.assertEqual(json.loads(redis.cp)["completed_symbols"],["INTC","JPM"])

    def test_invalid_journal_never_advances(self):
        redis=FakeRedis()
        redis.raw=json.dumps(dict(status="PREPARED",symbols=["GE"],run_id=RUN,
                                 created_at=CREATED.isoformat()))
        with patch("stage0_jpm_postwrite_reconcile.Stage0BatchCoordinator",CoordinatorStub):
            with self.assertRaises(Stage0Error):
                asyncio.run(reconcile_jpm_postwrite(redis=redis,provider=FakeProvider(),
                                                    now=NOW,sheet_reader=sheets))
        self.assertNotIn("EVAL",redis.calls)

    def test_bad_checkpoint_never_advances(self):
        redis=FakeRedis()
        redis.cp=json.dumps(dict(version="V6.5.1",utc_date=DAY,
             status="COMMITTED_VERIFY_PASS",completed_symbols=["JPM"]))
        with patch("stage0_jpm_postwrite_reconcile.Stage0BatchCoordinator",CoordinatorStub):
            with self.assertRaises(Stage0Error):
                asyncio.run(reconcile_jpm_postwrite(redis=redis,provider=FakeProvider(),
                                                    now=NOW,sheet_reader=sheets))
        self.assertNotIn("EVAL",redis.calls)

    def test_recent_lease_blocks(self):
        redis=FakeRedis()
        with patch("stage0_jpm_postwrite_reconcile.Stage0BatchCoordinator",CoordinatorStub):
            with self.assertRaises(Stage0Error):
                asyncio.run(reconcile_jpm_postwrite(redis=redis,provider=FakeProvider(),
                      now=CREATED+timedelta(minutes=4),sheet_reader=sheets))
        self.assertNotIn("EVAL",redis.calls)

if __name__=="__main__":unittest.main()
