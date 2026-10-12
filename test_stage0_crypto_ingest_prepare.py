"""Strict offline tests for crypto full-bar frame preparation. NO live writes."""
from datetime import datetime, timezone, timedelta
import unittest

import pandas as pd

from stage0_crypto_ingest_prepare import (
    SOURCE, SOURCE_PRICE, frames_from_native_perp, preview_native_perp_stage0
)
from stage0_crypto_history_probe import DAY_MS
from stage0_ingestor import Stage0_Daily_Ingestor, TABS

NOW = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)
RUN = "STAGE0-20261012-V6.5.1"


def daily_data(symbol="BTC", days=150, gap=None):
    today = datetime(2026, 10, 12, tzinfo=timezone.utc)
    out = []
    for i in range(days, 0, -1):
        if i == gap:
            continue
        t = int((today - timedelta(days=i)).timestamp()*1000)
        p = 100 + (days - i)
        out.append({
            "t":t,"T":t+DAY_MS-1,"s":symbol,"i":"1d",
            "o":str(p),"h":str(p+2),
            "l":str(p-2),"c":str(p+1),"v":"42.1"
        })
    return out


def ctx_data(symbol="BTC"):
    return [
        {"universe":[{"name":symbol}]},
        [{"markPx":"101","oraclePx":"102","funding":"0.00001",
          "openInterest":"500","dayNtlVlm":"10000"}],
    ]


def funding_data(symbol="BTC"):
    return [{"coin":symbol,"time":int((NOW-timedelta(hours=i)).timestamp()*1000),
             "fundingRate":"0.00001"} for i in range(1, 5)]


def prepare(symbol="BTC", bars=None, context=None, funding=None):
    return frames_from_native_perp(
        symbol, candles=daily_data(symbol) if bars is None else bars,
        context_payload=ctx_data(symbol) if context is None else context,
        funding_payload=funding_data(symbol) if funding is None else funding,
        now=NOW, run_id=RUN,
    )


class FrameConversionTests(unittest.TestCase):
    def test_150_real_daily_rows_with_unique_canonical_source_keys(self):
        frames, report = prepare()
        bars = frames["Raw_OHLCV"]
        self.assertEqual(len(bars), 150)
        self.assertEqual(report["upsert_unique_keys"], 150)
        self.assertEqual(bars.iloc[0]["trading_date"], "2026-05-15")
        self.assertEqual(bars.iloc[-1]["trading_date"], "2026-10-11")
        self.assertTrue(all(bars["source"] == SOURCE))
        self.assertEqual(bars.iloc[0]["upsert_key"],
                         "BTC|2026-05-15|HYPERLIQUID_NATIVE_PERP")
        self.assertEqual(len(report["ohlcv_sha256"]), 64)
        self.assertNotIn("FMP_Runtime_State", frames)
        self.assertFalse(report["sheet_writes_performed"])
        self.assertFalse(report["ranking_permitted"])
        self.assertFalse(report["execution_permitted"])

    def test_never_use_perpetual_as_independent_spot_or_scores(self):
        frames, report = prepare()
        row = frames["Market_Data_Snapshot"].iloc[0]
        self.assertEqual(row["analysis_price_source"], SOURCE_PRICE)
        for field in ("last_price","return_1m","return_3m","volatility_20d"):
            self.assertEqual(row[field], "")
        self.assertEqual(row["score_model"], "CRYPTO_V1")
        self.assertEqual(row["mandatory_broad_pass"], "FALSE")
        self.assertAlmostEqual(row["funding_rate"], 0.00001)
        self.assertEqual(row["open_interest"], 500)
        self.assertIn("momentum_score", row["missing_mandatory_fields"])
        self.assertFalse(report["independent_spot_acquired"])

    def test_gap_in_candles_blocks_frame_entirely(self):
        with self.assertRaisesRegex(ValueError,"NATIVE_PERP_HISTORY_DISCONTINUOUS"):
            prepare(bars=daily_data(gap=80))

    def test_mismatched_contract_blocks_frame(self):
        with self.assertRaisesRegex(ValueError,"CANDLE_INSTRUMENT_MISMATCH"):
            prepare(bars=daily_data("ETH"))

    def test_missing_funding_blocks_frame(self):
        with self.assertRaisesRegex(ValueError,"NATIVE_PERP_FUNDING_NOT_VALIDATED"):
            prepare(funding=[])

    def test_invalid_oi_blocks_frame(self):
        ctx=ctx_data()
        ctx[1][0]["openInterest"] = "-1"
        with self.assertRaisesRegex(ValueError,"NATIVE_PERP_DERIVATIVES_CONTEXT_INCOMPLETE"):
            prepare(context=ctx)

    def test_missing_source_context_blocks_frame(self):
        with self.assertRaisesRegex(ValueError,"NATIVE_PERP_NOT_LISTED"):
            prepare(context=ctx_data("ETH"))

    def test_run_id_validated(self):
        with self.assertRaisesRegex(ValueError,"INVALID_STAGE0_RUN_ID"):
            frames_from_native_perp(
                "BTC",candles=daily_data(),context_payload=ctx_data(),
                funding_payload=funding_data(),now=NOW,run_id="WRONG",
            )

    def test_staged_frames_fit_existing_sheet_keys_without_overwriting_fmp(self):
        frames, report=prepare()
        ingestor = Stage0_Daily_Ingestor(
            providers={"test":lambda:frames},spreadsheet_id="test-only"
        )
        headers_raw=[
            "ranking_run_id","canonical_symbol","provider_symbol",
            "asset_class","source","source_url_template","trading_date",
            "open","high","low","close","volume","fetched_at_utc",
            "upsert_key","quality_status","notes",
        ]
        headers_snapshot=[
            "ranking_run_id","canonical_symbol","asset_class","score_model",
            "data_timestamp","analysis_price_source","last_price",
            "return_1m","return_3m","volatility_20d","funding_rate",
            "open_interest","mandatory_broad_pass","broad_readiness",
            "missing_mandatory_fields","source_summary","enrichment_status",
            "notes",
        ]
        headers_state=["Setting","Configured Value"]
        existing_fmp={
            "canonical_symbol":"BTC","source":"FMP",
            "trading_date":"2026-10-11","close":"100",
        }
        fmp_key=("BTC","2026-10-11","FMP")
        before={
            "Raw_OHLCV":{
                "headers":headers_raw,"rows":{fmp_key:existing_fmp},
                "locations":{fmp_key:2}
            },
            "Market_Data_Snapshot":{
                "headers":headers_snapshot,"rows":{},"locations":{}
            },
            "FMP_Runtime_State":{
                "headers":headers_state,"rows":{},"locations":{}
            },
        }
        total_frames = {
            **frames,
            "FMP_Runtime_State":pd.DataFrame(),
        }
        plans, expected=ingestor._plan(before,total_frames)
        self.assertEqual(len(plans["Raw_OHLCV"]["inserts"]), 150)
        self.assertEqual(len(plans["Market_Data_Snapshot"]["inserts"]), 1)
        self.assertEqual(len(plans["FMP_Runtime_State"]["inserts"]), 0)
        self.assertEqual(expected["Raw_OHLCV"][fmp_key],existing_fmp)
        self.assertEqual(len(expected["Raw_OHLCV"]),151)
        self.assertFalse(report["checkpoint_advanced"])


class FakeResponse:
    def __init__(self, data, status=200):
        self.data,self.status_code=data,status
    def json(self):
        return self.data


class FakeClient:
    def __init__(self, *, context=200, candle=200, funding=200):
        self.calls=[]
        self.codes={"metaAndAssetCtxs":context,
                    "candleSnapshot":candle,"fundingHistory":funding}
    async def post(self,url,*,json,timeout):
        self.calls.append(json)
        typ=json["type"]
        data=(
            ctx_data() if typ=="metaAndAssetCtxs" else
            daily_data() if typ=="candleSnapshot" else funding_data()
        )
        return FakeResponse(data,self.codes[typ])


class PreviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_is_only_a_preview(self):
        client=FakeClient()
        result=await preview_native_perp_stage0("BTC",client=client,now=NOW)
        self.assertEqual(result["status"],"PREPARED_PREVIEW_NO_WRITES")
        self.assertEqual(result["prepared"]["daily_rows_prepared"],150)
        self.assertEqual(result["requests_performed"],3)
        self.assertFalse(result["sheet_writes_performed"])
        self.assertFalse(result["ranking_permitted"])
        self.assertFalse(result["checkpoint_advanced"])
        self.assertFalse(result["prepared"]["independent_spot_acquired"])

    async def test_rate_limit_stops_without_retries(self):
        client=FakeClient(candle=429)
        result=await preview_native_perp_stage0("BTC",client=client,now=NOW)
        self.assertEqual(result["status"],"SOURCE_UNAVAILABLE")
        self.assertEqual(result["reason"],"CANDLES_HTTP_429")
        self.assertEqual(result["requests_performed"],2)

    async def test_unknown_symbol_rejected_without_requests(self):
        client=FakeClient()
        with self.assertRaisesRegex(ValueError,"CRYPTO_SYMBOL_NOT_IN_CANONICAL_MAPPING"):
            await preview_native_perp_stage0("FAKE",client=client,now=NOW)
        self.assertEqual(client.calls,[])


if __name__=="__main__":
    unittest.main()
