"""Offline, deterministic, no-write production-sheet dry run tests."""
import unittest

from stage0_crypto_ingest_prepare import SOURCE, SOURCE_PRICE
from stage0_crypto_sheet_preflight import compare_prepared_crypto_frames
from stage0_ingestor import Stage0Error
from test_stage0_crypto_ingest_prepare import prepare, RUN

RAW_HEADERS = [
    "ranking_run_id","canonical_symbol","provider_symbol","asset_class",
    "source","source_url_template","trading_date","open","high","low",
    "close","volume","fetched_at_utc","upsert_key","quality_status","notes",
]
SNAP_HEADERS = [
    "ranking_run_id","canonical_symbol","asset_class","score_model",
    "data_timestamp","analysis_price_source","last_price","return_1d",
    "return_5d","return_1m","return_3m","return_6m","return_12m",
    "volatility_20d","volatility_60d","drawdown_52w","distance_20dma",
    "distance_50dma","distance_200dma","momentum_score","trend_score",
    "relative_strength_score","volume_score","macro_score","fundamental_score",
    "breadth_score","rate_diff_score","curve_carry_score","supply_demand_score",
    "funding_rate","open_interest","oi_change","liquidity_proxy_score",
    "catalyst_score","data_completeness_pct","mandatory_broad_pass",
    "broad_readiness","missing_mandatory_fields","missing_optional_fields",
    "source_summary","enrichment_status","notes",
]
STATE_HEADERS = ["Setting","Configured Value","Runtime State","Secret?","Version","Notes"]


def captured_state(*, fmp=False, derivative=False, collision=False):
    raw = {}
    snapshot = {}
    if fmp:
        key=("BTC","2026-10-11","FMP")
        raw[key]={"canonical_symbol":"BTC","trading_date":"2026-10-11",
                  "source":"FMP","close":"123","ranking_run_id":"OTHER"}
    if derivative:
        rawkey=("BTC","2026-10-11",SOURCE)
        raw[rawkey]={"canonical_symbol":"BTC","trading_date":"2026-10-11",
                     "source":SOURCE,"close":"250","ranking_run_id":RUN}
    if collision or derivative:
        key=(RUN,"BTC")
        snapshot[key] = {
            "ranking_run_id":RUN,"canonical_symbol":"BTC",
            "analysis_price_source":("FMP" if collision else SOURCE_PRICE),
            "enrichment_status":("ACQUIRED_PERPETUAL_NOT_SPOT"
                                 if derivative else "ACQUIRED_SPOT"),
            "last_price":("" if derivative else "140"),
        }
    def tab(headers, rows):
        return {"headers":headers,"rows":rows,
                "locations":{key:i+2 for i,key in enumerate(rows)}}
    return {
        "Raw_OHLCV":tab(RAW_HEADERS,raw),
        "Market_Data_Snapshot":tab(SNAP_HEADERS,snapshot),
        "FMP_Runtime_State":tab(STATE_HEADERS,{}),
    }


class SheetDryRunTests(unittest.TestCase):
    def test_new_crypto_history_is_150_inserts_snapshot_one(self):
        frames, report = prepare()
        result=compare_prepared_crypto_frames(
            frames,symbol="BTC",run_id=RUN,captured=captured_state(fmp=True)
        )
        self.assertEqual(result["status"],"SHEET_DRY_RUN_PREPARED")
        self.assertEqual(result["plans"]["Raw_OHLCV"]["inserts"],150)
        self.assertEqual(result["plans"]["Market_Data_Snapshot"]["inserts"],1)
        self.assertEqual(result["plans"]["Raw_OHLCV"]["existing_rows_before"],1)
        self.assertEqual(result["plans"]["FMP_Runtime_State"]["updates"],0)
        self.assertFalse(result["sheet_writes_performed"])
        self.assertFalse(result["ranking_permitted"])
        self.assertEqual(len(result["state_before_sha256"]),64)

    def test_block_foreign_spot_snapshot_same_run_id(self):
        frames, report=prepare()
        result=compare_prepared_crypto_frames(
            frames,symbol="BTC",run_id=RUN,
            captured=captured_state(collision=True)
        )
        self.assertEqual(result["status"],"BLOCKED_EXISTING_NONPERP_SNAPSHOT")
        self.assertTrue(result["snapshot_collision"])
        self.assertFalse(result["sheet_writes_performed"])

    def test_same_source_previous_rows_would_update_not_duplicate(self):
        frames, report=prepare()
        result=compare_prepared_crypto_frames(
            frames,symbol="BTC",run_id=RUN,
            captured=captured_state(derivative=True)
        )
        self.assertEqual(result["status"],"SHEET_DRY_RUN_PREPARED")
        self.assertEqual(result["plans"]["Raw_OHLCV"]["inserts"],149)
        self.assertEqual(result["plans"]["Raw_OHLCV"]["updates"],1)
        self.assertEqual(result["plans"]["Market_Data_Snapshot"]["inserts"],0)

    def test_refuse_fake_spot_in_derivative_preview(self):
        frames, report=prepare()
        frames["Market_Data_Snapshot"].loc[0,"last_price"] = 300
        with self.assertRaisesRegex(Stage0Error,"Crypto/perpetual"):
            compare_prepared_crypto_frames(
                frames,symbol="BTC",run_id=RUN,captured=captured_state()
            )

    def test_refuse_raw_spot_labels(self):
        frames, report=prepare()
        frames["Raw_OHLCV"].loc[0,"source"] = "HYPERLIQUID_SPOT"
        with self.assertRaisesRegex(Stage0Error,"Crypto/perpetual"):
            compare_prepared_crypto_frames(
                frames,symbol="BTC",run_id=RUN,captured=captured_state()
            )

if __name__ == "__main__":
    unittest.main()
