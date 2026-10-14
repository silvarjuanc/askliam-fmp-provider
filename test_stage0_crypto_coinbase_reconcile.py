"""Offline data-integrity tests for guarded ETH post-write reconciliation."""
import hashlib
import json
import unittest
from datetime import datetime, timedelta, timezone

from stage0_crypto_coinbase_reconcile import validated_saved_spot
from stage0_crypto_coinbase_spot import SOURCE, ENDPOINT
from stage0_crypto_coinbase_prepare import MANDATORY
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

NOW = datetime(2026, 10, 14, 8, 15, tzinfo=timezone.utc)
RUN_ID = "STAGE0-20261014-V6.5.1"
FIRST = NOW.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=299)


def fixture():
    candles = []
    raw = {}
    values = []
    for i in range(299):
        stamp = int((FIRST + timedelta(days=i)).timestamp())
        opn = 2500 + i
        low, high, close, volume = opn - 3, opn + 4, opn + 2, 12345.1 + i
        candles.append([stamp, low, high, opn, close, volume])
        day = (FIRST + timedelta(days=i)).date().isoformat()
        canonical = [day, float(opn), float(high), float(low), float(close), float(volume)]
        values.append(canonical)
        raw[("ETH", day, SOURCE)] = {
            "canonical_symbol": "ETH", "source": SOURCE, "provider_symbol": "ETH-USD",
            "ranking_run_id": RUN_ID, "quality_status": "INDEPENDENT_SPOT_OHLCV_VALIDATED",
            "upsert_key": "ETH|" + day + "|" + SOURCE,
            "source_url_template": ENDPOINT + "/products/ETH-USD/candles",
            **{
                key: Stage0_Daily_Ingestor._scalar(value)
                for key, value in zip(("open","high","low","close","volume"),canonical[1:])
            },
        }
    snapshot = {
        "asset_class": "Crypto", "score_model": "CRYPTO_V1",
        "analysis_price_source": SOURCE, "mandatory_broad_pass": "TRUE",
        "data_completeness_pct": "100", "missing_mandatory_fields": "NONE",
        "enrichment_status": "ACQUIRED_NOT_SCORED",
        "broad_readiness": "BROAD_INPUTS_VALIDATED_NOT_SCORED",
        "data_timestamp": "2026-10-14T08:04:40+00:00",
        **{k:"1.0" for k in MANDATORY},
    }
    digest = hashlib.sha256(json.dumps(
        values, separators=(",", ":"), allow_nan=False
    ).encode()).hexdigest()
    saved = {
        "Raw_OHLCV": {"rows":raw},
        "Market_Data_Snapshot": {"rows":{(RUN_ID,"ETH"):snapshot}},
    }
    return candles, saved, digest


class PostWriteReadbackTests(unittest.TestCase):
    def test_299_complete_saved_candles_and_mandatory_snapshot(self):
        candles, saved, digest = fixture()
        r=validated_saved_spot(saved, candles, symbol="ETH", run_id=RUN_ID,
                               today=NOW, journal_digest=digest)
        self.assertEqual(r["daily_bars_verified"],299)
        self.assertEqual(r["stage0_fields_verified"],9)
        self.assertTrue(r["snapshot_verified"])

    def test_refuse_mismatching_journal_hash(self):
        candles, saved, _ = fixture()
        with self.assertRaisesRegex(Stage0Error,"SHA mismatch"):
            validated_saved_spot(saved, candles, symbol="ETH", run_id=RUN_ID,
                                 today=NOW, journal_digest="0"*64)

    def test_refuse_any_missing_saved_row(self):
        candles, saved, digest = fixture()
        saved["Raw_OHLCV"]["rows"].pop(("ETH","2026-10-13",SOURCE))
        with self.assertRaisesRegex(Stage0Error,"Missing saved Coinbase candle"):
            validated_saved_spot(saved, candles, symbol="ETH", run_id=RUN_ID,
                                 today=NOW, journal_digest=digest)

    def test_refuse_unknown_snapshot(self):
        candles, saved, digest = fixture()
        saved["Market_Data_Snapshot"]["rows"].clear()
        with self.assertRaisesRegex(Stage0Error,"Snapshot absent"):
            validated_saved_spot(saved, candles, symbol="ETH", run_id=RUN_ID,
                                 today=NOW, journal_digest=digest)


if __name__=="__main__":
    unittest.main()
