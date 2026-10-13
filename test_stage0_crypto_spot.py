"""Offline tests for CoinGecko independent spot extraction and s47 transforms."""
from __future__ import annotations

import math
import unittest
from datetime import datetime, timedelta, timezone

from stage0_crypto_spot import (
    MIN_HISTORY, SPOT_SOURCE, PRICE_SOURCE,
    BROAD_MANDATORY,
    calculate_canonical_spot_technicals,
    make_spot_frames, merge_derivative_and_spot_snapshot,
    validate_coin_gecko_chart, validate_spot_quote,
)

NOW = datetime(2026,10,13,20,30,tzinfo=timezone.utc)
RUN = "STAGE0-20261013-V6.5.1"


def fixtures(symbol="BTC", count=300, gap=None):
    start = datetime(2026,10,13,tzinfo=timezone.utc) - timedelta(days=count)
    pts = []
    prices = []
    volumes = []
    for i in range(count):
        day = start + timedelta(days=i)
        if gap is not None and i == gap:
            continue
        timestamp = int(day.timestamp()*1000)
        price = 50 + i / 7
        volume = 1_000_000 + (i % 29) * 10_000
        prices.append([timestamp, price])
        volumes.append([timestamp, volume])
        pts.append({"day": day.date().isoformat(),
                    "close": price,"usd_volume_24h": volume})
    ids={"BTC":"bitcoin","ETH":"ethereum","ADA":"cardano"}
    quote=[{"id":ids.get(symbol,"bitcoin"),"symbol":symbol.lower(),
           "current_price":1000,
           "last_updated":"2026-10-13T20:25:00Z"}]
    return {"prices":prices,"total_volumes":volumes},quote,pts


class SpotValidationTests(unittest.TestCase):
    def test_spot_sufficiency_over_300_utc_points(self):
        chart,quote,_=fixtures()
        frames,report=make_spot_frames("BTC",chart=chart,quotes=quote,now=NOW,run_id=RUN)
        self.assertEqual(report["data_points"],300)
        raw=frames["Raw_OHLCV"]
        snap=frames["Market_Data_Snapshot"].iloc[0]
        self.assertEqual(len(raw),300)
        self.assertTrue(raw["open"].eq("").all())
        self.assertTrue(raw["high"].eq("").all())
        self.assertTrue(raw["low"].eq("").all())
        self.assertEqual(raw.iloc[0]["source"],SPOT_SOURCE)
        self.assertEqual(raw.iloc[0]["upsert_key"],
                         "BTC|2025-12-17|COINGECKO_SPOT_USD_DAILY_POINT")
        self.assertEqual(snap["analysis_price_source"], PRICE_SOURCE)
        self.assertEqual(snap["last_price"],1000)
        self.assertIsNotNone(snap["return_1m"])
        self.assertIsNotNone(snap["volatility_20d"])
        self.assertIsNotNone(snap["momentum_score"])
        self.assertIsNotNone(snap["trend_score"])
        self.assertIsNotNone(snap["volume_score"])
        self.assertNotIn("open",snap.index)
        self.assertFalse(report["ranking_permitted"])

    def test_technical_formulas_exact(self):
        _,_,points=fixtures()
        derived=calculate_canonical_spot_technicals(points)
        p=[x["close"] for x in points]
        expected_return=100*(p[-1]/p[-22]-1)
        self.assertAlmostEqual(derived["return_1m"],expected_return,places=11)
        self.assertAlmostEqual(derived["return_3m"],100*(p[-1]/p[-64]-1))
        mom=.45*derived["return_1m"]+.35*derived["return_3m"]+.20*derived["return_6m"]
        self.assertAlmostEqual(derived["momentum_score"],50+50*math.tanh(mom/15))
        self.assertTrue(0<=derived["volume_score"]<=100)
        self.assertTrue(derived["volatility_20d"]>0)

    def test_strict_daily_gaps_or_duplicate_or_nondaily_fails(self):
        chart,_,_=fixtures(gap=120)
        with self.assertRaisesRegex(ValueError,"SPOT_DAILY_GAP"):
            validate_coin_gecko_chart("BTC",chart,now=NOW)
        chart,_,_=fixtures()
        chart["prices"].append(chart["prices"][0])
        chart["total_volumes"].append(chart["total_volumes"][0])
        with self.assertRaisesRegex(ValueError,"SPOT_DUPLICATE_DAILY_SAMPLE"):
            validate_coin_gecko_chart("BTC",chart,now=NOW)
        chart,_,_=fixtures()
        chart["prices"][0][0]+=3_600_000
        with self.assertRaisesRegex(ValueError,"SPOT_SAMPLE_GRANULARITY"):
            validate_coin_gecko_chart("BTC",chart,now=NOW)

    def test_invalid_quote_identity_stale_and_price(self):
        _,quote,_=fixtures()
        validate_spot_quote("BTC",quote,now=NOW)
        quote[0]["last_updated"]="2026-10-13T18:00:00Z"
        with self.assertRaisesRegex(ValueError,"SPOT_QUOTE_STALE"):
            validate_spot_quote("BTC",quote,now=NOW)
        quote[0]["last_updated"]="2026-10-13T20:25:00Z"
        quote[0]["id"]="wrapped-bitcoin"
        with self.assertRaisesRegex(ValueError,"SPOT_QUOTE_IDENTITY"):
            validate_spot_quote("BTC",quote,now=NOW)

    def test_canonical_crypto_merge_correct_nine_fields(self):
        chart,quote,_=fixtures()
        frames,_=make_spot_frames("BTC",chart=chart,quotes=quote,now=NOW,run_id=RUN)
        existing={
            "ranking_run_id":RUN,"canonical_symbol":"BTC",
            "score_model":"CRYPTO_V1",
            "analysis_price_source":"HYPERLIQUID_NATIVE_PERP_REFERENCE_ONLY",
            "enrichment_status":"ACQUIRED_PERPETUAL_NOT_SPOT",
            "data_timestamp":"2026-10-13T20:24:00+00:00",
            "funding_rate":"0.00001","open_interest":"1000",
            "last_price":"","source_summary":"https://api.hyperliquid.xyz/info",
        }
        combined=merge_derivative_and_spot_snapshot(
            existing,frames["Market_Data_Snapshot"].iloc[0].to_dict(),now=NOW,
        )
        self.assertEqual(combined["data_completeness_pct"],100)
        self.assertEqual(combined["mandatory_broad_pass"],"TRUE")
        self.assertEqual(combined["broad_readiness"],"BROAD_SCAN_READY")
        self.assertEqual(set(BROAD_MANDATORY),{k for k in BROAD_MANDATORY
                          if combined[k] is not None})
        self.assertEqual(combined["funding_rate"],0.00001)
        self.assertEqual(combined["open_interest"],1000)
        self.assertEqual(combined["missing_mandatory_fields"],"")
        self.assertIn("Hyperliquid",combined["notes"])

    def test_stale_derivative_or_unauthorized_snapshot_blocks_merge(self):
        chart,quote,_=fixtures()
        frames,_=make_spot_frames("BTC",chart=chart,quotes=quote,now=NOW,run_id=RUN)
        proposed=frames["Market_Data_Snapshot"].iloc[0].to_dict()
        prior=dict(ranking_run_id=RUN,canonical_symbol="BTC",
                   score_model="CRYPTO_V1",analysis_price_source="HYPERLIQUID_NATIVE_PERP_REFERENCE_ONLY",
                   enrichment_status="ACQUIRED_PERPETUAL_NOT_SPOT",last_price="",
                   data_timestamp="2026-10-13T18:00:00Z",
                   funding_rate=".001",open_interest="123",
                   source_summary="https://api.hyperliquid.xyz/info")
        with self.assertRaisesRegex(ValueError,"SPOT_MERGE_DERIVATIVES_STALE"):
            merge_derivative_and_spot_snapshot(prior,proposed,now=NOW)
        prior["data_timestamp"]="2026-10-13T20:29:00Z"
        prior["analysis_price_source"]="FMP_STABLE"
        with self.assertRaisesRegex(ValueError,"SPOT_SNAPSHOT_COLLISION"):
            merge_derivative_and_spot_snapshot(prior,proposed,now=NOW)


if __name__ == "__main__":
    unittest.main()
