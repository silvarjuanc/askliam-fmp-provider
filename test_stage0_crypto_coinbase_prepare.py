"""Offline V6.5.1 Coinbase spot + derivatives context contract tests."""
import unittest
from datetime import datetime, timedelta, timezone

from stage0_crypto_coinbase_prepare import (
    MANDATORY, prepare_coinbase_spot_frames, validated_ticker,
)
from stage0_crypto_coinbase_spot import SOURCE


NOW = datetime(2026, 10, 14, 12, tzinfo=timezone.utc)
RUN_ID = "STAGE0-20261014-V6.5.1"
FIRST = datetime(2026, 10, 14, tzinfo=timezone.utc) - timedelta(days=299)


def candles():
    out = []
    for i in range(299):
        base = 40000 + i * 10
        when = int((FIRST + timedelta(days=i)).timestamp())
        out.append([when, base - 4, base + 8, base, base + 3, 100 + i])
    return list(reversed(out))


def derivatives(symbol="BTC"):
    return [
        {"universe": [{"name": symbol}]},
        [{
            "markPx": "45000", "oraclePx": "45010",
            "funding": "0.0001", "openInterest": "5000",
            "dayNtlVlm": "10000000",
        }],
    ]


def quote(age=30):
    return {
        "price": "45120.45",
        "time": (NOW - timedelta(seconds=age)).isoformat(),
    }


class SpotStage0PreparationTests(unittest.TestCase):
    def test_independent_spot_and_derivatives_fields(self):
        for symbol in ("BTC", "ETH"):
            frames, report = prepare_coinbase_spot_frames(
                symbol, candles=candles(), ticker=quote(),
                hyperliquid_context=derivatives(symbol),
                now=NOW, run_id=RUN_ID,
            )
            self.assertEqual(len(frames["Raw_OHLCV"]), 299)
            self.assertEqual(
                set(frames["Raw_OHLCV"]["source"]), {SOURCE},
            )
            snap = frames["Market_Data_Snapshot"].iloc[0]
            self.assertEqual(snap["analysis_price_source"], SOURCE)
            self.assertEqual(snap["last_price"], 45120.45)
            self.assertEqual(snap["funding_rate"], 0.0001)
            self.assertEqual(snap["open_interest"], 5000)
            self.assertEqual(snap["mandatory_broad_pass"], "TRUE")
            self.assertEqual(snap["missing_mandatory_fields"], "NONE")
            self.assertTrue(all(k in snap for k in MANDATORY))
            self.assertTrue(all(
                frames["Raw_OHLCV"]["upsert_key"].str.endswith("|" + SOURCE)
            ))
            self.assertFalse(report["ranking_permitted"])
            self.assertFalse(report["execution_permitted"])

    def test_reject_stale_spot_ticker(self):
        with self.assertRaisesRegex(ValueError, "COINBASE_TICKER_STALE"):
            validated_ticker(quote(age=901), NOW)

    def test_reject_future_spot_ticker(self):
        with self.assertRaisesRegex(ValueError, "COINBASE_TICKER_STALE"):
            validated_ticker(quote(age=-5), NOW)

    def test_reject_wrong_run_id(self):
        with self.assertRaisesRegex(ValueError, "SPOT_RUN_ID_INVALID"):
            prepare_coinbase_spot_frames(
                "BTC", candles=candles(), ticker=quote(),
                hyperliquid_context=derivatives(),
                now=NOW, run_id="STAGE0-20261013-V6.5.1",
            )

    def test_invalid_bar_fail_closed(self):
        corrupt = candles()
        corrupt[0][2] = 0
        with self.assertRaises(ValueError):
            prepare_coinbase_spot_frames(
                "BTC", candles=corrupt, ticker=quote(),
                hyperliquid_context=derivatives(),
                now=NOW, run_id=RUN_ID,
            )

    def test_missing_derivative_context_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "DERIVATIVES_CONTEXT_INCOMPLETE"):
            prepare_coinbase_spot_frames(
                "BTC", candles=candles(), ticker=quote(),
                hyperliquid_context=[
                    {"universe": [{"name": "BTC"}]},
                    [{"markPx": "45000", "funding": None, "openInterest": "5000"}],
                ],
                now=NOW, run_id=RUN_ID,
            )


if __name__ == "__main__":
    unittest.main()
