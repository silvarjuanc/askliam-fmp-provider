"""Offline Hyperliquid native-perpetual history probe tests, no real API."""
import unittest
from datetime import datetime, timedelta, timezone

from stage0_crypto_history_probe import (
    DAY_MS, validate_daily_candles, validate_funding_history,
    probe_hyperliquid_history,
)

NOW = datetime(2026, 10, 12, 14, 0, tzinfo=timezone.utc)


def make_candles(count=100, symbol="BTC", gap_at=None):
    today = datetime(NOW.year, NOW.month, NOW.day, tzinfo=timezone.utc)
    rows = []
    for k in range(count, 0, -1):
        day = today - timedelta(days=k)
        ts = int(day.timestamp() * 1000)
        if gap_at is not None and k == gap_at:
            continue
        price = 100 + (count - k) * 0.1
        rows.append({
            "t": ts, "T": ts + DAY_MS - 1,
            "s": symbol, "i": "1d",
            "o": str(price), "h": str(price + 2),
            "l": str(price - 2), "c": str(price + 1),
            "v": "35.0",
        })
    return rows


def make_fundings(symbol="BTC"):
    return [
        {"coin": symbol,
         "time": int((NOW-timedelta(hours=h)).timestamp()*1000),
         "fundingRate": rate}
        for h,rate in [(7, "0.000012"), (3, "-0.00001"), (1, "0.00003")]
    ]


class Response:
    def __init__(self, data, status=200):
        self.data, self.status_code = data, status
    def json(self):
        return self.data


class MockClient:
    def __init__(self, *, candle_code=200, payload=None, coin="BTC"):
        self.calls = []
        self.candle_code = candle_code
        self.coin = coin
        self.payload = payload if payload is not None else make_candles()
    async def post(self, url, *, json, timeout):
        self.calls.append(json)
        typ = json["type"]
        if typ == "metaAndAssetCtxs":
            return Response([
                {"universe": [{"name": self.coin}]},
                [{"markPx": "110", "oraclePx": "109",
                  "funding": "0.0001", "openInterest": "100",
                  "dayNtlVlm": "10000"}],
            ])
        if typ == "candleSnapshot":
            return Response(self.payload, self.candle_code)
        if typ == "fundingHistory":
            return Response(make_fundings(self.coin))
        raise AssertionError("Unexpected request type")


class HistoricalParserTests(unittest.TestCase):
    def test_daily_100_consecutive_returns_3m_and_volatility(self):
        r = validate_daily_candles(make_candles(), "BTC", now=NOW)
        self.assertEqual(r["status"], "DERIVATIVE_HISTORY_VALIDATED")
        self.assertEqual(r["consecutive_candles"], 100)
        self.assertGreater(r["perpetual_return_3m_pct"], 0)
        self.assertGreaterEqual(r["perpetual_volatility_20d_annual_pct"], 0)
        self.assertEqual(r["history_kind"], "HYPERLIQUID_NATIVE_PERPETUAL")

    def test_gap_reduces_contiguous_history(self):
        r = validate_daily_candles(make_candles(gap_at=40), "BTC", now=NOW)
        self.assertEqual(r["status"], "DERIVATIVE_HISTORY_INSUFFICIENT")
        self.assertEqual(r["consecutive_candles"], 39)
        self.assertNotIn("perpetual_return_3m_pct", r)

    def test_live_daily_candle_excluded(self):
        rows = make_candles()
        t = int(datetime(2026, 10, 12, tzinfo=timezone.utc).timestamp()*1000)
        rows.append({"s":"BTC", "i":"1d", "t":t, "T":t+DAY_MS-1,
                     "o":"150", "h":"151", "l":"149", "c":"150", "v":"0"})
        r=validate_daily_candles(rows,"BTC",now=NOW)
        self.assertEqual(r["complete_candles"],100)
        self.assertEqual(r["last_complete_day_utc"],"2026-10-11")

    def test_wrong_asset_refused(self):
        with self.assertRaisesRegex(ValueError,"CANDLE_INSTRUMENT_MISMATCH"):
            validate_daily_candles(make_candles(symbol="ETH"),"BTC",now=NOW)

    def test_invalid_ohlc_refused(self):
        rows=make_candles()
        rows[0]["h"]="1"
        with self.assertRaisesRegex(ValueError,"CANDLE_OHLC_INCONSISTENT"):
            validate_daily_candles(rows,"BTC",now=NOW)

    def test_duplicate_day_refused(self):
        rows=make_candles()
        rows.append(dict(rows[-1]))
        with self.assertRaisesRegex(ValueError,"CANDLE_DUPLICATE_DAY"):
            validate_daily_candles(rows,"BTC",now=NOW)

    def test_history_inadequate_below_64_bars(self):
        r=validate_daily_candles(make_candles(count=63),"BTC",now=NOW)
        self.assertEqual(r["status"],"DERIVATIVE_HISTORY_INSUFFICIENT")

    def test_funding_rate_mean_and_latest(self):
        r=validate_funding_history(make_fundings(),"BTC",now=NOW)
        self.assertEqual(r["status"],"REALIZED_FUNDING_OBSERVED")
        self.assertEqual(r["funding_observations"],3)
        self.assertAlmostEqual(r["last_rate"],0.00003)

    def test_wrong_funding_coin_refused(self):
        with self.assertRaisesRegex(ValueError,"FUNDING_INSTRUMENT_MISMATCH"):
            validate_funding_history(make_fundings("ETH"),"BTC",now=NOW)


class ProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_normal_three_calls_never_ranks_or_writes(self):
        client=MockClient()
        r=await probe_hyperliquid_history("BTC",client=client,now=NOW)
        self.assertEqual(len(client.calls),3)
        self.assertEqual(r["perpetual_daily_history"]["status"],
                         "DERIVATIVE_HISTORY_VALIDATED")
        self.assertFalse(r["ranking_permitted"])
        self.assertFalse(r["execution_permitted"])
        self.assertFalse(r["sheets_written"])
        self.assertTrue(r["perpetual_not_spot"])
        self.assertEqual(client.calls[1]["req"]["interval"],"1d")
        self.assertEqual(client.calls[2]["type"],"fundingHistory")

    async def test_429_stops_after_two_calls(self):
        client=MockClient(candle_code=429)
        r=await probe_hyperliquid_history("BTC",client=client,now=NOW)
        self.assertEqual(r["perpetual_daily_history"]["status"],"HTTP_429")
        self.assertEqual(r["funding_history"]["status"],
                         "NOT_REQUESTED_AFTER_SOURCE_FAILURE")
        self.assertEqual(len(client.calls),2)

    async def test_nonlisted_coin_stops_after_one_call(self):
        client=MockClient(coin="ETH")
        r=await probe_hyperliquid_history("BTC",client=client,now=NOW)
        self.assertEqual(r["context_status"],"PERP_NOT_LISTED")
        self.assertEqual(len(client.calls),1)

    async def test_bad_crypto_symbol_never_calls_network(self):
        client=MockClient()
        with self.assertRaisesRegex(ValueError,"CRYPTO_SYMBOL_NOT_IN_CANONICAL_MAPPING"):
            await probe_hyperliquid_history("XAUT",client=client,now=NOW)
        self.assertFalse(client.calls)


if __name__ == "__main__":
    unittest.main()
