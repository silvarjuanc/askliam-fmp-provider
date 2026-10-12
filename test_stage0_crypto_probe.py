"""Offline CRYPTO_V1 source-probe tests, no FMP/Redis/Sheet calls."""
import asyncio
from datetime import datetime, timedelta, timezone
import unittest

import httpx

from stage0_crypto_probe import (
    COINGECKO_IDS, coingecko_rows, hyperliquid_rows,
    probe_crypto_sources, positive_finite, parse_source_time
)

NOW = datetime(2026, 10, 12, 14, 0, tzinfo=timezone.utc)


def sample_markets(*, dt=None, id_symbol="btc", price=63000.0):
    return [{
        "id": "bitcoin", "symbol": id_symbol,
        "current_price": price,
        "last_updated": (dt or NOW).isoformat(),
        "market_cap": 1_000_000,
        "total_volume": 50_000,
    }]


def sample_hl():
    return [
        {"universe": [{"name":"ETH"}, {"name":"BTC"}, {"name":"SOL"}]},
        [
            {"markPx":"3100", "oraclePx":"3099", "funding":"0.00001",
             "openInterest":"100", "dayNtlVlm":"2000"},
            {"markPx":"63001", "oraclePx":"62999", "funding":"0.00002",
             "openInterest":"200", "dayNtlVlm":"3000"},
            {"markPx":"180", "oraclePx":"180", "funding":"-0.00001",
             "openInterest":"300", "dayNtlVlm":"5000"},
        ],
    ]


class MockResponse:
    def __init__(self, value, code=200):
        self.value = value
        self.status_code = code
    def json(self):
        return self.value


class MockClient:
    def __init__(self, *, quote=None, perp=None, cg_code=200, hl_code=200):
        self.quote = quote if quote is not None else sample_markets()
        self.perp = perp if perp is not None else sample_hl()
        self.cg_code = cg_code
        self.hl_code = hl_code
        self.calls = []
    async def get(self, url, *, params, headers, timeout):
        self.calls.append(("GET", url, params, headers, timeout))
        return MockResponse(self.quote, self.cg_code)
    async def post(self, url, *, json, timeout):
        self.calls.append(("POST", url, json, timeout))
        return MockResponse(self.perp, self.hl_code)


class ParsingTests(unittest.TestCase):
    def test_all_eight_canonical_ids_unique(self):
        self.assertEqual(
            set(COINGECKO_IDS), {"BTC","ETH","SOL","ADA","XRP","DOGE","LINK","SUI"}
        )
        self.assertEqual(len(set(COINGECKO_IDS.values())), 8)

    def test_fresh_market_quote_and_exact_coin_id(self):
        row = coingecko_rows(sample_markets(), ["BTC"], NOW)["BTC"]
        self.assertEqual(row["status"], "QUOTE_FRESH")
        self.assertEqual(row["price_usd"], 63000.0)

    def test_stale_market_is_not_fresh(self):
        data = sample_markets(dt=NOW - timedelta(minutes=30))
        row = coingecko_rows(data, ["BTC"], NOW)["BTC"]
        self.assertEqual(row["status"], "QUOTE_STALE_OR_INVALID")

    def test_id_symbol_mismatch_never_binds(self):
        row = coingecko_rows(sample_markets(id_symbol="btcfake"), ["BTC"], NOW)["BTC"]
        self.assertEqual(row["status"], "ID_SYMBOL_MISMATCH")

    def test_nonfinite_price_not_accepted(self):
        self.assertIsNone(positive_finite(float("nan")))
        self.assertIsNone(positive_finite(-2))
        row = coingecko_rows(sample_markets(price=0), ["BTC"], NOW)["BTC"]
        self.assertEqual(row["status"], "QUOTE_STALE_OR_INVALID")

    def test_hyperliquid_perp_uses_exact_universe(self):
        out = hyperliquid_rows(sample_hl(), ["BTC","ADA"])
        self.assertEqual(out["BTC"]["mark_price_usd"], 63001.0)
        self.assertEqual(out["BTC"]["funding_rate"], 0.00002)
        self.assertEqual(out["ADA"]["status"], "PERP_NOT_LISTED")

    def test_hyperliquid_length_mismatch_fails(self):
        with self.assertRaisesRegex(ValueError, "HL_CONTEXT_LENGTH_MISMATCH"):
            hyperliquid_rows([{"universe":[{"name":"BTC"}]}, []], ["BTC"])

    def test_naive_timestamp_not_assumed_utc(self):
        self.assertIsNone(parse_source_time("2026-10-12T13:00:00"))


class SourceProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_sources_two_requests_no_ready_to_rank(self):
        client = MockClient()
        result = await probe_crypto_sources(["BTC","ETH"], client=client, now=NOW)
        self.assertEqual(len(client.calls), 2)
        self.assertFalse(result["ranking_permitted"])
        self.assertFalse(result["execution_permitted"])
        self.assertFalse(result["sheets_written"])
        self.assertFalse(result["checkpoint_advanced"])
        self.assertFalse(result["fmp_requests_performed"])
        self.assertEqual(result["assets"][0]["canonical_symbol"], "BTC")
        self.assertFalse(result["assets"][0]["mandatory_broad_pass"])

    async def test_upstream_http_errors_are_sanitized(self):
        client = MockClient(cg_code=429, hl_code=503)
        result = await probe_crypto_sources(["BTC"], client=client, now=NOW)
        self.assertEqual(result["coingecko_status"], "HTTP_429")
        self.assertEqual(result["hyperliquid_status"], "HTTP_503")
        self.assertNotIn("price_usd", result["assets"][0]["underlying_spot_quote"])

    async def test_unknown_mapping_fails_no_network(self):
        client = MockClient()
        with self.assertRaisesRegex(ValueError, "CRYPTO_UNIVERSE_REQUIRES_EXACT_MAPPING"):
            await probe_crypto_sources(["XAUT"], client=client, now=NOW)
        self.assertEqual(client.calls, [])

if __name__ == "__main__":
    unittest.main()
