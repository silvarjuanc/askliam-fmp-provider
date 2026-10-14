"""Offline tests: DST guard, no duplicate crypto, stop on uncertain state.

No Render, Sheets, Upstash, FMP or Hyperliquid requests are performed.
"""
import unittest
import os
from datetime import datetime, timezone
from unittest.mock import patch
from scripts.stage0_daily_scheduler import (
    VERSION, CRYPTO, SchedulerHalt, run, schedule_gate, safe_error, main,
)
import httpx

UTC = timezone.utc
SUMMER = datetime(2026, 10, 13, 13, 0, tzinfo=UTC)
WINTER = datetime(2026, 12, 14, 14, 0, tzinfo=UTC)


class SchedulerTests(unittest.TestCase):
    def test_disarmed_cron_does_not_require_credentials(self):
        import io
        from contextlib import redirect_stdout
        buffer = io.StringIO()
        with patch.dict(os.environ, {"ASKLIAM_CRON_ENABLED": "false",
                                    "ASKLIAM_SERVICE_TOKEN": ""}, clear=True):
            with patch("scripts.stage0_daily_scheduler.schedule_gate", return_value=True):
                with redirect_stdout(buffer):
                    exit_code = main()
        self.assertEqual(exit_code, 2)
        self.assertIn("CRON_DISARMED", buffer.getvalue())

    def test_off_window_without_credentials_is_safe_noop(self):
        import io
        from contextlib import redirect_stdout
        buffer=io.StringIO()
        with patch.dict(os.environ, {"ASKLIAM_CRON_ENABLED":"true",
                                     "ASKLIAM_SERVICE_TOKEN":""}, clear=True):
            with patch("scripts.stage0_daily_scheduler.schedule_gate",return_value=False):
                with redirect_stdout(buffer):
                    exit_code=main()
        self.assertEqual(exit_code,0)
        self.assertIn("NOT_SCHEDULED_STOCKHOLM_PREANALYSIS",buffer.getvalue())

    def test_stockholm_preanalysis_windows_across_europe_dst(self):
        # Stockholm windows 09:00, 12:00, 14:00, 15:00, regardless of US DST.
        for summer_utc, winter_utc in ((7,8), (10,11), (12,13), (13,14)):
            self.assertTrue(schedule_gate(datetime(2026,10,13,summer_utc,0,tzinfo=UTC)))
            self.assertTrue(schedule_gate(datetime(2026,12,14,winter_utc,0,tzinfo=UTC)))
        self.assertTrue(schedule_gate(SUMMER))
        self.assertTrue(schedule_gate(WINTER))
        self.assertFalse(schedule_gate(datetime(2026,10,13,14,0,tzinfo=UTC))) # 16:00 CEST
        self.assertFalse(schedule_gate(datetime(2026,12,14,15,0,tzinfo=UTC))) # 16:00 CET
        self.assertFalse(schedule_gate(datetime(2026,10,17,7,0,tzinfo=UTC)))  # Saturday
        self.assertFalse(schedule_gate(datetime(2026,10,13,13,11,tzinfo=UTC)))
        self.assertFalse(schedule_gate(datetime(2026,10,13,13,45,tzinfo=UTC)))
        # European DST fall-back (2026-10-25) keeps 09:00 CET as 08:00 UTC.
        self.assertTrue(schedule_gate(datetime(2026,10,26,8,0,tzinfo=UTC)))
        self.assertFalse(schedule_gate(datetime(2026,10,26,7,0,tzinfo=UTC)))

    def test_rejects_unscheduled_without_any_network(self):
        with patch("scripts.stage0_daily_scheduler.request") as rq:
            out = run(object(), "placeholder", now=datetime(2026, 10, 13, 21, 0, tzinfo=UTC))
            self.assertEqual(out["status"], "NOT_SCHEDULED_STOCKHOLM_PREANALYSIS")
            rq.assert_not_called()

    def test_missing_server_opt_in_fails_before_writer(self):
        calls = []
        def fake_request(client, method, path, token, **kw):
            calls.append((method, path))
            return {"version": VERSION, "stage0_auto_enabled": False}
        with patch("scripts.stage0_daily_scheduler.request", new=fake_request):
            out = run(object(), "secret", now=SUMMER)
        self.assertEqual(out["status"], "HALTED")
        self.assertEqual(out["halt_reason"], "SCHEDULER_CONFIG_INCOMPLETE")
        self.assertEqual(calls, [("GET", "/stage0/auto/config")])

    def test_pending_reconciliation_stops_all_writes(self):
        calls = []
        def fake_request(client, method, path, token, **kw):
            calls.append(method)
            if path == "/stage0/auto/config":
                return dict(version=VERSION, stage0_auto_enabled=True, crypto_auto_enabled=True,
                            fmp_stage0_enabled=True, redis_configured=True,
                            sheets_configured=True, manual_crypto_write_enabled=False)
            return dict(version=VERSION, utc_date="2026-10-13",pending_reconciliation=True)
        with patch("scripts.stage0_daily_scheduler.request", new=fake_request):
            out = run(object(), "secret", now=SUMMER)
        self.assertEqual(out["status"], "HALTED")
        self.assertEqual(out["halt_reason"], "RECONCILIATION_REQUIRED")
        self.assertNotIn("POST", calls)

    def test_eight_verified_crypto_idempotent_then_fmp_budget_guard(self):
        completed = {"ADA", "BTC"}
        calls = []
        last_symbol = "BTC"
        last_digest = "f" * 64
        run_id = "STAGE0-20261013-V6.5.1"
        def fake_request(client, method, path, token, *, json_body=None):
            nonlocal last_symbol, last_digest
            calls.append((method, path))
            if path == "/stage0/auto/config":
                return dict(version=VERSION, stage0_auto_enabled=True, crypto_auto_enabled=True,
                            fmp_stage0_enabled=True, redis_configured=True,
                            sheets_configured=True, manual_crypto_write_enabled=False)
            if path == "/stage0/batch/status":
                return dict(version=VERSION, utc_date="2026-10-13",pending_reconciliation=False,
                            quota={"remaining":40, "blocked":False}, checkpoint={"completed_symbols":[]})
            if path.startswith("/stage0/crypto/checkpoint"):
                return dict(version=VERSION, utc_date="2026-10-13",
                            completed_symbols=sorted(completed), pending_reconciliation=False,
                            last_verified_symbol=last_symbol,
                            last_verified_run_id=run_id,
                            last_verified_ohlcv_sha256=last_digest)
            if path == "/stage0/auto/runtime-evidence-sync":
                return dict(version=VERSION, sync_status="COMMITTED_VERIFY_PASS",
                            spot_verified=[], fmp_used=200, fmp_remaining=40)
            if path == "/stage0/auto/crypto-step":
                symbol=json_body["symbol"]
                self.assertNotIn(symbol, completed)
                completed.add(symbol)
                last_symbol=symbol
                last_digest = symbol * 16
                return dict(version=VERSION, canonical_symbol=symbol,
                            status="COMMITTED_VERIFY_PASS",run_id=run_id,
                            daily_bars_expected=150,ohlcv_sha256=last_digest,
                            ranking_permitted=False,execution_permitted=False)
            raise AssertionError("Unexpected request: " + path)
        with patch("scripts.stage0_daily_scheduler.request", new=fake_request):
            out = run(object(), "secret", now=SUMMER, sleep=lambda _: None)
        self.assertEqual(out["status"], "STAGE0_VERIFIED_OR_PROVIDER_DEFERRED")
        self.assertEqual(set(out["crypto_verified"]), set(CRYPTO)-{"ADA","BTC"})
        self.assertEqual(out["crypto_previously_verified"], ["ADA","BTC"])
        self.assertEqual(out["fmp_stop_reason"], "FMP_QUOTA_RESERVE")
        self.assertEqual(len([x for x in calls if x[0] == "POST" and x[1]=="/stage0/auto/crypto-step"]), 6)
        self.assertEqual(len([x for x in calls if x[1]=="/stage0/auto/runtime-evidence-sync"]), 1)
        self.assertFalse(out["ranking_permitted"])

    def test_spot_verified_btc_eth_do_not_halt_remaining_daily_run(self):
        completed = {"ADA"}
        spot = {"BTC", "ETH"}
        last_symbol = "ADA"
        last_digest = "f" * 64
        run_id = "STAGE0-20261013-V6.5.1"
        posts = []
        def fake_request(client, method, path, token, *, json_body=None):
            nonlocal last_symbol, last_digest
            if path == "/stage0/auto/config":
                return dict(version=VERSION, stage0_auto_enabled=True, crypto_auto_enabled=True,
                            fmp_stage0_enabled=True, redis_configured=True,
                            sheets_configured=True, manual_crypto_write_enabled=False)
            if path == "/stage0/batch/status":
                return dict(version=VERSION, utc_date="2026-10-13",pending_reconciliation=False,
                            quota={"remaining":40, "blocked":False}, checkpoint={"completed_symbols":[]})
            if path.startswith("/stage0/crypto/checkpoint"):
                return dict(version=VERSION, utc_date="2026-10-13",
                            completed_symbols=sorted(completed),
                            spot_completed_symbols=sorted(spot),
                            pending_reconciliation=False,
                            last_verified_symbol=last_symbol,
                            last_verified_run_id=run_id,
                            last_verified_ohlcv_sha256=last_digest)
            if path == "/stage0/auto/runtime-evidence-sync":
                return dict(version=VERSION, sync_status="COMMITTED_VERIFY_PASS",
                            spot_verified=["BTC","ETH"], fmp_used=200, fmp_remaining=40)
            if path == "/stage0/auto/crypto-step":
                symbol = json_body["symbol"]
                self.assertNotIn(symbol, spot)
                self.assertNotIn(symbol, completed)
                posts.append(symbol)
                completed.add(symbol)
                last_symbol = symbol
                last_digest = symbol * 16
                return dict(version=VERSION, canonical_symbol=symbol,
                            status="COMMITTED_VERIFY_PASS", run_id=run_id,
                            daily_bars_expected=150, ohlcv_sha256=last_digest,
                            ranking_permitted=False, execution_permitted=False)
            raise AssertionError("Unexpected request: " + path)
        with patch("scripts.stage0_daily_scheduler.request", new=fake_request):
            result = run(object(), "secret", now=SUMMER, sleep=lambda _: None)
        self.assertEqual(result["status"], "STAGE0_VERIFIED_OR_PROVIDER_DEFERRED")
        self.assertEqual(result["crypto_spot_previously_verified"], ["BTC", "ETH"])
        self.assertEqual(result["crypto_previously_verified"], ["ADA", "BTC", "ETH"])
        self.assertEqual(set(posts), set(CRYPTO)-{"ADA", "BTC", "ETH"})
        self.assertEqual(result["fmp_stop_reason"], "FMP_QUOTA_RESERVE")

    def test_fmp_step_precedes_crypto_writer(self):
        paths = []
        def fake_request(client, method, path, token, *, json_body=None):
            paths.append((method, path))
            if path == "/stage0/auto/config":
                return dict(version=VERSION, stage0_auto_enabled=True,
                            crypto_auto_enabled=True, fmp_stage0_enabled=True,
                            redis_configured=True, sheets_configured=True,
                            manual_crypto_write_enabled=False)
            if path == "/stage0/batch/status":
                return dict(version=VERSION, utc_date="2026-10-13",
                            pending_reconciliation=False,quota={"remaining":45,"blocked":False},
                            checkpoint={"completed_symbols":[]})
            if path == "/stage0/daily/plan":
                return dict(version=VERSION, utc_date="2026-10-13",
                            pending_reconciliation=False,next_symbols=["SPY"])
            if path == "/stage0/daily/step":
                return dict(version=VERSION,status="SOURCE_RETRYABLE",symbol="SPY",
                            checkpoint_advanced=False)
            if path.startswith("/stage0/crypto/checkpoint?"):
                return dict(version=VERSION,utc_date="2026-10-13",
                            completed_symbols=list(CRYPTO),
                            spot_completed_symbols=[],pending_reconciliation=False)
            if path == "/stage0/auto/runtime-evidence-sync":
                return dict(version=VERSION, sync_status="COMMITTED_VERIFY_PASS",
                            spot_verified=[],fmp_used=5,fmp_remaining=235)
            raise AssertionError("unexpected path: "+path)
        with patch("scripts.stage0_daily_scheduler.request",new=fake_request):
            out=run(object(),"secret",now=SUMMER,sleep=lambda _:None)
        self.assertEqual(out["fmp_stop_reason"],"FMP_RETRYABLE_SOURCE")
        self.assertIn(("POST","/stage0/daily/step"),paths)
        self.assertLess(
            paths.index(("POST","/stage0/daily/step")),
            max(i for i, item in enumerate(paths)
                if item == ("GET","/stage0/crypto/checkpoint?day=2026-10-13&symbol=BTC")),
        )
        self.assertNotIn(("POST","/stage0/auto/crypto-step"),paths)

    def test_precutoff_1600_stops_before_any_remote_action(self):
        with patch("scripts.stage0_daily_scheduler.request") as request:
            out=run(object(),"secret",now=datetime(2026,10,13,20,0,tzinfo=UTC))
        self.assertEqual(out["status"],"NOT_SCHEDULED_STOCKHOLM_PREANALYSIS")
        request.assert_not_called()

    def test_unsafe_crypto_error_halts_before_next_asset(self):
        calls = []
        def fake_request(client, method, path, token, *, json_body=None):
            calls.append((method,path))
            if path == "/stage0/auto/config":
                return dict(version=VERSION, stage0_auto_enabled=True, crypto_auto_enabled=True,
                            fmp_stage0_enabled=True, redis_configured=True,
                            sheets_configured=True, manual_crypto_write_enabled=False)
            if path == "/stage0/batch/status":
                return dict(version=VERSION, utc_date="2026-10-13",pending_reconciliation=False)
            if path.startswith("/stage0/crypto/checkpoint"):
                return dict(version=VERSION, utc_date="2026-10-13",
                            completed_symbols=[],pending_reconciliation=False)
            if path == "/stage0/auto/crypto-step":
                raise SchedulerHalt("CHECKPOINT_NOT_COMMITTED")
            raise AssertionError(path)
        with patch("scripts.stage0_daily_scheduler.request", new=fake_request):
            out = run(object(), "secret", now=SUMMER, sleep=lambda _: None)
        self.assertEqual(out["status"], "HALTED")
        self.assertEqual(out["halt_reason"], "CHECKPOINT_NOT_COMMITTED")
        self.assertEqual(len([x for x in calls if x[0]=="POST"]),1)


if __name__=="__main__":
    unittest.main()
