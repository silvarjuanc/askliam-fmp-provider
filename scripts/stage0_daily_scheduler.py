"""AskLiam V6.5.1 unattended Stage0 driver. No trading or Ranking.

Invoke with Render Cron at '0 20,21 * * 1-5' UTC. The NY-local guard
selects exactly 16:00 America/New_York across DST transitions.
Environment: ASKLIAM_SERVICE_TOKEN (set ONCE on the cron service),
ASKLIAM_BASE_URL (optional; hard-coded trusted default).
Server must separately opt-in via ASKLIAM_STAGE0_AUTO_ENABLED and
ASKLIAM_CRYPTO_AUTO_ENABLED. Manual per-symbol writer remains separate.

Every state change is via existing authenticated Stage0 writer routes.
This process never handles Google Sheets credentials, Redis credentials,
broker tokens, or an execution API. No secret values are logged.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx

VERSION = "V6.5.1"
CRYPTO = ("ADA", "BTC", "DOGE", "ETH", "LINK", "SOL", "SUI", "XRP")
MAX_WALL_SECONDS = 75 * 60
MIN_FMP_QUOTA = 40
BASE_URL = "https://askliam-fmp-provider.onrender.com"
NY = ZoneInfo("America/New_York")


class SchedulerHalt(RuntimeError):
    """Reason is a fixed, non-secret diagnostic code."""


def schedule_gate(now: datetime) -> bool:
    if now.tzinfo is None:
        raise ValueError("timezone-aware UTC required")
    local = now.astimezone(NY)
    # Tolerate reasonable cron startup delay without allowing a late rerun.
    return local.weekday() < 5 and local.hour == 16


def safe_error(response: httpx.Response) -> str:
    try:
        detail = response.json().get("detail")
        if isinstance(detail, dict):
            value = str(detail.get("stage0_status") or "HTTP_ERROR")
        elif isinstance(detail, str):
            value = detail
        else:
            value = "HTTP_ERROR"
    except (ValueError, AttributeError, TypeError):
        value = "HTTP_ERROR"
    # Never print arbitrary provider responses, headers, URL queries or tokens.
    allowed = {
        "PROVIDER_DEFERRED", "SOURCE_RETRYABLE", "FMP_QUOTA_GUARD",
        "RECONCILIATION_REQUIRED", "ALREADY_COMMITTED_TODAY",
        "STAGE0_DISABLED", "CRYPTO_AUTO_DISABLED",
        "MANUAL_CRYPTO_WRITE_STILL_ENABLED", "CONCURRENT_RUN_BLOCKED",
        "SHEET_PREFLIGHT_UNAVAILABLE", "SNAPSHOT_COLLISION",
        "CHECKPOINT_NOT_COMMITTED", "AUTO_CRYPTO_COMMIT_HALTED",
        "CONFIGURATION_FAILED", "UPSTASH_UNAVAILABLE",
    }
    return value if value in allowed else f"HTTP_{response.status_code}"


def request(client, method: str, path: str, token: str, *, json_body=None):
    try:
        response = client.request(
            method, path,
            headers={"X-Askliam-Token": token},
            json=json_body,
            timeout=300,
        )
        if response.status_code >= 400:
            raise SchedulerHalt(safe_error(response))
        data = response.json()
        if not isinstance(data, dict):
            raise SchedulerHalt("INVALID_RESPONSE")
        return data
    except (httpx.TransportError, httpx.TimeoutException):
        raise SchedulerHalt("HTTP_TRANSPORT_UNAVAILABLE") from None
    except ValueError:
        raise SchedulerHalt("INVALID_RESPONSE") from None


def assert_state(client, token: str, day: str) -> dict:
    state = request(client, "GET", "/stage0/batch/status", token)
    if state.get("version") != VERSION:
        raise SchedulerHalt("INVALID_ENGINE_VERSION")
    if state.get("utc_date") != day:
        raise SchedulerHalt("UTC_DAY_CHANGED")
    if state.get("pending_reconciliation"):
        raise SchedulerHalt("RECONCILIATION_REQUIRED")
    return state


def crypto_checkpoint(client, token: str, day: str):
    result = request(
        client, "GET", "/stage0/crypto/checkpoint?day=" + day + "&symbol=BTC", token
    )
    if result.get("version") != VERSION or result.get("utc_date") != day:
        raise SchedulerHalt("INVALID_CRYPTO_CHECKPOINT")
    if result.get("pending_reconciliation"):
        raise SchedulerHalt("RECONCILIATION_REQUIRED")
    return result


def run(client, token: str, *, now=None, sleep=time.sleep, max_wall_seconds=MAX_WALL_SECONDS):
    now = now or datetime.now(timezone.utc)
    if not schedule_gate(now):
        return {"version": VERSION, "status": "NOT_SCHEDULED_NEW_YORK_1600",
                "wrote_data": False}
    day = now.astimezone(timezone.utc).date().isoformat()
    run_id = "STAGE0-" + day.replace("-", "") + "-V6.5.1"
    started = time.monotonic()
    result = {
        "version": VERSION,
        "utc_date": day,
        "scheduled_time": "16:00 America/New_York",
        "status": "RUNNING",
        "crypto_verified": [],
        "crypto_previously_verified": [],
        "crypto_spot_previously_verified": [],
        "crypto_provider_deferred": [],
        "fmp_verified": [],
        "fmp_deferred": [],
        "fmp_stop_reason": "",
        "ranking_permitted": False,
        "execution_permitted": False,
    }

    def budget_ok():
        return time.monotonic() - started < max_wall_seconds

    try:
        config = request(client, "GET", "/stage0/auto/config", token)
        if config.get("version") != VERSION:
            raise SchedulerHalt("INVALID_ENGINE_VERSION")
        if not all(config.get(field) is True for field in (
            "stage0_auto_enabled", "crypto_auto_enabled",
            "fmp_stage0_enabled", "redis_configured", "sheets_configured",
        )):
            raise SchedulerHalt("SCHEDULER_CONFIG_INCOMPLETE")
        if config.get("manual_crypto_write_enabled") is not False:
            raise SchedulerHalt("MANUAL_CRYPTO_WRITE_STILL_ENABLED")
        assert_state(client, token, day)
        cp = crypto_checkpoint(client, token, day)
        native_existing = set(cp.get("completed_symbols") or [])
        spot_existing = set(cp.get("spot_completed_symbols") or [])
        if not native_existing.issubset(CRYPTO) or not spot_existing.issubset(CRYPTO):
            raise SchedulerHalt("INVALID_CRYPTO_CHECKPOINT")

        # Native-perpetual history and derivatives context are reference-only.
        # Idempotent daily checkpoints prevent a repeat POST to completed symbols.
        for symbol in CRYPTO:
            if not budget_ok():
                result["status"] = "BUDGET_EXHAUSTED_REMAINING_NEXT_RUN"
                break
            assert_state(client, token, day)
            cp = crypto_checkpoint(client, token, day)
            native_existing = set(cp.get("completed_symbols") or [])
            spot_existing = set(cp.get("spot_completed_symbols") or [])
            if not native_existing.issubset(CRYPTO) or not spot_existing.issubset(CRYPTO):
                raise SchedulerHalt("INVALID_CRYPTO_CHECKPOINT")
            if symbol in native_existing or symbol in spot_existing:
                result["crypto_previously_verified"].append(symbol)
                if symbol in spot_existing:
                    result["crypto_spot_previously_verified"].append(symbol)
                continue
            try:
                commit = request(client, "POST", "/stage0/auto/crypto-step",
                                 token, json_body={"symbol": symbol})
            except SchedulerHalt as exc:
                # Only known pre-write provider deferral can be safely skipped.
                if str(exc) == "PROVIDER_DEFERRED":
                    result["crypto_provider_deferred"].append(symbol)
                    continue
                if str(exc) == "ALREADY_COMMITTED_TODAY":
                    verified = crypto_checkpoint(client, token, day)
                    if symbol not in (verified.get("completed_symbols") or []):
                        raise SchedulerHalt("CRYPTO_CHECKPOINT_MISMATCH") from None
                    result["crypto_previously_verified"].append(symbol)
                    continue
                raise
            if (
                commit.get("version") != VERSION
                or commit.get("canonical_symbol") != symbol
                or commit.get("status") != "COMMITTED_VERIFY_PASS"
                or commit.get("run_id") != run_id
                or commit.get("daily_bars_expected", 0) < 64
                or commit.get("ranking_permitted") is not False
                or commit.get("execution_permitted") is not False
            ):
                raise SchedulerHalt("CRYPTO_COMMIT_NOT_VERIFIED")
            check = crypto_checkpoint(client, token, day)
            if (
                symbol not in (check.get("completed_symbols") or [])
                or check.get("last_verified_symbol") != symbol
                or check.get("last_verified_run_id") != run_id
                or check.get("last_verified_ohlcv_sha256") != commit.get("ohlcv_sha256")
            ):
                # symbol_checkpoint_verified in this query is only for BTC,
                # so check the completed list + latest symbol/hash for all assets.
                raise SchedulerHalt("CRYPTO_CHECKPOINT_MISMATCH")
            result["crypto_verified"].append(symbol)
            sleep(1)

        # FMP: exhaust the time/quota/provider budget, NOT a fixed symbol count.
        # Every step independently captures Sheets, commits, verifies, and
        # advances the FMP-only UTC checkpoint.
        while budget_ok():
            state = assert_state(client, token, day)
            quota = state.get("quota") or {}
            if quota.get("blocked"):
                result["fmp_stop_reason"] = "FMP_DAY_BLOCKED"
                break
            if int(quota.get("remaining", 0)) <= MIN_FMP_QUOTA:
                result["fmp_stop_reason"] = "FMP_QUOTA_RESERVE"
                break
            plan = request(client, "GET", "/stage0/daily/plan", token)
            if plan.get("version") != VERSION or plan.get("utc_date") != day:
                raise SchedulerHalt("FMP_PLAN_MISMATCH")
            if plan.get("pending_reconciliation"):
                raise SchedulerHalt("RECONCILIATION_REQUIRED")
            next_symbols = plan.get("next_symbols") or []
            if not next_symbols:
                result["fmp_stop_reason"] = "FMP_QUEUE_EMPTY"
                break
            expected = next_symbols[0]
            try:
                step = request(client, "POST", "/stage0/daily/step", token)
            except SchedulerHalt as exc:
                # Never retry failed/uncertain FMP writes automatically.
                raise SchedulerHalt("FMP_STEP_HALTED_" + str(exc)) from None
            if step.get("status") == "PROVIDER_DEFERRED":
                if step.get("symbol") != expected or step.get("checkpoint_advanced") is not False:
                    raise SchedulerHalt("FMP_DEFERRED_INCONSISTENT")
                result["fmp_deferred"].append(expected)
            elif step.get("status") == "SOURCE_RETRYABLE":
                result["fmp_stop_reason"] = "FMP_RETRYABLE_SOURCE"
                break
            elif step.get("status") == "FMP_QUEUE_EMPTY":
                result["fmp_stop_reason"] = "FMP_QUEUE_EMPTY"
                break
            elif (
                step.get("daily_symbol") == expected
                and step.get("commit_status") == "COMMITTED"
                and step.get("verify_status") == "PASS"
                and step.get("checkpoint_status") == "COMMITTED_VERIFY_PASS"
            ):
                state = assert_state(client, token, day)
                if expected not in (state.get("checkpoint", {}).get("completed_symbols") or []):
                    raise SchedulerHalt("FMP_CHECKPOINT_MISMATCH")
                result["fmp_verified"].append(expected)
            else:
                raise SchedulerHalt("UNKNOWN_FMP_STEP_RESULT")
            sleep(1)
        else:
            result["fmp_stop_reason"] = "FMP_RUNTIME_BUDGET"

        assert_state(client, token, day)
        crypto_checkpoint(client, token, day)
        # The connected ChatGPT run cannot call protected Redis with a service
        # token. Mirror non-secret final evidence into the workbook after
        # all Stage0 writes. Mirror failures are observable but never mislabel
        # acquisition checkpoints as unverified.
        try:
            mirror = request(client, "POST", "/stage0/auto/runtime-evidence-sync", token)
            result["evidence_sync_status"] = mirror.get("sync_status", "UNKNOWN")
            result["evidence_spot_verified"] = mirror.get("spot_verified", [])
            result["evidence_fmp_used"] = mirror.get("fmp_used")
            result["evidence_fmp_remaining"] = mirror.get("fmp_remaining")
        except SchedulerHalt as exc:
            result["evidence_sync_status"] = "DEFERRED_" + str(exc)
        if result["status"] == "RUNNING":
            result["status"] = "STAGE0_VERIFIED_OR_PROVIDER_DEFERRED"
    except SchedulerHalt as exc:
        result["status"] = "HALTED"
        result["halt_reason"] = str(exc)
    return result


def main() -> int:
    if os.getenv("ASKLIAM_CRON_ENABLED", "").lower() != "true":
        print(json.dumps({"version": VERSION, "status": "CRON_DISARMED", "wrote_data": False}))
        return 0
    token = os.getenv("ASKLIAM_SERVICE_TOKEN", "")
    if not token:
        print(json.dumps({"version": VERSION, "status": "HALTED",
                          "halt_reason": "CRON_SERVICE_TOKEN_MISSING"}))
        return 2
    base = os.getenv("ASKLIAM_BASE_URL", BASE_URL).rstrip("/")
    if base != BASE_URL:
        # Only the deployed trusted service may be targeted.
        print(json.dumps({"version": VERSION, "status": "HALTED",
                          "halt_reason": "UNTRUSTED_BASE_URL"}))
        return 2
    with httpx.Client(base_url=base, follow_redirects=False) as client:
        output = run(client, token)
    print(json.dumps(output, sort_keys=True, separators=(",", ":")))
    return 0 if output["status"] in (
        "STAGE0_VERIFIED_OR_PROVIDER_DEFERRED", "NOT_SCHEDULED_NEW_YORK_1600",
        "BUDGET_EXHAUSTED_REMAINING_NEXT_RUN",
    ) else 1


if __name__ == "__main__":
    sys.exit(main())
