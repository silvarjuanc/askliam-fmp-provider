"""V6.5.1 authenticated-runtime FMP domain entitlement check.

Probes only named allowlisted symbols, up to three FMP domains each,
cache first via Provider.get. No credentials, prices or data payloads in
returned/logged output. No Google Sheets, Ranking, Lifecycle or trades write.
This is diagnostics, NOT ingestion / COMMITTED evidence.
"""
from __future__ import annotations

from datetime import datetime, timezone

SYMBOLS = ("SPY", "TSLA", "AAPL")
DOMAINS = ("quote", "historical", "profile")
VERSION = "V6.5.1"
ALLOWED_ERRORS = frozenset({
    "FMP_HTTP_400", "FMP_HTTP_401", "FMP_HTTP_402",
    "FMP_HTTP_403_KEY_OR_PLAN", "FMP_HTTP_404",
    "FMP_HTTP_408", "FMP_HTTP_429", "FMP_HTTP_500",
    "FMP_HTTP_502", "FMP_HTTP_503", "FMP_HTTP_504",
    "FMP_QUOTA_EXHAUSTED", "REDIS_UNAVAILABLE",
    "REDIS_RESERVATION_FAILED", "PERSISTENT_STATE_UNAVAILABLE",
    "MISSING_FMP_API_KEY", "FMP_NETWORK_ERROR",
    "FMP_EMPTY_OR_ERROR", "PROVIDER_BLOCKED",
    "INVALID_JSON", "REDIS_CACHE_WRITE_FAILED",
})


async def probe_preclose_entitlements(provider):
    if provider.state is None:
        return {"version": VERSION, "status": "UPSTASH_UNAVAILABLE",
                "read_only": True, "sheets_writes": 0}
    quota_before = await provider.state.quota()
    # Bounded network budget <= nine calls; keep 40 as an independent
    # scheduler safety reserve. Historical 402 doesn't imply quote 402.
    if quota_before.get("blocked") or int(quota_before.get("remaining", 0)) < 50:
        return {"version": VERSION, "status": "QUOTA_GUARD",
                "read_only": True, "sheets_writes": 0}
    results = []
    for symbol in SYMBOLS:
        domain_states = {}
        for domain in DOMAINS:
            output = await provider.get(domain, symbol)
            if output.get("status") == "READY":
                domain_states[domain] = {
                    "status": "AVAILABLE",
                    "cache_hit": bool(output.get("cache_hit", False)),
                }
            else:
                raw_code = str(output.get("error") or "")
                safe_code = raw_code if raw_code in ALLOWED_ERRORS else "PROVIDER_ERROR"
                domain_states[domain] = {
                    "status": "UNAVAILABLE", "reason": safe_code,
                }
                # Stop on a hard FMP rate-limit, not on an entitlement 402:
                # quote/profile/history permission is independent.
                if safe_code in (
                    "FMP_HTTP_429", "FMP_QUOTA_EXHAUSTED",
                    "REDIS_UNAVAILABLE", "REDIS_RESERVATION_FAILED",
                ):
                    break
        results.append({"symbol": symbol, "domains": domain_states})
        if any(
            domain.get("reason") in (
                "FMP_HTTP_429", "FMP_QUOTA_EXHAUSTED",
                "REDIS_UNAVAILABLE", "REDIS_RESERVATION_FAILED",
            )
            for domain in domain_states.values()
        ):
            break
    quota_after = await provider.state.quota()
    return {
        "version": VERSION,
        "status": "SOURCE_ENTITLEMENT_AUDIT_COMPLETED",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbols": results,
        "quota_used_before": quota_before.get("used"),
        "quota_remaining_before": quota_before.get("remaining"),
        "quota_used_after": quota_after.get("used"),
        "quota_remaining_after": quota_after.get("remaining"),
        "read_only": True,
        "sheets_writes": 0,
        "checkpoint_advanced": False,
        "ranking_permitted": False,
        "execution_permitted": False,
    }
