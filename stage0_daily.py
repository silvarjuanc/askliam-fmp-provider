"""Daily AskLiam V6.5.1 coverage inventory and single-asset fire test.

All analysis-enabled assets are inventoried. FMP has a distinct, deliberately
conservative candidate list; unsupported models need their own live adapters.
"""
from __future__ import annotations
from collections import Counter
import json
import os
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

ELIGIBLE_FMP_CLASSES = {"equity", "index", "fixed income etf", "index / etf / basket"}

# Canonical benchmark, explicitly requested symbols and active watchlist US
# large caps take precedence over alphabetic queue order. This affects ONLY
# ingestion order; it never awards scoring points or authorizes trades.
FMP_PRECLOSE_PRIORITY = (
    "SPY", "TSLA", "AAPL", "NVDA", "MSFT", "META", "AMD", "QQQ",
    "AMZN", "GOOGL", "TSM", "PLTR", "COIN", "TLT", "XLE", "IWM",
    "SMH", "EWJ", "EWZ", "EWY", "EWT",
)
FMP_PRIORITY_RANK = {symbol: i for i, symbol in enumerate(FMP_PRECLOSE_PRIORITY)}


def classify(asset_class):
    name = str(asset_class or "").strip().lower()
    if name in ELIGIBLE_FMP_CLASSES:
        return "FMP_CANDIDATE"
    if name == "crypto":
        return "CRYPTO_PROVIDER_PENDING"
    if name in ("fx", "fx / macro"):
        return "FX_MACRO_PROVIDER_PENDING"
    if name in ("commodity",):
        return "COMMODITY_PROVIDER_PENDING"
    if name in ("rates / fixed income",):
        return "OFFICIAL_RATES_PROVIDER_PENDING"
    return "ALTERNATIVE_PROVIDER_PENDING"

def read_universe():
    reader = Stage0_Daily_Ingestor(providers={"noop": lambda: {}})
    book = reader._open()
    raw = book.worksheet("Universe").get_all_records(numericise_ignore=["all"])
    entries = []
    seen = set()
    for row in raw:
        if str(row.get("Analysis Enabled", "")).strip().upper() != "TRUE":
            continue
        symbol = str(row.get("Symbol", "")).strip().upper()
        if not symbol:
            raise Stage0Error("SCHEMA_FAILED", "Enabled asset has no symbol")
        if symbol in seen:
            raise Stage0Error("DUPLICATE_KEY", "Duplicate enabled symbol in Universe")
        seen.add(symbol)
        asset_class = str(row.get("Asset Class", "")).strip()
        entries.append({"symbol": symbol, "asset_class": asset_class,
                        "route": classify(asset_class)})
    return sorted(entries, key=lambda item: item["symbol"])

UNIVERSE_CACHE_KEY = "askliam:v651:stage0:daily_universe"
UNIVERSE_CACHE_TTL_SECONDS = 300

async def read_universe_cached(redis, *, reader=None):
    """Reduce Google Sheets reads during step loops; strict five-minute TTL."""
    import asyncio
    if reader is None:
        reader = read_universe
    cached = await redis.call("GET", UNIVERSE_CACHE_KEY)
    if cached:
        try:
            data = json.loads(cached)
            if isinstance(data, list) and data and all(
                isinstance(item, dict)
                and isinstance(item.get("symbol"), str)
                and isinstance(item.get("route"), str)
                for item in data
            ):
                return data
        except (ValueError, TypeError):
            pass
    entries = await asyncio.to_thread(reader)
    if not entries:
        raise Stage0Error("SCHEMA_FAILED", "Enabled Universe unexpectedly empty")
    await redis.call(
        "SET", UNIVERSE_CACHE_KEY, json.dumps(entries, separators=(",", ":")),
        "EX", UNIVERSE_CACHE_TTL_SECONDS,
    )
    return entries

def plan_from_universe(entries, status, *, batch_size=1):
    completed = set(status["checkpoint"].get("completed_symbols") or [])
    deferred = set(status.get("deferred_today") or {})
    blocked_402 = set(status.get("fmp_quote_402_cooldowns") or {})
    temporarily_held = set(status.get("transient_prewrite_today") or {})
    # Operator-scoped 2026-10-14 quarantine ONLY for GE: a provider/workbook
    # step halted with HTTP 503 and left a PREPARED journal. The journal was
    # separately reconciled with 300 immutable legacy rows and no same-day
    # snapshot. Do not let a repeated transient GE error starve 80+ others.
    # This is NOT a provider deferral nor a valid completed checkpoint.
    held = ({"GE"} if (
        os.getenv("ASKLIAM_STAGE0_QUARANTINE_GE_UTC_DAY") == "2026-10-14"
        and status.get("utc_date") == "2026-10-14"
    ) else set())
    counters = Counter(item["route"] for item in entries)
    symbols_by_route = {route: [item["symbol"] for item in entries if item["route"] == route] for route in sorted(counters)}
    eligible = [item["symbol"] for item in entries if item["route"] == "FMP_CANDIDATE"]
    unattempted = [symbol for symbol in eligible if symbol not in completed and symbol not in deferred]
    cooldown_skipped = [symbol for symbol in unattempted if symbol in blocked_402]
    pending = [symbol for symbol in unattempted if symbol not in blocked_402
               and symbol not in held and symbol not in temporarily_held]
    # Repeated Stage0 steps consume a verified Redis checkpoint. The order
    # MUST remain deterministic even if Universe/Sheets change row order.
    pending.sort(key=lambda s: (FMP_PRIORITY_RANK.get(s, len(FMP_PRIORITY_RANK)), s))
    return {
        "version": "V6.5.1",
        "utc_date": status["utc_date"],
        "universe_enabled_count": len(entries),
        "by_provider_route": dict(sorted(counters.items())),
        "symbols_by_route": symbols_by_route,
        "fmp_candidate_count": len(eligible),
        "fmp_completed_today": len(set(eligible) & completed),
        "fmp_deferred_today": len(set(eligible) & deferred),
        "fmp_remaining_today": len(pending),
        "fmp_unattempted_today_including_cooldowns": len(unattempted),
        "fmp_http402_cooldown_count": len(set(eligible) & blocked_402),
        "fmp_http402_skipped_today": len(cooldown_skipped),
        "fmp_transient_prewrite_held_symbols": sorted(
            temporarily_held & set(unattempted)
        ),
        "fmp_transient_prewrite_held_count": len(temporarily_held & set(unattempted)),
        "fmp_http402_skipped_symbols": cooldown_skipped,
        "fmp_operationally_held_symbols": sorted(held & set(unattempted)),
        "fmp_operational_hold_reason": ("GE_PRIOR_PREWRITE_503_RECONCILED"
                                        if held & set(unattempted) else "NONE"),
        "fmp_cooldowns_expire_at_utc": {
            sym: status["fmp_quote_402_cooldowns"][sym]
            for sym in cooldown_skipped
        },
        "unsupported_today": len(entries) - len(eligible),
        "next_symbols": pending[:max(1, min(batch_size, 5))],
        "quota": status["quota"],
        "pending_reconciliation": status["pending_reconciliation"],
        "ranking_permitted": False,
        "acquisition_only": True,
        "note": "FMP HTTP 402 cooldown only suppresses repeated FMP quote requests; alternatives remain eligible. Classification does not prove provider entitlement.",
    }
