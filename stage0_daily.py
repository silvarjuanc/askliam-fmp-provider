"""Daily AskLiam V6.5.1 coverage inventory and single-asset fire test.

All analysis-enabled assets are inventoried. FMP has a distinct, deliberately
conservative candidate list; unsupported models need their own live adapters.
"""
from __future__ import annotations
from collections import Counter
from stage0_ingestor import Stage0_Daily_Ingestor, Stage0Error

ELIGIBLE_FMP_CLASSES = {"equity", "index", "fixed income etf", "index / etf / basket"}

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

def plan_from_universe(entries, status, *, batch_size=1):
    completed = set(status["checkpoint"].get("completed_symbols") or [])
    deferred = set(status.get("deferred_today") or {})
    counters = Counter(item["route"] for item in entries)
    symbols_by_route = {route: [item["symbol"] for item in entries if item["route"] == route] for route in sorted(counters)}
    eligible = [item["symbol"] for item in entries if item["route"] == "FMP_CANDIDATE"]
    pending = [symbol for symbol in eligible if symbol not in completed and symbol not in deferred]
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
        "unsupported_today": len(entries) - len(eligible),
        "next_symbols": pending[:max(1, min(batch_size, 5))],
        "quota": status["quota"],
        "pending_reconciliation": status["pending_reconciliation"],
        "ranking_permitted": False,
        "acquisition_only": True,
        "note": "Route classification is not provider entitlement verification.",
    }
