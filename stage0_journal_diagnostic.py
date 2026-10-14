"""V6.5.1 non-secret read-only Redis journal inspection; cannot alter any key."""
import json

from stage0_batch import pending_key


async def summarize_pending_journal(redis):
    raw = await redis.call("GET", pending_key(None))
    if not raw:
        return {"version": "V6.5.1", "pending": False, "status": "NO_PENDING"}
    try:
        journal = json.loads(raw)
    except (ValueError, TypeError):
        return {"version": "V6.5.1", "pending": True, "status": "CORRUPT_JOURNAL"}
    if not isinstance(journal, dict):
        return {"version": "V6.5.1", "pending": True, "status": "INVALID_JOURNAL_TYPE"}
    symbols = journal.get("symbols")
    if not isinstance(symbols, list) or len(symbols) > 5 or not all(
        isinstance(s, str) and s.isascii() and 1 <= len(s) <= 40
        and all(ch.isalnum() or ch in ".^_/-" for ch in s)
        for s in symbols
    ):
        symbols = ["INVALID_SYMBOL_SCOPE"]
    return {
        "version": "V6.5.1", "pending": True,
        "status": str(journal.get("status")) if journal.get("status") in
                  ("PREPARED", "COMMITTING", "POST_WRITE", "VERIFIED") else "UNKNOWN_STAGE",
        "symbols": symbols,
        "run_id": (journal.get("run_id") if isinstance(journal.get("run_id"), str)
                   and len(journal.get("run_id")) <= 75 else "UNKNOWN_RUN"),
        "created_at_utc": (journal.get("created_at") if isinstance(journal.get("created_at"),str)
                           and len(journal.get("created_at"))<=45 else None),
    }
