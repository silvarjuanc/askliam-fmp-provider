"""Conservative NY-session completion guard for FMP US equity/index bars.

Provider-supplied EOD histories sometimes expose bars for the current session
before volume is final.  Never treat those provisional rows as a settled daily
exchange volume observation.  The 18:00 NY buffer is deliberately conservative
but NOT an exchange/issuer finality certificate.

No market-calendar inference: holiday/half-day bars remain provider-qualified.
"""
from __future__ import annotations

from datetime import datetime, date, time, timezone
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
SETTLEMENT_BUFFER_HOUR_NY = 18


def provider_eod_date_guard(trading_date: str, captured_at: datetime) -> bool:
    if not isinstance(captured_at, datetime) or captured_at.tzinfo is None:
        raise ValueError("FMP_CLOCK_NOT_AWARE")
    try:
        day=date.fromisoformat(trading_date)
        if day.isoformat()!=trading_date:
            raise ValueError()
    except (ValueError, TypeError):
        raise ValueError("FMP_TRADING_DATE_INVALID") from None
    captured_ny=captured_at.astimezone(NY)
    stable_after=datetime.combine(
        day,time(hour=SETTLEMENT_BUFFER_HOUR_NY),tzinfo=NY,
    )
    return captured_ny >= stable_after


def classify_stored_fmp_bar(row: dict) -> dict:
    """Flag suspected premature daily US bars; NEVER silently repair history."""
    src=str(row.get("source","")).upper()
    if src!="FMP":
        return {"state":"NOT_FMP"}
    when=str(row.get("trading_date",""))
    timestamp=str(row.get("fetched_at_utc","")).replace("Z","+00:00")
    try:
        fetched=datetime.fromisoformat(timestamp)
        valid=provider_eod_date_guard(when,fetched)
    except (ValueError, TypeError):
        return {"state":"INVALID_DATE_OR_PROVENANCE"}
    return {"state":"ACQUIRED_BEFORE_EOD_BUFFER" if not valid
            else "ACQUIRED_AFTER_EOD_BUFFER_UNCONFIRMED",
            "trading_date":when,"canonical_symbol":row.get("canonical_symbol"),
            "upsert_key":row.get("upsert_key")}


def audit_fmp_history_quality(rows: list[dict]) -> dict:
    issues=[]
    examined=0
    for r in rows:
        if str(r.get("source","")).upper()!="FMP":
            continue
        examined+=1
        status=classify_stored_fmp_bar(r)
        if status["state"]!="ACQUIRED_AFTER_EOD_BUFFER_UNCONFIRMED":
            issues.append(status)
    return {
        "version":"V6.5.1",
        "source":"FMP",
        "checked_rows":examined,
        "premature_or_invalid_rows":len(issues),
        "issues":issues[:150],
        "truncated":len(issues)>150,
        "reader_only":True,
        "sheet_writes_performed":False,
        "ranking_permitted":False,
    }


def audit_fmp_history_sync():
    """Inspect real Raw_OHLCV without updating/delete/backfilling any row."""
    from stage0_ingestor import Stage0_Daily_Ingestor
    reader=Stage0_Daily_Ingestor(providers={"read_only":lambda:{}})
    book=reader._open()
    records=book.worksheet("Raw_OHLCV").get_all_records(numericise_ignore=["all"])
    return audit_fmp_history_quality(records)
