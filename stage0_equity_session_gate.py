"""Reject incomplete US market-session FMP daily candles before persistence.

Use NY civil time and a delay after cash-market close. This is intentionally
conservative and never asserts an exchange holiday or special close.
"""
from datetime import datetime, date, time, timezone, timedelta
from zoneinfo import ZoneInfo

NY=ZoneInfo("America/New_York")

def closed_us_equity_session(candle_day, observed_at):
    if observed_at.tzinfo is None: raise ValueError("NAIVE_SOURCE_TIME")
    session=date.fromisoformat(str(candle_day)[:10])
    captured=observed_at.astimezone(NY)
    if session>captured.date(): return False
    if session==captured.date():
        if captured.weekday()>=5: return False
        return captured.time()>=time(16,20)
    # Prior date is historical, but further provider validation is still needed.
    return True

def partition_confirmed_bars(candles, observed_at):
    accepted=[]; rejected=[]
    for bar in candles:
        day=bar.get("date") if isinstance(bar,dict) else None
        if not day: raise ValueError("MISSING_CANDLE_DATE")
        if closed_us_equity_session(day,observed_at):
            accepted.append(bar)
        else:
            rejected.append({"trading_date":str(day)[:10],
                             "reason":"CURRENT_SESSION_UNCONFIRMED"})
    return accepted,rejected
