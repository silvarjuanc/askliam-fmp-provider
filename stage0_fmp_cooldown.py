"""V6.5.1: durable, temporary FMP quote HTTP 402 cooldowns.

An upstream 402 is *not* a statement that a symbol is invalid or that no
fallback provider may acquire it. It only throttles further calls to the
same FMP endpoint while an alternative provider is evaluated.
"""
from __future__ import annotations

from datetime import datetime, timezone
import re
import time

FMP_402_KEY = "askliam:v651:stage0:fmp:quote_http402_cooldown"
COOLDOWN_SECONDS = 7 * 86400
COOLDOWN_HASH_TTL_SECONDS = 30 * 86400
FMP_402_REASON = "fmp: FMP_HTTP_402"
_SYMBOL_RE = re.compile(r"^[A-Z0-9^][A-Z0-9^._:/-]{0,39}$")


def parse_hgetall(data):
    """Normalize Upstash JSON replies without silently swallowing corruption."""
    if isinstance(data, list):
        if len(data) % 2:
            raise ValueError("MALFORMED_FMP_COOLDOWN_HASH")
        data = dict(zip(data[::2], data[1::2]))
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError("MALFORMED_FMP_COOLDOWN_HASH")
    return data


def active_cooldowns(data, *, now_epoch=None):
    """Return current blocked symbols only; per-field timestamps expire."""
    now_epoch = int(time.time()) if now_epoch is None else int(now_epoch)
    raw = parse_hgetall(data)
    result = {}
    for symbol, expiry in raw.items():
        if not isinstance(symbol, str) or not _SYMBOL_RE.fullmatch(symbol):
            raise ValueError("INVALID_FMP_COOLDOWN_SYMBOL")
        try:
            ts = int(expiry)
        except (TypeError, ValueError):
            raise ValueError("INVALID_FMP_COOLDOWN_EXPIRY") from None
        if ts > now_epoch:
            result[symbol] = datetime.fromtimestamp(ts, timezone.utc).isoformat()
    return result


def cooldown_expiry(*, now_epoch=None):
    now_epoch = int(time.time()) if now_epoch is None else int(now_epoch)
    return now_epoch + COOLDOWN_SECONDS
