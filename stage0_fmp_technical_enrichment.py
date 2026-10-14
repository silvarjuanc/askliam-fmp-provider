"""Source-bound FMP EOD technical enrichment for ASKLIAM V6.5.1.

No fundamental or macro proxies invented. A complete FMP EOD response yields
technicals and (when volume and a current-session SPY benchmark are present)
volume/relative-strength scores. It does not grant Broad Scan or trading.
"""
from datetime import datetime, timezone
from math import isfinite
from stage0_canonical_factors import canonical_technicals, relative_strength, volume_score
from stage0_equity_session_gate import partition_confirmed_bars

class TechnicalEvidenceError(ValueError):
    pass


def derived_fmp_eod_factors(history, observed_at, *, benchmark_return_3m=None,
                            benchmark_last_eod=None):
    if not isinstance(history, list):
        raise TechnicalEvidenceError("HISTORY_NOT_LIST")
    if observed_at.tzinfo is None:
        raise TechnicalEvidenceError("NAIVE_OBSERVATION")
    try:
        good, dropped = partition_confirmed_bars(history, observed_at)
        by_day = {}
        for item in good:
            date = str(item["date"])[:10]
            if date in by_day:
                raise TechnicalEvidenceError("DUPLICATE_EOD_SESSION")
            o,h,l,c = [float(item[key]) for key in ("open","high","low","close")]
            if (not all(isfinite(x) for x in (o,h,l,c))
                    or min(o,h,l,c)<=0 or h<max(o,c,l) or l>min(o,c,h)):
                raise TechnicalEvidenceError("INVALID_OHLC")
            by_day[date]=item
        rows=[by_day[k] for k in sorted(by_day)]
        if len(rows)<253:
            raise TechnicalEvidenceError("EOD_HISTORY_UNDER_253")
        days=[str(row["date"])[:10] for row in rows]
        closes=[float(x["close"]) for x in rows]
        computed=canonical_technicals(closes)
        result = {
            **computed,
            "last_eod_date": days[-1],
            "validated_eod_rows": len(rows),
            "excluded_partial_eod_rows": len(dropped),
            "technical_provenance": "FMP_STABLE_EOD_SCHEMA_VALIDATED",
            "source_independently_crosschecked": False,
        }
        if all(item.get("volume") is not None for item in rows[-21:]):
            values=[float(x["volume"]) for x in rows[-21:]]
            if all(isfinite(v) and v>=0 for v in values):
                vol=volume_score(values[-1],sum(values[:-1])/20,source_verified=True)
                if vol is not None:result["volume_score"]=vol
        if benchmark_return_3m is not None and benchmark_last_eod == days[-1]:
            result["relative_strength_score"]=relative_strength(
                computed["return_3m"],benchmark_return_3m,
            )
        return result
    except TechnicalEvidenceError:
        raise
    except (KeyError,ValueError,TypeError,OverflowError) as exc:
        raise TechnicalEvidenceError("EOD_TECHNICAL_COMPUTATION_INVALID") from None
