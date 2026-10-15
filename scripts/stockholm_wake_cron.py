"""Secret-isolated Render wake Cron for AskLiam V6.5.1.

Every ten minutes in candidate UTC hours, try the authenticated wake route.
The receiver enforces local Europe/Stockholm clock, Redis one-shot claim,
all existing Stage0 writer gates, and 16:00 Sweden hard cutoff.
This cron has no Google/FMP/Hyperliquid/portfolio credentials.
"""
from __future__ import annotations
import json
import os
import sys
import urllib.error
import urllib.request

BASE="https://askliam-fmp-provider.onrender.com/stage0/scheduler/wake"
ALLOWED={"PHASE_ACCEPTED","PHASE_ALREADY_CLAIMED",
         "NOT_SCHEDULED_STOCKHOLM_PREANALYSIS"}


def fire(url=BASE, token=None, opener=urllib.request.urlopen):
    if url!=BASE:
        return {"status":"UNTRUSTED_WAKE_DESTINATION"}
    token=token if token is not None else os.environ.get("ASKLIAM_WAKE_TOKEN","")
    if not token:
        return {"status":"WAKE_SECRET_MISSING"}
    req=urllib.request.Request(
        url,data=b"{}",method="POST",
        headers={"X-Askliam-Wake-Token":token,"Content-Type":"application/json"},
    )
    try:
        with opener(req,timeout=130) as response:
            doc=json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError,TimeoutError,ValueError,TypeError) as e:
        return {"status":"WAKE_TRANSPORT_FAILED","error_class":type(e).__name__}
    if not isinstance(doc,dict):
        return {"status":"WAKE_INVALID_RESPONSE"}
    prior=doc.get("prior_result") or {}
    safe_status=str(prior.get("status","NONE"))
    safe_reason=str(prior.get("halt_reason","NONE"))
    import re
    if not re.fullmatch(r"[A-Z0-9_]{1,96}",safe_status):
        safe_status="REDACTED"
    if not re.fullmatch(r"[A-Z0-9_]{1,96}",safe_reason):
        safe_reason="REDACTED"
    return {"status":doc.get("status","UNKNOWN"),
            "phase":doc.get("phase"),
            "accepted":doc.get("accepted",False),
            "prior_status":safe_status,"prior_halt_reason":safe_reason}


if __name__=="__main__":
    result=fire()
    print(json.dumps(result,separators=(",",":")))
    sys.exit(0 if result["status"] in ALLOWED else 1)
