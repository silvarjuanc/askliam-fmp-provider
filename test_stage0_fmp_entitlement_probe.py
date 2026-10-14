import unittest
from stage0_fmp_entitlement_probe import probe_preclose_entitlements


class Redis:
    def __init__(self, remaining=240, blocked=False):
        self.remaining=remaining
        self.blocked=blocked
    async def quota(self):
        return {"used": 240-self.remaining, "remaining": self.remaining,
                "blocked":self.blocked}


class Provider:
    def __init__(self, domains=None, remaining=240):
        self.state=Redis(remaining)
        self.domains=domains or {}
        self.calls=[]
    async def get(self,kind,symbol):
        self.calls.append((kind,symbol))
        self.state.remaining-=1
        return self.domains.get((kind,symbol),
                                {"status":"READY","cache_hit":False})


class Tests(unittest.IsolatedAsyncioTestCase):
    async def test_all_domains_probe_independently(self):
        p=Provider(domains={("quote","SPY"): {
            "status":"PROVIDER_DEFERRED","error":"FMP_HTTP_402"
        }})
        r=await probe_preclose_entitlements(p)
        self.assertEqual(r["status"],"SOURCE_ENTITLEMENT_AUDIT_COMPLETED")
        self.assertEqual(r["symbols"][0]["domains"]["quote"]["reason"],"FMP_HTTP_402")
        self.assertEqual(r["symbols"][0]["domains"]["historical"]["status"],"AVAILABLE")
        self.assertEqual(r["symbols"][0]["domains"]["profile"]["status"],"AVAILABLE")
        self.assertEqual(len(p.calls),9)
        self.assertEqual(r["quota_used_after"],9)
        self.assertFalse(r["ranking_permitted"])
        self.assertEqual(r["sheets_writes"],0)

    async def test_untrusted_error_is_sanitized(self):
        p=Provider(domains={("quote","TSLA"):{
            "status":"PROVIDER_DEFERRED",
            "error":"SECRET_SENSITIVE_PROVIDER_DATA"
        }})
        r=await probe_preclose_entitlements(p)
        self.assertEqual(r["symbols"][1]["domains"]["quote"]["reason"],
                         "PROVIDER_ERROR")
        self.assertNotIn("SECRET",str(r))

    async def test_quota_reserve_fails_closed(self):
        p=Provider(remaining=49)
        r=await probe_preclose_entitlements(p)
        self.assertEqual(r["status"],"QUOTA_GUARD")
        self.assertEqual(p.calls,[])

    async def test_rate_limiter_stops(self):
        p=Provider(domains={("historical","SPY"):{
            "status":"PROVIDER_DEFERRED","error":"FMP_HTTP_429"
        }})
        r=await probe_preclose_entitlements(p)
        self.assertEqual(len(p.calls),2)
        self.assertEqual(len(r["symbols"]),1)

    async def test_no_redis_fails_closed(self):
        p=Provider()
        p.state=None
        r=await probe_preclose_entitlements(p)
        self.assertEqual(r["status"],"UPSTASH_UNAVAILABLE")
        self.assertEqual(p.calls,[])


if __name__=="__main__":
    unittest.main()
