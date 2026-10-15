"""Repeated FMP TTM 402 errors should not burn the whole daily quota."""
import os
import unittest
from unittest.mock import patch
from app import Provider,FMP_FUNDAMENTAL_402_BLOCKED,FMP_FUNDAMENTAL_ENTITLEMENT_TTL

class DummyResponse:
    status_code=402

class DummyClient:
    def __init__(self):self.calls=0
    async def get(self,*args,**kwargs):
        self.calls+=1
        return DummyResponse()

class DummyState:
    def __init__(self):
        self.observed=set()
        self.blocked=False
        self.reservations=0
    async def cache_get(self,key):return None
    async def reserve(self):
        self.reservations+=1
        return True
    async def call(self,*args):
        if args[0]=="GET" and args[1]==FMP_FUNDAMENTAL_402_BLOCKED:
            return "HTTP_402" if self.blocked else None
        raise AssertionError(args)
    async def eval(self,script,keys,args):
        self.observed.add(args[0])
        if len(self.observed)>=2:self.blocked=True
        return len(self.observed)

class TtmEntitlementTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_distinct_402_symbols_disable_ratios_only(self):
        p=Provider()
        p.state=DummyState()
        p.client=DummyClient()
        with patch.dict(os.environ,{"FMP_API_KEY":"test-fixture"}):
            a=await p.get("fundamental","AAPL")
            b=await p.get("fundamental","MSFT")
            c=await p.get("fundamental","TSLA")
        self.assertEqual(a["error"],"FMP_HTTP_402")
        self.assertEqual(b["error"],"FMP_HTTP_402")
        self.assertEqual(c["error"],"FMP_TTM_ENDPOINT_ENTITLEMENT_402")
        self.assertEqual(p.state.reservations,2)
        self.assertEqual(p.client.calls,2)

    async def test_same_symbol_402_twice_cannot_trigger_global_entitlement(self):
        p=Provider()
        p.state=DummyState()
        p.client=DummyClient()
        with patch.dict(os.environ,{"FMP_API_KEY":"test-fixture"}):
            await p.get("fundamental","AAPL")
            await p.get("fundamental","AAPL")
        self.assertFalse(p.state.blocked)
        self.assertEqual(p.state.observed,{"AAPL"})
        self.assertEqual(p.client.calls,2)

if __name__=="__main__":
    unittest.main()
