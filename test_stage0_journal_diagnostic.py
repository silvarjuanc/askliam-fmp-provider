import asyncio
import json
import unittest
from stage0_journal_diagnostic import summarize_pending_journal

class StubRedis:
    def __init__(self,v):self.v=v;self.calls=[]
    async def call(self,*args):self.calls.append(args);return self.v

class JournalSummaryTest(unittest.TestCase):
    def test_no_pending(self):
        x=StubRedis(None)
        result=asyncio.run(summarize_pending_journal(x))
        self.assertFalse(result["pending"])
        self.assertEqual(len(x.calls),1)
        self.assertEqual(x.calls[0][0],"GET")

    def test_pending_scope_no_token_no_raw_payload(self):
        x=StubRedis(json.dumps({"status":"PREPARED","symbols":["CIEN"],
          "run_id":"STAGE0-20261014-V6.5.1",
          "created_at":"2026-10-14T12:12:14+00:00",
          "secret_token":"supersecret","cache":"private"}))
        result=asyncio.run(summarize_pending_journal(x))
        self.assertEqual(result["symbols"],["CIEN"])
        self.assertEqual(result["status"],"PREPARED")
        self.assertNotIn("secret",json.dumps(result))
        self.assertEqual(len(x.calls),1)

    def test_corrupt_fails_closed(self):
        x=StubRedis("{bad")
        result=asyncio.run(summarize_pending_journal(x))
        self.assertEqual(result["status"],"CORRUPT_JOURNAL")
        self.assertTrue(result["pending"])
        self.assertEqual(len(x.calls),1)

    def test_invalid_symbol_not_reexposed(self):
        x=StubRedis(json.dumps({"status":"PREPARED",
              "symbols":["\nAUTH_PRIVATE_123:abc"]}))
        result=asyncio.run(summarize_pending_journal(x))
        self.assertEqual(result["symbols"],["INVALID_SYMBOL_SCOPE"])

if __name__=="__main__":unittest.main()
