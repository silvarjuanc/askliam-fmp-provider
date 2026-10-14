import unittest
from datetime import date, datetime, timedelta, timezone
from stage1_sheet_reader import load_and_preview

CURRENT=datetime(2026,10,14,15,20,tzinfo=timezone.utc)

class Worksheet:
    def __init__(self, rows=None, records=None):
        self.rows=rows or []
        self.records=records or []
        self.row_count=len(self.rows)+3
        self.calls=[]
    def get_all_records(self, **kwargs):
        self.calls.append(("get_all_records",))
        return self.records
    def batch_get(self, ranges):
        self.calls.append(("batch_get",tuple(ranges)))
        if ranges[0].startswith("B2:B"):
            return [[[x["canonical_symbol"]] for x in self.rows],
                    [[x["source"]] for x in self.rows]]
        # Position 2-based; each row has dates G and quality O.
        result=[]
        for rg in ranges:
            a,b=rg.replace("G","").replace("O","").split(":")
            first=int(a)-2
            last=int(b)-2
            result.append([[item.get(k,"") for k in (
                "trading_date","open","high","low","close","volume",
                "fetched_at_utc","upsert_key","quality_status"
            )] for item in self.rows[first:last+1]])
        return result

class Book:
    def __init__(self,tabs):
        self.tabs=tabs
    def worksheet(self,name):
        return self.tabs[name]


class ReaderTests(unittest.TestCase):
    def test_source_key_scan_only_and_no_sheet_writes(self):
        dates=[]
        d=date(2026,10,13)
        while len(dates)<260:
            if d.weekday()<5:
                dates.append(d)
            d-=timedelta(days=1)
        raw=[{"canonical_symbol":"SPY","source":"FMP",
              "trading_date":day.isoformat(),
              "open":"100","high":"103","low":"99","close":"101",
              "volume":"10000","quality_status":"RECEIVED_UNVERIFIED"}
             for day in reversed(dates)]
        tabs={
            "Raw_OHLCV":Worksheet(rows=raw),
            "Market_Data_Snapshot":Worksheet(records=[{
                "ranking_run_id":"STAGE0-20261014-V6.5.1",
                "canonical_symbol":"SPY","score_model":"INDEX_V1",
                "last_price":"101","data_timestamp":"2026-10-14T15:19:00Z"
            }]),
            "Data_Requirements":Worksheet(records=[{
                "Model":"INDEX_V1","Broad Min %":"70",
                "Broad Mandatory Data":"last_price; return_1m; return_3m; volatility_20d; momentum_score; trend_score; macro_score; breadth_score_or_proxy"
            }]),
        }
        result=load_and_preview(["SPY"],today=CURRENT,opener=lambda:Book(tabs))
        self.assertEqual(result["checked_symbols"],1)
        self.assertEqual(result["source_rows_examined"],260)
        self.assertEqual(len(tabs["Raw_OHLCV"].calls),2)
        self.assertEqual(result["results"][0]["history_observations"],260)
        self.assertIn("macro_score",result["results"][0]["mandatory_missing"])
        self.assertFalse(result["ranking_permitted"])
        self.assertFalse(result["sheet_writes_performed"])

    def test_daily_audit_loader_does_not_read_histories_or_write(self):
        from stage1_sheet_reader import load_daily_audit
        tabs={
            "Universe":Worksheet(records=[
                {"Symbol":"SPY","Asset Class":"Index",
                 "Analysis Enabled":"TRUE","Ranking Enabled":"TRUE"},
                {"Symbol":"JPM","Asset Class":"Equity",
                 "Analysis Enabled":"TRUE","Ranking Enabled":"FALSE"},
            ]),
            "Data_Requirements":Worksheet(records=[
                {"Model":"INDEX_V1","Broad Min %":"70",
                 "Broad Mandatory Data":"last_price; macro_score"},
                {"Model":"EQUITY_V2","Broad Min %":"70",
                 "Broad Mandatory Data":"last_price; fundamental_factor_count>=1"},
            ]),
            "Market_Data_Snapshot":Worksheet(records=[
                {"ranking_run_id":"STAGE0-20261014-V6.5.1",
                 "canonical_symbol":"SPY","score_model":"INDEX_V1","last_price":"101",
                 "data_timestamp":"2026-10-14T15:19:00Z"},
                {"ranking_run_id":"STAGE0-20261014-V6.5.1",
                 "canonical_symbol":"JPM","score_model":"EQUITY_V2","last_price":"120",
                 "data_timestamp":"2026-10-14T15:19:00Z"},
            ]),
            "Raw_OHLCV":Worksheet(rows=[{"canonical_symbol":"SPY","source":"FMP"}]),
        }
        result=load_daily_audit(
            day="2026-10-14",now=CURRENT,opener=lambda:Book(tabs))
        self.assertEqual(result["stage0_snapshots"],2)
        self.assertEqual(result["stage0_eligible_acquired"],1)
        self.assertEqual(result["excluded_symbols"],["JPM"])
        self.assertEqual(tabs["Raw_OHLCV"].calls,[])
        self.assertFalse(result["sheet_writes_performed"])

if __name__=="__main__":
    unittest.main()
