"""Bounded Google Sheets reader for Stage1 read-only diagnostic.

Reads 2 key columns once, then only needed contiguous OHLCV cells for up
to 10 requested assets. No Stage0._capture full-sheet rewrite/readback and
no API-based quote refresh. In-memory data are discarded after each call.
"""
from __future__ import annotations
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from stage0_ingestor import Stage0_Daily_Ingestor
from stage1_history_reuse import SOURCES, preview

RAW_FIELDS = ("trading_date","open","high","low","close","volume","fetched_at_utc",
              "upsert_key","quality_status")
PER_SOURCE_LIMIT = 365
MAX_GROUPS = 100


def _cell(rows, index):
    if index >= len(rows) or not rows[index]:
        return ""
    return str(rows[index][0]).strip()


def contiguous_runs(entries):
    """(row_number, symbol, source) -> minimal exact reads, no empty ranges."""
    if not entries:
        return []
    runs=[]
    entries=sorted(entries,key=lambda v:v[0])
    current=[entries[0]]
    for item in entries[1:]:
        if item[0]==current[-1][0]+1:
            current.append(item)
        else:
            runs.append(current)
            current=[item]
    runs.append(current)
    return runs


def load_and_preview(symbols, *, today=None, opener=None):
    today=today or datetime.now(timezone.utc)
    if opener is None:
        reader=Stage0_Daily_Ingestor(providers={"read_only":lambda:{}})
        book=reader._open()
    else:
        book=opener()
    required=book.worksheet("Data_Requirements").get_all_records(numericise_ignore=["all"])
    snapshots=book.worksheet("Market_Data_Snapshot").get_all_records(numericise_ignore=["all"])
    current="STAGE0-"+today.strftime("%Y%m%d")+"-V6.5.1"
    chosen={r.get("canonical_symbol"):r.get("score_model") for r in snapshots
            if r.get("ranking_run_id")==current and r.get("canonical_symbol") in symbols}
    allowed={symbol:SOURCES.get(model,set()) for symbol,model in chosen.items()}
    ws=book.worksheet("Raw_OHLCV")
    height=ws.row_count
    if height<2:
        raise ValueError("RAW_OHLCV_EMPTY")
    # Key-index pass is ~2x row count rather than repeatedly downloading
    # 16 columns and calculating full-workbook fingerprints for every asset.
    key_columns=ws.batch_get([f"B2:B{height}",f"E2:E{height}"])
    symbols_col,sources_col=(list(x) for x in key_columns)
    candidates=defaultdict(list)
    for index in range(max(len(symbols_col),len(sources_col))):
        symbol=_cell(symbols_col,index).upper()
        source=_cell(sources_col,index).upper()
        if symbol in allowed and source in allowed[symbol]:
            candidates[(symbol,source)].append(index+2)
    selected=[]
    for symbol in symbols:
        relevant=[(source,positions) for (sym,source),positions in candidates.items()
                  if sym==symbol]
        if not relevant:
            continue
        source,indices=max(relevant,key=lambda item:len(item[1]))
        # Every session has immutable composite key; limit to 365 most
        # recent rows from a single source. If a source is fragmented,
        # the grouping still uses exact row coordinates.
        selected.extend((i,symbol,source) for i in indices[-PER_SOURCE_LIMIT:])
    groups=contiguous_runs(selected)
    if len(groups)>MAX_GROUPS:
        raise ValueError("RAW_SOURCE_TOO_FRAGMENTED")
    ranges=[f"G{group[0][0]}:O{group[-1][0]}" for group in groups]
    matrices=ws.batch_get(ranges) if ranges else []
    raw=[]
    for group,matrix in zip(groups,matrices):
        for info,values in zip(group,matrix):
            padded=(list(values)+[""]*len(RAW_FIELDS))[:len(RAW_FIELDS)]
            row=dict(zip(RAW_FIELDS,padded))
            row["canonical_symbol"]=info[1]
            row["source"]=info[2]
            raw.append(row)
        if len(matrix)!=len(group):
            raise ValueError("RAW_READBACK_INCOMPLETE")
    report=preview(current,symbols,snapshots,required,raw,now=today)
    report["source_rows_examined"]=len(raw)
    report["workbook_reads"]="bounded_columns_plus_source_segments"
    return report
