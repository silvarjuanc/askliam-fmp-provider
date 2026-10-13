"""Read-only V6.5.1 broad ranking calculation; no publication or orders."""
from stage0_model_readiness import validate_mandatory
from stage0_canonical_factors import normalize_score

def preview_broad(universe,snapshots,specs,factor_evidence,run_id):
    if not run_id.endswith("-V6.5.1"): raise ValueError("WRONG_VERSION")
    specs_by_model={x["Model"]:x for x in specs}
    latest={}
    for row in snapshots:
        if row.get("ranking_run_id")!=run_id: continue
        sym=row["canonical_symbol"]
        if sym in latest: raise ValueError("DUPLICATE_SNAPSHOT")
        latest[sym]=dict(row)
    out=[];seen=set()
    for entry in universe:
        symbol=str(entry.get("Symbol","")).strip().upper()
        if symbol in seen: raise ValueError("DUPLICATE_UNIVERSE")
        seen.add(symbol)
        if str(entry.get("Analysis Enabled")).upper()!="TRUE" or str(entry.get("Ranking Enabled")).upper()!="TRUE":
            continue
        record={"canonical_symbol":symbol,"score_status":"DATA_WAIT",
                "global_score":None,"deep_dive_tier":"DATA_WAIT"}
        row=latest.get(symbol)
        if row and row.get("score_model") in specs_by_model:
            check=validate_mandatory(row,specs_by_model[row["score_model"]])
            record["missing_mandatory_fields"]=check["missing"]
            evidence=factor_evidence.get(symbol) or {}
            if check["mandatory_input_complete"] and evidence.get("source_quality_verified") is True:
                if evidence.get("risk_penalty") is not None:
                    score=normalize_score(evidence.get("factor_points") or {},
                                          evidence["risk_penalty"],mandatory_complete=True)
                    if score is not None:
                        record["score_status"]="SCORED"
                        record["global_score"]=round(score,5)
                        record["deep_dive_tier"]="BROAD_SCAN"
        else:
            record["missing_mandatory_fields"]=["CURRENT_RUN_MODEL_SNAPSHOT"]
        out.append(record)
    ranked=sorted((x for x in out if x["score_status"]=="SCORED"),
                  key=lambda x:(-x["global_score"],x["canonical_symbol"]))
    for i,row in enumerate(ranked,1):
        row["rank"]=i
        row["deep_dive_tier"]="TOP_10" if i<=10 else "TOP_30" if i<=30 else "BROAD_SCAN"
    return {"version":"V6.5.1","run_id":run_id,"eligible_count":len(out),
            "scored_count":len(ranked),"data_wait_count":len(out)-len(ranked),
            "top30_count":min(len(ranked),30),"rows":out,
            "ranking_validation_pass":False,"publication_permitted":False,
            "sheet_writes_performed":False,"execution_permitted":False}
