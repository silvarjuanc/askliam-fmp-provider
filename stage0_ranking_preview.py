"""Read-only V6.5.1 broad ranking preview — never invent DATA_WAIT or publish."""
from stage0_model_readiness import validate_mandatory
from stage0_canonical_factors import normalize_score


def preview_broad(universe, snapshots, specs, factor_evidence, run_id):
    if not run_id.endswith("-V6.5.1"):
        raise ValueError("WRONG_VERSION")
    specs_by_model={x["Model"]:x for x in specs}
    latest={}
    for row in snapshots:
        if row.get("ranking_run_id")!=run_id:
            continue
        sym=str(row.get("canonical_symbol","")).upper()
        if not sym or sym in latest:
            raise ValueError("DUPLICATE_SNAPSHOT")
        latest[sym]=dict(row)
    out=[]
    seen=set()
    for entry in universe:
        symbol=str(entry.get("Symbol","")).strip().upper()
        if not symbol:
            continue
        if symbol in seen:
            raise ValueError("DUPLICATE_UNIVERSE")
        seen.add(symbol)
        if str(entry.get("Analysis Enabled")).upper()!="TRUE" or str(entry.get("Ranking Enabled")).upper()!="TRUE":
            continue
        record={"canonical_symbol":symbol,"score_status":"NOT_ACQUIRED",
                "global_score":None,"deep_dive_tier":"NOT_ACQUIRED",
                "missing_mandatory_fields":["CURRENT_RUN_MODEL_SNAPSHOT"]}
        row=latest.get(symbol)
        if row is None:
            # Missing observation does not prove source failure or analytical DATA_WAIT.
            out.append(record)
            continue
        record["score_status"]="ACQUIRED_INCOMPLETE"
        record["deep_dive_tier"]="ACQUIRED_INCOMPLETE"
        if row.get("score_model") not in specs_by_model:
            record["missing_mandatory_fields"]=["MODEL_NOT_CONFIGURED"]
            out.append(record)
            continue
        check=validate_mandatory(row,specs_by_model[row["score_model"]])
        record["missing_mandatory_fields"]=check["missing"]
        if not check["mandatory_input_complete"]:
            out.append(record)
            continue
        record["score_status"]="FACTOR_EVIDENCE_WAIT"
        record["deep_dive_tier"]="FACTOR_EVIDENCE_WAIT"
        evidence=factor_evidence.get(symbol) or {}
        if evidence.get("source_quality_verified") is not True:
            out.append(record)
            continue
        if evidence.get("risk_penalty") is None:
            out.append(record)
            continue
        try:
            score=normalize_score(evidence.get("factor_points") or {},
                                  evidence["risk_penalty"],mandatory_complete=True)
        except (TypeError,ValueError,OverflowError):
            score=None
        if score is not None:
            record["score_status"]="SCORED"
            record["global_score"]=round(score,5)
            record["deep_dive_tier"]="BROAD_SCAN"
        out.append(record)
    ranked=sorted((x for x in out if x["score_status"]=="SCORED"),
                  key=lambda x:(-x["global_score"],x["canonical_symbol"]))
    for i,row in enumerate(ranked,1):
        row["rank"]=i
        row["deep_dive_tier"]="TOP_10" if i<=10 else "TOP_30" if i<=30 else "BROAD_SCAN"
    status_counts={}
    for row in out:
        key=row["score_status"]
        status_counts[key]=status_counts.get(key,0)+1
    return {"version":"V6.5.1","run_id":run_id,"eligible_count":len(out),
            "scored_count":len(ranked),"data_wait_count":status_counts.get("DATA_WAIT",0),
            "not_acquired_count":status_counts.get("NOT_ACQUIRED",0),
            "acquired_incomplete_count":status_counts.get("ACQUIRED_INCOMPLETE",0),
            "factor_evidence_wait_count":status_counts.get("FACTOR_EVIDENCE_WAIT",0),
            "status_counts":status_counts,
            "top30_count":min(len(ranked),30),"rows":out,
            "ranking_validation_pass":False,"publication_permitted":False,
            "sheet_writes_performed":False,"execution_permitted":False}
