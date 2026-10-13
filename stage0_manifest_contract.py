"""AskLiam V6.5.1 Run_Manifest contract (non-mutating validation)."""
REQUIRED=(
 "run_id","version","started_at","finished_at","universe_count",
 "scored_count","data_wait_count","excluded_count","top30_count",
 "lifecycle_updates","execution_ready","execution_wait",
 "calculation_status","commit_status","verify_status","final_status",
 "readback_details","notes",
)
VALID_FINAL={
 "COMMITTED","COMMITTED_WITH_ANALYTICAL_DATA_WAIT",
 "COMMITTED_WITH_EXECUTION_DATA_WAIT",
 "COMMITTED_WITH_ANALYTICAL_AND_EXECUTION_DATA_WAIT",
 "CALCULATED_NOT_PERSISTED","PARTIAL_COMMIT_ROLLED_BACK",
}
VALID_COMMIT={"COMMITTED","CALCULATED_NOT_PERSISTED","PARTIAL_COMMIT_ROLLED_BACK"}

def validate_manifest(row):
 missing=[x for x in REQUIRED if x not in row or row[x] is None]
 if missing: return {"valid":False,"reason":"MISSING_FIELDS","fields":missing}
 if row["version"]!="V6.5.1" or row["commit_status"] not in VALID_COMMIT:
  return {"valid":False,"reason":"VERSION_OR_COMMIT_STATUS_INVALID"}
 if row["final_status"] not in VALID_FINAL:
  return {"valid":False,"reason":"FINAL_STATUS_INVALID"}
 if row["commit_status"]=="COMMITTED" and row["verify_status"]!="PASS":
  return {"valid":False,"reason":"COMMITTED_WITHOUT_READBACK"}
 if row["commit_status"]!="COMMITTED" and row["final_status"].startswith("COMMITTED"):
  return {"valid":False,"reason":"UNVERIFIED_FALSE_SUCCESS"}
 if row["top30_count"]>min(30,row["scored_count"]):
  return {"valid":False,"reason":"TOP30_EXCEEDS_SCORED"}
 if row["universe_count"]!=row["scored_count"]+row["data_wait_count"]+row["excluded_count"]:
  return {"valid":False,"reason":"UNIVERSE_COUNT_INCONSISTENT"}
 return {"valid":True,"reason":"CONTRACT_PASS",
         "publication_permitted":row["commit_status"]=="COMMITTED" and
         row["verify_status"]=="PASS"}
