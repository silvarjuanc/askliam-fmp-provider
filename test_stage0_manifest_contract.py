import unittest
from stage0_manifest_contract import validate_manifest
def row():
 return {"run_id":"RUN-20261013-V6.5.1","version":"V6.5.1",
 "started_at":"2026-10-13T01:00:00Z","finished_at":"2026-10-13T01:05:00Z",
 "universe_count":2,"scored_count":1,"data_wait_count":1,"excluded_count":0,
 "top30_count":1,"lifecycle_updates":0,"execution_ready":0,"execution_wait":0,
 "calculation_status":"VALIDATED","commit_status":"COMMITTED",
 "verify_status":"PASS","final_status":"COMMITTED_WITH_ANALYTICAL_DATA_WAIT",
 "readback_details":"VALIDATED","notes":"TEST"}
class ManifestTests(unittest.TestCase):
 def test_full_readback(self):
  self.assertTrue(validate_manifest(row())["valid"])
 def test_false_success_fails(self):
  x=row();x["commit_status"]="CALCULATED_NOT_PERSISTED"
  self.assertFalse(validate_manifest(x)["valid"])
 def test_unverified_committed_fails(self):
  x=row();x["verify_status"]="FAILED"
  self.assertFalse(validate_manifest(x)["valid"])
 def test_too_many_top30_fails(self):
  x=row();x["top30_count"]=2
  self.assertFalse(validate_manifest(x)["valid"])
if __name__=="__main__": unittest.main()
