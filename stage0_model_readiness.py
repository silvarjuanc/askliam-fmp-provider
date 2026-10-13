"""Read-only V6.5.1 model-specific mandatory field validator.

Accepts the current Data_Requirements row from the actual workbook.
Does not grant Ranking or trading access.
"""
import math

class ReadinessError(ValueError):
    pass

def available(value):
    if value is None or isinstance(value,bool): return False
    if isinstance(value,(float,int)): return math.isfinite(value)
    if not isinstance(value,str): return False
    value=value.strip()
    if not value or value.upper() in {"NAN","NULL","NONE","UNKNOWN","PENDING"}: return False
    try: return math.isfinite(float(value))
    except ValueError: return True

def rule_ok(rule,record):
    if ">=" in rule:
        key,limit=rule.split(">=",1)
        if not available(record.get(key)): return False
        try: return float(record[key])>=float(limit)
        except (TypeError,ValueError): return False
    if "_or_" in rule:
        return any(available(record.get(key)) for key in rule.split("_or_"))
    return available(record.get(rule))

def validate_mandatory(record,spec):
    if record.get("score_model")!=spec.get("Model"): raise ReadinessError("MODEL_MISMATCH")
    rules=[r.strip() for r in spec.get("Broad Mandatory Data","").split(";") if r.strip()]
    if not rules: raise ReadinessError("NO_MANDATORY_SPEC")
    missing=[r for r in rules if not rule_ok(r,record)]
    try: threshold=float(spec["Broad Min %"])
    except (KeyError,ValueError,TypeError): raise ReadinessError("BAD_THRESHOLD") from None
    if not 0<=threshold<=100: raise ReadinessError("BAD_THRESHOLD")
    pct=round(100*(len(rules)-len(missing))/len(rules),2)
    return {
        "version":"V6.5.1","model":spec["Model"],"total":len(rules),
        "present":len(rules)-len(missing),"mandatory_coverage_pct":pct,
        "missing":missing,"mandatory_input_complete":not missing and pct>=threshold,
        "broad_scan_ready":False,"ranking_permitted":False,
        "execution_permitted":False,"sheet_writes_performed":False,
        "note":"Source freshness and canonical score provenance are still mandatory."
    }
