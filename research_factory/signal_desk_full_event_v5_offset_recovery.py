"""Explicitly inspected three-span repair, never a relaxed generic offset gate."""
import json
from .signal_desk_full_event_v4_repair import propose
from .signal_desk_full_event_v5 import validate
from .signal_desk_rubric_reference_packets import digest

WID = "sdw_c38394ac01937fde7f33"
INSPECTED = [
    (["events",0,"attitude","target"], "unwind it enough", 941,957,943,959),
    (["events",4,"attitude","target"], "deep, low-level network knowledge and the CSS knowledge",2725,2784,2729,2784),
    (["events",7,"attitude","evaluation_evidence",0], "they're gonna be directly involved if they choose to be",4476,4532,4477,4532),
]


def recover(original, *, source, window_id):
    if window_id != WID or len(original["events"]) != 9: raise ValueError("not the inspected call")
    changes=[]
    for path,text,start,end,fixed_start,fixed_end in INSPECTED:
        span=original
        for key in path:span=span[key]
        if span != {"text":text,"start":start,"end":end}:raise ValueError("inspected span changed")
        if source.count(text)!=1 or source[fixed_start:fixed_end]!=text:
            raise ValueError("inspected unique source location changed")
        for field,before,after in (("start",start,fixed_start),("end",end,fixed_end)):
            if before != after:changes.append({"path":path+[field],"before":before,"after":after,
                "reason":"Explicit source-inspected unique exact string; change offsets only, never text or semantic labels."})
    fixed,receipt=propose(original,source=source,window_id=window_id,expected_original_sha256=digest(original),
        replacements=changes,output_validator=validate)
    receipt.update(repair="v5-inspected-three-attitude-spans-v1",semantic_fields_changed=False,original_failure_preserved=True)
    return fixed,receipt


def load_call(directory,packet):
    sha=packet["packet_sha256"]
    raw=json.loads((directory/f"{sha}.output.json").read_text())
    result=json.loads((directory/f"{sha}.result.json").read_text())
    provenance={"raw_sha256":digest(raw),"result_sha256":digest(result),"offset_recovery":False}
    if raw != result:
        expected,receipt=recover(raw,source=packet["transcript_window"],window_id=packet["window_id"])
        if expected != result or json.loads((directory/"offset-recovery.json").read_text()) != receipt:
            raise ValueError("unverified v5 offset projection")
        provenance.update(offset_recovery=True,recovery_receipt_sha256=digest(receipt))
    validate(result,source=packet["transcript_window"],window_id=packet["window_id"])
    return result,provenance
