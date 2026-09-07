#!/usr/bin/env python3
"""Stage verified field repairs without deleting events or declaring gold accepted."""
import copy
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from scripts.pif_signal_desk_gold_review_corrected import R,A,_sha_json,build_cases
from scripts.pif_signal_desk_gold_repair_proposals import OUT as PROPOSALS,FIELDS,validate
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_rebuild_contracts import validate_event
from research_factory.signal_desk_gold_audit import _load_frozen_window_text

OUT=R/"verified-field-repair-candidate-v1"


def patch_event(original,patch,text):
    if set(patch)-FIELDS:raise ValueError("repair exceeds field scope")
    candidate=copy.deepcopy(original)
    candidate.update(patch)
    validate_event(candidate,transcript_window=text)
    for key in set(original)-FIELDS:
        if candidate[key]!=original[key]:raise ValueError("protected event data changed")
    return candidate


def project():
    manifest=json.loads((R/"merged-manifest.json").read_text())
    rows={w["window_id"]:w for w in manifest["windows"] if w["split"]=="development"}
    audit=json.loads((A/"audit.json").read_text())
    cases={c["case_id"]:c for c in build_cases(manifest_path=R/"merged-manifest.json",result_root=A/"results",project_root=ROOT,
        selected_window_ids=audit["audit_selection"]["window_ids"])}
    outputs={wid:json.loads((A/"results/C"/f"{wid}.json").read_text()) for wid in rows}
    before={wid:_sha_json(v) for wid,v in outputs.items()}
    event_count=sum(len(v["events"]) for v in outputs.values())
    patches=[]; unresolved=[]
    for digest in json.loads((PROPOSALS/"plan.json").read_text())["packet_digests"]:
        packet=json.loads((PROPOSALS/f"{digest}.packet.json").read_text())
        if _sha_json({k:v for k,v in packet.items() if k!="packet_sha256"})!=digest:raise ValueError("packet drift")
        proposal=packet["repair_proposal"];cid=proposal["case_id"]
        verdict=json.loads((PROPOSALS/f"{digest}.verification.json").read_text());validate(verdict,cid)
        case=cases[cid];wid=case["window_id"];i=case["gold_index"]
        status={"case_id":cid,"window_id":wid,"gold_index":i,"packet_sha256":digest,"verification_sha256":_sha_json(verdict)}
        if verdict["verdict"]!="accept_repair" or proposal["action"]!="field_patch":
            unresolved.append({**status,"reason":verdict["verdict"],"missing_context":verdict["missing_context"]});continue
        original=outputs[wid]["events"][i]
        if any(original.get(k)!=v for k,v in proposal["original_gold"].items()):raise ValueError("original event drift")
        try:candidate=patch_event(original,proposal["proposed_patch"],_load_frozen_window_text(rows[wid],project_root=ROOT))
        except ValueError as exc:
            unresolved.append({**status,"reason":"schema_rejected","detail":str(exc)});continue
        outputs[wid]["events"][i]=candidate
        patches.append({**status,"event_id":original["event_id"],"patch":proposal["proposed_patch"],"before_sha256":_sha_json(original),"after_sha256":_sha_json(candidate)})
    if sum(len(v["events"]) for v in outputs.values())!=event_count:raise ValueError("events dropped")
    OUT.mkdir(parents=True,exist_ok=True,mode=0o700)
    inventory={}
    for wid,value in outputs.items():
        immutable_json(OUT/"C"/f"{wid}.json",value);inventory[wid]=_sha_json(value)
    immutable_json(OUT/"patches.json",patches)
    immutable_json(OUT/"unresolved.json",unresolved)
    receipt={"applied_to_candidate":len(patches),"unresolved":len(unresolved),"windows":len(outputs),"events_preserved":event_count,
        "audit_denominator":1909,"before_inventory":before,"after_inventory":inventory,"authored_gold_modified":False,
        "gold_accepted":False,"independent_reaudit_required":True,"all_unresolved_records_retained":True}
    immutable_json(OUT/"receipt.json",receipt)
    print(json.dumps({k:v for k,v in receipt.items() if "inventory" not in k}))


if __name__=="__main__":project()
