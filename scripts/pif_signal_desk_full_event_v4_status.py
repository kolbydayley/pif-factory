#!/usr/bin/env python3
"""Read-only source-bound progress; distinguish authored from accepted records."""
import json
from collections import Counter
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_full_event_v4_run import OUT, BASE
from research_factory.signal_desk_full_event_v4 import validate, semantic_bundle
from research_factory.signal_desk_rubric_reference_packets import digest


def summarize(root=OUT, base=BASE):
    plan = json.loads((root / "plan.json").read_text())
    states = Counter(); by_role = {}; problems = []; rows = []; usage = 0; usage_missing = 0
    for wid in plan["window_ids"]:
        sha = plan["source_packets"][wid]
        source = json.loads((base / f"{sha}.packet.json").read_text())
        if digest({k: v for k, v in source.items() if k != "packet_sha256"}) != sha:
            raise ValueError("source lineage changed")
        for role in ("A", "B", "C", "AUDIT"):
            directory = root / "calls" / wid / role
            item = {"window_id": wid, "role": role, "state": "not_started"}
            ppath = directory / "packet.json"
            if ppath.exists():
                p = json.loads(ppath.read_text()); psha = p["packet_sha256"]
                if digest({k: v for k, v in p.items() if k != "packet_sha256"}) != psha or p["original_source_packet_sha256"] != sha or p["transcript_window"] != source["transcript_window"]:
                    raise ValueError("call packet lineage changed")
                if p["system_sha256"] != plan["contract"]["role_hashes"][role] or p["schema_sha256"] != plan["contract"]["schema_sha256"]:
                    raise ValueError("call contract changed")
                item["state"] = "prepared"
                sidecar_path = directory / f"{psha}.sidecar.json"
                if sidecar_path.exists():
                    s = json.loads(sidecar_path.read_text()); item["state"] = s["state"]
                    item["wall_elapsed_seconds"] = s.get("wall_elapsed_seconds")
                    measured = s.get("usage")
                    if measured and isinstance(measured.get("total_tokens"), int): usage += measured["total_tokens"]
                    elif s["state"] != "in_progress": usage_missing += 1
                    if s.get("error_class"): problems.append({"window_id": wid, "role": role, "error_class": s["error_class"]})
                    result_path = directory / f"{psha}.result.json"
                    raw_path = directory / f"{psha}.output.json"
                    if s["state"] == "completed" and raw_path.exists():
                        raw = json.loads(raw_path.read_text())
                        try: validate(raw, source=source["transcript_window"], window_id=wid)
                        except (ValueError, TypeError, KeyError) as exc:
                            item["state"] = "held_contract_failure"
                            problems.append({"window_id": wid, "role": role, "validation_error": str(exc)})
                        else:
                            if result_path.exists():
                                if json.loads(result_path.read_text()) != raw: raise ValueError("saved result differs from raw; explicit recovery required")
                                item["state"] = "authored_not_accepted"
                                bundle, _ = semantic_bundle(raw, source["transcript_window"])
                                voices = {r["candidate_id"]: r for r in bundle["voice"]["decisions"]}
                                totals = by_role.setdefault(role, Counter())
                                totals["windows"] += 1; totals["records"] += len(raw["events"])
                                if not raw["events"]: totals["empty_windows"] += 1
                                for event in raw["events"]:
                                    v = voices[event["event_id"]]; a = event["attribution"]
                                    totals["source_kind:" + v["source_kind"]] += 1
                                    totals["role:" + event["evidence_role"]["role"]] += 1
                                    totals["publishability:" + event["publishability_state"]] += 1
                                    if v["source_kind"] == "spoken_transcript":
                                        totals["spoken_named_voice" if a["transcript_voice"] is not None else "spoken_indeterminate_voice"] += 1
                                    if a["proposition_owner"] is not None:
                                        totals["named_owner:" + a["relation"]] += 1
                                spans = Counter((e["evidence_start"], e["evidence_end"]) for e in raw["events"])
                                item["max_same_span"] = max(spans.values(), default=0)
                                item["records"] = len(raw["events"])
            states[item["state"]] += 1; rows.append(item)
    return {"plan_sha256": digest(plan), "planned_windows": len(plan["window_ids"]), "planned_calls": len(rows),
        "states": dict(states), "by_role": {k: dict(v) for k, v in by_role.items()}, "calls": rows,
        "problems": problems, "measured_usage_tokens": usage, "terminal_calls_without_usage": usage_missing,
        "accepted_gold": False, "qualified": False, "counts_are_role_specific_not_independent_corpus_events": True}


if __name__ == "__main__": print(json.dumps(summarize(), sort_keys=True))
