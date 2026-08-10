from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path

from . import db
from .direction_metric_remediation import _population, _sha256
from .labels import _v31_direction_semantic_failure, recover_v31_direction_evidence
from .orchestrator import pipeline_lock
from .paths import root
from .util import dumps_json, now_iso, stable_id, write_text_atomic
from .worker import _insert_label_metric_quarantines


def run(*, backup_path: Path, backup_sha256: str) -> dict:
    if not backup_path.is_file() or _sha256(backup_path) != backup_sha256:
        raise RuntimeError("verified_backup_required")
    label_ids, population_run_ids = _population(root())
    conn = db.connect()
    run_by_label = {}
    for offset in range(0, len(population_run_ids), 300):
        chunk = population_run_ids[offset : offset + 300]
        marks = ",".join("?" for _ in chunk)
        for row in conn.execute(
            f"SELECT l.id label_id, lr.id label_run_id FROM label_runs lr JOIN labels l ON l.segment_id=lr.segment_id WHERE lr.id IN ({marks}) AND lr.status='completed'",
            chunk,
        ):
            run_by_label[row["label_id"]] = row["label_run_id"]
    rows = []
    for offset in range(0, len(label_ids), 300):
        chunk = label_ids[offset : offset + 300]
        marks = ",".join("?" for _ in chunk)
        rows.extend(conn.execute(f"SELECT id,segment_id,output_json FROM labels WHERE id IN ({marks})", chunk).fetchall())
    planned = []
    for row in rows:
        output = json.loads(row["output_json"])
        for index, event in enumerate(output.get("discourse_events") or []):
            metric = event.get("metric") or {}
            if metric.get("direction") in (None, "not_applicable"):
                continue
            if any(metric.get(field) not in (None, "") for field in ("raw_text", "value", "unit", "comparator")):
                continue
            direction_evidence = str(metric.get("direction_evidence") or "")
            failure = _v31_direction_semantic_failure(str(metric["direction"]), direction_evidence)
            if direction_evidence and failure:
                replacement, replacement_failure = recover_v31_direction_evidence(
                    str(metric["direction"]), str(event.get("evidence") or "")
                )
                planned.append((row, output, index, event, dict(metric), replacement, replacement_failure or failure))
    recoverable = sum(bool(item[5]) for item in planned)
    unrecoverable = len(planned) - recoverable
    if len(planned) != 62 or recoverable != 9 or unrecoverable != 53:
        raise RuntimeError(f"strictness_delta_mismatch:{len(planned)}:{recoverable}:{unrecoverable}")
    by_label = {}
    for row, output, index, event, metric, replacement, failure in planned:
        by_label.setdefault(row["id"], {"row": row, "output": output, "items": []})["items"].append((index, event, metric, replacement, failure))
    inserted = 0
    classes = Counter()
    with pipeline_lock(wait=False) as acquired:
        if not acquired:
            raise RuntimeError("pipeline_lock_busy")
        conn.execute("BEGIN IMMEDIATE")
        try:
            for label_id, bundle in by_label.items():
                row = bundle["row"]
                output = bundle["output"]
                quarantines = []
                for index, event, metric, replacement, failure in bundle["items"]:
                    if replacement:
                        event["metric"]["direction_evidence"] = replacement
                    else:
                        classes[failure] += 1
                        quarantines.append({"event_index": index, "claim_text": str(event.get("claim_text") or ""), "original_metric": metric, "evidence": str(event.get("evidence") or ""), "failed_rules": [failure]})
                        event["metric"] = {"value": None, "unit": None, "comparator": None, "direction": "not_applicable", "direction_evidence": None, "raw_text": None}
                        flags = list(event.get("quality_flags") or [])
                        if "direction_metric_quarantined" not in flags:
                            flags.append("direction_metric_quarantined")
                        event["quality_flags"] = flags
                    updated = conn.execute(
                        "UPDATE discourse_event_contexts SET metric_json=? WHERE label_id=? AND discourse_event_id=(SELECT id FROM discourse_events WHERE label_id=? AND event_index=?)",
                        (dumps_json(event["metric"]), label_id, label_id, index),
                    )
                    if updated.rowcount != 1:
                        raise RuntimeError(f"context_update_mismatch:{label_id}:{index}")
                if quarantines:
                    output["needs_review"] = True
                    reason = f"Quarantined {len(quarantines)} legacy magnitude-only direction metric(s)."
                    current = str(output.get("review_reason") or "").strip()
                    if reason not in current:
                        output["review_reason"] = f"{current} {reason}".strip()[:240]
                conn.execute("UPDATE labels SET output_json=?,needs_review=? WHERE id=?", (dumps_json(output), int(bool(output.get("needs_review"))), label_id))
                inserted += _insert_label_metric_quarantines(conn, label_id=label_id, label_run_id=run_by_label[label_id], segment_id=row["segment_id"], quarantines=quarantines)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    report = {
        "schema_version": "pif_direction_metric_strictness_followup_v1",
        "run_id": stable_id(now_iso(), "direction-strictness-followup", prefix="dmf_"),
        "population_labels": len(label_ids),
        "additional_quarantines": inserted,
        "direction_evidence_rewritten_exactly": recoverable,
        "failure_classes": dict(classes),
        "backup": {"path": str(backup_path), "sha256": backup_sha256, "bytes": backup_path.stat().st_size},
        "completed_at": now_iso(),
    }
    report["content_sha256"] = hashlib.sha256(dumps_json(report).encode()).hexdigest()
    out = root() / "work/pif-ops/direction-remediation" / report["run_id"]
    out.mkdir(parents=True, exist_ok=False)
    write_text_atomic(out / "report.json", dumps_json(report) + "\n")
    conn.close()
    return {**report, "report_path": str(out / "report.json")}
