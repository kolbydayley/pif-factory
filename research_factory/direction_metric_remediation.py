from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

from . import db
from .labels import recover_v31_direction_evidence
from .orchestrator import pipeline_lock
from .paths import root as project_root
from .util import dumps_json, now_iso, stable_id, write_text_atomic
from .worker import _insert_label_metric_quarantines


SERIES_REPORT = Path(
    "work/pif-ops/label-campaign-series/plcs_b4d115c31de3aaafe3fe3486/report.json"
)
EXPECTED_LABELS = 791
EXPECTED_DIRECTION_ONLY = 2_040


def _population(root: Path) -> tuple[list[str], list[str]]:
    report = json.loads((root / SERIES_REPORT).read_text(encoding="utf-8"))
    label_ids: list[str] = []
    run_ids: list[str] = []
    for campaign in report["campaigns"]:
        manifest = json.loads(Path(campaign["manifest_path"]).read_text(encoding="utf-8"))
        label_ids.extend(manifest["label_ids"])
        run_ids.extend(manifest["label_run_ids"])
    if len(label_ids) != EXPECTED_LABELS or len(set(label_ids)) != EXPECTED_LABELS:
        raise RuntimeError(f"population_mismatch:{len(label_ids)}:{len(set(label_ids))}")
    return label_ids, run_ids


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _nonmetric_projection(value: dict[str, Any]) -> dict[str, Any]:
    projected = copy.deepcopy(value)
    projected.pop("needs_review", None)
    projected.pop("review_reason", None)
    for event in projected.get("discourse_events") or []:
        if isinstance(event, dict):
            event.pop("metric", None)
            event.pop("quality_flags", None)
    return projected


def run(*, execute: bool, backup_path: Path | None = None, backup_sha256: str = "") -> dict[str, Any]:
    root = project_root()
    label_ids, population_run_ids = _population(root)
    if execute:
        if backup_path is None or not backup_path.is_file():
            raise RuntimeError("verified_backup_required")
        measured_backup_sha = _sha256(backup_path)
        if not backup_sha256 or measured_backup_sha != backup_sha256:
            raise RuntimeError("backup_hash_mismatch")
    conn = db.connect()
    run_by_label: dict[str, str] = {}
    for offset in range(0, len(population_run_ids), 300):
        chunk = population_run_ids[offset : offset + 300]
        marks = ",".join("?" for _ in chunk)
        for run_row in conn.execute(
            f"""
            SELECT l.id AS label_id, lr.id AS label_run_id
            FROM label_runs AS lr
            JOIN labels AS l ON l.segment_id = lr.segment_id
            WHERE lr.id IN ({marks}) AND lr.status = 'completed'
            """,
            chunk,
        ):
            run_by_label[run_row["label_id"]] = run_row["label_run_id"]
    missing_runs = set(label_ids) - set(run_by_label)
    if missing_runs:
        raise RuntimeError(f"completed_label_runs_missing:{len(missing_runs)}")
    rows = []
    for offset in range(0, len(label_ids), 300):
        chunk = label_ids[offset : offset + 300]
        marks = ",".join("?" for _ in chunk)
        rows.extend(
            conn.execute(
                f"SELECT id, segment_id, output_json FROM labels WHERE id IN ({marks})",
                chunk,
            ).fetchall()
        )
    if len(rows) != EXPECTED_LABELS:
        raise RuntimeError(f"label_rows_missing:{len(rows)}")

    planned: list[dict[str, Any]] = []
    failures: Counter[str] = Counter()
    recovered_by_direction: Counter[str] = Counter()
    pre_projection = {
        row["id"]: hashlib.sha256(
            dumps_json(_nonmetric_projection(json.loads(row["output_json"]))).encode()
        ).hexdigest()
        for row in rows
    }
    for row in rows:
        output = json.loads(row["output_json"])
        for event_index, event in enumerate(output.get("discourse_events") or []):
            metric = event.get("metric") if isinstance(event, dict) else None
            if not isinstance(metric, dict):
                continue
            if metric.get("direction") in (None, "not_applicable"):
                continue
            if any(metric.get(field) not in (None, "") for field in ("value", "unit", "comparator", "raw_text")):
                continue
            phrase, failure = recover_v31_direction_evidence(
                str(metric["direction"]), str(event.get("evidence") or "")
            )
            planned.append(
                {
                    "label_id": row["id"],
                    "label_run_id": run_by_label[row["id"]],
                    "segment_id": row["segment_id"],
                    "event_index": event_index,
                    "claim_text": str(event.get("claim_text") or ""),
                    "original_metric": copy.deepcopy(metric),
                    "evidence": str(event.get("evidence") or ""),
                    "direction_evidence": phrase,
                    "failure": failure,
                }
            )
            if phrase:
                recovered_by_direction[str(metric["direction"])] += 1
            else:
                failures[str(failure)] += 1
    if len(planned) != EXPECTED_DIRECTION_ONLY:
        raise RuntimeError(f"direction_population_mismatch:{len(planned)}")

    inserted = 0
    changed_labels: set[str] = set()
    if execute:
        by_label: dict[str, list[dict[str, Any]]] = {}
        for item in planned:
            by_label.setdefault(item["label_id"], []).append(item)
        with pipeline_lock(wait=False) as acquired:
            if not acquired:
                raise RuntimeError("pipeline_lock_busy")
            conn.execute("BEGIN IMMEDIATE")
            try:
                for row in rows:
                    items = by_label.get(row["id"], [])
                    if not items:
                        continue
                    output = json.loads(row["output_json"])
                    quarantines: list[dict[str, Any]] = []
                    for item in items:
                        event = output["discourse_events"][item["event_index"]]
                        if item["direction_evidence"]:
                            event["metric"]["direction_evidence"] = item["direction_evidence"]
                        else:
                            quarantines.append(
                                {
                                    "event_index": item["event_index"],
                                    "claim_text": item["claim_text"],
                                    "original_metric": item["original_metric"],
                                    "evidence": item["evidence"],
                                    "failed_rules": [item["failure"]],
                                }
                            )
                            event["metric"] = {
                                "value": None,
                                "unit": None,
                                "comparator": None,
                                "direction": "not_applicable",
                                "direction_evidence": None,
                                "raw_text": None,
                            }
                            flags = list(event.get("quality_flags") or [])
                            if "direction_metric_quarantined" not in flags:
                                flags.append("direction_metric_quarantined")
                            event["quality_flags"] = flags
                    if quarantines:
                        output["needs_review"] = True
                        reason = f"Quarantined {len(quarantines)} legacy ungrounded direction metric(s)."
                        current = str(output.get("review_reason") or "").strip()
                        if reason not in current:
                            output["review_reason"] = f"{current} {reason}".strip()[:240]
                    conn.execute(
                        "UPDATE labels SET output_json = ?, needs_review = ? WHERE id = ?",
                        (dumps_json(output), int(bool(output.get("needs_review"))), row["id"]),
                    )
                    for item in items:
                        metric_json = dumps_json(
                            output["discourse_events"][item["event_index"]]["metric"]
                        )
                        updated = conn.execute(
                            """
                            UPDATE discourse_event_contexts SET metric_json = ?
                            WHERE label_id = ? AND discourse_event_id = (
                              SELECT id FROM discourse_events
                              WHERE label_id = ? AND event_index = ?
                            )
                            """,
                            (metric_json, row["id"], row["id"], item["event_index"]),
                        )
                        if updated.rowcount != 1:
                            raise RuntimeError(
                                f"context_metric_update_mismatch:{row['id']}:{item['event_index']}"
                            )
                    inserted += _insert_label_metric_quarantines(
                        conn,
                        label_id=row["id"],
                        label_run_id=run_by_label[row["id"]],
                        segment_id=row["segment_id"],
                        quarantines=quarantines,
                    )
                    changed_labels.add(row["id"])
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    post_projection_equal = True
    if execute:
        for label_id, prior_hash in pre_projection.items():
            row = conn.execute("SELECT output_json FROM labels WHERE id = ?", (label_id,)).fetchone()
            current_hash = hashlib.sha256(
                dumps_json(_nonmetric_projection(json.loads(row["output_json"]))).encode()
            ).hexdigest()
            post_projection_equal &= current_hash == prior_hash
    result = {
        "schema_version": "pif_direction_metric_remediation_v1",
        "run_id": stable_id(now_iso(), str(execute), prefix="dmr_"),
        "executed": execute,
        "population": {"labels": len(rows), "direction_only_metrics": len(planned)},
        "deterministically_recovered": sum(recovered_by_direction.values()),
        "deterministically_recovered_fraction": sum(recovered_by_direction.values()) / len(planned),
        "recovered_by_direction": dict(sorted(recovered_by_direction.items())),
        "semantic_judgment_required_and_quarantined": sum(failures.values()),
        "failure_classes": dict(sorted(failures.items())),
        "quarantine_rows_inserted": inserted,
        "labels_changed": len(changed_labels),
        "nonmetric_projection_byte_equivalent": post_projection_equal,
        "backup": (
            {"path": str(backup_path), "sha256": backup_sha256, "bytes": backup_path.stat().st_size}
            if execute and backup_path
            else None
        ),
        "completed_at": now_iso(),
    }
    result["content_sha256"] = hashlib.sha256(dumps_json(result).encode()).hexdigest()
    out = root / "work/pif-ops/direction-remediation" / result["run_id"]
    out.mkdir(parents=True, exist_ok=False)
    write_text_atomic(out / "report.json", dumps_json(result) + "\n")
    write_text_atomic(
        out / "manifest.json",
        dumps_json({
            "label_ids": label_ids,
            "source_series": str(root / SERIES_REPORT),
            "content_sha256": hashlib.sha256(dumps_json(label_ids).encode()).hexdigest(),
        }) + "\n",
    )
    conn.close()
    return {**result, "report_path": str(out / "report.json"), "manifest_path": str(out / "manifest.json")}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--backup-path", type=Path)
    parser.add_argument("--backup-sha256", default="")
    args = parser.parse_args()
    print(dumps_json(run(execute=args.execute, backup_path=args.backup_path, backup_sha256=args.backup_sha256)))


if __name__ == "__main__":
    main()
