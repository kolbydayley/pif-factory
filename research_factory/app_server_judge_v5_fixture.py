from __future__ import annotations

"""Executable truth-audit boundary for the pipeline-v5 judge protocol."""

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_llm_judge import make_v2_calibration_pool, write_immutable_json


FIXTURE_AUDIT_VERSION = "pif_judge_fixture_truth_audit_v3"
FIXTURE_AUDIT_RECEIPT_VERSION = "pif_judge_fixture_truth_audit_receipt_v1"
DEFAULT_FIXTURE_AUDIT_PATH = (
    Path(__file__).resolve().parent / "evaluation/judge_fixture_truth_audit_v3.json"
)


class FixtureTruthAuditError(ValueError):
    """The frozen v5 fixture audit does not match authoritative v4 evidence."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FixtureTruthAuditError(f"{purpose} is missing or invalid JSON") from exc
    if not isinstance(value, dict):
        raise FixtureTruthAuditError(f"{purpose} is not an object")
    return value


def _file_record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise FixtureTruthAuditError("required fixture-audit artifact is missing")
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def compact_empty_event_fields(value: Any) -> Any:
    """Remove structural empty values without inspecting semantic names or text."""

    if isinstance(value, Mapping):
        compacted = {}
        for key, child in value.items():
            rendered = compact_empty_event_fields(child)
            if rendered is None or rendered == "" or rendered == [] or rendered == {}:
                continue
            compacted[str(key)] = rendered
        return compacted
    if isinstance(value, list):
        return [compact_empty_event_fields(item) for item in value]
    return value


def measure_v4_prompt_compaction(calibration_root: Path) -> dict[str, Any]:
    root = calibration_root.expanduser().resolve()
    paths = sorted(root.glob("shards/*/judge/prompt-*.private.md"))
    if len(paths) != 22:
        raise FixtureTruthAuditError("v4 prompt compaction requires all 22 prompts")
    marker = "\n\n# Blinded cases\n"
    before = 0
    after = 0
    for path in paths:
        text = path.read_text(encoding="utf-8")
        if text.count(marker) != 1:
            raise FixtureTruthAuditError("v4 judge prompt framing drifted")
        instructions, packet_text = text.split(marker, 1)
        try:
            packet = json.loads(packet_text)
        except json.JSONDecodeError as exc:
            raise FixtureTruthAuditError("v4 judge prompt packet is invalid") from exc
        compacted = compact_empty_event_fields(packet)
        rendered = (
            instructions
            + marker
            + json.dumps(
                compacted,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
        before += len(text.encode("utf-8"))
        after += len(rendered.encode("utf-8"))
    return {
        "prompt_count": len(paths),
        "before_prompt_bytes": before,
        "after_prompt_bytes": after,
        "reduction_fraction": round(1 - after / before, 4),
        "empty_values_only": True,
        "semantic_pruning_performed": False,
    }


def recompute_v4_error_forensics(calibration_report_path: Path) -> dict[str, Any]:
    report = _load_json(
        calibration_report_path.expanduser().resolve(),
        purpose="pipeline-v4 calibration report",
    )
    if (
        report.get("schema_version")
        != "pif_app_server_sharded_judge_calibration_v3"
        or report.get("state") != "completed"
        or report.get("calibrated") is not False
        or report.get("accounting_complete") is not True
        or report.get("usage_status") != "complete"
        or report.get("retry_count") != 0
    ):
        raise FixtureTruthAuditError("pipeline-v4 report is not the frozen measured failure")
    _pool, _mapping, expected = make_v2_calibration_pool()
    truth_by_base = {
        value["base_case_id"]: value for value in expected["cases"].values()
    }
    orientation_records = {}
    normalized_by_orientation = {}
    for orientation in ("ab", "ba"):
        score = (report.get("scores") or {}).get(orientation) or {}
        predicted = score.get("by_base_case")
        if not isinstance(predicted, dict) or set(predicted) != set(truth_by_base):
            raise FixtureTruthAuditError("pipeline-v4 per-case scoring coverage drifted")
        normalized_by_orientation[orientation] = predicted
        false_negatives = []
        field_hits: Counter[str] = Counter()
        field_misses: Counter[str] = Counter()
        for base_case_id, truth in truth_by_base.items():
            row = predicted[base_case_id]
            for witness_id, verdict in truth["support"].items():
                if verdict == "supported" and row["support"][witness_id] != "supported":
                    false_negatives.append(base_case_id)
            for pair in truth["pairs"]:
                key = "%s|%s" % (
                    pair["left_witness_id"],
                    pair["right_witness_id"],
                )
                observed = set(
                    ((row.get("pairs") or {}).get(key) or {}).get("mismatch_fields")
                    or []
                )
                wanted = set(pair["mismatch_fields"])
                field_hits.update(wanted & observed)
                field_misses.update(wanted - observed)
        orientation_records[orientation] = {
            "support_false_negative_count": len(false_negatives),
            "support_false_negative_by_shape": dict(
                sorted(
                    Counter(
                        item.split("__", 1)[1] if "__" in item else item
                        for item in false_negatives
                    ).items()
                )
            ),
            "support_false_negative_by_topic": dict(
                sorted(Counter(item.split("__", 1)[0] for item in false_negatives).items())
            ),
            "field_hits": dict(sorted(field_hits.items())),
            "field_misses": dict(sorted(field_misses.items())),
            "support_sensitivity": score.get("support_sensitivity"),
            "support_specificity": score.get("support_specificity"),
        }
    changed = [
        case_id
        for case_id in sorted(normalized_by_orientation["ab"])
        if normalized_by_orientation["ab"][case_id]
        != normalized_by_orientation["ba"][case_id]
    ]
    return {
        "ab": orientation_records["ab"],
        "ba": orientation_records["ba"],
        "orientation_changed_case_count": len(changed),
        "orientation_changed_by_topic": dict(
            sorted(Counter(item.split("__", 1)[0] for item in changed).items())
        ),
        "orientation_changed_by_shape": dict(
            sorted(
                Counter(
                    item.split("__", 1)[1] if "__" in item else item
                    for item in changed
                ).items()
            )
        ),
    }


def load_fixture_truth_audit(
    path: Path = DEFAULT_FIXTURE_AUDIT_PATH,
) -> dict[str, Any]:
    audit_path = path.expanduser().resolve()
    audit = _load_json(audit_path, purpose="pipeline-v5 fixture truth audit")
    if (
        audit.get("schema_version") != FIXTURE_AUDIT_VERSION
        or audit.get("status") != "frozen_before_pipeline_v5_semantic_calls"
    ):
        raise FixtureTruthAuditError("unsupported fixture truth audit")
    source = audit.get("source_fixture") or {}
    source_path = (audit_path.parents[2] / str(source.get("path") or "")).resolve()
    if (
        not source_path.is_file()
        or _sha256_file(source_path) != source.get("sha256")
        or (audit.get("legacy_truth_classification") or {}).get(
            "admissible_as_pipeline_v5_proposition_support_truth"
        )
        is not False
        or (audit.get("legacy_truth_classification") or {}).get(
            "admissible_as_pipeline_v5_structured_field_truth"
        )
        is not False
    ):
        raise FixtureTruthAuditError("fixture source or legacy-label boundary drifted")
    checklist = audit.get("mismatch_checklist")
    if not isinstance(checklist, list) or len(checklist) != 15:
        raise FixtureTruthAuditError("fixture audit does not define all 15 mismatch rows")
    fields = [item.get("field") for item in checklist if isinstance(item, dict)]
    expected_fields = [
        "actor",
        "attribution",
        "causal_mechanism",
        "certainty",
        "event_boundary",
        "event_type",
        "evidence",
        "metric",
        "negation",
        "reported_actor",
        "speaker",
        "stance",
        "target",
        "temporal_horizon",
        "unsupported_inference",
    ]
    if fields != expected_fields or any(
        not isinstance(item.get("definition"), str)
        or not item["definition"]
        or not isinstance(item.get("decision_rule"), str)
        or not item["decision_rule"]
        for item in checklist
    ):
        raise FixtureTruthAuditError("fixture mismatch checklist drifted")
    material = audit.get("material_field_error_reclassifications")
    language = audit.get("language_tutor_structured_field_reclassifications")
    if (
        not isinstance(material, list)
        or len(material) != 10
        or sum(item.get("legacy_mutated_event_support") == "supported" for item in material)
        != 7
        or not isinstance(language, list)
        or len(language) != 6
    ):
        raise FixtureTruthAuditError("fixture reclassification coverage drifted")
    return audit


def build_fixture_truth_audit_receipt(
    *, repo_root: Path, output_path: Path, audit_path: Path = DEFAULT_FIXTURE_AUDIT_PATH
) -> dict[str, Any]:
    repo = repo_root.expanduser().resolve()
    output = output_path.expanduser().resolve()
    audit_file = audit_path.expanduser().resolve()
    audit = load_fixture_truth_audit(audit_file)
    v4_root = repo / "work/app-server-development-v2/unattended-pipeline-v4"
    calibration_root = v4_root / "development-selection-sharded-v3/calibration"
    report_path = calibration_root / "report.json"
    terminal_receipt_path = (
        repo / "work/app-server-development-v2/unattended-control-v5/terminal-receipt-v5.json"
    )
    pipeline_terminal_path = v4_root / "pipeline-terminal.json"
    declared = audit["pipeline_v4_evidence"]
    if (
        _sha256_file(report_path) != declared["calibration_report_sha256"]
        or _sha256_file(terminal_receipt_path) != declared["terminal_receipt_sha256"]
        or _sha256_file(pipeline_terminal_path) != declared["pipeline_terminal_sha256"]
    ):
        raise FixtureTruthAuditError("pipeline-v4 evidence hash drifted")
    forensics = recompute_v4_error_forensics(report_path)
    measured = measure_v4_prompt_compaction(calibration_root)
    expected_forensics = audit["v4_forensics"]
    if (
        forensics["ab"]["support_false_negative_count"]
        != expected_forensics["support_false_negatives_ab"]
        or forensics["ba"]["support_false_negative_count"]
        != expected_forensics["support_false_negatives_ba"]
        or forensics["ab"]["support_false_negative_by_shape"].get(
            "material_field_error"
        )
        != expected_forensics["material_field_error_false_negatives_ab"]
        or forensics["ba"]["support_false_negative_by_topic"].get("language_tutor")
        != expected_forensics["language_tutor_false_negatives_ba"]
        or forensics["orientation_changed_by_topic"].get("language_tutor")
        != expected_forensics["language_tutor_orientation_changed_case_count"]
        or forensics["ab"]["field_misses"].get("evidence")
        != expected_forensics["expected_evidence_mismatches"]
        or forensics["ba"]["field_misses"].get("evidence")
        != expected_forensics["expected_evidence_mismatches"]
        or measured != {
            "prompt_count": 22,
            "before_prompt_bytes": 401660,
            "after_prompt_bytes": 241992,
            "reduction_fraction": 0.3975,
            "empty_values_only": True,
            "semantic_pruning_performed": False,
        }
    ):
        raise FixtureTruthAuditError("fixture audit findings do not reproduce")
    payload = {
        "schema_version": FIXTURE_AUDIT_RECEIPT_VERSION,
        "status": "verified_before_pipeline_v5_semantic_calls",
        "fixture_audit": _file_record(audit_file),
        "source_fixture": _file_record(
            repo / audit["source_fixture"]["path"]
        ),
        "pipeline_v4_calibration_report": _file_record(report_path),
        "pipeline_v4_terminal_receipt": _file_record(terminal_receipt_path),
        "pipeline_v4_terminal": _file_record(pipeline_terminal_path),
        "forensics": forensics,
        "serialization_measurement": measured,
        "legacy_joint_support_labels_admissible": False,
        "truth_dimensions": audit["truth_dimensions"],
        "material_field_reclassification_count": len(
            audit["material_field_error_reclassifications"]
        ),
        "language_tutor_case_reclassification_count": len(
            audit["language_tutor_structured_field_reclassifications"]
        ),
        "mismatch_checklist_row_count": len(audit["mismatch_checklist"]),
        "semantic_model_calls_performed": 0,
        "production_mutation_performed": False,
        "privacy": "hashes_counts_metrics_case_categories_and_protocol_definitions_no_source_text",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    write_immutable_json(output, payload)
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify pipeline-v5 fixture truth audit")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", required=True)
    parser.add_argument("--audit")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    payload = build_fixture_truth_audit_receipt(
        repo_root=Path(args.repo_root),
        output_path=Path(args.output),
        audit_path=Path(args.audit) if args.audit else DEFAULT_FIXTURE_AUDIT_PATH,
    )
    print(
        json.dumps(
            {
                "ok": True,
                "schema_version": payload["schema_version"],
                "output": str(Path(args.output).expanduser().resolve()),
                "semantic_model_calls_performed": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
