from __future__ import annotations

"""Reject the v229 incremental strategy when its best-case alignment cannot pass."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .util import now_iso, write_text_atomic


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v231_gate_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v231_terminal_v1"
PHASE_ID = "development_selection_v5_4_v231_incremental_nonacceptance"
PROJECT_ROOT = Path("/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory")
PIPELINE_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
)
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v231-incremental-nonacceptance"
)
NONINFERIORITY_FLOOR = 0.97
TOKEN_RATIO_TARGET = 0.28

PREDECESSOR_FILES = {
    "v225_terminal": (
        PIPELINE_ROOT
        / "development-selection-v5_4-v225-alignment-transport-recovery"
        / "terminal.json",
        "4583418ce3380917c5b623917efe06d6ac5ee1a17574c892d593933358c8b7d1",
    ),
    "v225_score": (
        PIPELINE_ROOT
        / "development-selection-v5_4-v225-alignment-transport-recovery"
        / "alignment-score.json",
        "395fe03c4a5167e3a72e0443f26f1aef21f7352ff952d6da5eb679efa2ea6a9b",
    ),
    "v229_terminal": (
        PIPELINE_ROOT
        / "development-selection-v5_4-v229-metric-grounding-repair"
        / "terminal.json",
        "6ead7bb87f266c55e14c0a001463cc449314733044675dcda2a02f81a9fd2329",
    ),
    "v229_gate": (
        PIPELINE_ROOT
        / "development-selection-v5_4-v229-metric-grounding-repair"
        / "turns"
        / "v229-metric-grounding-repair"
        / "metric-repair-gate.json",
        "d7063a4cc5b51b1ca1c238924687bb63e0234f3d8f93c3b5ec750aa70399d2f9",
    ),
    "v230_terminal": (
        PIPELINE_ROOT
        / "development-selection-v5_4-v230-incremental-support"
        / "terminal.json",
        "e38a3f2b7e9c8cc180b90dffc8a4f4b6a47bdecbe3b9a7dc0bbbe5527d3e70f4",
    ),
    "v230_support": (
        PIPELINE_ROOT
        / "development-selection-v5_4-v230-incremental-support"
        / "support-audit.json",
        "fa5fa9e121df68370af99678ec5b3643581934948d9539536cc1e90ab2cfe7af",
    ),
}


class V231GateError(RuntimeError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _record(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    return {
        "path": str(resolved),
        "sha256": _sha256(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise V231GateError(f"{path.name} is not an object")
    return value


def _write_immutable(path: Path, value: Mapping[str, Any]) -> None:
    text = _canonical_json(dict(value)) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise V231GateError(f"immutable {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(path, text)


def validate_predecessors() -> dict[str, Any]:
    values: dict[str, Any] = {}
    records: dict[str, Any] = {}
    for name, (path, expected_sha256) in PREDECESSOR_FILES.items():
        if not path.is_file() or _sha256(path) != expected_sha256:
            raise V231GateError(f"{name} drifted")
        values[name] = _load(path)
        records[name] = _record(path)

    score = values["v225_score"]
    v229_terminal = values["v229_terminal"]
    v229_gate = values["v229_gate"]
    v230_terminal = values["v230_terminal"]
    support = values["v230_support"]
    if (
        score.get("passed") is not False
        or score.get("metrics", {}).get("reference_semantic_unit_count") != 50
        or score.get("metrics", {}).get("represented_reference_semantic_unit_count")
        != 16
        or score.get("metrics", {}).get("permutation_exact_case_count") != 1
        or score.get("metrics", {}).get("permutation_case_count") != 3
        or v229_terminal.get("state") != "v229_metric_grounding_repair_passed"
        or v229_terminal.get("support_alignment_authorized") is not True
        or v229_gate.get("passed") is not True
        or v229_gate.get("production_amortized_total_token_ratio") != 0.268681
        or v230_terminal.get("state") != "v230_incremental_support_passed"
        or v230_terminal.get("alignment_authorized") is not True
        or support.get("passed") is not True
        or support.get("support_status_counts") != {"supported": 2}
    ):
        raise V231GateError("incremental strategy predecessor contract drifted")
    return {"values": values, "records": records}


def _f1(recall: float) -> float:
    return 1.0 if recall == 0 else (2.0 * recall) / (1.0 + recall)


def build_gate(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    score = predecessor["values"]["v225_score"]
    additions = {
        "seg_afbf7ba7ce69db15f02bd14e": 1,
        "seg_c84d5569778c65202b572971": 1,
    }
    cases = []
    for row in score["case_scores"]:
        reference_count = int(row["reference_semantic_unit_count"])
        represented_before = int(row["represented_reference_semantic_unit_count"])
        new_witness_count = additions.get(str(row["segment_id"]), 0)
        represented_ceiling = min(
            reference_count,
            represented_before + new_witness_count,
        )
        recall_ceiling = (
            represented_ceiling / reference_count if reference_count else 1.0
        )
        cases.append(
            {
                "segment_id": row["segment_id"],
                "source_id": row["source_id"],
                "density_stratum": row["density_stratum"],
                "reference_semantic_unit_count": reference_count,
                "represented_reference_before": represented_before,
                "new_support_positive_witness_count": new_witness_count,
                "represented_reference_ceiling": represented_ceiling,
                "reference_recall_ceiling": round(recall_ceiling, 6),
                "semantic_f1_ceiling": round(_f1(recall_ceiling), 6),
            }
        )
    macro_ceiling = round(
        sum(float(row["semantic_f1_ceiling"]) for row in cases) / len(cases),
        6,
    )
    source_values: dict[str, list[float]] = {}
    for row in cases:
        source_values.setdefault(str(row["source_id"]), []).append(
            float(row["semantic_f1_ceiling"])
        )
    source_macro = {
        source_id: round(sum(values) / len(values), 6)
        for source_id, values in sorted(source_values.items())
    }
    checks = {
        "v229_structural_gate_passed": True,
        "v230_side_free_support_gate_passed": True,
        "two_support_positive_additions_only": sum(additions.values()) == 2,
        "frozen_one_to_one_alignment_contract": True,
        "best_case_macro_f1_gte_0_97": macro_ceiling >= NONINFERIORITY_FLOOR,
        "best_case_no_material_source_macro_regression": all(
            value >= NONINFERIORITY_FLOOR for value in source_macro.values()
        ),
        "frozen_permutation_projection_exact": score["metrics"]
        ["permutation_exact_case_count"]
        == score["metrics"]["permutation_case_count"],
        "production_amortized_total_token_ratio_lte_0_28": predecessor["values"]
        ["v229_gate"]["production_amortized_total_token_ratio"]
        <= TOKEN_RATIO_TARGET,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": PHASE_ID,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(name for name, passed in checks.items() if not passed),
        "frozen_alignment_contract": {
            "assignment": "one_to_one",
            "maximum_new_reference_units_per_new_witness": 1,
            "semantic_alignment_turn_required_to_reject": False,
            "reason": "best_case_upper_bound_is_below_frozen_acceptance_floor",
        },
        "case_ceilings": cases,
        "metrics": {
            "prior_macro_f1": score["metrics"][
                "development_supported_event_semantic_macro_f1"
            ],
            "best_case_macro_f1": macro_ceiling,
            "noninferiority_floor": NONINFERIORITY_FLOOR,
            "best_case_source_macro_f1": source_macro,
            "prior_permutation_exact_case_count": score["metrics"]
            ["permutation_exact_case_count"],
            "prior_permutation_case_count": score["metrics"]["permutation_case_count"],
            "projected_production_amortized_total_token_ratio": predecessor["values"]
            ["v229_gate"]["production_amortized_total_token_ratio"],
        },
        "semantic_alignment_of_new_witnesses_measured": False,
        "new_model_turn_count": 0,
        "new_model_usage": {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        },
        "incremental_exact_span_field_repair_strategy_accepted": False,
        "architecture_level_redesign_authorized": True,
        "further_isolated_field_repair_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "sanitized_counts_metrics_and_hash_bound_lineage_only",
    }


def freeze_v231(output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load(terminal_path)
    if root.exists() and any(root.iterdir()):
        raise V231GateError("v231 root is nonempty without a terminal")
    predecessor = validate_predecessors()
    gate = build_gate(predecessor)
    gate_path = root / "alignment-ceiling-gate.json"
    _write_immutable(gate_path, gate)
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "state": "incremental_strategy_not_accepted",
        "terminal_at": now_iso(),
        "terminal_reason": "v231_incremental_strategy_best_case_quality_ceiling_not_passed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "predecessor_records": predecessor["records"],
        "gate": _record(gate_path),
        "semantic_attempt_count": 0,
        "semantic_retry_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": gate["new_model_usage"],
        "incremental_exact_span_field_repair_strategy_accepted": False,
        "architecture_level_redesign_authorized": True,
        "further_isolated_field_repair_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "exact_next_action": "one_predeclared_architecture_level_redesign",
    }
    _write_immutable(terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v231 incremental nonacceptance")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    result = freeze_v231(Path(args.output_dir))
    print(_canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
