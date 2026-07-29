from __future__ import annotations

"""Recover the completed v177 turns from its local normalized-score KeyError."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_selection_v177_alignment_scale_diagnostic as v177
from .app_server_judge_v5 import normalize_neutral_alignment_output
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .util import now_iso, sha256_text


V178_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v178_spec_v1"
V178_SCORE_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v178_score_v1"
V178_RECEIPT_VERSION = "pif_app_server_judge_v5_4_selection_alignment_scale_receipt_v1"
V178_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v178_terminal_v1"
V178_PHASE_ID = "judge_v5_4_selection_v178_alignment_scale_postprocess_recovery"
DEFAULT_OUTPUT_ROOT = (
    v177.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v178-alignment-scale-postprocess-recovery"
).resolve()


class JudgeV5SelectionV178Error(RuntimeError):
    """The immutable zero-token v178 recovery cannot be preserved."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v177_failure() -> dict[str, Any]:
    root = v177.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "selection-alignment-scale-diagnostic-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "base_input": root / "turns/selection-alignment-scale-base/input.private.json",
        "base_output": root / "turns/selection-alignment-scale-base/output.private.json",
        "base_projected": root
        / "turns/selection-alignment-scale-base/alignment-projected.private.json",
        "base_projection_audit": root
        / "turns/selection-alignment-scale-base/structural-projection-audit.json",
        "canary_input": root / "turns/selection-alignment-scale-canary/input.private.json",
        "canary_output": root / "turns/selection-alignment-scale-canary/output.private.json",
        "canary_projected": root
        / "turns/selection-alignment-scale-canary/alignment-projected.private.json",
        "canary_projection_audit": root
        / "turns/selection-alignment-scale-canary/structural-projection-audit.json",
    }
    values = {name: _load_json(path, f"v177 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    expected_usage = {
        "input_tokens": 114770,
        "cached_input_tokens": 3840,
        "output_tokens": 58765,
        "reasoning_output_tokens": 11910,
        "total_tokens": 173535,
    }
    expected_turn_usage = {
        v177.TURN_NAMES[0]: {
            "input_tokens": 57385,
            "cached_input_tokens": 1920,
            "output_tokens": 27329,
            "reasoning_output_tokens": 4142,
            "total_tokens": 84714,
        },
        v177.TURN_NAMES[1]: {
            "input_tokens": 57385,
            "cached_input_tokens": 1920,
            "output_tokens": 31436,
            "reasoning_output_tokens": 7768,
            "total_tokens": 88821,
        },
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != expected_usage
        or terminal.get("full_alignment_authorized") is not False
        or terminal.get("selection_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != v177.TURN_NAMES[1]
        or failure.get("error_class") != "KeyError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("usage_status") != "complete"
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("usage") != expected_usage
        or spec.get("turn_plan") != list(v177.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("lossless_compact_serialization") is not True
        or spec.get("semantic_fields_pruned") is not False
        or spec.get("full_alignment_authorized") is not False
    ):
        raise JudgeV5SelectionV178Error("v177 failure contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV178Error("v177 runtime record drifted")
    for row in spec["frozen_inputs"]["turns"]:
        for key in ("input", "prompt", "schema", "serialization_audit"):
            if not _verify_record(row[key]):
                raise JudgeV5SelectionV178Error("v177 frozen request record drifted")

    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    if len(attempts) != 2 or {row["turn_name"] for row in attempts} != set(v177.TURN_NAMES):
        raise JudgeV5SelectionV178Error("v177 attempt coverage drifted")
    observed_usage = {field: 0 for field in USAGE_FIELDS}
    attempts_by_name = {}
    for attempt in attempts:
        turn_name = str(attempt["turn_name"])
        for key in ("capacity", "sidecar", "output"):
            record = attempt.get(key)
            if not isinstance(record, Mapping) or not _verify_record(record):
                raise JudgeV5SelectionV178Error(f"v177 {key} record drifted")
        sidecar = _load_json(Path(attempt["sidecar"]["path"]), "v177 sidecar")
        usage = _validate_usage(sidecar)
        if usage != expected_turn_usage[turn_name]:
            raise JudgeV5SelectionV178Error("v177 turn usage drifted")
        for field in USAGE_FIELDS:
            observed_usage[field] += usage[field]
        attempts_by_name[turn_name] = attempt
    if observed_usage != expected_usage:
        raise JudgeV5SelectionV178Error("v177 aggregate usage drifted")

    normalized = []
    for prefix in ("base", "canary"):
        alignment_input = values[f"{prefix}_input"]
        raw_output = values[f"{prefix}_output"]
        projected, audit = v157.project_exact_spans_and_relation(raw_output, alignment_input)
        if (
            projected != values[f"{prefix}_projected"]
            or audit != values[f"{prefix}_projection_audit"]
        ):
            raise JudgeV5SelectionV178Error("v177 structural projection drifted")
        normalized.append(normalize_neutral_alignment_output(projected, alignment_input))

    predecessor = v177._validate_v176_success()
    cumulative = _sum_usage(predecessor["cumulative_usage"], expected_usage)
    if (
        failure.get("predecessor_cumulative_usage") != predecessor["cumulative_usage"]
        or failure.get("cumulative_known_usage_lower_bound") != cumulative
        or terminal.get("cumulative_known_usage_lower_bound") != cumulative
        or terminal.get("failure") != _record(paths["failure"])
    ):
        raise JudgeV5SelectionV178Error("v177 cumulative lineage drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts_by_name,
        "normalized_base": normalized[0],
        "normalized_canary": normalized[1],
        "usage": expected_usage,
        "cumulative_usage": cumulative,
        "v176": predecessor,
    }


def score_v178(base: Mapping[str, Any], canary: Mapping[str, Any]) -> dict[str, Any]:
    base_case = base["cases"][0]
    canary_case = canary["cases"][0]
    base_projection = v130._project_alignment(base_case)
    canary_projection = v130._project_alignment(canary_case)
    pairs = base_case["alignment_pairs"]
    abstained = sum(
        pair["relation"] == "abstain"
        or any(value == "abstain" for value in pair["checklist_decisions"].values())
        for pair in pairs
    )
    checks = {
        "base_canary_projection_exact": base_projection == canary_projection,
        "minimum_alignment_pair_count": len(pairs) >= 1,
        "abstained_pair_count": abstained == 0,
    }
    return {
        "schema_version": V178_SCORE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "base_canary_projection_exact": base_projection == canary_projection,
        "alignment_pair_count": len(pairs),
        "canary_alignment_pair_count": len(canary_case["alignment_pairs"]),
        "abstained_pair_count": abstained,
        "base_equivalence_group_count": len(base_case["equivalence_groups"]),
        "base_unpaired_witness_count": len(base_case["unpaired_witness_ids"]),
        "normalized_checklist_field_used": "checklist_decisions",
    }


def freeze_v178(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v178 terminal")}
    predecessor = _validate_v177_failure()
    score = score_v178(predecessor["normalized_base"], predecessor["normalized_canary"])
    if not score["passed"]:
        raise JudgeV5SelectionV178Error("v177 completed outputs do not pass the frozen scale gate")
    score_path = root / "selection-alignment-scale-postprocess-score.json"
    _write_immutable(score_path, score)
    receipt = {
        "schema_version": V178_RECEIPT_VERSION,
        "frozen_at": now_iso(),
        "diagnostic_passed": True,
        "model": v177.MODEL,
        "reasoning_effort": v177.EFFORT,
        "alignment_instructions_sha256": sha256_text(v130.alignment_instructions_v130()),
        "lossless_compact_serialization": True,
        "semantic_fields_pruned": False,
        "base_canary_projection_exact": True,
        "alignment_pair_count": score["alignment_pair_count"],
        "abstained_pair_count": score["abstained_pair_count"],
        "maximum_observed_total_tokens_per_turn": max(
            _validate_usage(
                _load_json(Path(attempt["sidecar"]["path"]), "v177 receipt sidecar")
            )["total_tokens"]
            for attempt in predecessor["attempts"].values()
        ),
        "v177_terminal": predecessor["records"]["terminal"],
        "v177_failure": predecessor["records"]["failure"],
        "v177_base_projected": predecessor["records"]["base_projected"],
        "v177_canary_projected": predecessor["records"]["canary_projected"],
        "score": _record(score_path),
        "full_alignment_authorized": True,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    receipt_path = root / "alignment-scale-diagnostic-receipt.json"
    _write_immutable(receipt_path, receipt)
    spec = {
        "schema_version": V178_SPEC_VERSION,
        "state": "zero_token_postprocess_recovery",
        "created_at": now_iso(),
        "phase_id": V178_PHASE_ID,
        "semantic_turn_count": 0,
        "semantic_usage": {field: 0 for field in USAGE_FIELDS},
        "v177_turns_replayed": 0,
        "v177_completed_outputs_reused": 2,
        "correction": "read_normalized_checklist_decisions_instead_of_removed_raw_checklist",
        "support_receipts_frozen": True,
        "full_alignment_authorized": True,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "runtime_files": [_record(Path(__file__)), _record(Path(v177.__file__))],
        "predecessor": predecessor["records"],
        "score": _record(score_path),
        "receipt": _record(receipt_path),
        "privacy": "private_outputs_reused_sanitized_counts_hashes_only",
    }
    spec_path = root / "selection-alignment-scale-postprocess-recovery-spec.json"
    _write_immutable(spec_path, spec)
    zero_usage = {field: 0 for field in USAGE_FIELDS}
    terminal = {
        "schema_version": V178_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v178_alignment_scale_postprocess_passed_full_alignment_authorized",
        "overall_evaluation_complete": False,
        "support_receipts_frozen": True,
        "full_alignment_authorized": True,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "semantic_turn_count": 0,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": zero_usage,
        "predecessor_v177_usage": predecessor["usage"],
        "cumulative_evaluation_usage": predecessor["cumulative_usage"],
        "score": _record(score_path),
        "receipt": _record(receipt_path),
    }
    _write_immutable(terminal_path, terminal)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "score": score,
        "receipt": receipt,
        "receipt_path": receipt_path,
        "terminal": terminal,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Freeze v178 alignment postprocess recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    frozen = freeze_v178(output_dir=Path(args.output_dir))
    terminal = frozen["terminal"]
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "full_alignment_authorized": terminal["full_alignment_authorized"],
                "semantic_turn_count": terminal["semantic_turn_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
