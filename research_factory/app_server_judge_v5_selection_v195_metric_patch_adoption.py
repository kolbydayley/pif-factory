from __future__ import annotations

"""Adopt the completed v194 patch output without replaying its failed version."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v193_residual_repair_diagnostic as v193
from . import app_server_judge_v5_selection_v194_metric_patch_repair as v194
from .app_server_evaluation import normalize_episode_batch_output
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .util import now_iso


V195_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v195_adoption_audit_v1"
V195_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v195_spec_v1"
V195_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v195_terminal_v1"
DEFAULT_OUTPUT_ROOT = (
    v194.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v195-metric-patch-adoption"
).resolve()


class JudgeV5SelectionV195Error(RuntimeError):
    """The completed v194 output cannot be adopted safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v194_completed_failed_attempt() -> dict[str, Any]:
    root = v194.DEFAULT_OUTPUT_ROOT
    terminal_path = root / "terminal.json"
    failure_path = root / "failure.json"
    spec_path = root / "metric-patch-attempt-spec.json"
    turn_root = root / "turns" / v194.TURN_NAME.replace("_", "-")
    sidecar_path = turn_root / "sidecar.json"
    capacity_path = turn_root / "capacity.json"
    output_path = turn_root / "output.private.json"
    input_path = turn_root / "input.private.json"
    prompt_path = turn_root / "prompt.private.md"
    schema_path = turn_root / "schema.json"
    terminal = _load_json(terminal_path, "v194 terminal")
    failure = _load_json(failure_path, "v194 failure")
    spec = _load_json(spec_path, "v194 spec")
    sidecar = _load_json(sidecar_path, "v194 sidecar")
    capacity = _load_json(capacity_path, "v194 capacity")
    output = _load_json(output_path, "v194 patch output")
    usage = _validate_usage(sidecar)
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_semantic_attempt_failed"
        or terminal.get("failure") != _record(failure_path)
        or terminal.get("support_judge_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != usage
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 8367769
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or failure.get("classification") != "infrastructure_or_semantic_attempt_failed"
        or failure.get("error_class") != "ReserveCapacityError"
        or failure.get("failed_turn_name") != v194.TURN_NAME
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("accounting_complete") is not True
        or failure.get("usage_status") != "complete"
        or failure.get("usage") != usage
        or failure.get("unknown_usage_turn_count") != 0
        or usage.get("total_tokens") != 22368
        or spec.get("state") != "frozen_before_model_call"
        or spec.get("turn_plan") != [v194.TURN_NAME]
        or spec.get("repair_packet_count") != 5
        or spec.get("retry_count_per_turn") != 0
        or spec.get("metric_fields_only") is not True
        or spec.get("event_claim_or_evidence_changes_allowed") is not False
        or spec.get("extraction_replay_allowed") is not False
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("cli_version") != "0.144.1"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
    ):
        raise JudgeV5SelectionV195Error("v194 completed-failure contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV195Error("v194 runtime binding drifted")
    for path in (input_path, prompt_path, schema_path, output_path, sidecar_path, capacity_path):
        if not path.is_file():
            raise JudgeV5SelectionV195Error("v194 turn artifact disappeared")
    predecessor = v194._validate_v193_metric_failure()
    packets = v194._metric_repair_packets(predecessor)
    if v194.validate_metric_patch_output(output, packets):
        raise JudgeV5SelectionV195Error("v194 completed patch output is invalid")
    expected_cumulative = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], usage
    )
    if terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative:
        raise JudgeV5SelectionV195Error("v194 cumulative accounting drifted")
    return {
        "root": root,
        "terminal": terminal,
        "failure": failure,
        "spec": spec,
        "sidecar": sidecar,
        "capacity": capacity,
        "output": output,
        "usage": usage,
        "predecessor": predecessor,
        "packets": packets,
        "records": {
            "terminal": _record(terminal_path),
            "failure": _record(failure_path),
            "spec": _record(spec_path),
            "sidecar": _record(sidecar_path),
            "capacity": _record(capacity_path),
            "output": _record(output_path),
            "input": _record(input_path),
            "prompt": _record(prompt_path),
            "schema": _record(schema_path),
        },
    }


def freeze_v195(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v195 terminal")
    predecessor = _validate_v194_completed_failed_attempt()
    repaired = v194._apply_patches(
        output=predecessor["predecessor"]["output"],
        packets=predecessor["packets"],
        patches=predecessor["output"],
    )
    prepared = [
        {
            "segment_id": row["case_id"],
            "segment_text": row["source_excerpt"],
            "boundaries": row["boundaries"],
        }
        for row in predecessor["predecessor"]["predecessor"]["private_input"]["cases"]
    ]
    normalized, diagnostics = normalize_episode_batch_output(
        repaired,
        episode_id=v192.DIAGNOSTIC_EPISODE_ID,
        prepared_segments=prepared,
        max_events_per_segment=v192.MAX_EVENTS_PER_CASE,
    )
    repaired_path = root / "residual-repair-metric-patched-output.private.json"
    diagnostics_path = root / "metric-patched-structural-diagnostics.json"
    _write_immutable(repaired_path, normalized)
    _write_immutable(diagnostics_path, {"segments": diagnostics})
    combined_usage = _sum_usage(
        predecessor["predecessor"]["usage"], predecessor["usage"]
    )
    gate = v193._phase_one_gate(
        normalized=normalized,
        diagnostics=diagnostics,
        usage=combined_usage,
        predecessor=predecessor["predecessor"]["predecessor"],
    )
    gate = {
        **gate,
        "schema_version": V195_AUDIT_VERSION,
        "v193_repair_usage": predecessor["predecessor"]["usage"],
        "v194_metric_patch_usage": predecessor["usage"],
        "combined_repair_usage": combined_usage,
        "v194_output_adopted_without_replay": True,
        "v194_terminal_failure_preserved": True,
        "v194_local_phase_bound_exceeded": True,
        "v194_usage_complete": True,
        "metric_patch_count": len(predecessor["packets"]),
        "non_metric_event_fields_changed": False,
    }
    if gate.get("passed") is not True:
        raise JudgeV5SelectionV195Error("adopted metric patches did not clear phase-one gates")
    gate_path = root / "metric-patch-adoption-gate.json"
    _write_immutable(gate_path, gate)
    spec = {
        "schema_version": V195_SPEC_VERSION,
        "state": "zero_model_call_adoption_completed",
        "created_at": now_iso(),
        "semantic_model_calls_declared": 0,
        "semantic_model_calls_started": 0,
        "v194_turn_replayed": False,
        "v194_failed_version_mutated": False,
        "adoption_scope": "completed_valid_metric_patch_output_with_measured_usage",
        "deterministic_operations": [
            "verify_v194_failed_terminal_and_complete_sidecar",
            "validate_metric_patch_literal_grounding",
            "apply_metric_fields_only",
            "recompute_exact_evidence_metric_cap_no_signal_and_token_gates",
        ],
        "semantic_regex_or_keyword_rules_used": False,
        "semantic_similarity_or_embeddings_used": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v194.__file__)),
            _record(Path(v193.__file__)),
            _record(Path(v192.__file__)),
        ],
        "outputs": {
            "repaired_output": _record(repaired_path),
            "diagnostics": _record(diagnostics_path),
            "gate": _record(gate_path),
        },
        "privacy": "private_events_separate_sanitized_counts_hashes_metrics_only",
    }
    spec_path = root / "metric-patch-adoption-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    terminal = {
        "schema_version": V195_TERMINAL_VERSION,
        "state": "completed",
        "terminal_at": now_iso(),
        "terminal_reason": "v195_adopted_metric_patch_cleared_phase_one_gates_support_judge_authorized",
        "overall_evaluation_complete": False,
        "v194_turn_replayed": False,
        "v194_failure_preserved": True,
        "repaired_output": _record(repaired_path),
        "structural_diagnostics": _record(diagnostics_path),
        "phase_one_gate": _record(gate_path),
        "support_judge_authorized": True,
        "alignment_judge_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "attempt_usage_status": "complete",
        "attempt_usage": {field: 0 for field in USAGE_FIELDS},
        "adopted_v194_usage": predecessor["usage"],
        "combined_repair_usage": combined_usage,
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": predecessor["terminal"][
            "cumulative_known_usage_lower_bound"
        ],
        "cumulative_unknown_usage_turn_count": 1,
        "cumulative_conservative_unknown_usage_upper_bound": 120000,
        "spec": _record(spec_path),
        "required_next_artifact_path": str(
            root.parent
            / "development-selection-v5_4-v196-residual-repair-support"
            / "terminal.json"
        ),
    }
    _write_stable_time(terminal_path, terminal, "terminal_at")
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Adopt completed v194 metric patches")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = freeze_v195(output_dir=Path(args.output_dir))
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "v194_turn_replayed": terminal["v194_turn_replayed"],
                "support_judge_authorized": terminal["support_judge_authorized"],
                "holdout_authorized": terminal["holdout_authorized"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
