from __future__ import annotations

"""One capped independent alignment repair for the sole v117 trigger."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v108_layered_diagnostic as v108
from .app_server_judge_v5 import (
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _validate_scoreable_alignment_output,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v110_sol_reference_audit import (
    _project_alignment,
)
from .app_server_judge_v5_calibration_v116_capped_contested_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V116_ROOT,
)
from .app_server_judge_v5_calibration_v117_alignment_reference_owner import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V117_ROOT,
    _case_has_abstention,
    _iter_records,
    _validate_v116,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V118_INPUT_VERSION = "pif_app_server_judge_v5_4_v118_capped_alignment_repair_input_v1"
V118_TRUTH_VERSION = "pif_app_server_judge_v5_4_v118_capped_alignment_repair_truth_v1"
V118_SELECTION_VERSION = "pif_app_server_judge_v5_4_v118_selection_v1"
V118_SPEC_VERSION = "pif_app_server_judge_v5_4_v118_spec_v1"
V118_SCORE_VERSION = "pif_app_server_judge_v5_4_v118_score_v1"
V118_REFERENCE_VERSION = "pif_app_server_judge_v5_4_calibration_truth_v9_frozen"
V118_FAILURE_VERSION = "pif_app_server_judge_v5_4_v118_failure_v1"
V118_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v118_terminal_v1"
V118_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V118_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V118_PHASE_ID = "judge_v5_4_v118_capped_alignment_repair"

MODEL = "gpt-5.4"
EFFORT = "high"
TURN_NAME = "capped_alignment_repair"
TIMEOUT_SECONDS = 900.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V117_ROOT.parent / "judge-calibration-v5_4-v118-capped-alignment-repair"
).resolve()


class JudgeV5CalibrationV118Error(RuntimeError):
    """The v118 capped alignment repair cannot preserve its frozen contract."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v117() -> dict[str, Any]:
    root = DEFAULT_V117_ROOT
    paths = {
        "spec": root / "alignment-owner-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "alignment-owner-score.json",
        "input": root / "alignment-owner-input.private.json",
        "truth": root / "alignment-owner-truth.private.json",
        "primary": root / "alignment-owner-normalized.private.json",
        "canary": root / "alignment-owner-canary-normalized.private.json",
    }
    values = {name: _load_json(path, f"v117 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    triggers = score.get("observable_repair_triggers") or []
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v117_alignment_owner_repair_or_recovery_required"
        or terminal.get("capped_repair_authorized") is not True
        or terminal.get("reference_frozen") is not False
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 86895
        or score.get("passed") is not False
        or score.get("capped_repair_authorized") is not True
        or score.get("metrics", {}).get("unanimous_control_exact_count") != 6
        or score.get("metrics", {}).get("permutation_canary_exact_count") != 5
        or score.get("metrics", {}).get("primary_abstention_case_count") != 0
        or score.get("metrics", {}).get("observable_repair_trigger_count") != 1
        or len(triggers) != 1
        or triggers[0].get("reasons")
        != ["canary_disagreement", "unanimous_control_mismatch"]
        or spec.get("model") != "gpt-5.6-terra"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV118Error("v117 predecessor contract drifted")
    if not all(_verify_record(record) for record in _iter_records(spec)):
        raise JudgeV5CalibrationV118Error("v117 frozen record drifted")
    usage = {field: 0 for field in USAGE_FIELDS}
    turn_records = {}
    for turn_name in spec["turn_plan"]:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        turn_paths = {
            name: turn_root / filename
            for name, filename in {
                "capacity": "capacity.json",
                "input": "input.private.json",
                "prompt": "prompt.private.md",
                "schema": "schema.json",
                "sidecar": "sidecar.json",
                "output": "output.private.json",
            }.items()
        }
        if any(not path.is_file() for path in turn_paths.values()):
            raise JudgeV5CalibrationV118Error("v117 turn coverage is incomplete")
        measured = _validate_usage(_load_json(turn_paths["sidecar"], "v117 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        turn_records[turn_name] = {
            name: _record(path) for name, path in turn_paths.items()
        }
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV118Error("v117 usage aggregate drifted")
    trigger_id = str(triggers[0]["case_id"])
    truth_map = {str(row["case_id"]): row for row in values["truth"]["cases"]}
    primary_map = {str(row["case_id"]): row for row in values["primary"]["cases"]}
    canary_map = {str(row["case_id"]): row for row in values["canary"]["cases"]}
    if (
        truth_map[trigger_id]["role"] != "unanimous_control"
        or _project_alignment(primary_map[trigger_id])
        == truth_map[trigger_id]["current_reference"]
        or _project_alignment(canary_map[trigger_id])
        != truth_map[trigger_id]["current_reference"]
    ):
        raise JudgeV5CalibrationV118Error("v117 sole trigger semantics drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "turn_records": turn_records,
        "usage": usage,
        "trigger_id": trigger_id,
    }


def build_v118_input(
    v117: Mapping[str, Any], current_reference: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source = {
        str(row["case_id"]): deepcopy(row)
        for row in v117["values"]["input"]["cases"]
    }
    truth_map = {
        str(row["case_id"]): row for row in v117["values"]["truth"]["cases"]
    }
    primary = {
        str(row["case_id"]): row for row in v117["values"]["primary"]["cases"]
    }
    trigger_id = v117["trigger_id"]
    controls = [
        case_id
        for case_id, row in truth_map.items()
        if case_id != trigger_id
        and row["role"] == "unanimous_control"
        and _project_alignment(primary[case_id]) == row["current_reference"]
    ]
    controls.sort(
        key=lambda case_id: (
            sha256_text(
                f"v118|control|{current_reference['cases'][case_id].get('shape')}|{case_id}"
            )
        )
    )
    controls = controls[:4]
    if len(controls) != 4:
        raise JudgeV5CalibrationV118Error("v118 matched control coverage drifted")
    selected = [trigger_id] + controls
    value = {
        key: deepcopy(item)
        for key, item in v117["values"]["input"].items()
        if key != "cases"
    }
    value["owner_protocol_version"] = V118_INPUT_VERSION
    value["permutation"] = "v118_capped_side_free_owner"
    value["permuted_axes"] = ["case_order"]
    value["cases"] = [
        source[case_id]
        for case_id in sorted(
            selected, key=lambda case_id: sha256_text(f"v118|order|{case_id}")
        )
    ]
    truth = {
        "schema_version": V118_TRUTH_VERSION,
        "case_count": 5,
        "repair_case_count": 1,
        "matched_control_count": 4,
        "cases": [
            {
                "case_id": case_id,
                "role": "observable_repair" if case_id == trigger_id else "matched_control",
                "current_reference": truth_map[case_id]["current_reference"],
            }
            for case_id in sorted(selected)
        ],
    }
    selection = {
        "schema_version": V118_SELECTION_VERSION,
        "created_at": now_iso(),
        "case_count": 5,
        "observable_repair_count": 1,
        "matched_control_count": 4,
        "source_trigger_reasons": [
            "canary_disagreement",
            "unanimous_control_mismatch",
        ],
        "owner_is_side_free": True,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "majority_voting_used": False,
        "selection_uses_source_text": False,
        "privacy": "aggregate_counts_only",
    }
    return value, truth, selection


def score_v118(output: Mapping[str, Any], truth: Mapping[str, Any]) -> dict[str, Any]:
    observed = {str(row["case_id"]): row for row in output["cases"]}
    expected = {str(row["case_id"]): row for row in truth["cases"]}
    if set(observed) != set(expected):
        raise JudgeV5CalibrationV118Error("v118 score coverage drifted")
    controls = [row for row in expected.values() if row["role"] == "matched_control"]
    repairs = [row for row in expected.values() if row["role"] == "observable_repair"]
    control_exact = sum(
        _project_alignment(observed[row["case_id"]]) == row["current_reference"]
        for row in controls
    )
    abstention_cases = sum(_case_has_abstention(row) for row in observed.values())
    repair_decisive = len(repairs) == 1 and not _case_has_abstention(
        observed[repairs[0]["case_id"]]
    )
    checks = {
        "matched_control_exact_rate": control_exact == 4,
        "repair_decisive": repair_decisive,
        "abstention_case_count": abstention_cases == 0,
        "schema_and_exact_evidence_validation": True,
    }
    return {
        "schema_version": V118_SCORE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": {
            "case_count": 5,
            "observable_repair_count": 1,
            "matched_control_count": 4,
            "matched_control_exact_count": control_exact,
            "abstention_case_count": abstention_cases,
        },
        "alignment_reference_frozen": all(checks.values()),
        "fresh_full_calibration_authorized": all(checks.values()),
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "majority_voting_used": False,
    }


def reconcile_reference(
    *,
    current_reference: Mapping[str, Any],
    v117_truth: Mapping[str, Any],
    v117_primary: Mapping[str, Any],
    v118_truth: Mapping[str, Any],
    v118_output: Mapping[str, Any],
) -> dict[str, Any]:
    candidate = deepcopy(current_reference)
    primary = {str(row["case_id"]): row for row in v117_primary["cases"]}
    repair = {str(row["case_id"]): row for row in v118_output["cases"]}
    patch_ids = {
        str(row["case_id"])
        for row in v117_truth["cases"]
        if row["role"] == "alignment_dispute"
    }
    repair_ids = {
        str(row["case_id"])
        for row in v118_truth["cases"]
        if row["role"] == "observable_repair"
    }
    if len(patch_ids) != 5 or len(repair_ids) != 1:
        raise JudgeV5CalibrationV118Error("v118 reconciliation coverage drifted")
    for case_id in patch_ids | repair_ids:
        projection = _project_alignment(
            repair[case_id] if case_id in repair_ids else primary[case_id]
        )
        case = candidate["cases"][case_id]
        case["pairs"] = deepcopy(projection["pairs"])
        case["equivalence_groups"] = deepcopy(projection["equivalence_groups"])
        case["unpaired_witness_ids"] = deepcopy(projection["unpaired_witness_ids"])
    candidate["schema_version"] = V118_REFERENCE_VERSION
    candidate["reference_version"] = "fixture_reference_v9_pointwise_alignment_and_capped_owner_frozen"
    candidate["pointwise_reference_patch_authorized"] = True
    candidate["alignment_reference_frozen"] = True
    candidate["reference_frozen"] = True
    candidate["alignment_primary_owner_case_count"] = 5
    candidate["alignment_capped_repair_case_count"] = 1
    candidate["fresh_full_calibration_authorized"] = True
    return candidate


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V118_CAPACITY_AUDIT_VERSION,
        "phase_id": V118_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V118_CAPACITY_POLICY_VERSION,
        "phase_id": V118_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": math.ceil(
            MAX_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v118(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v118 terminal")}
    v117 = _validate_v117()
    v116 = _validate_v116()
    value, truth, selection = build_v118_input(
        v117, v116["values"]["reference"]
    )
    input_path = root / "capped-alignment-input.private.json"
    truth_path = root / "capped-alignment-truth.private.json"
    selection_path = root / "selection-audit.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    prompt = v108.build_alignment_prompt_v108(value)
    schema = neutral_alignment_output_schema(value)
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=value,
        prompt=prompt,
        schema=schema,
    )
    predecessor = {
        **{f"v117_{name}": record for name, record in v117["records"].items()},
        "v117_turns": v117["turn_records"],
        **{f"v116_{name}": record for name, record in v116["records"].items()},
        "v116_turn": v116["turn_records"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V118_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_independent_side_free_owner_for_the_sole_v117_trigger_plus_four_controls",
        "case_count": 5,
        "observable_repair_count": 1,
        "matched_control_count": 4,
        "turn_plan": [TURN_NAME],
        "owner_is_side_free": True,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "four_exact_controls_decisive_repair_zero_abstentions_and_exact_schema_evidence",
        "pointwise_reference_patch_authorized": True,
        "alignment_reference_frozen": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v117_alignment_reference_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v116_capped_contested_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v108_layered_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "turn_input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_outputs_no_source_text_in_reports",
    }
    spec_path = root / "capped-alignment-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "value": value,
        "truth": truth,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "v117": v117,
        "v116": v116,
    }


def _write_failure(root: Path, error_class: str) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v118 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V118_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "known_usage_lower_bound": usage,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V118_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "pointwise_reference_patch_authorized": True,
        "alignment_reference_frozen": False,
        "reference_frozen": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v118(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v118 terminal")
    frozen = freeze_v118(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=v108.alignment_instructions_v108(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=5,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: _validate_scoreable_alignment_output(
                    candidate, frozen["value"]
                ),
            )
        output_path = root / "capped-alignment-output.private.json"
        normalized_path = root / "capped-alignment-normalized.private.json"
        _write_immutable(output_path, output)
        normalized = normalize_neutral_alignment_output(output, frozen["value"])
        _write_immutable(normalized_path, normalized)
        score = score_v118(normalized, frozen["truth"])
        score_path = root / "capped-alignment-score.json"
        _write_immutable(score_path, score)
        reference_path = root / "fixture-reference-v9-frozen.private.json"
        if score["passed"]:
            reference = reconcile_reference(
                current_reference=frozen["v116"]["values"]["reference"],
                v117_truth=frozen["v117"]["values"]["truth"],
                v117_primary=frozen["v117"]["values"]["primary"],
                v118_truth=frozen["truth"],
                v118_output=normalized,
            )
            _write_immutable(reference_path, reference)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V118_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v118_capped_alignment_repair_passed_reference_frozen"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v118_reference_frozen_full_calibration_authorized"
                if passed
                else "v118_capped_alignment_repair_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "pointwise_reference_patch_authorized": True,
            "alignment_reference_frozen": passed,
            "reference_frozen": passed,
            "fresh_full_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "reference": _record(reference_path) if passed else None,
            "repair_output": _record(output_path),
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v117_usage": frozen["v117"]["usage"],
            "predecessor_v116_usage": frozen["v116"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v118 capped alignment repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v118(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "reference_frozen": terminal.get("reference_frozen", False),
                "fresh_full_calibration_authorized": terminal.get(
                    "fresh_full_calibration_authorized", False
                ),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
