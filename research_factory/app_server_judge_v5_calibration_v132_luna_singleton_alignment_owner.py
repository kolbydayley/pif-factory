from __future__ import annotations

"""Fresh Luna singleton owner after the v131 GPT-5.4 control failure."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import neutral_alignment_output_schema, normalize_neutral_alignment_output
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
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
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import _record, _verify_record
from .app_server_judge_v5_calibration_v130_retained_alignment_owner import (
    _support_only_projection,
    alignment_instructions_v130,
    alignment_prompt_v130,
)
from .app_server_judge_v5_calibration_v131_singleton_alignment_repair import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V131_ROOT,
    _validate_v130,
    reconcile_alignment_reference as reconcile_alignment_reference_v131,
    score_v131,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V132_TRUTH_VERSION = "pif_app_server_judge_v5_4_v132_luna_singleton_truth_v1"
V132_SELECTION_VERSION = "pif_app_server_judge_v5_4_v132_selection_v1"
V132_SPEC_VERSION = "pif_app_server_judge_v5_4_v132_spec_v1"
V132_SCORE_VERSION = "pif_app_server_judge_v5_4_v132_score_v1"
V132_OUTPUT_VERSION = "pif_app_server_judge_v5_4_v132_luna_singleton_output_v1"
V132_REFERENCE_VERSION = (
    "pif_app_server_judge_v5_4_calibration_truth_v10_retained_alignment_luna_singleton_frozen"
)
V132_FAILURE_VERSION = "pif_app_server_judge_v5_4_v132_failure_v1"
V132_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v132_terminal_v1"
V132_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V132_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V132_PHASE_ID = "judge_v5_4_v132_luna_singleton_alignment_owner"

MODEL = "gpt-5.6-luna"
EFFORT = "high"
CONTROL_TURNS = tuple(f"luna_singleton_alignment_control_{index:02d}" for index in range(3))
OWNER_TURN = "luna_singleton_alignment_owner"
TURN_NAMES = CONTROL_TURNS + (OWNER_TURN,)
CONTROL_COUNT = 3
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V131_ROOT.parent / "judge-calibration-v5_4-v132-luna-singleton-alignment-owner"
).resolve()


class JudgeV5CalibrationV132Error(RuntimeError):
    """The v132 Luna singleton alignment contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _validate_v131() -> dict[str, Any]:
    root = DEFAULT_V131_ROOT
    paths = {
        "spec": root / "singleton-alignment-repair-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "singleton-alignment-repair-score.json",
        "outputs": root / "singleton-alignment-repair-outputs.private.json",
        "truth": root / "singleton-alignment-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v131 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or terminal.get("development_terminal_reason")
        != "v131_singleton_alignment_repair_quality_gate_not_passed"
        or terminal.get("alignment_reference_frozen") is not False
        or terminal.get("fresh_diagnostic_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 113818
        or score.get("passed") is not False
        or score.get("failed_checks") != ["singleton_control_exact_rate"]
        or score.get("metrics", {}).get("singleton_control_exact_count") != 2
        or score.get("metrics", {}).get("singleton_owner_abstention_count") != 0
        or score.get("metrics", {}).get("all_turn_abstention_count") != 0
        or spec.get("model") != "gpt-5.4"
        or spec.get("retry_count_per_turn") != 0
        or spec.get("maximum_cases_per_turn") != 1
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV132Error("v131 predecessor contract drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5CalibrationV132Error("v131 runtime record drifted")
    for name, key in {"score": "score", "outputs": "outputs"}.items():
        record = terminal.get(key)
        if not isinstance(record, Mapping) or dict(record) != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV132Error(f"v131 {name} record drifted")
    attempts = {}
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_name in spec["turn_plan"]:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        records = {}
        for name, filename in {
            "capacity": "capacity.json",
            "sidecar": "sidecar.json",
            "output": "output.private.json",
        }.items():
            path = turn_root / filename
            if not path.is_file():
                raise JudgeV5CalibrationV132Error("v131 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(_load_json(Path(records["sidecar"]["path"]), "v131 sidecar"))
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV132Error("v131 usage aggregate drifted")
    outputs = values["outputs"]["turns"]
    failed_controls = []
    owner_case_id = None
    for row in values["truth"]["cases"]:
        observed = outputs[row["turn_name"]]["cases"][0]
        if row["role"] == "singleton_control" and (
            _support_only_projection(observed, row["proposition"])
            != row["current_reference"]
        ):
            failed_controls.append(str(row["case_id"]))
        if row["role"] == "singleton_owner":
            owner_case_id = str(row["case_id"])
    if len(failed_controls) != 1 or owner_case_id is None:
        raise JudgeV5CalibrationV132Error("v131 observable failure projection drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "failed_control_case_id": failed_controls[0],
        "owner_case_id": owner_case_id,
        "v130": _validate_v130(),
    }


def _select_controls(v131: Mapping[str, Any]) -> list[str]:
    excluded = v131["failed_control_case_id"]
    candidates = [
        row
        for row in v131["v130"]["values"]["truth"]["cases"]
        if row["role"] == "unanimous_control" and row["case_id"] != excluded
    ]
    buckets: dict[str, list[str]] = {}
    for row in candidates:
        buckets.setdefault(str(row["shape"]), []).append(str(row["case_id"]))
    for shape, values in buckets.items():
        values.sort(key=lambda case_id: sha256_text(f"v132|control|{shape}|{case_id}"))
    selected = []
    while len(selected) < CONTROL_COUNT:
        progressed = False
        for shape in sorted(buckets):
            if buckets[shape] and len(selected) < CONTROL_COUNT:
                selected.append(buckets[shape].pop(0))
                progressed = True
        if not progressed:
            break
    if len(selected) != CONTROL_COUNT or excluded in selected:
        raise JudgeV5CalibrationV132Error("v132 control selection drifted")
    return selected


def _singleton_value(base: Mapping[str, Any], case: Mapping[str, Any], turn_name: str) -> dict[str, Any]:
    value = {key: deepcopy(item) for key, item in base.items() if key != "cases"}
    rendered = deepcopy(case)
    rendered["witnesses"] = sorted(
        rendered["witnesses"],
        key=lambda witness: sha256_text(
            f"v132|witness-order|{turn_name}|{witness['witness_id']}"
        ),
    )
    value["cases"] = [rendered]
    value["permutation"] = "v132_luna_singleton_origin_neutral"
    value["permuted_axes"] = ["isolated_case", "hash_bound_witness_order"]
    value["singleton_context"] = True
    return value


def build_v132_inputs(
    v131: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    v130 = v131["v130"]
    source = {str(case["case_id"]): case for case in v130["values"]["input"]["cases"]}
    truth_rows = {str(row["case_id"]): row for row in v130["values"]["truth"]["cases"]}
    controls = _select_controls(v131)
    owner_id = v131["owner_case_id"]
    rows = []
    for turn_name, case_id in zip(CONTROL_TURNS, controls, strict=True):
        rows.append(
            {
                "turn_name": turn_name,
                "role": "singleton_control",
                "case_id": case_id,
                "value": _singleton_value(v130["values"]["input"], source[case_id], turn_name),
            }
        )
    rows.append(
        {
            "turn_name": OWNER_TURN,
            "role": "singleton_owner",
            "case_id": owner_id,
            "value": _singleton_value(v130["values"]["input"], source[owner_id], OWNER_TURN),
        }
    )
    truth = {
        "schema_version": V132_TRUTH_VERSION,
        "case_count": 4,
        "singleton_control_count": CONTROL_COUNT,
        "singleton_owner_count": 1,
        "cases": [
            {
                "turn_name": row["turn_name"],
                "case_id": row["case_id"],
                "role": row["role"],
                "current_reference": deepcopy(truth_rows[row["case_id"]]["current_reference"]),
                "proposition": deepcopy(truth_rows[row["case_id"]]["proposition"]),
            }
            for row in rows
        ],
    }
    selection = {
        "schema_version": V132_SELECTION_VERSION,
        "created_at": now_iso(),
        "v131_failed_control_exclusion_count": 1,
        "v131_owner_output_reused": False,
        "singleton_control_count": CONTROL_COUNT,
        "singleton_owner_count": 1,
        "maximum_cases_per_turn": 1,
        "support_positive_witnesses_only": True,
        "selection_uses_source_text": False,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "trigger_reason_in_model_input": False,
        "system_identity_in_model_input": False,
        "singleton_owner_is_decisive": True,
        "majority_voting_used": False,
        "privacy": "opaque_ids_and_aggregate_counts_only",
    }
    return rows, truth, selection


def score_v132(
    outputs: Mapping[str, Mapping[str, Any]], truth: Mapping[str, Any]
) -> dict[str, Any]:
    score = score_v131(outputs, truth)
    score["schema_version"] = V132_SCORE_VERSION
    score["metrics"] = {
        **score["metrics"],
        "v131_failed_control_exclusion_count": 1,
        "v131_owner_output_reused": False,
    }
    return score


def reconcile_alignment_reference(
    *, v131: Mapping[str, Any], outputs: Mapping[str, Mapping[str, Any]], truth: Mapping[str, Any]
) -> dict[str, Any]:
    reference = reconcile_alignment_reference_v131(
        v130=v131["v130"], outputs=outputs, truth=truth
    )
    reference.update(
        {
            "schema_version": V132_REFERENCE_VERSION,
            "reference_version": "fixture_reference_v10_retained_alignment_luna_singleton_frozen",
            "retained_alignment_singleton_owner_model": MODEL,
            "v131_owner_output_reused": False,
        }
    )
    return reference


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {
        "schema_version": V132_CAPACITY_AUDIT_VERSION,
        "phase_id": V132_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V132_CAPACITY_POLICY_VERSION,
        "phase_id": V132_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(TURN_NAMES),
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v132(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v132 terminal")}
    v131 = _validate_v131()
    rows, truth, selection = build_v132_inputs(v131)
    _write_immutable(root / "luna-singleton-alignment-truth.private.json", truth)
    _write_stable_time(root / "selection-audit.json", selection, "created_at")
    turns = []
    for row in rows:
        prompt, schema = alignment_prompt_v130(row["value"]), neutral_alignment_output_schema(row["value"])
        paths = _freeze_turn_request(
            root=root, turn_name=row["turn_name"], input_value=row["value"], prompt=prompt, schema=schema
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v131_{name}": record for name, record in v131["records"].items()},
        "v131_attempts": v131["attempts"],
        **{f"v130_{name}": record for name, record in v131["v130"]["records"].items()},
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V132_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_luna_singleton_owner_after_gpt54_control_failure",
        "singleton_control_count": CONTROL_COUNT,
        "singleton_owner_count": 1,
        "maximum_cases_per_turn": 1,
        "turn_plan": list(TURN_NAMES),
        "support_positive_witnesses_only": True,
        "v131_failed_control_excluded": True,
        "v131_owner_output_reused": False,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "trigger_reason_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "three_exact_singleton_controls_one_decisive_owner_and_zero_abstentions",
        "proposition_reference_frozen": True,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
        "fresh_full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v131_singleton_alignment_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v130_retained_alignment_owner.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_instruction_hash": sha256_text(alignment_instructions_v130()),
        "frozen_inputs": {
            "truth": _record(root / "luna-singleton-alignment-truth.private.json"),
            "selection": _record(root / "selection-audit.json"),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["role"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_outputs_no_source_text_in_reports",
    }
    spec_path = root / "luna-singleton-alignment-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "truth": truth,
        "v131": v131,
    }


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [
        row for row in _attempt_records(root)
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v132 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {
        "schema_version": V132_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
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
        "schema_version": V132_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "proposition_reference_frozen": True,
        "alignment_reference_frozen": False,
        "fresh_diagnostic_authorized": False,
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


async def run_v132(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v132 terminal")
    frozen = freeze_v132(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs, sidecars = {}, []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=alignment_instructions_v130(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=1,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: _validate_scoreable_alignment_output(candidate, item),
                )
                outputs[current_turn] = normalize_neutral_alignment_output(output, turn["value"])
                sidecars.append(sidecar)
        outputs_path = root / "luna-singleton-alignment-outputs.private.json"
        _write_immutable(outputs_path, {"schema_version": V132_OUTPUT_VERSION, "turns": outputs})
        score = score_v132(outputs, frozen["truth"])
        score_path = root / "luna-singleton-alignment-score.json"
        _write_immutable(score_path, score)
        reference_path = root / "calibration-truth-v10-retained-alignment-luna-singleton.private.json"
        if score["passed"]:
            reference = reconcile_alignment_reference(
                v131=frozen["v131"], outputs=outputs, truth=frozen["truth"]
            )
            _write_immutable(reference_path, reference)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V132_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v132_luna_singleton_owner_passed_reference_frozen"
                if passed else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v132_retained_alignment_reference_frozen_fresh_diagnostic_authorized"
                if passed else "v132_luna_singleton_owner_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "proposition_reference_frozen": True,
            "alignment_reference_frozen": passed,
            "reference_frozen": passed,
            "fresh_diagnostic_authorized": passed,
            "fresh_full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "outputs": _record(outputs_path),
            "reference": _record(reference_path) if passed else None,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "predecessor_v131_usage": frozen["v131"]["usage"],
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v132 Luna singleton alignment owner")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v132(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "alignment_reference_frozen": terminal.get("alignment_reference_frozen", False),
                "fresh_diagnostic_authorized": terminal.get("fresh_diagnostic_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
