from __future__ import annotations

"""One capped source-first adjudication over Luna/gpt-5.4 disagreement units."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_judge_v5 import CHECKLIST_FIELDS
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
from .app_server_judge_v5_calibration_v26_diagnostic import (
    MAX_TOKENS_PER_TURN,
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v55_structured import (
    v55_output_schema,
    validate_v55_output,
)
from .app_server_judge_v5_calibration_v57_repair import score_v57
from .app_server_judge_v5_calibration_v59_projection import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V59_ROOT,
)
from .app_server_judge_v5_calibration_v60_gpt54 import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V60_ROOT,
    project_frozen_contract,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _canonical_json,
    _record,
    _sha256_file,
    _validate_usage,
)
from .app_server_judge_v5_fixture import load_fixture_truth_audit
from .util import now_iso


V61_INPUT_VERSION = "pif_app_server_judge_v5_4_v61_disagreement_adjudication_input_v1"
V61_SPEC_VERSION = "pif_app_server_judge_v5_4_v61_disagreement_adjudication_spec_v1"
V61_SCORE_VERSION = "pif_app_server_judge_v5_4_v61_disagreement_adjudication_score_v1"
V61_AUDIT_VERSION = "pif_app_server_judge_v5_4_v61_adjudication_audit_v1"
V61_FAILURE_VERSION = "pif_app_server_judge_v5_4_v61_disagreement_adjudication_failure_v1"
V61_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v61_disagreement_adjudication_terminal_v1"
V61_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V61_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V61_PHASE_ID = "judge_v5_4_v61_capped_disagreement_adjudication"
TURN_NAME = "whole_unit_disagreement_adjudication"
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_V60_ROOT.parent / "judge-calibration-v5_4-v61-disagreement-adjudication"
).resolve()


class JudgeV5CalibrationV61AdjudicationError(RuntimeError):
    """The capped disagreement adjudication cannot preserve its contract."""


def _record_matches(record: Any, path: Path) -> bool:
    return (
        isinstance(record, Mapping)
        and path.is_file()
        and record.get("sha256") == _sha256_file(path)
        and record.get("size_bytes") == path.stat().st_size
    )


def _validate_predecessors(v59_root: Path, v60_root: Path) -> dict[str, Any]:
    paths = {
        "v59_terminal": v59_root / "terminal.json",
        "v59_output": v59_root / "projected-root-output.private.json",
        "v60_terminal": v60_root / "terminal.json",
        "v60_spec": v60_root / "gpt54-specialist-spec.json",
        "v60_score": v60_root / "gpt54-specialist-score.json",
        "v60_output": v60_root / "gpt54-specialist-output-full.private.json",
        "v60_taxonomy": v60_root / "error-taxonomy.json",
        "v60_truth": v60_root / "diagnostic-truth.private.json",
    }
    for index in range(2):
        turn = v60_root / "turns" / f"root-projection-shard-{index:02d}"
        paths[f"v60_shard{index:02d}_input"] = turn / "input.private.json"
        paths[f"v60_shard{index:02d}_raw_output"] = turn / "output.private.json"
        paths[f"v60_shard{index:02d}_projected_output"] = turn / "projected-output.private.json"
        paths[f"v60_shard{index:02d}_capacity"] = turn / "capacity.json"
        paths[f"v60_shard{index:02d}_sidecar"] = turn / "sidecar.json"
    values = {name: _load_json(path, name) for name, path in paths.items()}
    v59 = values["v59_terminal"]
    v60 = values["v60_terminal"]
    score = values["v60_score"]
    if (
        v59.get("state") != "inactive"
        or v59.get("projection_passed") is not False
        or v59.get("semantic_attempt_started") is not False
        or v60.get("state") != "inactive"
        or v60.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or v60.get("development_terminal_reason")
        != "v60_gpt54_specialist_quality_gate_not_passed"
        or v60.get("specialist_diagnostic_passed") is not False
        or v60.get("usage_status") != "complete"
        or v60.get("accounting_complete") is not True
        or v60.get("semantic_retry_count") != 0
        or v60.get("production_mutated") is not False
        or not _record_matches(v60.get("score"), paths["v60_score"])
        or not _record_matches(v60.get("output"), paths["v60_output"])
        or score.get("passed") is not False
    ):
        raise JudgeV5CalibrationV61AdjudicationError("v59/v60 predecessors are inadmissible")
    attempts = v60.get("attempts") or []
    if len(attempts) != 2:
        raise JudgeV5CalibrationV61AdjudicationError("v60 attempt coverage drifted")
    for index, attempt in enumerate(attempts):
        sidecar = values[f"v60_shard{index:02d}_sidecar"]
        capacity = values[f"v60_shard{index:02d}_capacity"]
        _validate_usage(sidecar)
        if (
            attempt.get("turn_name") != f"root_projection_shard_{index:02d}"
            or attempt.get("state") != "completed"
            or attempt.get("usage_status") != "measured"
            or sidecar.get("model") != "gpt-5.4"
            or sidecar.get("auth_type") != "chatgpt"
            or capacity.get("cleared_for_semantic_turn") is not True
            or capacity.get("managed_chatgpt_auth_verified") is not True
            or capacity.get("rate_limit_reached_type") is not None
            or not _record_matches(attempt.get("sidecar"), paths[f"v60_shard{index:02d}_sidecar"])
            or not _record_matches(attempt.get("capacity"), paths[f"v60_shard{index:02d}_capacity"])
            or not _record_matches(attempt.get("output"), paths[f"v60_shard{index:02d}_raw_output"])
        ):
            raise JudgeV5CalibrationV61AdjudicationError("v60 measured turn drifted")
        projected, _operations = project_frozen_contract(
            values[f"v60_shard{index:02d}_raw_output"],
            values[f"v60_shard{index:02d}_input"],
        )
        if projected != values[f"v60_shard{index:02d}_projected_output"]:
            raise JudgeV5CalibrationV61AdjudicationError("v60 projection lineage drifted")
    return {name: _record(path) for name, path in paths.items()}


def _decisions(row: Mapping[str, Any]) -> list[dict[str, str]]:
    return [
        {"field": str(item["field"]), "decision": str(item["decision"])}
        for item in row["checklist"]
    ]


def build_v61_input(
    base_input: Mapping[str, Any],
    luna_output: Mapping[str, Any],
    gpt54_output: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in base_input.get("units") or []
    }
    luna = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in luna_output.get("units") or []
    }
    gpt54 = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in gpt54_output.get("units") or []
    }
    if set(source) != set(luna) or set(source) != set(gpt54):
        raise JudgeV5CalibrationV61AdjudicationError("candidate coverage drifted")
    units = []
    agreement_units = []
    source_a_count = 0
    source_b_count = 0
    for key in sorted(source):
        luna_decisions = _decisions(luna[key])
        gpt54_decisions = _decisions(gpt54[key])
        if luna_decisions == gpt54_decisions:
            agreement_units.append({"case_id": key[0], "witness_id": key[1]})
            continue
        luna_is_a = len(units) % 2 == 0
        candidate_a = luna_decisions if luna_is_a else gpt54_decisions
        candidate_b = gpt54_decisions if luna_is_a else luna_decisions
        source_a_count += int(luna_is_a)
        source_b_count += int(not luna_is_a)
        row = source[key]
        units.append(
            {
                "case_id": key[0],
                "witness_id": key[1],
                "source_excerpt": row["source_excerpt"],
                "structured_event": deepcopy(row["structured_event"]),
                "frozen_proposition_verdict": row["frozen_proposition_verdict"],
                "candidate_a": candidate_a,
                "candidate_b": candidate_b,
            }
        )
    if len(units) != 13 or len(agreement_units) != 5 or (source_a_count, source_b_count) != (7, 6):
        raise JudgeV5CalibrationV61AdjudicationError("v61 disagreement cohort drifted")
    return (
        {
            "schema_version": V61_INPUT_VERSION,
            "units": units,
            "checklist_field_order": list(CHECKLIST_FIELDS),
            "candidate_origin_labels_present": False,
            "system_identity_present": False,
            "selection_trigger": "whole_unit_candidate_decision_disagreement",
        },
        agreement_units,
    )


def v61_base_instructions() -> str:
    return (
        "You are a side-free source-first adjudicator. Two anonymous candidate checklists are "
        "provided only because they disagree somewhere in the unit; neither is authoritative and "
        "you must not vote or compromise between them. Judge the structured event directly against "
        "the entire source and return all 15 rows. Report only minimal independent truth-conditional "
        "root conflicts. Before marking a field different, substitute source-supported values for "
        "other root conflicts; if the conflict disappears, mark it same. actor or speaker identity "
        "conflicts do not by themselves change attribution, evidence, event_type, or target. "
        "attribution is only the reporting/source relation. evidence is independently different only "
        "when the populated evidence is inexact or fails to support the frozen proposition. event_type "
        "is independently different only when the action/category remains wrong after participant "
        "correction. target is independently different only when the semantic object/recipient remains "
        "wrong. event_boundary must be same. unsupported_inference must follow the frozen proposition "
        "receipt exactly. Use exact source substrings or [] for witness-only conflicts. Do not infer "
        "candidate origin, use confidence, regex, keywords, overlap, embeddings, or majority voting."
    )


def build_v61_prompt(value: Mapping[str, Any]) -> str:
    fixture = load_fixture_truth_audit()
    packet = {
        "rubric": fixture["mismatch_checklist"],
        "mismatch_precedence": fixture["mismatch_precedence"],
        "units": value["units"],
    }
    return (
        "Return each case_id/witness_id exactly once and all checklist fields in supplied order. "
        "structured_field_verdict=correct iff all rows are same; incorrect iff any row is different; "
        "abstain iff no row differs and at least one abstains. Resolve the whole unit from source, "
        "including fields on which candidates agree. Every nonempty evidence span must be an exact "
        "substring; use [] instead of paraphrasing.\n\n# Neutral disagreement units\n"
        + _canonical_json(packet)
        + "\n"
    )


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    audit = {
        "schema_version": V61_CAPACITY_AUDIT_VERSION,
        "phase_id": V61_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior = _load_json(audit_path, "v61 capacity audit")
        candidate = deepcopy(audit)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV61AdjudicationError("immutable v61 capacity audit drifted")
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V61_CAPACITY_POLICY_VERSION,
        "phase_id": V61_PHASE_ID,
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
    if policy_path.exists():
        prior = _load_json(policy_path, "v61 capacity policy")
        candidate = deepcopy(policy)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV61AdjudicationError("immutable v61 capacity policy drifted")
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v61(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v59_root: Path = DEFAULT_V59_ROOT,
    v60_root: Path = DEFAULT_V60_ROOT,
    model: str = "gpt-5.5",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    predecessor = _validate_predecessors(v59_root.resolve(), v60_root.resolve())
    base_input = {
        "units": [
            row
            for index in range(2)
            for row in _load_json(
                v60_root / "turns" / f"root-projection-shard-{index:02d}" / "input.private.json",
                f"v60 shard{index:02d} input",
            )["units"]
        ]
    }
    luna = _load_json(v59_root / "projected-root-output.private.json", "v59 Luna output")
    gpt54 = _load_json(v60_root / "gpt54-specialist-output-full.private.json", "v60 output")
    value, agreement_units = build_v61_input(base_input, luna, gpt54)
    truth = _load_json(v60_root / "diagnostic-truth.private.json", "v60 truth")
    taxonomy = _load_json(v60_root / "error-taxonomy.json", "v60 taxonomy")
    input_path = root / "adjudication-input.private.json"
    truth_path = root / "diagnostic-truth.private.json"
    taxonomy_path = root / "error-taxonomy.json"
    agreement_path = root / "agreement-units.json"
    _write_immutable(input_path, value)
    _write_immutable(truth_path, truth)
    _write_immutable(taxonomy_path, taxonomy)
    _write_immutable(agreement_path, {"units": agreement_units})
    prompt = build_v61_prompt(value)
    schema = v55_output_schema(value)
    paths = _freeze_turn_request(
        root=root, turn_name=TURN_NAME, input_value=value, prompt=prompt, schema=schema
    )
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V61_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_capped_source_first_whole_unit_disagreement_adjudication",
        "disagreement_unit_count": 13,
        "agreement_unit_count": 5,
        "candidate_order_balance": {"luna_as_a": 7, "gpt54_as_a": 6},
        "candidate_origin_visible_to_model": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "pass_authorizes_one_fresh_integrated_12_case_development_diagnostic_only",
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v60_gpt54.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v59_projection.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v57_repair.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v55_structured.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "input": _record(input_path),
            "truth": _record(truth_path),
            "taxonomy": _record(taxonomy_path),
            "agreement_units": _record(agreement_path),
            "turn_input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_candidate_content_no_source_text_in_reports",
    }
    spec_path = root / "adjudication-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v61 spec")
        candidate = deepcopy(spec)
        candidate["created_at"] = prior.get("created_at")
        if candidate != prior:
            raise JudgeV5CalibrationV61AdjudicationError("immutable v61 spec drifted")
        spec = prior
    else:
        _write_immutable(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "value": value,
        "truth": truth,
        "taxonomy": taxonomy,
        "agreement_units": agreement_units,
        "luna": luna,
        "gpt54": gpt54,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
    }


def merge_v61(
    adjudicated: Mapping[str, Any],
    agreement_units: Sequence[Mapping[str, Any]],
    gpt54: Mapping[str, Any],
) -> dict[str, Any]:
    agreed = {(row["case_id"], row["witness_id"]) for row in agreement_units}
    carried = [
        deepcopy(row)
        for row in gpt54["units"]
        if (row["case_id"], row["witness_id"]) in agreed
    ]
    merged = {"units": [*deepcopy(adjudicated["units"]), *carried]}
    if len(merged["units"]) != 18:
        raise JudgeV5CalibrationV61AdjudicationError("v61 merged coverage drifted")
    return merged


def _write_failure(root: Path, error_class: str) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            continue
        try:
            usage = _validate_usage(_load_json(Path(record["path"]), "v61 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    complete = unknown == 0
    failure = {
        "schema_version": V61_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V61_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "integrated_diagnostic_authorized": False,
        "full_calibration_authorized": False,
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


async def run_v61(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v59_root: Path = DEFAULT_V59_ROOT,
    v60_root: Path = DEFAULT_V60_ROOT,
    model: str = "gpt-5.5",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v61 terminal")
    frozen = freeze_v61(
        output_dir=root,
        v59_root=v59_root,
        v60_root=v60_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    factory = client_factory or _client_factory
    try:
        async with factory(frozen["capacity_policy"]) as client:
            output, sidecar, was_adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=v61_base_instructions(),
                model=model,
                effort=reasoning_effort,
                timeout_seconds=timeout_seconds,
                batch_size=13,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_v55_output(
                    project_frozen_contract(value, frozen["value"])[0], frozen["value"]
                ),
            )
        adjudicated, operations = project_frozen_contract(output, frozen["value"])
        if validate_v55_output(adjudicated, frozen["value"]):
            raise JudgeV5CalibrationV61AdjudicationError("projected adjudication remained invalid")
        adjudicated_path = root / "adjudicated-output.private.json"
        _write_immutable(adjudicated_path, adjudicated)
        merged = merge_v61(adjudicated, frozen["agreement_units"], frozen["gpt54"])
        merged_path = root / "merged-output.private.json"
        _write_immutable(merged_path, merged)
        audit = {
            "schema_version": V61_AUDIT_VERSION,
            "created_at": now_iso(),
            "trigger": "whole_unit_candidate_decision_disagreement",
            "adjudicated_unit_count": 13,
            "carried_agreement_unit_count": 5,
            "candidate_origin_visible_to_model": False,
            "candidate_order_balance": {"luna_as_a": 7, "gpt54_as_a": 6},
            "contract_projection_operation_count": len(operations),
            "contract_projection_operations": operations,
            "majority_voting_used": False,
            "privacy": "opaque_ids_enums_counts_only",
        }
        audit_path = root / "adjudication-audit.json"
        _write_immutable(audit_path, audit)
        score = score_v57(merged, frozen["truth"], frozen["taxonomy"])
        score["schema_version"] = V61_SCORE_VERSION
        score["adjudication_audit"] = _record(audit_path)
        score_path = root / "adjudication-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage([sidecar])
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V61_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v61_adjudication_passed_integrated_diagnostic_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v61_adjudication_passed_integrated_diagnostic_authorized"
                if passed
                else "v61_adjudication_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "adjudication_passed": passed,
            "integrated_diagnostic_authorized": passed,
            "full_calibration_authorized": False,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "score": _record(score_path),
            "adjudicated_output": _record(adjudicated_path),
            "merged_output": _record(merged_path),
            "adjudication_audit": _record(audit_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": {TURN_NAME: was_adopted},
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.error_class)
    except Exception as exc:
        return _write_failure(root, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run capped v61 disagreement adjudication")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v59-root", default=str(DEFAULT_V59_ROOT))
    parser.add_argument("--v60-root", default=str(DEFAULT_V60_ROOT))
    parser.add_argument("--model", default="gpt-5.5")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v61(
            output_dir=Path(args.output_dir),
            v59_root=Path(args.v59_root),
            v60_root=Path(args.v60_root),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "adjudication_passed": terminal.get("adjudication_passed", False),
                "usage_status": terminal["usage_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
