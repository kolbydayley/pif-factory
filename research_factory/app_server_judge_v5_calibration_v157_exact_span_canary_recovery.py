from __future__ import annotations

"""Recover v156 by exact-span filtering and checklist-derived relation projection."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v153_capped_alignment_adjudication as v153
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v156_relation_projection_recovery as v156
from .app_server_judge_v5 import (
    build_disagreement_adjudication_input,
    expected_relation_from_checklist,
    neutral_alignment_output_schema,
    reconcile_neutral_alignment,
    validate_neutral_alignment_output,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V157_AUDIT_VERSION = "pif_app_server_judge_v5_4_v157_structural_projection_audit_v1"
V157_SPEC_VERSION = "pif_app_server_judge_v5_4_v157_spec_v1"
V157_SCORE_VERSION = "pif_app_server_judge_v5_4_v157_score_v1"
V157_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v157_frozen_protocol_v1"
V157_FAILURE_VERSION = "pif_app_server_judge_v5_4_v157_failure_v1"
V157_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v157_terminal_v1"
V157_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V157_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V157_PHASE_ID = "judge_v5_4_v157_exact_span_canary_recovery"

MODEL = v155.ALIGNMENT_MODEL
ADJUDICATOR_MODEL = v155.ADJUDICATOR_MODEL
EFFORT = v155.EFFORT
CANARY_TURN = "recovery2_alignment_canary_01"
ADJUDICATION_TURN = "recovery2_alignment_disagreement_adjudication"
TURN_NAMES = (CANARY_TURN, ADJUDICATION_TURN)
MAXIMUM_TOTAL_TOKENS_PER_TURN = v155.MAXIMUM_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = v155.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v156.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v157-exact-span-canary-recovery"
).resolve()


class JudgeV5CalibrationV157Error(RuntimeError):
    """The immutable v157 recovery contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def project_exact_spans_and_relation(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    projected = deepcopy(output)
    sources = {
        str(case["case_id"]): str(case["source_excerpt"])
        for case in alignment_input["cases"]
    }
    dropped = []
    relation_changes = []
    pair_count = 0
    checklist_count = 0
    try:
        for case_index, case in enumerate(projected["cases"]):
            source = sources[str(case["case_id"])]
            for pair_index, pair in enumerate(case["alignment_pairs"]):
                pair_count += 1
                for checklist_index, row in enumerate(pair["checklist"]):
                    checklist_count += 1
                    spans = row.get("source_evidence_spans")
                    if not isinstance(spans, list):
                        continue
                    retained = [
                        span
                        for span in spans
                        if isinstance(span, str) and bool(span) and span in source
                    ]
                    if len(retained) != len(spans):
                        dropped.append(
                            {
                                "case_index": case_index,
                                "pair_index": pair_index,
                                "checklist_index": checklist_index,
                                "field": row.get("field"),
                                "dropped_span_count": len(spans) - len(retained),
                            }
                        )
                        row["source_evidence_spans"] = retained
                relation = expected_relation_from_checklist(pair["checklist"])
                if pair["relation"] != relation:
                    relation_changes.append(
                        {
                            "case_index": case_index,
                            "pair_index": pair_index,
                            "before_relation": pair["relation"],
                            "projected_relation": relation,
                        }
                    )
                    pair["relation"] = relation
    except Exception as exc:
        raise JudgeV5CalibrationV157Error("structural projection input is malformed") from exc

    errors = validate_neutral_alignment_output(projected, alignment_input)
    if errors:
        raise JudgeV5CalibrationV157Error(
            "structural projection did not produce a valid alignment output"
        )
    audit = {
        "schema_version": V157_AUDIT_VERSION,
        "pair_count": pair_count,
        "checklist_count": checklist_count,
        "dropped_nonexact_span_count": sum(
            row["dropped_span_count"] for row in dropped
        ),
        "affected_checklist_count": len(dropped),
        "affected_checklists": dropped,
        "relation_projection_count": len(relation_changes),
        "relation_projection_rows": relation_changes,
        "semantic_checklist_decisions_changed": False,
        "witness_assignments_changed": False,
        "equivalence_groups_changed": False,
        "unpaired_witnesses_changed": False,
        "rationales_changed": False,
        "deterministic_operations": [
            "retain_only_exact_source_substrings",
            "derive_relation_from_frozen_llm_checklist_precedence",
        ],
    }
    return projected, audit


def validate_structurally_projectable_output(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    try:
        project_exact_spans_and_relation(output, alignment_input)
    except Exception:
        errors = validate_neutral_alignment_output(output, alignment_input)
        return errors or ["structural_projection_failed"]
    return []


def _validate_v156_failure() -> dict[str, Any]:
    root = v156.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "relation-projection-recovery-spec.json",
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "reused_alignment_00": root / "reused-alignment-shard-00-projected.private.json",
        "reused_alignment_00_audit": root
        / "reused-alignment-shard-00-projection-audit.json",
        "failed_canary_input": root
        / "turns/recovery-alignment-canary-00/input.private.json",
        "failed_canary_output": root
        / "turns/recovery-alignment-canary-00/output.private.json",
        "failed_canary_sidecar": root
        / "turns/recovery-alignment-canary-00/sidecar.json",
        "failed_canary_capacity": root
        / "turns/recovery-alignment-canary-00/capacity.json",
    }
    values = {name: _load_json(path, f"v156 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    expected_usage = {
        "input_tokens": 276214,
        "cached_input_tokens": 0,
        "output_tokens": 124860,
        "reasoning_output_tokens": 41789,
        "total_tokens": 401074,
    }
    expected_cumulative = {
        "input_tokens": 1267388,
        "cached_input_tokens": 61440,
        "output_tokens": 168121,
        "reasoning_output_tokens": 57849,
        "total_tokens": 1435509,
    }
    expected_attempts = set(v156.PRIMARY_TURNS + (v156.CANARY_TURNS[0],))
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != expected_usage
        or terminal.get("predecessor_v155_usage", {}).get("total_tokens") != 1034435
        or terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or terminal.get("development_judge_frozen") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != v156.CANARY_TURNS[0]
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("accounting_complete") is not True
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("usage") != expected_usage
        or failure.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or spec.get("turn_plan") != list(v156.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("v155_completed_turn_count_reused") != 46
        or spec.get("v155_turn_count_replayed") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or terminal.get("failure") != _record(paths["failure"])
    ):
        raise JudgeV5CalibrationV157Error("v156 failed-attempt contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV157Error("v156 runtime record drifted")
    if any(
        not _verify_record(row[key])
        for row in spec["frozen_inputs"]["turns"]
        for key in ("input", "prompt", "schema")
    ):
        raise JudgeV5CalibrationV157Error("v156 request record drifted")

    attempt_rows = failure.get("attempts") or []
    if (
        len(attempt_rows) != len(expected_attempts)
        or {row.get("turn_name") for row in attempt_rows} != expected_attempts
    ):
        raise JudgeV5CalibrationV157Error("v156 attempt coverage drifted")
    usage = {field: 0 for field in USAGE_FIELDS}
    attempts = {}
    for row in attempt_rows:
        turn_name = str(row["turn_name"])
        records = {key: row.get(key) for key in ("capacity", "sidecar", "output")}
        if any(
            not isinstance(record, Mapping) or not _verify_record(record)
            for record in records.values()
        ):
            raise JudgeV5CalibrationV157Error("v156 attempt record drifted")
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v156 measured sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != expected_usage:
        raise JudgeV5CalibrationV157Error("v156 measured usage drifted")

    frozen_turns = {
        row["turn_name"]: row for row in spec["frozen_inputs"]["turns"]
    }
    primary_projected = []
    primary_inputs = []
    primary_projection_records = []
    for turn_name in v156.PRIMARY_TURNS:
        turn_root = root / "turns" / turn_name.replace("_", "-")
        input_value = _load_json(
            Path(frozen_turns[turn_name]["input"]["path"]), "v156 primary input"
        )
        raw = _load_json(turn_root / "output.private.json", "v156 primary raw output")
        projected = _load_json(
            turn_root / "relation-projected.private.json", "v156 projected output"
        )
        audit = _load_json(
            turn_root / "relation-projection-audit.json", "v156 projection audit"
        )
        rebuilt, rebuilt_audit = v156.project_relation_from_checklist(raw, input_value)
        if (
            projected != rebuilt
            or audit != rebuilt_audit
            or validate_neutral_alignment_output(projected, input_value)
        ):
            raise JudgeV5CalibrationV157Error("v156 primary projection drifted")
        primary_projected.append(projected)
        primary_inputs.append(input_value)
        primary_projection_records.append(
            {
                "turn_name": turn_name,
                "projected": _record(turn_root / "relation-projected.private.json"),
                "audit": _record(turn_root / "relation-projection-audit.json"),
            }
        )

    strict_errors = validate_neutral_alignment_output(
        values["failed_canary_output"], values["failed_canary_input"]
    )
    if strict_errors != [
        "case_2_pair_0_checklist_13_invalid",
        "case_3_pair_0_checklist_13_invalid",
        "case_4_pair_0_checklist_13_invalid",
    ]:
        raise JudgeV5CalibrationV157Error("v156 canary failure signature drifted")
    projected_canary, canary_audit = project_exact_spans_and_relation(
        values["failed_canary_output"], values["failed_canary_input"]
    )
    if (
        canary_audit["dropped_nonexact_span_count"] != 3
        or canary_audit["affected_checklist_count"] != 3
    ):
        raise JudgeV5CalibrationV157Error("v156 canary projection count drifted")

    for turn_name in (v156.CANARY_TURNS[1], v156.ADJUDICATION_TURN):
        turn_root = root / "turns" / turn_name.replace("_", "-")
        if any(
            (turn_root / filename).exists()
            for filename in ("capacity.json", "sidecar.json", "output.private.json")
        ):
            raise JudgeV5CalibrationV157Error("v156 unstarted turn artifact appeared")

    v155_source = v156._validate_v155_failure()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "cumulative_usage": expected_cumulative,
        "spec": spec,
        "frozen_turns": frozen_turns,
        "primary_projected": primary_projected,
        "primary_inputs": primary_inputs,
        "primary_projection_records": primary_projection_records,
        "projected_canary_00": projected_canary,
        "canary_00_audit": canary_audit,
        "v155": v155_source,
    }


def _fresh_canary(source: Mapping[str, Any]) -> dict[str, Any]:
    frozen = source["frozen_turns"][v156.CANARY_TURNS[1]]
    for key in ("input", "prompt", "schema"):
        if not _verify_record(frozen[key]):
            raise JudgeV5CalibrationV157Error("v156 canary request record drifted")
    value = _load_json(Path(frozen["input"]["path"]), "v156 canary 01 input")
    prompt = Path(frozen["prompt"]["path"]).read_text()
    schema = _load_json(Path(frozen["schema"]["path"]), "v156 canary 01 schema")
    return {
        "turn_name": CANARY_TURN,
        "turn_role": "alignment_canary",
        "case_ids": frozen["case_ids"],
        "value": value,
        "prompt": prompt,
        "schema": schema,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V157_CAPACITY_AUDIT_VERSION,
        "phase_id": V157_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "v156_failed_canary_total_tokens": 33612,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V157_CAPACITY_POLICY_VERSION,
        "phase_id": V157_PHASE_ID,
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
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": math.ceil(
            bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
        ),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v157(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v157 terminal")}
    source = _validate_v156_failure()
    canary_path = root / "reused-canary-00-projected.private.json"
    canary_audit_path = root / "reused-canary-00-projection-audit.json"
    _write_immutable(canary_path, source["projected_canary_00"])
    _write_immutable(canary_audit_path, source["canary_00_audit"])

    turn = _fresh_canary(source)
    paths = _freeze_turn_request(
        root=root,
        turn_name=turn["turn_name"],
        input_value=turn["value"],
        prompt=turn["prompt"],
        schema=turn["schema"],
    )
    turn = {**turn, "paths": paths}
    predecessor = {
        **{f"v156_{name}": record for name, record in source["records"].items()},
        "v156_attempts": source["attempts"],
        "v156_primary_projections": source["primary_projection_records"],
        "v156_usage": source["usage"],
        "v155_plus_v156_cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V157_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "adjudicator_model": ADJUDICATOR_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "reuse_all_57_measured_predecessor_turns_project_exact_spans_and_relation_run_only_missing_canary_and_optional_adjudication",
        "predecessor_semantic_turn_count_reused": 57,
        "predecessor_turn_count_replayed": 0,
        "fresh_canary_turn_count": 1,
        "maximum_adjudication_call_count": 1,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": 1,
        "maximum_turn_count": 2,
        "retry_count_per_turn": 0,
        "semantic_decisions_changed_by_projection": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v156_relation_projection_recovery.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v155_fresh_full_development.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v153_capped_alignment_adjudication.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v130_retained_alignment_owner.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
            _record(runtime_dir / "util.py"),
        ],
        "frozen_instructions": {
            "alignment_sha256": sha256_text(v130.alignment_instructions_v130()),
            "adjudication_sha256": sha256_text(v153.adjudication_instructions_v153()),
        },
        "frozen_inputs": {
            "truth": source["v155"]["records"]["truth"],
            "support": source["v155"]["records"]["support"],
            "support_canary": source["v155"]["records"]["support_canary"],
            "support_receipts": source["v155"]["records"]["support_receipts"],
            "fields": source["v155"]["records"]["fields"],
            "field_repeats": source["v155"]["records"]["field_repeats"],
            "reused_canary_input": source["records"]["failed_canary_input"],
            "reused_canary_output": source["records"]["failed_canary_output"],
            "projected_canary_output": _record(canary_path),
            "projection_audit": _record(canary_audit_path),
            "fresh_canary": {
                "turn_name": turn["turn_name"],
                "case_ids": turn["case_ids"],
                "input": _record(turn["paths"]["input"]),
                "prompt": _record(turn["paths"]["prompt"]),
                "schema": _record(turn["paths"]["schema"]),
            },
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "exact-span-canary-recovery-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turn": turn,
        "source": source,
    }


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
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
            measured = _validate_usage(
                _load_json(Path(record["path"]), "v157 measured sidecar")
            )
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v156_failure()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V157_FAILURE_VERSION,
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
        "predecessor_cumulative_usage": predecessor,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V157_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "development_judge_frozen": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "predecessor_cumulative_usage": predecessor,
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


def _project_and_write(
    *, root: Path, turn_name: str, output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    projected, audit = project_exact_spans_and_relation(output, alignment_input)
    turn_root = root / "turns" / turn_name.replace("_", "-")
    _write_immutable(turn_root / "structurally-projected.private.json", projected)
    _write_immutable(turn_root / "structural-projection-audit.json", audit)
    return projected, audit


async def run_v157(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v157 terminal")
    frozen = freeze_v157(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars = []
    projection_audits = []
    try:
        turn = frozen["turn"]
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            current_turn = CANARY_TURN
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=CANARY_TURN,
                paths=turn["paths"],
                prompt=turn["prompt"],
                schema=turn["schema"],
                base_instructions=v130.alignment_instructions_v130(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=len(turn["value"]["cases"]),
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_structurally_projectable_output(candidate, turn["value"]),
            )
            projected_canary_01, audit = _project_and_write(
                root=root,
                turn_name=CANARY_TURN,
                output=output,
                alignment_input=turn["value"],
            )
            sidecars.append(sidecar)
            projection_audits.append(audit)

            source = frozen["source"]
            v155_source = source["v155"]
            primary_outputs = [
                _load_json(
                    Path(source["spec"]["frozen_inputs"]["projected_alignment_output"]["path"]),
                    "v156 reused alignment 00",
                ),
                *source["primary_projected"],
            ]
            base_output = v155._merge_outputs(primary_outputs, "cases")
            pool = v155_source["values"]["pool"]
            receipts = v155_source["values"]["support_receipts"]
            base_input = v155._support_positive_alignment_input(
                pool,
                receipts,
                case_ids=[case["case_id"] for case in pool["cases"]],
                permutation="base",
            )
            if validate_neutral_alignment_output(base_output, base_input):
                raise JudgeV5CalibrationV157Error("v157 aggregate base alignment is invalid")
            _write_immutable(root / "alignment-base-projected.private.json", base_output)

            canary_output = v155._merge_outputs(
                [source["projected_canary_00"], projected_canary_01], "cases"
            )
            canary_input = deepcopy(source["values"]["failed_canary_input"])
            canary_input["cases"] = [
                *deepcopy(source["values"]["failed_canary_input"]["cases"]),
                *deepcopy(turn["value"]["cases"]),
            ]
            if validate_neutral_alignment_output(canary_output, canary_input):
                raise JudgeV5CalibrationV157Error("v157 aggregate canary alignment is invalid")
            _write_immutable(root / "alignment-canary-projected.private.json", canary_output)

            disagreements = build_disagreement_adjudication_input(
                base_input=base_input,
                base_output=base_output,
                canary_input=canary_input,
                canary_output=canary_output,
                support_receipts=receipts,
            )
            _write_immutable(
                root / "alignment-observable-disagreements.private.json", disagreements
            )
            adjudication_output = None
            adjudication_alignment = None
            if disagreements["adjudication_required"]:
                packet = v153._balance_anonymous_candidates(deepcopy(disagreements))
                adjudication_alignment = v153.adjudication_alignment_input(
                    base_input=base_input, adjudication_input=packet
                )
                prompt = v153.build_disagreement_adjudication_prompt(
                    adjudication_input=packet,
                    adjudication_alignment=adjudication_alignment,
                )
                schema = neutral_alignment_output_schema(adjudication_alignment)
                request_paths = _freeze_turn_request(
                    root=root,
                    turn_name=ADJUDICATION_TURN,
                    input_value={
                        "packet": packet,
                        "alignment_input": adjudication_alignment,
                    },
                    prompt=prompt,
                    schema=schema,
                )
                current_turn = ADJUDICATION_TURN
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=ADJUDICATION_TURN,
                    paths=request_paths,
                    prompt=prompt,
                    schema=schema,
                    base_instructions=v153.adjudication_instructions_v153(),
                    model=ADJUDICATOR_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(adjudication_alignment["cases"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate: validate_structurally_projectable_output(candidate, adjudication_alignment),
                )
                adjudication_output, audit = _project_and_write(
                    root=root,
                    turn_name=ADJUDICATION_TURN,
                    output=output,
                    alignment_input=adjudication_alignment,
                )
                sidecars.append(sidecar)
                projection_audits.append(audit)

            reconciled = reconcile_neutral_alignment(
                base_output=base_output,
                base_input=base_input,
                canary_output=canary_output,
                canary_input=canary_input,
                support_receipts=receipts,
                adjudication_output=adjudication_output,
                adjudication_input=adjudication_alignment,
            )
            _write_immutable(root / "alignment-reconciled.private.json", reconciled)

        truth = frozen["source"]["v155"]["values"]["truth"]
        final_canary_exact = (
            len(truth["alignment_canary_case_ids"])
            if not reconciled["unresolved_cases_abstained"]
            else 0
        )
        score = v155.score_v155(
            support=frozen["source"]["v155"]["values"]["support"],
            support_canary=frozen["source"]["v155"]["values"]["support_canary"],
            fields=frozen["source"]["v155"]["values"]["fields"],
            field_repeats=frozen["source"]["v155"]["values"]["field_repeats"],
            alignment=reconciled,
            truth=truth,
            raw_alignment_disagreement_count=disagreements["disagreement_case_count"],
            final_canary_exact_count=final_canary_exact,
        )
        score = {**score, "schema_version": V157_SCORE_VERSION}
        score_path = root / "full-calibration-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "development-judge-protocol-v157.json"
        if passed:
            protocol = {
                "schema_version": V157_PROTOCOL_VERSION,
                "frozen_at": now_iso(),
                "support_model": v155.SUPPORT_MODEL,
                "field_model": v155.FIELD_MODEL,
                "alignment_model": MODEL,
                "adjudicator_model": ADJUDICATOR_MODEL,
                "reasoning_effort": EFFORT,
                "structural_projection_operations": [
                    "retain_only_exact_source_substrings",
                    "derive_relation_from_frozen_llm_checklist_precedence",
                ],
                "semantic_checklist_decisions_changed": False,
                "predecessor_turns_reused": 57,
                "predecessor_turns_replayed": 0,
                "quality_gates_unchanged": True,
                "reference": frozen["source"]["v155"]["values"]["spec"]["frozen_inputs"]["reference"],
                "truth": frozen["source"]["v155"]["records"]["truth"],
                "selection_authorized": True,
                "holdout_authorized": False,
                "production_mutation_allowed": False,
            }
            _write_immutable(protocol_path, protocol)

        accounting = _aggregate_usage(sidecars)
        incremental_usage = accounting["usage"]
        predecessor_usage = frozen["source"]["cumulative_usage"]
        cumulative_usage = _sum_usage(predecessor_usage, incremental_usage)
        predecessor_audit = frozen["source"]["canary_00_audit"]
        projection_summary = {
            "turn_count": len(projection_audits) + 1,
            "dropped_nonexact_span_count": predecessor_audit[
                "dropped_nonexact_span_count"
            ]
            + sum(row["dropped_nonexact_span_count"] for row in projection_audits),
            "relation_projection_count": predecessor_audit[
                "relation_projection_count"
            ]
            + sum(row["relation_projection_count"] for row in projection_audits),
            "semantic_checklist_decisions_changed": False,
        }
        terminal = {
            "schema_version": V157_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v157_full_development_calibration_passed_selection_authorized"
            if passed
            else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v157_full_development_calibration_passed"
            if passed
            else "v157_full_development_calibration_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "development_judge_frozen": passed,
            "selection_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "predecessor_turns_replayed": False,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "projection_summary": projection_summary,
            "score": _record(score_path),
            "protocol": _record(protocol_path) if passed else None,
            "reference": frozen["source"]["v155"]["values"]["spec"]["frozen_inputs"]["reference"],
            "truth": frozen["source"]["v155"]["records"]["truth"],
            "predecessor_cumulative_usage": predecessor_usage,
            "cumulative_calibration_usage": cumulative_usage,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v157 exact-span canary recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v157(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "development_judge_frozen": terminal.get("development_judge_frozen", False),
                "selection_authorized": terminal.get("selection_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
