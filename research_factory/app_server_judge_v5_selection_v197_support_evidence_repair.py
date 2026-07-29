from __future__ import annotations

"""Repair only the six nonexact v196 support-evidence decisions."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_selection_v176_support_evidence_ids as v176
from . import app_server_judge_v5_selection_v196_residual_repair_support as v196
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
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .app_server_llm_judge import validate_app_server_output_schema_subset
from .util import now_iso, sha256_text


V197_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v197_spec_v1"
V197_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v197_terminal_v1"
V197_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v197_failure_v1"
V197_INPUT_VERSION = "pif_app_server_support_evidence_repair_input_v1"
V197_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V197_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V197_PHASE_ID = "judge_v5_4_selection_v197_support_evidence_repair"

TURN_NAME = "selection_residual_support_evidence_repair_00"
MODEL = v196.MODEL
EFFORT = v196.EFFORT
MAXIMUM_TOTAL_TOKENS_PER_TURN = 30_000
TIMEOUT_SECONDS = v196.TIMEOUT_SECONDS
EXPECTED_INVALID_OUTPUT_INDICES = (5, 25, 26, 27, 28, 29)
DEFAULT_OUTPUT_ROOT = (
    v196.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v197-support-evidence-repair"
).resolve()


class JudgeV5SelectionV197Error(RuntimeError):
    """The bounded v196 evidence repair cannot be frozen safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _invalid_indices(output: Mapping[str, Any], value: Mapping[str, Any]) -> list[int]:
    expected = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in value["units"]
    }
    invalid = []
    for index, row in enumerate(output["units"]):
        key = (str(row["case_id"]), str(row["witness_id"]))
        source = str(expected[key]["source_excerpt"])
        spans = row["source_evidence_spans"]
        if any(not isinstance(span, str) or not span or span not in source for span in spans):
            invalid.append(index)
    return invalid


def _validate_v196_failed_attempt() -> dict[str, Any]:
    root = v196.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "residual-repair-support-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "pool": root / "residual-repair-witness-pool.private.json",
        "mapping": root / "residual-repair-witness-mapping.private.json",
    }
    values = {name: _load_json(path, f"v196 {name}") for name, path in paths.items()}
    terminal = values["terminal"]
    failure = values["failure"]
    spec = values["spec"]
    expected_usage = {
        "input_tokens": 27160,
        "cached_input_tokens": 0,
        "output_tokens": 3724,
        "reasoning_output_tokens": 126,
        "total_tokens": 30884,
    }
    expected_cumulative = {
        "input_tokens": 7164153,
        "cached_input_tokens": 821248,
        "output_tokens": 1234500,
        "reasoning_output_tokens": 379234,
        "total_tokens": 8398653,
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason")
        != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != expected_usage
        or terminal.get("support_receipts_frozen") is not False
        or terminal.get("alignment_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or failure.get("classification")
        != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != v196.TURN_NAME
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("accounting_complete") is not True
        or failure.get("usage") != expected_usage
        or failure.get("unknown_usage_turn_count") != 0
        or spec.get("turn_plan") != [v196.TURN_NAME]
        or spec.get("witness_count") != 30
        or spec.get("retry_count_per_turn") != 0
        or spec.get("alignment_authorized") is not False
        or spec.get("holdout_authorized") is not False
    ):
        raise JudgeV5SelectionV197Error("v196 failed-attempt contract drifted")
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5SelectionV197Error("v196 artifact disappeared")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV197Error("v196 runtime binding drifted")
    for key in ("pool", "mapping", "input", "prompt", "schema"):
        record = spec["frozen_inputs"][key]
        if not _verify_record(record):
            raise JudgeV5SelectionV197Error(f"v196 frozen {key} drifted")
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    if len(attempts) != 1:
        raise JudgeV5SelectionV197Error("v196 attempt coverage drifted")
    attempt = attempts[0]
    for key in ("capacity", "sidecar", "output"):
        if not isinstance(attempt.get(key), Mapping) or not _verify_record(attempt[key]):
            raise JudgeV5SelectionV197Error(f"v196 {key} record drifted")
    sidecar = _load_json(Path(attempt["sidecar"]["path"]), "v196 sidecar")
    if _validate_usage(sidecar) != expected_usage:
        raise JudgeV5SelectionV197Error("v196 usage drifted")
    value = _load_json(Path(spec["frozen_inputs"]["input"]["path"]), "v196 input")
    output = _load_json(Path(attempt["output"]["path"]), "v196 output")
    errors = v143.validate_support_output(output, value)
    expected_errors = [
        f"support_{index}_evidence_not_exact"
        for index in EXPECTED_INVALID_OUTPUT_INDICES
    ]
    if errors != expected_errors or _invalid_indices(output, value) != list(
        EXPECTED_INVALID_OUTPUT_INDICES
    ):
        raise JudgeV5SelectionV197Error("v196 exact-evidence failure set drifted")
    predecessor = v196._validate_v195_authorization()
    if terminal["cumulative_known_usage_lower_bound"] != _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], expected_usage
    ):
        raise JudgeV5SelectionV197Error("v196 cumulative lineage drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempt": attempt,
        "sidecar": sidecar,
        "value": value,
        "output": output,
        "invalid_indices": list(EXPECTED_INVALID_OUTPUT_INDICES),
        "terminal": terminal,
        "predecessor": predecessor,
    }


def build_repair_input(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in predecessor["value"]["units"]
    }
    invalid_rows = [
        predecessor["output"]["units"][index]
        for index in predecessor["invalid_indices"]
    ]
    by_case: dict[str, list[dict[str, Any]]] = {}
    for row in invalid_rows:
        key = (str(row["case_id"]), str(row["witness_id"]))
        by_case.setdefault(key[0], []).append(expected[key])
    cases = []
    for case_index, case_id in enumerate(sorted(by_case)):
        witnesses = sorted(by_case[case_id], key=lambda row: str(row["witness_id"]))
        sources = {str(row["source_excerpt"]) for row in witnesses}
        if len(sources) != 1:
            raise JudgeV5SelectionV197Error("repair case source drifted")
        source_units = v176.build_source_units(next(iter(sources)))
        for unit in source_units:
            unit["evidence_id"] = f"src_{case_index:02d}_{unit['evidence_id']}"
        cases.append(
            {
                "case_id": case_id,
                "source_units": source_units,
                "witnesses": [
                    {
                        "witness_id": row["witness_id"],
                        "proposition": deepcopy(row["proposition"]),
                    }
                    for row in witnesses
                ],
            }
        )
    counts = sorted(len(case["witnesses"]) for case in cases)
    if len(cases) != 2 or counts != [1, 5] or sum(counts) != 6:
        raise JudgeV5SelectionV197Error("v197 repair coverage drifted")
    return {
        "schema_version": V197_INPUT_VERSION,
        "cases": cases,
        "side_free": True,
        "system_identity_present": False,
        "prior_support_decisions_present": False,
        "source_units_are_deterministic_exact_overlapping_character_windows": True,
        "semantic_support_and_evidence_selection_owned_by_llm": True,
    }


def repair_prompt_v197(value: Mapping[str, Any]) -> str:
    return (
        "Return one fresh, independent support decision for every opaque witness_id. Judge only "
        "whether every material claim in proposition.claim_text is entailed by that witness's "
        "case source_units. Harmless paraphrase and resolved coreference pass; unsupported "
        "inference fails. Select one to four source_evidence_unit_ids from the same case that "
        "directly support or contradict the decision. Copy IDs only, never source text. Preserve "
        "case_id and witness_id exactly. Do not compare witnesses or infer any system identity.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def repair_schema_v197(value: Mapping[str, Any]) -> dict[str, Any]:
    case_ids = [str(case["case_id"]) for case in value["cases"]]
    witness_ids = [
        str(witness["witness_id"])
        for case in value["cases"]
        for witness in case["witnesses"]
    ]
    evidence_ids = [
        str(unit["evidence_id"])
        for case in value["cases"]
        for unit in case["source_units"]
    ]
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["units"],
        "properties": {
            "units": {
                "type": "array",
                "minItems": len(witness_ids),
                "maxItems": len(witness_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "case_id",
                        "witness_id",
                        "support_status",
                        "source_evidence_unit_ids",
                        "rationale",
                    ],
                    "properties": {
                        "case_id": {"type": "string", "enum": case_ids},
                        "witness_id": {"type": "string", "enum": witness_ids},
                        "support_status": {
                            "type": "string",
                            "enum": ["supported", "unsupported", "abstain"],
                        },
                        "source_evidence_unit_ids": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 4,
                            "items": {"type": "string", "enum": evidence_ids},
                        },
                        "rationale": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 600,
                        },
                    },
                },
            }
        },
    }
    if validate_app_server_output_schema_subset(schema):
        raise JudgeV5SelectionV197Error("v197 output schema exceeds supported subset")
    return schema


def validate_repair_output_v197(
    output: Any, value: Mapping[str, Any]
) -> list[str]:
    if (
        not isinstance(output, Mapping)
        or set(output) != {"units"}
        or not isinstance(output["units"], list)
    ):
        return ["invalid_repair_output_root"]
    expected: dict[tuple[str, str], set[str]] = {}
    for case in value["cases"]:
        evidence_ids = {str(unit["evidence_id"]) for unit in case["source_units"]}
        for witness in case["witnesses"]:
            expected[(str(case["case_id"]), str(witness["witness_id"]))] = evidence_ids
    required = {
        "case_id",
        "witness_id",
        "support_status",
        "source_evidence_unit_ids",
        "rationale",
    }
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for index, row in enumerate(output["units"]):
        prefix = f"repair_{index}"
        if not isinstance(row, Mapping) or set(row) != required:
            errors.append(prefix + "_shape")
            continue
        key = (str(row["case_id"]), str(row["witness_id"]))
        selected = row["source_evidence_unit_ids"]
        if key not in expected or key in seen:
            errors.append(prefix + "_identity")
            continue
        seen.add(key)
        if (
            row["support_status"] not in {"supported", "unsupported", "abstain"}
            or not isinstance(selected, list)
            or not 1 <= len(selected) <= 4
            or len(selected) != len(set(selected))
            or any(item not in expected[key] for item in selected)
        ):
            errors.append(prefix + "_decision_or_evidence_ids")
        rationale = row["rationale"]
        if not isinstance(rationale, str) or not rationale or len(rationale) > 600:
            errors.append(prefix + "_rationale")
    if seen != set(expected):
        errors.append("repair_coverage_mismatch")
    return errors


def project_repair_output_v197(
    output: Mapping[str, Any], value: Mapping[str, Any]
) -> dict[str, Any]:
    errors = validate_repair_output_v197(output, value)
    if errors:
        raise JudgeV5SelectionV197Error("v197 repair output is invalid")
    evidence: dict[tuple[str, str], str] = {}
    for case in value["cases"]:
        for unit in case["source_units"]:
            evidence[(str(case["case_id"]), str(unit["evidence_id"]))] = str(
                unit["text"]
            )
    rows = []
    for row in output["units"]:
        spans = []
        for evidence_id in row["source_evidence_unit_ids"]:
            text = evidence[(str(row["case_id"]), str(evidence_id))]
            if text not in spans:
                spans.append(text)
        rows.append(
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "support_status": row["support_status"],
                "source_evidence_spans": spans,
                "rationale": row["rationale"],
            }
        )
    return {"units": rows}


def merge_repaired_support_output(
    predecessor: Mapping[str, Any], repaired: Mapping[str, Any]
) -> dict[str, Any]:
    invalid_keys = {
        (
            str(predecessor["output"]["units"][index]["case_id"]),
            str(predecessor["output"]["units"][index]["witness_id"]),
        )
        for index in predecessor["invalid_indices"]
    }
    replacements = {
        (str(row["case_id"]), str(row["witness_id"])): deepcopy(row)
        for row in repaired["units"]
    }
    if set(replacements) != invalid_keys:
        raise JudgeV5SelectionV197Error("v197 replacement coverage drifted")
    rows = []
    for row in predecessor["output"]["units"]:
        key = (str(row["case_id"]), str(row["witness_id"]))
        rows.append(replacements[key] if key in replacements else deepcopy(row))
    rows.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    merged = {"units": rows}
    if v143.validate_support_output(merged, predecessor["value"]):
        raise JudgeV5SelectionV197Error("v197 merged support output is invalid")
    return merged


def _build_capacity_policy(
    root: Path, predecessor: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        MAXIMUM_TOTAL_TOKENS_PER_TURN
        * QUOTA_POINTS_PER_MILLION_TOKENS
        / 1_000_000
    )
    audit = {
        "schema_version": V197_CAPACITY_AUDIT_VERSION,
        "phase_id": V197_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v196_terminal": predecessor["records"]["terminal"],
        "v196_failure": predecessor["records"]["failure"],
        "v196_measured_sidecar": predecessor["attempt"]["sidecar"],
        "measured_basis": {
            "v196_witness_count": 30,
            "v196_valid_support_decision_count": 24,
            "v196_nonexact_evidence_decision_count": 6,
            "v196_measured_total_tokens": 30884,
            "v197_repair_case_count": 2,
            "v197_repair_witness_count": 6,
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V197_CAPACITY_POLICY_VERSION,
        "phase_id": V197_PHASE_ID,
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
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v197(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {
            "root": root,
            "terminal": _load_json(root / "terminal.json", "v197 terminal"),
        }
    predecessor = _validate_v196_failed_attempt()
    value = build_repair_input(predecessor)
    prompt = repair_prompt_v197(value)
    schema = repair_schema_v197(value)
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=value,
        prompt=prompt,
        schema=schema,
    )
    capacity = _build_capacity_policy(root, predecessor)
    instructions = v143.support_base_instructions_v143()
    spec = {
        "schema_version": V197_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": V197_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "preserve_24_valid_v196_support_receipts_rejudge_6_nonexact_evidence_receipts_with_fixed_source_unit_ids",
        "turn_plan": [TURN_NAME],
        "input_witness_count": 30,
        "preserved_valid_support_decision_count": 24,
        "fresh_repair_support_decision_count": 6,
        "repair_case_count": 2,
        "retry_count_per_turn": 0,
        "v196_turn_replayed": False,
        "v196_failure_preserved": True,
        "support_rubric_changed": False,
        "semantic_support_and_evidence_selection_owned_by_llm": True,
        "deterministic_exact_evidence_id_projection_only": True,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "side_labels_present": False,
        "system_identity_present": False,
        "alignment_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "predecessor_attempt": predecessor["attempt"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v196.__file__)),
            _record(Path(v176.__file__)),
            _record(Path(v155.__file__)),
            _record(Path(v143.__file__)),
        ],
        "frozen_instructions": {
            "support_base_sha256": sha256_text(instructions),
            "repair_prompt_prefix_sha256": sha256_text(
                repair_prompt_v197(
                    {
                        "schema_version": V197_INPUT_VERSION,
                        "cases": [],
                        "side_free": True,
                        "system_identity_present": False,
                        "prior_support_decisions_present": False,
                        "source_units_are_deterministic_exact_overlapping_character_windows": True,
                        "semantic_support_and_evidence_selection_owned_by_llm": True,
                    }
                )
            ),
        },
        "frozen_inputs": {
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "support-evidence-repair-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "paths": paths,
        "predecessor": predecessor,
        "value": value,
        "prompt": prompt,
        "schema": schema,
        "instructions": instructions,
    }


def _write_failure(
    root: Path, predecessor: Mapping[str, Any], error_class: str
) -> dict[str, Any]:
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
                _load_json(Path(record["path"]), "v197 sidecar")
            )
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], usage
    )
    failure = {
        "schema_version": V197_FAILURE_VERSION,
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
        "cumulative_known_usage_lower_bound": cumulative,
        "cumulative_unknown_usage_turn_count": 1 + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": 120000
        + unknown * MAXIMUM_TOTAL_TOKENS_PER_TURN,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V197_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "v196_turn_replayed": False,
        "v196_failure_preserved": True,
        "support_receipts_frozen": False,
        "alignment_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_usage_status": "unknown",
        "cumulative_known_usage_lower_bound": cumulative,
        "cumulative_unknown_usage_turn_count": 1 + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": failure[
            "cumulative_conservative_unknown_usage_upper_bound"
        ],
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v197(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v197 terminal")
    frozen = freeze_v197(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=frozen["instructions"],
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=6,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_repair_output_v197(
                    candidate, frozen["value"]
                ),
            )
        projected = project_repair_output_v197(output, frozen["value"])
        merged = merge_repaired_support_output(frozen["predecessor"], projected)
        receipts = v155._support_receipts(merged)
        patch_path = root / "support-evidence-repair-patch.private.json"
        output_path = root / "support-output-merged.private.json"
        receipts_path = root / "support-receipts.private.json"
        diagnostics_path = root / "support-repair-diagnostics.json"
        _write_immutable(patch_path, projected)
        _write_immutable(output_path, merged)
        _write_immutable(receipts_path, receipts)
        status_counts = {status: 0 for status in ("supported", "unsupported", "abstain")}
        for row in merged["units"]:
            status_counts[str(row["support_status"])] += 1
        diagnostics = {
            "schema_version": "pif_app_server_support_evidence_repair_diagnostics_v1",
            "input_witness_count": 30,
            "v196_valid_decisions_preserved": 24,
            "v197_decisions_rejudged": 6,
            "repair_case_count": 2,
            "merged_validator_error_count": 0,
            "all_projected_evidence_exact": True,
            "support_status_counts": status_counts,
            "semantic_similarity_used": False,
            "semantic_regex_or_keyword_rules_used": False,
            "production_mutated": False,
        }
        _write_immutable(diagnostics_path, diagnostics)
        accounting = _aggregate_usage([sidecar])
        cumulative = _sum_usage(
            frozen["predecessor"]["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        terminal = {
            "schema_version": V197_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v197_support_evidence_repair_completed_alignment_authorized",
            "overall_evaluation_complete": False,
            "v196_turn_replayed": False,
            "v196_failure_preserved": True,
            "v196_valid_support_decision_count_preserved": 24,
            "v197_fresh_support_decision_count": 6,
            "support_output": _record(output_path),
            "support_receipts": _record(receipts_path),
            "support_repair_patch": _record(patch_path),
            "support_repair_diagnostics": _record(diagnostics_path),
            "support_receipts_frozen": True,
            "support_status_counts": status_counts,
            "alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "cumulative_usage_status": "unknown",
            "cumulative_known_usage_lower_bound": cumulative,
            "cumulative_unknown_usage_turn_count": 1,
            "cumulative_conservative_unknown_usage_upper_bound": 120000,
            "required_next_artifact_path": str(
                root.parent
                / "development-selection-v5_4-v198-residual-repair-alignment"
                / "terminal.json"
            ),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, frozen["predecessor"], exc.error_class)
    except Exception as exc:
        return _write_failure(root, frozen["predecessor"], type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v197 support evidence repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v197(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "support_receipts_frozen": terminal.get(
                    "support_receipts_frozen", False
                ),
                "alignment_authorized": terminal.get("alignment_authorized", False),
                "usage_status": terminal.get("usage_status"),
                "holdout_authorized": terminal.get("holdout_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
