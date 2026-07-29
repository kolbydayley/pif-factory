from __future__ import annotations

"""Recover v175 with smaller turns and LLM-selected exact evidence-unit IDs."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_selection_v175_support as v175
from .app_server_judge_v5_calibration_v25_diagnostic import QUOTA_POINTS_PER_MILLION_TOKENS
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
from .app_server_llm_judge import validate_app_server_output_schema_subset
from .util import now_iso, sha256_text


V176_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_support_v176_spec_v1"
V176_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_support_v176_terminal_v1"
V176_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_support_v176_failure_v1"
V176_EVIDENCE_VERSION = "pif_app_server_selection_support_evidence_units_v1"
V176_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V176_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V176_PHASE_ID = "judge_v5_4_selection_v176_support_evidence_ids"

MODEL = v175.MODEL
EFFORT = v175.EFFORT
SOURCE_UNIT_CHARS = 450
SOURCE_UNIT_OVERLAP_CHARS = 50
MAXIMUM_TOTAL_TOKENS_PER_TURN = 45_000
TIMEOUT_SECONDS = v175.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v175.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v176-support-evidence-ids"
).resolve()


class JudgeV5SelectionV176Error(RuntimeError):
    """The v176 selection-support recovery contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v175_failure() -> dict[str, Any]:
    root = v175.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "selection-support-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
    }
    values = {name: _load_json(path, f"v175 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    expected_usage = {
        "input_tokens": 31170,
        "cached_input_tokens": 0,
        "output_tokens": 14823,
        "reasoning_output_tokens": 1780,
        "total_tokens": 45993,
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != expected_usage
        or terminal.get("support_receipts_frozen") is not False
        or terminal.get("alignment_authorized") is not False
        or terminal.get("production_mutated") is not False
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != "selection_support_00"
        or failure.get("error_class") != "ReserveCapacityError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("unknown_usage_turn_count") != 0
        or failure.get("usage") != expected_usage
        or spec.get("retry_count_per_turn") != 0
    ):
        raise JudgeV5SelectionV176Error("v175 failure contract drifted")
    attempts = [
        row for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    if len(attempts) != 1:
        raise JudgeV5SelectionV176Error("v175 attempt coverage drifted")
    attempt = attempts[0]
    for key in ("capacity", "sidecar", "output"):
        if not isinstance(attempt.get(key), Mapping):
            raise JudgeV5SelectionV176Error(f"v175 {key} record is missing")
        _verify_record(attempt[key])
    _validate_usage(_load_json(Path(attempt["sidecar"]["path"]), "v175 sidecar"))
    request = spec["frozen_inputs"]["turns"][0]
    for key in ("input", "prompt", "schema"):
        _verify_record(request[key])
    value = _load_json(Path(request["input"]["path"]), "v175 failed support input")
    output = _load_json(Path(attempt["output"]["path"]), "v175 failed support output")
    errors = v143.validate_support_output(output, value)
    if len(errors) != 12 or any(not error.endswith("_evidence_not_exact") for error in errors):
        raise JudgeV5SelectionV176Error("v175 evidence failure classification drifted")
    gate = v175._validate_v174_selection_gate()
    sources = v175._validate_selection_sources(v175.DEFAULT_REUSE_CONTRACT)
    cumulative = _sum_usage(gate["cumulative_usage"], expected_usage)
    if terminal.get("cumulative_known_usage_lower_bound") != cumulative:
        raise JudgeV5SelectionV176Error("v175 cumulative usage drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "gate": gate,
        "sources": sources,
        "cumulative_usage": cumulative,
        "validator_error_count": len(errors),
    }


def build_source_units(source: str) -> list[dict[str, Any]]:
    if not isinstance(source, str) or not source:
        raise JudgeV5SelectionV176Error("support source is empty")
    step = SOURCE_UNIT_CHARS - SOURCE_UNIT_OVERLAP_CHARS
    units = []
    start = 0
    while start < len(source):
        end = min(len(source), start + SOURCE_UNIT_CHARS)
        units.append(
            {
                "evidence_id": f"ev_{len(units):03d}",
                "start": start,
                "end": end,
                "text": source[start:end],
            }
        )
        if end == len(source):
            break
        start += step
    if any(unit["text"] != source[unit["start"] : unit["end"]] for unit in units):
        raise JudgeV5SelectionV176Error("source-unit grounding drifted")
    return units


def build_case_input(units: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    case_ids = {str(row["case_id"]) for row in units}
    sources = {str(row["source_excerpt"]) for row in units}
    if len(case_ids) != 1 or len(sources) != 1 or not units:
        raise JudgeV5SelectionV176Error("support turn must contain one nonempty source case")
    source = next(iter(sources))
    return {
        "schema_version": V176_EVIDENCE_VERSION,
        "case_id": next(iter(case_ids)),
        "source_units": build_source_units(source),
        "witnesses": [
            {
                "witness_id": row["witness_id"],
                "proposition": deepcopy(row["proposition"]),
            }
            for row in sorted(units, key=lambda row: str(row["witness_id"]))
        ],
        "source_units_are_deterministic_exact_overlapping_character_windows": True,
        "semantic_evidence_selection_owned_by_llm": True,
    }


def support_prompt_v176(value: Mapping[str, Any]) -> str:
    return (
        "Return one independent support decision for every opaque witness_id. Preserve the supplied "
        "case_id and witness_id exactly. Select one to four source_evidence_unit_ids that directly "
        "support or contradict the decision; IDs refer to exact source substrings and must be copied "
        "from source_units. Do not quote or paraphrase source text in the output.\n\n"
        + json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        + "\n"
    )


def support_schema_v176(value: Mapping[str, Any]) -> dict[str, Any]:
    witness_ids = [str(row["witness_id"]) for row in value["witnesses"]]
    evidence_ids = [str(row["evidence_id"]) for row in value["source_units"]]
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
                        "case_id", "witness_id", "support_status",
                        "source_evidence_unit_ids", "rationale",
                    ],
                    "properties": {
                        "case_id": {"type": "string", "enum": [value["case_id"]]},
                        "witness_id": {"type": "string", "enum": witness_ids},
                        "support_status": {
                            "type": "string", "enum": ["supported", "unsupported", "abstain"]
                        },
                        "source_evidence_unit_ids": {
                            "type": "array", "minItems": 1, "maxItems": 4,
                            "items": {"type": "string", "enum": evidence_ids},
                        },
                        "rationale": {"type": "string", "minLength": 1, "maxLength": 600},
                    },
                },
            }
        },
    }
    if validate_app_server_output_schema_subset(schema):
        raise JudgeV5SelectionV176Error("v176 support schema exceeds supported subset")
    return schema


def validate_support_output_v176(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, Mapping) or set(output) != {"units"} or not isinstance(output["units"], list):
        return ["invalid_support_output_root"]
    expected = {str(row["witness_id"]) for row in value["witnesses"]}
    evidence_ids = {str(row["evidence_id"]) for row in value["source_units"]}
    errors, seen = [], set()
    required = {"case_id", "witness_id", "support_status", "source_evidence_unit_ids", "rationale"}
    for index, row in enumerate(output["units"]):
        prefix = f"unit_{index}"
        if not isinstance(row, Mapping) or set(row) != required:
            errors.append(prefix + "_shape")
            continue
        witness_id = str(row["witness_id"])
        selected = row["source_evidence_unit_ids"]
        if witness_id not in expected or witness_id in seen or row["case_id"] != value["case_id"]:
            errors.append(prefix + "_identity")
            continue
        seen.add(witness_id)
        if (
            row["support_status"] not in {"supported", "unsupported", "abstain"}
            or not isinstance(selected, list)
            or not 1 <= len(selected) <= 4
            or len(selected) != len(set(selected))
            or any(item not in evidence_ids for item in selected)
        ):
            errors.append(prefix + "_decision_or_evidence_ids")
        if not isinstance(row["rationale"], str) or not row["rationale"]:
            errors.append(prefix + "_rationale")
    if seen != expected:
        errors.append("support_coverage_mismatch")
    return errors


def project_support_output_v176(
    output: Mapping[str, Any], value: Mapping[str, Any]
) -> dict[str, Any]:
    errors = validate_support_output_v176(output, value)
    if errors:
        raise JudgeV5SelectionV176Error("v176 support output is invalid")
    evidence = {str(row["evidence_id"]): str(row["text"]) for row in value["source_units"]}
    rows = []
    for row in output["units"]:
        spans = []
        for evidence_id in row["source_evidence_unit_ids"]:
            text = evidence[evidence_id]
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


def plan_case_turns(representatives: Sequence[Mapping[str, Any]]) -> list[list[dict[str, Any]]]:
    by_case: dict[str, list[dict[str, Any]]] = {}
    for unit in representatives:
        by_case.setdefault(str(unit["case_id"]), []).append(deepcopy(unit))
    turns = [sorted(by_case[case_id], key=lambda row: str(row["witness_id"])) for case_id in sorted(by_case)]
    if len(turns) != 28 or sum(map(len, turns)) != 1566 or max(map(len, turns)) != 76:
        raise JudgeV5SelectionV176Error("v176 one-case turn plan drifted")
    return turns


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any], turn_count: int) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    turn_names = tuple(f"selection_support_evidence_ids_{index:02d}" for index in range(turn_count))
    bound = turn_count * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V176_CAPACITY_AUDIT_VERSION,
        "phase_id": V176_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v175_terminal": predecessor["records"]["terminal"],
        "v175_failure": predecessor["records"]["failure"],
        "measured_basis": {
            "v175_failed_turn_units": 120,
            "v175_measured_total_tokens": 45993,
            "v175_nonexact_evidence_count": 12,
            "v176_maximum_units_per_turn": 76,
            "v176_evidence_output_is_ids_not_copied_text": True,
            "declared_turn_count": turn_count,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V176_CAPACITY_POLICY_VERSION,
        "phase_id": V176_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(turn_names),
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v176(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v176 terminal")}
    predecessor = _validate_v175_failure()
    representatives, expansion, dedup_audit = v175.build_exact_claim_dedup(
        predecessor["sources"]["pointwise"]
    )
    case_turns = plan_case_turns(representatives)
    expansion_path = root / "exact-claim-expansion.private.json"
    dedup_path = root / "exact-claim-dedup-audit.json"
    _write_immutable(
        expansion_path,
        {"schema_version": v175.V175_DEDUP_VERSION, "representative_to_witness_ids": expansion},
    )
    _write_stable_time(dedup_path, {**dedup_audit, "created_at": now_iso()}, "created_at")
    turns = []
    for index, units in enumerate(case_turns):
        turn_name = f"selection_support_evidence_ids_{index:02d}"
        value = build_case_input(units)
        prompt = support_prompt_v176(value)
        schema = support_schema_v176(value)
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        turns.append(
            {"turn_name": turn_name, "value": value, "prompt": prompt, "schema": schema, "paths": paths}
        )
    capacity = _build_capacity_policy(root, predecessor, len(turns))
    spec = {
        "schema_version": V176_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V176_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_case_per_turn_llm_selected_fixed_character_evidence_ids",
        "input_witness_count": 1960,
        "representative_count": 1566,
        "nonempty_case_count": 28,
        "turn_plan": [turn["turn_name"] for turn in turns],
        "retry_count_per_turn": 0,
        "v175_output_reused": False,
        "source_unit_chars": SOURCE_UNIT_CHARS,
        "source_unit_overlap_chars": SOURCE_UNIT_OVERLAP_CHARS,
        "semantic_evidence_selection_owned_by_llm": True,
        "deterministic_evidence_projection_only": True,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "support_receipts_frozen": False,
        "alignment_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "v174_gate": predecessor["gate"]["records"],
        "reuse_contract": predecessor["sources"]["contract_record"],
        "runtime_files": [_record(Path(__file__)), *predecessor["values"]["spec"]["runtime_files"]],
        "frozen_instructions": {
            "support_base_sha256": sha256_text(v143.support_base_instructions_v143()),
            "evidence_id_prompt_prefix_sha256": sha256_text(support_prompt_v176({"hash_sentinel": True})),
        },
        "frozen_inputs": {
            "pool": predecessor["sources"]["pool_record"],
            "dedup_audit": _record(dedup_path),
            "exact_claim_expansion": _record(expansion_path),
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "selection-support-evidence-ids-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "predecessor": predecessor,
        "expansion": expansion,
    }


def _write_failure(
    root: Path, predecessor: Mapping[str, Any], turn_name: Optional[str], error_class: str
) -> dict[str, Any]:
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v176 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    cumulative = _sum_usage(predecessor["cumulative_usage"], usage)
    failure = {
        "schema_version": V176_FAILURE_VERSION,
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
        "predecessor_cumulative_usage": predecessor["cumulative_usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V176_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "support_receipts_frozen": False,
        "alignment_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v176(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v176 terminal")
    frozen = freeze_v176(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    outputs, sidecars = [], []
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v143.support_base_instructions_v143(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["witnesses"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_support_output_v176(candidate, item),
                )
                outputs.append(project_support_output_v176(output, turn["value"]))
                sidecars.append(sidecar)
        representative_output = v155._merge_outputs(outputs, "units")
        sources = frozen["predecessor"]["sources"]
        full_value = v175._support_value(sources["pointwise"]["units"])
        expanded = v175.expand_support_output(
            representative_output=representative_output,
            expansion=frozen["expansion"],
            full_value=full_value,
        )
        receipts = v155._support_receipts(expanded)
        output_path = root / "support-output-expanded.private.json"
        receipts_path = root / "support-receipts.private.json"
        _write_immutable(output_path, expanded)
        _write_immutable(receipts_path, receipts)
        counts = {status: 0 for status in ("supported", "unsupported", "abstain")}
        for row in expanded["units"]:
            counts[row["support_status"]] += 1
        accounting = _aggregate_usage(sidecars)
        cumulative = _sum_usage(frozen["predecessor"]["cumulative_usage"], accounting["usage"])
        terminal = {
            "schema_version": V176_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v176_selection_support_completed_alignment_authorized",
            "overall_evaluation_complete": False,
            "support_receipts_frozen": True,
            "alignment_authorized": True,
            "selection_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "input_witness_count": 1960,
            "representative_semantic_decision_count": 1566,
            "exact_identity_expanded_decision_count": 394,
            "support_status_counts": counts,
            "support_output": _record(output_path),
            "support_receipts": _record(receipts_path),
            "v174_protocol": frozen["predecessor"]["gate"]["records"]["protocol"],
            "predecessor_cumulative_usage": frozen["predecessor"]["cumulative_usage"],
            "cumulative_evaluation_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, frozen["predecessor"], exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, frozen["predecessor"], current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v176 support evidence-ID recovery")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v176(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({
        "state": terminal["state"],
        "terminal_reason": terminal["terminal_reason"],
        "support_receipts_frozen": terminal.get("support_receipts_frozen", False),
        "alignment_authorized": terminal.get("alignment_authorized", False),
        "usage_status": terminal.get("usage_status"),
    }, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
