from __future__ import annotations

"""Repair two omitted equivalence-group memberships without replaying v182."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_selection_v177_alignment_scale_diagnostic as v177
from . import app_server_judge_v5_selection_v181_structural_partition_recovery as v181
from . import app_server_judge_v5_selection_v182_primary_phase1_completion as v182
from .app_server_judge_v5 import normalize_neutral_alignment_output, validate_neutral_alignment_output
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
    _validate_completed_turn,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V183_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v183_spec_v1"
V183_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v183_terminal_v1"
V183_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v183_failure_v1"
V183_REPAIR_INPUT_VERSION = "pif_app_server_judge_v5_4_selection_missing_group_repair_input_v1"
V183_REPAIR_OUTPUT_VERSION = "pif_app_server_judge_v5_4_selection_missing_group_repair_output_v1"
V183_REPAIR_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_missing_group_repair_audit_v1"
V183_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V183_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V183_PHASE_ID = "judge_v5_4_selection_v183_missing_group_repair"

MODEL = v177.MODEL
EFFORT = v177.EFFORT
TURN_NAME = "selection_alignment_missing_group_repair"
TURN_NAMES = (TURN_NAME,)
MAXIMUM_TOTAL_TOKENS_PER_TURN = 120_000
MAXIMUM_PROMPT_BYTES = 90_000
MAXIMUM_SCHEMA_BYTES = 20_000
TIMEOUT_SECONDS = v182.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v182.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v183-missing-group-repair"
).resolve()


class JudgeV5SelectionV183Error(RuntimeError):
    """The immutable v183 missing-group repair cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def missing_group_repair_instructions() -> str:
    return (
        "You are a side-free semantic alignment repair judge. The prior judge assigned every "
        "visible witness to equivalence groups except the explicitly listed omitted witnesses. "
        "For each omitted witness, decide whether it expresses the same event as one existing "
        "group representative or must form a new singleton. Existing-group placement requires "
        "truth-conditional equivalence across actor, attribution, causal mechanism, certainty, "
        "event boundary, event type, evidence, metric, negation, reported actor, speaker, stance, "
        "target, temporal horizon, and unsupported inference. Harmless paraphrase and coreference "
        "may match. Any material conflict requires a new singleton. Use abstain only when the "
        "provided source and witness records cannot support either decision. Cite exact source "
        "substrings when useful. Never infer system identity, vote, or use verbal confidence."
    )


def _validate_v182_failure() -> dict[str, Any]:
    root = v182.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "failure": root / "failure.json",
        "spec": root / "primary-alignment-phase1-completion-spec.json",
        "policy": root / "capacity-policy.json",
    }
    values = {name: _load_json(path, f"v182 {name}") for name, path in paths.items()}
    terminal, failure, spec = values["terminal"], values["failure"], values["spec"]
    usage = {
        "cached_input_tokens": 96256,
        "input_tokens": 280268,
        "output_tokens": 80915,
        "reasoning_output_tokens": 32225,
        "total_tokens": 361183,
    }
    if (
        terminal.get("state") != "failed"
        or terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage_status") != "complete"
        or terminal.get("usage") != usage
        or terminal.get("primary_phase2_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or failure.get("classification") != "infrastructure_or_judge_attempt_failed"
        or failure.get("failed_turn_name") != v182.TURN_NAMES[3]
        or failure.get("error_class") != "JudgeV5CalibrationV26DiagnosticError"
        or failure.get("retry_allowed_in_this_version") is not False
        or failure.get("accounting_complete") is not True
        or failure.get("usage") != usage
        or failure.get("unknown_usage_turn_count") != 0
        or len(failure.get("attempts") or []) != 4
        or spec.get("state") != "frozen_before_model_calls"
        or spec.get("turn_plan") != list(v182.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV183Error("v182 immutable failure contract drifted")
    if terminal.get("failure") != _record(paths["failure"]):
        raise JudgeV5SelectionV183Error("v182 failure binding drifted")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV183Error("v182 runtime record drifted")

    predecessor = v182._validate_v181_success()
    partition = predecessor["partition"]
    rows = partition["phases"][0][5:]
    frozen_turns = spec.get("frozen_inputs", {}).get("turns") or []
    if len(rows) != 4 or len(frozen_turns) != 4:
        raise JudgeV5SelectionV183Error("v182 turn coverage drifted")
    completed, sidecars = [], []
    for index, (turn_name, row) in enumerate(zip(v182.TURN_NAMES, rows, strict=True)):
        turn_root = root / "turns" / turn_name.replace("_", "-")
        turn_paths = {
            "input": turn_root / "input.private.json",
            "prompt": turn_root / "prompt.private.md",
            "schema": turn_root / "schema.json",
            "capacity": turn_root / "capacity.json",
            "sidecar": turn_root / "sidecar.json",
            "output": turn_root / "output.private.json",
        }
        frozen_turn = frozen_turns[index]
        if (
            frozen_turn.get("case_id") != row["case_id"]
            or frozen_turn.get("input") != _record(turn_paths["input"])
            or frozen_turn.get("prompt") != _record(turn_paths["prompt"])
            or frozen_turn.get("schema") != _record(turn_paths["schema"])
            or _load_json(turn_paths["input"], "v182 input") != row["value"]
            or turn_paths["prompt"].read_text() != row["prompt"]
            or _load_json(turn_paths["schema"], "v182 schema") != row["schema"]
        ):
            raise JudgeV5SelectionV183Error("v182 completed request drifted")
        output, sidecar = _validate_completed_turn(
            paths=turn_paths,
            prompt=row["prompt"],
            schema=row["schema"],
            base_instructions=v130.alignment_instructions_v130(),
            model=MODEL,
            effort=EFFORT,
            policy_path=paths["policy"],
            output_validator=lambda candidate: [],
        )
        errors = v181.validate_structurally_completable_output(output, row["value"])
        item = {
            "row": row,
            "turn_name": turn_name,
            "paths": turn_paths,
            "output": output,
            "sidecar": sidecar,
            "errors": errors,
        }
        if index < 3:
            if errors:
                raise JudgeV5SelectionV183Error("v182 valid prefix output drifted")
            projected, audit = v181.project_structural_unpaired_and_exact_spans(
                output, row["value"]
            )
            if (
                projected
                != _load_json(turn_root / "alignment-projected.private.json", "v182 projected")
                or audit
                != _load_json(turn_root / "structural-projection-audit.json", "v182 audit")
            ):
                raise JudgeV5SelectionV183Error("v182 valid projection drifted")
            item.update(
                {
                    "projected": projected,
                    "audit": audit,
                    "normalized": normalize_neutral_alignment_output(projected, row["value"]),
                }
            )
        completed.append(item)
        sidecars.append(sidecar)
    expected_errors = [
        "case_0_equivalence_partition_mismatch",
        "case_0_alignment_partition_mismatch",
    ]
    failed = completed[3]
    if failed["errors"] != expected_errors:
        raise JudgeV5SelectionV183Error("v182 repairable defect drifted")
    case_input = failed["row"]["value"]["cases"][0]
    case_output = failed["output"]["cases"][0]
    all_ids = {str(row["witness_id"]) for row in case_input["witnesses"]}
    grouped_ids = [
        str(value)
        for group in case_output["equivalence_groups"]
        for value in group["witness_ids"]
    ]
    paired_ids = {
        str(value)
        for pair in case_output["alignment_pairs"]
        for value in (pair["witness_id_1"], pair["witness_id_2"])
    }
    unpaired_ids = {str(value) for value in case_output["unpaired_witness_ids"]}
    missing_group_ids = sorted(all_ids - set(grouped_ids))
    missing_alignment_ids = sorted(all_ids - paired_ids - unpaired_ids)
    if (
        len(grouped_ids) != len(set(grouped_ids))
        or set(grouped_ids) - all_ids
        or missing_group_ids != missing_alignment_ids
        or len(missing_group_ids) != 2
    ):
        raise JudgeV5SelectionV183Error("v182 omitted witness evidence drifted")
    accounting = _aggregate_usage(sidecars)
    if accounting.get("usage") != usage:
        raise JudgeV5SelectionV183Error("v182 usage aggregation drifted")
    expected_cumulative = _sum_usage(predecessor["cumulative_usage"], usage)
    if terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative:
        raise JudgeV5SelectionV183Error("v182 cumulative usage drifted")
    normalized_cases = [*predecessor["normalized"]["cases"]]
    for item in completed[:3]:
        normalized_cases.extend(item["normalized"]["cases"])
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "predecessor": predecessor,
        "partition": partition,
        "completed": completed,
        "failed": failed,
        "missing_ids": missing_group_ids,
        "normalized_prefix": {
            "schema_version": "pif_app_server_judge_v5_4_selection_primary_alignment_v183_prefix_v1",
            "cases": sorted(normalized_cases, key=lambda row: str(row["case_id"])),
            "case_count": 8,
            "origin_neutral": True,
        },
        "usage": usage,
        "cumulative_usage": expected_cumulative,
    }


def build_repair_input(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    case_input = predecessor["failed"]["row"]["value"]["cases"][0]
    case_output = predecessor["failed"]["output"]["cases"][0]
    witnesses = {str(row["witness_id"]): row for row in case_input["witnesses"]}
    groups = []
    representative_ids = []
    for index, group in enumerate(case_output["equivalence_groups"]):
        ids = sorted(str(value) for value in group["witness_ids"])
        anchor = ids[0]
        representative_ids.append(anchor)
        groups.append(
            {
                "group_index": index,
                "anchor_witness_id": anchor,
                "existing_member_count": len(ids),
            }
        )
    repair_ids = sorted(set(representative_ids) | set(predecessor["missing_ids"]))
    repair_case = {
        "case_id": str(case_input["case_id"]),
        "source_excerpt": case_input["source_excerpt"],
        "witnesses": [deepcopy(witnesses[value]) for value in repair_ids],
    }
    compact = v177.build_lossless_compact_case(repair_case)
    return {
        "schema_version": V183_REPAIR_INPUT_VERSION,
        "case_id": str(case_input["case_id"]),
        "omitted_witness_ids": list(predecessor["missing_ids"]),
        "existing_group_representatives": groups,
        "lossless_compact_case": compact,
        "supported_witnesses_only": True,
        "structured_field_receipts_withheld": True,
        "origin_neutral": True,
    }


def repair_prompt(value: Mapping[str, Any]) -> str:
    payload = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return (
        "Assign each omitted witness to exactly one existing equivalence group by its listed "
        "anchor_witness_id, or to a new singleton. Decode lossless_compact_case using its "
        "string_table and event-definition references. Do not alter or reassess any other group. "
        "For existing_group, target_group_anchor_witness_id must be the chosen anchor and "
        "witness_evidence_ids must contain the omitted ID and anchor. For new_singleton or "
        "abstain, target_group_anchor_witness_id must be empty and witness_evidence_ids must "
        "contain only the omitted ID. Return exact source substrings only.\nINPUT_JSON:\n" + payload
    )


def repair_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    missing = list(value["omitted_witness_ids"])
    anchors = [row["anchor_witness_id"] for row in value["existing_group_representatives"]]
    evidence_ids = sorted(set(missing) | set(anchors))
    assignment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "witness_id",
            "placement",
            "target_group_anchor_witness_id",
            "source_evidence_spans",
            "witness_evidence_ids",
            "rationale",
        ],
        "properties": {
            "witness_id": {"type": "string", "enum": missing},
            "placement": {
                "type": "string",
                "enum": ["existing_group", "new_singleton", "abstain"],
            },
            "target_group_anchor_witness_id": {
                "type": "string",
                "enum": ["", *anchors],
            },
            "source_evidence_spans": {
                "type": "array",
                "maxItems": 4,
                "uniqueItems": True,
                "items": {"type": "string", "minLength": 1},
            },
            "witness_evidence_ids": {
                "type": "array",
                "minItems": 1,
                "maxItems": 2,
                "uniqueItems": True,
                "items": {"type": "string", "enum": evidence_ids},
            },
            "rationale": {"type": "string", "minLength": 1},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["assignments"],
        "properties": {
            "assignments": {
                "type": "array",
                "minItems": len(missing),
                "maxItems": len(missing),
                "items": assignment,
            }
        },
    }


def validate_repair_output(output: Any, value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, dict) or set(output) != {"assignments"}:
        return ["invalid_repair_root"]
    assignments = output.get("assignments")
    missing = set(value["omitted_witness_ids"])
    anchors = {
        row["anchor_witness_id"] for row in value["existing_group_representatives"]
    }
    if not isinstance(assignments, list) or len(assignments) != len(missing):
        return ["invalid_repair_assignment_count"]
    errors, seen = [], set()
    source = v177.decode_lossless_compact_case(value["lossless_compact_case"])[
        "source_excerpt"
    ]
    required = {
        "witness_id",
        "placement",
        "target_group_anchor_witness_id",
        "source_evidence_spans",
        "witness_evidence_ids",
        "rationale",
    }
    for index, row in enumerate(assignments):
        prefix = f"assignment_{index}"
        if not isinstance(row, dict) or set(row) != required:
            errors.append(prefix + "_invalid_shape")
            continue
        witness_id = row.get("witness_id")
        placement = row.get("placement")
        target = row.get("target_group_anchor_witness_id")
        spans = row.get("source_evidence_spans")
        evidence = row.get("witness_evidence_ids")
        if witness_id not in missing or witness_id in seen:
            errors.append(prefix + "_invalid_witness_id")
            continue
        seen.add(witness_id)
        if (
            placement not in {"existing_group", "new_singleton", "abstain"}
            or not isinstance(spans, list)
            or len(spans) > 4
            or len(spans) != len(set(spans))
            or any(not isinstance(span, str) or not span or span not in source for span in spans)
            or not isinstance(evidence, list)
            or len(evidence) != len(set(evidence))
            or not isinstance(row.get("rationale"), str)
            or not row["rationale"]
        ):
            errors.append(prefix + "_invalid_evidence_or_decision")
            continue
        expected_evidence = {witness_id}
        if placement == "existing_group":
            if target not in anchors:
                errors.append(prefix + "_invalid_existing_target")
                continue
            expected_evidence.add(target)
        elif target != "":
            errors.append(prefix + "_nonempty_new_or_abstain_target")
            continue
        if set(evidence) != expected_evidence:
            errors.append(prefix + "_witness_evidence_mismatch")
    if seen != missing:
        errors.append("repair_assignment_coverage_mismatch")
    return errors


def apply_group_repair(
    predecessor: Mapping[str, Any], repair_output: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    value = build_repair_input(predecessor)
    errors = validate_repair_output(repair_output, value)
    if errors:
        raise JudgeV5SelectionV183Error("invalid missing-group repair output")
    if any(row["placement"] == "abstain" for row in repair_output["assignments"]):
        raise JudgeV5SelectionV183Error("missing-group repair abstained")
    failed = predecessor["failed"]
    repaired = deepcopy(failed["output"])
    case = repaired["cases"][0]
    by_anchor = {
        str(group["witness_ids"][0] if len(group["witness_ids"]) == 1 else sorted(group["witness_ids"])[0]): group
        for group in case["equivalence_groups"]
    }
    assignment_audit = []
    for row in sorted(repair_output["assignments"], key=lambda item: item["witness_id"]):
        witness_id = str(row["witness_id"])
        if row["placement"] == "existing_group":
            anchor = str(row["target_group_anchor_witness_id"])
            if anchor not in by_anchor:
                raise JudgeV5SelectionV183Error("repair target anchor disappeared")
            by_anchor[anchor]["witness_ids"].append(witness_id)
            by_anchor[anchor]["witness_ids"] = sorted(by_anchor[anchor]["witness_ids"])
        else:
            case["equivalence_groups"].append(
                {
                    "witness_ids": [witness_id],
                    "rationale": row["rationale"],
                }
            )
        case["unpaired_witness_ids"].append(witness_id)
        assignment_audit.append(
            {
                "witness_id": witness_id,
                "placement": row["placement"],
                "target_group_anchor_witness_id": row["target_group_anchor_witness_id"],
            }
        )
    case["equivalence_groups"] = sorted(
        case["equivalence_groups"], key=lambda group: tuple(sorted(group["witness_ids"]))
    )
    case["unpaired_witness_ids"] = sorted(case["unpaired_witness_ids"])
    projected, structural_audit = v181.project_structural_unpaired_and_exact_spans(
        repaired, failed["row"]["value"]
    )
    audit = {
        "schema_version": V183_REPAIR_AUDIT_VERSION,
        "repaired_witness_count": len(assignment_audit),
        "assignments": assignment_audit,
        "prior_valid_group_memberships_changed": False,
        "prior_alignment_pairs_changed": False,
        "prior_checklists_changed": False,
        "prior_rationales_changed": False,
        "llm_owned_group_placement": True,
        "deterministic_operations": [
            "apply_llm_group_assignment_by_exact_opaque_identity",
            "append_repaired_ids_to_unpaired_coverage",
            "sort_identity_arrays",
        ],
        "structural_projection": structural_audit,
    }
    return projected, audit


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V183_CAPACITY_AUDIT_VERSION,
        "phase_id": V183_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v182_terminal": predecessor["records"]["terminal"],
        "v182_failure": predecessor["records"]["failure"],
        "measured_basis": {
            "omitted_witness_count": len(predecessor["missing_ids"]),
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": bound,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V183_CAPACITY_POLICY_VERSION,
        "phase_id": V183_PHASE_ID,
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
        "maximum_total_tokens_per_turn": bound,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v183(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v183 terminal")}
    predecessor = _validate_v182_failure()
    value = build_repair_input(predecessor)
    prompt = repair_prompt(value)
    schema = repair_schema(value)
    prompt_bytes, schema_bytes = v177._request_bytes(prompt, schema)
    if prompt_bytes > MAXIMUM_PROMPT_BYTES or schema_bytes > MAXIMUM_SCHEMA_BYTES:
        raise JudgeV5SelectionV183Error("v183 repair request exceeds frozen byte cap")
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=value,
        prompt=prompt,
        schema=schema,
    )
    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V183_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V183_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_capped_side_free_two_witness_group_membership_repair",
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "v182_completed_turn_replayed": False,
        "v182_completed_output_adopted": True,
        "full_case_reread": False,
        "omitted_witness_count": len(predecessor["missing_ids"]),
        "existing_group_representative_count": len(value["existing_group_representatives"]),
        "prompt_bytes": prompt_bytes,
        "schema_bytes": schema_bytes,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "lossless_compact_serialization": True,
        "semantic_fields_pruned": False,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "support_receipts_frozen": True,
        "primary_alignment_complete": False,
        "primary_phase2_authorized": False,
        "balanced_canary_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v182.__file__)),
            _record(Path(v181.__file__)),
            _record(Path(v177.__file__)),
            _record(Path(v130.__file__)),
            *predecessor["values"]["spec"]["runtime_files"],
        ],
        "frozen_instructions": {
            "repair_sha256": sha256_text(missing_group_repair_instructions()),
        },
        "frozen_inputs": {
            "pool": predecessor["predecessor"]["predecessor"]["predecessor"][
                "predecessor"
            ]["pool_record"],
            "support_receipts": predecessor["predecessor"]["predecessor"]["predecessor"][
                "predecessor"
            ]["receipts_record"],
            "primary_partition": predecessor["predecessor"]["predecessor"][
                "predecessor"
            ]["records"]["partition"],
            "turn": {
                "turn_name": TURN_NAME,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
                "input": _record(paths["input"]),
                "prompt": _record(paths["prompt"]),
                "schema": _record(paths["schema"]),
            },
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "missing-group-repair-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "predecessor": predecessor,
        "value": value,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
    }


def _write_failure(
    root: Path, predecessor: Mapping[str, Any], turn_name: Optional[str], error_class: str
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
            measured = _validate_usage(_load_json(Path(record["path"]), "v183 sidecar"))
        except Exception:
            unknown += 1
            continue
        usage = _sum_usage(usage, measured)
    complete = unknown == 0
    cumulative = _sum_usage(predecessor["cumulative_usage"], usage)
    failure = {
        "schema_version": V183_FAILURE_VERSION,
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
        "schema_version": V183_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "primary_alignment_complete": False,
        "primary_phase2_authorized": False,
        "balanced_canary_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v183(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v183 terminal")
    frozen = freeze_v183(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            current_turn = TURN_NAME
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=missing_group_repair_instructions(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=len(frozen["value"]["omitted_witness_ids"]),
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: validate_repair_output(
                    candidate, frozen["value"]
                ),
            )
        if any(row["placement"] == "abstain" for row in output["assignments"]):
            accounting = _aggregate_usage([sidecar])
            cumulative = _sum_usage(
                frozen["predecessor"]["cumulative_usage"], accounting["usage"]
            )
            terminal = {
                "schema_version": V183_TERMINAL_VERSION,
                "state": "inactive",
                "terminal_at": now_iso(),
                "terminal_reason": "v183_missing_group_repair_abstained_recovery_required",
                "overall_evaluation_complete": False,
                "primary_alignment_complete": False,
                "primary_phase2_authorized": False,
                "balanced_canary_authorized": False,
                "selection_winner_frozen": False,
                "holdout_authorized": False,
                "production_mutated": False,
                "semantic_retry_count": 0,
                "cumulative_evaluation_usage": cumulative,
                **accounting,
            }
            _write_immutable(terminal_path, terminal)
            return terminal
        repaired, repair_audit = apply_group_repair(frozen["predecessor"], output)
        repaired_path = root / "repaired-primary-alignment-case.private.json"
        repair_audit_path = root / "missing-group-repair-audit.json"
        _write_immutable(repaired_path, repaired)
        _write_immutable(repair_audit_path, repair_audit)
        normalized_case = normalize_neutral_alignment_output(
            repaired, frozen["predecessor"]["failed"]["row"]["value"]
        )["cases"]
        normalized = {
            "schema_version": "pif_app_server_judge_v5_4_selection_primary_alignment_phase1_complete_v183_v1",
            "cases": sorted(
                [*frozen["predecessor"]["normalized_prefix"]["cases"], *normalized_case],
                key=lambda row: str(row["case_id"]),
            ),
            "case_count": 9,
            "missing_group_repair_count": 2,
            "origin_neutral": True,
        }
        normalized_path = root / "primary-alignment-phase1-complete.private.json"
        _write_immutable(normalized_path, normalized)
        accounting = _aggregate_usage([sidecar])
        cumulative = _sum_usage(
            frozen["predecessor"]["cumulative_usage"], accounting["usage"]
        )
        terminal = {
            "schema_version": V183_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v183_missing_group_repair_completed_phase2_authorized",
            "overall_evaluation_complete": False,
            "v182_completed_turn_replayed": False,
            "v182_completed_output_adopted": True,
            "repair_turn_count": 1,
            "repaired_witness_count": 2,
            "primary_alignment_complete": False,
            "primary_phase1_complete": True,
            "primary_phase2_authorized": True,
            "balanced_canary_authorized": False,
            "selection_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "normalized_output": _record(normalized_path),
            "repaired_case": _record(repaired_path),
            "repair_audit": _record(repair_audit_path),
            "predecessor_cumulative_usage": frozen["predecessor"]["cumulative_usage"],
            "cumulative_evaluation_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(
            root, frozen["predecessor"], exc.turn_name, exc.error_class
        )
    except Exception as exc:
        return _write_failure(
            root, frozen["predecessor"], current_turn, type(exc).__name__
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v183 missing-group repair")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v183(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "primary_phase2_authorized": terminal.get("primary_phase2_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
