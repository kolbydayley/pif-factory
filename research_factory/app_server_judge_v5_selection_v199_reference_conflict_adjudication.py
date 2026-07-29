from __future__ import annotations

"""Resolve only v198 repair placements that cross frozen reference groups."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_selection_v177_alignment_scale_diagnostic as v177
from . import app_server_judge_v5_selection_v198_residual_repair_alignment as v198
from .app_server_judge_v5 import (
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
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
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .util import now_iso, sha256_text


V199_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v199_spec_v1"
V199_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v199_terminal_v1"
V199_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v199_failure_v1"
V199_PLAN_VERSION = "pif_app_server_reference_conflict_adjudication_plan_v1"
V199_RECONCILIATION_VERSION = "pif_app_server_reference_conflict_reconciliation_v1"
V199_AUDIT_VERSION = "pif_app_server_reference_conflict_reconciliation_audit_v1"
V199_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V199_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V199_PHASE_ID = "judge_v5_4_selection_v199_reference_conflict_adjudication"

MODEL = v177.MODEL
EFFORT = v177.EFFORT
TURN_NAME = "selection_reference_conflict_adjudication"
MAXIMUM_TOTAL_TOKENS_PER_TURN = 100_000
MAXIMUM_PROMPT_BYTES = 40_000
MAXIMUM_SCHEMA_BYTES = 20_000
TIMEOUT_SECONDS = v198.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v198.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v199-reference-conflict-adjudication"
).resolve()


class JudgeV5SelectionV199Error(RuntimeError):
    """The capped v199 reference-conflict adjudication cannot proceed safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v198_conflict() -> dict[str, Any]:
    root = v198.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "spec": root / "residual-repair-alignment-spec.json",
        "alignment": root / "residual-repair-alignment.private.json",
        "audit": root / "residual-repair-alignment-audit.json",
        "pool": root / "augmented-alignment-pool.private.json",
        "receipts": root / "augmented-support-receipts.private.json",
        "mapping": root / "augmented-alignment-mapping.private.json",
    }
    values = {name: _load_json(path, f"v198 {name}") for name, path in paths.items()}
    terminal, spec, audit = values["terminal"], values["spec"], values["audit"]
    expected_usage = {
        "input_tokens": 167203,
        "cached_input_tokens": 54784,
        "output_tokens": 116717,
        "reasoning_output_tokens": 59465,
        "total_tokens": 283920,
    }
    expected_cumulative = {
        "input_tokens": 7355828,
        "cached_input_tokens": 876032,
        "output_tokens": 1352413,
        "reasoning_output_tokens": 439201,
        "total_tokens": 8708241,
    }
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason")
        != "v198_frozen_reference_partition_conflict_requires_capped_adjudication"
        or terminal.get("terminal_classification")
        != "inactive_incomplete_recovery_required"
        or terminal.get("alignment_completed") is not True
        or terminal.get("frozen_reference_partition_conflict_count") != 7
        or terminal.get("scoring_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != expected_usage
        or terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or audit.get("frozen_reference_partition_conflict_count") != 7
        or audit.get("scoring_safe") is not False
        or audit.get("repair_witness_count") != 30
        or audit.get("semantic_similarity_used") is not False
        or audit.get("semantic_regex_or_keyword_rules_used") is not False
        or spec.get("turn_plan") != list(v198.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("scoring_authorized") is not False
        or spec.get("holdout_authorized") is not False
    ):
        raise JudgeV5SelectionV199Error("v198 conflict terminal drifted")
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5SelectionV199Error("v198 artifact disappeared")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV199Error("v198 runtime binding drifted")
    records = [
        spec["capacity_policy"],
        spec["capacity_audit"],
        spec["frozen_reference_consensus"],
        *spec["v192_authorization"].values(),
        spec["frozen_inputs"]["pool"],
        spec["frozen_inputs"]["support_receipts"],
        spec["frozen_inputs"]["private_mapping"],
    ]
    for turn in spec["frozen_inputs"]["turns"]:
        records.extend(turn[key] for key in ("input", "prompt", "schema"))
    if any(not _verify_record(record) for record in records):
        raise JudgeV5SelectionV199Error("v198 frozen input binding drifted")
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    if len(attempts) != 4:
        raise JudgeV5SelectionV199Error("v198 attempt coverage drifted")
    measured = {field: 0 for field in USAGE_FIELDS}
    for attempt in attempts:
        for key in ("capacity", "sidecar", "output"):
            if not isinstance(attempt.get(key), Mapping) or not _verify_record(attempt[key]):
                raise JudgeV5SelectionV199Error(f"v198 {key} record drifted")
        usage = _validate_usage(_load_json(Path(attempt["sidecar"]["path"]), "v198 sidecar"))
        measured = _sum_usage(measured, usage)
    if measured != expected_usage:
        raise JudgeV5SelectionV199Error("v198 measured usage drifted")
    recomputed = v198.build_reference_conflict_audit(
        values["alignment"]["cases"], values["mapping"]
    )
    if recomputed != audit:
        raise JudgeV5SelectionV199Error("v198 conflict audit drifted")
    predecessor = v198._validate_v197_success()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "spec": spec,
        "alignment": values["alignment"],
        "mapping": values["mapping"],
        "predecessor": predecessor,
    }


def _input_cases(source: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    cases: dict[str, dict[str, Any]] = {}
    for turn in source["spec"]["frozen_inputs"]["turns"]:
        value = _load_json(Path(turn["input"]["path"]), "v198 alignment input")
        for case in value["cases"]:
            case_id = str(case["case_id"])
            if case_id in cases:
                raise JudgeV5SelectionV199Error("v198 input case duplicated")
            cases[case_id] = case
    return cases


def build_v199_input(
    source: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    input_cases = _input_cases(source)
    alignment = {str(row["case_id"]): row for row in source["alignment"]["cases"]}
    mapping = {str(row["case_id"]): row for row in source["mapping"]["cases"]}
    disputes, conflicts = [], []
    for case_id in sorted(mapping):
        references = {
            str(row["representative_witness_id"])
            for row in mapping[case_id]["reference_groups"]
        }
        repairs = {str(value) for value in mapping[case_id]["repair_witness_ids"]}
        witnesses = {
            str(row["witness_id"]): row for row in input_cases[case_id]["witnesses"]
        }
        for group_index, group in enumerate(alignment[case_id]["equivalence_groups"]):
            group_ids = {str(value) for value in group}
            group_references = sorted(group_ids & references)
            group_repairs = sorted(group_ids & repairs)
            if len(group_references) <= 1:
                continue
            if len(group_references) != 2 or len(group_repairs) > 1:
                raise JudgeV5SelectionV199Error("v198 conflict shape exceeds capped contract")
            row = {
                "original_case_id": case_id,
                "original_group_index": group_index,
                "reference_witness_ids": group_references,
                "repair_witness_ids": group_repairs,
                "all_witness_ids": sorted(group_ids),
            }
            if group_repairs:
                dispute_case_id = "jcase_" + sha256_text(
                    "v199|" + case_id + "|" + "|".join(sorted(group_ids))
                )[:24]
                dispute_witnesses = []
                for witness_id in sorted(group_ids):
                    witness = deepcopy(witnesses[witness_id])
                    receipt = witness.get("support_receipt")
                    if isinstance(receipt, dict) and "case_id" in receipt:
                        receipt["case_id"] = dispute_case_id
                    dispute_witnesses.append(witness)
                disputes.append(
                    {
                        "case_id": dispute_case_id,
                        "source_excerpt": input_cases[case_id]["source_excerpt"],
                        "witnesses": dispute_witnesses,
                    }
                )
                row["dispute_case_id"] = dispute_case_id
                row["resolution_mode"] = "one_capped_side_free_llm_adjudication"
            else:
                row["dispute_case_id"] = None
                row["resolution_mode"] = "preserve_frozen_reference_partition_structurally"
            conflicts.append(row)
    if (
        len(conflicts) != 7
        or len(disputes) != 3
        or sum(not row["repair_witness_ids"] for row in conflicts) != 4
        or any(len(row["witnesses"]) != 3 for row in disputes)
        or len({row["case_id"] for row in disputes}) != 3
    ):
        raise JudgeV5SelectionV199Error("v199 capped dispute coverage drifted")
    template = _load_json(
        Path(source["spec"]["frozen_inputs"]["turns"][0]["input"]["path"]),
        "v198 alignment template",
    )
    value = {
        **{key: deepcopy(value) for key, value in template.items() if key != "cases"},
        "permutation": "base",
        "permuted_axes": [],
        "cases": sorted(disputes, key=lambda row: str(row["case_id"])),
        "side_labels_present": False,
        "system_identity_present": False,
        "support_receipts_frozen": True,
        "adjudication_only": True,
    }
    plan = {
        "schema_version": V199_PLAN_VERSION,
        "conflicts": conflicts,
        "frozen_reference_only_conflict_count": 4,
        "repair_placement_dispute_count": 3,
        "adjudication_case_count": 3,
        "adjudication_call_cap": 1,
        "fallback": "unresolved_repair_placement_abstains_its_original_case",
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "privacy": "private_opaque_ids_and_provenance_no_source_or_event_text",
    }
    return value, plan


def _decision_for_dispute(
    case: Mapping[str, Any], conflict: Mapping[str, Any]
) -> tuple[str, Optional[str]]:
    references = set(conflict["reference_witness_ids"])
    repair = str(conflict["repair_witness_ids"][0])
    group = next(
        ({str(value) for value in row} for row in case["equivalence_groups"] if repair in row),
        None,
    )
    if group is None:
        raise JudgeV5SelectionV199Error("adjudication omitted repair partition")
    matched = sorted(group & references)
    if len(matched) == 1:
        return "matched_reference", matched[0]
    if not matched:
        return "novel", None
    return "abstain", None


def reconcile_v199(
    *,
    source: Mapping[str, Any],
    plan: Mapping[str, Any],
    adjudication_output: Mapping[str, Any],
    adjudication_input: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    projected, projection_audit = v198.project_alignment_output_v198(
        adjudication_output, adjudication_input
    )
    adjudicated = {
        str(row["case_id"]): row
        for row in normalize_neutral_alignment_output(projected, adjudication_input)["cases"]
    }
    conflicts_by_case: dict[str, dict[int, Mapping[str, Any]]] = {}
    decisions = {}
    abstained_cases = set()
    for conflict in plan["conflicts"]:
        case_id = str(conflict["original_case_id"])
        conflicts_by_case.setdefault(case_id, {})[
            int(conflict["original_group_index"])
        ] = conflict
        dispute_case_id = conflict.get("dispute_case_id")
        if dispute_case_id is None:
            continue
        decision, reference = _decision_for_dispute(
            adjudicated[str(dispute_case_id)], conflict
        )
        decisions[str(dispute_case_id)] = {
            "decision": decision,
            "matched_reference_witness_id": reference,
        }
        if decision == "abstain":
            abstained_cases.add(case_id)
    if set(decisions) != {
        str(row["dispute_case_id"])
        for row in plan["conflicts"]
        if row.get("dispute_case_id") is not None
    }:
        raise JudgeV5SelectionV199Error("adjudication decision coverage drifted")

    reconciled_cases = []
    for original in source["alignment"]["cases"]:
        case_id = str(original["case_id"])
        conflicts = conflicts_by_case.get(case_id, {})
        if not conflicts:
            reconciled_cases.append(deepcopy(original))
            continue
        replacement_groups = []
        conflicted_ids = set()
        for group_index, group in enumerate(original["equivalence_groups"]):
            conflict = conflicts.get(group_index)
            if conflict is None:
                replacement_groups.append([str(value) for value in group])
                continue
            references = [str(value) for value in conflict["reference_witness_ids"]]
            repairs = [str(value) for value in conflict["repair_witness_ids"]]
            conflicted_ids.update(references)
            conflicted_ids.update(repairs)
            if not repairs:
                replacement_groups.extend([[value] for value in references])
                continue
            decision = decisions[str(conflict["dispute_case_id"])]
            matched = decision["matched_reference_witness_id"]
            repair = repairs[0]
            if decision["decision"] == "matched_reference":
                replacement_groups.append(sorted([repair, str(matched)]))
                replacement_groups.extend([[value] for value in references if value != matched])
            else:
                replacement_groups.extend([[value] for value in [*references, repair]])
        kept_pairs = [
            deepcopy(pair)
            for pair in original["alignment_pairs"]
            if not (set(map(str, pair["witness_ids"])) & conflicted_ids)
        ]
        for conflict in conflicts.values():
            dispute_case_id = conflict.get("dispute_case_id")
            if dispute_case_id is None:
                continue
            decision = decisions[str(dispute_case_id)]
            if decision["decision"] == "abstain":
                continue
            allowed = set(conflict["all_witness_ids"])
            for pair in adjudicated[str(dispute_case_id)]["alignment_pairs"]:
                ids = set(map(str, pair["witness_ids"]))
                if ids.issubset(allowed) and not set(conflict["reference_witness_ids"]).issubset(ids):
                    kept_pairs.append(deepcopy(pair))
        all_ids = {
            str(value)
            for group in replacement_groups
            for value in group
        }
        paired_ids = {
            str(value)
            for pair in kept_pairs
            for value in pair["witness_ids"]
        }
        row = {
            **deepcopy(original),
            "equivalence_groups": sorted(
                [sorted(set(group)) for group in replacement_groups],
                key=lambda group: tuple(group),
            ),
            "alignment_pairs": kept_pairs,
            "unpaired_witness_ids": sorted(all_ids - paired_ids),
            "status": "partial_abstain" if case_id in abstained_cases else "adjudicated",
        }
        reconciled_cases.append(row)
    reconciled_cases.sort(key=lambda row: str(row["case_id"]))
    conflict_audit = v198.build_reference_conflict_audit(
        reconciled_cases, source["mapping"]
    )
    if conflict_audit["frozen_reference_partition_conflict_count"] != 0:
        raise JudgeV5SelectionV199Error("v199 reconciliation retained reference conflicts")
    reconciliation = {
        "schema_version": V199_RECONCILIATION_VERSION,
        "cases": reconciled_cases,
        "adjudication_decisions": decisions,
        "abstained_original_case_ids": sorted(abstained_cases),
        "frozen_reference_partition_preserved": True,
        "majority_voting_used": False,
    }
    audit = {
        "schema_version": V199_AUDIT_VERSION,
        "observable_v198_conflict_count": 7,
        "frozen_reference_only_conflict_count": 4,
        "repair_placement_dispute_count": 3,
        "adjudication_call_count": 1,
        "adjudication_call_cap": 1,
        "decision_counts": {
            decision: sum(row["decision"] == decision for row in decisions.values())
            for decision in ("matched_reference", "novel", "abstain")
        },
        "abstained_original_case_count": len(abstained_cases),
        "post_reconciliation_reference_conflict_count": 0,
        "scoring_authorized": True,
        "projection_audit": projection_audit,
        "majority_voting_used": False,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "production_mutated": False,
    }
    return reconciliation, audit


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        MAXIMUM_TOTAL_TOKENS_PER_TURN * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": V199_CAPACITY_AUDIT_VERSION,
        "phase_id": V199_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v198_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "declared_turn_count": 1,
            "adjudication_case_count": 3,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V199_CAPACITY_POLICY_VERSION,
        "phase_id": V199_PHASE_ID,
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


def freeze_v199(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v199 terminal")}
    source = _validate_v198_conflict()
    value, plan = build_v199_input(source)
    prompt, _compact = v177.compact_alignment_prompt(value)
    schema = neutral_alignment_output_schema(value)
    prompt_bytes, schema_bytes = v177._request_bytes(prompt, schema)
    if prompt_bytes > MAXIMUM_PROMPT_BYTES or schema_bytes > MAXIMUM_SCHEMA_BYTES:
        raise JudgeV5SelectionV199Error("v199 request exceeds frozen byte cap")
    plan_path = root / "reference-conflict-adjudication-plan.private.json"
    _write_immutable(plan_path, plan)
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=value,
        prompt=prompt,
        schema=schema,
    )
    capacity = _build_capacity_policy(root, source)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V199_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V199_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_capped_side_free_alignment_pass_for_three_observable_repair_placement_conflicts",
        "turn_plan": [TURN_NAME],
        "adjudication_case_count": 3,
        "adjudication_call_cap": 1,
        "retry_count_per_turn": 0,
        "new_judge_prompt_or_rubric_created": False,
        "frozen_v130_alignment_instructions_reused": True,
        "frozen_v177_lossless_compact_prompt_reused": True,
        "majority_voting_used": False,
        "system_identity_present": False,
        "side_labels_present": False,
        "scoring_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "prompt_bytes": prompt_bytes,
        "schema_bytes": schema_bytes,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": source["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v198.__file__)),
            _record(Path(v177.__file__)),
            _record(Path(v130.__file__)),
            _record(Path(v157.__file__)),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_instructions": {
            "alignment_base_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            "plan": _record(plan_path),
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
        },
        "privacy": "private_source_event_prompt_output_plan_sanitized_counts_hashes_only",
    }
    spec_path = root / "reference-conflict-adjudication-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "source": source,
        "value": value,
        "plan": plan,
        "prompt": prompt,
        "schema": schema,
        "paths": paths,
        "capacity_policy": capacity["policy"],
        "spec": spec,
        "spec_path": spec_path,
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
            usage = _sum_usage(
                usage, _validate_usage(_load_json(Path(record["path"]), "v199 sidecar"))
            )
        except Exception:
            unknown += 1
    complete = unknown == 0
    cumulative = _sum_usage(
        predecessor["terminal"]["cumulative_known_usage_lower_bound"], usage
    )
    failure = {
        "schema_version": V199_FAILURE_VERSION,
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
        "cumulative_known_usage_lower_bound": cumulative,
        "cumulative_unknown_usage_turn_count": 1 + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": 120000
        + unknown * MAXIMUM_TOTAL_TOKENS_PER_TURN,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V199_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "scoring_authorized": False,
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


async def run_v199(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v199 terminal")
    frozen = freeze_v199(output_dir=root, timeout_seconds=timeout_seconds)
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            output, sidecar, _ = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=frozen["prompt"],
                schema=frozen["schema"],
                base_instructions=v130.alignment_instructions_v130(),
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=3,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda candidate: v198.validate_projectable_alignment_v198(
                    candidate, frozen["value"]
                ),
            )
        projected, projection_audit = v198.project_alignment_output_v198(
            output, frozen["value"]
        )
        reconciliation, audit = reconcile_v199(
            source=frozen["source"],
            plan=frozen["plan"],
            adjudication_output=output,
            adjudication_input=frozen["value"],
        )
        paths = {
            "projected": root / "reference-conflict-adjudication-projected.private.json",
            "projection_audit": root / "reference-conflict-projection-audit.json",
            "reconciliation": root / "residual-repair-alignment-reconciled.private.json",
            "audit": root / "reference-conflict-reconciliation-audit.json",
        }
        _write_immutable(paths["projected"], projected)
        _write_immutable(paths["projection_audit"], projection_audit)
        _write_immutable(paths["reconciliation"], reconciliation)
        _write_immutable(paths["audit"], audit)
        accounting = _aggregate_usage([sidecar])
        cumulative = _sum_usage(
            frozen["source"]["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        terminal = {
            "schema_version": V199_TERMINAL_VERSION,
            "state": "completed",
            "terminal_at": now_iso(),
            "terminal_reason": "v199_reference_conflicts_reconciled_scoring_authorized",
            "terminal_classification": "active_development_recovery_required",
            "overall_evaluation_complete": False,
            "observable_v198_conflict_count": 7,
            "frozen_reference_only_conflict_count": 4,
            "repair_placement_dispute_count": 3,
            "adjudication_call_count": 1,
            "adjudication_call_cap": 1,
            "adjudication_decision_counts": audit["decision_counts"],
            "abstained_original_case_count": audit["abstained_original_case_count"],
            "frozen_reference_partition_preserved": True,
            "scoring_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "majority_voting_used": False,
            "reconciliation": _record(paths["reconciliation"]),
            "reconciliation_audit": _record(paths["audit"]),
            "projection": _record(paths["projected"]),
            "projection_audit": _record(paths["projection_audit"]),
            "cumulative_usage_status": "unknown",
            "cumulative_known_usage_lower_bound": cumulative,
            "cumulative_unknown_usage_turn_count": 1,
            "cumulative_conservative_unknown_usage_upper_bound": 120000,
            "required_next_artifact_path": str(
                root.parent
                / "development-selection-v5_4-v200-residual-repair-score"
                / "terminal.json"
            ),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(
            root, frozen["source"], exc.turn_name, exc.error_class
        )
    except Exception as exc:
        return _write_failure(root, frozen["source"], TURN_NAME, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v199 reference-conflict adjudication")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v199(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "scoring_authorized": terminal.get("scoring_authorized", False),
                "usage_status": terminal.get("usage_status"),
                "holdout_authorized": terminal.get("holdout_authorized", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
