from __future__ import annotations

"""Align v197-supported repair events to the frozen shared reference."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_selection_v177_alignment_scale_diagnostic as v177
from . import app_server_judge_v5_selection_v179_primary_alignment_phase1 as v179
from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v193_residual_repair_diagnostic as v193
from . import app_server_judge_v5_selection_v197_support_evidence_repair as v197
from .app_server_judge_v5 import (
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
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
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .app_server_llm_judge import validate_shared_witness_pool
from .util import now_iso, sha256_text


V198_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v198_spec_v1"
V198_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v198_terminal_v1"
V198_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v198_failure_v1"
V198_AUDIT_VERSION = "pif_app_server_residual_alignment_audit_v1"
V198_PROJECTION_VERSION = "pif_app_server_residual_alignment_projection_v1"
V198_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V198_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V198_PHASE_ID = "judge_v5_4_selection_v198_residual_repair_alignment"

MODEL = v177.MODEL
EFFORT = v177.EFFORT
TURN_COUNT = 4
TURN_NAMES = tuple(f"selection_residual_alignment_{index:02d}" for index in range(TURN_COUNT))
MAXIMUM_TOTAL_TOKENS_PER_TURN = 100_000
MAXIMUM_PROMPT_BYTES = 90_000
MAXIMUM_SCHEMA_BYTES = 20_000
TIMEOUT_SECONDS = v197.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v197.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v198-residual-repair-alignment"
).resolve()


class JudgeV5SelectionV198Error(RuntimeError):
    """The residual alignment cannot be frozen or projected safely."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v197_success() -> dict[str, Any]:
    root = v197.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "spec": root / "support-evidence-repair-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "output": root / "support-output-merged.private.json",
        "receipts": root / "support-receipts.private.json",
        "patch": root / "support-evidence-repair-patch.private.json",
        "diagnostics": root / "support-repair-diagnostics.json",
    }
    values = {name: _load_json(path, f"v197 {name}") for name, path in paths.items()}
    terminal, spec, diagnostics = (
        values["terminal"],
        values["spec"],
        values["diagnostics"],
    )
    expected_usage = {
        "input_tokens": 24472,
        "cached_input_tokens": 0,
        "output_tokens": 1196,
        "reasoning_output_tokens": 502,
        "total_tokens": 25668,
    }
    expected_cumulative = {
        "input_tokens": 7188625,
        "cached_input_tokens": 821248,
        "output_tokens": 1235696,
        "reasoning_output_tokens": 379736,
        "total_tokens": 8424321,
    }
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v197_support_evidence_repair_completed_alignment_authorized"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != expected_usage
        or terminal.get("turn_count") != 1
        or terminal.get("cumulative_known_usage_lower_bound") != expected_cumulative
        or terminal.get("cumulative_unknown_usage_turn_count") != 1
        or terminal.get("cumulative_conservative_unknown_usage_upper_bound") != 120000
        or terminal.get("v196_turn_replayed") is not False
        or terminal.get("v196_failure_preserved") is not True
        or terminal.get("v196_valid_support_decision_count_preserved") != 24
        or terminal.get("v197_fresh_support_decision_count") != 6
        or terminal.get("support_receipts_frozen") is not True
        or terminal.get("support_status_counts")
        != {"supported": 30, "unsupported": 0, "abstain": 0}
        or terminal.get("alignment_authorized") is not True
        or terminal.get("development_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("support_output") != _record(paths["output"])
        or terminal.get("support_receipts") != _record(paths["receipts"])
        or terminal.get("support_repair_patch") != _record(paths["patch"])
        or terminal.get("support_repair_diagnostics") != _record(paths["diagnostics"])
        or spec.get("turn_plan") != [v197.TURN_NAME]
        or spec.get("retry_count_per_turn") != 0
        or spec.get("v196_turn_replayed") is not False
        or spec.get("v196_failure_preserved") is not True
        or spec.get("alignment_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or diagnostics.get("merged_validator_error_count") != 0
        or diagnostics.get("all_projected_evidence_exact") is not True
        or diagnostics.get("semantic_similarity_used") is not False
        or diagnostics.get("semantic_regex_or_keyword_rules_used") is not False
    ):
        raise JudgeV5SelectionV198Error("v197 success contract drifted")
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5SelectionV198Error("v197 artifact disappeared")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV198Error("v197 runtime binding drifted")
    for key in ("input", "prompt", "schema"):
        if not _verify_record(spec["frozen_inputs"][key]):
            raise JudgeV5SelectionV198Error(f"v197 frozen {key} drifted")
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    if len(attempts) != 1:
        raise JudgeV5SelectionV198Error("v197 attempt coverage drifted")
    attempt = attempts[0]
    for key in ("capacity", "sidecar", "output"):
        if not isinstance(attempt.get(key), Mapping) or not _verify_record(attempt[key]):
            raise JudgeV5SelectionV198Error(f"v197 {key} record drifted")
    if _validate_usage(_load_json(Path(attempt["sidecar"]["path"]), "v197 sidecar")) != expected_usage:
        raise JudgeV5SelectionV198Error("v197 measured usage drifted")
    predecessor = v197._validate_v196_failed_attempt()
    if v155._support_receipts(values["output"]) != values["receipts"]:
        raise JudgeV5SelectionV198Error("v197 support receipts drifted")
    if v197.v143.validate_support_output(values["output"], predecessor["value"]):
        raise JudgeV5SelectionV198Error("v197 merged support output drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempt": attempt,
        "terminal": terminal,
        "spec": spec,
        "output": values["output"],
        "receipts": values["receipts"],
        "predecessor": predecessor,
    }


def _event_lookup(case: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = [*(case.get("event_set_a") or []), *(case.get("event_set_b") or [])]
    result = {str(row["witness_id"]): deepcopy(row) for row in rows}
    if len(result) != len(rows):
        raise JudgeV5SelectionV198Error("witness event lookup contains duplicates")
    return result


def _load_alignment_sources() -> dict[str, Any]:
    repair = _validate_v197_success()
    authorization = v193._validate_v192_authorization()
    v190 = authorization["predecessor"]["predecessor"]
    consensus_record = v190["spec"]["frozen_inputs"]["consensus"]
    if not _verify_record(consensus_record):
        raise JudgeV5SelectionV198Error("v190 consensus record drifted")
    consensus = _load_json(Path(consensus_record["path"]), "v190 consensus")
    selection = v186._selection_sources()
    support = v179._validate_v178_success()
    augmented, base_score, _combinations = v192._all_composite_score()
    selected_cases = authorization["private_input"]["cases"]
    if (
        len(selected_cases) != 5
        or sum(row["density_stratum"] == "no_signal" for row in selected_cases) != 1
        or {str(row["case_id"]) for row in selected_cases}
        != {str(row["case_id"]) for row in consensus["cases"]}
        & {str(row["case_id"]) for row in selected_cases}
    ):
        raise JudgeV5SelectionV198Error("diagnostic case/reference coverage drifted")
    return {
        "repair": repair,
        "authorization": authorization,
        "v190": v190,
        "consensus": consensus,
        "consensus_record": consensus_record,
        "selection": selection,
        "support": support,
        "augmented": augmented,
        "base_score": base_score,
        "selected_cases": selected_cases,
    }


def build_augmented_alignment_pool(
    sources: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    old_pool = {str(row["case_id"]): row for row in sources["selection"]["pool"]["cases"]}
    consensus = {str(row["case_id"]): row for row in sources["consensus"]["cases"]}
    old_receipts = {
        str(row["witness_id"]): row for row in sources["support"]["receipts"]["units"]
    }
    repair_predecessor = sources["repair"]["predecessor"]
    repair_pool = {
        str(row["case_id"]): row for row in repair_predecessor["values"]["pool"]["cases"]
    }
    repair_mapping = {
        str(row["case_id"]): row for row in repair_predecessor["values"]["mapping"]["cases"]
    }
    repair_receipts = {
        str(row["witness_id"]): row for row in sources["repair"]["receipts"]["units"]
    }
    repair_case_by_original = {}
    for repair_case_id, row in repair_mapping.items():
        original_case_id = str(row["case_provenance"]["original_case_id"])
        if original_case_id in repair_case_by_original:
            raise JudgeV5SelectionV198Error("repair-to-original case mapping duplicates")
        repair_case_by_original[original_case_id] = repair_case_id

    source_by_case = {
        str(row["case_id"]): str(row["source_id"])
        for row in sources["selected_cases"]
    }
    pool_cases, mapping_cases, receipt_rows = [], [], []
    reference_count = repair_count = 0
    for original_case_id in sorted(source_by_case):
        repair_case_id = repair_case_by_original[original_case_id]
        repair_case = repair_pool[repair_case_id]
        repair_events = _event_lookup(repair_case)
        if not repair_events:
            continue
        original_case = old_pool[original_case_id]
        if repair_case["source_excerpt"] != original_case["source_excerpt"]:
            raise JudgeV5SelectionV198Error("repair/reference source text drifted")
        consensus_case = consensus[original_case_id]
        support_status = {
            str(row["witness_id"]): str(row["verdict"])
            for row in consensus_case["support_results"]
        }
        reference_groups = []
        for group_index, group in enumerate(consensus_case["equivalence_groups"]):
            supported = sorted(
                str(witness_id)
                for witness_id in group
                if support_status.get(str(witness_id)) == "supported"
            )
            if not supported:
                continue
            reference_groups.append(
                {
                    "group_index": group_index,
                    "representative_witness_id": supported[0],
                    "full_group_witness_ids": [str(value) for value in group],
                }
            )
        original_events = _event_lookup(original_case)
        representatives = [
            deepcopy(original_events[row["representative_witness_id"]])
            for row in reference_groups
        ]
        if any(
            old_receipts[row["representative_witness_id"]]["proposition_verdict"]
            != "supported"
            for row in reference_groups
        ):
            raise JudgeV5SelectionV198Error("reference representative support drifted")
        for row in reference_groups:
            receipt_rows.append(deepcopy(old_receipts[row["representative_witness_id"]]))
        repair_witnesses = sorted(repair_events)
        for witness_id in repair_witnesses:
            receipt = deepcopy(repair_receipts[witness_id])
            if receipt["proposition_verdict"] != "supported":
                raise JudgeV5SelectionV198Error("repair support-positive contract drifted")
            receipt["case_id"] = original_case_id
            receipt_rows.append(receipt)
        pool_cases.append(
            {
                "case_id": original_case_id,
                "source_excerpt": original_case["source_excerpt"],
                "event_set_a": representatives,
                "event_set_b": [deepcopy(repair_events[value]) for value in repair_witnesses],
            }
        )
        mapping_cases.append(
            {
                "case_id": original_case_id,
                "source_id": source_by_case[original_case_id],
                "repair_case_id": repair_case_id,
                "reference_groups": reference_groups,
                "repair_witness_ids": repair_witnesses,
            }
        )
        reference_count += len(reference_groups)
        repair_count += len(repair_witnesses)
    pool = {
        "schema_version": sources["selection"]["pool"]["schema_version"],
        "seed_sha256": sha256_text("pif-v198-augmented-reference-pool-v1"),
        "cases": pool_cases,
        "privacy": "private_analysis_only_blinded_no_origin_provenance",
    }
    errors = validate_shared_witness_pool(pool)
    if errors:
        raise JudgeV5SelectionV198Error("v198 augmented pool is invalid")
    receipts = {
        "schema_version": sources["repair"]["receipts"]["schema_version"],
        "units": sorted(
            receipt_rows,
            key=lambda row: (str(row["case_id"]), str(row["witness_id"])),
        ),
        "side_free": True,
        "claim_support_and_field_correctness_separate": True,
    }
    private_mapping = {
        "schema_version": "pif_app_server_residual_alignment_mapping_v1",
        "cases": mapping_cases,
        "reference_representative_count": reference_count,
        "repair_witness_count": repair_count,
        "no_signal_case_excluded_because_both_reference_and_repair_are_empty": True,
        "privacy": "opaque_ids_and_provenance_no_source_or_event_text",
    }
    if (
        len(pool_cases) != 4
        or reference_count != 100
        or repair_count != 30
        or len(receipts["units"]) != 130
        or len({row["witness_id"] for row in receipts["units"]}) != 130
    ):
        raise JudgeV5SelectionV198Error("v198 augmented witness coverage drifted")
    return pool, receipts, private_mapping


def build_alignment_turns(
    pool: Mapping[str, Any],
    receipts: Mapping[str, Any],
    private_mapping: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_by_case = {
        str(row["case_id"]): str(row["source_id"])
        for row in private_mapping["cases"]
    }
    candidates = []
    for case in pool["cases"]:
        case_id = str(case["case_id"])
        value = v177.build_supported_alignment_input(
            pool, receipts, case_id=case_id, permutation="base"
        )
        prompt, compact = v177.compact_alignment_prompt(value)
        schema = neutral_alignment_output_schema(value)
        prompt_bytes, schema_bytes = v177._request_bytes(prompt, schema)
        if prompt_bytes > MAXIMUM_PROMPT_BYTES or schema_bytes > MAXIMUM_SCHEMA_BYTES:
            raise JudgeV5SelectionV198Error("v198 alignment request exceeds frozen byte cap")
        candidates.append(
            {
                "case_id": case_id,
                "source_id": source_by_case[case_id],
                "value": value,
                "prompt": prompt,
                "compact": compact,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
                "witness_count": len(value["cases"][0]["witnesses"]),
            }
        )
    candidates.sort(key=lambda row: (-row["prompt_bytes"], row["case_id"]))
    if len(candidates) != TURN_COUNT:
        raise JudgeV5SelectionV198Error("v198 alignment turn count drifted")
    return [
        {**row, "turn_name": turn_name}
        for turn_name, row in zip(TURN_NAMES, candidates, strict=True)
    ]


def project_alignment_output_v198(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    projected = deepcopy(output)
    expected = {
        str(case["case_id"]): case for case in alignment_input.get("cases") or []
    }
    additions = []
    try:
        for case_index, row in enumerate(projected["cases"]):
            case_id = str(row["case_id"])
            all_ids = {
                str(item["witness_id"])
                for item in expected[case_id].get("witnesses") or []
            }
            paired = set()
            for pair in row["alignment_pairs"]:
                first = str(pair["witness_id_1"])
                second = str(pair["witness_id_2"])
                if first == second or first not in all_ids or second not in all_ids:
                    raise JudgeV5SelectionV198Error("v198 alignment pair identity drifted")
                paired.update((first, second))
            unpaired = [str(value) for value in row["unpaired_witness_ids"]]
            if len(unpaired) != len(set(unpaired)) or set(unpaired) & paired:
                raise JudgeV5SelectionV198Error("v198 unpaired identity drifted")
            missing = sorted(all_ids - paired - set(unpaired))
            row["unpaired_witness_ids"] = [*unpaired, *missing]
            additions.append(
                {
                    "case_index": case_index,
                    "added_unpaired_witness_count": len(missing),
                }
            )
    except JudgeV5SelectionV198Error:
        raise
    except Exception as exc:
        raise JudgeV5SelectionV198Error("v198 structural projection input is malformed") from exc
    exact, exact_audit = v157.project_exact_spans_and_relation(projected, alignment_input)
    audit = {
        **exact_audit,
        "schema_version": V198_PROJECTION_VERSION,
        "structural_unpaired_addition_count": sum(
            row["added_unpaired_witness_count"] for row in additions
        ),
        "structural_unpaired_additions": additions,
        "semantic_equivalence_groups_changed": False,
        "semantic_alignment_pairs_changed": False,
        "semantic_checklist_decisions_changed": False,
        "rationales_changed": False,
        "deterministic_operations": [
            "complete_pair_or_unpaired_identity_partition_by_exact_set_difference",
            *exact_audit["deterministic_operations"],
        ],
    }
    return exact, audit


def validate_projectable_alignment_v198(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    try:
        project_alignment_output_v198(output, alignment_input)
    except Exception:
        return validate_neutral_alignment_output(output, alignment_input) or [
            "v198_structural_projection_failed"
        ]
    return []


def build_reference_conflict_audit(
    normalized_cases: Sequence[Mapping[str, Any]],
    private_mapping: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = {str(row["case_id"]): row for row in normalized_cases}
    conflict_rows = []
    repair_match_count = repair_novel_group_count = 0
    repair_relation_counts: Counter[str] = Counter()
    total_repairs = 0
    for mapping_case in private_mapping["cases"]:
        case_id = str(mapping_case["case_id"])
        row = normalized[case_id]
        references = {
            str(item["representative_witness_id"])
            for item in mapping_case["reference_groups"]
        }
        repairs = {str(value) for value in mapping_case["repair_witness_ids"]}
        total_repairs += len(repairs)
        grouped_repairs = set()
        for group_index, group in enumerate(row["equivalence_groups"]):
            group_set = {str(value) for value in group}
            group_references = group_set & references
            group_repairs = group_set & repairs
            grouped_repairs.update(group_repairs)
            if len(group_references) > 1:
                conflict_rows.append(
                    {
                        "case_id": case_id,
                        "group_index": group_index,
                        "frozen_reference_representative_count": len(group_references),
                    }
                )
            if group_repairs and group_references:
                repair_match_count += len(group_repairs)
            elif group_repairs:
                repair_novel_group_count += 1
        if grouped_repairs != repairs:
            raise JudgeV5SelectionV198Error("repair witness group coverage drifted")
        for pair in row["alignment_pairs"]:
            if repairs & {str(value) for value in pair["witness_ids"]}:
                repair_relation_counts[str(pair["relation"])] += 1
    return {
        "schema_version": V198_AUDIT_VERSION,
        "aligned_case_count": len(normalized),
        "frozen_reference_representative_count": private_mapping[
            "reference_representative_count"
        ],
        "repair_witness_count": total_repairs,
        "repair_witnesses_grouped_with_existing_reference": repair_match_count,
        "repair_novel_equivalence_group_count": repair_novel_group_count,
        "repair_alignment_relation_counts": dict(sorted(repair_relation_counts.items())),
        "frozen_reference_partition_conflict_count": len(conflict_rows),
        "frozen_reference_partition_conflicts": conflict_rows,
        "scoring_safe": not conflict_rows,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "production_mutated": False,
    }


def _build_capacity_policy(
    root: Path, predecessor: Mapping[str, Any], turns: Sequence[Mapping[str, Any]]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(turns) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V198_CAPACITY_AUDIT_VERSION,
        "phase_id": V198_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v197_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "v197_supported_repair_witness_count": 30,
            "frozen_reference_representative_count": 100,
            "alignment_case_count": 4,
            "no_signal_empty_case_count": 1,
            "declared_turn_count": len(turns),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
            "maximum_prompt_bytes": max(int(row["prompt_bytes"]) for row in turns),
            "maximum_schema_bytes": max(int(row["schema_bytes"]) for row in turns),
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V198_CAPACITY_POLICY_VERSION,
        "phase_id": V198_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [str(row["turn_name"]) for row in turns],
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


def freeze_v198(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {
            "root": root,
            "terminal": _load_json(root / "terminal.json", "v198 terminal"),
        }
    sources = _load_alignment_sources()
    pool, receipts, private_mapping = build_augmented_alignment_pool(sources)
    turns = build_alignment_turns(pool, receipts, private_mapping)
    pool_path = root / "augmented-alignment-pool.private.json"
    receipts_path = root / "augmented-support-receipts.private.json"
    mapping_path = root / "augmented-alignment-mapping.private.json"
    _write_immutable(pool_path, pool)
    _write_immutable(receipts_path, receipts)
    _write_immutable(mapping_path, private_mapping)
    frozen_turns = []
    for turn in turns:
        paths = _freeze_turn_request(
            root=root,
            turn_name=turn["turn_name"],
            input_value=turn["value"],
            prompt=turn["prompt"],
            schema=turn["schema"],
        )
        frozen_turns.append({**turn, "paths": paths})
    capacity = _build_capacity_policy(root, sources["repair"], turns)
    instructions = v130.alignment_instructions_v130()
    spec = {
        "schema_version": V198_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V198_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "one_origin_neutral_compact_alignment_turn_per_dense_case_against_one_representative_per_frozen_reference_group",
        "turn_plan": [row["turn_name"] for row in turns],
        "alignment_case_count": 4,
        "no_signal_empty_case_count": 1,
        "frozen_reference_representative_count": 100,
        "supported_repair_witness_count": 30,
        "retry_count_per_turn": 0,
        "new_judge_prompt_or_rubric_created": False,
        "frozen_v130_alignment_instructions_reused": True,
        "frozen_v177_lossless_compact_prompt_reused": True,
        "system_identity_present": False,
        "side_labels_present": False,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "deterministic_normalization_allowed": [
            "complete_exact_pair_or_unpaired_id_coverage",
            "drop_nonexact_source_spans",
            "project_relation_from_frozen_llm_checklist",
            "ordering_schema_provenance_and_accounting",
        ],
        "scoring_authorized": False,
        "permutation_canary_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": sources["repair"]["records"],
        "frozen_reference_consensus": sources["consensus_record"],
        "v192_authorization": sources["authorization"]["records"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v197.__file__)),
            _record(Path(v177.__file__)),
            _record(Path(v130.__file__)),
            _record(Path(v157.__file__)),
            _record(Path(v192.__file__)),
        ],
        "frozen_instructions": {
            "alignment_base_sha256": sha256_text(instructions),
        },
        "frozen_inputs": {
            "pool": _record(pool_path),
            "support_receipts": _record(receipts_path),
            "private_mapping": _record(mapping_path),
            "turns": [
                {
                    "turn_name": row["turn_name"],
                    "case_id": row["case_id"],
                    "source_id": row["source_id"],
                    "witness_count": row["witness_count"],
                    "prompt_bytes": row["prompt_bytes"],
                    "schema_bytes": row["schema_bytes"],
                    "input": _record(row["paths"]["input"]),
                    "prompt": _record(row["paths"]["prompt"]),
                    "schema": _record(row["paths"]["schema"]),
                }
                for row in frozen_turns
            ],
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "residual-repair-alignment-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "sources": sources,
        "pool": pool,
        "receipts": receipts,
        "private_mapping": private_mapping,
        "turns": frozen_turns,
        "instructions": instructions,
    }


def _write_failure(
    root: Path,
    predecessor: Mapping[str, Any],
    turn_name: Optional[str],
    error_class: str,
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
                _load_json(Path(record["path"]), "v198 sidecar")
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
        "schema_version": V198_FAILURE_VERSION,
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
        "schema_version": V198_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "alignment_completed": False,
        "scoring_authorized": False,
        "permutation_canary_authorized": False,
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


async def run_v198(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v198 terminal")
    frozen = freeze_v198(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars, normalized_cases, projection_records = [], [], []
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = str(turn["turn_name"])
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=frozen["instructions"],
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=int(turn["witness_count"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: (
                        validate_projectable_alignment_v198(candidate, item)
                    ),
                )
                projected, audit = project_alignment_output_v198(output, turn["value"])
                turn_root = Path(turn["paths"]["output"]).parent
                projected_path = turn_root / "alignment-projected.private.json"
                audit_path = turn_root / "structural-projection-audit.json"
                _write_immutable(projected_path, projected)
                _write_immutable(audit_path, audit)
                normalized_cases.extend(
                    normalize_neutral_alignment_output(projected, turn["value"])["cases"]
                )
                sidecars.append(sidecar)
                projection_records.append(
                    {
                        "turn_name": current_turn,
                        "projected": _record(projected_path),
                        "audit": _record(audit_path),
                    }
                )
        normalized_cases.sort(key=lambda row: str(row["case_id"]))
        normalized = {
            "schema_version": "pif_app_server_residual_alignment_normalized_v1",
            "cases": normalized_cases,
            "origin_neutral": True,
            "mismatch_fields_projected_from_checklists": True,
        }
        conflict_audit = build_reference_conflict_audit(
            normalized_cases, frozen["private_mapping"]
        )
        normalized_path = root / "residual-repair-alignment.private.json"
        audit_path = root / "residual-repair-alignment-audit.json"
        _write_immutable(normalized_path, normalized)
        _write_immutable(audit_path, conflict_audit)
        accounting = _aggregate_usage(sidecars)
        cumulative = _sum_usage(
            frozen["sources"]["repair"]["terminal"][
                "cumulative_known_usage_lower_bound"
            ],
            accounting["usage"],
        )
        safe = bool(conflict_audit["scoring_safe"])
        terminal = {
            "schema_version": V198_TERMINAL_VERSION,
            "state": "completed" if safe else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v198_residual_repair_alignment_completed_scoring_authorized"
                if safe
                else "v198_frozen_reference_partition_conflict_requires_capped_adjudication"
            ),
            "terminal_classification": (
                "active_development_recovery_required"
                if safe
                else "inactive_incomplete_recovery_required"
            ),
            "overall_evaluation_complete": False,
            "alignment_completed": True,
            "alignment_output": _record(normalized_path),
            "alignment_audit": _record(audit_path),
            "projection_records": projection_records,
            "aligned_case_count": 4,
            "no_signal_empty_case_count": 1,
            "frozen_reference_partition_conflict_count": conflict_audit[
                "frozen_reference_partition_conflict_count"
            ],
            "scoring_authorized": safe,
            "permutation_canary_authorized": False,
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
                / (
                    "development-selection-v5_4-v199-residual-repair-score"
                    if safe
                    else "development-selection-v5_4-v199-reference-conflict-adjudication"
                )
                / "terminal.json"
            ),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(
            root, frozen["sources"]["repair"], exc.turn_name, exc.error_class
        )
    except Exception as exc:
        return _write_failure(
            root, frozen["sources"]["repair"], current_turn, type(exc).__name__
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v198 residual-repair alignment")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v198(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "alignment_completed": terminal.get("alignment_completed", False),
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
