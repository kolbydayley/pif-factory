from __future__ import annotations

"""Fresh full development calibration for the corrected v166 judge protocol."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_calibration_v145_comprehensive_field_reference_owner as v145
from . import app_server_judge_v5_calibration_v146_singleton_reference_owner as v146
from . import app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic as v149
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_calibration_v163_fresh_corrected_field_diagnostic as v163
from . import app_server_judge_v5_calibration_v166_capped_owner_reconciliation as v166
from .app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    build_neutral_alignment_input,
    build_pointwise_support_input,
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
    validate_neutral_alignment_output,
)
from .app_server_judge_v5_calibration import calibration_case_shards, pointwise_input_subset
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
from .app_server_judge_v5_calibration_v78_fresh_reconcile_diagnostic import (
    output_schema as field_output_schema,
    validate_output as validate_field_output,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import _record, _verify_record
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .util import now_iso, sha256_text


V167_SPEC_VERSION = "pif_app_server_judge_v5_4_v167_spec_v1"
V167_SCORE_VERSION = "pif_app_server_judge_v5_4_v167_score_v1"
V167_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v167_development_protocol_v1"
V167_FAILURE_VERSION = "pif_app_server_judge_v5_4_v167_failure_v1"
V167_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v167_terminal_v1"
V167_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V167_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V167_PHASE_ID = "judge_v5_4_v167_fresh_full_replacement"

SUPPORT_MODEL = "gpt-5.6-sol"
FIELD_MODEL = "gpt-5.5"
ALIGNMENT_MODEL = "gpt-5.5"
VERIFIER_MODEL = "gpt-5.6-sol"
EFFORT = "high"

SUPPORT_PRIMARY_TURNS = tuple(f"fresh_support_shard_{index:02d}" for index in range(10))
SUPPORT_CANARY_TURN = "fresh_support_canary"
FIELD_PRIMARY_TURNS = tuple(f"fresh_field_singleton_{index:02d}" for index in range(28))
FIELD_REPEAT_TURN = "fresh_field_repeat_batch"
ALIGNMENT_PRIMARY_TURNS = tuple(f"fresh_alignment_shard_{index:02d}" for index in range(11))
ALIGNMENT_CANARY_TURN = "fresh_alignment_canary"
VERIFIER_PRIMARY_TURNS = tuple(f"fresh_verifier_shard_{index:02d}" for index in range(12))
VERIFIER_CANARY_TURNS = tuple(f"fresh_verifier_canary_{index:02d}" for index in range(3))
TURN_NAMES = (
    SUPPORT_PRIMARY_TURNS
    + (SUPPORT_CANARY_TURN,)
    + FIELD_PRIMARY_TURNS
    + (FIELD_REPEAT_TURN,)
    + ALIGNMENT_PRIMARY_TURNS
    + (ALIGNMENT_CANARY_TURN,)
    + VERIFIER_PRIMARY_TURNS
    + VERIFIER_CANARY_TURNS
)
MAXIMUM_TOTAL_TOKENS_PER_TURN = 40_000
TIMEOUT_SECONDS = v166.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v166.DEFAULT_OUTPUT_ROOT.parent / "judge-calibration-v5_4-v167-fresh-full-replacement"
).resolve()


class JudgeV5CalibrationV167Error(RuntimeError):
    """The immutable v167 full-calibration contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v166() -> dict[str, Any]:
    root = v166.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "score": root / "capped-owner-reconciliation-score.json",
        "spec": root / "capped-owner-reconciliation-spec.json",
        "reference": root / "fixture-reference-v15-v165.private.json",
        "truth": root / "full-calibration-truth-v165.private.json",
        "protocol": root / "alignment-protocol-v166.json",
        "audit": root / "capped-reconciliation-audit.json",
    }
    values = {name: _load_json(path, f"v166 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    attempts = [
        row for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    sidecars = []
    for row in attempts:
        record = row.get("sidecar")
        if not isinstance(record, Mapping):
            raise JudgeV5CalibrationV167Error("v166 measured sidecar coverage drifted")
        _verify_record(record)
        _validate_usage(_load_json(Path(record["path"]), "v166 sidecar"))
        sidecars.append(record)
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason") != "v166_reference_reconciled_full_calibration_authorized"
        or terminal.get("reference_frozen") is not True
        or terminal.get("fresh_full_replacement_calibration_authorized") is not True
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 30423
        or terminal.get("cumulative_calibration_usage", {}).get("total_tokens") != 3216474
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or score.get("metrics", {}).get("adjudicated_control_exact_count") != 2
        or spec.get("maximum_turn_count") != 1
        or spec.get("retry_count_per_turn") != 0
        or len(attempts) != 1
        or len(sidecars) != 1
        or values["reference"].get("case_count") != 66
        or values["reference"].get("witness_count") != 182
        or values["reference"].get("reference_version") != "v15_v165"
        or values["truth"].get("case_count") != 66
        or values["truth"].get("witness_count") != 182
        or values["protocol"].get("fresh_full_replacement_calibration_authorized") is not True
    ):
        raise JudgeV5CalibrationV167Error("v166 authorization terminal drifted")
    for record in spec["runtime_files"]:
        _verify_record(record)
    field_source = v163._validate_v162()
    field_protocol_path = v163.DEFAULT_OUTPUT_ROOT / "field-protocol-v163.json"
    field_protocol = _load_json(field_protocol_path, "v163 field protocol")
    if (
        field_protocol.get("field_model") != "gpt-5.5"
        or field_protocol.get("support_model") != "gpt-5.6-sol"
        or field_protocol.get("unsupported_inference_owner")
        != "llm_pointwise_support_projection"
    ):
        raise JudgeV5CalibrationV167Error("v163 field protocol drifted")
    legacy = v155._validate_v154()
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "sidecars": sidecars,
        "field_source": field_source,
        "field_protocol": field_protocol,
        "field_protocol_record": _record(field_protocol_path),
        "legacy": legacy,
        "cumulative_usage": terminal["cumulative_calibration_usage"],
    }


def _balanced_shards(values: Sequence[str], count: int) -> list[list[str]]:
    if len(values) < count:
        raise JudgeV5CalibrationV167Error("fixed shard count exceeds item count")
    quotient, remainder = divmod(len(values), count)
    shards, offset = [], 0
    for index in range(count):
        size = quotient + int(index < remainder)
        shards.append(list(values[offset : offset + size]))
        offset += size
    if offset != len(values) or any(not shard for shard in shards):
        raise JudgeV5CalibrationV167Error("balanced shard coverage drifted")
    return shards


def _field_repeat_value(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    tasks = [deepcopy(row["value"]["tasks"][0]) for row in rows]
    value = deepcopy(rows[0]["value"])
    value["task_count"] = len(tasks)
    value["tasks"] = tasks
    value["singleton_context"] = False
    value["repeat_batch_only"] = True
    value["tasks_are_independent"] = True
    return value


def build_v167_inputs(source: Mapping[str, Any]) -> dict[str, Any]:
    pool = source["legacy"]["v153"]["v106"]["pool"]
    reference = source["values"]["reference"]
    frozen_truth = source["values"]["truth"]
    pointwise = build_pointwise_support_input(pool)
    case_ids = sorted(
        (str(row["case_id"]) for row in pool["cases"]),
        key=lambda value: sha256_text(f"v167|support-case|{value}"),
    )
    support_case_shards = _balanced_shards(case_ids, len(SUPPORT_PRIMARY_TURNS))
    support_turns = []
    for turn_name, shard in zip(SUPPORT_PRIMARY_TURNS, support_case_shards, strict=True):
        subset = pointwise_input_subset(pointwise, shard)
        support_turns.append(
            {"turn_name": turn_name, "turn_role": "support_primary", "value": v155._support_value(subset["units"])}
        )
    support_canary_units = v155._select_support_canary(pointwise, reference)
    support_turns.append(
        {
            "turn_name": SUPPORT_CANARY_TURN,
            "turn_role": "support_canary",
            "value": v155._support_value(list(reversed(support_canary_units))),
        }
    )
    witness_units = v155._witness_units(pool)
    field_primary = []
    for row in frozen_truth["field_tasks"]:
        field = row["field"]
        case = reference["cases"][row["case_id"]]
        expected_status = (
            ("correct" if case["proposition"][row["witness_id"]] == "supported" else "incorrect")
            if field == "unsupported_inference"
            else ("incorrect" if field in case["field_issues"][row["witness_id"]] else "correct")
        )
        if expected_status != row["expected_status"]:
            raise JudgeV5CalibrationV167Error("frozen field truth no longer matches corrected reference")
        if field == "unsupported_inference":
            continue
        unit = witness_units[row["witness_id"]]
        event = compact_empty_event_fields(deepcopy(unit["structured_event"]))
        task = {
            "task_id": row["task_id"],
            "field": field,
            "field_contract": {"field": field, **deepcopy(v145.FIELD_RULES_V145[field])},
            "requested_field_value": v155._general_requested_field_value(field, event),
            "source_excerpt": unit["source_excerpt"],
            "structured_event": event,
        }
        field_primary.append(
            {
                "turn_role": "field_singleton",
                "task_id": row["task_id"],
                "field": field,
                "value": v149._field_value(task),
            }
        )
    if len(field_primary) != 28:
        raise JudgeV5CalibrationV167Error("frozen field model task coverage drifted")
    for turn_name, row in zip(FIELD_PRIMARY_TURNS, field_primary, strict=True):
        row["turn_name"] = turn_name
    field_by_id = {row["task_id"]: row for row in field_primary}
    try:
        field_repeats = [deepcopy(field_by_id[key]) for key in frozen_truth["field_repeat_task_ids"]]
    except KeyError as exc:
        raise JudgeV5CalibrationV167Error("frozen field repeat task is not model-owned") from exc
    repeat_turn = {
        "turn_name": FIELD_REPEAT_TURN,
        "turn_role": "field_repeat_batch",
        "value": _field_repeat_value(field_repeats),
        "task_ids": [row["task_id"] for row in field_repeats],
    }
    derived_truth = {
        "support": [
            {"case_id": case_id, "witness_id": witness_id, "expected_status": verdict}
            for case_id, case in reference["cases"].items()
            for witness_id, verdict in case["proposition"].items()
        ],
        "support_canary_witness_ids": sorted(row["witness_id"] for row in support_canary_units),
        "field_tasks": frozen_truth["field_tasks"],
        "field_repeat_task_ids": frozen_truth["field_repeat_task_ids"],
        "alignment_cases": [
            {"case_id": case_id, "shape": case["shape"], "expected": v155._correct_support_only_projection(case)}
            for case_id, case in sorted(reference["cases"].items())
        ],
        "alignment_canary_case_ids": sorted(reference["canary_case_ids"]),
    }
    for key, value in derived_truth.items():
        if frozen_truth.get(key) != value:
            raise JudgeV5CalibrationV167Error(f"corrected v166 truth derivation drifted: {key}")
    alignment_case_shards = calibration_case_shards(pool)
    if len(alignment_case_shards) != len(ALIGNMENT_PRIMARY_TURNS):
        raise JudgeV5CalibrationV167Error("alignment shard coverage drifted")
    selection = {
        "schema_version": V167_SPEC_VERSION,
        "created_at": now_iso(),
        "field_truth_task_count": len(frozen_truth["field_tasks"]),
        "field_model_primary_turn_count": len(field_primary),
        "field_status_counts": {
            status: sum(row["expected_status"] == status for row in frozen_truth["field_tasks"])
            for status in ("correct", "incorrect")
        },
        "field_enum_count": len(CHECKLIST_FIELDS),
        "unsupported_inference_projection_task_count": sum(
            row["field"] == "unsupported_inference" for row in frozen_truth["field_tasks"]
        ),
        "selection_uses_source_text": False,
        "selection_uses_only_frozen_reference_labels_provenance_and_opaque_ids": True,
        "prior_model_outputs_used_for_selection": False,
        "support_primary_turn_count": len(SUPPORT_PRIMARY_TURNS),
        "support_canary_turn_count": 1,
        "field_repeat_turn_count": 1,
        "field_repeat_task_count": len(field_repeats),
        "alignment_primary_turn_count": len(ALIGNMENT_PRIMARY_TURNS),
        "alignment_canary_turn_count": 1,
        "verifier_primary_turn_count": len(VERIFIER_PRIMARY_TURNS),
        "verifier_canary_turn_count": len(VERIFIER_CANARY_TURNS),
        "semantic_pruning_performed": False,
    }
    return {
        "pool": pool,
        "reference": reference,
        "pointwise": pointwise,
        "truth": frozen_truth,
        "selection": selection,
        "support_case_shards": support_case_shards,
        "alignment_case_shards": alignment_case_shards,
        "static_turns": support_turns + field_primary + [repeat_turn],
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    audit = {
        "schema_version": V167_CAPACITY_AUDIT_VERSION,
        "phase_id": V167_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "predecessor": predecessor,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "measured_predecessor_maximum_total_tokens": 38416,
            "live_prelaunch_primary_used_percent": 34,
            "projected_terminal_remaining_percent": 20,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V167_CAPACITY_POLICY_VERSION,
        "phase_id": V167_PHASE_ID,
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
        "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000),
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v167(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v167 terminal")}
    source = _validate_v166()
    data = build_v167_inputs(source)
    paths = {
        "pool": root / "shared-witness-pool.private.json",
        "pointwise": root / "pointwise-input-full.private.json",
        "truth": root / "full-calibration-truth.private.json",
        "selection": root / "full-calibration-selection-audit.json",
    }
    _write_immutable(paths["pool"], data["pool"])
    _write_immutable(paths["pointwise"], data["pointwise"])
    _write_immutable(paths["truth"], data["truth"])
    _write_stable_time(paths["selection"], data["selection"], "created_at")
    turns = []
    for row in data["static_turns"]:
        is_support = row["turn_role"].startswith("support_")
        prompt = v143.support_prompt_v143(row["value"]) if is_support else v149.field_prompt_v149(row["value"])
        schema = v143.support_output_schema(row["value"]) if is_support else field_output_schema(row["value"])
        request_paths = _freeze_turn_request(
            root=root, turn_name=row["turn_name"], input_value=row["value"], prompt=prompt, schema=schema
        )
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": request_paths})
    predecessor = {
        **{f"v166_{name}": record for name, record in source["records"].items()},
        "v166_sidecars": source["sidecars"],
        "v163_field_protocol": source["field_protocol_record"],
        "legacy_pool": source["legacy"]["v153"]["v106"]["records"]["pool"],
        "cumulative_usage": source["cumulative_usage"],
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_files = [_record(Path(__file__)), _record(Path(v155.__file__)), *source["values"]["spec"]["runtime_files"]]
    spec = {
        "schema_version": V167_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "support_model": SUPPORT_MODEL,
        "field_model": FIELD_MODEL,
        "alignment_model": ALIGNMENT_MODEL,
        "equivalent_pair_verifier_model": VERIFIER_MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_support_singleton_fields_primary_alignment_all_equivalent_pair_verification_strict_balanced_canary",
        "case_count": 66,
        "witness_count": 182,
        "field_truth_task_count": 30,
        "turn_plan": list(TURN_NAMES),
        "minimum_turn_count": len(TURN_NAMES),
        "maximum_turn_count": len(TURN_NAMES),
        "retry_count_per_turn": 0,
        "all_semantic_turns_fresh": True,
        "prior_model_outputs_reused": False,
        "reference_truth_exposed_to_model": False,
        "all_primary_equivalent_pairs_verified": True,
        "strict_canary_no_repair_call": True,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": runtime_files,
        "frozen_instructions": {
            "support_sha256": sha256_text(v143.support_base_instructions_v143()),
            "field_sha256": sha256_text(v146.base_instructions_v146()),
            "alignment_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            **{name: _record(path) for name, path in paths.items()},
            "field_protocol": source["field_protocol_record"],
            "alignment_protocol": source["records"]["protocol"],
            "reference": source["records"]["reference"],
            "truth_source": source["records"]["truth"],
            "static_turns": [
                {
                    "turn_name": turn["turn_name"],
                    "role": turn["turn_role"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_truth_and_mapping_sanitized_terminal_only",
    }
    spec_path = root / "fresh-full-replacement-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "data": data,
        "source": source,
    }


def _pair_rows(
    *, normalized: Mapping[str, Any], alignment_input: Mapping[str, Any], prefix: str
) -> list[dict[str, Any]]:
    input_cases = {str(row["case_id"]): row for row in alignment_input["cases"]}
    rows = []
    for case in normalized["cases"]:
        case_id = str(case["case_id"])
        witnesses = {str(row["witness_id"]): row for row in input_cases[case_id]["witnesses"]}
        for pair in case["alignment_pairs"]:
            if pair["relation"] != "equivalent":
                continue
            witness_ids = sorted(str(value) for value in pair["witness_ids"])
            pair_case_id = "verifycase_" + sha256_text(
                f"v167|{prefix}|{case_id}|{'|'.join(witness_ids)}"
            )[:24]
            rows.append(
                {
                    "pair_case_id": pair_case_id,
                    "original_case_id": case_id,
                    "witness_ids": witness_ids,
                    "case": {
                        "case_id": pair_case_id,
                        "source_excerpt": input_cases[case_id]["source_excerpt"],
                        "witnesses": [deepcopy(witnesses[key]) for key in witness_ids],
                    },
                }
            )
    rows.sort(key=lambda row: sha256_text(f"v167|{prefix}|pair-order|{row['pair_case_id']}"))
    return rows


def _verifier_turn_value(
    *, template: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], permutation: str
) -> dict[str, Any]:
    value = deepcopy(dict(template))
    value["cases"] = [deepcopy(row["case"]) for row in rows]
    value["permutation"] = permutation
    value["permuted_axes"] = [] if permutation == "base" else ["anonymous_case_order", "anonymous_witness_order"]
    return value


def _merge_normalized(outputs: Sequence[Mapping[str, Any]], inputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    cases = []
    for output, value in zip(outputs, inputs, strict=True):
        cases.extend(normalize_neutral_alignment_output(output, value)["cases"])
    return {"cases": sorted(cases, key=lambda row: str(row["case_id"]))}


def _apply_verifier(
    *, normalized: Mapping[str, Any], pair_rows: Sequence[Mapping[str, Any]],
    verifier: Mapping[str, Any],
) -> dict[str, Any]:
    cases = {str(row["case_id"]): deepcopy(row) for row in normalized["cases"]}
    mappings = {row["pair_case_id"]: row for row in pair_rows}
    verifier_cases = {str(row["case_id"]): row for row in verifier["cases"]}
    if set(verifier_cases) != set(mappings):
        raise JudgeV5CalibrationV167Error("verifier output coverage drifted")
    for pair_case_id, result in verifier_cases.items():
        mapping = mappings[pair_case_id]
        case = cases[mapping["original_case_id"]]
        wanted = tuple(mapping["witness_ids"])
        replacement = deepcopy(result["alignment_pairs"][0])
        for index, pair in enumerate(case["alignment_pairs"]):
            if tuple(sorted(pair["witness_ids"])) == wanted:
                case["alignment_pairs"][index] = replacement
                break
        else:
            raise JudgeV5CalibrationV167Error("verifier pair target drifted")
    for case in cases.values():
        groups, paired = [], set()
        for pair in case["alignment_pairs"]:
            ids = sorted(pair["witness_ids"])
            if paired.intersection(ids):
                raise JudgeV5CalibrationV167Error("alignment pair ownership overlaps")
            paired.update(ids)
            groups.extend([ids] if pair["relation"] == "equivalent" else [[ids[0]], [ids[1]]])
        groups.extend([[value] for value in case["unpaired_witness_ids"] if value not in paired])
        case["equivalence_groups"] = sorted(groups)
    return {"cases": sorted(cases.values(), key=lambda row: str(row["case_id"]))}


def _write_failure(root: Path, turn_name: Optional[str], error_class: str) -> dict[str, Any]:
    attempts = [row for row in _attempt_records(root) if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v167 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    predecessor = _validate_v166()["cumulative_usage"]
    cumulative = _sum_usage(predecessor, usage)
    failure = {
        "schema_version": V167_FAILURE_VERSION,
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
        "schema_version": V167_TERMINAL_VERSION,
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


async def _run_alignment_turn(
    *, client: Any, root: Path, turn_name: str, value: Mapping[str, Any],
    model: str, timeout_seconds: float, policy_path: Path,
) -> tuple[dict[str, Any], Mapping[str, Any]]:
    prompt = v130.alignment_prompt_v130(value)
    schema = neutral_alignment_output_schema(value)
    paths = _freeze_turn_request(root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema)
    output, sidecar, _ = await _get_or_run_turn(
        client=client,
        turn_name=turn_name,
        paths=paths,
        prompt=prompt,
        schema=schema,
        base_instructions=v130.alignment_instructions_v130(),
        model=model,
        effort=EFFORT,
        timeout_seconds=timeout_seconds,
        batch_size=len(value["cases"]),
        policy_path=policy_path,
        output_validator=lambda candidate: v157.validate_structurally_projectable_output(candidate, value),
    )
    projected, audit = v157.project_exact_spans_and_relation(output, value)
    _write_immutable(root / "turns" / turn_name.replace("_", "-") / "structural-projection-audit.json", audit)
    return projected, sidecar


async def run_v167(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v167 terminal")
    frozen = freeze_v167(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    sidecars = []
    try:
        support_primary, support_canary, field_primary, field_repeats = [], [], [], []
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                is_support = turn["turn_role"].startswith("support_")
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v143.support_base_instructions_v143() if is_support else v146.base_instructions_v146(),
                    model=SUPPORT_MODEL if is_support else FIELD_MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=turn["value"].get("unit_count", turn["value"].get("task_count")),
                    policy_path=frozen["capacity_policy"],
                    output_validator=(lambda candidate, item=turn["value"]: v143.validate_support_output(candidate, item)) if is_support else (lambda candidate, item=turn["value"]: validate_field_output(candidate, item)),
                )
                sidecars.append(sidecar)
                if turn["turn_role"] == "support_primary": support_primary.append(output)
                elif turn["turn_role"] == "support_canary": support_canary.append(output)
                elif turn["turn_role"] == "field_singleton": field_primary.append(output)
                else: field_repeats.append(output)
            support = v155._merge_outputs(support_primary, "units")
            support_repeat = v155._merge_outputs(support_canary, "units")
            fields = v155._merge_outputs(field_primary, "decisions")
            repeats = v155._merge_outputs(field_repeats, "decisions")
            if v143.validate_support_output(support, v155._support_value(frozen["data"]["pointwise"]["units"])):
                raise JudgeV5CalibrationV167Error("aggregate support output is invalid")
            receipts = v155._support_receipts(support)
            _write_immutable(root / "support-output-full.private.json", support)
            _write_immutable(root / "support-canary-output.private.json", support_repeat)
            _write_immutable(root / "support-receipts.private.json", receipts)
            _write_immutable(root / "field-output.private.json", fields)
            _write_immutable(root / "field-repeat-output.private.json", repeats)

            alignment_outputs, alignment_inputs = [], []
            for turn_name, case_ids in zip(ALIGNMENT_PRIMARY_TURNS, frozen["data"]["alignment_case_shards"], strict=True):
                value = v155._support_positive_alignment_input(
                    frozen["data"]["pool"], receipts, case_ids=case_ids, permutation="base"
                )
                current_turn = turn_name
                output, sidecar = await _run_alignment_turn(
                    client=client, root=root, turn_name=turn_name, value=value,
                    model=ALIGNMENT_MODEL, timeout_seconds=timeout_seconds,
                    policy_path=frozen["capacity_policy"],
                )
                alignment_outputs.append(output)
                alignment_inputs.append(value)
                sidecars.append(sidecar)
            base_output = v155._merge_outputs(alignment_outputs, "cases")
            all_case_ids = [str(row["case_id"]) for row in frozen["data"]["pool"]["cases"]]
            base_input = v155._support_positive_alignment_input(
                frozen["data"]["pool"], receipts, case_ids=all_case_ids, permutation="base"
            )
            if validate_neutral_alignment_output(base_output, base_input):
                raise JudgeV5CalibrationV167Error("aggregate base alignment is invalid")
            base_normalized = normalize_neutral_alignment_output(base_output, base_input)
            _write_immutable(root / "alignment-base-output.private.json", base_output)

            canary_ids = frozen["data"]["truth"]["alignment_canary_case_ids"]
            canary_input = v155._support_positive_alignment_input(
                frozen["data"]["pool"], receipts, case_ids=canary_ids, permutation="balanced_canary"
            )
            current_turn = ALIGNMENT_CANARY_TURN
            canary_output, sidecar = await _run_alignment_turn(
                client=client, root=root, turn_name=ALIGNMENT_CANARY_TURN,
                value=canary_input, model=ALIGNMENT_MODEL,
                timeout_seconds=timeout_seconds, policy_path=frozen["capacity_policy"],
            )
            sidecars.append(sidecar)
            canary_normalized = normalize_neutral_alignment_output(canary_output, canary_input)
            _write_immutable(root / "alignment-canary-output.private.json", canary_output)

            base_pairs = _pair_rows(normalized=base_normalized, alignment_input=base_input, prefix="base")
            canary_pairs = _pair_rows(normalized=canary_normalized, alignment_input=canary_input, prefix="canary")
            base_shards = _balanced_shards([row["pair_case_id"] for row in base_pairs], len(VERIFIER_PRIMARY_TURNS))
            canary_shards = _balanced_shards([row["pair_case_id"] for row in canary_pairs], len(VERIFIER_CANARY_TURNS))
            base_by_id = {row["pair_case_id"]: row for row in base_pairs}
            canary_by_id = {row["pair_case_id"]: row for row in canary_pairs}
            verifier_audit = {
                "schema_version": V167_SPEC_VERSION,
                "base_primary_equivalent_pair_count": len(base_pairs),
                "canary_primary_equivalent_pair_count": len(canary_pairs),
                "all_primary_equivalent_pairs_selected": True,
                "base_turn_count": len(base_shards),
                "canary_turn_count": len(canary_shards),
                "selection_uses_only_primary_relation_and_opaque_ids": True,
                "truth_labels_used_for_selection": False,
            }
            _write_immutable(root / "equivalent-pair-verifier-selection-audit.json", verifier_audit)
            verifier_outputs, verifier_inputs = [], []
            for turn_name, shard_ids in zip(VERIFIER_PRIMARY_TURNS, base_shards, strict=True):
                rows = [base_by_id[key] for key in shard_ids]
                value = _verifier_turn_value(template=base_input, rows=rows, permutation="base")
                current_turn = turn_name
                output, sidecar = await _run_alignment_turn(
                    client=client, root=root, turn_name=turn_name, value=value,
                    model=VERIFIER_MODEL, timeout_seconds=timeout_seconds,
                    policy_path=frozen["capacity_policy"],
                )
                verifier_outputs.append(output)
                verifier_inputs.append(value)
                sidecars.append(sidecar)
            verifier_normalized = _merge_normalized(verifier_outputs, verifier_inputs)
            final_base = _apply_verifier(
                normalized=base_normalized, pair_rows=base_pairs, verifier=verifier_normalized
            )
            _write_immutable(root / "alignment-final-base.private.json", final_base)

            verifier_canary_outputs, verifier_canary_inputs = [], []
            for turn_name, shard_ids in zip(VERIFIER_CANARY_TURNS, canary_shards, strict=True):
                rows = [canary_by_id[key] for key in shard_ids]
                value = _verifier_turn_value(template=canary_input, rows=rows, permutation="balanced_canary")
                current_turn = turn_name
                output, sidecar = await _run_alignment_turn(
                    client=client, root=root, turn_name=turn_name, value=value,
                    model=VERIFIER_MODEL, timeout_seconds=timeout_seconds,
                    policy_path=frozen["capacity_policy"],
                )
                verifier_canary_outputs.append(output)
                verifier_canary_inputs.append(value)
                sidecars.append(sidecar)
            verifier_canary_normalized = _merge_normalized(verifier_canary_outputs, verifier_canary_inputs)
            final_canary = _apply_verifier(
                normalized=canary_normalized, pair_rows=canary_pairs, verifier=verifier_canary_normalized
            )
            _write_immutable(root / "alignment-final-canary.private.json", final_canary)

        base_cases = {str(row["case_id"]): v130._project_alignment(row) for row in final_base["cases"]}
        canary_cases = {str(row["case_id"]): v130._project_alignment(row) for row in final_canary["cases"]}
        canary_exact = sum(base_cases[key] == canary_cases[key] for key in canary_cases)
        disagreement_count = len(canary_cases) - canary_exact
        score = v155.score_v155(
            support=support,
            support_canary=support_repeat,
            fields=fields,
            field_repeats=repeats,
            alignment=final_base,
            truth=frozen["data"]["truth"],
            raw_alignment_disagreement_count=disagreement_count,
            final_canary_exact_count=canary_exact,
        )
        score["schema_version"] = V167_SCORE_VERSION
        score["equivalent_pair_verifier_base_count"] = len(base_pairs)
        score["equivalent_pair_verifier_canary_count"] = len(canary_pairs)
        score_path = root / "fresh-full-replacement-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        protocol_path = root / "development-judge-protocol-v167.json"
        if passed:
            _write_immutable(
                protocol_path,
                {
                    "schema_version": V167_PROTOCOL_VERSION,
                    "frozen_at": now_iso(),
                    "support_model": SUPPORT_MODEL,
                    "field_model": FIELD_MODEL,
                    "alignment_model": ALIGNMENT_MODEL,
                    "equivalent_pair_verifier_model": VERIFIER_MODEL,
                    "reasoning_effort": EFFORT,
                    "support_instructions_sha256": sha256_text(v143.support_base_instructions_v143()),
                    "field_instructions_sha256": sha256_text(v146.base_instructions_v146()),
                    "alignment_instructions_sha256": sha256_text(v130.alignment_instructions_v130()),
                    "field_protocol": frozen["spec"]["frozen_inputs"]["field_protocol"],
                    "alignment_protocol": frozen["spec"]["frozen_inputs"]["alignment_protocol"],
                    "reference": frozen["spec"]["frozen_inputs"]["reference"],
                    "truth": frozen["spec"]["frozen_inputs"]["truth_source"],
                    "all_primary_equivalent_pairs_verified": True,
                    "strict_canary_no_repair_call": True,
                    "retry_count_per_turn": 0,
                    "quality_gates_unchanged": True,
                    "selection_authorized": True,
                    "holdout_authorized": False,
                    "production_mutation_allowed": False,
                },
            )
        accounting = _aggregate_usage(sidecars)
        predecessor = frozen["source"]["cumulative_usage"]
        cumulative = _sum_usage(predecessor, accounting["usage"])
        terminal = {
            "schema_version": V167_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": "v167_full_development_calibration_passed_selection_authorized" if passed else "inactive_incomplete_recovery_required",
            "development_terminal_reason": "v167_full_development_calibration_passed" if passed else "v167_full_development_calibration_quality_gate_not_passed",
            "overall_evaluation_complete": False,
            "development_judge_frozen": passed,
            "selection_authorized": passed,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_count": 0,
            "failed_quality_gates": score["failed_checks"],
            "metrics": score["metrics"],
            "equivalent_pair_verifier_base_count": len(base_pairs),
            "equivalent_pair_verifier_canary_count": len(canary_pairs),
            "score": _record(score_path),
            "protocol": _record(protocol_path) if passed else None,
            "reference": frozen["spec"]["frozen_inputs"]["reference"],
            "truth": frozen["spec"]["frozen_inputs"]["truth_source"],
            "predecessor_cumulative_usage": predecessor,
            "cumulative_calibration_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v167 fresh full replacement calibration")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v167(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({
        "state": terminal["state"],
        "terminal_reason": terminal["terminal_reason"],
        "development_judge_frozen": terminal.get("development_judge_frozen", False),
        "selection_authorized": terminal.get("selection_authorized", False),
        "usage_status": terminal.get("usage_status"),
    }, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
