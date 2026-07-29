from __future__ import annotations

"""Fresh sharded neutral-alignment diagnostic against reference v13."""

import argparse
import asyncio
import json
import math
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic as v149
from .app_server_judge_v5 import (
    CHECKLIST_FIELDS,
    build_neutral_alignment_input,
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
    validate_neutral_alignment_output,
)
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
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_calibration_v107_recovery_receipt import _validate_v106
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _attempt_records, _validate_usage
from .util import now_iso, sha256_text


V150_RECEIPT_VERSION = "pif_app_server_judge_v5_4_v150_support_receipts_v1"
V150_TRUTH_VERSION = "pif_app_server_judge_v5_4_v150_alignment_truth_v1"
V150_SELECTION_VERSION = "pif_app_server_judge_v5_4_v150_selection_v1"
V150_SPEC_VERSION = "pif_app_server_judge_v5_4_v150_spec_v1"
V150_SCORE_VERSION = "pif_app_server_judge_v5_4_v150_score_v1"
V150_PROTOCOL_VERSION = "pif_app_server_judge_v5_4_v150_alignment_protocol_v1"
V150_FAILURE_VERSION = "pif_app_server_judge_v5_4_v150_failure_v1"
V150_TERMINAL_VERSION = "pif_app_server_judge_v5_4_v150_terminal_v1"
V150_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V150_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V150_PHASE_ID = "judge_v5_4_v150_fresh_alignment_diagnostic"

MODEL = "gpt-5.6-luna"
EFFORT = "high"
PRIMARY_TURNS = ("fresh_alignment_primary_00", "fresh_alignment_primary_01")
CANARY_TURNS = ("fresh_alignment_canary_00", "fresh_alignment_canary_01")
TURN_NAMES = PRIMARY_TURNS + CANARY_TURNS
CASES_PER_SHARD = 6
TIMEOUT_SECONDS = 1200.0
DEFAULT_OUTPUT_ROOT = (
    v149.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v150-fresh-alignment-diagnostic"
).resolve()
V106_ROOT = (
    v149.DEFAULT_OUTPUT_ROOT.parent / "judge-calibration-v5_4-v106-full-development"
).resolve()


class JudgeV5CalibrationV150Error(RuntimeError):
    """The v150 alignment diagnostic contract cannot be preserved."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _f1(tp: int, fp: int, fn: int) -> float:
    denominator = 2 * tp + fp + fn
    return round(2 * tp / denominator, 6) if denominator else 1.0


def _validate_v149() -> dict[str, Any]:
    root = v149.DEFAULT_OUTPUT_ROOT
    paths = {
        "spec": root / "fresh-corrected-field-diagnostic-spec.json",
        "terminal": root / "terminal.json",
        "score": root / "fresh-corrected-field-score.json",
        "protocol": root / "field-judge-protocol-v149.json",
        "support": root / "fresh-support-primary.private.json",
        "support_canary": root / "fresh-support-canary.private.json",
        "fields": root / "fresh-field-singletons.private.json",
        "repeats": root / "fresh-field-repeats.private.json",
        "truth": root / "fresh-corrected-field-truth.private.json",
        "selection": root / "selection-audit.json",
    }
    values = {name: _load_json(path, f"v149 {name}") for name, path in paths.items()}
    terminal, score, spec = values["terminal"], values["score"], values["spec"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v149_field_protocol_frozen_alignment_diagnostic_authorized"
        or terminal.get("field_protocol_frozen") is not True
        or terminal.get("alignment_diagnostic_authorized") is not True
        or terminal.get("fresh_full_calibration_authorized") is not False
        or terminal.get("selection_authorized") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage", {}).get("total_tokens") != 472641
        or score.get("passed") is not True
        or score.get("failed_checks") != []
        or score.get("metrics", {}).get("field_decision_accuracy") != 1.0
        or score.get("metrics", {}).get("pointwise_field_issue_f1") != 1.0
        or score.get("metrics", {}).get("support_sensitivity") != 1.0
        or score.get("metrics", {}).get("support_specificity") != 1.0
        or spec.get("turn_plan") != list(v149.TURN_NAMES)
        or spec.get("retry_count_per_turn") != 0
        or spec.get("selection_authorized") is not False
        or spec.get("holdout_authorized") is not False
        or spec.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5CalibrationV150Error("v149 predecessor contract drifted")
    if not all(_verify_record(record) for record in spec.get("runtime_files") or []):
        raise JudgeV5CalibrationV150Error("v149 runtime record drifted")
    for name, key in {
        "score": "score",
        "protocol": "protocol",
        "support": "support_output",
        "support_canary": "support_canary",
        "fields": "field_outputs",
        "repeats": "field_repeats",
    }.items():
        record = terminal.get(key)
        if record != _record(paths[name]) or not _verify_record(record):
            raise JudgeV5CalibrationV150Error(f"v149 {name} record drifted")
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
                raise JudgeV5CalibrationV150Error("v149 turn coverage is incomplete")
            records[name] = _record(path)
        measured = _validate_usage(
            _load_json(Path(records["sidecar"]["path"]), "v149 sidecar")
        )
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
        attempts[turn_name] = records
    if usage != terminal["usage"]:
        raise JudgeV5CalibrationV150Error("v149 usage aggregate drifted")
    source = v149._validate_v148()
    if terminal.get("reference") != source["records"]["reference"]:
        raise JudgeV5CalibrationV150Error("v149 reference binding drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "usage": usage,
        "v148": source,
    }


def _validate_v106_sources() -> dict[str, Any]:
    validated = _validate_v106()
    paths = {
        "pool": V106_ROOT / "shared-witness-pool.private.json",
        "receipts": V106_ROOT / "support-receipts.private.json",
    }
    records = {name: _record(path) for name, path in paths.items()}
    if any(not _verify_record(record) for record in records.values()):
        raise JudgeV5CalibrationV150Error("v106 alignment source drifted")
    return {
        "validated": validated,
        "paths": paths,
        "records": records,
        "pool": _load_json(paths["pool"], "v106 pool"),
        "receipts": _load_json(paths["receipts"], "v106 support receipts"),
    }


def _case_witness_ids(case: Mapping[str, Any]) -> list[str]:
    return [
        str(row["witness_id"])
        for side in ("event_set_a", "event_set_b")
        for row in case[side]
    ]


def _expected_projection(case: Mapping[str, Any]) -> dict[str, Any]:
    return v130._support_only_projection(
        {
            "alignment_pairs": case["pairs"],
            "equivalence_groups": case["equivalence_groups"],
            "unpaired_witness_ids": case["unpaired_witness_ids"],
        },
        case["proposition"],
    )


def _relation_bucket(projection: Mapping[str, Any]) -> str:
    counts = Counter(row["relation"] for row in projection["pairs"])
    if counts["partial"]:
        return "partial"
    if counts["non_equivalent"]:
        return "non_equivalent"
    if sum(counts.values()) > 1:
        return "multi_equivalent"
    return "equivalent"


def build_v150_inputs(
    source: Mapping[str, Any], v106: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]:
    reference = source["v148"]["values"]["reference"]
    pool = v106["pool"]
    pool_cases = {str(row["case_id"]): row for row in pool["cases"]}
    receipts = {str(row["witness_id"]): row for row in v106["receipts"]["units"]}
    candidates: dict[str, list[dict[str, Any]]] = {
        key: [] for key in ("equivalent", "multi_equivalent", "partial", "non_equivalent")
    }
    for case_id, case in reference["cases"].items():
        pool_case = pool_cases[case_id]
        witness_ids = _case_witness_ids(pool_case)
        if any(receipts[witness_id]["proposition_verdict"] != case["proposition"][witness_id] for witness_id in witness_ids):
            continue
        supported = [witness_id for witness_id in witness_ids if case["proposition"][witness_id] == "supported"]
        if len(supported) < 2:
            continue
        projection = _expected_projection(case)
        bucket = _relation_bucket(projection)
        candidates[bucket].append(
            {
                "case_id": case_id,
                "shape": case["shape"],
                "has_unpaired": bool(projection["unpaired_witness_ids"]),
                "projection": projection,
            }
        )
    selected = []
    for bucket, values in candidates.items():
        values.sort(
            key=lambda row: (
                row["has_unpaired"] != (bucket in {"partial", "equivalent"}),
                sha256_text(f"v150|select|{bucket}|{row['case_id']}"),
            )
        )
        if len(values) < 3:
            raise JudgeV5CalibrationV150Error("v150 alignment stratum coverage drifted")
        selected.extend(values[:3])
    selected.sort(key=lambda row: sha256_text(f"v150|case-order|{row['case_id']}"))
    if (
        len(selected) != 12
        or Counter(_relation_bucket(row["projection"]) for row in selected)
        != {"equivalent": 3, "multi_equivalent": 3, "partial": 3, "non_equivalent": 3}
        or sum(row["has_unpaired"] for row in selected) < 4
    ):
        raise JudgeV5CalibrationV150Error("v150 selected alignment mix drifted")
    selected_ids = {row["case_id"] for row in selected}
    selected_witness_ids = {
        witness_id
        for case_id in selected_ids
        for witness_id in _case_witness_ids(pool_cases[case_id])
    }
    neutral_receipts = []
    for witness_id in sorted(selected_witness_ids):
        row = deepcopy(receipts[witness_id])
        row["structured_field_verdict"] = "abstain"
        row["field_issue_fields"] = []
        row["field_evidence_spans"] = []
        row["field_rationale"] = "Structured-field labels withheld from neutral alignment input."
        neutral_receipts.append(row)
    receipt_value = {
        "schema_version": v106["receipts"]["schema_version"],
        "units": neutral_receipts,
        "side_free": True,
        "claim_support_and_field_correctness_separate": True,
        "owner_projection_version": V150_RECEIPT_VERSION,
    }

    rows = []
    shards = [selected[index : index + CASES_PER_SHARD] for index in range(0, 12, CASES_PER_SHARD)]
    for ordinal, shard in enumerate(shards):
        case_ids = [row["case_id"] for row in shard]
        for role, permutation, turn_name in (
            ("alignment_primary", "base", PRIMARY_TURNS[ordinal]),
            ("alignment_canary", "balanced_canary", CANARY_TURNS[ordinal]),
        ):
            value = build_neutral_alignment_input(
                pool, receipt_value, case_ids=case_ids, permutation=permutation
            )
            status = {
                str(receipt["witness_id"]): receipt["proposition_verdict"]
                for receipt in receipt_value["units"]
            }
            for case in value["cases"]:
                case["witnesses"] = [
                    witness
                    for witness in case["witnesses"]
                    if status[str(witness["witness_id"])] == "supported"
                ]
                if len(case["witnesses"]) < 2:
                    raise JudgeV5CalibrationV150Error("v150 support-positive case collapsed")
            value["supported_witnesses_only"] = True
            value["structured_field_receipts_withheld"] = True
            rows.append(
                {
                    "turn_name": turn_name,
                    "turn_role": role,
                    "shard_ordinal": ordinal,
                    "case_ids": sorted(case_ids),
                    "value": value,
                }
            )
    rows.sort(key=lambda row: TURN_NAMES.index(row["turn_name"]))
    truth = {
        "schema_version": V150_TRUTH_VERSION,
        "case_count": 12,
        "relation_bucket_counts": {
            "equivalent": 3,
            "multi_equivalent": 3,
            "partial": 3,
            "non_equivalent": 3,
        },
        "unpaired_case_count": sum(row["has_unpaired"] for row in selected),
        "cases": [
            {
                "case_id": row["case_id"],
                "shape": row["shape"],
                "relation_bucket": _relation_bucket(row["projection"]),
                "expected": row["projection"],
            }
            for row in selected
        ],
    }
    selection = {
        "schema_version": V150_SELECTION_VERSION,
        "created_at": now_iso(),
        "candidate_case_count": sum(len(values) for values in candidates.values()),
        "selected_case_count": 12,
        "relation_bucket_counts": truth["relation_bucket_counts"],
        "unpaired_case_count": truth["unpaired_case_count"],
        "shape_counts": dict(sorted(Counter(row["shape"] for row in selected).items())),
        "primary_shard_count": 2,
        "canary_shard_count": 2,
        "cases_per_shard": 6,
        "primary_canary_membership_identical_by_shard": True,
        "canary_permuted_axes": ["anonymous_case_order", "anonymous_witness_order"],
        "support_positive_witnesses_only": True,
        "structured_field_receipts_withheld": True,
        "selection_uses_source_text": False,
        "selection_uses_only_frozen_reference_labels_provenance_and_opaque_ids": True,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "system_identity_in_model_input": False,
        "majority_voting_used": False,
        "privacy": "private_source_event_output_aggregate_reports_only",
    }
    return rows, truth, selection, receipt_value


def _merge_normalized(
    outputs: Mapping[str, Mapping[str, Any]], inputs: Mapping[str, Mapping[str, Any]], names: Sequence[str]
) -> dict[str, Any]:
    cases = []
    for name in names:
        normalized = normalize_neutral_alignment_output(outputs[name], inputs[name])
        cases.extend(deepcopy(normalized["cases"]))
    if len(cases) != 12 or len({row["case_id"] for row in cases}) != 12:
        raise JudgeV5CalibrationV150Error("v150 normalized merge coverage drifted")
    return {"cases": cases, "origin_neutral": True, "mismatch_fields_projected_from_checklists": True}


def score_v150(
    *, primary: Mapping[str, Any], canary: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {str(row["case_id"]): row["expected"] for row in truth["cases"]}
    observed_rows = {str(row["case_id"]): row for row in primary["cases"]}
    repeated_rows = {str(row["case_id"]): row for row in canary["cases"]}
    if set(observed_rows) != set(expected) or set(repeated_rows) != set(expected):
        raise JudgeV5CalibrationV150Error("v150 score coverage drifted")
    expected_pairs = {}
    observed_pairs = {}
    pair_tp = pair_fp = pair_fn = relation_exact = relation_count = 0
    eq_exact = eq_count = partial_exact = partial_count = non_exact = non_count = 0
    field_tp = field_fp = field_fn = 0
    case_exact = group_exact = unpaired_exact = canary_exact = abstentions = 0
    for case_id, wanted in expected.items():
        actual = v130._project_alignment(observed_rows[case_id])
        canary_exact += int(v130._owner_projection(observed_rows[case_id]) == v130._owner_projection(repeated_rows[case_id]))
        abstentions += int(v130._case_has_abstention(observed_rows[case_id]))
        wanted_map = {tuple(sorted(row["witness_ids"])): row for row in wanted["pairs"]}
        actual_map = {tuple(sorted(row["witness_ids"])): row for row in actual["pairs"]}
        expected_pairs.update({(case_id, key): row for key, row in wanted_map.items()})
        observed_pairs.update({(case_id, key): row for key, row in actual_map.items()})
        pair_tp += len(set(wanted_map) & set(actual_map))
        pair_fp += len(set(actual_map) - set(wanted_map))
        pair_fn += len(set(wanted_map) - set(actual_map))
        for key in set(wanted_map) & set(actual_map):
            wanted_relation = wanted_map[key]["relation"]
            actual_relation = actual_map[key]["relation"]
            relation_count += 1
            relation_exact += int(wanted_relation == actual_relation)
            if wanted_relation == "equivalent":
                eq_count += 1
                eq_exact += int(actual_relation == wanted_relation)
            elif wanted_relation == "partial":
                partial_count += 1
                partial_exact += int(actual_relation == wanted_relation)
            elif wanted_relation == "non_equivalent":
                non_count += 1
                non_exact += int(actual_relation == wanted_relation)
            wanted_fields = set(wanted_map[key]["mismatch_fields"])
            actual_fields = set(actual_map[key]["mismatch_fields"])
            field_tp += len(wanted_fields & actual_fields)
            field_fp += len(actual_fields - wanted_fields)
            field_fn += len(wanted_fields - actual_fields)
        case_exact += int(actual == wanted)
        group_exact += int(actual["equivalence_groups"] == wanted["equivalence_groups"])
        unpaired_exact += int(actual["unpaired_witness_ids"] == wanted["unpaired_witness_ids"])
    metrics = {
        "case_count": 12,
        "alignment_f1": _f1(pair_tp, pair_fp, pair_fn),
        "relation_accuracy": _ratio(relation_exact, relation_count),
        "equivalent_sensitivity": _ratio(eq_exact, eq_count),
        "partial_sensitivity": _ratio(partial_exact, partial_count),
        "non_equivalent_sensitivity": _ratio(non_exact, non_count),
        "mismatch_field_f1": _f1(field_tp, field_fp, field_fn),
        "exact_case_rate": _ratio(case_exact, 12),
        "equivalence_partition_exact_case_rate": _ratio(group_exact, 12),
        "unpaired_exact_case_rate": _ratio(unpaired_exact, 12),
        "permutation_canary_exact_count": canary_exact,
        "permutation_canary_case_count": 12,
        "order_bias": round(1 - canary_exact / 12, 6),
        "abstention_case_count": abstentions,
        "matched_pair_count": relation_count,
    }
    checks = {
        "alignment_f1": metrics["alignment_f1"] >= 0.95,
        "relation_accuracy": metrics["relation_accuracy"] >= 0.95,
        "equivalent_sensitivity": metrics["equivalent_sensitivity"] >= 0.95,
        "partial_sensitivity": metrics["partial_sensitivity"] >= 0.95,
        "non_equivalent_sensitivity": metrics["non_equivalent_sensitivity"] >= 0.95,
        "mismatch_field_f1": metrics["mismatch_field_f1"] >= 0.95,
        "equivalence_partition_exact_case_rate": metrics["equivalence_partition_exact_case_rate"] >= 0.95,
        "unpaired_exact_case_rate": metrics["unpaired_exact_case_rate"] >= 0.95,
        "permutation_canary_exact_rate": canary_exact == 12,
        "order_bias": metrics["order_bias"] <= 0.05,
        "abstention_case_count": abstentions == 0,
        "schema_and_exact_evidence_validation": True,
    }
    passed = all(checks.values())
    return {
        "schema_version": V150_SCORE_VERSION,
        "passed": passed,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "metrics": metrics,
        "fresh_full_development_calibration_authorized": passed,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path, policy_path = root / "capacity-policy-audit.json", root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOKENS_PER_TURN
    audit = {"schema_version": V150_CAPACITY_AUDIT_VERSION, "phase_id": V150_PHASE_ID, "created_at": now_iso(), "production_mutation_performed": False, "predecessor": predecessor, "measured_basis": {"declared_turn_count": len(TURN_NAMES), "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN, "phase_total_token_bound": bound}}
    _write_stable_time(audit_path, audit, "created_at")
    policy = {"schema_version": V150_CAPACITY_POLICY_VERSION, "phase_id": V150_PHASE_ID, "created_at": now_iso(), "managed_chatgpt_auth_only": True, "official_persistent_codex_app_server_only": True, "retry_count_per_turn": 0, "production_mutation_allowed": False, "rate_limit_reached_type_must_be_null": True, "unknown_usage_hard_stop": True, "ordered_turn_names": list(TURN_NAMES), "minimum_remaining_reserve_percent": 20, "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS, "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN, "phase_total_token_bound": bound, "projected_phase_quota_points": math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000), "semantic_output_root": str(root), "audit": _record(audit_path)}
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v150(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {"root": root, "terminal": _load_json(terminal_path, "v150 terminal")}
    source = _validate_v149()
    v106 = _validate_v106_sources()
    rows, truth, selection, receipts = build_v150_inputs(source, v106)
    truth_path = root / "fresh-alignment-truth.private.json"
    selection_path = root / "selection-audit.json"
    receipts_path = root / "neutralized-support-receipts.private.json"
    _write_immutable(truth_path, truth)
    _write_stable_time(selection_path, selection, "created_at")
    _write_immutable(receipts_path, receipts)
    turns = []
    for row in rows:
        value = row["value"]
        prompt = v130.alignment_prompt_v130(value)
        schema = neutral_alignment_output_schema(value)
        paths = _freeze_turn_request(root=root, turn_name=row["turn_name"], input_value=value, prompt=prompt, schema=schema)
        turns.append({**row, "prompt": prompt, "schema": schema, "paths": paths})
    predecessor = {
        **{f"v149_{name}": record for name, record in source["records"].items()},
        "v149_attempts": source["attempts"],
        "v148_reference": source["v148"]["records"]["reference"],
        "v106_pool": v106["records"]["pool"],
        "v106_support_receipts": v106["records"]["receipts"],
        **{f"v106_{name}": record for name, record in v106["validated"]["records"].items()},
    }
    capacity = _build_capacity_policy(root, predecessor)
    runtime_dir = Path(__file__).resolve().parent
    spec = {
        "schema_version": V150_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "fresh_stratified_support_positive_neutral_alignment_two_by_six_case_primary_canary_shards",
        "case_count": 12,
        "cases_per_shard": 6,
        "primary_shard_count": 2,
        "canary_shard_count": 2,
        "turn_plan": list(TURN_NAMES),
        "support_positive_witnesses_only": True,
        "structured_field_receipts_withheld": True,
        "prior_alignment_outputs_in_model_input": False,
        "fixture_truth_in_model_input": False,
        "system_identity_in_model_input": False,
        "majority_voting_used": False,
        "retry_count_per_turn": 0,
        "promotion_rule": "all_frozen_alignment_relation_field_partition_unpaired_order_abstention_schema_gates",
        "fresh_full_development_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor,
        "runtime_files": [
            _record(Path(__file__)),
            _record(runtime_dir / "app_server_judge_v5_calibration_v149_fresh_corrected_field_diagnostic.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v130_retained_alignment_owner.py"),
            _record(runtime_dir / "app_server_judge_v5.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v107_recovery_receipt.py"),
            _record(runtime_dir / "app_server_judge_v5_calibration_v26_diagnostic.py"),
            _record(runtime_dir / "app_server_capacity_reserve.py"),
            _record(runtime_dir / "codex_app_server.py"),
        ],
        "frozen_inputs": {
            "truth": _record(truth_path),
            "selection": _record(selection_path),
            "neutralized_support_receipts": _record(receipts_path),
            "reference": source["v148"]["records"]["reference"],
            "field_protocol": source["records"]["protocol"],
            "turns": [
                {"turn_name": turn["turn_name"], "role": turn["turn_role"], "case_ids": turn["case_ids"], "input": _record(turn["paths"]["input"]), "prompt": _record(turn["paths"]["prompt"]), "schema": _record(turn["paths"]["schema"])}
                for turn in turns
            ],
        },
        "privacy": "private_source_event_output_no_source_text_in_reports",
    }
    spec_path = root / "fresh-alignment-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {"root": root, "spec": spec, "spec_path": spec_path, "capacity_policy": capacity["policy"], "turns": turns, "truth": truth, "source": source}


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
            measured = _validate_usage(_load_json(Path(record["path"]), "v150 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    failure = {"schema_version": V150_FAILURE_VERSION, "terminal_at": now_iso(), "classification": "infrastructure_or_judge_attempt_failed", "failed_turn_name": turn_name, "error_class": error_class, "retry_allowed_in_this_version": False, "accounting_complete": complete, "usage_status": "complete" if complete else "unknown", "usage": usage if complete else None, "known_usage_lower_bound": usage, "unknown_usage_turn_count": unknown, "attempts": attempts}
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {"schema_version": V150_TERMINAL_VERSION, "state": "failed", "terminal_reason": "infrastructure_or_judge_attempt_failed", "overall_evaluation_complete": False, "failure": _record(failure_path), "alignment_protocol_frozen": False, "fresh_full_development_calibration_authorized": False, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_retry_count": 0, "accounting_complete": complete, "usage_status": failure["usage_status"], "usage": failure["usage"]}
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v150(*, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS, client_factory: Optional[Callable[[Path], Any]] = None) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v150 terminal")
    frozen = freeze_v150(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    try:
        outputs = {}
        sidecars = []
        inputs = {turn["turn_name"]: turn["value"] for turn in frozen["turns"]}
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v130.alignment_instructions_v130(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=CASES_PER_SHARD,
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: validate_neutral_alignment_output(candidate, item),
                )
                outputs[current_turn] = output
                sidecars.append(sidecar)
        primary = _merge_normalized(outputs, inputs, PRIMARY_TURNS)
        canary = _merge_normalized(outputs, inputs, CANARY_TURNS)
        score = score_v150(primary=primary, canary=canary, truth=frozen["truth"])
        paths = {"raw": root / "fresh-alignment-raw.private.json", "primary": root / "fresh-alignment-primary-normalized.private.json", "canary": root / "fresh-alignment-canary-normalized.private.json", "score": root / "fresh-alignment-score.json", "protocol": root / "alignment-judge-protocol-v150.json"}
        _write_immutable(paths["raw"], {"turns": outputs})
        _write_immutable(paths["primary"], primary)
        _write_immutable(paths["canary"], canary)
        _write_immutable(paths["score"], score)
        passed = bool(score["passed"])
        if passed:
            protocol = {"schema_version": V150_PROTOCOL_VERSION, "frozen_at": now_iso(), "model": MODEL, "reasoning_effort": EFFORT, "alignment_instructions_sha256": sha256_text(v130.alignment_instructions_v130()), "reference": frozen["source"]["v148"]["records"]["reference"], "field_protocol": frozen["source"]["records"]["protocol"], "support_positive_witnesses_only": True, "structured_field_receipts_withheld": True, "quality_gates_unchanged": True, "selection_authorized": False, "holdout_authorized": False, "production_mutation_allowed": False}
            _write_immutable(paths["protocol"], protocol)
        accounting = _aggregate_usage(sidecars)
        terminal = {"schema_version": V150_TERMINAL_VERSION, "state": "completed" if passed else "inactive", "terminal_at": now_iso(), "terminal_reason": "v150_alignment_protocol_frozen_full_development_calibration_authorized" if passed else "inactive_incomplete_recovery_required", "development_terminal_reason": "v150_fresh_alignment_diagnostic_passed" if passed else "v150_fresh_alignment_diagnostic_quality_gate_not_passed", "overall_evaluation_complete": False, "alignment_protocol_frozen": passed, "fresh_full_development_calibration_authorized": passed, "selection_authorized": False, "holdout_authorized": False, "production_mutated": False, "semantic_attempt_started": True, "semantic_retry_count": 0, "score": _record(paths["score"]), "raw_outputs": _record(paths["raw"]), "primary_output": _record(paths["primary"]), "canary_output": _record(paths["canary"]), "protocol": _record(paths["protocol"]) if passed else None, "reference": frozen["source"]["v148"]["records"]["reference"], "field_protocol": frozen["source"]["records"]["protocol"], "failed_quality_gates": score["failed_checks"], "metrics": score["metrics"], "predecessor_v149_usage": frozen["source"]["usage"], **accounting}
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, exc.turn_name, exc.error_class)
    except Exception as exc:
        return _write_failure(root, current_turn, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v150 fresh alignment diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(run_v150(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds))
    print(json.dumps({"state": terminal["state"], "terminal_reason": terminal["terminal_reason"], "alignment_protocol_frozen": terminal.get("alignment_protocol_frozen", False), "fresh_full_development_calibration_authorized": terminal.get("fresh_full_development_calibration_authorized", False), "usage_status": terminal.get("usage_status")}, sort_keys=True))
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
