from __future__ import annotations

"""Versioned v25 repair diagnostic for the scoreable calibration failure."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from .app_server_capacity_reserve import (
    RESERVE_CAPACITY_CHECKPOINT_VERSION,
    ReserveCapacityGatedCodexAppServerClient,
)
from .app_server_judge_v5 import (
    ALIGNMENT_RELATIONS,
    CHECKLIST_DECISIONS,
    CHECKLIST_FIELDS,
    POINTWISE_OUTPUT_VERSION,
    STRUCTURED_FIELD_VERDICTS,
    SUPPORT_VERDICTS,
    expected_relation_from_checklist,
    project_mismatch_fields,
    _f1,
    _ratio,
)
from .app_server_judge_v5_calibration import CALIBRATION_GATES
from .app_server_judge_v5_calibration_v25 import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_V25_PRESEMANTIC_ROOT,
    DIAGNOSTIC_CASE_IDS,
    V25_DIAGNOSTIC_SPEC_VERSION,
    _load_json as _load_v25_json,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _canonical_json,
    _record,
    _sha256_file,
    _validate_usage,
    _write_immutable_json,
    _write_immutable_text,
)
from .app_server_judge_v5_fixture import compact_empty_event_fields
from .codex_app_server import APP_SERVER_CLIENT_VERSION, CodexAppServerClient
from .util import now_iso, sha256_text, write_text_atomic


V25_REPAIR_DIAGNOSTIC_SPEC_VERSION = (
    "pif_app_server_judge_v5_4_v25_repair_diagnostic_spec_v1"
)
V25_REPAIR_DIAGNOSTIC_SCORE_VERSION = (
    "pif_app_server_judge_v5_4_v25_repair_diagnostic_score_v1"
)
V25_REPAIR_DIAGNOSTIC_TERMINAL_VERSION = (
    "pif_app_server_judge_v5_4_v25_repair_diagnostic_terminal_v1"
)
V25_REPAIR_DIAGNOSTIC_FAILURE_VERSION = (
    "pif_app_server_judge_v5_4_v25_repair_diagnostic_failure_v1"
)
V25_REPAIR_DIAGNOSTIC_NON_ACCEPTANCE_VERSION = (
    "pif_app_server_judge_v5_4_v25_repair_diagnostic_non_acceptance_v1"
)
V26_PRESEMANTIC_DESIGN_VERSION = (
    "pif_app_server_judge_v5_4_v26_presemantic_design_v1"
)
V25_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V25_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"

DEFAULT_PIPELINE_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DEFAULT_V23_ROOT = (
    DEFAULT_PIPELINE_ROOT
    / "judge-calibration-v5_4-reference-v2-capacity-v23/fresh-attempt"
)
DEFAULT_OUTPUT_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v25-repair-diagnostic"
).resolve()
DEFAULT_V26_PRESEMANTIC_DESIGN_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-v26-presemantic-design"
).resolve()
PINNED_CODEX_0_144_1 = Path(
    "/Users/kolbydayley/.codex/packages/standalone/releases/"
    "0.144.1-aarch64-apple-darwin/bin/codex"
)
MAX_PROMPT_BYTES = 96 * 1024
MAX_SCHEMA_BYTES = 64 * 1024
MAX_TOKENS_PER_TURN = 70_000
QUOTA_POINTS_PER_MILLION_TOKENS = 17


class JudgeV5CalibrationV25DiagnosticError(RuntimeError):
    """The v25 repair diagnostic cannot continue safely."""


class JudgeV5CalibrationV25DiagnosticAttemptFailed(
    JudgeV5CalibrationV25DiagnosticError
):
    def __init__(self, *, turn_name: Optional[str], error_class: str) -> None:
        self.turn_name = turn_name
        self.error_class = error_class
        super().__init__(f"{turn_name or 'pre_turn'} failed: {error_class}")


def _load_json(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JudgeV5CalibrationV25DiagnosticError(
            f"{purpose} is missing or invalid"
        ) from exc
    if not isinstance(value, dict):
        raise JudgeV5CalibrationV25DiagnosticError(f"{purpose} is not an object")
    return value


def _write_immutable(path: Path, value: Any) -> None:
    target = path.expanduser().resolve()
    rendered = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if target.exists():
        if target.read_text(encoding="utf-8") != rendered:
            raise JudgeV5CalibrationV25DiagnosticError(
                f"immutable v25 diagnostic artifact drifted: {target}"
            )
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(target, rendered)


def _turn_names() -> list[str]:
    return [
        f"repair_{permutation}_shard_{index:02d}"
        for permutation in ("base", "canary")
        for index in range(3)
    ]


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "output": turn_root / "output.private.json",
        "sidecar": turn_root / "sidecar.json",
        "capacity": turn_root / "capacity.json",
    }


def _selected_pool_cases(pool: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    cases = {
        str(case["case_id"]): case
        for case in pool.get("cases") or []
        if str(case.get("case_id")) in set(DIAGNOSTIC_CASE_IDS)
    }
    if set(cases) != set(DIAGNOSTIC_CASE_IDS):
        raise JudgeV5CalibrationV25DiagnosticError("selected v25 cases drifted")
    return cases


def _witnesses_for_case(case: Mapping[str, Any], *, canary: bool) -> list[dict[str, Any]]:
    witnesses = []
    for side in ("a", "b"):
        for witness in case.get(f"event_set_{side}") or []:
            rendered = deepcopy(witness)
            rendered["event"] = compact_empty_event_fields(rendered["event"])
            witnesses.append(rendered)
    witnesses = sorted(witnesses, key=lambda item: str(item["witness_id"]))
    if canary:
        witnesses.reverse()
    return witnesses


def _case_alignment_by_id(reconciled: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {str(case["case_id"]): case for case in reconciled.get("cases") or []}


def _pointwise_by_id(pointwise: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {str(unit["witness_id"]): unit for unit in pointwise.get("units") or []}


def _build_shard_input(
    *,
    pool_cases: Mapping[str, Mapping[str, Any]],
    pointwise_rows: Mapping[str, Mapping[str, Any]],
    alignment_rows: Mapping[str, Mapping[str, Any]],
    case_ids: Sequence[str],
    permutation: str,
) -> dict[str, Any]:
    canary = permutation == "canary"
    rendered_cases = []
    for case_id in case_ids:
        source_case = pool_cases[case_id]
        witnesses = []
        for witness in _witnesses_for_case(source_case, canary=canary):
            witness_id = str(witness["witness_id"])
            support = pointwise_rows[witness_id]
            witnesses.append(
                {
                    "witness_id": witness_id,
                    "event": witness["event"],
                    "v24_support_receipt": {
                        "proposition_verdict": support["proposition_verdict"],
                        "structured_field_verdict": support[
                            "structured_field_verdict"
                        ],
                        "field_issue_fields": support["field_issue_fields"],
                    },
                }
            )
        rendered_cases.append(
            {
                "case_id": case_id,
                "source_excerpt": source_case["source_excerpt"],
                "witnesses": witnesses,
                "v24_alignment": alignment_rows[case_id],
            }
        )
    return {
        "schema_version": "pif_app_server_judge_v5_4_v25_repair_input_v1",
        "permutation": permutation,
        "side_labels_present": False,
        "system_identity_present": False,
        "v24_outputs_are_starting_evidence_not_truth": True,
        "cases": rendered_cases,
    }


def _repair_output_schema(input_value: Mapping[str, Any]) -> dict[str, Any]:
    case_ids = [str(case["case_id"]) for case in input_value["cases"]]
    witness_ids = sorted(
        {
            str(witness["witness_id"])
            for case in input_value["cases"]
            for witness in case["witnesses"]
        }
    )
    witness_schema = {"type": "string", "enum": witness_ids}
    evidence_spans = {
        "type": "array",
        "maxItems": 4,
        "items": {"type": "string", "minLength": 1, "maxLength": 1000},
    }
    support_row = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "witness_id",
            "proposition_verdict",
            "structured_field_verdict",
            "field_issue_fields",
            "source_evidence_spans",
            "rationale",
        ],
        "properties": {
            "witness_id": witness_schema,
            "proposition_verdict": {
                "type": "string",
                "enum": list(SUPPORT_VERDICTS),
            },
            "structured_field_verdict": {
                "type": "string",
                "enum": list(STRUCTURED_FIELD_VERDICTS),
            },
            "field_issue_fields": {
                "type": "array",
                "maxItems": len(CHECKLIST_FIELDS),
                "items": {"type": "string", "enum": list(CHECKLIST_FIELDS)},
            },
            "source_evidence_spans": evidence_spans,
            "rationale": {"type": "string", "minLength": 1, "maxLength": 600},
        },
    }
    checklist_row = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "field",
            "decision",
            "source_evidence_spans",
            "witness_evidence_ids",
            "rationale",
        ],
        "properties": {
            "field": {"type": "string", "enum": list(CHECKLIST_FIELDS)},
            "decision": {"type": "string", "enum": list(CHECKLIST_DECISIONS)},
            "source_evidence_spans": evidence_spans,
            "witness_evidence_ids": {
                "type": "array",
                "minItems": 2,
                "maxItems": 2,
                "items": witness_schema,
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 500},
        },
    }
    pair = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "witness_id_1",
            "witness_id_2",
            "relation",
            "checklist",
            "rationale",
        ],
        "properties": {
            "witness_id_1": witness_schema,
            "witness_id_2": witness_schema,
            "relation": {"type": "string", "enum": list(ALIGNMENT_RELATIONS)},
            "checklist": {
                "type": "array",
                "minItems": len(CHECKLIST_FIELDS),
                "maxItems": len(CHECKLIST_FIELDS),
                "items": checklist_row,
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 600},
        },
    }
    group = {
        "type": "object",
        "additionalProperties": False,
        "required": ["witness_ids", "rationale"],
        "properties": {
            "witness_ids": {"type": "array", "minItems": 1, "items": witness_schema},
            "rationale": {"type": "string", "minLength": 1, "maxLength": 500},
        },
    }
    case = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "case_id",
            "support_units",
            "equivalence_groups",
            "alignment_pairs",
            "unpaired_witness_ids",
        ],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "support_units": {"type": "array", "items": support_row},
            "equivalence_groups": {"type": "array", "items": group},
            "alignment_pairs": {"type": "array", "items": pair},
            "unpaired_witness_ids": {"type": "array", "items": witness_schema},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["cases"],
        "properties": {
            "cases": {
                "type": "array",
                "minItems": len(case_ids),
                "maxItems": len(case_ids),
                "items": case,
            }
        },
    }


def _repair_base_instructions() -> str:
    return (
        "You are a side-free v25 judge repair verifier. The supplied v24 support "
        "receipts and alignment are starting evidence, not truth. Re-evaluate every "
        "witness and pair from the source excerpt only. Keep proposition support and "
        "structured-event field correctness separate. Exact text alone is not enough "
        "for unsupported inference; every material claim must be entailed. Pair "
        "support-positive witnesses by semantic equivalence and leave unsupported "
        "residual witnesses unpaired unless source-supported overlap justifies a pair. "
        "Use the 15-row checklist exactly, with abstain instead of guessing. Do not use "
        "origin, keyword rules, regex, token overlap, embeddings, verbal confidence, or "
        "majority voting."
    )


def _repair_prompt(input_value: Mapping[str, Any]) -> str:
    instructions = (
        "Return every case exactly once. Every witness in each case must appear exactly "
        "once in support_units. Every witness must also appear exactly once in either one "
        "alignment pair or unpaired_witness_ids, and equivalence_groups must partition all "
        "witnesses. For each pair checklist, include every field in this order: "
        + ", ".join(CHECKLIST_FIELDS)
        + ". Equivalent means all rows same. Partial is only event_boundary+evidence. "
        "Any other material difference is non_equivalent. source_evidence_spans must be "
        "verbatim source substrings; use [] when the conflict exists only in witness text."
    )
    packet = {
        "permutation": input_value["permutation"],
        "cases": input_value["cases"],
    }
    return instructions + "\n\n# V25 repair cases\n" + _canonical_json(packet) + "\n"


def _enforce_caps(prompt: str, schema: Mapping[str, Any]) -> None:
    if len(prompt.encode("utf-8")) > MAX_PROMPT_BYTES:
        raise JudgeV5CalibrationV25DiagnosticError("v25 repair prompt exceeds byte cap")
    if len(_canonical_json(schema).encode("utf-8")) > MAX_SCHEMA_BYTES:
        raise JudgeV5CalibrationV25DiagnosticError("v25 repair schema exceeds byte cap")


def _freeze_turn(
    *, root: Path, turn_name: str, input_value: Mapping[str, Any]
) -> dict[str, Path]:
    prompt = _repair_prompt(input_value)
    schema = _repair_output_schema(input_value)
    _enforce_caps(prompt, schema)
    paths = _turn_paths(root, turn_name)
    _write_immutable_json(paths["input"], input_value)
    _write_immutable_text(paths["prompt"], prompt)
    _write_immutable_json(paths["schema"], schema)
    return paths


def _validate_repair_output(output: Any, input_value: Mapping[str, Any]) -> list[str]:
    if not isinstance(output, dict) or set(output) != {"cases"}:
        return ["invalid_repair_output_root"]
    expected_cases = {str(case["case_id"]): case for case in input_value["cases"]}
    if not isinstance(output.get("cases"), list):
        return ["invalid_repair_cases"]
    errors: list[str] = []
    seen_cases = set()
    for case_index, row in enumerate(output["cases"]):
        prefix = f"case_{case_index}"
        if not isinstance(row, dict) or set(row) != {
            "case_id",
            "support_units",
            "equivalence_groups",
            "alignment_pairs",
            "unpaired_witness_ids",
        }:
            errors.append(prefix + "_invalid_shape")
            continue
        case_id = str(row.get("case_id"))
        if case_id not in expected_cases or case_id in seen_cases:
            errors.append(prefix + "_invalid_or_duplicate_case")
            continue
        seen_cases.add(case_id)
        source = expected_cases[case_id]["source_excerpt"]
        all_ids = {str(witness["witness_id"]) for witness in expected_cases[case_id]["witnesses"]}
        support_seen = set()
        support_verdicts = {}
        support_units = row.get("support_units")
        if not isinstance(support_units, list):
            errors.append(prefix + "_support_units_invalid")
        else:
            for unit_index, unit in enumerate(support_units):
                unit_prefix = f"{prefix}_support_{unit_index}"
                if not isinstance(unit, dict) or set(unit) != {
                    "witness_id",
                    "proposition_verdict",
                    "structured_field_verdict",
                    "field_issue_fields",
                    "source_evidence_spans",
                    "rationale",
                }:
                    errors.append(unit_prefix + "_invalid_shape")
                    continue
                witness_id = str(unit.get("witness_id"))
                support_seen.add(witness_id)
                support_verdicts[witness_id] = unit.get("proposition_verdict")
                issues = unit.get("field_issue_fields")
                spans = unit.get("source_evidence_spans")
                if (
                    witness_id not in all_ids
                    or unit.get("proposition_verdict") not in SUPPORT_VERDICTS
                    or unit.get("structured_field_verdict") not in STRUCTURED_FIELD_VERDICTS
                    or not isinstance(issues, list)
                    or len(issues) != len(set(issues))
                    or any(field not in CHECKLIST_FIELDS for field in issues)
                    or not isinstance(spans, list)
                    or len(spans) > 4
                    or len(spans) != len(set(spans))
                    or any(not isinstance(span, str) or span not in source for span in spans)
                ):
                    errors.append(unit_prefix + "_invalid")
                    continue
                structured = unit["structured_field_verdict"]
                if structured == "incorrect" and not issues:
                    errors.append(unit_prefix + "_incorrect_without_issue")
                if structured in {"correct", "abstain"} and issues:
                    errors.append(unit_prefix + "_nonincorrect_with_issue")
        if support_seen != all_ids:
            errors.append(prefix + "_support_coverage_mismatch")
        grouped = set()
        groups = row.get("equivalence_groups")
        if not isinstance(groups, list):
            errors.append(prefix + "_groups_invalid")
        else:
            for group in groups:
                ids = group.get("witness_ids") if isinstance(group, dict) else None
                if (
                    not isinstance(group, dict)
                    or set(group) != {"witness_ids", "rationale"}
                    or not isinstance(ids, list)
                    or not ids
                    or len(ids) != len(set(ids))
                    or any(witness_id not in all_ids for witness_id in ids)
                    or grouped & set(ids)
                ):
                    errors.append(prefix + "_group_invalid")
                    continue
                grouped.update(ids)
        if grouped != all_ids:
            errors.append(prefix + "_group_partition_mismatch")
        used = set()
        pairs = row.get("alignment_pairs")
        if not isinstance(pairs, list):
            errors.append(prefix + "_pairs_invalid")
        else:
            for pair_index, pair in enumerate(pairs):
                pair_prefix = f"{prefix}_pair_{pair_index}"
                if not isinstance(pair, dict) or set(pair) != {
                    "witness_id_1",
                    "witness_id_2",
                    "relation",
                    "checklist",
                    "rationale",
                }:
                    errors.append(pair_prefix + "_invalid_shape")
                    continue
                first = str(pair.get("witness_id_1"))
                second = str(pair.get("witness_id_2"))
                if first not in all_ids or second not in all_ids or first == second or first in used or second in used:
                    errors.append(pair_prefix + "_invalid_partition")
                else:
                    used.update((first, second))
                checklist = pair.get("checklist")
                if not isinstance(checklist, list) or [item.get("field") for item in checklist if isinstance(item, dict)] != list(CHECKLIST_FIELDS):
                    errors.append(pair_prefix + "_checklist_coverage")
                    continue
                for item in checklist:
                    spans = item.get("source_evidence_spans") if isinstance(item, dict) else None
                    witness_evidence = item.get("witness_evidence_ids") if isinstance(item, dict) else None
                    if (
                        not isinstance(item, dict)
                        or set(item) != {
                            "field",
                            "decision",
                            "source_evidence_spans",
                            "witness_evidence_ids",
                            "rationale",
                        }
                        or item.get("decision") not in CHECKLIST_DECISIONS
                        or not isinstance(spans, list)
                        or len(spans) > 4
                        or len(spans) != len(set(spans))
                        or any(not isinstance(span, str) or span not in source for span in spans)
                        or not isinstance(witness_evidence, list)
                        or set(witness_evidence) != {first, second}
                    ):
                        errors.append(pair_prefix + "_checklist_invalid")
                        continue
                try:
                    expected_relation = expected_relation_from_checklist(checklist)
                    projected = set(project_mismatch_fields(checklist))
                except Exception:
                    errors.append(pair_prefix + "_projection_failed")
                    continue
                if pair.get("relation") != expected_relation:
                    errors.append(pair_prefix + "_relation_precedence_mismatch")
                if "event_boundary" in projected and "evidence" not in projected:
                    errors.append(pair_prefix + "_boundary_without_evidence")
        unpaired = row.get("unpaired_witness_ids")
        if (
            not isinstance(unpaired, list)
            or len(unpaired) != len(set(unpaired))
            or any(witness_id not in all_ids or witness_id in used for witness_id in unpaired)
            or used | set(unpaired) != all_ids
        ):
            errors.append(prefix + "_unpaired_partition_mismatch")
    if seen_cases != set(expected_cases):
        errors.append("repair_case_coverage_mismatch")
    return errors


def _project_case(row: Mapping[str, Any]) -> dict[str, Any]:
    support = sorted(
        [
            {
                "witness_id": unit["witness_id"],
                "proposition_verdict": unit["proposition_verdict"],
                "structured_field_verdict": unit["structured_field_verdict"],
                "field_issue_fields": sorted(unit["field_issue_fields"]),
            }
            for unit in row["support_units"]
        ],
        key=lambda item: item["witness_id"],
    )
    pairs = []
    for pair in row["alignment_pairs"]:
        witness_ids = sorted((pair["witness_id_1"], pair["witness_id_2"]))
        pairs.append(
            {
                "witness_ids": witness_ids,
                "relation": pair["relation"],
                "mismatch_fields": project_mismatch_fields(pair["checklist"]),
            }
        )
    return {
        "case_id": row["case_id"],
        "support_units": support,
        "alignment_pairs": sorted(pairs, key=lambda item: tuple(item["witness_ids"])),
        "equivalence_groups": sorted(
            [sorted(group["witness_ids"]) for group in row["equivalence_groups"]]
        ),
        "unpaired_witness_ids": sorted(row["unpaired_witness_ids"]),
    }


def _merge_outputs(outputs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    cases = []
    seen = set()
    for output in outputs:
        for row in output["cases"]:
            case_id = str(row["case_id"])
            if case_id in seen:
                raise JudgeV5CalibrationV25DiagnosticError("duplicate case output")
            seen.add(case_id)
            cases.append(deepcopy(row))
    if seen != set(DIAGNOSTIC_CASE_IDS):
        raise JudgeV5CalibrationV25DiagnosticError("v25 diagnostic output coverage drifted")
    return {"cases": sorted(cases, key=lambda row: DIAGNOSTIC_CASE_IDS.index(row["case_id"]))}


def _score_outputs(
    *, base_output: Mapping[str, Any], canary_output: Mapping[str, Any], truth: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {case_id: truth["cases"][case_id] for case_id in DIAGNOSTIC_CASE_IDS}
    base_rows = {str(row["case_id"]): row for row in base_output["cases"]}
    canary_rows = {str(row["case_id"]): row for row in canary_output["cases"]}
    support_tp = support_fn = support_tn = support_fp = 0
    structured_correct = structured_total = 0
    pointwise_field_tp = pointwise_field_fp = pointwise_field_fn = 0
    alignment_tp = alignment_fp = alignment_fn = 0
    relation_correct = relation_total = 0
    field_tp = field_fp = field_fn = 0
    equivalent_tp = equivalent_fn = equivalent_tn = equivalent_fp = 0
    partition_exact = unpaired_exact = 0
    abstentions = 0
    disagreement_cases = 0
    for case_id, case_truth in expected.items():
        row = base_rows[case_id]
        canary_projected = _project_case(canary_rows[case_id])
        base_projected = _project_case(row)
        disagreement_cases += int(base_projected != canary_projected)
        support_rows = {unit["witness_id"]: unit for unit in row["support_units"]}
        for witness_id, expected_verdict in case_truth["proposition"].items():
            observed = support_rows[witness_id]["proposition_verdict"]
            if observed == "abstain":
                abstentions += 1
            if expected_verdict == "supported" and observed == "supported":
                support_tp += 1
            elif expected_verdict == "supported":
                support_fn += 1
            elif observed == "unsupported":
                support_tn += 1
            else:
                support_fp += 1
            structured_total += 1
            structured = support_rows[witness_id]["structured_field_verdict"]
            if structured == "abstain":
                abstentions += 1
            structured_correct += int(structured == case_truth["structured_fields"][witness_id])
            wanted = set(case_truth["field_issues"][witness_id])
            observed_issues = set(support_rows[witness_id]["field_issue_fields"])
            pointwise_field_tp += len(wanted & observed_issues)
            pointwise_field_fp += len(observed_issues - wanted)
            pointwise_field_fn += len(wanted - observed_issues)
        observed_pairs = {
            tuple(sorted(pair["witness_ids"])): pair
            for pair in base_projected["alignment_pairs"]
        }
        expected_pairs = {
            tuple(sorted(pair["witness_ids"])): pair for pair in case_truth["pairs"]
        }
        alignment_tp += len(set(observed_pairs) & set(expected_pairs))
        alignment_fp += len(set(observed_pairs) - set(expected_pairs))
        alignment_fn += len(set(expected_pairs) - set(observed_pairs))
        for pair_ids, expected_pair in expected_pairs.items():
            observed_pair = observed_pairs.get(pair_ids)
            observed_relation = observed_pair.get("relation") if observed_pair else None
            if observed_relation == "abstain":
                abstentions += 1
            relation_total += 1
            relation_correct += int(observed_relation == expected_pair["relation"])
            expected_equiv = expected_pair["relation"] == "equivalent"
            if expected_equiv and observed_relation == "equivalent":
                equivalent_tp += 1
            elif expected_equiv:
                equivalent_fn += 1
            elif observed_relation == "equivalent" or observed_relation in {None, "abstain"}:
                equivalent_fp += 1
            else:
                equivalent_tn += 1
            wanted_fields = set(expected_pair["mismatch_fields"])
            observed_fields = set(observed_pair.get("mismatch_fields") if observed_pair else [])
            field_tp += len(wanted_fields & observed_fields)
            field_fp += len(observed_fields - wanted_fields)
            field_fn += len(wanted_fields - observed_fields)
        partition_exact += int(
            {tuple(group) for group in base_projected["equivalence_groups"]}
            == {tuple(sorted(group)) for group in case_truth["equivalence_groups"]}
        )
        unpaired_exact += int(
            base_projected["unpaired_witness_ids"]
            == sorted(case_truth["unpaired_witness_ids"])
        )
    metrics = {
        "case_count": len(expected),
        "witness_count": structured_total,
        "support_sensitivity": _ratio(support_tp, support_tp + support_fn),
        "support_specificity": _ratio(support_tn, support_tn + support_fp),
        "structured_field_accuracy": _ratio(structured_correct, structured_total),
        "pointwise_field_issue_f1": _f1(pointwise_field_tp, pointwise_field_fp, pointwise_field_fn),
        "alignment_f1": _f1(alignment_tp, alignment_fp, alignment_fn),
        "equivalent_sensitivity": _ratio(equivalent_tp, equivalent_tp + equivalent_fn),
        "equivalent_specificity": _ratio(equivalent_tn, equivalent_tn + equivalent_fp),
        "field_diagnostic_f1": _f1(field_tp, field_fp, field_fn),
        "relation_accuracy": _ratio(relation_correct, relation_total),
        "equivalence_partition_exact_case_rate": _ratio(partition_exact, len(expected)),
        "unpaired_exact_case_rate": _ratio(unpaired_exact, len(expected)),
        "abstention_count": abstentions,
        "abstention_rate": _ratio(abstentions, structured_total + relation_total + len(expected)),
        "order_bias": _ratio(disagreement_cases, len(expected)),
    }
    counts = {
        "case_count": len(expected),
        "witness_count": structured_total,
        "support": {
            "true_positive": support_tp,
            "false_negative": support_fn,
            "true_negative": support_tn,
            "false_positive": support_fp,
        },
        "structured_field": {
            "correct": structured_correct,
            "total": structured_total,
        },
        "pointwise_field_issue": {
            "true_positive": pointwise_field_tp,
            "false_positive": pointwise_field_fp,
            "false_negative": pointwise_field_fn,
        },
        "alignment_pair": {
            "true_positive": alignment_tp,
            "false_positive": alignment_fp,
            "false_negative": alignment_fn,
        },
        "equivalence_relation": {
            "true_positive": equivalent_tp,
            "false_negative": equivalent_fn,
            "true_negative": equivalent_tn,
            "false_positive": equivalent_fp,
        },
        "field_diagnostic": {
            "true_positive": field_tp,
            "false_positive": field_fp,
            "false_negative": field_fn,
        },
        "relation": {
            "correct": relation_correct,
            "total": relation_total,
        },
        "equivalence_partition": {
            "exact_cases": partition_exact,
            "total_cases": len(expected),
        },
        "unpaired": {
            "exact_cases": unpaired_exact,
            "total_cases": len(expected),
        },
        "order": {
            "disagreement_cases": disagreement_cases,
            "total_cases": len(expected),
        },
        "abstention": {
            "count": abstentions,
            "denominator": structured_total + relation_total + len(expected),
        },
    }
    checks = {
        "support_sensitivity": metrics["support_sensitivity"] >= CALIBRATION_GATES["support_sensitivity_min"],
        "support_specificity": metrics["support_specificity"] >= CALIBRATION_GATES["support_specificity_min"],
        "structured_field_accuracy": metrics["structured_field_accuracy"] >= CALIBRATION_GATES["structured_field_accuracy_min"],
        "alignment_f1": metrics["alignment_f1"] >= CALIBRATION_GATES["alignment_f1_min"],
        "equivalent_sensitivity": metrics["equivalent_sensitivity"] >= CALIBRATION_GATES["equivalent_sensitivity_min"],
        "equivalent_specificity": metrics["equivalent_specificity"] >= CALIBRATION_GATES["equivalent_specificity_min"],
        "field_diagnostic_f1": metrics["field_diagnostic_f1"] >= CALIBRATION_GATES["field_diagnostic_f1_min"],
        "relation_accuracy": metrics["relation_accuracy"] >= CALIBRATION_GATES["relation_accuracy_min"],
        "equivalence_partition_exact_case_rate": metrics["equivalence_partition_exact_case_rate"] >= CALIBRATION_GATES["equivalence_partition_exact_case_rate_min"],
        "unpaired_exact_case_rate": metrics["unpaired_exact_case_rate"] >= CALIBRATION_GATES["unpaired_exact_case_rate_min"],
        "abstention_rate": metrics["abstention_rate"] <= CALIBRATION_GATES["abstention_rate_max"],
        "order_bias": metrics["order_bias"] <= CALIBRATION_GATES["order_bias_max"],
    }
    return {
        "schema_version": V25_REPAIR_DIAGNOSTIC_SCORE_VERSION,
        "passed": all(checks.values()),
        "metrics": metrics,
        "counts": counts,
        "checks": checks,
        "failed_checks": sorted(key for key, value in checks.items() if not value),
        "thresholds": CALIBRATION_GATES,
        "gates": CALIBRATION_GATES,
    }


def _build_capacity_policy(root: Path, presemantic_root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    turn_names = _turn_names()
    preterminal = presemantic_root / "terminal.json"
    audit = {
        "schema_version": V25_CAPACITY_AUDIT_VERSION,
        "phase_id": "judge_v5_4_v25_repair_diagnostic",
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "presemantic_terminal": _record(preterminal),
        "measured_basis": {
            "v23_total_tokens": 790731,
            "v25_declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
            "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        },
    }
    if audit_path.exists():
        prior_audit = _load_json(audit_path, "v25 capacity audit")
        stable_audit = deepcopy(audit)
        stable_audit["created_at"] = prior_audit.get("created_at")
        if prior_audit != stable_audit:
            raise JudgeV5CalibrationV25DiagnosticError(
                "immutable v25 capacity audit drifted"
            )
        audit = prior_audit
    else:
        _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V25_CAPACITY_POLICY_VERSION,
        "phase_id": "judge_v5_4_v25_repair_diagnostic",
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": turn_names,
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOKENS_PER_TURN,
        "phase_total_token_bound": len(turn_names) * MAX_TOKENS_PER_TURN,
        "projected_phase_quota_points": 8,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    if policy_path.exists():
        prior_policy = _load_json(policy_path, "v25 capacity policy")
        stable_policy = deepcopy(policy)
        stable_policy["created_at"] = prior_policy.get("created_at")
        if prior_policy != stable_policy:
            raise JudgeV5CalibrationV25DiagnosticError(
                "immutable v25 capacity policy drifted"
            )
        policy = prior_policy
    else:
        _write_immutable(policy_path, policy)
    return {"audit": audit_path, "policy": policy_path}


def freeze_v25_repair_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v23_root: Path = DEFAULT_V23_ROOT,
    presemantic_root: Path = DEFAULT_V25_PRESEMANTIC_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    preterminal = _load_v25_json(presemantic_root / "terminal.json", "v25 presemantic terminal")
    if (
        preterminal.get("terminal_reason") != "inactive_incomplete_recovery_required"
        or preterminal.get("semantic_attempt_started") is not False
        or preterminal.get("selection_authorized") is not False
    ):
        raise JudgeV5CalibrationV25DiagnosticError("v25 presemantic terminal is not admissible")
    spec_record = preterminal.get("diagnostic_spec")
    if not isinstance(spec_record, Mapping):
        raise JudgeV5CalibrationV25DiagnosticError("v25 diagnostic spec record missing")
    spec_path = Path(str(spec_record["path"]))
    if _sha256_file(spec_path) != spec_record["sha256"]:
        raise JudgeV5CalibrationV25DiagnosticError("v25 diagnostic spec drifted")
    pre_spec = _load_json(spec_path, "v25 presemantic diagnostic spec")
    if pre_spec.get("schema_version") != V25_DIAGNOSTIC_SPEC_VERSION:
        raise JudgeV5CalibrationV25DiagnosticError("v25 diagnostic spec version drifted")
    truth = _load_json(v23_root / "calibration-truth.private.json", "v23 truth")
    pool = _load_json(v23_root / "shared-witness-pool.private.json", "v23 witness pool")
    pointwise = _load_json(v23_root / "pointwise-output-full.private.json", "v23 pointwise output")
    reconciled = _load_json(
        Path(preterminal["source_records"]["v24_reconciled_alignment"]["path"]),
        "v24 reconciled alignment",
    )
    pool_cases = _selected_pool_cases(pool)
    pointwise_rows = _pointwise_by_id(pointwise)
    alignment_rows = _case_alignment_by_id(reconciled)
    shards = [
        list(DIAGNOSTIC_CASE_IDS[index : index + 6])
        for index in range(0, len(DIAGNOSTIC_CASE_IDS), 6)
    ]
    if len(shards) != 3 or any(len(shard) != 6 for shard in shards):
        raise JudgeV5CalibrationV25DiagnosticError("v25 repair shard layout drifted")
    turn_requests = {}
    for permutation in ("base", "canary"):
        for index, shard in enumerate(shards):
            turn_name = f"repair_{permutation}_shard_{index:02d}"
            input_value = _build_shard_input(
                pool_cases=pool_cases,
                pointwise_rows=pointwise_rows,
                alignment_rows=alignment_rows,
                case_ids=shard,
                permutation=permutation,
            )
            paths = _freeze_turn(root=root, turn_name=turn_name, input_value=input_value)
            turn_requests[turn_name] = {
                "paths": paths,
                "input": input_value,
                "prompt": paths["prompt"].read_text(encoding="utf-8"),
                "schema": _load_json(paths["schema"], "v25 repair schema"),
            }
    capacity = _build_capacity_policy(root, presemantic_root.expanduser().resolve())
    spec = {
        "schema_version": V25_REPAIR_DIAGNOSTIC_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "turn_plan": _turn_names(),
        "case_count": len(DIAGNOSTIC_CASE_IDS),
        "cases_per_shard": 6,
        "base_turn_count": 3,
        "canary_turn_count": 3,
        "semantic_model_calls_performed_during_freeze": 0,
        "retry_count_per_turn": 0,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "presemantic_terminal": _record(presemantic_root / "terminal.json"),
        "presemantic_diagnostic_spec": _record(spec_path),
        "v23_truth": _record(v23_root / "calibration-truth.private.json"),
        "v23_witness_pool": _record(v23_root / "shared-witness-pool.private.json"),
        "v23_pointwise_output": _record(v23_root / "pointwise-output-full.private.json"),
        "v24_reconciled_alignment": _record(
            Path(preterminal["source_records"]["v24_reconciled_alignment"]["path"])
        ),
        "request_byte_caps": {
            "prompt": MAX_PROMPT_BYTES,
            "output_schema": MAX_SCHEMA_BYTES,
        },
        "promotion_rule": pre_spec["promotion_rule"],
        "turns": {
            name: {
                "input": _record(request["paths"]["input"]),
                "prompt": _record(request["paths"]["prompt"]),
                "schema": _record(request["paths"]["schema"]),
            }
            for name, request in turn_requests.items()
        },
        "privacy": "private_inputs_prompts_outputs_no_sanitized_text_in_terminal",
    }
    spec_path = root / "repair-diagnostic-spec.json"
    if spec_path.exists():
        prior = _load_json(spec_path, "v25 repair diagnostic spec")
        stable = deepcopy(spec)
        stable["created_at"] = prior.get("created_at")
        if prior != stable:
            raise JudgeV5CalibrationV25DiagnosticError("v25 repair diagnostic spec drifted")
        spec = prior
    else:
        _write_immutable_json(spec_path, spec)
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "truth": truth,
        "turn_requests": turn_requests,
        "capacity_policy": capacity["policy"],
    }


def _validate_capacity_checkpoint(path: Path, policy_path: Path) -> None:
    value = _load_json(path, "v25 reserve capacity checkpoint")
    if (
        value.get("schema_version") != RESERVE_CAPACITY_CHECKPOINT_VERSION
        or value.get("policy_sha256") != _sha256_file(policy_path)
        or value.get("cleared_for_semantic_turn") is not True
        or value.get("managed_chatgpt_auth_verified") is not True
        or value.get("rate_limit_reached_type") is not None
        or value.get("retry_checkpoint_reuse_allowed") is not False
    ):
        raise JudgeV5CalibrationV25DiagnosticError("v25 reserve capacity checkpoint drifted")


def _validate_completed_turn(
    *,
    paths: Mapping[str, Path],
    prompt: str,
    schema: Mapping[str, Any],
    model: str,
    effort: str,
    policy_path: Path,
    input_value: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    output = _load_json(paths["output"], "v25 repair output")
    sidecar = _load_json(paths["sidecar"], "v25 repair sidecar")
    _validate_capacity_checkpoint(paths["capacity"], policy_path)
    required = {
        "state": "completed",
        "status": "completed",
        "client_version": APP_SERVER_CLIENT_VERSION,
        "transport": "stdio",
        "auth_type": "chatgpt",
        "thread_mode": "new_thread",
        "model": model,
        "effort": effort,
        "prompt_sha256": sha256_text(prompt),
        "base_instructions_sha256": sha256_text(_repair_base_instructions()),
        "output_schema_sha256": sha256_text(_canonical_json(schema)),
    }
    for key, expected in required.items():
        if sidecar.get(key) != expected:
            raise JudgeV5CalibrationV25DiagnosticError(f"v25 sidecar mismatch: {key}")
    _validate_usage(sidecar)
    output_text = paths["output"].read_text(encoding="utf-8")
    acceptable = {sha256_text(output_text)}
    if output_text.endswith("\n"):
        acceptable.add(sha256_text(output_text[:-1]))
    if sidecar.get("output_sha256") not in acceptable:
        raise JudgeV5CalibrationV25DiagnosticError("v25 output hash mismatch")
    errors = _validate_repair_output(output, input_value)
    if errors:
        raise JudgeV5CalibrationV25DiagnosticError(
            "v25 repair output invalid: " + "; ".join(errors)
        )
    return output, sidecar


def _checkpoint_state(paths: Mapping[str, Path]) -> str:
    present = {key: paths[key].exists() for key in ("output", "sidecar", "capacity")}
    if not any(present.values()):
        return "absent"
    if all(present.values()):
        sidecar = _load_json(paths["sidecar"], "v25 existing sidecar")
        return "completed" if sidecar.get("state") == "completed" else "terminal_noncomplete"
    return "partial"


async def _get_or_run_turn(
    *,
    client: Any,
    turn_name: str,
    request: Mapping[str, Any],
    model: str,
    effort: str,
    timeout_seconds: float,
    policy_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    paths = request["paths"]
    state = _checkpoint_state(paths)
    if state == "completed":
        output, sidecar = _validate_completed_turn(
            paths=paths,
            prompt=request["prompt"],
            schema=request["schema"],
            model=model,
            effort=effort,
            policy_path=policy_path,
            input_value=request["input"],
        )
        return output, sidecar, True
    if state != "absent":
        raise JudgeV5CalibrationV25DiagnosticAttemptFailed(
            turn_name=turn_name,
            error_class=f"immutable_{state}_checkpoint",
        )
    try:
        result = await client.run_ephemeral_structured_turn(
            model=model,
            effort=effort,
            base_instructions=_repair_base_instructions(),
            prompt=request["prompt"],
            output_schema=dict(request["schema"]),
            cwd=Path.cwd(),
            sidecar_path=paths["sidecar"],
            capacity_checkpoint_path=paths["capacity"],
            output_path=paths["output"],
            batch_size=len(request["input"]["cases"]),
            thread_mode="new_thread",
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        raise JudgeV5CalibrationV25DiagnosticAttemptFailed(
            turn_name=turn_name, error_class=type(exc).__name__
        ) from exc
    if not result.status_ok or not isinstance(result.output, dict):
        raise JudgeV5CalibrationV25DiagnosticAttemptFailed(
            turn_name=turn_name,
            error_class=str(result.error_class or result.status or "turn_failed"),
        )
    try:
        output, sidecar = _validate_completed_turn(
            paths=paths,
            prompt=request["prompt"],
            schema=request["schema"],
            model=model,
            effort=effort,
            policy_path=policy_path,
            input_value=request["input"],
        )
    except Exception as exc:
        raise JudgeV5CalibrationV25DiagnosticAttemptFailed(
            turn_name=turn_name, error_class=type(exc).__name__
        ) from exc
    if _canonical_json(output) != _canonical_json(result.output):
        raise JudgeV5CalibrationV25DiagnosticAttemptFailed(
            turn_name=turn_name, error_class="returned_output_checkpoint_mismatch"
        )
    return output, sidecar, False


def _attempt_records(root: Path) -> list[dict[str, Any]]:
    rows = []
    for turn_root in sorted((root / "turns").glob("*")) if (root / "turns").is_dir() else []:
        if not turn_root.is_dir():
            continue
        row: dict[str, Any] = {"turn_name": turn_root.name.replace("-", "_")}
        for key, filename in (
            ("capacity", "capacity.json"),
            ("sidecar", "sidecar.json"),
            ("output", "output.private.json"),
        ):
            path = turn_root / filename
            row[key] = _record(path) if path.is_file() else None
        if row["sidecar"] is not None:
            sidecar = _load_json(turn_root / "sidecar.json", "v25 attempt sidecar")
            row.update(
                {
                    "state": sidecar.get("state"),
                    "status": sidecar.get("status"),
                    "error_class": sidecar.get("error_class"),
                    "usage_status": sidecar.get("usage_status"),
                }
            )
        rows.append(row)
    return rows


def _aggregate_usage(sidecars: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = {field: 0 for field in USAGE_FIELDS}
    for sidecar in sidecars:
        usage = _validate_usage(sidecar)
        for field in USAGE_FIELDS:
            total[field] += usage[field]
    return {
        "accounting_complete": True,
        "usage_status": "complete",
        "usage": total,
        "turn_count": len(sidecars),
        "wall_elapsed_seconds_sum": round(
            sum(float(sidecar.get("wall_elapsed_seconds") or 0.0) for sidecar in sidecars),
            3,
        ),
    }


def _minimum_corrections_for_score(score: Mapping[str, Any]) -> dict[str, Any]:
    metrics = score["metrics"]
    counts = score.get("counts") or {}
    case_count = int(metrics["case_count"])
    witness_count = int(metrics["witness_count"])
    structured = counts.get("structured_field") or {}
    support = counts.get("support") or {}
    alignment = counts.get("alignment_pair") or {}
    relation = counts.get("relation") or {}
    unpaired = counts.get("unpaired") or {}
    partition = counts.get("equivalence_partition") or {}
    order = counts.get("order") or {}
    equivalent = counts.get("equivalence_relation") or {}
    field = counts.get("field_diagnostic") or {}

    structured_correct = int(
        structured.get("correct", round(metrics["structured_field_accuracy"] * witness_count))
    )
    relation_total = int(relation.get("total", 0))
    relation_correct = int(
        relation.get("correct", round(metrics["relation_accuracy"] * relation_total))
    )
    support_tn = int(support.get("true_negative", 0))
    support_fp = int(support.get("false_positive", 0))
    equivalent_tn = int(equivalent.get("true_negative", 0))
    equivalent_fp = int(equivalent.get("false_positive", 0))
    alignment_tp = int(alignment.get("true_positive", 0))
    alignment_fp = int(alignment.get("false_positive", 0))
    alignment_fn = int(alignment.get("false_negative", 0))
    field_tp = int(field.get("true_positive", 0))
    field_fp = int(field.get("false_positive", 0))
    field_fn = int(field.get("false_negative", 0))
    unpaired_exact = int(unpaired.get("exact_cases", round(metrics["unpaired_exact_case_rate"] * case_count)))
    partition_exact = int(
        partition.get(
            "exact_cases",
            round(metrics["equivalence_partition_exact_case_rate"] * case_count),
        )
    )
    order_disagreement = int(order.get("disagreement_cases", round(metrics["order_bias"] * case_count)))

    def required_successes(total: int, minimum: float) -> int:
        return math.ceil(total * minimum)

    def f1_corrections(tp: int, fp: int, fn: int, minimum: float) -> int:
        # Prefer pair-replacement corrections because one wrong link often removes
        # one false positive and restores one false negative in the same case.
        for replacements in range(0, max(fp, fn) + 1):
            tp2 = tp + replacements
            fp2 = max(0, fp - replacements)
            fn2 = max(0, fn - replacements)
            if _f1(tp2, fp2, fn2) >= minimum:
                return replacements
        for single_edits in range(0, fp + fn + 1):
            for removed_fp in range(0, min(fp, single_edits) + 1):
                restored_fn = min(fn, single_edits - removed_fp)
                if _f1(tp + restored_fn, fp - removed_fp, fn - restored_fn) >= minimum:
                    return single_edits
        return fp + fn + 1

    return {
        "support_specificity": {
            "current_true_negative": support_tn,
            "current_false_positive": support_fp,
            "minimum_false_positive_to_true_negative_corrections": max(
                0,
                required_successes(
                    support_tn + support_fp,
                    CALIBRATION_GATES["support_specificity_min"],
                )
                - support_tn,
            ),
        },
        "structured_field_accuracy": {
            "current_correct": structured_correct,
            "total": witness_count,
            "minimum_corrections": max(
                0,
                required_successes(
                    witness_count,
                    CALIBRATION_GATES["structured_field_accuracy_min"],
                )
                - structured_correct,
            ),
        },
        "alignment_f1": {
            "true_positive": alignment_tp,
            "false_positive": alignment_fp,
            "false_negative": alignment_fn,
            "minimum_pair_replacement_corrections": f1_corrections(
                alignment_tp,
                alignment_fp,
                alignment_fn,
                CALIBRATION_GATES["alignment_f1_min"],
            ),
        },
        "equivalent_specificity": {
            "current_true_negative": equivalent_tn,
            "current_false_positive": equivalent_fp,
            "minimum_false_positive_to_true_negative_corrections": max(
                0,
                required_successes(
                    equivalent_tn + equivalent_fp,
                    CALIBRATION_GATES["equivalent_specificity_min"],
                )
                - equivalent_tn,
            ),
        },
        "field_diagnostic_f1": {
            "true_positive": field_tp,
            "false_positive": field_fp,
            "false_negative": field_fn,
            "minimum_single_field_corrections": f1_corrections(
                field_tp,
                field_fp,
                field_fn,
                CALIBRATION_GATES["field_diagnostic_f1_min"],
            ),
        },
        "relation_accuracy": {
            "current_correct": relation_correct,
            "total": relation_total,
            "minimum_corrections": max(
                0,
                required_successes(
                    relation_total,
                    CALIBRATION_GATES["relation_accuracy_min"],
                )
                - relation_correct,
            ),
        },
        "equivalence_partition_exact_case_rate": {
            "current_exact_cases": partition_exact,
            "total_cases": case_count,
            "minimum_case_corrections": max(
                0,
                required_successes(
                    case_count,
                    CALIBRATION_GATES["equivalence_partition_exact_case_rate_min"],
                )
                - partition_exact,
            ),
        },
        "unpaired_exact_case_rate": {
            "current_exact_cases": unpaired_exact,
            "total_cases": case_count,
            "minimum_case_corrections": max(
                0,
                required_successes(
                    case_count,
                    CALIBRATION_GATES["unpaired_exact_case_rate_min"],
                )
                - unpaired_exact,
            ),
        },
        "order_bias": {
            "current_disagreement_cases": order_disagreement,
            "total_cases": case_count,
            "maximum_allowed_disagreement_cases": math.floor(
                case_count * CALIBRATION_GATES["order_bias_max"]
            ),
            "minimum_disagreement_case_corrections": max(
                0,
                order_disagreement
                - math.floor(case_count * CALIBRATION_GATES["order_bias_max"]),
            ),
        },
    }


def write_v25_repair_diagnostic_non_acceptance(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v26_design_root: Path = DEFAULT_V26_PRESEMANTIC_DESIGN_ROOT,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    score_path = root / "diagnostic-score.json"
    terminal = _load_json(terminal_path, "v25 repair diagnostic terminal")
    score = _load_json(score_path, "v25 repair diagnostic score")
    if terminal.get("diagnostic_passed") is True or terminal.get("full_calibration_authorized") is True:
        raise JudgeV5CalibrationV25DiagnosticError(
            "cannot write non-acceptance for a passing v25 diagnostic"
        )

    if "counts" not in score:
        spec = _load_json(root / "repair-diagnostic-spec.json", "v25 repair diagnostic spec")
        truth = _load_json(Path(spec["v23_truth"]["path"]), "v23 truth")
        base = _load_json(root / "base-output.private.json", "v25 base output")
        canary = _load_json(root / "canary-output.private.json", "v25 canary output")
        score = _score_outputs(base_output=base, canary_output=canary, truth=truth)

    failed_checks = score.get("failed_checks") or [
        key for key, passed in score["checks"].items() if not passed
    ]
    usage = terminal.get("usage") or {}
    terminal_record = _record(terminal_path)
    score_record = _record(score_path)

    v26_design_root = v26_design_root.expanduser().resolve()
    v26_design_root.mkdir(parents=True, exist_ok=True)
    next_spec_path = v26_design_root / "next-experiment-spec.json"
    next_spec = {
        "schema_version": V26_PRESEMANTIC_DESIGN_VERSION,
        "state": "prepared_without_semantic_model_call",
        "created_at": (
            _load_json(next_spec_path, "v26 next experiment spec").get("created_at")
            if next_spec_path.exists()
            else now_iso()
        ),
        "predecessor_v25_terminal": terminal_record,
        "predecessor_v25_score": score_record,
        "semantic_attempt_authorized": False,
        "production_mutation_allowed": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "diagnostic_required_before_full_calibration": True,
        "proposed_protocol": {
            "summary": (
                "Structured-field verification must become an isolated field-by-field "
                "pass before relation/alignment, followed by side-free relation "
                "resolution only for support-positive and field-verified witnesses."
            ),
            "layers": [
                "pointwise proposition support with exact source spans",
                "pointwise structured-event field correctness with one independent decision per enum field",
                "alignment over support-positive and field-verified witness IDs only",
                "bounded side-free adjudication for base/canary disagreements",
            ],
            "forbidden_methods": [
                "embeddings",
                "semantic regex or keyword pruning",
                "non-LLM semantic matching",
                "verbal confidence routing",
                "majority voting",
            ],
        },
        "diagnostic_case_policy": {
            "smallest_authorized_shape": "12-18 opaque cases",
            "must_include": [
                "all v25 order-disagreement cases",
                "structured-field false-positive heavy cases",
                "the support-specificity false-positive case",
                "unpaired partition misses",
                "fresh balanced no-error and material-error controls",
            ],
            "must_predeclare_thresholds": True,
            "retry_policy": "zero silent retries; any failed or unknown-usage turn blocks that version",
        },
        "promotion_rule": (
            "Only a new immutable diagnostic that passes every frozen support, "
            "field, relation, equivalence, abstention, and order gate may authorize "
            "one fresh full development calibration."
        ),
    }
    _write_immutable(next_spec_path, next_spec)
    next_terminal_path = v26_design_root / "terminal.json"
    next_terminal = {
        "schema_version": V26_PRESEMANTIC_DESIGN_VERSION,
        "state": "inactive",
        "terminal_reason": "inactive_incomplete_recovery_required",
        "babysitter_status": "inactive_incomplete_recovery_required",
        "created_at": (
            _load_json(next_terminal_path, "v26 presemantic terminal").get("created_at")
            if next_terminal_path.exists()
            else now_iso()
        ),
        "semantic_attempt_started": False,
        "semantic_attempt_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "predecessor_v25_terminal": terminal_record,
        "next_experiment_spec": _record(next_spec_path),
    }
    _write_immutable(next_terminal_path, next_terminal)

    path = root / "non-acceptance.json"
    non_acceptance = {
        "schema_version": V25_REPAIR_DIAGNOSTIC_NON_ACCEPTANCE_VERSION,
        "state": "inactive",
        "terminal_reason": "inactive_incomplete_recovery_required",
        "babysitter_status": "inactive_incomplete_recovery_required",
        "created_at": (
            _load_json(path, "v25 non-acceptance").get("created_at")
            if path.exists()
            else now_iso()
        ),
        "acceptance_receipt_allowed": False,
        "blocker_class": "development_judge_protocol_quality_gate_failed",
        "development_terminal_reason": "v25_repair_diagnostic_quality_gate_not_passed",
        "semantic_attempt_completed": True,
        "semantic_attempt_started": True,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "terminal_semantics_correction": {
            "immutable_terminal_record": terminal_record,
            "do_not_treat_terminal_state_completed_as_overall_goal_completion": True,
            "supervisor_status": "inactive_incomplete_recovery_required",
        },
        "usage_status": terminal.get("usage_status"),
        "usage": usage,
        "v25_score": {
            "record": score_record,
            "metrics": score["metrics"],
            "failed_checks": sorted(failed_checks),
            "minimum_corrections": _minimum_corrections_for_score(score),
        },
        "root_cause_assessment": {
            "single_root_cause": False,
            "primary": "structured_field_verification_still_overbroad_and_order_sensitive",
            "secondary": [
                "relation and equivalence specificity remain below threshold",
                "unpaired partitioning is not stable enough",
                "support specificity has a residual false positive",
            ],
        },
        "required_next_artifact_path": str(next_spec_path),
        "prepared_next_experiment": _record(next_spec_path),
        "prepared_next_terminal": _record(next_terminal_path),
    }
    _write_immutable(path, non_acceptance)
    return non_acceptance


def _write_failure_terminal(
    *, root: Path, spec_path: Path, turn_name: Optional[str], error_class: str
) -> dict[str, Any]:
    attempts = _attempt_records(root)
    known = {field: 0 for field in USAGE_FIELDS}
    known_turns = unknown_turns = 0
    partial = False
    for attempt in attempts:
        sidecar_record = attempt.get("sidecar")
        if not isinstance(sidecar_record, dict):
            if attempt.get("capacity") is not None or attempt.get("output") is not None:
                partial = True
            continue
        sidecar = _load_json(Path(sidecar_record["path"]), "failed v25 sidecar")
        try:
            usage = _validate_usage(sidecar)
        except Exception:
            unknown_turns += 1
            continue
        known_turns += 1
        for field in USAGE_FIELDS:
            known[field] += usage[field]
    accounting_complete = not partial and unknown_turns == 0
    failure = {
        "schema_version": V25_REPAIR_DIAGNOSTIC_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "full_calibration_authorized": False,
        "accounting_complete": accounting_complete,
        "usage_status": "complete" if accounting_complete else "unknown",
        "usage": known if accounting_complete else None,
        "known_usage_lower_bound": known,
        "known_usage_turn_count": known_turns,
        "unknown_usage_turn_count": unknown_turns,
        "partial_attempt_without_sidecar": partial,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable_json(failure_path, failure)
    terminal = {
        "schema_version": V25_REPAIR_DIAGNOSTIC_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "spec_sha256": _sha256_file(spec_path),
        "failure": _record(failure_path),
        "diagnostic_passed": False,
        "full_calibration_authorized": False,
        "selection_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_allowed": False,
        "accounting_complete": accounting_complete,
        "usage_status": "complete" if accounting_complete else "unknown",
        "usage": known if accounting_complete else None,
    }
    _write_immutable_json(root / "terminal.json", terminal)
    return terminal


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    def inner() -> CodexAppServerClient:
        return CodexAppServerClient(
            command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
        )

    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=inner
    )


async def run_v25_repair_diagnostic(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    v23_root: Path = DEFAULT_V23_ROOT,
    presemantic_root: Path = DEFAULT_V25_PRESEMANTIC_ROOT,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout_seconds: float = 1200.0,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        terminal = _load_json(terminal_path, "v25 repair terminal")
        if (
            terminal.get("diagnostic_passed") is False
            and terminal.get("full_calibration_authorized") is False
        ):
            write_v25_repair_diagnostic_non_acceptance(
                output_dir=root,
                v26_design_root=root.parent
                / "judge-calibration-v5_4-v26-presemantic-design",
            )
        return terminal
    frozen = freeze_v25_repair_diagnostic(
        output_dir=root,
        v23_root=v23_root,
        presemantic_root=presemantic_root,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
    )
    policy_path = frozen["capacity_policy"]
    factory = client_factory or _client_factory
    outputs: dict[str, dict[str, Any]] = {}
    sidecars: list[dict[str, Any]] = []
    adopted: dict[str, bool] = {}
    current_turn: Optional[str] = None
    try:
        async with factory(policy_path) as client:
            for turn_name in _turn_names():
                current_turn = turn_name
                output, sidecar, was_adopted = await _get_or_run_turn(
                    client=client,
                    turn_name=turn_name,
                    request=frozen["turn_requests"][turn_name],
                    model=model,
                    effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    policy_path=policy_path,
                )
                outputs[turn_name] = output
                sidecars.append(sidecar)
                adopted[turn_name] = was_adopted
        base = _merge_outputs([outputs[name] for name in _turn_names() if "_base_" in name])
        canary = _merge_outputs([outputs[name] for name in _turn_names() if "_canary_" in name])
        base_path = root / "base-output.private.json"
        canary_path = root / "canary-output.private.json"
        _write_immutable_json(base_path, base)
        _write_immutable_json(canary_path, canary)
        score = _score_outputs(base_output=base, canary_output=canary, truth=frozen["truth"])
        score_path = root / "diagnostic-score.json"
        _write_immutable_json(score_path, score)
        accounting = _aggregate_usage(sidecars)
        passed = bool(score["passed"])
        usage = accounting["usage"]
        terminal = {
            "schema_version": V25_REPAIR_DIAGNOSTIC_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v25_repair_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v25_repair_diagnostic_passed_full_calibration_authorized"
                if passed
                else "v25_repair_diagnostic_quality_gate_not_passed"
            ),
            "babysitter_status": (
                "v25_repair_diagnostic_passed_full_calibration_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "spec_sha256": _sha256_file(frozen["spec_path"]),
            "diagnostic_passed": passed,
            "full_calibration_authorized": passed,
            "selection_authorized": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_attempt_started": True,
            "semantic_retry_allowed": False,
            "semantic_retry_count": 0,
            "total_tokens": usage["total_tokens"],
            "input_tokens": usage["input_tokens"],
            "cached_input_tokens": usage["cached_input_tokens"],
            "output_tokens": usage["output_tokens"],
            "reasoning_output_tokens": usage["reasoning_output_tokens"],
            "score": _record(score_path),
            "base_output": _record(base_path),
            "canary_output": _record(canary_path),
            "attempts": _attempt_records(root),
            "completed_checkpoint_adoptions": adopted,
            **accounting,
        }
        _write_immutable_json(terminal_path, terminal)
        if not passed:
            write_v25_repair_diagnostic_non_acceptance(
                output_dir=root,
                v26_design_root=root.parent
                / "judge-calibration-v5_4-v26-presemantic-design",
            )
        return terminal
    except JudgeV5CalibrationV25DiagnosticAttemptFailed as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            turn_name=exc.turn_name,
            error_class=exc.error_class,
        )
    except Exception as exc:
        return _write_failure_terminal(
            root=root,
            spec_path=frozen["spec_path"],
            turn_name=current_turn,
            error_class=type(exc).__name__,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v25 repair diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--v23-root", default=str(DEFAULT_V23_ROOT))
    parser.add_argument("--presemantic-root", default=str(DEFAULT_V25_PRESEMANTIC_ROOT))
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning-effort", default="high")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v25_repair_diagnostic(
            output_dir=Path(args.output_dir),
            v23_root=Path(args.v23_root),
            presemantic_root=Path(args.presemantic_root),
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal.get("state"),
                "terminal_reason": terminal.get("terminal_reason"),
                "diagnostic_passed": terminal.get("diagnostic_passed"),
                "full_calibration_authorized": terminal.get("full_calibration_authorized"),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
