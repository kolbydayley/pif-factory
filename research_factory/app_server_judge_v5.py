from __future__ import annotations

"""Side-free pointwise support and neutral checklist alignment for pipeline-v5."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_judge_v5_fixture import (
    compact_empty_event_fields,
    load_fixture_truth_audit,
)
from .app_server_llm_judge import (
    SUPPORT_VERDICTS,
    make_shared_witness_pool,
    validate_app_server_output_schema_subset,
    validate_shared_witness_pool,
)


POINTWISE_INPUT_VERSION = "pif_app_server_v5_pointwise_support_input_v2"
POINTWISE_OUTPUT_VERSION = "pif_app_server_v5_pointwise_support_output_v2"
ALIGNMENT_INPUT_VERSION = "pif_app_server_v5_neutral_alignment_input_v5"
ALIGNMENT_OUTPUT_VERSION = "pif_app_server_v5_neutral_alignment_output_v5"
ADJUDICATION_INPUT_VERSION = "pif_app_server_v5_disagreement_adjudication_input_v5"
PROTOCOL_VERSION = "pif_app_server_judge_protocol_v5_4"
DIAGNOSTIC_FIXTURE_VERSION = "pif_judge_v5_diagnostic_fixture_v3"
DIAGNOSTIC_FIXTURE_PATCH_VERSION = "pif_judge_v5_diagnostic_fixture_patch_v3"
DIAGNOSTIC_TRUTH_VERSION = "pif_judge_v5_diagnostic_truth_v3"
DEFAULT_DIAGNOSTIC_FIXTURE_PATH = (
    Path(__file__).resolve().parent / "evaluation/judge_v5_diagnostic_v3.json"
)

STRUCTURED_FIELD_VERDICTS = ("correct", "incorrect", "abstain")
CHECKLIST_DECISIONS = ("same", "different", "abstain")
ALIGNMENT_RELATIONS = ("equivalent", "partial", "non_equivalent", "abstain")


class JudgeV5ProtocolError(ValueError):
    """A v5 protocol input/output violates its frozen semantic-control boundary."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, *, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JudgeV5ProtocolError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise JudgeV5ProtocolError(f"{purpose} is not an object")
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _apply_fixture_truth_patch(
    *, patch: Mapping[str, Any], patch_path: Path
) -> dict[str, Any]:
    base_record = patch.get("base_fixture")
    if not isinstance(base_record, Mapping):
        raise JudgeV5ProtocolError("v5 diagnostic fixture patch has no base")
    relative = base_record.get("path")
    expected_sha = base_record.get("sha256")
    if (
        not isinstance(relative, str)
        or not relative
        or Path(relative).is_absolute()
        or not isinstance(expected_sha, str)
        or len(expected_sha) != 64
    ):
        raise JudgeV5ProtocolError("v5 diagnostic fixture patch base is invalid")
    base_path = (patch_path.parent / relative).resolve()
    try:
        base_path.relative_to(patch_path.parent.resolve())
    except ValueError as exc:
        raise JudgeV5ProtocolError("v5 diagnostic fixture base escapes directory") from exc
    if not base_path.is_file() or _sha256_file(base_path) != expected_sha:
        raise JudgeV5ProtocolError("v5 diagnostic fixture base hash drifted")
    fixture = deepcopy(_load_json(base_path, purpose="v5 diagnostic base fixture"))
    corrections = patch.get("corrections")
    if (
        patch.get("effective_schema_version") != DIAGNOSTIC_FIXTURE_VERSION
        or patch.get("predecessor_case_content_reused") is not True
        or patch.get("semantic_truth_correction_count") != 1
        or not isinstance(corrections, list)
        or len(corrections) != 1
    ):
        raise JudgeV5ProtocolError("v5 diagnostic fixture patch contract drifted")
    correction = corrections[0]
    operations = correction.get("operations") if isinstance(correction, Mapping) else None
    if (
        correction.get("case_key") != "diag2_unsupported_inference"
        or correction.get("field") != "event_type"
        or not isinstance(operations, list)
        or len(operations) != 2
    ):
        raise JudgeV5ProtocolError("v5 diagnostic fixture correction drifted")
    case = next(
        (
            item
            for item in fixture.get("cases", [])
            if item.get("case_key") == correction["case_key"]
        ),
        None,
    )
    if not isinstance(case, dict):
        raise JudgeV5ProtocolError("v5 diagnostic fixture correction case is missing")
    expected_paths = {
        "expected.field_issues_b",
        "expected.mismatch_fields",
    }
    if {item.get("path") for item in operations if isinstance(item, Mapping)} != expected_paths:
        raise JudgeV5ProtocolError("v5 diagnostic fixture correction paths drifted")
    for operation in operations:
        path = str(operation["path"])
        key = path.removeprefix("expected.")
        if case["expected"].get(key) != operation.get("old"):
            raise JudgeV5ProtocolError("v5 diagnostic fixture correction old truth drifted")
        case["expected"][key] = deepcopy(operation.get("new"))
    fixture["schema_version"] = DIAGNOSTIC_FIXTURE_VERSION
    fixture["seed"] = patch.get("seed")
    fixture["predecessor_case_content_reused"] = True
    fixture["fixture_patch_path"] = str(patch_path)
    fixture["base_fixture_path"] = str(base_path)
    return fixture


def mismatch_checklist_fields() -> tuple[str, ...]:
    audit = load_fixture_truth_audit()
    return tuple(item["field"] for item in audit["mismatch_checklist"])


CHECKLIST_FIELDS = mismatch_checklist_fields()
ROOT_CONFLICT_FIELDS = frozenset(
    {
        "actor",
        "attribution",
        "causal_mechanism",
        "certainty",
        "event_type",
        "metric",
        "negation",
        "reported_actor",
        "speaker",
        "stance",
        "target",
        "temporal_horizon",
    }
)


def load_v5_diagnostic_fixture(
    path: Path = DEFAULT_DIAGNOSTIC_FIXTURE_PATH,
) -> dict[str, Any]:
    source = path.expanduser().resolve()
    fixture = _load_json(source, purpose="v5 diagnostic fixture")
    if fixture.get("schema_version") == DIAGNOSTIC_FIXTURE_PATCH_VERSION:
        fixture = _apply_fixture_truth_patch(patch=fixture, patch_path=source)
    cases = fixture.get("cases")
    if (
        fixture.get("schema_version") != DIAGNOSTIC_FIXTURE_VERSION
        or fixture.get("case_count") != 18
        or fixture.get("predecessor_case_content_reused") is not True
        or not isinstance(cases, list)
        or len(cases) != 18
    ):
        raise JudgeV5ProtocolError("v5 diagnostic fixture must contain exactly 18 cases")
    case_keys = [case.get("case_key") for case in cases if isinstance(case, dict)]
    if len(case_keys) != 18 or len(set(case_keys)) != 18:
        raise JudgeV5ProtocolError("v5 diagnostic case keys are invalid")
    focus = {str(case.get("focus_field")) for case in cases}
    if not set(CHECKLIST_FIELDS) <= focus:
        raise JudgeV5ProtocolError("v5 diagnostic does not cover all mismatch fields")
    canary = fixture.get("canary_case_keys")
    if (
        not isinstance(canary, list)
        or len(canary) != 6
        or len(set(canary)) != 6
        or not set(canary) <= set(case_keys)
    ):
        raise JudgeV5ProtocolError("v5 permutation canary must contain six cases")
    required_expected = {
        "proposition_a",
        "proposition_b",
        "structured_a",
        "structured_b",
        "field_issues_a",
        "field_issues_b",
        "relation",
        "mismatch_fields",
    }
    for case in cases:
        if (
            not isinstance(case.get("source_excerpt"), str)
            or not case["source_excerpt"]
            or not isinstance(case.get("event_a"), dict)
            or not isinstance(case.get("event_b"), dict)
            or not isinstance(case.get("expected"), dict)
            or set(case["expected"]) != required_expected
            or case["expected"]["proposition_a"] not in SUPPORT_VERDICTS
            or case["expected"]["proposition_b"] not in SUPPORT_VERDICTS
            or case["expected"]["structured_a"] not in STRUCTURED_FIELD_VERDICTS
            or case["expected"]["structured_b"] not in STRUCTURED_FIELD_VERDICTS
            or case["expected"]["relation"] not in ALIGNMENT_RELATIONS
            or any(
                field not in CHECKLIST_FIELDS
                for field in case["expected"]["mismatch_fields"]
            )
        ):
            raise JudgeV5ProtocolError("v5 diagnostic case truth is malformed")
    return fixture


def make_v5_diagnostic_pool(
    *, fixture_path: Path = DEFAULT_DIAGNOSTIC_FIXTURE_PATH
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    fixture = load_v5_diagnostic_fixture(fixture_path)
    raw_cases = []
    for case in fixture["cases"]:
        raw_cases.append(
            {
                "case_key": case["case_key"],
                "source_excerpt": case["source_excerpt"],
                "event_set_a": [
                    {
                        "event": case["event_a"],
                        "provenance": {"fixture_side": "a"},
                    }
                ],
                "event_set_b": [
                    {
                        "event": case["event_b"],
                        "provenance": {"fixture_side": "b"},
                    }
                ],
                "provenance": {
                    "focus_field": case["focus_field"],
                    "diagnostic_fixture": DIAGNOSTIC_FIXTURE_VERSION,
                },
            }
        )
    pool, mapping = make_shared_witness_pool(raw_cases, seed=fixture["seed"])
    mapping_by_key = {item["case_key"]: item for item in mapping["cases"]}
    pool_by_id = {item["case_id"]: item for item in pool["cases"]}
    expected_cases = {}
    canary_case_ids = []
    for case in fixture["cases"]:
        mapped = mapping_by_key[case["case_key"]]
        witness_by_side = {
            item["canonical_side"]: item["witness_id"] for item in mapped["witnesses"]
        }
        case_id = mapped["case_id"]
        first = witness_by_side["a"]
        second = witness_by_side["b"]
        expected = case["expected"]
        expected_cases[case_id] = {
            "case_key": case["case_key"],
            "focus_field": case["focus_field"],
            "proposition": {
                first: expected["proposition_a"],
                second: expected["proposition_b"],
            },
            "structured_fields": {
                first: expected["structured_a"],
                second: expected["structured_b"],
            },
            "field_issues": {
                first: expected["field_issues_a"],
                second: expected["field_issues_b"],
            },
            "pair_witness_ids": sorted((first, second)),
            "relation": expected["relation"],
            "mismatch_fields": expected["mismatch_fields"],
            "equivalence_groups": (
                [sorted((first, second))]
                if expected["relation"] == "equivalent"
                else [[first], [second]]
            ),
        }
        if case["case_key"] in fixture["canary_case_keys"]:
            canary_case_ids.append(case_id)
        if case_id not in pool_by_id:
            raise JudgeV5ProtocolError("diagnostic pool case mapping drifted")
    expected_truth = {
        "schema_version": DIAGNOSTIC_TRUTH_VERSION,
        "fixture_path": str(fixture_path.expanduser().resolve()),
        "cases": expected_cases,
        "canary_case_ids": sorted(canary_case_ids),
        "case_count": 18,
        "legacy_joint_support_labels_used": False,
    }
    return pool, mapping, expected_truth


def _all_case_witnesses(case: Mapping[str, Any]) -> list[dict[str, Any]]:
    witnesses = []
    for side in ("a", "b"):
        values = case.get("event_set_%s" % side)
        if not isinstance(values, list):
            raise JudgeV5ProtocolError("shared witness case has an invalid event set")
        witnesses.extend(deepcopy(values))
    return sorted(witnesses, key=lambda item: str(item.get("witness_id")))


def build_pointwise_support_input(pool: Mapping[str, Any]) -> dict[str, Any]:
    errors = validate_shared_witness_pool(pool)
    if errors:
        raise JudgeV5ProtocolError("invalid witness pool: %s" % "; ".join(errors))
    units = []
    for case in pool["cases"]:
        for witness in _all_case_witnesses(case):
            event = compact_empty_event_fields(witness["event"])
            claim_text = event.get("claim_text")
            if not isinstance(claim_text, str) or not claim_text:
                raise JudgeV5ProtocolError(
                    "pointwise support requires an explicit claim_text proposition"
                )
            units.append(
                {
                    "case_id": case["case_id"],
                    "witness_id": witness["witness_id"],
                    "source_excerpt": case["source_excerpt"],
                    "proposition": {"claim_text": claim_text},
                    "structured_event": event,
                }
            )
    return {
        "schema_version": POINTWISE_INPUT_VERSION,
        "units": units,
        "side_labels_present": False,
        "system_identity_present": False,
        "empty_event_fields_omitted_only": True,
    }


def pointwise_support_output_schema(pointwise_input: Mapping[str, Any]) -> dict[str, Any]:
    units = pointwise_input.get("units")
    if not isinstance(units, list) or not units:
        raise JudgeV5ProtocolError("pointwise support input has no units")
    case_ids = sorted({str(item["case_id"]) for item in units})
    witness_ids = [str(item["witness_id"]) for item in units]
    fields = list(CHECKLIST_FIELDS)
    evidence_spans = {
        "type": "array",
        "maxItems": 4,
        "items": {"type": "string", "minLength": 1, "maxLength": 1000},
    }
    row = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "case_id",
            "witness_id",
            "proposition_verdict",
            "proposition_evidence_spans",
            "proposition_rationale",
            "structured_field_verdict",
            "field_issue_fields",
            "field_evidence_spans",
            "field_rationale",
        ],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "witness_id": {"type": "string", "enum": witness_ids},
            "proposition_verdict": {
                "type": "string",
                "enum": list(SUPPORT_VERDICTS),
            },
            "proposition_evidence_spans": evidence_spans,
            "proposition_rationale": {
                "type": "string",
                "minLength": 1,
                "maxLength": 600,
            },
            "structured_field_verdict": {
                "type": "string",
                "enum": list(STRUCTURED_FIELD_VERDICTS),
            },
            "field_issue_fields": {
                "type": "array",
                "maxItems": len(fields),
                "items": {"type": "string", "enum": fields},
            },
            "field_evidence_spans": evidence_spans,
            "field_rationale": {
                "type": "string",
                "minLength": 1,
                "maxLength": 600,
            },
        },
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["units"],
        "properties": {
            "units": {
                "type": "array",
                "minItems": len(units),
                "maxItems": len(units),
                "items": row,
            }
        },
    }
    if validate_app_server_output_schema_subset(schema):
        raise JudgeV5ProtocolError("pointwise schema exceeds Structured Outputs subset")
    return schema


def pointwise_support_base_instructions() -> str:
    return (
        "You are a side-free pointwise factual-support evaluator. Each unit is an "
        "independent anonymous proposition, complete structured event, and source excerpt; "
        "no system or comparison side is present. First judge proposition_source_support "
        "using only proposition.claim_text against the source. Do not let speaker, actor, "
        "reported_actor, source_kind, event_type, evidence, or any other structured field "
        "change the proposition verdict. Every material claim_text proposition must be "
        "entailed. Harmless paraphrase and resolved coreference pass. Exact text or an exact "
        "evidence span alone does not make an unsupported claim supported. Separately judge "
        "the complete structured_event: every populated semantic field must be licensed by "
        "the source and none may contradict it; empty or omitted fields are not errors. Cite "
        "exact source substrings. Use abstain only when the excerpt cannot resolve the "
        "decision. Do not infer origin or use keyword, regex, token-overlap, or embeddings."
    )


def build_pointwise_support_prompt(pointwise_input: Mapping[str, Any]) -> str:
    if pointwise_input.get("schema_version") != POINTWISE_INPUT_VERSION:
        raise JudgeV5ProtocolError("unsupported pointwise input")
    instructions = (
        "Return every case_id/witness_id exactly once. A supported proposition requires "
        "one or more exact proposition_evidence_spans. For unsupported or abstain, spans "
        "may identify the closest relevant or contradictory source text but must remain exact. "
        "structured_field_verdict=correct requires field_issue_fields=[]; incorrect requires "
        "one or more minimal issue fields; add unsupported_inference only when claim_text "
        "itself adds an ungrounded assertion, never merely because a structured field is "
        "wrong. Abstain requires no asserted issue fields. Evaluate the explicit proposition "
        "and complete structured event independently."
    )
    packet = {"units": pointwise_input["units"]}
    return instructions + "\n\n# Side-free pointwise units\n" + _canonical_json(packet) + "\n"


def validate_pointwise_support_output(
    output: Any, pointwise_input: Mapping[str, Any]
) -> list[str]:
    if not isinstance(output, dict) or set(output) != {"units"} or not isinstance(
        output.get("units"), list
    ):
        return ["invalid_pointwise_output_root"]
    expected = {
        (str(item["case_id"]), str(item["witness_id"])): item
        for item in pointwise_input.get("units") or []
    }
    errors = []
    seen = set()
    required_keys = {
        "case_id",
        "witness_id",
        "proposition_verdict",
        "proposition_evidence_spans",
        "proposition_rationale",
        "structured_field_verdict",
        "field_issue_fields",
        "field_evidence_spans",
        "field_rationale",
    }
    for index, row in enumerate(output["units"]):
        prefix = "unit_%d" % index
        if not isinstance(row, dict) or set(row) != required_keys:
            errors.append(prefix + "_invalid_shape")
            continue
        key = (str(row.get("case_id")), str(row.get("witness_id")))
        if key not in expected or key in seen:
            errors.append(prefix + "_invalid_or_duplicate_id")
            continue
        seen.add(key)
        source = expected[key]["source_excerpt"]
        proposition = row.get("proposition_verdict")
        structured = row.get("structured_field_verdict")
        prop_spans = row.get("proposition_evidence_spans")
        field_spans = row.get("field_evidence_spans")
        issues = row.get("field_issue_fields")
        for label, spans in (("proposition", prop_spans), ("field", field_spans)):
            if (
                not isinstance(spans, list)
                or len(spans) > 4
                or len(spans) != len(set(spans))
                or any(
                    not isinstance(span, str)
                    or not span
                    or len(span) > 1000
                    or span not in source
                    for span in spans
                )
            ):
                errors.append(prefix + "_%s_evidence_not_exact" % label)
        if proposition not in SUPPORT_VERDICTS:
            errors.append(prefix + "_invalid_proposition_verdict")
        elif proposition == "supported" and not prop_spans:
            errors.append(prefix + "_supported_without_evidence")
        if structured not in STRUCTURED_FIELD_VERDICTS:
            errors.append(prefix + "_invalid_structured_field_verdict")
        if (
            not isinstance(issues, list)
            or len(issues) != len(set(issues))
            or any(field not in CHECKLIST_FIELDS for field in issues)
        ):
            errors.append(prefix + "_invalid_field_issues")
        elif structured == "incorrect" and not issues:
            errors.append(prefix + "_incorrect_without_field_issue")
        elif structured in {"correct", "abstain"} and issues:
            errors.append(prefix + "_nonincorrect_with_field_issue")
        for rationale_key in ("proposition_rationale", "field_rationale"):
            rationale = row.get(rationale_key)
            if not isinstance(rationale, str) or not rationale or len(rationale) > 600:
                errors.append(prefix + "_invalid_%s" % rationale_key)
    if seen != set(expected):
        errors.append("pointwise_unit_coverage_mismatch")
    return errors


def freeze_support_receipts(
    output: Mapping[str, Any], pointwise_input: Mapping[str, Any]
) -> dict[str, Any]:
    errors = validate_pointwise_support_output(output, pointwise_input)
    if errors:
        raise JudgeV5ProtocolError("invalid support output: %s" % "; ".join(errors))
    rows = sorted(
        deepcopy(output["units"]),
        key=lambda item: (item["case_id"], item["witness_id"]),
    )
    return {
        "schema_version": POINTWISE_OUTPUT_VERSION,
        "units": rows,
        "side_free": True,
        "claim_support_and_field_correctness_separate": True,
    }


def _support_by_id(receipts: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    if receipts.get("schema_version") != POINTWISE_OUTPUT_VERSION:
        raise JudgeV5ProtocolError("unsupported frozen support receipt")
    rows = receipts.get("units")
    if not isinstance(rows, list):
        raise JudgeV5ProtocolError("frozen support receipt units are malformed")
    by_id = {str(item.get("witness_id")): item for item in rows if isinstance(item, dict)}
    if len(by_id) != len(rows):
        raise JudgeV5ProtocolError("frozen support receipt IDs are not unique")
    return by_id


def build_neutral_alignment_input(
    pool: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
    *,
    case_ids: Optional[Sequence[str]] = None,
    permutation: str = "base",
) -> dict[str, Any]:
    errors = validate_shared_witness_pool(pool)
    if errors:
        raise JudgeV5ProtocolError("invalid witness pool: %s" % "; ".join(errors))
    if permutation not in {"base", "balanced_canary"}:
        raise JudgeV5ProtocolError("unsupported alignment permutation")
    support = _support_by_id(support_receipts)
    selected_ids = set(case_ids) if case_ids is not None else None
    cases = []
    selected_cases = [
        case
        for case in pool["cases"]
        if selected_ids is None or case["case_id"] in selected_ids
    ]
    if permutation == "balanced_canary":
        selected_cases.reverse()
    for case in selected_cases:
        witnesses = _all_case_witnesses(case)
        if permutation == "balanced_canary":
            witnesses.reverse()
        rendered = []
        for witness in witnesses:
            witness_id = witness["witness_id"]
            receipt = support.get(witness_id)
            if receipt is None or receipt.get("case_id") != case["case_id"]:
                raise JudgeV5ProtocolError("support receipt coverage does not match pool")
            rendered.append(
                {
                    "witness_id": witness_id,
                    "event": compact_empty_event_fields(witness["event"]),
                    "support_receipt": {
                        "proposition_verdict": receipt["proposition_verdict"],
                        "proposition_evidence_spans": receipt[
                            "proposition_evidence_spans"
                        ],
                        "structured_field_verdict": receipt[
                            "structured_field_verdict"
                        ],
                        "field_issue_fields": receipt["field_issue_fields"],
                        "field_evidence_spans": receipt["field_evidence_spans"],
                    },
                }
            )
        cases.append(
            {
                "case_id": case["case_id"],
                "source_excerpt": case["source_excerpt"],
                "witnesses": rendered,
            }
        )
    if selected_ids is not None and {case["case_id"] for case in cases} != selected_ids:
        raise JudgeV5ProtocolError("alignment canary case coverage drifted")
    field_order = list(CHECKLIST_FIELDS)
    decision_order = list(CHECKLIST_DECISIONS)
    return {
        "schema_version": ALIGNMENT_INPUT_VERSION,
        "permutation": permutation,
        "cases": cases,
        "checklist_field_order": field_order,
        "checklist_decision_order": decision_order,
        "side_labels_present": False,
        "system_identity_present": False,
        "support_receipts_frozen": True,
        "permuted_axes": (
            ["anonymous_case_order", "anonymous_witness_order"]
            if permutation == "balanced_canary"
            else []
        ),
        "rubric_and_decision_order_fixed": True,
    }


def neutral_alignment_output_schema(alignment_input: Mapping[str, Any]) -> dict[str, Any]:
    cases = alignment_input.get("cases")
    if not isinstance(cases, list) or not cases:
        raise JudgeV5ProtocolError("neutral alignment input has no cases")
    case_ids = [str(case["case_id"]) for case in cases]
    witness_ids = [
        str(witness["witness_id"])
        for case in cases
        for witness in case["witnesses"]
    ]
    field_order = alignment_input.get("checklist_field_order")
    decision_order = alignment_input.get("checklist_decision_order")
    if set(field_order or []) != set(CHECKLIST_FIELDS) or set(decision_order or []) != set(
        CHECKLIST_DECISIONS
    ):
        raise JudgeV5ProtocolError("alignment rubric permutation is malformed")
    witness_schema = {"type": "string", "enum": witness_ids}
    source_evidence_spans = {
        "type": "array",
        "maxItems": 4,
        "items": {"type": "string", "minLength": 1, "maxLength": 1000},
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
            "field": {"type": "string", "enum": list(field_order)},
            "decision": {"type": "string", "enum": list(decision_order)},
            "source_evidence_spans": source_evidence_spans,
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
            "witness_ids": {
                "type": "array",
                "minItems": 1,
                "items": witness_schema,
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 500},
        },
    }
    case_result = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "case_id",
            "equivalence_groups",
            "alignment_pairs",
            "unpaired_witness_ids",
        ],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "equivalence_groups": {"type": "array", "items": group},
            "alignment_pairs": {"type": "array", "items": pair},
            "unpaired_witness_ids": {"type": "array", "items": witness_schema},
        },
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["cases"],
        "properties": {
            "cases": {
                "type": "array",
                "minItems": len(cases),
                "maxItems": len(cases),
                "items": case_result,
            }
        },
    }
    if validate_app_server_output_schema_subset(schema):
        raise JudgeV5ProtocolError("alignment schema exceeds Structured Outputs subset")
    return schema


def neutral_alignment_base_instructions() -> str:
    return (
        "You are an origin-neutral semantic alignment evaluator. All witnesses are pooled "
        "under opaque IDs; there are no systems or preferred sides. Frozen pointwise support "
        "receipts are evidence, not labels to override. Partition witnesses by full semantic "
        "equivalence, then form one-to-one corresponding pairs or leave witnesses unpaired. "
        "For every pair complete every checklist row independently with same, different, or "
        "abstain. For each row, source_evidence_spans may contain only verbatim substrings "
        "of source_excerpt; use [] when the relevant conflicting content exists only in a "
        "witness. Never copy witness/event text into source_evidence_spans. Cite exactly both "
        "paired opaque IDs in witness_evidence_ids. The unsupported_inference row is anchored "
        "to frozen proposition verdicts: use different exactly when one verdict is supported "
        "and the other unsupported; use same when the two verdicts match; use abstain when "
        "either verdict abstains. Structured-field errors alone never change that row. Apply "
        "minimal truth-conditional root differences: "
        "direct material conflicts are non_equivalent; merge/split-only differences are "
        "partial with event_boundary plus evidence; an added ungrounded assertion requires "
        "its specific root field plus unsupported_inference. event_boundary is different "
        "only when witnesses merge or split source propositions or otherwise change event "
        "scope; it must always co-occur with evidence=different. A witness-only unsupported "
        "clause inside one event is not an event-boundary change. Compare event_type "
        "categories directly: a measurement claim and a capability claim differ in "
        "event_type even when one proposition is unsupported. Minimal means omit downstream "
        "duplicates, not a directly changed category. Do not infer origin, use verbal "
        "confidence, vote, or rely on keyword, regex, token-overlap, or embeddings."
    )


def build_neutral_alignment_prompt(alignment_input: Mapping[str, Any]) -> str:
    if alignment_input.get("schema_version") != ALIGNMENT_INPUT_VERSION:
        raise JudgeV5ProtocolError("unsupported alignment input")
    audit = load_fixture_truth_audit()
    rubric = [
        item
        for field in alignment_input["checklist_field_order"]
        for item in audit["mismatch_checklist"]
        if item["field"] == field
    ]
    instructions = (
        "Return every case exactly once. equivalence_groups must be a disjoint partition of "
        "all witness IDs. Every witness must appear exactly once in either one alignment pair "
        "or unpaired_witness_ids. Every pair checklist must contain the supplied 15 fields "
        "exactly once and in the supplied order. Equivalent requires every row same. Partial "
        "is reserved for exactly event_boundary+evidence merge/split differences. Any "
        "unresolved material checklist row requires relation=abstain. Each checklist row must "
        "cite both pair witness IDs. source_evidence_spans are source-only and must be exact; "
        "when no exact source substring supports a witness-only conflict, return an empty list. "
        "For unsupported_inference, obey the frozen proposition-verdict consistency rule exactly. "
        "Never mark event_boundary different unless evidence is also different and the "
        "witnesses genuinely merge or split source-proposition scope. Added unsupported claims "
        "remain one event unless a separate merge/split exists; mark every directly altered "
        "root category such as event_type or target instead."
    )
    packet = {
        "rubric": rubric,
        "mismatch_precedence": audit["mismatch_precedence"],
        "cases": alignment_input["cases"],
    }
    return instructions + "\n\n# Neutral pooled cases\n" + _canonical_json(packet) + "\n"


def expected_relation_from_checklist(checklist: Sequence[Mapping[str, Any]]) -> str:
    decisions = {str(item.get("field")): item.get("decision") for item in checklist}
    if set(decisions) != set(CHECKLIST_FIELDS):
        raise JudgeV5ProtocolError("checklist field coverage is incomplete")
    if any(value == "abstain" for value in decisions.values()):
        return "abstain"
    different = {field for field, value in decisions.items() if value == "different"}
    if not different:
        return "equivalent"
    if different == {"event_boundary", "evidence"}:
        return "partial"
    return "non_equivalent"


def project_mismatch_fields(checklist: Sequence[Mapping[str, Any]]) -> list[str]:
    decisions = {str(item.get("field")): item.get("decision") for item in checklist}
    if set(decisions) != set(CHECKLIST_FIELDS):
        raise JudgeV5ProtocolError("cannot project an incomplete mismatch checklist")
    return [field for field in CHECKLIST_FIELDS if decisions[field] == "different"]


def validate_neutral_alignment_output(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    if not isinstance(output, dict) or set(output) != {"cases"} or not isinstance(
        output.get("cases"), list
    ):
        return ["invalid_alignment_output_root"]
    expected = {str(case["case_id"]): case for case in alignment_input.get("cases") or []}
    field_order = list(alignment_input.get("checklist_field_order") or [])
    errors = []
    seen_cases = set()
    for case_index, row in enumerate(output["cases"]):
        prefix = "case_%d" % case_index
        if not isinstance(row, dict) or set(row) != {
            "case_id",
            "equivalence_groups",
            "alignment_pairs",
            "unpaired_witness_ids",
        }:
            errors.append(prefix + "_invalid_shape")
            continue
        case_id = str(row.get("case_id"))
        if case_id not in expected or case_id in seen_cases:
            errors.append(prefix + "_invalid_or_duplicate_id")
            continue
        seen_cases.add(case_id)
        source = expected[case_id]["source_excerpt"]
        all_ids = {
            str(item["witness_id"]) for item in expected[case_id]["witnesses"]
        }
        support_verdicts = {
            str(item["witness_id"]): item["support_receipt"]["proposition_verdict"]
            for item in expected[case_id]["witnesses"]
        }
        groups = row.get("equivalence_groups")
        grouped = set()
        if not isinstance(groups, list):
            errors.append(prefix + "_groups_not_array")
        else:
            for group_index, group in enumerate(groups):
                ids = group.get("witness_ids") if isinstance(group, dict) else None
                if (
                    not isinstance(group, dict)
                    or set(group) != {"witness_ids", "rationale"}
                    or not isinstance(ids, list)
                    or not ids
                    or len(ids) != len(set(ids))
                    or any(item not in all_ids for item in ids)
                    or set(ids) & grouped
                ):
                    errors.append(prefix + "_group_%d_invalid" % group_index)
                    continue
                grouped.update(ids)
            if grouped != all_ids:
                errors.append(prefix + "_equivalence_partition_mismatch")
        used = set()
        pairs = row.get("alignment_pairs")
        if not isinstance(pairs, list):
            errors.append(prefix + "_pairs_not_array")
        else:
            for pair_index, pair in enumerate(pairs):
                pair_prefix = "%s_pair_%d" % (prefix, pair_index)
                if not isinstance(pair, dict) or set(pair) != {
                    "witness_id_1",
                    "witness_id_2",
                    "relation",
                    "checklist",
                    "rationale",
                }:
                    errors.append(pair_prefix + "_invalid_shape")
                    continue
                first = pair.get("witness_id_1")
                second = pair.get("witness_id_2")
                if (
                    first not in all_ids
                    or second not in all_ids
                    or first == second
                    or first in used
                    or second in used
                ):
                    errors.append(pair_prefix + "_invalid_witness_partition")
                else:
                    used.update((first, second))
                checklist = pair.get("checklist")
                if (
                    not isinstance(checklist, list)
                    or [item.get("field") for item in checklist if isinstance(item, dict)]
                    != field_order
                ):
                    errors.append(pair_prefix + "_checklist_order_or_coverage")
                    continue
                for checklist_index, item in enumerate(checklist):
                    if not isinstance(item, dict) or set(item) != {
                        "field",
                        "decision",
                        "source_evidence_spans",
                        "witness_evidence_ids",
                        "rationale",
                    }:
                        errors.append(
                            pair_prefix + "_checklist_%d_invalid_shape" % checklist_index
                        )
                        continue
                    spans = item.get("source_evidence_spans")
                    witness_evidence = item.get("witness_evidence_ids")
                    if (
                        item.get("decision") not in CHECKLIST_DECISIONS
                        or not isinstance(spans, list)
                        or len(spans) > 4
                        or len(spans) != len(set(spans))
                        or any(
                            not isinstance(span, str)
                            or not span
                            or span not in source
                            for span in spans
                        )
                        or not isinstance(witness_evidence, list)
                        or len(witness_evidence) != 2
                        or len(set(witness_evidence)) != 2
                        or set(witness_evidence) != {first, second}
                    ):
                        errors.append(pair_prefix + "_checklist_%d_invalid" % checklist_index)
                try:
                    projected = project_mismatch_fields(checklist)
                    expected_relation = expected_relation_from_checklist(checklist)
                except JudgeV5ProtocolError:
                    errors.append(pair_prefix + "_checklist_projection_failed")
                    continue
                if pair.get("relation") != expected_relation:
                    errors.append(pair_prefix + "_relation_precedence_mismatch")
                unsupported_row = next(
                    item
                    for item in checklist
                    if item.get("field") == "unsupported_inference"
                )
                first_support = support_verdicts[first]
                second_support = support_verdicts[second]
                expected_unsupported = (
                    "abstain"
                    if "abstain" in {first_support, second_support}
                    else "same"
                    if first_support == second_support
                    else "different"
                )
                if unsupported_row.get("decision") != expected_unsupported:
                    errors.append(
                        pair_prefix + "_unsupported_inference_support_inconsistent"
                    )
                if "unsupported_inference" in projected and not (
                    set(projected) & ROOT_CONFLICT_FIELDS
                ):
                    errors.append(pair_prefix + "_unsupported_without_specific_root")
                if "event_boundary" in projected and "evidence" not in projected:
                    errors.append(pair_prefix + "_boundary_without_evidence")
                if pair.get("relation") == "partial" and set(projected) != {
                    "event_boundary",
                    "evidence",
                }:
                    errors.append(pair_prefix + "_partial_without_boundary_evidence")
        unpaired = row.get("unpaired_witness_ids")
        if (
            not isinstance(unpaired, list)
            or len(unpaired) != len(set(unpaired))
            or any(item not in all_ids or item in used for item in unpaired)
            or used | set(unpaired) != all_ids
        ):
            errors.append(prefix + "_alignment_partition_mismatch")
    if seen_cases != set(expected):
        errors.append("alignment_case_coverage_mismatch")
    return errors


def normalize_neutral_alignment_output(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> dict[str, Any]:
    errors = validate_neutral_alignment_output(output, alignment_input)
    if errors:
        raise JudgeV5ProtocolError("invalid alignment output: %s" % "; ".join(errors))
    normalized_cases = []
    for row in output["cases"]:
        pairs = []
        for pair in row["alignment_pairs"]:
            ids = sorted((pair["witness_id_1"], pair["witness_id_2"]))
            checklist_by_field = {item["field"]: item for item in pair["checklist"]}
            pairs.append(
                {
                    "witness_ids": ids,
                    "relation": pair["relation"],
                    "mismatch_fields": project_mismatch_fields(pair["checklist"]),
                    "checklist_decisions": {
                        field: checklist_by_field[field]["decision"]
                        for field in CHECKLIST_FIELDS
                    },
                }
            )
        normalized_cases.append(
            {
                "case_id": row["case_id"],
                "equivalence_groups": sorted(
                    (sorted(group["witness_ids"]) for group in row["equivalence_groups"]),
                    key=lambda values: tuple(values),
                ),
                "alignment_pairs": sorted(
                    pairs, key=lambda item: tuple(item["witness_ids"])
                ),
                "unpaired_witness_ids": sorted(row["unpaired_witness_ids"]),
            }
        )
    return {
        "schema_version": ALIGNMENT_OUTPUT_VERSION,
        "cases": sorted(normalized_cases, key=lambda item: item["case_id"]),
        "mismatch_fields_projected_from_checklists": True,
        "origin_neutral": True,
    }


def find_observable_alignment_disagreements(
    *,
    base_output: Mapping[str, Any],
    base_input: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    canary_input: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
) -> dict[str, Any]:
    base = normalize_neutral_alignment_output(base_output, base_input)
    canary = normalize_neutral_alignment_output(canary_output, canary_input)
    base_by_case = {item["case_id"]: item for item in base["cases"]}
    canary_by_case = {item["case_id"]: item for item in canary["cases"]}
    if not set(canary_by_case) <= set(base_by_case):
        raise JudgeV5ProtocolError("canary cases are outside the base alignment run")
    support = _support_by_id(support_receipts)
    disagreements = []
    for case_id, canary_case in sorted(canary_by_case.items()):
        reasons = []
        base_case = base_by_case[case_id]
        if base_case != canary_case:
            reasons.append("permutation_output_changed")
        for pair in base_case["alignment_pairs"]:
            first, second = pair["witness_ids"]
            first_support = support[first]["proposition_verdict"]
            second_support = support[second]["proposition_verdict"]
            expected_unsupported = (
                "abstain"
                if "abstain" in {first_support, second_support}
                else "same"
                if first_support == second_support
                else "different"
            )
            if pair["checklist_decisions"]["unsupported_inference"] != expected_unsupported:
                reasons.append("support_alignment_unsupported_inference_conflict")
            first_fields = support[first]["structured_field_verdict"]
            second_fields = support[second]["structured_field_verdict"]
            if (
                first_fields != second_fields
                and pair["relation"] == "equivalent"
            ):
                reasons.append("support_alignment_structured_field_conflict")
        if reasons:
            disagreements.append(
                {"case_id": case_id, "reasons": sorted(set(reasons))}
            )
    return {
        "disagreement_case_count": len(disagreements),
        "disagreements": disagreements,
        "adjudication_required": bool(disagreements),
        "adjudication_call_cap": 1,
        "majority_voting_used": False,
    }


def build_disagreement_adjudication_input(
    *,
    base_input: Mapping[str, Any],
    base_output: Mapping[str, Any],
    canary_input: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
) -> dict[str, Any]:
    disagreement = find_observable_alignment_disagreements(
        base_output=base_output,
        base_input=base_input,
        canary_output=canary_output,
        canary_input=canary_input,
        support_receipts=support_receipts,
    )
    case_ids = {item["case_id"] for item in disagreement["disagreements"]}
    if not case_ids:
        return {
            "schema_version": ADJUDICATION_INPUT_VERSION,
            "cases": [],
            "adjudication_required": False,
            "call_cap": 1,
        }
    base_cases = {
        item["case_id"]: item for item in base_input.get("cases") or []
    }
    normalized_base = {
        item["case_id"]: item
        for item in normalize_neutral_alignment_output(base_output, base_input)["cases"]
    }
    normalized_canary = {
        item["case_id"]: item
        for item in normalize_neutral_alignment_output(canary_output, canary_input)["cases"]
    }
    cases = []
    for item in disagreement["disagreements"]:
        case_id = item["case_id"]
        cases.append(
            {
                **deepcopy(base_cases[case_id]),
                "observed_disagreement_reasons": item["reasons"],
                "anonymous_candidate_1": normalized_base[case_id],
                "anonymous_candidate_2": normalized_canary[case_id],
            }
        )
    return {
        "schema_version": ADJUDICATION_INPUT_VERSION,
        "cases": cases,
        "adjudication_required": True,
        "call_cap": 1,
        "candidate_order_has_no_vote_meaning": True,
    }


def adjudication_alignment_input(
    *,
    base_input: Mapping[str, Any],
    adjudication_input: Mapping[str, Any],
) -> dict[str, Any]:
    if adjudication_input.get("schema_version") != ADJUDICATION_INPUT_VERSION:
        raise JudgeV5ProtocolError("unsupported disagreement adjudication input")
    case_ids = {str(item["case_id"]) for item in adjudication_input.get("cases") or []}
    selected = [
        deepcopy(item)
        for item in base_input.get("cases") or []
        if item.get("case_id") in case_ids
    ]
    if {item["case_id"] for item in selected} != case_ids:
        raise JudgeV5ProtocolError("adjudication cases are outside base alignment input")
    return {
        "schema_version": ALIGNMENT_INPUT_VERSION,
        "permutation": "base",
        "cases": selected,
        "checklist_field_order": list(CHECKLIST_FIELDS),
        "checklist_decision_order": list(CHECKLIST_DECISIONS),
        "side_labels_present": False,
        "system_identity_present": False,
        "support_receipts_frozen": True,
        "adjudication_only": True,
    }


def build_disagreement_adjudication_prompt(
    *,
    adjudication_input: Mapping[str, Any],
    adjudication_alignment: Mapping[str, Any],
) -> str:
    if (
        adjudication_input.get("schema_version") != ADJUDICATION_INPUT_VERSION
        or adjudication_input.get("adjudication_required") is not True
        or adjudication_input.get("call_cap") != 1
        or adjudication_alignment.get("adjudication_only") is not True
    ):
        raise JudgeV5ProtocolError("adjudication prompt requires one frozen disagreement set")
    audit = load_fixture_truth_audit()
    instructions = (
        "Resolve only the listed observable disagreements. The two prior candidates are "
        "anonymous diagnostic evidence, not votes; do not choose by majority or candidate "
        "position. Re-evaluate the source, events, and frozen support receipts independently. "
        "Return the same neutral alignment schema with all 15 checklist rows. If the source "
        "cannot resolve a material row, use abstain. This is the sole capped adjudication call."
    )
    packet = {
        "rubric": audit["mismatch_checklist"],
        "mismatch_precedence": audit["mismatch_precedence"],
        "cases": adjudication_input["cases"],
    }
    return instructions + "\n\n# Side-free disagreement cases\n" + _canonical_json(packet) + "\n"


def reconcile_neutral_alignment(
    *,
    base_output: Mapping[str, Any],
    base_input: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    canary_input: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
    adjudication_output: Optional[Mapping[str, Any]] = None,
    adjudication_input: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    base = normalize_neutral_alignment_output(base_output, base_input)
    disagreement = find_observable_alignment_disagreements(
        base_output=base_output,
        base_input=base_input,
        canary_output=canary_output,
        canary_input=canary_input,
        support_receipts=support_receipts,
    )
    disagreement_ids = {item["case_id"] for item in disagreement["disagreements"]}
    adjudicated = {}
    adjudication_call_count = 0
    if disagreement_ids and adjudication_output is not None:
        if adjudication_input is None:
            raise JudgeV5ProtocolError("adjudication output has no frozen input")
        normalized = normalize_neutral_alignment_output(
            adjudication_output, adjudication_input
        )
        adjudicated = {item["case_id"]: item for item in normalized["cases"]}
        if set(adjudicated) != disagreement_ids:
            raise JudgeV5ProtocolError("adjudication output coverage drifted")
        adjudication_call_count = 1
    cases = []
    for item in base["cases"]:
        case_id = item["case_id"]
        if case_id not in disagreement_ids:
            cases.append({"case_id": case_id, "status": "accepted_base", **item})
        elif case_id in adjudicated:
            cases.append(
                {"case_id": case_id, "status": "adjudicated", **adjudicated[case_id]}
            )
        else:
            cases.append(
                {
                    "case_id": case_id,
                    "status": "abstain",
                    "equivalence_groups": [],
                    "alignment_pairs": [],
                    "unpaired_witness_ids": [],
                }
            )
    return {
        "schema_version": "pif_app_server_v5_reconciled_alignment_v4",
        "cases": cases,
        "observable_disagreement_case_count": len(disagreement_ids),
        "adjudication_call_count": adjudication_call_count,
        "adjudication_call_cap": 1,
        "majority_voting_used": False,
        "unresolved_cases_abstained": sorted(
            item["case_id"] for item in cases if item["status"] == "abstain"
        ),
    }


DIAGNOSTIC_GATES = {
    "minimum_cases": 18,
    "support_sensitivity_min": 0.95,
    "support_specificity_min": 0.95,
    "structured_field_accuracy_min": 0.95,
    "alignment_f1_min": 0.95,
    "equivalent_sensitivity_min": 0.9,
    "equivalent_specificity_min": 0.9,
    "field_diagnostic_f1_min": 0.85,
    "relation_accuracy_min": 0.9,
    "equivalence_partition_exact_case_rate_min": 0.95,
    "abstention_rate_max": 0.0,
    "order_bias_max": 0.05,
    "canary_case_count": 6,
}


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _f1(true_positive: int, false_positive: int, false_negative: int) -> float:
    precision = _ratio(true_positive, true_positive + false_positive)
    recall = _ratio(true_positive, true_positive + false_negative)
    return (
        round(2 * precision * recall / (precision + recall), 6)
        if precision + recall
        else 1.0
        if true_positive == false_positive == false_negative == 0
        else 0.0
    )


def score_v5_diagnostic(
    *,
    pointwise_output: Mapping[str, Any],
    pointwise_input: Mapping[str, Any],
    reconciled_alignment: Mapping[str, Any],
    expected: Mapping[str, Any],
    observable_disagreements: Mapping[str, Any],
) -> dict[str, Any]:
    pointwise_errors = validate_pointwise_support_output(
        pointwise_output, pointwise_input
    )
    if pointwise_errors:
        raise JudgeV5ProtocolError(
            "cannot score invalid pointwise output: %s" % "; ".join(pointwise_errors)
        )
    expected_cases = expected.get("cases")
    if (
        expected.get("schema_version") != DIAGNOSTIC_TRUTH_VERSION
        or not isinstance(expected_cases, dict)
        or len(expected_cases) != 18
    ):
        raise JudgeV5ProtocolError("diagnostic truth is not frozen or complete")
    support_rows = {
        str(item["witness_id"]): item for item in pointwise_output["units"]
    }
    support_tp = support_fn = support_tn = support_fp = 0
    structured_correct = structured_total = 0
    pointwise_field_tp = pointwise_field_fp = pointwise_field_fn = 0
    abstentions = 0
    for truth in expected_cases.values():
        for witness_id, expected_verdict in truth["proposition"].items():
            row = support_rows[witness_id]
            observed = row["proposition_verdict"]
            expected_supported = expected_verdict == "supported"
            if observed == "abstain":
                abstentions += 1
            if expected_supported and observed == "supported":
                support_tp += 1
            elif expected_supported:
                support_fn += 1
            elif observed == "unsupported":
                support_tn += 1
            else:
                support_fp += 1
            structured_total += 1
            structured_correct += int(
                row["structured_field_verdict"]
                == truth["structured_fields"][witness_id]
            )
            if row["structured_field_verdict"] == "abstain":
                abstentions += 1
            wanted = set(truth["field_issues"][witness_id])
            observed_issues = set(row["field_issue_fields"])
            pointwise_field_tp += len(wanted & observed_issues)
            pointwise_field_fp += len(observed_issues - wanted)
            pointwise_field_fn += len(wanted - observed_issues)
    alignment_rows = {
        str(item["case_id"]): item
        for item in reconciled_alignment.get("cases") or []
    }
    if set(alignment_rows) != set(expected_cases):
        raise JudgeV5ProtocolError("reconciled diagnostic case coverage drifted")
    alignment_tp = alignment_fp = alignment_fn = 0
    relation_correct = relation_total = 0
    field_tp = field_fp = field_fn = 0
    equivalent_tp = equivalent_fn = equivalent_tn = equivalent_fp = 0
    partition_exact = 0
    for case_id, truth in expected_cases.items():
        row = alignment_rows[case_id]
        if row.get("status") == "abstain":
            abstentions += 1
        expected_pair = tuple(truth["pair_witness_ids"])
        observed_pairs = {
            tuple(item["witness_ids"]): item
            for item in row.get("alignment_pairs") or []
        }
        observed_pair = observed_pairs.get(expected_pair)
        alignment_tp += int(observed_pair is not None)
        alignment_fn += int(observed_pair is None)
        alignment_fp += len(set(observed_pairs) - {expected_pair})
        observed_relation = (
            observed_pair.get("relation") if observed_pair is not None else None
        )
        if observed_relation == "abstain":
            abstentions += 1
        relation_total += 1
        relation_correct += int(observed_relation == truth["relation"])
        expected_equivalent = truth["relation"] == "equivalent"
        if expected_equivalent and observed_relation == "equivalent":
            equivalent_tp += 1
        elif expected_equivalent:
            equivalent_fn += 1
        elif observed_relation == "equivalent" or observed_relation in {None, "abstain"}:
            equivalent_fp += 1
        else:
            equivalent_tn += 1
        wanted_fields = set(truth["mismatch_fields"])
        observed_fields = set(
            observed_pair.get("mismatch_fields") if observed_pair is not None else []
        )
        field_tp += len(wanted_fields & observed_fields)
        field_fp += len(observed_fields - wanted_fields)
        field_fn += len(wanted_fields - observed_fields)
        observed_partition = {
            tuple(sorted(group)) for group in row.get("equivalence_groups") or []
        }
        expected_partition = {
            tuple(sorted(group)) for group in truth["equivalence_groups"]
        }
        partition_exact += int(observed_partition == expected_partition)
    support_sensitivity = _ratio(support_tp, support_tp + support_fn)
    support_specificity = _ratio(support_tn, support_tn + support_fp)
    alignment_f1 = _f1(alignment_tp, alignment_fp, alignment_fn)
    field_f1 = _f1(field_tp, field_fp, field_fn)
    order_denominator = len(expected.get("canary_case_ids") or [])
    order_bias = _ratio(
        int(observable_disagreements.get("disagreement_case_count") or 0),
        order_denominator,
    )
    metrics = {
        "case_count": len(expected_cases),
        "support_sensitivity": support_sensitivity,
        "support_specificity": support_specificity,
        "structured_field_accuracy": _ratio(
            structured_correct, structured_total
        ),
        "pointwise_field_issue_f1": _f1(
            pointwise_field_tp, pointwise_field_fp, pointwise_field_fn
        ),
        "alignment_f1": alignment_f1,
        "equivalent_sensitivity": _ratio(
            equivalent_tp, equivalent_tp + equivalent_fn
        ),
        "equivalent_specificity": _ratio(
            equivalent_tn, equivalent_tn + equivalent_fp
        ),
        "field_diagnostic_f1": field_f1,
        "relation_accuracy": _ratio(relation_correct, relation_total),
        "equivalence_partition_exact_case_rate": _ratio(
            partition_exact, len(expected_cases)
        ),
        "abstention_count": abstentions,
        "abstention_rate": _ratio(
            abstentions, structured_total + len(expected_cases) * 2
        ),
        "order_bias": order_bias,
        "canary_case_count": order_denominator,
    }
    checks = {
        "minimum_cases": metrics["case_count"] >= DIAGNOSTIC_GATES["minimum_cases"],
        "support_sensitivity": support_sensitivity
        >= DIAGNOSTIC_GATES["support_sensitivity_min"],
        "support_specificity": support_specificity
        >= DIAGNOSTIC_GATES["support_specificity_min"],
        "structured_field_accuracy": metrics["structured_field_accuracy"]
        >= DIAGNOSTIC_GATES["structured_field_accuracy_min"],
        "alignment_f1": alignment_f1 >= DIAGNOSTIC_GATES["alignment_f1_min"],
        "equivalent_sensitivity": metrics["equivalent_sensitivity"]
        >= DIAGNOSTIC_GATES["equivalent_sensitivity_min"],
        "equivalent_specificity": metrics["equivalent_specificity"]
        >= DIAGNOSTIC_GATES["equivalent_specificity_min"],
        "field_diagnostic_f1": field_f1
        >= DIAGNOSTIC_GATES["field_diagnostic_f1_min"],
        "relation_accuracy": metrics["relation_accuracy"]
        >= DIAGNOSTIC_GATES["relation_accuracy_min"],
        "equivalence_partition_exact_case_rate": metrics[
            "equivalence_partition_exact_case_rate"
        ]
        >= DIAGNOSTIC_GATES["equivalence_partition_exact_case_rate_min"],
        "abstention_rate": metrics["abstention_rate"]
        <= DIAGNOSTIC_GATES["abstention_rate_max"],
        "order_bias": order_bias <= DIAGNOSTIC_GATES["order_bias_max"],
        "canary_case_count": order_denominator
        == DIAGNOSTIC_GATES["canary_case_count"],
    }
    return {
        "schema_version": "pif_app_server_v5_diagnostic_score_v4",
        "passed": all(checks.values()),
        "gates": DIAGNOSTIC_GATES,
        "metrics": metrics,
        "checks": checks,
        "legacy_joint_support_labels_used": False,
        "proposition_and_structured_field_scores_separate": True,
    }
