from __future__ import annotations

"""Candidate-agnostic bundles for the frozen support and alignment judges."""

import copy
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_configured_experiment as configured
from . import app_server_judge_v5 as judge
from . import app_server_llm_judge
from . import app_server_runtime_verifier as runtime_verifier
from .app_server_runtime_verifier import ContentHashCache
from .util import now_iso, sha256_text


PROTOCOL_VERSION = "pif_candidate_semantic_evaluator_protocol_v3"
LOCK_VERSION = "pif_candidate_semantic_evaluator_runtime_lock_v3"
BUNDLE_VERSION = "pif_candidate_semantic_evaluation_bundle_v1"
SCORE_VERSION = "pif_candidate_semantic_evaluation_score_v2"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work" / "app-server-development-v2" / "unattended-pipeline-v5"
).resolve()
PREDECESSOR_PROTOCOL_ROOT = (
    PIPELINE_ROOT / "reusable-candidate-semantic-evaluator-v2"
).resolve()
DEFAULT_PROTOCOL_ROOT = (PIPELINE_ROOT / "reusable-candidate-semantic-evaluator-v3").resolve()
FROZEN_SUPPORT_JUDGE_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v276-frozen-support-v275"
).resolve()
FROZEN_ALIGNMENT_JUDGE_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v277-frozen-alignment-v275"
).resolve()
QUALITY_THRESHOLD = 0.97
TOKEN_RATIO_TARGET = 0.28
ALIGNMENT_PERMUTATIONS = ("base", "balanced_canary")
SUPPORT_MODEL = "gpt-5.6-sol"
SUPPORT_EFFORT = "high"
ALIGNMENT_MODEL = "gpt-5.5"
ALIGNMENT_EFFORT = "high"
ADJUDICATION_MODEL = ALIGNMENT_MODEL
ADJUDICATION_EFFORT = ALIGNMENT_EFFORT
SUPPORT_MAX_PROMPT_BYTES = 130_000
SUPPORT_MAX_SCHEMA_BYTES = 55_000
ALIGNMENT_MAX_PROMPT_BYTES = 180_000
ALIGNMENT_MAX_SCHEMA_BYTES = 20_000
ADJUDICATION_MAX_PROMPT_BYTES = ALIGNMENT_MAX_PROMPT_BYTES
ADJUDICATION_MAX_SCHEMA_BYTES = ALIGNMENT_MAX_SCHEMA_BYTES
HASH_CACHE = ContentHashCache()


class CandidateEvaluationError(RuntimeError):
    """A deterministic evaluator contract or artifact is invalid."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateEvaluationError(f"cannot read {label}") from exc


def _record(path: Path) -> dict[str, Any]:
    return HASH_CACHE.record(path.expanduser().resolve())


def _write_immutable(path: Path, value: Any) -> None:
    configured._write_immutable(path, value)  # noqa: SLF001


def support_instructions() -> str:
    return (
        "You are a side-free pointwise proposition-support evaluator. For each opaque witness, "
        "judge only whether every material claim in proposition.claim_text is entailed by that "
        "witness's source_excerpt. Harmless paraphrase and resolved coreference pass. Exact copied "
        "wording alone does not license an unsupported inference. Do not judge actor, speaker, "
        "reported actor, event type, evidence-field quality, or any other structured field in this "
        "pass. Cite one or more exact source substrings for every decision. Abstain only when the "
        "source genuinely cannot determine support. Do not compare witnesses, vote, use confidence, "
        "regex, keywords, overlap, embeddings, prior labels, model identity, or system identity."
    )


def alignment_instructions() -> str:
    return judge.neutral_alignment_base_instructions() + (
        " Every visible witness has a frozen supported proposition verdict. Unsupported witnesses "
        "were excluded before this pass and must not be inferred or reconstructed. Align only the "
        "visible support-positive witnesses. Freeze one-to-one assignment before the checklist. "
        "For merge/split candidates, normalize each visible witness into its source-supported atomic "
        "propositions; identical atom sets with grouping-only differences are partial with exactly "
        "event_boundary and evidence different."
    )


def _support_input(units: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "pif_app_server_judge_v5_4_v143_support_input_v1",
        "unit_count": len(units),
        "units": [
            {
                "case_id": row["case_id"],
                "witness_id": row["witness_id"],
                "proposition": copy.deepcopy(row["proposition"]),
                "source_excerpt": row["source_excerpt"],
            }
            for row in units
        ],
        "side_labels_present": False,
        "system_identity_present": False,
        "tasks_are_independent": True,
    }


def _compact_support_prompt(units: Sequence[Mapping[str, Any]]) -> str:
    by_case: dict[str, list[Mapping[str, Any]]] = {}
    for unit in units:
        by_case.setdefault(str(unit["case_id"]), []).append(unit)
    cases = []
    for case_id in sorted(by_case):
        rows = sorted(by_case[case_id], key=lambda row: str(row["witness_id"]))
        sources = {str(row["source_excerpt"]) for row in rows}
        if len(sources) != 1:
            raise CandidateEvaluationError("support case source excerpt drifted")
        cases.append(
            {
                "case_id": case_id,
                "source_excerpt": next(iter(sources)),
                "witnesses": [
                    {
                        "witness_id": row["witness_id"],
                        "proposition": copy.deepcopy(row["proposition"]),
                    }
                    for row in rows
                ],
            }
        )
    return (
        "Return one independent support decision for every opaque witness_id. Preserve case_id and "
        "witness_id exactly. Each case supplies its source_excerpt once; every witness in that case "
        "uses that same source. Every evidence span must be an exact substring of its case's "
        "source_excerpt.\n\n"
        + _canonical_json({"cases": cases})
        + "\n"
    )


def _support_output_schema(value: Mapping[str, Any]) -> dict[str, Any]:
    units = list(value.get("units") or [])
    case_ids = sorted({str(row["case_id"]) for row in units})
    witness_ids = [str(row["witness_id"]) for row in units]
    row = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "case_id",
            "witness_id",
            "support_status",
            "source_evidence_spans",
            "rationale",
        ],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "witness_id": {"type": "string", "enum": witness_ids},
            "support_status": {
                "type": "string",
                "enum": ["supported", "unsupported", "abstain"],
            },
            "source_evidence_spans": {
                "type": "array",
                "minItems": 1,
                "maxItems": 4,
                "items": {"type": "string", "minLength": 1, "maxLength": 1000},
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 600},
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
    errors = app_server_llm_judge.validate_app_server_output_schema_subset(schema)
    if errors:
        raise CandidateEvaluationError("support schema is unsupported")
    return schema


def _validate_support_output(
    output: Any, value: Mapping[str, Any]
) -> list[str]:
    if (
        not isinstance(output, Mapping)
        or set(output) != {"units"}
        or not isinstance(output["units"], list)
    ):
        return ["invalid_support_output_root"]
    expected = {
        (str(row["case_id"]), str(row["witness_id"])): row
        for row in value["units"]
    }
    required = {
        "case_id",
        "witness_id",
        "support_status",
        "source_evidence_spans",
        "rationale",
    }
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for index, row in enumerate(output["units"]):
        if not isinstance(row, Mapping) or set(row) != required:
            errors.append(f"support_{index}_invalid_shape")
            continue
        key = (str(row["case_id"]), str(row["witness_id"]))
        if key not in expected or key in seen:
            errors.append(f"support_{index}_invalid_or_duplicate_id")
            continue
        seen.add(key)
        spans = row["source_evidence_spans"]
        source = str(expected[key]["source_excerpt"])
        if (
            not isinstance(spans, list)
            or not 1 <= len(spans) <= 4
            or len(spans) != len(set(spans))
            or any(
                not isinstance(span, str)
                or not span
                or len(span) > 1000
                or span not in source
                for span in spans
            )
        ):
            errors.append(f"support_{index}_evidence_not_exact")
        if row["support_status"] not in {"supported", "unsupported", "abstain"}:
            errors.append(f"support_{index}_invalid_status")
        rationale = row["rationale"]
        if not isinstance(rationale, str) or not rationale or len(rationale) > 600:
            errors.append(f"support_{index}_invalid_rationale")
    if seen != set(expected):
        errors.append("support_coverage_mismatch")
    return errors


def _support_receipts(output: Mapping[str, Any]) -> dict[str, Any]:
    rows = [
        {
            "case_id": row["case_id"],
            "witness_id": row["witness_id"],
            "proposition_verdict": row["support_status"],
            "proposition_evidence_spans": row["source_evidence_spans"],
            "structured_field_verdict": "abstain",
            "field_issue_fields": [],
            "field_evidence_spans": [],
        }
        for row in output["units"]
    ]
    rows.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    return {
        "schema_version": judge.POINTWISE_OUTPUT_VERSION,
        "units": rows,
        "side_free": True,
        "claim_support_and_field_correctness_separate": True,
    }


def _alignment_prompt(value: Mapping[str, Any]) -> str:
    return (
        "All presented witnesses are support-positive. Align only these visible witnesses, freeze "
        "assignment, then complete the full checklist. Do not infer omitted witnesses.\n\n"
        + judge.build_neutral_alignment_prompt(value)
    )


def _owner_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    checklists = [
        {
            "witness_ids": sorted(str(value) for value in pair["witness_ids"]),
            "decisions": {
                field: str((pair.get("checklist_decisions") or {})[field])
                for field in judge.CHECKLIST_FIELDS
            },
        }
        for pair in row.get("alignment_pairs") or []
    ]
    checklists.sort(key=lambda item: tuple(item["witness_ids"]))
    return {
        "alignment": {
            "equivalence_groups": sorted(
                (sorted(group) for group in row.get("equivalence_groups") or []),
                key=lambda values: tuple(values),
            ),
            "unpaired_witness_ids": sorted(row.get("unpaired_witness_ids") or []),
            "relations": sorted(
                (
                    sorted(str(value) for value in pair["witness_ids"]),
                    str(pair["relation"]),
                    sorted(str(value) for value in pair.get("mismatch_fields") or []),
                )
                for pair in row.get("alignment_pairs") or []
            ),
        },
        "checklists": checklists,
    }


def _alignment_consistency_issues(
    normalized: Mapping[str, Any]
) -> list[dict[str, str]]:
    issues = []
    for case in normalized.get("cases") or []:
        group_by_id = {
            witness_id: index
            for index, group in enumerate(case.get("equivalence_groups") or [])
            for witness_id in group
        }
        for pair in case.get("alignment_pairs") or []:
            first, second = pair["witness_ids"]
            same_group = (
                first in group_by_id
                and second in group_by_id
                and group_by_id[first] == group_by_id[second]
            )
            if (pair["relation"] == "equivalent") != same_group:
                issues.append(
                    {
                        "case_id": str(case["case_id"]),
                        "reason": "relation_equivalence_partition_conflict",
                    }
                )
    return issues


def _case_has_abstention(case: Mapping[str, Any]) -> bool:
    return case.get("status") == "abstain" or any(
        pair.get("relation") == "abstain"
        or "abstain" in (pair.get("checklist_decisions") or {}).values()
        for pair in case.get("alignment_pairs") or []
    )


def _event_claim(event: Mapping[str, Any]) -> str:
    claim = event.get("claim_text")
    if not isinstance(claim, str) or not claim.strip():
        raise CandidateEvaluationError("event has no explicit claim_text")
    return claim


def _source_excerpt(segment: Mapping[str, Any]) -> str:
    units = segment.get("units")
    if not isinstance(units, list) or not units:
        raise CandidateEvaluationError("source segment has no units")
    excerpt = "\n".join(str(unit.get("text") or "") for unit in units)
    if not excerpt:
        raise CandidateEvaluationError("source segment excerpt is empty")
    return excerpt


def validate_candidate_grounding(
    *,
    source: Mapping[str, Any],
    candidate: Mapping[str, Any],
    provenance: Mapping[str, Any] | None = None,
    event_cap: int = 32,
) -> dict[str, Any]:
    """Validate only exact spans, literal metrics, provenance, and caps."""

    source_segments = list(source.get("segments") or [])
    candidate_segments = list(candidate.get("segments") or [])
    source_ids = [str(row.get("segment_id")) for row in source_segments]
    candidate_ids = [str(row.get("segment_id")) for row in candidate_segments]
    if (
        source.get("episode_id") != candidate.get("episode_id")
        or source_ids != candidate_ids
        or len(source_ids) != len(set(source_ids))
    ):
        raise CandidateEvaluationError("candidate episode or segment coverage drifted")

    exact_events = 0
    metric_events = 0
    total_events = 0
    candidate_by_id = {str(row["segment_id"]): row for row in candidate_segments}
    for source_segment in source_segments:
        segment_id = str(source_segment["segment_id"])
        segment_text = str(source_segment.get("segment_text") or "")
        events = list(candidate_by_id[segment_id].get("events") or [])
        if len(events) > event_cap:
            raise CandidateEvaluationError("candidate event cap exceeded")
        for event in events:
            if not isinstance(event, Mapping):
                raise CandidateEvaluationError("candidate event is malformed")
            _event_claim(event)
            evidence = event.get("evidence")
            if not isinstance(evidence, str) or not evidence or evidence not in segment_text:
                raise CandidateEvaluationError("candidate evidence is not an exact source span")
            metric_values = [
                str(event.get(field) or "")
                for field in (
                    "metric_value",
                    "metric_unit",
                    "metric_comparator",
                    "metric_raw_text",
                )
            ]
            if any(value and value not in evidence for value in metric_values):
                raise CandidateEvaluationError("candidate metric is not literal evidence")
            has_metric = any(metric_values)
            if has_metric == (event.get("metric_direction") == "not_applicable"):
                raise CandidateEvaluationError("candidate metric applicability is inconsistent")
            total_events += 1
            exact_events += 1
            metric_events += int(has_metric)

    normalized_provenance_rows = []
    if provenance is not None:
        if provenance.get("episode_id") != candidate.get("episode_id"):
            raise CandidateEvaluationError("candidate provenance episode drifted")
        rows = list(provenance.get("events") or [])
        if len(rows) != total_events:
            raise CandidateEvaluationError("candidate provenance count drifted")
        rows_by_segment: dict[str, list[Mapping[str, Any]]] = {
            segment_id: [] for segment_id in source_ids
        }
        for row in rows:
            segment_id = str(row.get("segment_id"))
            if segment_id not in rows_by_segment:
                raise CandidateEvaluationError("candidate provenance segment drifted")
            rows_by_segment[segment_id].append(row)
        source_by_id = {str(row["segment_id"]): row for row in source_segments}
        for segment_id in source_ids:
            events = list(candidate_by_id[segment_id].get("events") or [])
            segment_rows = rows_by_segment[segment_id]
            if len(segment_rows) != len(events):
                raise CandidateEvaluationError("candidate segment provenance count drifted")
            for event_index, (event, row) in enumerate(zip(events, segment_rows)):
                evidence = str(event["evidence"])
                if row.get("evidence_sha256") != sha256_text(evidence):
                    raise CandidateEvaluationError("candidate provenance evidence hash drifted")
                start = row.get("start_char")
                end = row.get("end_char")
                segment_text = str(source_by_id[segment_id].get("segment_text") or "")
                if (
                    not isinstance(start, int)
                    or not isinstance(end, int)
                    or start < 0
                    or end < start
                    or segment_text[start:end] != evidence
                ):
                    raise CandidateEvaluationError("candidate provenance offsets drifted")
                normalized_provenance_rows.append(
                    {
                        **dict(row),
                        "segment_id": segment_id,
                        "event_index": event_index,
                    }
                )

    return {
        "candidate_event_count": total_events,
        "exact_evidence_event_count": exact_events,
        "exact_evidence_rate": 1.0 if total_events == 0 else exact_events / total_events,
        "metric_event_count": metric_events,
        "metric_grounding_error_event_count": 0,
        "event_cap_violation_count": 0,
        "normalized_provenance_rows": normalized_provenance_rows,
    }


def build_support_bundle(
    *,
    evaluation_id: str,
    source: Mapping[str, Any],
    candidate: Mapping[str, Any],
    shared_reference: Mapping[str, Any],
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not evaluation_id or not evaluation_id.isascii():
        raise CandidateEvaluationError("evaluation_id must be nonempty ASCII")
    grounding = validate_candidate_grounding(
        source=source, candidate=candidate, provenance=provenance
    )
    source_segments = list(source.get("segments") or [])
    candidate_by_id = {
        str(row["segment_id"]): row for row in candidate.get("segments") or []
    }
    reference_by_id = {
        str(row["segment_id"]): row for row in shared_reference.get("references") or []
    }
    segment_ids = [str(row["segment_id"]) for row in source_segments]
    if any(segment_id not in reference_by_id for segment_id in segment_ids):
        raise CandidateEvaluationError("shared reference does not cover source segments")

    cases = []
    units = []
    origins = []
    for source_row in source_segments:
        segment_id = str(source_row["segment_id"])
        excerpt = _source_excerpt(source_row)
        reference_row = reference_by_id[segment_id]
        if reference_row.get("text_sha256") != sha256_text(excerpt):
            raise CandidateEvaluationError("shared reference source hash drifted")
        case_id = "case_" + sha256_text(
            f"{evaluation_id}|support-case|{segment_id}"
        )[:24]
        reference_events = list(
            (reference_row.get("golden_output") or {}).get("discourse_events") or []
        )
        candidate_events = list(candidate_by_id[segment_id].get("events") or [])
        witnesses = []
        for origin, events in (
            ("reference", reference_events),
            ("candidate", candidate_events),
        ):
            for event_index, event in enumerate(events):
                if not isinstance(event, Mapping):
                    raise CandidateEvaluationError("support event is malformed")
                event_value = judge.compact_empty_event_fields(copy.deepcopy(dict(event)))
                event_hash = sha256_text(_canonical_json(event_value))
                witness_id = "w_" + sha256_text(
                    f"{evaluation_id}|support-witness|{case_id}|{origin}|"
                    f"{event_index}|{event_hash}"
                )[:24]
                witnesses.append({"witness_id": witness_id, "event": event_value})
                units.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "proposition": {"claim_text": _event_claim(event_value)},
                        "source_excerpt": excerpt,
                    }
                )
                origins.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "segment_id": segment_id,
                        "density_stratum": str(source_row.get("density_stratum") or "unknown"),
                        "origin": origin,
                        "event_index": event_index,
                        "event_sha256": event_hash,
                    }
                )
        cases.append(
            {
                "case_id": case_id,
                "segment_id": segment_id,
                "density_stratum": str(source_row.get("density_stratum") or "unknown"),
                "source_excerpt": excerpt,
                "witnesses": sorted(witnesses, key=lambda row: str(row["witness_id"])),
                "reference_event_count": len(reference_events),
                "candidate_event_count": len(candidate_events),
            }
        )

    cases.sort(key=lambda row: str(row["case_id"]))
    units.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    origins.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    counts = Counter(str(row["origin"]) for row in origins)
    support_value = _support_input(units)
    prompt = _compact_support_prompt(units)
    schema = _support_output_schema(support_value)
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if (
        prompt_bytes > SUPPORT_MAX_PROMPT_BYTES
        or schema_bytes > SUPPORT_MAX_SCHEMA_BYTES
    ):
        raise CandidateEvaluationError("support request size cap exceeded")
    return {
        "schema_version": BUNDLE_VERSION,
        "evaluation_id": evaluation_id,
        "cases": cases,
        "units": units,
        "origins": origins,
        "support_value": support_value,
        "prompt": prompt,
        "schema": schema,
        "grounding": grounding,
        "counts": {
            "case_count": len(cases),
            "reference_witness_count": counts["reference"],
            "candidate_witness_count": counts["candidate"],
            "total_witness_count": len(origins),
            "counts_are_observed_not_targets": True,
        },
        "request_sizes": {"prompt_bytes": prompt_bytes, "schema_bytes": schema_bytes},
    }


def score_support_output(
    *, output: Mapping[str, Any], support_bundle: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    errors = _validate_support_output(output, support_bundle["support_value"])
    if errors:
        raise CandidateEvaluationError("frozen support output validation failed")
    decisions = {str(row["witness_id"]): row for row in output["units"]}
    origins = {str(row["witness_id"]): row for row in support_bundle["origins"]}
    if set(decisions) != set(origins):
        raise CandidateEvaluationError("support decision coverage drifted")
    rows = []
    for witness_id, decision in decisions.items():
        rows.append(
            {
                **dict(origins[witness_id]),
                "support_status": str(decision["support_status"]),
                "source_evidence_span_count": len(decision["source_evidence_spans"]),
            }
        )
    rows.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    case_summaries = []
    alignable_case_ids = []
    for case in support_bundle["cases"]:
        case_rows = [row for row in rows if row["case_id"] == case["case_id"]]
        status_counts = Counter(
            (str(row["origin"]), str(row["support_status"])) for row in case_rows
        )
        supported_reference = status_counts[("reference", "supported")]
        supported_candidate = status_counts[("candidate", "supported")]
        if supported_reference and supported_candidate:
            alignable_case_ids.append(str(case["case_id"]))
        case_summaries.append(
            {
                "case_id": case["case_id"],
                "density_stratum": case["density_stratum"],
                "reference_witness_count": case["reference_event_count"],
                "candidate_witness_count": case["candidate_event_count"],
                "supported_reference_witness_count": supported_reference,
                "supported_candidate_witness_count": supported_candidate,
                "unsupported_reference_witness_count": status_counts[
                    ("reference", "unsupported")
                ],
                "unsupported_candidate_witness_count": status_counts[
                    ("candidate", "unsupported")
                ],
                "abstain_reference_witness_count": status_counts[("reference", "abstain")],
                "abstain_candidate_witness_count": status_counts[("candidate", "abstain")],
            }
        )
    statuses = Counter(str(row["support_status"]) for row in rows)
    audit = {
        "schema_version": BUNDLE_VERSION,
        "passed": bool(alignable_case_ids),
        "witness_count": len(rows),
        "support_status_counts": dict(sorted(statuses.items())),
        "case_summaries": case_summaries,
        "alignable_case_count": len(alignable_case_ids),
        "alignment_audit_authorized": bool(alignable_case_ids),
        "semantic_quality_passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "sanitized counts and statuses no source or event text",
    }
    private_score = {
        "schema_version": BUNDLE_VERSION,
        "rows": rows,
        "privacy": "private opaque ids origins and statuses no source text",
    }
    receipts = _support_receipts(output)
    return audit, private_score, receipts


def _opaque_id(evaluation_id: str, kind: str, value: str) -> str:
    prefix = "jcase_" if kind == "case" else "wit_"
    return prefix + sha256_text(f"{evaluation_id}|alignment|{kind}|{value}")[:24]


def build_alignment_bundle(
    *,
    support_bundle: Mapping[str, Any],
    support_private_score: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
) -> dict[str, Any]:
    evaluation_id = str(support_bundle["evaluation_id"])
    origin_by_id = {
        str(row["witness_id"]): row for row in support_bundle["origins"]
    }
    status_by_id = {
        str(row["witness_id"]): str(row["support_status"])
        for row in support_private_score["rows"]
    }
    receipt_by_id = {
        str(row["witness_id"]): row for row in support_receipts["units"]
    }
    if set(origin_by_id) != set(status_by_id) or set(origin_by_id) != set(receipt_by_id):
        raise CandidateEvaluationError("support receipt coverage drifted")

    pool_cases = []
    mapping_rows = []
    filtered_receipts = []
    for case in support_bundle["cases"]:
        supported = [
            witness
            for witness in case["witnesses"]
            if status_by_id[str(witness["witness_id"])] == "supported"
        ]
        supported_origins = Counter(
            str(origin_by_id[str(row["witness_id"])]["origin"]) for row in supported
        )
        alignable = bool(supported_origins["reference"] and supported_origins["candidate"])
        opaque_case_id = (
            _opaque_id(evaluation_id, "case", str(case["case_id"])) if alignable else None
        )
        opaque_by_legacy = {
            str(row["witness_id"]): _opaque_id(
                evaluation_id, "witness", str(row["witness_id"])
            )
            for row in supported
        } if alignable else {}
        rendered: dict[str, list[dict[str, Any]]] = {"reference": [], "candidate": []}
        for witness in case["witnesses"]:
            legacy_id = str(witness["witness_id"])
            metadata = origin_by_id[legacy_id]
            opaque_id = opaque_by_legacy.get(legacy_id)
            mapping_rows.append(
                {
                    "case_id": opaque_case_id,
                    "legacy_case_id": case["case_id"],
                    "witness_id": opaque_id,
                    "legacy_witness_id": legacy_id,
                    "origin": metadata["origin"],
                    "support_status": status_by_id[legacy_id],
                    "segment_id": metadata["segment_id"],
                    "density_stratum": metadata["density_stratum"],
                    "event_sha256": metadata["event_sha256"],
                }
            )
            if opaque_id is None:
                continue
            rendered[str(metadata["origin"])].append(
                {
                    "witness_id": opaque_id,
                    "event": judge.compact_empty_event_fields(
                        copy.deepcopy(dict(witness["event"]))
                    ),
                }
            )
            filtered_receipts.append(
                {
                    **copy.deepcopy(dict(receipt_by_id[legacy_id])),
                    "case_id": opaque_case_id,
                    "witness_id": opaque_id,
                }
            )
        if alignable:
            pool_cases.append(
                {
                    "case_id": opaque_case_id,
                    "source_excerpt": case["source_excerpt"],
                    "event_set_a": sorted(
                        rendered["reference"], key=lambda row: str(row["witness_id"])
                    ),
                    "event_set_b": sorted(
                        rendered["candidate"], key=lambda row: str(row["witness_id"])
                    ),
                }
            )
    if not pool_cases:
        raise CandidateEvaluationError("no support-positive case can enter alignment")
    pool = {
        "schema_version": app_server_llm_judge.SHARED_WITNESS_POOL_VERSION,
        "seed_sha256": sha256_text(f"{evaluation_id}|supported-shared-pool"),
        "cases": sorted(pool_cases, key=lambda row: str(row["case_id"])),
        "privacy": "private analysis blinded no origin provenance",
    }
    errors = judge.validate_shared_witness_pool(pool)
    if errors:
        raise CandidateEvaluationError("shared alignment pool is invalid")
    receipts = {
        **dict(support_receipts),
        "units": sorted(
            filtered_receipts,
            key=lambda row: (str(row["case_id"]), str(row["witness_id"])),
        ),
    }
    base = judge.build_neutral_alignment_input(pool, receipts, permutation="base")
    canary = judge.build_neutral_alignment_input(
        pool, receipts, permutation="balanced_canary"
    )
    base_case_ids = [str(row["case_id"]) for row in base["cases"]]
    canary_case_ids = [str(row["case_id"]) for row in canary["cases"]]
    if canary_case_ids != list(reversed(base_case_ids)):
        raise CandidateEvaluationError("balanced case permutation drifted")
    canary_by_case = {str(row["case_id"]): row for row in canary["cases"]}
    for base_case in base["cases"]:
        base_ids = [str(row["witness_id"]) for row in base_case["witnesses"]]
        canary_ids = [
            str(row["witness_id"])
            for row in canary_by_case[str(base_case["case_id"])]["witnesses"]
        ]
        if canary_ids != list(reversed(base_ids)):
            raise CandidateEvaluationError("balanced witness permutation drifted")

    turns = []
    for permutation, value in zip(ALIGNMENT_PERMUTATIONS, (base, canary)):
        prompt = _alignment_prompt(value)
        schema = judge.neutral_alignment_output_schema(value)
        prompt_bytes = len(prompt.encode("utf-8"))
        schema_bytes = len(_canonical_json(schema).encode("utf-8"))
        if (
            prompt_bytes > ALIGNMENT_MAX_PROMPT_BYTES
            or schema_bytes > ALIGNMENT_MAX_SCHEMA_BYTES
        ):
            raise CandidateEvaluationError("alignment request size cap exceeded")
        turns.append(
            {
                "permutation": permutation,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
            }
        )
    return {
        "schema_version": BUNDLE_VERSION,
        "evaluation_id": evaluation_id,
        "pool": pool,
        "receipts": receipts,
        "mapping": {
            "schema_version": BUNDLE_VERSION,
            "rows": sorted(
                mapping_rows,
                key=lambda row: (str(row["legacy_case_id"]), str(row["legacy_witness_id"])),
            ),
        },
        "turns": turns,
        "counts": {
            "alignment_case_count": len(pool_cases),
            "aligned_witness_count": len(filtered_receipts),
            "checklist_field_count": len(judge.CHECKLIST_FIELDS),
            "counts_are_observed_not_targets": True,
        },
    }


def _f1(precision: float, recall: float) -> float:
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


def build_adjudication_bundle(
    *,
    base_output: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    alignment_bundle: Mapping[str, Any],
) -> dict[str, Any]:
    turns = {str(row["permutation"]): row for row in alignment_bundle["turns"]}
    base_input = turns["base"]["value"]
    canary_input = turns["balanced_canary"]["value"]
    packet = judge.build_disagreement_adjudication_input(
        base_input=base_input,
        base_output=base_output,
        canary_input=canary_input,
        canary_output=canary_output,
        support_receipts=alignment_bundle["receipts"],
    )
    if packet["adjudication_required"] is not True:
        return {
            "schema_version": BUNDLE_VERSION,
            "adjudication_required": False,
            "adjudication_call_cap": 1,
            "packet": packet,
            "turn": None,
        }
    value = judge.adjudication_alignment_input(
        base_input=base_input,
        adjudication_input=packet,
    )
    prompt = judge.build_disagreement_adjudication_prompt(
        adjudication_input=packet,
        adjudication_alignment=value,
    )
    schema = judge.neutral_alignment_output_schema(value)
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if (
        prompt_bytes > ADJUDICATION_MAX_PROMPT_BYTES
        or schema_bytes > ADJUDICATION_MAX_SCHEMA_BYTES
    ):
        raise CandidateEvaluationError("adjudication request size cap exceeded")
    return {
        "schema_version": BUNDLE_VERSION,
        "adjudication_required": True,
        "adjudication_call_cap": 1,
        "packet": packet,
        "turn": {
            "name": "observable_disagreement_adjudication",
            "value": value,
            "base_instructions": alignment_instructions(),
            "prompt": prompt,
            "schema": schema,
            "prompt_bytes": prompt_bytes,
            "schema_bytes": schema_bytes,
            "model": ADJUDICATION_MODEL,
            "effort": ADJUDICATION_EFFORT,
        },
    }


def score_alignment(
    *,
    base_output: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    alignment_bundle: Mapping[str, Any],
    support_bundle: Mapping[str, Any],
    support_private_score: Mapping[str, Any],
    production_amortized_total_token_ratio: float,
    adjudication_bundle: Mapping[str, Any] | None = None,
    adjudication_output: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    turns = {str(row["permutation"]): row for row in alignment_bundle["turns"]}
    try:
        base = judge.normalize_neutral_alignment_output(
            base_output, turns["base"]["value"]
        )
        canary = judge.normalize_neutral_alignment_output(
            canary_output, turns["balanced_canary"]["value"]
        )
    except Exception as exc:
        raise CandidateEvaluationError("alignment output validation failed") from exc
    base_by_case_raw = {str(row["case_id"]): row for row in base["cases"]}
    canary_by_case = {str(row["case_id"]): row for row in canary["cases"]}
    if set(base_by_case_raw) != set(canary_by_case):
        raise CandidateEvaluationError("alignment case coverage drifted")
    disagreements = judge.find_observable_alignment_disagreements(
        base_output=base_output,
        base_input=turns["base"]["value"],
        canary_output=canary_output,
        canary_input=turns["balanced_canary"]["value"],
        support_receipts=alignment_bundle["receipts"],
    )
    disagreement_case_ids = sorted(
        str(row["case_id"]) for row in disagreements["disagreements"]
    )
    if disagreement_case_ids != sorted(
        case_id
        for case_id in base_by_case_raw
        if _owner_projection(base_by_case_raw[case_id])
        != _owner_projection(canary_by_case[case_id])
    ):
        raise CandidateEvaluationError("observable disagreement projection drifted")
    adjudication_input = None
    if adjudication_bundle is not None:
        expected_bundle = build_adjudication_bundle(
            base_output=base_output,
            canary_output=canary_output,
            alignment_bundle=alignment_bundle,
        )
        if adjudication_bundle != expected_bundle:
            raise CandidateEvaluationError("adjudication bundle drifted")
        turn = adjudication_bundle.get("turn")
        adjudication_input = turn.get("value") if isinstance(turn, Mapping) else None
    if adjudication_output is not None and adjudication_input is None:
        raise CandidateEvaluationError("adjudication output has no frozen request")
    try:
        reconciled = judge.reconcile_neutral_alignment(
            base_output=base_output,
            base_input=turns["base"]["value"],
            canary_output=canary_output,
            canary_input=turns["balanced_canary"]["value"],
            support_receipts=alignment_bundle["receipts"],
            adjudication_output=adjudication_output,
            adjudication_input=adjudication_input,
        )
    except Exception as exc:
        raise CandidateEvaluationError("alignment reconciliation failed") from exc
    base_by_case = {str(row["case_id"]): row for row in reconciled["cases"]}
    consistency_issues = _alignment_consistency_issues(reconciled)
    abstention_case_ids = sorted(
        case_id
        for case_id, row in base_by_case.items()
        if _case_has_abstention(row)
    )

    mapping_rows = list(alignment_bundle["mapping"]["rows"])
    aligned_rows = [row for row in mapping_rows if row["witness_id"]]
    origin_by_id = {str(row["witness_id"]): str(row["origin"]) for row in aligned_rows}
    support_by_id = {
        str(row["witness_id"]): str(row["support_status"])
        for row in support_private_score["rows"]
    }
    case_metrics = []
    case_f1_values = []
    represented_total = 0
    reference_total = 0
    for case in support_bundle["cases"]:
        legacy_case_id = str(case["case_id"])
        rows = [row for row in mapping_rows if row["legacy_case_id"] == legacy_case_id]
        candidate_total = sum(row["origin"] == "candidate" for row in rows)
        candidate_supported = sum(
            row["origin"] == "candidate"
            and support_by_id[str(row["legacy_witness_id"])] == "supported"
            for row in rows
        )
        raw_reference_total = sum(row["origin"] == "reference" for row in rows)
        unsupported_reference = sum(
            row["origin"] == "reference"
            and support_by_id[str(row["legacy_witness_id"])] != "supported"
            for row in rows
        )
        opaque_case_id = next((str(row["case_id"]) for row in rows if row["case_id"]), None)
        represented = 0
        semantic_reference_units = raw_reference_total
        if opaque_case_id is not None:
            output_case = base_by_case[opaque_case_id]
            if output_case.get("status") != "abstain":
                groups = output_case["equivalence_groups"]
                reference_groups = [
                    group
                    for group in groups
                    if any(origin_by_id[witness_id] == "reference" for witness_id in group)
                ]
                represented = sum(
                    {origin_by_id[witness_id] for witness_id in group}
                    == {"reference", "candidate"}
                    for group in reference_groups
                )
                semantic_reference_units = len(reference_groups) + unsupported_reference
        precision = 1.0 if candidate_total == 0 else candidate_supported / candidate_total
        recall = (
            1.0
            if semantic_reference_units == 0
            else represented / semantic_reference_units
        )
        case_f1 = _f1(precision, recall)
        case_f1_values.append(case_f1)
        represented_total += represented
        reference_total += semantic_reference_units
        case_metrics.append(
            {
                "case_id": legacy_case_id,
                "density_stratum": case["density_stratum"],
                "candidate_witness_count": candidate_total,
                "source_supported_candidate_witness_count": candidate_supported,
                "reference_semantic_unit_count": semantic_reference_units,
                "strictly_equivalent_reference_semantic_unit_count": represented,
                "source_supported_candidate_precision": round(precision, 6),
                "strict_full_field_recall": round(recall, 6),
                "strict_full_field_f1": round(case_f1, 6),
            }
        )
    macro_f1 = sum(case_f1_values) / len(case_f1_values)
    all_candidate = sum(row["candidate_witness_count"] for row in case_metrics)
    all_supported_candidate = sum(
        row["source_supported_candidate_witness_count"] for row in case_metrics
    )
    global_precision = 1.0 if all_candidate == 0 else all_supported_candidate / all_candidate
    global_recall = 1.0 if reference_total == 0 else represented_total / reference_total
    global_f1 = _f1(global_precision, global_recall)
    all_reference_supported = all(
        row["support_status"] == "supported"
        for row in support_private_score["rows"]
        if row["origin"] == "reference"
    )
    grounding = support_bundle["grounding"]
    checks = {
        "permutation_projection_exact": not disagreement_case_ids,
        "observable_disagreement_adjudication_complete": (
            not disagreement_case_ids or reconciled["adjudication_call_count"] == 1
        ),
        "adjudication_call_cap_lte_1": reconciled["adjudication_call_count"] <= 1,
        "unresolved_alignment_cases_0": not reconciled["unresolved_cases_abstained"],
        "primary_abstention_count_0": not abstention_case_ids,
        "alignment_partition_relation_consistent": not consistency_issues,
        "all_reference_witnesses_source_supported": all_reference_supported,
        "strict_full_field_macro_f1_gte_0_97": macro_f1 >= QUALITY_THRESHOLD,
        "no_material_source_macro_regression": macro_f1 >= QUALITY_THRESHOLD,
        "exact_evidence_rate_1": grounding["exact_evidence_rate"] == 1.0,
        "metric_grounding_error_events_0": grounding[
            "metric_grounding_error_event_count"
        ] == 0,
        "event_cap_violations_0": grounding["event_cap_violation_count"] == 0,
        "production_amortized_total_token_ratio_lte_0_28":
        production_amortized_total_token_ratio <= TOKEN_RATIO_TARGET,
    }
    mismatch_counts = Counter(
        field
        for row in reconciled["cases"]
        for pair in row["alignment_pairs"]
        for field in pair.get("mismatch_fields") or []
    )
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCORE_VERSION,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "metrics": {
            "case_count": len(case_metrics),
            "alignment_case_count": len(base_by_case),
            "permutation_exact_case_count": len(base_by_case) - len(disagreement_case_ids),
            "observable_disagreement_case_count": len(disagreement_case_ids),
            "adjudication_call_count": reconciled["adjudication_call_count"],
            "unresolved_alignment_case_count": len(
                reconciled["unresolved_cases_abstained"]
            ),
            "abstention_case_count": len(abstention_case_ids),
            "alignment_consistency_issue_count": len(consistency_issues),
            "reference_semantic_unit_count": reference_total,
            "strictly_equivalent_reference_semantic_unit_count": represented_total,
            "candidate_witness_count": all_candidate,
            "source_supported_candidate_witness_count": all_supported_candidate,
            "source_supported_candidate_precision": round(global_precision, 6),
            "strict_full_field_recall": round(global_recall, 6),
            "strict_full_field_f1": round(global_f1, 6),
            "development_strict_full_field_macro_f1": round(macro_f1, 6),
            "production_amortized_total_token_ratio": round(
                production_amortized_total_token_ratio, 6
            ),
        },
        "case_metrics": case_metrics,
        "mismatch_field_counts": dict(sorted(mismatch_counts.items())),
        "permutation_disagreement_case_ids": disagreement_case_ids,
        "observable_disagreements": disagreements,
        "adjudication": {
            "required": bool(disagreement_case_ids),
            "call_count": reconciled["adjudication_call_count"],
            "call_cap": reconciled["adjudication_call_cap"],
            "unresolved_case_ids": reconciled["unresolved_cases_abstained"],
            "majority_voting_used": reconciled["majority_voting_used"],
        },
        "abstention_case_ids": abstention_case_ids,
        "alignment_consistency_issues": consistency_issues,
        "development_winner_frozen": not failed,
        "holdout_authorized": not failed,
        "overall_evaluation_complete": False,
        "production_mutated": False,
        "privacy": "sanitized counts metrics and opaque ids no source or event text",
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(configured.__file__).resolve(),
                Path(judge.__file__).resolve(),
                Path(app_server_llm_judge.__file__).resolve(),
                Path(runtime_verifier.__file__).resolve(),
            },
            key=str,
        )
    )


def _direct_record_digest(records: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json(list(records)).encode("utf-8")).hexdigest()


def freeze_protocol(output_dir: Path = DEFAULT_PROTOCOL_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    protocol_path = root / "protocol.json"
    lock_path = root / "runtime-lock.json"
    receipt_path = root / "runtime-lock-receipt.json"
    predecessor_records = []
    for predecessor_name in ("runtime-lock.json", "runtime-lock-receipt.json"):
        predecessor_path = PREDECESSOR_PROTOCOL_ROOT / predecessor_name
        if predecessor_path.is_file():
            predecessor_records.append(_record(predecessor_path))
    protocol = {
        "schema_version": PROTOCOL_VERSION,
        "frozen_at": now_iso(),
        "support_model": SUPPORT_MODEL,
        "support_effort": SUPPORT_EFFORT,
        "alignment_model": ALIGNMENT_MODEL,
        "alignment_effort": ALIGNMENT_EFFORT,
        "adjudication_model": ADJUDICATION_MODEL,
        "adjudication_effort": ADJUDICATION_EFFORT,
        "adjudication_call_cap": 1,
        "quality_threshold": QUALITY_THRESHOLD,
        "token_ratio_target": TOKEN_RATIO_TARGET,
        "support_instructions_sha256": sha256_text(support_instructions()),
        "alignment_instructions_sha256": sha256_text(alignment_instructions()),
        "adjudication_contract_sha256": sha256_text(
            "|".join(
                (
                    judge.ADJUDICATION_INPUT_VERSION,
                    alignment_instructions(),
                    *judge.CHECKLIST_FIELDS,
                )
            )
        ),
        "checklist_fields": list(judge.CHECKLIST_FIELDS),
        "alignment_permutations": list(ALIGNMENT_PERMUTATIONS),
        "candidate_counts_are_observed_not_targets": True,
        "side_labels_in_model_input": False,
        "system_identity_in_model_input": False,
        "empty_event_fields_omitted_only": True,
        "semantic_normalization_allowed": False,
        "semantic_regex_or_keyword_rules_allowed": False,
        "embeddings_allowed": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "supersedes_zero_turn_protocol": predecessor_records,
        "supersession_reason": (
            "remove_numbered_import_chain_and_add_capped_observable_disagreement_adjudication"
        ),
    }
    if protocol_path.exists():
        protocol["frozen_at"] = _load_json(protocol_path, "protocol")["frozen_at"]
    _write_immutable(protocol_path, protocol)
    runtime_records = [_record(path) for path in _runtime_files()]
    judge_records = [
        _record(FROZEN_SUPPORT_JUDGE_ROOT / "runtime-lock.json"),
        _record(FROZEN_ALIGNMENT_JUDGE_ROOT / "runtime-lock.json"),
    ]
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": protocol["frozen_at"],
        "protocol": _record(protocol_path),
        "runtime_files": runtime_records,
        "frozen_judge_runtime_locks": judge_records,
        "superseded_predecessor": predecessor_records,
        "direct_record_digest": _direct_record_digest(
            [*runtime_records, *judge_records, *predecessor_records, _record(protocol_path)]
        ),
        "semantic_turn_count": 0,
        "production_mutation_allowed": False,
    }
    _write_immutable(lock_path, lock)
    _write_immutable(
        receipt_path,
        {
            "schema_version": LOCK_VERSION,
            "manifest": _record(lock_path),
            "direct_record_digest": lock["direct_record_digest"],
            "semantic_turn_count": 0,
        },
    )
    return verify_protocol(root)


def verify_protocol(output_dir: Path = DEFAULT_PROTOCOL_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    protocol_path = root / "protocol.json"
    lock_path = root / "runtime-lock.json"
    receipt_path = root / "runtime-lock-receipt.json"
    protocol = _load_json(protocol_path, "protocol")
    lock = _load_json(lock_path, "runtime lock")
    receipt = _load_json(receipt_path, "runtime lock receipt")
    expected_runtime_paths = {str(path) for path in _runtime_files()}
    actual_runtime_paths = {
        str(Path(record["path"]).expanduser().resolve())
        for record in lock.get("runtime_files") or []
    }
    expected_judge_paths = {
        str((FROZEN_SUPPORT_JUDGE_ROOT / "runtime-lock.json").resolve()),
        str((FROZEN_ALIGNMENT_JUDGE_ROOT / "runtime-lock.json").resolve()),
    }
    actual_judge_paths = {
        str(Path(record["path"]).expanduser().resolve())
        for record in lock.get("frozen_judge_runtime_locks") or []
    }
    expected_predecessor_paths = {
        str((PREDECESSOR_PROTOCOL_ROOT / name).resolve())
        for name in ("runtime-lock.json", "runtime-lock-receipt.json")
        if (PREDECESSOR_PROTOCOL_ROOT / name).is_file()
    }
    actual_predecessor_paths = {
        str(Path(record["path"]).expanduser().resolve())
        for record in lock.get("superseded_predecessor") or []
    }
    if (
        protocol.get("schema_version") != PROTOCOL_VERSION
        or protocol.get("quality_threshold") != QUALITY_THRESHOLD
        or protocol.get("token_ratio_target") != TOKEN_RATIO_TARGET
        or protocol.get("checklist_fields") != list(judge.CHECKLIST_FIELDS)
        or protocol.get("support_instructions_sha256")
        != sha256_text(support_instructions())
        or protocol.get("alignment_instructions_sha256")
        != sha256_text(alignment_instructions())
        or protocol.get("adjudication_model") != ADJUDICATION_MODEL
        or protocol.get("adjudication_effort") != ADJUDICATION_EFFORT
        or protocol.get("adjudication_call_cap") != 1
        or protocol.get("adjudication_contract_sha256")
        != sha256_text(
            "|".join(
                (
                    judge.ADJUDICATION_INPUT_VERSION,
                    alignment_instructions(),
                    *judge.CHECKLIST_FIELDS,
                )
            )
        )
        or lock.get("schema_version") != LOCK_VERSION
        or lock.get("semantic_turn_count") != 0
        or lock.get("production_mutation_allowed") is not False
        or actual_runtime_paths != expected_runtime_paths
        or actual_judge_paths != expected_judge_paths
        or actual_predecessor_paths != expected_predecessor_paths
        or protocol.get("supersedes_zero_turn_protocol")
        != lock.get("superseded_predecessor")
        or protocol.get("supersession_reason")
        != "remove_numbered_import_chain_and_add_capped_observable_disagreement_adjudication"
    ):
        raise CandidateEvaluationError("candidate evaluator protocol contract drifted")
    records = [
        *lock["runtime_files"],
        *lock["frozen_judge_runtime_locks"],
        *lock.get("superseded_predecessor", []),
        lock["protocol"],
    ]
    if any(not HASH_CACHE.verify_record(record) for record in records):
        raise CandidateEvaluationError("candidate evaluator direct artifact drifted")
    if lock.get("direct_record_digest") != _direct_record_digest(records):
        raise CandidateEvaluationError("candidate evaluator direct record digest drifted")
    if (
        receipt.get("manifest") != _record(lock_path)
        or receipt.get("direct_record_digest") != lock["direct_record_digest"]
        or receipt.get("semantic_turn_count") != 0
    ):
        raise CandidateEvaluationError("candidate evaluator receipt drifted")
    return {
        "root": root,
        "protocol": protocol,
        "runtime_lock": lock_path,
        "receipt": receipt_path,
        "semantic_turn_count": 0,
    }
