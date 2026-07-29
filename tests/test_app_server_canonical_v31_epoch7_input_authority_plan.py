from __future__ import annotations

import copy
import hashlib
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_epoch7_input_authority_plan as authority


def _quality() -> dict:
    return {
        "artifact_type": "dialogue_transcript",
        "boilerplate_risk": "low",
        "substantive_word_count": 12,
        "transcript_preparation_id": "prep_fixture_authority",
    }


def _coded_label(*, episode_id: str, segment_id: str, text: str) -> dict:
    evidence = "Latency fell by 40 percent"
    start = text.index(evidence)
    return {
        "schema_version": "ai_discourse_v3_1",
        "segment_id": segment_id,
        "episode_id": episode_id,
        "extraction_status": "coded",
        "segment_quality": _quality(),
        "segment_source_context": {
            "kind": "substantive_dialogue",
            "confidence": 0.99,
            "rationale": "The segment directly states a substantive technical result.",
        },
        "discourse_events": [
            {
                "event_type": "capability_claim",
                "event_subtype": "latency_reduction",
                "actor": {
                    "name": "Atlas",
                    "actor_type": "organization",
                    "affiliation": None,
                    "role": "system developer",
                },
                "speaker_context": {
                    "name": "Guest",
                    "role": "guest",
                    "affiliation": "Atlas",
                    "confidence": 0.95,
                },
                "reported_actor": {
                    "name": "",
                    "actor_type": "none",
                    "affiliation": None,
                    "confidence": 1.0,
                },
                "source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 0.99,
                    "rationale": "The evidence is a direct statement in the discussion.",
                },
                "target": {
                    "raw_target": "latency",
                    "candidate_concept": "inference latency",
                    "canonical_concept": None,
                    "concept_confidence": 0.9,
                },
                "surface_terms": ["latency"],
                "frames": ["performance improvement"],
                "model_names": [],
                "product_names": ["Atlas"],
                "organizations": ["Atlas"],
                "people": [],
                "stance": "supportive",
                "claim_text": "Atlas reduced inference latency by forty percent.",
                "claim_type": "comparative",
                "certainty": "high",
                "temporal_horizon": "present",
                "causal_mechanism": "",
                "counterclaim": "",
                "metric": {
                    "value": "40",
                    "unit": "percent",
                    "comparator": None,
                    "direction": "decrease",
                    "raw_text": "40 percent",
                },
                "signal_reason": (
                    "The source directly reports a quantified reduction in system latency."
                ),
                "exclusion_flags": [],
                "quality_flags": [],
                "evidence": evidence,
                "evidence_start": start,
                "evidence_end": start + len(evidence),
                "confidence": 0.94,
                "audit_notes": "The metric and event are grounded in the exact evidence span.",
            }
        ],
        "concept_candidates": [],
        "rejected_candidates": [],
        "no_signal_reason": None,
        "overall_confidence": 0.94,
        "needs_review": False,
        "review_reason": None,
    }


def _no_signal_label(*, episode_id: str, segment_id: str) -> dict:
    return {
        "schema_version": "ai_discourse_v3_1",
        "segment_id": segment_id,
        "episode_id": episode_id,
        "extraction_status": "no_signal",
        "segment_quality": _quality(),
        "segment_source_context": {
            "kind": "show_setup",
            "confidence": 1.0,
            "rationale": "The segment contains only a greeting and show setup.",
        },
        "discourse_events": [],
        "concept_candidates": [],
        "rejected_candidates": [],
        "no_signal_reason": "The segment has no grounded research event.",
        "overall_confidence": 1.0,
        "needs_review": False,
        "review_reason": None,
    }


def _fixture_turn(tmp_path: Path) -> tuple[dict, dict, dict]:
    episode_id = "ep_authority_fixture"
    coded_id = "seg_authority_coded"
    setup_id = "seg_authority_setup"
    valid_id = "seg_authority_preserved"
    texts = {
        coded_id: "Guest: Latency fell by 40 percent after the update.\n",
        setup_id: "Host: Welcome to the show.\n",
        valid_id: "Guest: The release is available today.\n",
    }
    raw_path = tmp_path / "raw.txt"
    prepared_path = tmp_path / "prepared.txt"
    raw_path.write_text("raw authority fixture", encoding="utf-8")
    prepared_path.write_text("prepared authority fixture", encoding="utf-8")
    segment_records = []
    for index, (segment_id, text) in enumerate(texts.items()):
        path = tmp_path / f"{segment_id}.txt"
        path.write_text(text, encoding="utf-8")
        record = authority._record(path, project_root=tmp_path)
        segment_records.append(
            {
                "segment_id": segment_id,
                "segment_index": index,
                "text_sha256": record["sha256"],
                "within_prepared_text_bounds": False,
                "prepared_text_projection_exact": False,
                "artifact": record,
            }
        )
    full_text = "\n\n".join(texts.values())
    coded = _coded_label(
        episode_id=episode_id, segment_id=coded_id, text=texts[coded_id]
    )
    no_signal = _no_signal_label(episode_id=episode_id, segment_id=setup_id)
    raw_record = authority._record(raw_path, project_root=tmp_path)
    prepared_record = authority._record(prepared_path, project_root=tmp_path)
    turn_input = {
        "schema_version": authority.TURN_VERSION,
        "episode_id": episode_id,
        "episode_metadata": {
            "episode_id": episode_id,
            "transcript_id": "tr_authority_fixture",
            "transcript_preparation_id": "prep_authority_fixture",
        },
        "full_segmented_episode_text": full_text,
        "full_source_binding": {
            "canonical_transcript": {
                "transcript_id": "tr_authority_fixture",
                "preparation_id": "prep_authority_fixture",
                "raw_text_sha256": raw_record["sha256"],
                "prepared_text_sha256": prepared_record["sha256"],
            },
            "raw_transcript": raw_record,
            "prepared_transcript": prepared_record,
            "full_segmented_episode_text_sha256": hashlib.sha256(
                full_text.encode("utf-8")
            ).hexdigest(),
            "full_segmented_episode_text_chars": len(full_text),
            "source_assembly_policy": "fixture exact source assembly",
            "overlapping_segment_count": 0,
            "within_prepared_text_bounds_count": 0,
            "exact_prepared_text_projection_count": 0,
            "normalized_projection_count": 3,
            "segments": segment_records,
        },
        "original_context": {
            "episode_id": episode_id,
            "source_name": "Fixture Source",
            "episode_title": "Fixture Episode",
            "context_summary": "A technical discussion.",
            "speaker_map": [{"name": "Guest", "role": "guest"}],
            "section_map": [
                {"section_id": "discussion", "segment_ids": [coded_id]}
            ],
            "entity_seed": {"organizations": ["Atlas"]},
            "concept_seed": ["inference latency"],
            "extraction_guidance": "Code grounded technical claims only.",
        },
        "original_context_sha256": hashlib.sha256(
            authority._canonical_json(
                {
                    "episode_id": episode_id,
                    "source_name": "Fixture Source",
                    "episode_title": "Fixture Episode",
                    "context_summary": "A technical discussion.",
                    "speaker_map": [{"name": "Guest", "role": "guest"}],
                    "section_map": [
                        {"section_id": "discussion", "segment_ids": [coded_id]}
                    ],
                    "entity_seed": {"organizations": ["Atlas"]},
                    "concept_seed": ["inference latency"],
                    "extraction_guidance": "Code grounded technical claims only.",
                }
            ).encode("ascii")
        ).hexdigest(),
        "missing_context_fields": ["excluded_source_context"],
        "reference_repairs": [
            {
                "segment_id": coded_id,
                "segment_text": texts[coded_id],
                "segment_text_sha256": hashlib.sha256(
                    texts[coded_id].encode("utf-8")
                ).hexdigest(),
                "legacy_reference_label": copy.deepcopy(coded),
                "legacy_reference_label_sha256": hashlib.sha256(
                    authority._canonical_json(coded).encode("ascii")
                ).hexdigest(),
                "validation_diagnostic_path": "$.discourse_events[0].metric",
            },
            {
                "segment_id": setup_id,
                "segment_text": texts[setup_id],
                "segment_text_sha256": hashlib.sha256(
                    texts[setup_id].encode("utf-8")
                ).hexdigest(),
                "legacy_reference_label": copy.deepcopy(no_signal),
                "legacy_reference_label_sha256": hashlib.sha256(
                    authority._canonical_json(no_signal).encode("ascii")
                ).hexdigest(),
                "validation_diagnostic_path": "$.segment_source_context",
            },
        ],
        "invalid_reference_segment_ids": [coded_id, setup_id],
        "preserved_valid_reference_segment_ids": [valid_id],
        "privacy": "private full transcript and development reference labels",
    }
    output = {
        "schema_version": authority.OUTPUT_VERSION,
        "episode_id": episode_id,
        "excluded_source_context": ["opening greeting"],
        "repaired_reference_labels": [coded, no_signal],
    }
    schema = authority._output_schema(
        episode_id=episode_id,
        invalid_segment_ids=[coded_id, setup_id],
    )
    return turn_input, output, schema


def test_full_canonical_coded_and_no_signal_output_is_accepted(tmp_path: Path) -> None:
    turn_input, output, schema = _fixture_turn(tmp_path)

    authority._verify_turn_source_lineage(turn_input, project_root=tmp_path)
    validated = authority.validate_authority_output(
        output, turn_input=turn_input, output_schema=schema
    )

    assert [row["segment_id"] for row in validated["repaired_reference_labels"]] == [
        "seg_authority_coded",
        "seg_authority_setup",
    ]


def test_output_requires_exact_frozen_reference_order(tmp_path: Path) -> None:
    turn_input, output, schema = _fixture_turn(tmp_path)
    output["repaired_reference_labels"].reverse()

    with pytest.raises(
        authority.CanonicalV31Epoch7InputAuthorityPlanError,
        match="order or membership",
    ):
        authority.validate_authority_output(
            output, turn_input=turn_input, output_schema=schema
        )


def test_metric_must_be_exactly_grounded_in_evidence(tmp_path: Path) -> None:
    turn_input, output, schema = _fixture_turn(tmp_path)
    output["repaired_reference_labels"][0]["discourse_events"][0]["metric"][
        "raw_text"
    ] = "41 percent"

    with pytest.raises(
        authority.CanonicalV31Epoch7InputAuthorityPlanError,
        match="current canonical validation",
    ):
        authority.validate_authority_output(
            output, turn_input=turn_input, output_schema=schema
        )


def test_direct_source_artifact_tamper_is_rejected(tmp_path: Path) -> None:
    turn_input, _output, _schema = _fixture_turn(tmp_path)
    segment_path = Path(
        turn_input["full_source_binding"]["segments"][0]["artifact"]["path"]
    )
    segment_path.write_text("tampered", encoding="utf-8")

    with pytest.raises(
        authority.CanonicalV31Epoch7InputAuthorityPlanError,
        match="record drifted",
    ):
        authority._verify_turn_source_lineage(turn_input, project_root=tmp_path)


def test_reference_partition_substitution_is_rejected(tmp_path: Path) -> None:
    turn_input, _output, _schema = _fixture_turn(tmp_path)
    turn_input["preserved_valid_reference_segment_ids"] = ["seg_unknown"]

    with pytest.raises(
        authority.CanonicalV31Epoch7InputAuthorityPlanError,
        match="reference partition",
    ):
        authority._verify_turn_source_lineage(turn_input, project_root=tmp_path)


def test_four_turn_capacity_policy_is_bounded_and_nonexecutable() -> None:
    policy = authority._capacity_policy(
        {
            "source_maximum_total_tokens_per_turn": 102_000,
            "quota_points_per_million_tokens": 17,
            "minimum_remaining_reserve_percent": 20,
        },
        exact_turn_count=4,
    )

    assert policy["phase_total_token_bound"] == 408_000
    assert policy["projected_phase_quota_points"] == 7
    assert policy["semantic_retry_count"] == 0
    assert policy["operator_authorization_present"] is False
    assert policy["executable"] is False
    assert policy["holdout_authorized"] is False
    assert policy["production_mutation_allowed"] is False
