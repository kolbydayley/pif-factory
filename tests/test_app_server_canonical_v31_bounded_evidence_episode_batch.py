from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_bounded_evidence_episode_batch as bounded
from research_factory import app_server_canonical_v31_episode_batch as base


def _episode() -> dict:
    text = "Host: Welcome to the show.\nGuest: Thanks for having me.\n"
    return {
        "episode_id": "ep_bounded_evidence_fixture",
        "source_name": "Fixture Podcast",
        "episode_title": "Bounded evidence fixture",
        "context_summary": "This fixture contains only show setup.",
        "speaker_map": [
            {"name": "Host", "role": "host"},
            {"name": "Guest", "role": "guest"},
        ],
        "section_map": [
            {"section_id": "setup", "segment_ids": ["seg_bounded_fixture"]}
        ],
        "entity_seed": {"organizations": []},
        "concept_seed": [],
        "extraction_guidance": "Do not code show setup.",
        "excluded_source_context": [],
        "segments": [
            {
                "segment_id": "seg_bounded_fixture",
                "segment_text": text,
                "segment_quality": {
                    "artifact_type": "dialogue_transcript",
                    "boilerplate_risk": "low",
                    "substantive_word_count": len(text.split()),
                    "transcript_preparation_id": "prep_bounded_fixture",
                },
                "density_stratum": "no_signal",
                "boundaries": [
                    {
                        "window_id": 0,
                        "chunk_index": 0,
                        "owner_start": 0,
                        "owner_end": len(text),
                        "extract_start": 0,
                        "extract_end": len(text),
                    }
                ],
            }
        ],
    }


def _all_no_signal_output(request: dict) -> dict:
    packet = json.loads(request["prompt"].split("\n", 1)[1])
    return {
        "episode_id": packet["episode_id"],
        "segments": [
            {
                "segment_id": segment["segment_id"],
                "extraction_status": "no_signal",
                "segment_source_context": {
                    "kind": "show_setup",
                    "confidence": 1.0,
                    "rationale": "The segment contains only show setup.",
                },
                "discourse_events": [],
                "concept_candidates": [],
                "rejected_candidates": [],
                "no_signal_reason": "No grounded discourse event is present.",
                "overall_confidence": 1.0,
                "needs_review": False,
                "review_reason": None,
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "reviewed": True,
                        "grounded_event_count": 0,
                        "grounded_concept_candidate_count": 0,
                        "unresolved_count": 0,
                    }
                    for unit in segment["source_units"]
                ],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
            }
            for segment in packet["segments"]
        ],
    }


def _request() -> dict:
    requests = bounded.prepare_episode_batches(
        _episode(), batch_size=3, thread_mode="new_thread"
    )
    assert len(requests) == 1
    return requests[0]


def test_bounded_request_injects_one_exact_existing_schema_constraint() -> None:
    request = _request()

    assert request["base_instructions"].endswith(bounded.EVIDENCE_BOUND_INSTRUCTION)
    assert request["base_instructions"].count(bounded.EVIDENCE_BOUND_INSTRUCTION) == 1
    assert "at most 1000 source characters" in request["base_instructions"]
    assert "exceed 1000 characters" in request["base_instructions"]
    assert request["base_instructions_sha256"] == base.sha256_text(
        request["base_instructions"]
    )
    assert bounded.validate_prepared_request(request) == request


def test_bounded_request_tamper_is_rejected() -> None:
    request = _request()
    request["base_instructions"] = request["base_instructions"].replace(
        "at most 1000 source characters", "at most 1001 source characters"
    )
    request["base_instructions_sha256"] = base.sha256_text(
        request["base_instructions"]
    )

    with pytest.raises(
        bounded.CanonicalV31EpisodeBatchError,
        match="evidence-bound instructions drifted",
    ):
        bounded.validate_prepared_request(request)


def test_bounded_request_projects_with_unchanged_canonical_validator() -> None:
    request = _request()
    projected = bounded.validate_and_project_output(
        request, _all_no_signal_output(request)
    )

    assert [row["segment_id"] for row in projected["labels"]] == [
        "seg_bounded_fixture"
    ]
    assert projected["labels"][0]["extraction_status"] == "no_signal"
    assert projected["labels"][0]["discourse_events"] == []


def test_bounded_adapter_does_not_mutate_frozen_base_adapter() -> None:
    base_path = Path(base.__file__).resolve()
    before = hashlib.sha256(base_path.read_bytes()).hexdigest()

    request = _request()
    bounded.validate_and_project_output(request, _all_no_signal_output(request))

    assert hashlib.sha256(base_path.read_bytes()).hexdigest() == before
    binding = bounded.build_six_arm_matrix_binding()
    assert binding["base_adapter_module_sha256"] == before
    assert binding["canonical_evidence_max_chars"] == 1000
    assert binding["semantic_postprocessing"] is False
    assert binding["deterministic_semantic_pruning"] is False
