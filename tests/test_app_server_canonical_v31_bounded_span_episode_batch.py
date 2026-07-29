from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from research_factory import app_server_canonical_v31_bounded_span_episode_batch as adapter


EPOCH12_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "canonical-v31-epoch12-canonical-consistency-authority-recovery-v1"
)
EPOCH12_FAILED_TURN = "authority12_canonical_labels_6f0779f9233160dc06d14b36"


def _quality(words: int) -> dict:
    return {
        "artifact_type": "dialogue_transcript",
        "boilerplate_risk": "low",
        "substantive_word_count": words,
        "transcript_preparation_id": "prep_bounded_span_fixture",
    }


def _episode(text: str) -> dict:
    return {
        "episode_id": "ep_bounded_span_fixture",
        "source_name": "Fixture Podcast",
        "episode_title": "Bounded evidence spans",
        "context_summary": "A system performance discussion.",
        "speaker_map": [
            {"name": "Host", "role": "host"},
            {"name": "Guest", "role": "guest", "affiliation": "New Atlas"},
        ],
        "section_map": [{"section_id": "discussion", "segment_ids": ["seg_fixture"]}],
        "entity_seed": {"organizations": ["New Atlas"]},
        "concept_seed": ["inference latency"],
        "extraction_guidance": "Code grounded technical propositions.",
        "excluded_source_context": [],
        "segments": [
            {
                "segment_id": "seg_fixture",
                "segment_text": text,
                "segment_quality": _quality(len(text.split())),
                "density_stratum": "coded",
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


def _event(span_id: str) -> dict:
    return {
        "event_type": "capability_claim",
        "event_subtype": "inference_performance",
        "actor": {
            "name": "New Atlas",
            "actor_type": "organization",
            "affiliation": None,
            "role": "system developer",
        },
        "speaker_context": {
            "name": "Host",
            "role": "host",
            "affiliation": None,
            "confidence": 0.96,
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
            "rationale": "The speaker directly reports a measured system improvement.",
        },
        "target": {
            "raw_target": "inference latency",
            "candidate_concept": "inference efficiency",
            "canonical_concept": None,
            "concept_confidence": 0.92,
        },
        "surface_terms": ["inference latency"],
        "frames": ["performance improvement"],
        "model_names": [],
        "product_names": ["New Atlas"],
        "organizations": ["New Atlas"],
        "people": [],
        "stance": "supportive",
        "claim_text": "New Atlas cuts inference latency by 40 percent.",
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
        "signal_reason": "The source states the quantified latency reduction.",
        "exclusion_flags": [],
        "quality_flags": [],
        "confidence": 0.95,
        "audit_notes": "All fields are supported by the selected span.",
        "evidence_span_id": span_id,
    }


def _coded_output(request: dict) -> dict:
    source = request["private_input"]["segments"][0]
    span = source["evidence_spans"][0]
    event = _event(span["evidence_span_id"])
    receipts = [
        {
            "unit_id": unit["unit_id"],
            "reviewed": True,
            "grounded_event_count": int(
                unit["unit_id"] == span["evidence_start_unit_id"]
            ),
            "grounded_concept_candidate_count": 0,
            "unresolved_count": 0,
        }
        for unit in source["units"]
    ]
    return {
        "episode_id": request["episode_id"],
        "segments": [
            {
                "segment_id": source["segment_id"],
                "extraction_status": "coded",
                "segment_source_context": {
                    "kind": "substantive_dialogue",
                    "confidence": 0.99,
                    "rationale": "The segment contains a direct technical claim.",
                },
                "discourse_events": [event],
                "concept_candidates": [],
                "rejected_candidates": [],
                "no_signal_reason": None,
                "overall_confidence": 0.95,
                "needs_review": False,
                "review_reason": None,
                "unit_receipts": receipts,
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
            }
        ],
    }


def test_partition_and_span_catalog_are_exact_bounded_and_deterministic() -> None:
    text = ("A" * 440) + "\n  " + ("B" * 440) + "\n" + ("C" * 440)
    request = adapter.prepare_episode_batches(
        _episode(text), batch_size=3, thread_mode="new_thread"
    )[0]
    source = request["private_input"]["segments"][0]
    assert request == adapter.validate_prepared_request(request)
    assert [len(unit["text"]) for unit in source["units"]] == [450, 450, 424]
    assert all(
        source["segment_text"][unit["start_char"] : unit["end_char"]] == unit["text"]
        for unit in source["units"]
    )
    assert all(
        span["character_count"] <= adapter.CANONICAL_EVIDENCE_MAX_CHARS
        and source["segment_text"][span["start_char"] : span["end_char"]]
        and span["character_count"] == span["end_char"] - span["start_char"]
        for span in source["evidence_spans"]
    )
    repeated = adapter.prepare_episode_batches(
        _episode(text), batch_size=3, thread_mode="new_thread"
    )[0]
    assert request == repeated


def test_schema_requires_one_bounded_span_and_removes_open_ended_pair() -> None:
    text = "Host: New Atlas cuts inference latency by 40 percent.\n"
    request = adapter.prepare_episode_batches(
        _episode(text), batch_size=3, thread_mode="new_thread"
    )[0]
    event_schema = request["output_schema"]["properties"]["segments"]["items"][
        "properties"
    ]["discourse_events"]["items"]
    assert "evidence_span_id" in event_schema["required"]
    assert "evidence_start_unit_id" not in event_schema["properties"]
    assert "evidence_end_unit_id" not in event_schema["properties"]
    assert "evidence_start_unit_id" not in request["base_instructions"]
    assert "evidence_end_unit_id" not in request["base_instructions"]


def test_projection_maps_span_to_exact_canonical_evidence_without_semantic_change() -> None:
    text = "Host: New Atlas cuts inference latency by 40 percent.\n"
    request = adapter.prepare_episode_batches(
        _episode(text), batch_size=3, thread_mode="new_thread"
    )[0]
    output = _coded_output(request)
    untouched = copy.deepcopy(output)
    projected = adapter.validate_and_project_output(request, output)
    label = projected["labels"][0]
    event = label["discourse_events"][0]
    assert output == untouched
    assert event["evidence"] == text.rstrip("\n")
    assert event["evidence_start"] == 0
    assert event["evidence_end"] == len(text.rstrip("\n"))
    assert projected["fidelity"]["all_emitted_semantic_values_preserved"] is True
    assert projected["provenance"]["segments"][0]["discourse_events"][0][
        "evidence_span_id"
    ] == output["segments"][0]["discourse_events"][0]["evidence_span_id"]


def test_cross_segment_or_tampered_span_is_rejected() -> None:
    text = "Host: New Atlas cuts inference latency by 40 percent.\n"
    request = adapter.prepare_episode_batches(
        _episode(text), batch_size=3, thread_mode="new_thread"
    )[0]
    output = _coded_output(request)
    output["segments"][0]["discourse_events"][0]["evidence_span_id"] = "E9999S0000E0000"
    with pytest.raises(adapter.CanonicalV31OutputError):
        adapter.validate_and_project_output(request, output)


def test_epoch12_failed_range_is_unrepresentable_but_nearby_bounded_spans_exist() -> None:
    request_path = (
        EPOCH12_ROOT
        / "prepared-turns"
        / EPOCH12_FAILED_TURN
        / "request.private.json"
    )
    raw_path = EPOCH12_ROOT / "turns" / EPOCH12_FAILED_TURN / "output.private.json"
    original = json.loads(request_path.read_text(encoding="utf-8"))
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    episode = copy.deepcopy(original["episode_context"])
    episode["segments"] = [
        {
            key: copy.deepcopy(segment[key])
            for key in (
                "segment_id",
                "segment_text",
                "segment_quality",
                "density_stratum",
                "boundaries",
            )
        }
        for segment in original["private_input"]["segments"]
    ]
    rebuilt = adapter.prepare_episode_batches(
        episode, batch_size=8, thread_mode="new_thread"
    )[0]
    source = rebuilt["private_input"]["segments"][0]
    failed = raw["segments"][0]["discourse_events"][4]
    old_source = original["private_input"]["segments"][0]
    old_units = {unit["unit_id"]: unit for unit in old_source["units"]}
    failed_start = old_units[failed["evidence_start_unit_id"]]["start_char"]
    failed_end = old_units[failed["evidence_end_unit_id"]]["end_char"]
    assert failed_end - failed_start == 1117
    assert all(span["character_count"] <= 1000 for span in source["evidence_spans"])
    assert any(
        span["start_char"] <= failed_start < span["end_char"]
        and span["character_count"] >= 900
        for span in source["evidence_spans"]
    )
    assert len(source["units"]) < len(old_source["units"])


def test_request_tamper_is_rejected_fail_closed() -> None:
    request = adapter.prepare_episode_batches(
        _episode("Host: New Atlas cuts inference latency by 40 percent.\n"),
        batch_size=3,
        thread_mode="new_thread",
    )[0]
    request["private_input"]["segments"][0]["evidence_spans"][0][
        "character_count"
    ] += 1
    with pytest.raises(adapter.CanonicalV31EpisodeBatchError):
        adapter.validate_prepared_request(request)
