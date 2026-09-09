from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from research_factory.headless_codex import (
    _finalize_submission_failure,
    _provider_pressure_signals,
    _usage_profile_from_jsonl,
    resolve_codex_binary,
)
from research_factory.instrumented_backfill import EXPECTED_MUTATION_TABLES
from research_factory.labels import (
    label_pack_provenance,
    load_label_pack,
    render_prompt,
)
from research_factory.worker import (
    EPISODE_CONTEXT_SCHEMA_VERSION,
    LABEL_EPISODE_CONTEXT_FIELDS,
    render_episode_context_prompt,
    slim_episode_context_for_label,
    validate_episode_context_output,
)


def test_usage_profile_separates_cumulative_and_last_turn_unique(tmp_path) -> None:
    log_path = tmp_path / "usage.jsonl"
    log_path.write_text(
        json.dumps(
            {
                "usage": {
                    "input_tokens": 527_341,
                    "cached_input_tokens": 461_312,
                    "output_tokens": 17_763,
                    "total_tokens": 545_104,
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    profile = _usage_profile_from_jsonl(log_path)
    assert profile is not None
    assert profile["cumulative_billed_total_tokens"] == 545_104
    assert profile["last_turn_unique_input_tokens"] == 66_029
    assert profile["last_turn_unique_total_tokens"] == 83_792


def test_provider_pressure_signals_only_read_error_events(tmp_path) -> None:
    log_path = tmp_path / "pressure.jsonl"
    log_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "type": "item.completed",
                        "item": {
                            "type": "message",
                            "text": "The prompt discusses capacity planning.",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "item.completed",
                        "item": {
                            "type": "error",
                            "message": "429 rate limit capacity exceeded",
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    assert _provider_pressure_signals(log_path) == [
        "429,rate limit,capacity"
    ]


def test_provider_pressure_ignores_numeric_429_in_tool_error_payload(
    tmp_path,
) -> None:
    log_path = tmp_path / "tool-error.jsonl"
    log_path.write_text(
        "2026-07-30T11:30:41Z ERROR apply_patch verification failed: "
        '{"evidence_end":429,"message":"ordinary tool error"}\n',
        encoding="utf-8",
    )
    assert _provider_pressure_signals(log_path) == []


def test_metric_grounding_contract_is_in_prompt_and_schema() -> None:
    pack = load_label_pack("ai_discourse_v3_1")
    assert "verbatim contiguous substring" in pack.prompt
    assert "not free-prose descriptors" in pack.prompt
    metric = pack.schema["properties"]["discourse_events"]["items"][
        "properties"
    ]["metric"]["properties"]
    assert "verbatim contiguous substring" in metric["raw_text"]["description"]
    assert "never a free-prose descriptor" in metric["unit"]["description"]
    provenance = label_pack_provenance(pack)
    assert len(provenance["pack_configuration_sha256"]) == 64


def test_label_context_slimming_keeps_only_consumed_fields() -> None:
    artifact = {
        key: f"value-{key}" for key in LABEL_EPISODE_CONTEXT_FIELDS
    }
    artifact.update(
        {
            "context_summary": "duplicate narrative",
            "episode_context": {"speaker_map": []},
            "quality_flags": [],
            "schema_version": "v1",
        }
    )
    slim = slim_episode_context_for_label(artifact)
    assert tuple(slim) == LABEL_EPISODE_CONTEXT_FIELDS
    assert "context_summary" not in slim
    assert "episode_context" not in slim


def test_label_context_slimming_filters_distant_and_weak_named_turns() -> None:
    artifact = {
        "speaker_map": [
            {
                "speaker_id": "guest",
                "name": "Guest",
                "turn_assignments": [
                    {"segment_index": 4, "confidence": 0.91},
                    {"segment_index": 5, "confidence": 0.84},
                    {"segment_index": 9, "confidence": 0.99},
                ],
                "turn_anchors": [
                    {"segment_index": 4},
                    {"segment_index": 9},
                ],
            },
            {
                "speaker_id": "unknown",
                "name": "Unknown",
                "turn_assignments": [
                    {"segment_index": 5, "confidence": 0.55},
                    {"segment_index": 9, "confidence": 0.40},
                ],
                "turn_anchors": [],
            },
        ]
    }
    slim = slim_episode_context_for_label(
        artifact, relevant_segment_indices={4, 5}
    )
    assert slim["speaker_map"][0]["turn_assignments"] == [
        {"segment_index": 4, "confidence": 0.91}
    ]
    assert slim["speaker_map"][0]["turn_anchors"] == [
        {"segment_index": 4}
    ]
    assert slim["speaker_map"][1]["turn_assignments"] == [
        {"segment_index": 5, "confidence": 0.55}
    ]


def test_episode_context_prompt_requires_mixed_turn_speaker_anchors() -> None:
    prompt = render_episode_context_prompt(
        "ai_discourse_v3_1",
        {
            "episode": {"id": "episode-panel", "title": "Panel"},
            "full_segmented_episode_text": "[segment 0]\nA question. An answer.",
        },
    )
    assert EPISODE_CONTEXT_SCHEMA_VERSION.endswith("_v5")
    assert "turn_assignments" in prompt
    assert "assign every logical dialogue turn exactly once" in prompt
    assert "one object per logical dialogue turn" in prompt
    assert "confidence >=0.85" in prompt
    assert "company knowledge shared by two guests" in prompt
    assert "attribution_basis" in prompt
    assert "Do not infer speaker identity from alternating turns alone" in prompt
    assert "at least two independent attribution clues" in prompt
    assert "speaker_id='unknown'" in prompt


def test_episode_context_v5_rejects_duplicate_turn_assignments() -> None:
    output = {
        "schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
        "episode_id": "episode-panel",
        "context_summary": "A three speaker panel episode.",
        "speaker_map": [
            {
                "name": "Host",
                "direct_speaker": True,
                "turn_assignments": [
                    {
                        "segment_index": 0,
                        "turn_orders": [1],
                        "opening_words": "Welcome to the panel",
                        "attribution_basis": ["explicit self-identification"],
                        "confidence": 0.9,
                    }
                ],
                "turn_anchors": [],
            },
            {
                "name": "Guest",
                "direct_speaker": True,
                "turn_assignments": [
                    {
                        "segment_index": 0,
                        "turn_orders": [1],
                        "opening_words": "Welcome to the panel",
                        "attribution_basis": ["direct address"],
                        "confidence": 0.8,
                    }
                ],
                "turn_anchors": [],
            },
        ],
        "section_map": [],
        "entity_seed": {},
        "concept_seed": [],
        "extraction_guidance": "Use the complete panel context for careful speaker attribution.",
        "quality_flags": [],
        "overall_confidence": 0.8,
        "needs_review": False,
        "review_reason": None,
    }
    with pytest.raises(ValueError, match="duplicate a segment/turn pair"):
        validate_episode_context_output(
            output, expected_episode_id="episode-panel"
        )


def test_episode_context_v5_resolves_named_unknown_overlap_to_unknown() -> None:
    output = {
        "schema_version": EPISODE_CONTEXT_SCHEMA_VERSION,
        "episode_id": "episode-panel",
        "context_summary": "A three speaker panel episode.",
        "speaker_map": [
            {
                "speaker_id": "guest",
                "name": "Guest",
                "direct_speaker": True,
                "turn_assignments": [
                    {
                        "segment_index": 0,
                        "turn_orders": [1],
                        "opening_words": "The first answer",
                        "attribution_basis": ["direct address"],
                        "confidence": 0.91,
                    },
                    {
                        "segment_index": 0,
                        "turn_orders": [2],
                        "opening_words": "The second answer",
                        "attribution_basis": ["topic continuity"],
                        "confidence": 0.71,
                    },
                ],
                "turn_anchors": [],
            },
            {
                "speaker_id": "unknown",
                "name": "Unknown or unresolved speaker",
                "direct_speaker": True,
                "turn_assignments": [
                    {
                        "segment_index": 0,
                        "turn_orders": [2],
                        "opening_words": "The second answer",
                        "attribution_basis": ["identity unresolved"],
                        "confidence": 0.58,
                    }
                ],
                "turn_anchors": [],
            },
        ],
        "section_map": [],
        "entity_seed": {},
        "concept_seed": [],
        "extraction_guidance": "Use the complete panel context for careful speaker attribution.",
        "quality_flags": [],
        "overall_confidence": 0.8,
        "needs_review": False,
        "review_reason": None,
    }
    artifact = validate_episode_context_output(
        output, expected_episode_id="episode-panel"
    )
    assert artifact["speaker_map"][0]["turn_assignments"][0][
        "turn_orders"
    ] == [1]
    assert artifact["speaker_map"][1]["turn_assignments"][0][
        "turn_orders"
    ] == [2]


def test_embedded_json_sections_are_compact() -> None:
    rendered = render_prompt(
        "ai_discourse_v3_1",
        {"text": "Speaker: current text"},
        {"episode_id": "episode-1", "nested": {"value": 1}},
    )
    static = rendered.split(
        "# Static Schema And Examples\n", 1
    )[1].split("\n\n# Variable Context", 1)[0]
    context = rendered.split("# Variable Context\n", 1)[1].split(
        "\n\n# Segment Text", 1
    )[0]
    assert "\n  " not in static
    assert context.strip() == '{"episode_id":"episode-1","nested":{"value":1}}'


def test_label_submission_derivative_tables_are_expected() -> None:
    assert {
        "concepts",
        "concept_versions",
        "concept_candidates",
        "concept_aliases",
        "topic_mentions",
        "term_mentions",
        "entity_mentions",
        "speaker_positions",
    } <= EXPECTED_MUTATION_TABLES


def test_codex_binary_falls_back_to_user_local_bin_under_launchd(tmp_path) -> None:
    binary = tmp_path / ".local" / "bin" / "codex"
    binary.parent.mkdir(parents=True)
    binary.write_text("#!/bin/sh\n", encoding="utf-8")
    binary.chmod(0o755)
    with patch.dict(os.environ, {}, clear=True), patch(
        "research_factory.headless_codex.shutil.which", return_value=None
    ), patch("research_factory.headless_codex.Path.home", return_value=tmp_path):
        assert resolve_codex_binary() == str(binary.resolve())


class _FailureConnection:
    def __init__(self):
        self.job = {
            "job_status": "claimed",
            "attempts": 2,
            "max_attempts": 2,
            "lease_owner": "worker",
            "run_status": "claimed",
        }
        self.calls = []

    def rollback(self):
        pass

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        if "SELECT jobs.status" in sql:
            return type("_Row", (), {"fetchone": lambda _: self.job})()
        return type("_Change", (), {})()

    def commit(self):
        pass


def test_prelaunch_failure_releases_claim_without_consuming_attempt() -> None:
    conn = _FailureConnection()
    result = _finalize_submission_failure(
        conn,
        job_id=1,
        label_run_id="run-1",
        lease_owner="worker",
        error="codex_exec_launch_failed",
        consume_attempt=False,
    )
    assert result["job_status"] == "pending"
    assert result["attempt_consumed"] is False
    assert result["resulting_attempts"] == 1
