from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

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
    LABEL_EPISODE_CONTEXT_FIELDS,
    slim_episode_context_for_label,
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
