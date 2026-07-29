from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from research_factory import pif_cli
from research_factory.glm_workhorse import (
    CanaryCase,
    aggregate_case_scores,
    build_private_job,
    candidate_schema,
    judge_schema,
    opencode_config,
    recover_exact_evidence,
    score_judge_output,
    score_source_audit,
    source_audit_schema,
    validate_and_repair_candidate,
)


def _case(text: str = "Alpha  beta gamma.") -> CanaryCase:
    return CanaryCase(
        segment_id="seg_test",
        source_name="Test Source",
        chunk_index=2,
        extract_text=text,
        left_context="Context before.",
        right_context="Context after.",
        episode_context={"episode_title": "Test episode"},
        adjacent_segments=({"chunk_index": 1, "text": "Earlier context."},),
        baseline_events=(),
        prompt_path=Path("/private/prompt"),
        baseline_path=Path("/private/baseline"),
    )


def test_candidate_job_restores_context_but_keeps_it_evidence_ineligible() -> None:
    job = build_private_job(_case(), max_events=10)
    assert set(job) == {"task", "instructions", "output_schema", "input"}
    assert set(job["input"]) == {
        "segment_id",
        "chunk_index",
        "evidence_eligible_extract_text",
        "context_only",
    }
    assert job["input"]["context_only"]["left_context"] == "Context before."
    assert "context-only text" in " ".join(job["instructions"]).lower()
    assert "baseline" not in json.dumps(job).lower()
    assert job["output_schema"] == candidate_schema("seg_test", 2, max_events=10)


def test_opencode_config_uses_dedicated_non_coding_agent_and_denies_tools() -> None:
    config = opencode_config("opencode-go/glm-5.2")
    agent = config["agent"]["pif-extractor"]
    assert config["default_agent"] == "pif-extractor"
    assert config["share"] == "disabled"
    assert config["snapshot"] is False
    assert config["permission"] == {"*": "deny"}
    assert agent["model"] == "opencode-go/glm-5.2"
    assert agent["steps"] == 8
    assert agent["temperature"] == 0.1
    assert agent["permission"] == {"*": "deny"}


def test_unique_whitespace_normalization_recovers_original_exact_span() -> None:
    recovered, mode = recover_exact_evidence("Alpha  beta\ngamma.", "Alpha beta gamma.")
    assert recovered == "Alpha  beta\ngamma."
    assert mode == "normalized_unique"


def test_ambiguous_normalized_span_fails_closed() -> None:
    recovered, mode = recover_exact_evidence("same  span / same\nspan", "same span")
    assert recovered is None
    assert mode == "ambiguous"


def test_candidate_validation_repairs_evidence_and_rejects_schema_drift() -> None:
    payload = {
        "segment_id": "seg_test",
        "chunk_index": 2,
        "events": [
            {
                "event_type": "capability_claim",
                "evidence": "Alpha beta gamma.",
                "claim_text": "Alpha supports beta.",
            }
        ],
    }
    repaired, receipt = validate_and_repair_candidate(payload, case=_case(), max_events=10)
    assert repaired["events"][0]["evidence"] == "Alpha  beta gamma."
    assert receipt["exact_grounding_after_repair"] is True
    assert receipt["repair_modes"] == {"normalized_unique": 1}


def test_candidate_validation_drops_only_the_ungrounded_event() -> None:
    payload = {
        "segment_id": "seg_test",
        "chunk_index": 2,
        "events": [
            {
                "event_type": "capability_claim",
                "evidence": "Alpha beta gamma.",
                "claim_text": "Alpha supports beta.",
            },
            {
                "event_type": "forecast",
                "evidence": "This quote was invented.",
                "claim_text": "An unsupported forecast.",
            },
        ],
    }
    repaired, receipt = validate_and_repair_candidate(payload, case=_case(), max_events=10)
    assert len(repaired["events"]) == 1
    assert receipt["invalid_evidence_events_dropped"] == 1
    assert receipt["all_returned_events_grounded"] is False
    assert receipt["exact_grounding_after_repair"] is True
    assert receipt["usable_after_repair"] is True


def test_semantic_judge_scoring_is_one_to_one_and_bounded() -> None:
    packet = {
        "cases": [
            {
                "case_id": "seg_test",
                "reference": [{"id": 0}, {"id": 1}],
                "candidate": [{"id": 0}, {"id": 1}, {"id": 2}],
            }
        ]
    }
    output = {
        "evaluations": [
            {
                "case_id": "seg_test",
                "pairs": [
                    {"reference_id": 0, "candidate_id": 0, "relation": "equivalent"},
                    {"reference_id": 0, "candidate_id": 1, "relation": "equivalent"},
                    {"reference_id": 1, "candidate_id": 2, "relation": "partial"},
                ],
            }
        ]
    }
    score = score_judge_output(packet, output)
    assert score["equivalent_pairs"] == 1
    assert score["duplicate_pairs"] == 1
    assert score["precision"] == 0.333333
    assert score["recall"] == 0.5


def test_case_score_aggregation_can_isolate_successful_transport() -> None:
    result = aggregate_case_scores(
        [
            {
                "case_id": "usable",
                "reference_events": 4,
                "candidate_events": 5,
                "equivalent_pairs": 3,
                "partial_pairs": 1,
            },
            {
                "case_id": "failed",
                "reference_events": 6,
                "candidate_events": 0,
                "equivalent_pairs": 0,
                "partial_pairs": 0,
            },
        ],
        case_ids={"usable"},
    )
    assert result == {
        "case_count": 1,
        "reference_events": 4,
        "candidate_events": 5,
        "equivalent_pairs": 3,
        "partial_pairs": 1,
        "precision": 0.6,
        "recall": 0.75,
        "f1": 0.666667,
    }


def test_judge_schema_requires_exact_case_count() -> None:
    schema = judge_schema(["a", "b"])
    evaluations = schema["properties"]["evaluations"]
    assert evaluations["minItems"] == 2
    assert evaluations["maxItems"] == 2


def test_source_audit_requires_complete_unique_candidate_coverage() -> None:
    packet = {
        "cases": [
            {
                "case_id": "seg_test",
                "candidates": [{"candidate_id": 0}, {"candidate_id": 1}],
            }
        ]
    }
    output = {
        "evaluations": [
            {
                "case_id": "seg_test",
                "candidate_id": 0,
                "support": "supported",
                "distinctness": "distinct",
                "signal": "useful",
                "type_fit": "correct",
                "verdict": "keep",
            },
            {
                "case_id": "seg_test",
                "candidate_id": 1,
                "support": "partial",
                "distinctness": "distinct",
                "signal": "useful",
                "type_fit": "neighbor",
                "verdict": "revise",
            },
        ]
    }
    score = score_source_audit(packet, output)
    assert score["supported_or_partial_fraction"] == 1.0
    assert score["actionable_fraction"] == 1.0
    assert score["clean_keep_fraction"] == 0.5
    schema = source_audit_schema(["seg_test"], 2)
    assert schema["properties"]["evaluations"]["minItems"] == 2


def test_compact_cli_dispatches_glm_lab_without_opening_legacy_cli() -> None:
    with patch("research_factory.glm_workhorse.main", return_value=0) as dispatched, patch.object(
        pif_cli.legacy_cli,
        "main",
        side_effect=AssertionError("legacy CLI must not receive GLM lab command"),
    ):
        assert pif_cli.main(["lab", "glm-workhorse", "status"]) == 0
    dispatched.assert_called_once_with(["status"])


def test_canary_cli_dry_run_makes_no_model_call_or_database_open() -> None:
    fake_cases = [_case()]
    with tempfile.TemporaryDirectory() as temp, patch(
        "research_factory.glm_workhorse.select_blinded_cases",
        return_value=fake_cases,
    ), patch(
        "research_factory.glm_workhorse.run_canary",
        side_effect=AssertionError("dry run must not dispatch"),
    ):
        from research_factory.glm_workhorse import main

        assert main(["canary", "--limit", "1", "--output-root", temp]) == 0


def test_codex_transport_dry_run_reports_selected_model_without_dispatch() -> None:
    fake_cases = [_case()]
    with tempfile.TemporaryDirectory() as temp, patch(
        "research_factory.glm_workhorse.select_blinded_cases",
        return_value=fake_cases,
    ), patch(
        "research_factory.glm_workhorse.run_canary",
        side_effect=AssertionError("dry run must not dispatch"),
    ):
        from research_factory.glm_workhorse import main

        assert (
            main(
                [
                    "canary",
                    "--limit",
                    "1",
                    "--output-root",
                    temp,
                    "--transport",
                    "codex",
                    "--model",
                    "gpt-5.3-codex-spark",
                ]
            )
            == 0
        )
