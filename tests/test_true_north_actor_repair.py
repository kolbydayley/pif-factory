from __future__ import annotations

import copy

import pytest

from research_factory import true_north_actor_repair as actor_repair


def gold_fixture() -> dict:
    return {
        "schema_version": "pif_true_north_gold_v1",
        "gold_sha256": "fixture",
        "items": [
            {
                "candidate_id": "candidate-1",
                "episode_id": "episode-1",
                "segment_id": "segment-1",
                "disposition": "retain",
                "reason_code": "fixture",
                "atomic_claims": [
                    {
                        "claim_text": "The lab changed its policy.",
                        "claim_type": "descriptive",
                        "raw_speaker": "Host",
                        "reported_actor": "Old Lab",
                        "stance": "neutral",
                        "certainty": "high",
                        "time_horizon": "past",
                        "confidence": 0.9,
                        "subject_text": "policy",
                        "subject_type": "policy",
                        "domain": "governance",
                        "proposition_text": "The lab changed its policy.",
                        "polarity": "positive",
                        "position": "asserts",
                        "evidence_text": "The lab changed its policy.",
                        "evidence_start": 0,
                        "evidence_end": 27,
                    }
                ],
            }
        ],
    }


def test_declared_budget_covers_full_current_gold_at_sixty_packets() -> None:
    budget = actor_repair.actor_repair_budget(1458)
    assert budget["pass_a_packet_count"] == 20
    assert budget["pass_b_packet_count"] == 20
    assert budget["pass_c_packet_ceiling"] == 20
    assert budget["maximum_packet_count"] == 60
    assert budget["max_calls"] == 60


def test_budget_rejects_scope_that_cannot_fit() -> None:
    with pytest.raises(actor_repair.ActorRepairError, match="exceeds 60"):
        actor_repair.actor_repair_budget(1501)


def test_actor_repair_is_development_only(tmp_path) -> None:
    with pytest.raises(
        actor_repair.ActorRepairError, match="development-only"
    ):
        actor_repair.prepare_actor_repair(tmp_path, partition="holdout")


def test_apply_repairs_changes_only_reported_actor() -> None:
    source = gold_fixture()
    expected_without_actor = copy.deepcopy(source)
    expected_without_actor["items"][0]["atomic_claims"][0][
        "reported_actor"
    ] = None

    revised = actor_repair.apply_actor_repairs(
        source, {"candidate-1:00": None}
    )

    assert source["items"][0]["atomic_claims"][0]["reported_actor"] == "Old Lab"
    assert revised == expected_without_actor


def test_apply_repairs_requires_exact_atomic_scope() -> None:
    with pytest.raises(actor_repair.ActorRepairError, match="missing"):
        actor_repair.apply_actor_repairs(gold_fixture(), {})
    with pytest.raises(actor_repair.ActorRepairError, match="out-of-scope"):
        actor_repair.apply_actor_repairs(
            gold_fixture(),
            {"candidate-1:00": None, "candidate-2:00": "Other"},
        )


def test_actor_output_validator_rejects_duplicate_ids() -> None:
    rows = [
        {"atomic_id": "a"},
        {"atomic_id": "b"},
    ]
    packet = {
        "input": {"items": rows},
        "output_schema": actor_repair._actor_output_schema(["a", "b"]),
    }
    output = {
        "schema_version": actor_repair.OUTPUT_SCHEMA_VERSION,
        "items": [
            {"atomic_id": "a", "reported_actor": None},
            {"atomic_id": "a", "reported_actor": None},
        ],
    }
    with pytest.raises(Exception):
        actor_repair._validate_actor_output(output, packet)


def test_actor_output_enforces_evidence_span_and_direct_speaker_rule() -> None:
    packet = {
        "input": {
            "items": [
                {
                    "atomic_id": "a",
                    "evidence_text": "Anthropic changed the policy.",
                    "raw_speaker": "Host",
                },
                {
                    "atomic_id": "b",
                    "evidence_text": "I changed the policy.",
                    "raw_speaker": "Host",
                },
            ]
        },
        "output_schema": actor_repair._actor_output_schema(["a", "b"]),
    }
    output = {
        "schema_version": actor_repair.OUTPUT_SCHEMA_VERSION,
        "items": [
            {"atomic_id": "a", "reported_actor": "Anthropic"},
            {"atomic_id": "b", "reported_actor": "Host"},
        ],
    }

    actor_repair._validate_actor_output(output, packet)

    assert output["items"][0]["reported_actor"] == "Anthropic"
    assert output["items"][1]["reported_actor"] is None
