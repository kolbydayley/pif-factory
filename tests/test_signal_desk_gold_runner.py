from research_factory.signal_desk_gold_runner import (
    RESERVE_TOKENS,
    SYSTEM_PROMPTS,
    repair_unique_evidence_offsets,
)


def test_runner_reserves_above_measured_p90_and_keeps_turn_prompts_distinct():
    measured_p90 = {"A": 39288, "B": 40725.1, "C": 47078.8, "AUDIT": 39774.4}
    assert all(RESERVE_TOKENS[key] > value for key, value in measured_p90.items())
    assert set(SYSTEM_PROMPTS) == {"A", "B", "C", "AUDIT"}
    assert len(set(SYSTEM_PROMPTS.values())) == 4


def test_unique_exact_excerpt_repairs_offsets_without_changing_semantics():
    output = {
        "window_id": "w1",
        "events": [
            {
                "evidence_text": "unique excerpt",
                "evidence_start": 3,
                "evidence_end": 17,
                "claim_text": "unchanged",
            }
        ],
    }
    repaired, count = repair_unique_evidence_offsets(
        output, transcript_window="prefix unique excerpt suffix"
    )
    assert count == 1
    assert repaired["events"][0]["evidence_start"] == 7
    assert repaired["events"][0]["evidence_end"] == 21
    assert repaired["events"][0]["claim_text"] == "unchanged"
    assert output["events"][0]["evidence_start"] == 3


def test_ambiguous_excerpt_is_never_rebound():
    output = {
        "events": [
            {"evidence_text": "same", "evidence_start": 1, "evidence_end": 5}
        ]
    }
    repaired, count = repair_unique_evidence_offsets(
        output, transcript_window="same and same"
    )
    assert count == 0
    assert repaired == output
