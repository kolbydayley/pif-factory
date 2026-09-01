from __future__ import annotations

from unittest.mock import patch

import pytest

from research_factory.signal_desk_rebuild_scorer import (
    SignalDeskScorerError,
    event_eligibility,
    match_events,
    qualify_scorer,
    scorer_registry_entry,
    scorer_sha256,
    scorer_specification,
)


def _event(
    *,
    speaker: str = "Ajeya Cotra",
    role: str = "direct_speech",
    subject: str = "frontier labs",
    issue: str = "ai-labor",
    stance: str = "skeptical",
    start: int = 100,
    end: int = 200,
    claim: str = "AI deployment may displace some kinds of work.",
) -> dict:
    return {
        "speaker_id": speaker,
        "speaker_role": role,
        "subject_id": subject,
        "issue_id": issue,
        "stance": stance,
        "evidence_start": start,
        "evidence_end": end,
        "transcript_id": "episode-1",
        "claim_text": claim,
    }


def test_eligibility_requires_overlap_role_identity_issue_and_stance() -> None:
    gold = _event(issue="issue-ai-employment")
    predicted = _event(
        speaker="Dr. Ajeya Cotra",
        role="direct speech",
        issue="AI jobs",
        start=150,
        end=250,
    )
    result = event_eligibility(
        gold,
        predicted,
        issue_registry={"issue-ai-employment": ["AI jobs", "ai labor"]},
    )
    assert result["eligible"] is True
    assert result["evidence_overlap"] == 0.5

    wrong_role = event_eligibility(gold, {**predicted, "speaker_role": "third_party_mention"})
    assert wrong_role["eligible"] is False
    assert "speaker_role_disagreement" in wrong_role["failures"]

    wrong_stance = event_eligibility(gold, {**predicted, "issue_id": "issue-ai-employment", "stance": "supportive"})
    assert wrong_stance["eligible"] is False
    assert "stance_disagreement" in wrong_stance["failures"]

    different_claim = event_eligibility(
        gold,
        {
            **predicted,
            "issue_id": "issue-ai-employment",
            "claim_text": "A completely unrelated product shipped yesterday.",
        },
    )
    assert different_claim["eligible"] is False
    assert "insufficient_claim_text_agreement" in different_claim["failures"]


def test_numeric_spans_from_different_sources_never_match() -> None:
    result = event_eligibility(_event(), {**_event(), "transcript_id": "episode-2"})
    assert result["eligible"] is False
    assert result["evidence_overlap"] == 0.0


def test_clean_event_v2_fields_match_without_legacy_subject() -> None:
    gold = {
        "speaker_id": "Ajeya Cotra",
        "attribution_type": "direct_speech",
        "issue_label": "AI impact on employment",
        "stance": "warning",
        "evidence_start": 10,
        "evidence_end": 80,
        "claim_text": "AI deployment could displace some categories of work.",
    }
    predicted = dict(gold)
    result = event_eligibility(gold, predicted)
    assert result["eligible"] is True
    assert result["gold_surface"]["issue"] == "ai impact on employment"
    assert result["gold_surface"]["subject"] == ""


def test_clean_event_role_entities_are_binding() -> None:
    quoted = {
        **_event(subject=""),
        "speaker_role": "quoted_speech",
        "quoted_person_id": "Dario Amodei",
    }
    wrong_quote = {**quoted, "quoted_person_id": "Sam Altman"}
    assert "quoted_person_disagreement" in event_eligibility(quoted, wrong_quote)["failures"]

    mentioned = {
        **_event(subject=""),
        "speaker_role": "third_party_mention",
        "mentioned_person_ids": ["Dario Amodei", "Sam Altman"],
    }
    missing_person = {**mentioned, "mentioned_person_ids": ["Dario Amodei"]}
    assert "mentioned_people_disagreement" in event_eligibility(
        mentioned, missing_person
    )["failures"]


def test_issue_is_scored_after_matching_not_used_to_hide_event_recall() -> None:
    gold = _event(issue="AI impact on employment")
    predicted = _event(issue="automation and jobs")
    result = event_eligibility(gold, predicted)
    assert result["eligible"] is True
    assert result["field_agreement"]["issue"] is False


def test_flattened_gold_rewards_indeterminable_and_penalizes_fabricated_speaker() -> None:
    gold = _event(speaker="", role="unresolved_speaker")
    indeterminable = _event(speaker="", role="unresolved_speaker")
    correct = event_eligibility(gold, indeterminable, transcript_structure="flattened")
    assert correct["eligible"] is True
    assert correct["unsupported_attribution"] is False

    fabricated = _event(speaker="Famous Host", role="direct_speech")
    wrong = event_eligibility(gold, fabricated, transcript_structure="flattened")
    assert wrong["eligible"] is False
    assert "unsupported_attribution" in wrong["failures"]
    scored = match_events([gold], [fabricated], transcript_structure="flattened")
    assert scored["unsupported_attributions"] == 1


def test_asr_entities_bind_surface_and_only_frozen_canonical_alias() -> None:
    gold = {
        **_event(speaker="Fluenz", subject="Benaich"),
        "entity_aliases": {
            "speaker_id": ["Fluence"],
            "subject_id": ["Nathan Benaich"],
        },
    }
    surface = event_eligibility(
        gold,
        _event(speaker="fluenz", subject="benaich"),
        transcript_structure="asr_diarized",
    )
    canonical = event_eligibility(
        gold,
        _event(speaker="Fluence", subject="Nathan Benaich"),
        transcript_structure="asr_diarized",
    )
    invented = event_eligibility(
        gold,
        _event(speaker="Florence", subject="Nathan Benay"),
        transcript_structure="asr_diarized",
    )
    assert surface["eligible"] is True
    assert canonical["eligible"] is True
    assert invented["eligible"] is False
    assert "speaker_disagreement" in invented["failures"]
    assert "subject_disagreement" in invented["failures"]


def test_split_and_merge_credit_is_strictly_one_to_one() -> None:
    gold = [_event(start=0, end=100), _event(start=0, end=100)]
    merged_prediction = [_event(start=0, end=100)]
    result = match_events(gold, merged_prediction)
    assert result["matched_events"] == 1
    assert result["false_negatives"] == 1
    assert result["false_positives"] == 0


def test_assignment_is_global_maximum_not_greedy() -> None:
    # Greedy would consume (0, 0)=.9 then leave .1, while the optimum is
    # (0, 1)=.8 plus (1, 0)=.85.
    weights = [[0.9, 0.8], [0.85, 0.1]]
    calls = {("g0", "p0"): weights[0][0], ("g0", "p1"): weights[0][1],
             ("g1", "p0"): weights[1][0], ("g1", "p1"): weights[1][1]}

    def fake(gold, predicted, **_kwargs):
        weight = calls[(gold["id"], predicted["id"])]
        return {"eligible": True, "failures": [], "evidence_overlap": weight,
                "claim_text_f1": weight, "weight": weight,
                "gold_surface": {}, "predicted_surface": {}}

    with patch("research_factory.signal_desk_rebuild_scorer.event_eligibility", fake):
        result = match_events(
            [{"id": "g0"}, {"id": "g1"}],
            [{"id": "p0"}, {"id": "p1"}],
        )
    assert {(row["gold_index"], row["predicted_index"]) for row in result["matches"]} == {
        (0, 1),
        (1, 0),
    }


def _qualification_rows(total: int, mistakes: int = 0) -> list[dict]:
    rows = []
    for index in range(total):
        expected = index % 2 == 0
        rows.append(
            {
                "expected_match": expected,
                "scorer_match": (not expected) if index < mistakes else expected,
                "error_family": f"family-{index % 10}",
            }
        )
    return rows


def test_scorer_qualification_expands_until_lcb_clears() -> None:
    too_small = qualify_scorer(_qualification_rows(99))
    assert too_small["status"] == "needs_more_cases"
    assert too_small["required_total"] == 100

    qualified = qualify_scorer(_qualification_rows(100))
    assert qualified["status"] == "qualified"
    assert qualified["agreement_lcb"] >= 0.97
    registry = scorer_registry_entry(qualified)
    assert registry["frozen"] is True
    assert registry["sha256"] == scorer_sha256()

    needs_block = qualify_scorer(_qualification_rows(100, mistakes=1))
    assert needs_block["status"] == "needs_more_cases"
    assert needs_block["required_total"] == 150


def test_qualification_rejects_bad_stratification_and_unqualified_registry() -> None:
    rows = _qualification_rows(100)
    for row in rows:
        row["error_family"] = "only-one"
    with pytest.raises(SignalDeskScorerError, match="ten error families"):
        qualify_scorer(rows)
    with pytest.raises(SignalDeskScorerError, match="unqualified"):
        scorer_registry_entry({"status": "needs_more_cases"})


def test_specification_and_hash_are_stable() -> None:
    spec = scorer_specification()
    assert spec["matching"] == "maximum_weight_one_to_one_hungarian"
    assert spec["event_credit"] == "binary_after_eligibility_no_partial_tp_credit"
    assert len(scorer_sha256()) == 64
