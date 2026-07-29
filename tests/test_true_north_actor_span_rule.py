from __future__ import annotations

import pytest

from research_factory import true_north_actor_span_rule as rule


def test_literal_span_is_kept() -> None:
    assert (
        rule.span_enforced_actor(
            "Anthropic",
            evidence_text="Anthropic published the evaluation last week.",
            raw_speaker="Nilay",
        )
        == "Anthropic"
    )


def test_span_match_is_case_insensitive_but_preserves_the_prior() -> None:
    assert (
        rule.span_enforced_actor(
            "OpenAI",
            evidence_text="openai shipped the model on Tuesday.",
            raw_speaker="Nilay",
        )
        == "OpenAI"
    )


def test_absent_span_becomes_null() -> None:
    """The contract: a value that is not a literal evidence span is null."""
    assert (
        rule.span_enforced_actor(
            "Google DeepMind",
            evidence_text="They shipped the model on Tuesday.",
            raw_speaker="Nilay",
        )
        is None
    )


def test_speaker_talking_about_themselves_is_not_a_reported_actor() -> None:
    assert (
        rule.span_enforced_actor(
            "Nilay",
            evidence_text="Nilay said he ran the benchmark himself.",
            raw_speaker="Nilay",
        )
        is None
    )


def test_speaker_self_exclusion_is_case_and_whitespace_insensitive() -> None:
    assert (
        rule.span_enforced_actor(
            "  nilay patel ",
            evidence_text="Nilay Patel ran the benchmark himself.",
            raw_speaker="Nilay Patel",
        )
        is None
    )


def test_absent_sentinels_and_blanks_are_null() -> None:
    for value in (None, "", "   ", "none", "N/A", "null", "unknown"):
        assert (
            rule.span_enforced_actor(
                value,
                evidence_text="Anthropic published the evaluation.",
                raw_speaker="Nilay",
            )
            is None
        )


def test_speaker_exclusion_can_be_disabled_for_ablation() -> None:
    assert (
        rule.span_enforced_actor(
            "Nilay",
            evidence_text="Nilay said he ran the benchmark himself.",
            raw_speaker="Nilay",
            exclude_speaker_self=False,
        )
        == "Nilay"
    )


def atomic(actor: str | None, evidence: str, speaker: str = "Nilay") -> dict:
    return {
        "claim_text": "a claim",
        "raw_speaker": speaker,
        "reported_actor": actor,
        "evidence_text": evidence,
    }


def test_apply_rule_rewrites_atomics_and_reports_each_reason() -> None:
    atomics = [
        atomic("Anthropic", "Anthropic published the evaluation."),
        atomic("Google DeepMind", "They shipped it Tuesday."),
        atomic("Nilay", "Nilay ran the benchmark himself."),
        atomic(None, "Nothing named here."),
    ]
    result = rule.apply_actor_span_rule(atomics)

    assert [a["reported_actor"] for a in result.atomics] == [
        "Anthropic",
        None,
        None,
        None,
    ]
    assert result.report["total"] == 4
    assert result.report["kept"] == 1
    assert result.report["nulled_absent_span"] == 1
    assert result.report["nulled_speaker_self"] == 1
    assert result.report["already_null"] == 1


def test_apply_rule_does_not_mutate_the_input() -> None:
    atomics = [atomic("Google DeepMind", "They shipped it Tuesday.")]
    rule.apply_actor_span_rule(atomics)
    assert atomics[0]["reported_actor"] == "Google DeepMind"


def test_apply_rule_leaves_every_other_field_untouched() -> None:
    atomics = [atomic("Google DeepMind", "They shipped it Tuesday.")]
    result = rule.apply_actor_span_rule(atomics)
    before = {k: v for k, v in atomics[0].items() if k != "reported_actor"}
    after = {k: v for k, v in result.atomics[0].items() if k != "reported_actor"}
    assert before == after


def test_missing_evidence_text_is_rejected() -> None:
    with pytest.raises(rule.ActorSpanRuleError, match="evidence_text"):
        rule.apply_actor_span_rule([{"reported_actor": "X", "raw_speaker": "Y"}])
