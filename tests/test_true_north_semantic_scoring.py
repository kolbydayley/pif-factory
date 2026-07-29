from __future__ import annotations

import copy
import unittest

from research_factory.true_north_semantic_scoring import (
    _align,
    canonicalize_participant,
    normalize_enum_field,
    score_campaign,
    score_candidate,
)


def claim(
    text: str,
    *,
    speaker: str = "Dr. Ada",
    actor: str | None = None,
    time: str = "present",
    certainty: str = "high",
    stance: str = "warning",
) -> dict[str, object]:
    return {
        "claim_text": text,
        "proposition_text": text,
        "raw_speaker": speaker,
        "reported_actor": actor,
        "subject_text": "AI agents",
        "subject_type": "technology",
        "claim_type": "claim",
        "time_horizon": time,
        "certainty": certainty,
        "stance": stance,
        "polarity": "positive",
        "position": "supports",
    }


def consensus(
    *texts: str,
    disposition: str = "revise",
    candidate_id: str = "candidate-1",
    strictly_scoreable: bool = True,
) -> dict[str, object]:
    value_state = "junk" if disposition == "reject" else "value"
    return {
        "candidate_id": candidate_id,
        "consensus_state": f"consensus_{value_state}",
        "strictly_scoreable": strictly_scoreable,
        "acceptable_dispositions": [disposition],
        "acceptable_value_states": [value_state],
        "acceptable_atomic_counts": [len(texts)],
        "minimum_atomic_count": len(texts),
        "maximum_atomic_count": len(texts),
        "preferred_atomic_count": len(texts),
        "preferred_disposition": disposition,
        "decompositions": [
            {
                "source": "adjudicated",
                "disposition": disposition,
                "value_state": value_state,
                "atomic_count": len(texts),
                "claim_texts": list(texts),
            }
        ],
    }


class TrueNorthSemanticScoringTest(unittest.TestCase):
    def test_canonicalize_participant_matches_real_roster_shape(self) -> None:
        speaker_map = [
            {
                "name": "Dario Amodei",
                "aliases": ["Dario", "Amodei"],
                "role": "reported actor",
            }
        ]
        self.assertEqual(
            canonicalize_participant("Dario", speaker_map),
            "dario amodei",
        )
        self.assertEqual(
            canonicalize_participant("DARIO AMODEI", speaker_map),
            "dario amodei",
        )

    def test_canonicalize_participant_unknown_normalizes(self) -> None:
        self.assertEqual(
            canonicalize_participant("  Dr.  Jane   Doe ", {}),
            "jane doe",
        )

    def test_align_speaker_exactness_uses_canonicalization(self) -> None:
        speaker_map = [
            {
                "name": "Dario Amodei",
                "aliases": ["Dario"],
                "role": "guest",
            }
        ]
        result = _align(
            [{"claim_text": "Models scale.", "raw_speaker": "Dario"}],
            [{"claim_text": "Models scale.", "raw_speaker": "Dario Amodei"}],
            speaker_map=speaker_map,
        )
        self.assertTrue(
            result["pairs"][0]["field_exactness"]["raw_speaker"]
        )

    def test_enum_like_fields_normalize_to_the_v31_contract(self) -> None:
        self.assertEqual(normalize_enum_field("certainty", "high confidence"), "high")
        self.assertEqual(normalize_enum_field("certainty", "probably"), "medium")
        self.assertEqual(normalize_enum_field("stance", "supports"), "supportive")
        self.assertEqual(normalize_enum_field("polarity", "affirmative"), "positive")
        self.assertEqual(
            normalize_enum_field("time_horizon", "near future"),
            "near_future",
        )

    def test_enum_like_fields_preserve_canonical_and_unknown_values(self) -> None:
        self.assertEqual(normalize_enum_field("certainty", "hedged"), "hedged")
        self.assertEqual(
            normalize_enum_field("stance", "unmapped bespoke stance"),
            "unmapped bespoke stance",
        )

    def test_align_enum_exactness_uses_contract_normalization(self) -> None:
        predicted = claim(
            "Models may improve.",
            time="near future",
            certainty="probably",
            stance="supports",
        )
        predicted["polarity"] = "affirmative"
        reference = claim(
            "Models may improve.",
            time="near_future",
            certainty="medium",
            stance="supportive",
        )
        reference["polarity"] = "positive"
        result = _align([predicted], [reference])
        exactness = result["pairs"][0]["field_exactness"]
        self.assertTrue(exactness["time_horizon"])
        self.assertTrue(exactness["certainty"])
        self.assertTrue(exactness["stance"])
        self.assertTrue(exactness["polarity"])

    def test_reordered_claims_align_identically(self) -> None:
        first = claim("AI agents may retain context across sessions.")
        second = claim("AI agents may communicate with one another.")
        contract = consensus(first["claim_text"], second["claim_text"])
        gold = {
            "candidate_id": "candidate-1",
            "disposition": "revise",
            "atomic_claims": [first, second],
        }
        prediction = {
            "candidate_id": "candidate-1",
            "disposition": "revise",
            "atomic_claims": [first, second],
        }
        reordered = copy.deepcopy(prediction)
        reordered["atomic_claims"].reverse()

        score_a = score_candidate(prediction, contract, gold)
        score_b = score_candidate(reordered, contract, gold)

        self.assertEqual(score_a, score_b)
        self.assertEqual(score_a["claim_text_faithfulness_proxy"]["score"], 1.0)
        self.assertEqual(score_a["speaker_exactness"]["score"], 1.0)
        self.assertFalse(score_a["hallucination_or_unsupported_proxy"]["flagged"])

    def test_extra_and_missing_claims_are_reported_without_greedy_misalignment(self) -> None:
        first = claim("Model access should require least privilege.")
        second = claim("Agent actions should be audited.")
        gold = {
            "candidate_id": "candidate-1",
            "disposition": "revise",
            "atomic_claims": [first, second],
        }
        contract = consensus(first["claim_text"], second["claim_text"])

        missing = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [second],
            },
            contract,
            gold,
        )
        self.assertEqual(len(missing["unmatched_gold"]), 1)
        self.assertEqual(missing["unmatched_predicted"], [])
        self.assertFalse(missing["atomic_count"]["acceptable_count"])
        self.assertEqual(
            missing["claim_text_faithfulness_proxy"]["score"], 1.0
        )
        self.assertLess(
            missing["claim_text_faithfulness_proxy"][
                "coupled_diagnostic"
            ]["score"],
            1.0,
        )

        extra_claim = claim("The moon is made of cheese.")
        extra = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [second, first, extra_claim],
            },
            contract,
            gold,
        )
        self.assertEqual(len(extra["unmatched_predicted"]), 1)
        self.assertEqual(extra["unmatched_gold"], [])
        self.assertEqual(
            extra["claim_text_faithfulness_proxy"]["score"], 1.0
        )
        self.assertLess(
            extra["claim_text_faithfulness_proxy"][
                "coupled_diagnostic"
            ]["score"],
            1.0,
        )
        self.assertTrue(extra["hallucination_or_unsupported_proxy"]["flagged"])
        self.assertIn(
            "unmatched_predicted_claim",
            {flag["kind"] for flag in extra["unsupported_field_flags"]},
        )
        self.assertIn(
            "hallucination",
            {flag["severity"] for flag in extra["unsupported_field_flags"]},
        )
        self.assertEqual(missing["speaker_exactness"]["score"], 1.0)
        self.assertEqual(
            missing["speaker_exactness"]["coupled_diagnostic"]["score"],
            0.5,
        )

    def test_wrong_speaker_and_reported_actor_fail_exactness(self) -> None:
        reference = claim(
            "The regulator may require independent evaluations.",
            speaker="Host",
            actor="The regulator",
        )
        predicted = copy.deepcopy(reference)
        predicted["raw_speaker"] = "The regulator"
        predicted["reported_actor"] = None
        score = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [predicted],
            },
            consensus(reference["claim_text"]),
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [reference],
            },
        )
        self.assertEqual(score["speaker_exactness"]["score"], 0.0)
        self.assertEqual(score["reported_actor_exactness"]["score"], 0.0)
        self.assertEqual(score["claim_text_faithfulness_proxy"]["score"], 1.0)

    def test_qualifier_loss_and_time_certainty_stance_changes_are_visible(self) -> None:
        reference = claim(
            "If access is broad, agents may eventually expose sensitive data.",
            time="future",
            certainty="medium",
            stance="warning",
        )
        predicted = claim(
            "Agents expose sensitive data.",
            time="present",
            certainty="high",
            stance="neutral",
        )
        score = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [predicted],
            },
            consensus(reference["claim_text"]),
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [reference],
            },
        )
        self.assertLess(score["qualifier_preservation_proxy"]["score"], 1.0)
        self.assertEqual(score["time_horizon_exactness"]["score"], 0.0)
        self.assertEqual(score["certainty_exactness"]["score"], 0.0)
        self.assertEqual(score["stance_exactness"]["score"], 0.0)

    def test_empty_junk_candidate_scores_as_correct_and_extra_claim_is_flagged(self) -> None:
        contract = consensus(disposition="reject")
        correct = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "reject",
                "atomic_claims": [],
            },
            contract,
            {
                "candidate_id": "candidate-1",
                "disposition": "reject",
                "atomic_claims": [],
            },
        )
        self.assertTrue(correct["disposition"]["exact_acceptable"])
        self.assertTrue(correct["value_state"]["exact_acceptable"])
        self.assertTrue(correct["atomic_count"]["acceptable_count"])
        self.assertIsNone(correct["speaker_exactness"]["score"])
        self.assertFalse(correct["hallucination_or_unsupported_proxy"]["flagged"])

        incorrect = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "retain",
                "atomic_claims": [claim("An unsupported assertion.")],
            },
            contract,
            {
                "candidate_id": "candidate-1",
                "disposition": "reject",
                "atomic_claims": [],
            },
        )
        self.assertFalse(incorrect["disposition"]["exact_acceptable"])
        self.assertFalse(incorrect["value_state"]["exact_acceptable"])
        self.assertEqual(len(incorrect["unmatched_predicted"]), 1)
        self.assertTrue(incorrect["hallucination_or_unsupported_proxy"]["flagged"])

    def test_campaign_is_order_independent_and_accepts_mapping_inputs(self) -> None:
        text = "AI agents may retain context."
        contract = consensus(text)
        prediction = {
            "candidate_id": "candidate-1",
            "disposition": "revise",
            "atomic_claims": [claim(text)],
        }
        gold = {
            "candidate_id": "candidate-1",
            "disposition": "revise",
            "atomic_claims": [claim(text)],
        }
        score = score_campaign(
            {"candidate-1": prediction},
            {"candidate-1": contract},
            {"candidate-1": gold},
        )
        self.assertEqual(score["candidate_count"], 1)
        self.assertEqual(score["aggregate"]["atomic_count_acceptability"], 1.0)
        self.assertEqual(score["aggregate"]["claim_text_faithfulness_proxy"], 1.0)

    def test_atomic_count_uses_consensus_interval_not_only_enumerated_endpoints(self) -> None:
        contract = consensus("one", "two", "three")
        contract["acceptable_atomic_counts"] = [1, 3]
        contract["minimum_atomic_count"] = 1
        contract["maximum_atomic_count"] = 3
        score = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [claim("one"), claim("two")],
            },
            contract,
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [claim("one"), claim("two"), claim("three")],
            },
        )
        self.assertTrue(score["atomic_count"]["interval_semantics"])
        self.assertTrue(score["atomic_count"]["acceptable_count"])

    def test_faithfulness_uses_best_consensus_decomposition(self) -> None:
        contract = consensus("first", "second", "third")
        contract["acceptable_atomic_counts"] = [2, 3]
        contract["minimum_atomic_count"] = 2
        contract["maximum_atomic_count"] = 3
        contract["decompositions"] = [
            {
                "source": "pass_a",
                "disposition": "revise",
                "value_state": "value",
                "atomic_count": 3,
                "claim_texts": ["first", "second", "third"],
            },
            {
                "source": "pass_b",
                "disposition": "revise",
                "value_state": "value",
                "atomic_count": 2,
                "claim_texts": ["combined first", "combined second"],
            },
        ]
        score = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [
                    claim("combined first"),
                    claim("combined second"),
                ],
            },
            contract,
        )
        self.assertEqual(score["claim_text_faithfulness_proxy"]["score"], 1.0)
        self.assertTrue(score["atomic_count"]["acceptable_count"])

    def test_added_qualifier_is_penalized_by_precision_and_f1(self) -> None:
        reference = claim("Agents expose sensitive data.")
        predicted = claim("Agents may eventually expose sensitive data.")
        score = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [predicted],
            },
            consensus(reference["claim_text"]),
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [reference],
            },
        )
        pair_metric = score["alignment"][0]["qualifier_preservation_proxy"]
        self.assertEqual(pair_metric["recall"], 0.0)
        self.assertEqual(pair_metric["precision"], 0.0)
        self.assertEqual(pair_metric["f1"], 0.0)
        self.assertIn("possibility", pair_metric["added_markers"])

    def test_differing_matched_fields_are_divergence_not_hallucination(self) -> None:
        reference = claim("Independent evaluation is required.")
        reference["domain"] = "safety"
        predicted = copy.deepcopy(reference)
        predicted["domain"] = "marketing"
        predicted["subject_text"] = "Advertising"
        score = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [predicted],
            },
            consensus(reference["claim_text"]),
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [reference],
            },
        )
        mismatch_fields = {
            flag["field"]
            for flag in score["unsupported_field_flags"]
            if flag["kind"] == "predicted_field_reference_mismatch_proxy"
        }
        self.assertEqual({"domain", "subject_text"} & mismatch_fields, {"domain", "subject_text"})
        self.assertFalse(score["hallucination_or_unsupported_proxy"]["flagged"])
        self.assertTrue(score["field_divergence_proxy"]["flagged"])
        self.assertEqual(
            {
                flag["severity"]
                for flag in score["unsupported_field_flags"]
                if flag["field"] in {"domain", "subject_text"}
            },
            {"divergence"},
        )

    def test_low_token_precision_separates_hallucination_from_divergence(self) -> None:
        reference = claim("alpha beta")
        divergence = claim("alpha beta gamma delta epsilon")
        divergence_score = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [divergence],
            },
            consensus(reference["claim_text"]),
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [reference],
            },
        )
        self.assertFalse(
            divergence_score["hallucination_or_unsupported_proxy"]["flagged"]
        )
        self.assertTrue(divergence_score["field_divergence_proxy"]["flagged"])

        hallucination = claim("alpha gamma delta epsilon zeta")
        hallucination_score = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [hallucination],
            },
            consensus(reference["claim_text"]),
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [reference],
            },
        )
        self.assertTrue(
            hallucination_score["hallucination_or_unsupported_proxy"]["flagged"]
        )

    def test_public_alignment_references_are_local_ordinals(self) -> None:
        first = claim("First assertion.")
        second = claim("Second assertion.")
        score = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [second, first],
            },
            consensus(first["claim_text"], second["claim_text"]),
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [first, second],
            },
        )
        refs = {
            pair["predicted_ref"] for pair in score["alignment"]
        } | {pair["gold_ref"] for pair in score["alignment"]}
        self.assertEqual(refs, {"pred:0001", "pred:0002", "gold:0001", "gold:0002"})
        self.assertTrue(all(len(ref.split(":")[1]) == 4 for ref in refs))

    def test_campaign_rejects_implicit_partial_scope_and_allows_explicit_subset(self) -> None:
        contract_one = consensus("one", candidate_id="candidate-1")
        contract_two = consensus("two", candidate_id="candidate-2")
        prediction = {
            "candidate_id": "candidate-1",
            "disposition": "revise",
            "atomic_claims": [claim("one")],
        }
        with self.assertRaisesRegex(ValueError, "incomplete campaign"):
            score_campaign([prediction], [contract_one, contract_two])
        result = score_campaign(
            [prediction],
            [contract_one, contract_two],
            subset_candidate_ids={"candidate-1"},
        )
        self.assertTrue(result["subset_filtered"])
        self.assertEqual(result["candidate_count"], 1)

    def test_nonstrict_consensus_is_diagnostic_but_excluded_from_gates(self) -> None:
        strict_contract = consensus(
            "useful", candidate_id="candidate-1", strictly_scoreable=True
        )
        ambiguous_contract = consensus(
            "ambiguous", candidate_id="candidate-2", strictly_scoreable=False
        )
        predictions = [
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [claim("useful")],
            },
            {
                "candidate_id": "candidate-2",
                "disposition": "reject",
                "atomic_claims": [],
            },
        ]
        result = score_campaign(predictions, [strict_contract, ambiguous_contract])
        self.assertEqual(result["candidate_count"], 2)
        self.assertEqual(result["strictly_scoreable_candidate_count"], 1)
        self.assertEqual(result["excluded_nonstrict_candidate_count"], 1)
        self.assertEqual(result["aggregate"]["disposition_accuracy"], 1.0)

    def test_campaign_gate_metrics_and_positive_reported_actor_rates(self) -> None:
        value_contract = consensus("reported", candidate_id="value")
        junk_contract = consensus(
            disposition="reject", candidate_id="junk"
        )
        value_reference = claim("reported", speaker="Host", actor="Lab")
        value_prediction = claim("reported", speaker="Host", actor="Wrong Lab")
        predictions = [
            {
                "candidate_id": "value",
                "disposition": "revise",
                "atomic_claims": [value_prediction],
            },
            {
                "candidate_id": "junk",
                "disposition": "retain",
                "atomic_claims": [claim("escaped junk")],
            },
        ]
        gold = [
            {
                "candidate_id": "value",
                "disposition": "revise",
                "atomic_claims": [value_reference],
            },
            {
                "candidate_id": "junk",
                "disposition": "reject",
                "atomic_claims": [],
            },
        ]
        result = score_campaign(
            predictions, [value_contract, junk_contract], gold
        )
        aggregate = result["aggregate"]
        self.assertEqual(aggregate["retained_value_recall"], 1.0)
        self.assertEqual(aggregate["junk_escape_rate"], 1.0)
        self.assertLess(aggregate["disposition_macro_f1"], 1.0)
        self.assertEqual(aggregate["reported_actor_positive_precision"], 0.0)
        self.assertEqual(aggregate["reported_actor_positive_recall"], 0.0)
        self.assertEqual(
            aggregate["required_field_exactness"]["reported_actor"][
                "micro_denominator"
            ],
            1,
        )
        self.assertEqual(
            aggregate["required_field_exactness"]["reported_actor"][
                "coupled_diagnostic"
            ]["micro_denominator"],
            2,
        )
        self.assertIn("hallucination_rate_proxy", aggregate)
        self.assertIn("field_divergence_rate", aggregate)
        self.assertIn("unsupported_candidate_rate_proxy_legacy", aggregate)

    def test_consensus_only_real_shape_disables_unavailable_field_scoring(self) -> None:
        contract = {
            "candidate_id": "candidate-1",
            "consensus_state": "consensus_value",
            "strictly_scoreable": True,
            "acceptable_atomic_counts": [1],
            "minimum_atomic_count": 1,
            "maximum_atomic_count": 1,
            "acceptable_dispositions": ["retain", "revise"],
            "acceptable_value_states": ["value"],
            "preferred_disposition": "revise",
            "decompositions": [
                {
                    "source": "pass_a",
                    "atomic_count": 1,
                    "claim_texts": ["Agents may retain context."],
                    "disposition": "revise",
                    "value_state": "value",
                }
            ],
        }
        score = score_candidate(
            {
                "candidate_id": "candidate-1",
                "disposition": "revise",
                "atomic_claims": [claim("Agents may retain context.")],
            },
            contract,
        )
        self.assertEqual(score["consensus_state"], "consensus_value")
        self.assertFalse(score["field_reference_available"])
        self.assertIsNone(score["speaker_exactness"]["score"])
        self.assertFalse(score["unsupported_field_flags"])


if __name__ == "__main__":
    unittest.main()
