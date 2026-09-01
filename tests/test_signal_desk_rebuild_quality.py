import unittest
from collections import Counter

from research_factory.signal_desk_rebuild_quality import (
    ApprovalAction,
    ApprovalState,
    ClassifiedLegacyWindow,
    ContextScope,
    LegacyAuditBucket,
    LegacyErrorFamily,
    LegacyWindowComparison,
    OODGateObservation,
    OODGateThresholds,
    QualityContractError,
    RepresentationVariant,
    ShowShape,
    classify_legacy_comparison,
    evaluate_ood_gate,
    evaluate_shadow_release,
    record_approval_attempt,
    sample_legacy_audit,
    validate_representation_family,
    wider_context_packet,
)


class ApprovalContractTests(unittest.TestCase):
    def test_wider_context_packet_uses_three_turns_each_side_or_full_segment(self):
        turns = tuple({"speaker": f"s{i}", "text": str(i)} for i in range(10))
        packet = wider_context_packet(
            turns=turns, candidate_turn_index=5, full_segment="fallback"
        )
        self.assertEqual(packet["turn_start"], 2)
        self.assertEqual(packet["turn_end_exclusive"], 9)
        self.assertEqual(len(packet["turns"]), 7)
        fallback = wider_context_packet(
            turns=None, candidate_turn_index=None, full_segment="complete text"
        )
        self.assertEqual(fallback["context_source"], "full_segment")

    def test_wider_context_is_one_extra_attempt_not_a_semantic_sample(self):
        state = ApprovalState("sample-1")
        state = record_approval_attempt(
            state,
            action=ApprovalAction.REQUEST_WIDER_CONTEXT,
            context_scope=ContextScope.BOUNDED,
        )
        self.assertTrue(state.awaiting_wider_context)
        state = record_approval_attempt(
            state,
            action=ApprovalAction.CORRECT,
            context_scope=ContextScope.WIDE,
        )
        self.assertEqual(state.semantic_sample_count, 1)
        self.assertEqual(state.approval_attempt_count, 2)
        self.assertEqual(state.wider_context_retry_count, 1)
        self.assertEqual(state.terminal_action, ApprovalAction.CORRECT)

    def test_fail_closed_requires_wider_retry(self):
        with self.assertRaises(QualityContractError):
            record_approval_attempt(
                ApprovalState("sample-1"),
                action=ApprovalAction.FAIL_CLOSED,
                context_scope=ContextScope.BOUNDED,
            )
        state = record_approval_attempt(
            ApprovalState("sample-1"),
            action=ApprovalAction.REQUEST_WIDER_CONTEXT,
            context_scope=ContextScope.BOUNDED,
        )
        state = record_approval_attempt(
            state,
            action=ApprovalAction.FAIL_CLOSED,
            context_scope=ContextScope.WIDE,
        )
        self.assertEqual(state.terminal_action, ApprovalAction.FAIL_CLOSED)

    def test_second_wider_request_and_post_terminal_attempt_are_rejected(self):
        state = record_approval_attempt(
            ApprovalState("sample-1"),
            action="request_wider_context",
            context_scope="bounded",
        )
        with self.assertRaises(QualityContractError):
            record_approval_attempt(
                state,
                action="request_wider_context",
                context_scope="wide",
            )
        terminal = record_approval_attempt(
            ApprovalState("sample-2"), action="accept", context_scope="bounded"
        )
        with self.assertRaises(QualityContractError):
            record_approval_attempt(
                terminal, action="reject", context_scope="bounded"
            )


class OODContractTests(unittest.TestCase):
    def setUp(self):
        self.thresholds = OODGateThresholds(0.80, 0.90)

    def test_claim_dense_requires_recall_and_precision(self):
        with self.assertRaises(QualityContractError):
            evaluate_ood_gate(
                OODGateObservation(
                    "Fresh Air", ShowShape.CLAIM_DENSE, contamination_ucb=0.001
                ),
                self.thresholds,
            )
        result = evaluate_ood_gate(
            OODGateObservation(
                "Fresh Air",
                ShowShape.CLAIM_DENSE,
                contamination_ucb=0.001,
                recall_lcb=0.82,
                precision_lcb=0.91,
            ),
            self.thresholds,
        )
        self.assertTrue(result["passed"])

    def test_narrative_uses_contamination_and_false_positive_gates(self):
        result = evaluate_ood_gate(
            OODGateObservation(
                "The Moth",
                ShowShape.NARRATIVE,
                contamination_ucb=0.004,
                false_positive_claims=1,
            ),
            self.thresholds,
        )
        self.assertNotIn("recall_lcb", result["checks"])
        self.assertFalse(result["passed"])

    def test_shape_drift_fails_closed(self):
        with self.assertRaises(QualityContractError):
            evaluate_ood_gate(
                OODGateObservation(
                    "The Moth", ShowShape.CLAIM_DENSE, contamination_ucb=0.0
                ),
                self.thresholds,
            )


class LegacyAuditTests(unittest.TestCase):
    def test_classification_preserves_diagnostic_families(self):
        classified = classify_legacy_comparison(
            LegacyWindowComparison(
                "w1",
                matched_events=2,
                new_only_events=1,
                attribution_disagreements=1,
            )
        )
        self.assertEqual(classified.bucket, LegacyAuditBucket.DISAGREEMENT)
        self.assertEqual(
            classified.error_families,
            (LegacyErrorFamily.ATTRIBUTION, LegacyErrorFamily.NEW_ONLY),
        )

    def test_weighted_sampler_is_exact_and_deterministic(self):
        windows = []
        for bucket in LegacyAuditBucket:
            windows.extend(
                ClassifiedLegacyWindow(f"{bucket.value}-{index}", bucket, ())
                for index in range(100)
            )
        first = sample_legacy_audit(windows, sample_size=100, seed="campaign")
        second = sample_legacy_audit(windows, sample_size=100, seed="campaign")
        self.assertEqual(first, second)
        self.assertEqual(
            Counter(item.bucket for item in first),
            {
                LegacyAuditBucket.DISAGREEMENT: 50,
                LegacyAuditBucket.NEW_ONLY: 25,
                LegacyAuditBucket.LEGACY_ONLY: 15,
                LegacyAuditBucket.AGREEMENT: 10,
            },
        )

    def test_sampler_fails_instead_of_silently_changing_weights(self):
        windows = [
            ClassifiedLegacyWindow(f"agree-{index}", LegacyAuditBucket.AGREEMENT, ())
            for index in range(100)
        ]
        with self.assertRaises(QualityContractError):
            sample_legacy_audit(windows, sample_size=20, seed="campaign")


class RepresentationFamilyTests(unittest.TestCase):
    def test_one_change_lineage_across_rounds_three_to_nine(self):
        variants = (
            RepresentationVariant("rep-v1", "baseline", None, 3),
            RepresentationVariant(
                "rep-v1", "8k", "baseline", 3, window_chars=8_000
            ),
            RepresentationVariant(
                "rep-v1", "8k-overlap", "8k", 4, window_chars=8_000,
                turn_aligned_overlap=True,
            ),
            RepresentationVariant(
                "rep-v1", "8k-overlap-speakers", "8k-overlap", 5,
                window_chars=8_000, turn_aligned_overlap=True,
                speaker_map_header=True,
            ),
        )
        self.assertEqual(
            validate_representation_family(variants),
            ("8k", "8k-overlap", "8k-overlap-speakers", "baseline"),
        )

    def test_two_changes_and_out_of_round_are_rejected(self):
        baseline = RepresentationVariant("rep-v1", "baseline", None, 3)
        two_changes = RepresentationVariant(
            "rep-v1", "bad", "baseline", 4, window_chars=8_000,
            speaker_map_header=True,
        )
        with self.assertRaises(QualityContractError):
            validate_representation_family((baseline, two_changes))
        with self.assertRaises(QualityContractError):
            validate_representation_family(
                (RepresentationVariant("rep-v1", "baseline", None, 2),)
            )


class ShadowReleaseTests(unittest.TestCase):
    @staticmethod
    def _terminal_state(index, action):
        return record_approval_attempt(
            ApprovalState(f"sample-{index}"),
            action=action,
            context_scope=ContextScope.BOUNDED,
        )

    def test_one_thousand_processed_and_approval_band_pass(self):
        states = [
            self._terminal_state(index, ApprovalAction.ACCEPT if index < 800 else ApprovalAction.REJECT)
            for index in range(1_000)
        ]
        result = evaluate_shadow_release(states)
        self.assertTrue(result["processing_complete"])
        self.assertEqual(result["processed_through_gpt55"], 1_000)
        self.assertEqual(result["approval_attempts"], 1_000)
        self.assertEqual(result["approval_rate"], 0.8)
        self.assertTrue(result["passed"])

    def test_wider_retry_adds_attempt_not_sample(self):
        wide = record_approval_attempt(
            ApprovalState("wide"),
            action="request_wider_context",
            context_scope="bounded",
        )
        wide = record_approval_attempt(
            wide, action="accept", context_scope="wide"
        )
        direct = self._terminal_state(1, ApprovalAction.REJECT)
        result = evaluate_shadow_release((wide, direct), expected_samples=2)
        self.assertEqual(result["semantic_samples"], 2)
        self.assertEqual(result["approval_attempts"], 3)
        self.assertEqual(result["wider_context_retries"], 1)

    def test_boundaries_and_incomplete_processing_block_release(self):
        low = [
            self._terminal_state(index, ApprovalAction.ACCEPT if index < 59 else ApprovalAction.REJECT)
            for index in range(100)
        ]
        self.assertEqual(
            evaluate_shadow_release(low, expected_samples=100)["disposition"],
            "investigate_upstream_quality",
        )
        high = [
            self._terminal_state(index, ApprovalAction.ACCEPT if index < 96 else ApprovalAction.REJECT)
            for index in range(100)
        ]
        self.assertEqual(
            evaluate_shadow_release(high, expected_samples=100)["disposition"],
            "investigate_possible_rubber_stamping",
        )
        pending = ApprovalState("pending")
        result = evaluate_shadow_release((pending,), expected_samples=1)
        self.assertFalse(result["passed"])
        self.assertEqual(result["disposition"], "incomplete_approval_processing")

    def test_approval_band_boundaries_are_inclusive(self):
        for approved in (60, 95):
            states = [
                self._terminal_state(
                    index,
                    ApprovalAction.ACCEPT if index < approved else ApprovalAction.REJECT,
                )
                for index in range(100)
            ]
            self.assertTrue(
                evaluate_shadow_release(states, expected_samples=100)["passed"]
            )


if __name__ == "__main__":
    unittest.main()
