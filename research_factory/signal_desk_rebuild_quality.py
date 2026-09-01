"""Quality contracts for the Signal Desk clean-corpus rebuild.

This module is deliberately side-effect free.  It defines the frozen approval,
OOD, legacy-audit, representation-experiment, and shadow-release contracts used
by the rebuild harness.  Provider transports and persistence live elsewhere.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "pif_signal_desk_rebuild_quality_v1"
SHADOW_SAMPLE_SIZE = 1_000
SHADOW_APPROVAL_RATE_MIN = 0.60
SHADOW_APPROVAL_RATE_MAX = 0.95
MAX_WIDER_CONTEXT_RETRIES = 1
WIDER_CONTEXT_TURNS_EACH_SIDE = 3


class QualityContractError(ValueError):
    """Raised when an experiment violates a frozen quality contract."""


class ApprovalAction(str, Enum):
    ACCEPT = "accept"
    CORRECT = "correct"
    SPLIT = "split"
    REJECT = "reject"
    REQUIRE_AUDIO = "require_audio"
    REQUEST_WIDER_CONTEXT = "request_wider_context"
    FAIL_CLOSED = "fail_closed"


class ContextScope(str, Enum):
    BOUNDED = "bounded"
    WIDE = "wide"


APPROVED_ACTIONS = frozenset(
    {ApprovalAction.ACCEPT, ApprovalAction.CORRECT, ApprovalAction.SPLIT}
)
TERMINAL_ACTIONS = frozenset(
    action
    for action in ApprovalAction
    if action != ApprovalAction.REQUEST_WIDER_CONTEXT
)


@dataclass(frozen=True)
class ApprovalAttempt:
    """One GPT-5.5 approval call for an existing semantic sample."""

    attempt_number: int
    context_scope: ContextScope
    action: ApprovalAction


@dataclass(frozen=True)
class ApprovalState:
    """State machine for one semantic candidate.

    A wider-context request adds an approval *attempt* to this same state; it
    never creates a second semantic sample.
    """

    semantic_sample_id: str
    attempts: tuple[ApprovalAttempt, ...] = ()

    @property
    def semantic_sample_count(self) -> int:
        return 1

    @property
    def approval_attempt_count(self) -> int:
        return len(self.attempts)

    @property
    def terminal_action(self) -> ApprovalAction | None:
        if self.attempts and self.attempts[-1].action in TERMINAL_ACTIONS:
            return self.attempts[-1].action
        return None

    @property
    def awaiting_wider_context(self) -> bool:
        return bool(
            self.attempts
            and self.attempts[-1].action == ApprovalAction.REQUEST_WIDER_CONTEXT
        )

    @property
    def wider_context_retry_count(self) -> int:
        return sum(
            attempt.context_scope == ContextScope.WIDE for attempt in self.attempts
        )


def record_approval_attempt(
    state: ApprovalState,
    *,
    action: ApprovalAction | str,
    context_scope: ContextScope | str,
) -> ApprovalState:
    """Apply one approval decision while enforcing the single-wide-retry rule."""

    if not state.semantic_sample_id.strip():
        raise QualityContractError("semantic_sample_id must not be empty")
    if state.terminal_action is not None:
        raise QualityContractError("terminal approval state cannot receive another attempt")
    action = ApprovalAction(action)
    context_scope = ContextScope(context_scope)
    attempt_number = len(state.attempts) + 1

    if not state.attempts:
        if context_scope != ContextScope.BOUNDED:
            raise QualityContractError("the first approval attempt must use bounded context")
        if action == ApprovalAction.FAIL_CLOSED:
            raise QualityContractError(
                "fail_closed requires the single wider-context retry first"
            )
    else:
        if not state.awaiting_wider_context:
            raise QualityContractError("only a wider-context request may create a retry")
        if context_scope != ContextScope.WIDE:
            raise QualityContractError(
                "the retry after request_wider_context must use wide context"
            )
        if state.wider_context_retry_count >= MAX_WIDER_CONTEXT_RETRIES:
            raise QualityContractError("only one wider-context retry is allowed")
        if action == ApprovalAction.REQUEST_WIDER_CONTEXT:
            raise QualityContractError("wide context cannot be requested more than once")

    attempt = ApprovalAttempt(
        attempt_number=attempt_number,
        context_scope=context_scope,
        action=action,
    )
    return replace(state, attempts=state.attempts + (attempt,))


def wider_context_packet(
    *,
    turns: Sequence[Mapping[str, Any]] | None,
    candidate_turn_index: int | None,
    full_segment: str,
) -> dict[str, object]:
    """Build the one allowed GPT-5.5 escalation packet.

    Speaker-turn data gets the candidate turn plus three turns on either side.
    Flattened transcripts or invalid turn pointers fail over to the complete
    segment instead of reconstructing another clipped window.
    """

    if turns and candidate_turn_index is not None and 0 <= candidate_turn_index < len(turns):
        start = max(0, candidate_turn_index - WIDER_CONTEXT_TURNS_EACH_SIDE)
        end = min(len(turns), candidate_turn_index + WIDER_CONTEXT_TURNS_EACH_SIDE + 1)
        selected = tuple(dict(turn) for turn in turns[start:end])
        return {
            "context_scope": ContextScope.WIDE.value,
            "context_source": "speaker_turns_plus_minus_three",
            "turn_start": start,
            "turn_end_exclusive": end,
            "turns": selected,
        }
    if not full_segment.strip():
        raise QualityContractError(
            "wider context requires speaker turns or a nonempty full segment"
        )
    return {
        "context_scope": ContextScope.WIDE.value,
        "context_source": "full_segment",
        "full_segment": full_segment,
    }


class ShowShape(str, Enum):
    CLAIM_DENSE = "claim_dense"
    NARRATIVE = "narrative"
    STRUCTURAL = "structural"


# Frozen before evaluation; changing membership requires a new campaign.
OOD_SHOW_SHAPES: Mapping[str, ShowShape] = MappingProxyType({
    "Freakonomics": ShowShape.CLAIM_DENSE,
    "Fresh Air": ShowShape.CLAIM_DENSE,
    "This American Life": ShowShape.CLAIM_DENSE,
    "Hidden Brain": ShowShape.CLAIM_DENSE,
    "On Being": ShowShape.CLAIM_DENSE,
    "The Moth": ShowShape.NARRATIVE,
    "Ear Hustle": ShowShape.NARRATIVE,
    "99% Invisible": ShowShape.NARRATIVE,
    "Gastropod": ShowShape.STRUCTURAL,
    "The Allusionist": ShowShape.STRUCTURAL,
})


@dataclass(frozen=True)
class OODGateThresholds:
    claim_dense_recall_lcb: float
    claim_dense_precision_lcb: float
    contamination_ucb_max: float = 0.005
    narrative_false_positive_max: int = 0


@dataclass(frozen=True)
class OODGateObservation:
    show_name: str
    shape: ShowShape
    contamination_ucb: float
    recall_lcb: float | None = None
    precision_lcb: float | None = None
    false_positive_claims: int = 0


def evaluate_ood_gate(
    observation: OODGateObservation,
    thresholds: OODGateThresholds,
) -> dict[str, object]:
    """Evaluate only metrics meaningful for a show's pre-frozen shape."""

    threshold_values = (
        thresholds.claim_dense_recall_lcb,
        thresholds.claim_dense_precision_lcb,
        thresholds.contamination_ucb_max,
    )
    if any(not 0.0 <= value <= 1.0 for value in threshold_values):
        raise QualityContractError("OOD rate thresholds must be in [0, 1]")
    if thresholds.narrative_false_positive_max < 0:
        raise QualityContractError("narrative false-positive threshold must be nonnegative")
    expected_shape = OOD_SHOW_SHAPES.get(observation.show_name)
    if expected_shape is None:
        raise QualityContractError(f"unregistered OOD show: {observation.show_name}")
    if observation.shape != expected_shape:
        raise QualityContractError(
            f"OOD shape drift for {observation.show_name}: "
            f"expected {expected_shape.value}, got {observation.shape.value}"
        )
    if not 0.0 <= observation.contamination_ucb <= 1.0:
        raise QualityContractError("contamination_ucb must be in [0, 1]")

    checks: dict[str, bool] = {
        "contamination_ucb": observation.contamination_ucb
        <= thresholds.contamination_ucb_max
    }
    if observation.shape == ShowShape.CLAIM_DENSE:
        if observation.recall_lcb is None or observation.precision_lcb is None:
            raise QualityContractError(
                "claim-dense OOD shows require recall and precision LCBs"
            )
        if not 0.0 <= observation.recall_lcb <= 1.0 or not 0.0 <= observation.precision_lcb <= 1.0:
            raise QualityContractError("recall_lcb and precision_lcb must be in [0, 1]")
        checks.update(
            {
                "recall_lcb": observation.recall_lcb
                >= thresholds.claim_dense_recall_lcb,
                "precision_lcb": observation.precision_lcb
                >= thresholds.claim_dense_precision_lcb,
            }
        )
    elif observation.shape == ShowShape.NARRATIVE:
        checks["false_positive_claims"] = (
            observation.false_positive_claims
            <= thresholds.narrative_false_positive_max
        )
    # Structural sources stress input boundaries and use contamination only.
    return {
        "schema_version": SCHEMA_VERSION,
        "show_name": observation.show_name,
        "shape": observation.shape.value,
        "checks": checks,
        "passed": all(checks.values()),
    }


class LegacyAuditBucket(str, Enum):
    DISAGREEMENT = "disagreement"
    NEW_ONLY = "new_only"
    LEGACY_ONLY = "legacy_only"
    AGREEMENT = "agreement"


class LegacyErrorFamily(str, Enum):
    FIELD = "field_disagreement"
    ATTRIBUTION = "attribution_disagreement"
    STANCE = "stance_disagreement"
    BOUNDARY = "boundary_disagreement"
    NEW_ONLY = "new_only_event"
    LEGACY_ONLY = "legacy_only_event"


@dataclass(frozen=True)
class LegacyWindowComparison:
    window_id: str
    matched_events: int
    new_only_events: int = 0
    legacy_only_events: int = 0
    attribution_disagreements: int = 0
    stance_disagreements: int = 0
    boundary_disagreements: int = 0
    other_field_disagreements: int = 0


@dataclass(frozen=True)
class ClassifiedLegacyWindow:
    window_id: str
    bucket: LegacyAuditBucket
    error_families: tuple[LegacyErrorFamily, ...]


def classify_legacy_comparison(
    comparison: LegacyWindowComparison,
) -> ClassifiedLegacyWindow:
    """Classify legacy labels for audit sampling, never for correctness voting."""

    counts = (
        comparison.matched_events,
        comparison.new_only_events,
        comparison.legacy_only_events,
        comparison.attribution_disagreements,
        comparison.stance_disagreements,
        comparison.boundary_disagreements,
        comparison.other_field_disagreements,
    )
    if not comparison.window_id.strip() or any(value < 0 for value in counts):
        raise QualityContractError("legacy comparison requires an id and nonnegative counts")

    families: list[LegacyErrorFamily] = []
    if comparison.attribution_disagreements:
        families.append(LegacyErrorFamily.ATTRIBUTION)
    if comparison.stance_disagreements:
        families.append(LegacyErrorFamily.STANCE)
    if comparison.boundary_disagreements:
        families.append(LegacyErrorFamily.BOUNDARY)
    if comparison.other_field_disagreements:
        families.append(LegacyErrorFamily.FIELD)
    if comparison.new_only_events:
        families.append(LegacyErrorFamily.NEW_ONLY)
    if comparison.legacy_only_events:
        families.append(LegacyErrorFamily.LEGACY_ONLY)

    has_field_disagreement = any(
        (
            comparison.attribution_disagreements,
            comparison.stance_disagreements,
            comparison.boundary_disagreements,
            comparison.other_field_disagreements,
        )
    )
    if has_field_disagreement or (
        comparison.new_only_events and comparison.legacy_only_events
    ):
        bucket = LegacyAuditBucket.DISAGREEMENT
    elif comparison.new_only_events:
        bucket = LegacyAuditBucket.NEW_ONLY
    elif comparison.legacy_only_events:
        bucket = LegacyAuditBucket.LEGACY_ONLY
    else:
        bucket = LegacyAuditBucket.AGREEMENT
    return ClassifiedLegacyWindow(comparison.window_id, bucket, tuple(families))


LEGACY_AUDIT_WEIGHTS: Mapping[LegacyAuditBucket, float] = {
    LegacyAuditBucket.DISAGREEMENT: 0.50,
    LegacyAuditBucket.NEW_ONLY: 0.25,
    LegacyAuditBucket.LEGACY_ONLY: 0.15,
    LegacyAuditBucket.AGREEMENT: 0.10,
}


def sample_legacy_audit(
    windows: Iterable[ClassifiedLegacyWindow],
    *,
    sample_size: int,
    seed: str,
) -> tuple[ClassifiedLegacyWindow, ...]:
    """Return a deterministic, no-replacement, exactly weighted blind sample."""

    if sample_size <= 0 or sample_size % 20:
        raise QualityContractError("sample_size must be a positive multiple of 20")
    by_bucket: dict[LegacyAuditBucket, list[ClassifiedLegacyWindow]] = {
        bucket: [] for bucket in LegacyAuditBucket
    }
    seen: set[str] = set()
    for window in windows:
        if window.window_id in seen:
            raise QualityContractError(f"duplicate window_id: {window.window_id}")
        seen.add(window.window_id)
        by_bucket[window.bucket].append(window)

    rng = random.Random(seed)
    selected: list[ClassifiedLegacyWindow] = []
    for bucket, weight in LEGACY_AUDIT_WEIGHTS.items():
        required = int(sample_size * weight)
        candidates = sorted(by_bucket[bucket], key=lambda item: item.window_id)
        if len(candidates) < required:
            raise QualityContractError(
                f"insufficient {bucket.value} windows: need {required}, "
                f"have {len(candidates)}"
            )
        selected.extend(rng.sample(candidates, required))
    rng.shuffle(selected)
    return tuple(selected)


@dataclass(frozen=True)
class RepresentationVariant:
    family_id: str
    variant_id: str
    parent_id: str | None
    introduced_round: int
    window_chars: int = 6_000
    turn_aligned_overlap: bool = False
    speaker_map_header: bool = False


def validate_representation_family(
    variants: Sequence[RepresentationVariant],
) -> tuple[str, ...]:
    """Validate a rounds 3-9, one-change-per-child representation lineage."""

    if not variants:
        raise QualityContractError("representation family must not be empty")
    family_ids = {variant.family_id for variant in variants}
    if len(family_ids) != 1 or not next(iter(family_ids)).strip():
        raise QualityContractError("variants must share one nonempty family_id")
    by_id = {variant.variant_id: variant for variant in variants}
    if len(by_id) != len(variants) or any(not key.strip() for key in by_id):
        raise QualityContractError("variant_id values must be unique and nonempty")
    roots = [variant for variant in variants if variant.parent_id is None]
    if len(roots) != 1:
        raise QualityContractError("representation family requires exactly one baseline")
    root = roots[0]
    if (
        root.window_chars != 6_000
        or root.turn_aligned_overlap
        or root.speaker_map_header
    ):
        raise QualityContractError("baseline must be the frozen 6,000-character contract")

    configuration_fields = (
        "window_chars",
        "turn_aligned_overlap",
        "speaker_map_header",
    )
    for variant in variants:
        if not 3 <= variant.introduced_round <= 9:
            raise QualityContractError("representation variants must run in rounds 3-9")
        if variant.window_chars <= 0:
            raise QualityContractError("window_chars must be positive")
        if variant is root:
            continue
        parent = by_id.get(variant.parent_id or "")
        if parent is None:
            raise QualityContractError(f"unknown parent for {variant.variant_id}")
        if parent.introduced_round > variant.introduced_round:
            raise QualityContractError("a child cannot precede its parent")
        changed = sum(
            getattr(parent, field) != getattr(variant, field)
            for field in configuration_fields
        )
        if changed != 1:
            raise QualityContractError(
                f"{variant.variant_id} must change exactly one representation field"
            )

    # Parent traversal catches cycles even when every immediate parent exists.
    for variant in variants:
        visited: set[str] = set()
        cursor = variant
        while cursor.parent_id is not None:
            if cursor.variant_id in visited:
                raise QualityContractError("representation lineage contains a cycle")
            visited.add(cursor.variant_id)
            cursor = by_id[cursor.parent_id]
    return tuple(sorted(by_id))


def evaluate_shadow_release(
    states: Sequence[ApprovalState],
    *,
    expected_samples: int = SHADOW_SAMPLE_SIZE,
) -> dict[str, object]:
    """Gate the 1,000-window shadow on processing coverage and approval band."""

    if expected_samples <= 0:
        raise QualityContractError("expected_samples must be positive")
    ids = [state.semantic_sample_id for state in states]
    if len(set(ids)) != len(ids):
        raise QualityContractError("shadow semantic_sample_id values must be unique")
    processed = sum(state.terminal_action is not None for state in states)
    approved = sum(state.terminal_action in APPROVED_ACTIONS for state in states)
    total_attempts = sum(state.approval_attempt_count for state in states)
    wider_retries = sum(state.wider_context_retry_count for state in states)
    complete = len(states) == expected_samples and processed == expected_samples
    approval_rate = approved / processed if processed else 0.0
    within_band = SHADOW_APPROVAL_RATE_MIN <= approval_rate <= SHADOW_APPROVAL_RATE_MAX
    if not complete:
        disposition = "incomplete_approval_processing"
    elif approval_rate > SHADOW_APPROVAL_RATE_MAX:
        disposition = "investigate_possible_rubber_stamping"
    elif approval_rate < SHADOW_APPROVAL_RATE_MIN:
        disposition = "investigate_upstream_quality"
    else:
        disposition = "pass"
    return {
        "schema_version": SCHEMA_VERSION,
        "expected_semantic_samples": expected_samples,
        "semantic_samples": len(states),
        "processed_through_gpt55": processed,
        "approval_attempts": total_attempts,
        "wider_context_retries": wider_retries,
        "approved_semantic_samples": approved,
        "approval_rate": round(approval_rate, 6),
        "approval_band": [SHADOW_APPROVAL_RATE_MIN, SHADOW_APPROVAL_RATE_MAX],
        "processing_complete": complete,
        "approval_rate_within_band": within_band,
        "passed": complete and within_band,
        "disposition": disposition,
    }
