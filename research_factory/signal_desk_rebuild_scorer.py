"""Frozen deterministic scorer for the Signal Desk clean-corpus rebuild.

The scorer deliberately answers a narrow question: whether a predicted event
is eligible to represent one adjudicated gold event.  It does not use an LLM,
embeddings, or hidden transcript context. Claim identity controls matching;
attribution, issue, and stance are scored on every matched pair.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from pathlib import Path
from statistics import NormalDist
from typing import Any, Mapping, Optional, Sequence


SCORER_VERSION = "signal-desk-rebuild-scorer-v6"
SPEC_VERSION = "signal-desk-rebuild-scorer-spec-v6"
EVIDENCE_OVERLAP_FLOOR = 0.50
CLAIM_TEXT_F1_FLOOR = 0.30
QUALIFICATION_ALPHA = 0.05
QUALIFICATION_LCB = 0.97
QUALIFICATION_INITIAL_CASES = 100
QUALIFICATION_CASE_BLOCK = 50
QUALIFICATION_MAX_CASES = 500

_TOKEN_RE = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)?")
_HONORIFIC_RE = re.compile(r"^(?:dr|mr|mrs|ms|prof)\.?\s+", re.I)


class SignalDeskScorerError(ValueError):
    """Raised when an input would make scorer results ambiguous or unsound."""


def _text(value: Any) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _identity(value: Any) -> str:
    return _HONORIFIC_RE.sub("", _text(value)).strip()


def _tokens(value: Any) -> Counter[str]:
    return Counter(_TOKEN_RE.findall(_text(value)))


def _first(event: Mapping[str, Any], *paths: str) -> Any:
    for path in paths:
        current: Any = event
        for part in path.split("."):
            if not isinstance(current, Mapping):
                current = None
                break
            current = current.get(part)
        if current not in (None, ""):
            return current
    return None


_ROLE_ALIASES = {
    "direct": "direct_speech",
    "speaker": "direct_speech",
    "direct speech": "direct_speech",
    "direct_speech": "direct_speech",
    "quote": "quoted_speech",
    "quoted": "quoted_speech",
    "quoted speech": "quoted_speech",
    "quoted_speech": "quoted_speech",
    "mention": "third_party_mention",
    "mentioned": "third_party_mention",
    "third party": "third_party_mention",
    "third-party mention": "third_party_mention",
    "third_party_mention": "third_party_mention",
    "unresolved": "unresolved_speaker",
    "indeterminable": "unresolved_speaker",
    "unresolved speaker": "unresolved_speaker",
    "unresolved_speaker": "unresolved_speaker",
}

_STANCE_ALIASES = {
    "support": "supportive",
    "supports": "supportive",
    "positive": "supportive",
    "oppose": "skeptical",
    "opposes": "skeptical",
    "critical": "skeptical",
    "negative": "skeptical",
    "descriptive": "neutral",
    "reporting": "neutral",
    "qualified": "mixed",
    "caution": "warning",
    "cautious": "warning",
}


def _normalize_enum(value: Any, aliases: Mapping[str, str]) -> str:
    normalized = _text(value).replace("-", "_")
    return aliases.get(normalized, aliases.get(normalized.replace("_", " "), normalized))


def _alias_table(registry: Optional[Mapping[str, Any]]) -> dict[str, str]:
    table: dict[str, str] = {}
    for raw_key, raw_value in (registry or {}).items():
        key = _text(raw_key)
        if isinstance(raw_value, Sequence) and not isinstance(raw_value, (str, bytes)):
            table[key] = key
            for alias in raw_value:
                table[_text(alias)] = key
        else:
            table[key] = _text(raw_value)
    return table


def _canonical(value: Any, registry: Optional[Mapping[str, Any]] = None) -> str:
    normalized = _identity(value)
    return _alias_table(registry).get(normalized, normalized)


def _gold_entity_forms(gold: Mapping[str, Any], field: str, surface: str) -> set[str]:
    """Return only transcript-bound forms explicitly frozen into gold.

    ASR gold uses the transcript-surface spelling as authority. A canonical
    correction is eligible only when the gold author recorded it as an alias;
    this intentionally does not consult an external person registry.
    """

    forms = {_identity(surface)} if _identity(surface) else set()
    aliases = gold.get("entity_aliases") or {}
    if isinstance(aliases, Mapping):
        values = aliases.get(field) or []
        if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
            forms.update(_identity(value) for value in values if _identity(value))
    return forms


def _source_key(event: Mapping[str, Any]) -> str:
    return _text(
        _first(event, "transcript_id", "episode_id", "segment_id", "source.id", "source_id")
    )


def _span(event: Mapping[str, Any]) -> Optional[tuple[float, float]]:
    start = _first(event, "evidence_start", "evidence.start", "evidence_span.start")
    end = _first(event, "evidence_end", "evidence.end", "evidence_span.end")
    try:
        left, right = float(start), float(end)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(left) or not math.isfinite(right) or right <= left:
        return None
    return left, right


def evidence_overlap(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    """Return overlap coefficient for locations, falling back to evidence text.

    Numeric locations are comparable only inside the same explicitly supplied
    source.  The denominator is the shorter span, so a precise excerpt nested
    in a larger gold excerpt can still be an eligible grounding.
    """

    left_span, right_span = _span(left), _span(right)
    if left_span and right_span:
        left_source, right_source = _source_key(left), _source_key(right)
        if left_source and right_source and left_source != right_source:
            return 0.0
        intersection = max(0.0, min(left_span[1], right_span[1]) - max(left_span[0], right_span[0]))
        denominator = min(left_span[1] - left_span[0], right_span[1] - right_span[0])
        return intersection / denominator if denominator else 0.0

    left_evidence = _tokens(_first(left, "evidence_text", "evidence.text", "evidence"))
    right_evidence = _tokens(_first(right, "evidence_text", "evidence.text", "evidence"))
    if not left_evidence or not right_evidence:
        return 0.0
    overlap = sum((left_evidence & right_evidence).values())
    return overlap / min(sum(left_evidence.values()), sum(right_evidence.values()))


def _token_f1(left: Any, right: Any) -> float:
    a, b = _tokens(left), _tokens(right)
    if not a or not b:
        return 0.0
    overlap = sum((a & b).values())
    precision = overlap / sum(a.values())
    recall = overlap / sum(b.values())
    return 2 * precision * recall / (precision + recall) if precision + recall else 0.0


def _identity_set(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(sorted({_identity(item) for item in value if _identity(item)}))


def event_surface(
    event: Mapping[str, Any], *, issue_registry: Optional[Mapping[str, Any]] = None
) -> dict[str, Any]:
    """Normalize the fields that determine eligibility and diagnostics."""

    return {
        "speaker": _canonical(
            _first(event, "speaker_id", "speaker.name", "speaker", "actor.id", "actor.name")
        ),
        "speaker_role": _normalize_enum(
            _first(event, "speaker_role", "attribution_type", "evidence.attribution_type"),
            _ROLE_ALIASES,
        ),
        "subject": _canonical(
            _first(
                event,
                "subject_id",
                "canonical_entity_id",
                "subject.name",
                "subject_text",
                "actor.id",
                "actor.name",
            )
        ),
        "quoted_person": _canonical(
            _first(event, "quoted_person_id", "quoted_person.name", "quote.person_id")
        ),
        "mentioned_people": _identity_set(
            _first(event, "mentioned_person_ids", "mentioned_people", "mentions.people")
        ),
        "issue": _canonical(
            _first(
                event,
                "issue_id",
                "topic_id",
                "issue_label",
                "issue",
                "target.issue_id",
                "target.candidate_concept",
            ),
            issue_registry,
        ),
        "stance": _normalize_enum(_first(event, "stance", "position", "polarity"), _STANCE_ALIASES),
        "claim": _text(_first(event, "claim_text", "proposition_text", "claim")),
    }


def event_eligibility(
    gold: Mapping[str, Any],
    predicted: Mapping[str, Any],
    *,
    issue_registry: Optional[Mapping[str, Any]] = None,
    evidence_overlap_floor: float = EVIDENCE_OVERLAP_FLOOR,
    transcript_structure: str | None = None,
) -> dict[str, Any]:
    """Return a fully explained eligibility decision and deterministic weight."""

    if not 0.0 <= evidence_overlap_floor <= 1.0:
        raise SignalDeskScorerError("evidence_overlap_floor must be in [0, 1]")
    g = event_surface(gold, issue_registry=issue_registry)
    p = event_surface(predicted, issue_registry=issue_registry)
    overlap = evidence_overlap(gold, predicted)
    failures: list[str] = []
    structure = str(
        transcript_structure
        or gold.get("transcript_structure")
        or predicted.get("transcript_structure")
        or "speaker_turn"
    )
    flattened_indeterminable = (
        structure in {"flattened", "asr_diarized"}
        and g["speaker_role"] == "unresolved_speaker"
        and not g["speaker"]
    )
    unsupported_attribution = bool(
        flattened_indeterminable
        and (p["speaker"] or p["speaker_role"] != "unresolved_speaker")
    )
    speaker_agreement = g["speaker"] == p["speaker"]
    subject_agreement = g["subject"] == p["subject"]
    if structure == "asr_diarized" and not speaker_agreement:
        speaker_agreement = p["speaker"] in _gold_entity_forms(gold, "speaker_id", g["speaker"])
    if structure == "asr_diarized" and not subject_agreement:
        subject_agreement = p["subject"] in _gold_entity_forms(gold, "subject_id", g["subject"])
    if overlap < evidence_overlap_floor:
        failures.append("insufficient_evidence_overlap")
    claim_f1 = _token_f1(g["claim"], p["claim"])
    if claim_f1 < CLAIM_TEXT_F1_FLOOR:
        failures.append("insufficient_claim_text_agreement")
    eligible = not failures
    # Only evidence and atomic-claim identity control matching. Field quality
    # is measured on the resulting pair and can therefore never disappear into
    # recall or inflate accuracy by pre-filtering disagreements.
    weight = (0.70 * overlap) + (0.30 * claim_f1) if eligible else 0.0
    return {
        "eligible": eligible,
        "failures": failures,
        "evidence_overlap": round(overlap, 6),
        "claim_text_f1": round(claim_f1, 6),
        "weight": round(weight, 6),
        "gold_surface": g,
        "predicted_surface": p,
        "transcript_structure": structure,
        "unsupported_attribution": unsupported_attribution,
        "field_agreement": {
            "speaker": speaker_agreement,
            "speaker_role": g["speaker_role"] == p["speaker_role"],
            "subject": subject_agreement,
            "issue": g["issue"] == p["issue"],
            "stance": g["stance"] == p["stance"],
            "quoted_person": g["quoted_person"] == p["quoted_person"],
            "mentioned_people": g["mentioned_people"] == p["mentioned_people"],
        },
    }


def _maximum_weight_assignment(matrix: Sequence[Sequence[float]]) -> list[tuple[int, int]]:
    """Hungarian assignment for a rectangular non-negative weight matrix."""

    if not matrix or not matrix[0]:
        return []
    rows, cols = len(matrix), len(matrix[0])
    size = max(rows, cols)
    maximum = max((max(row) for row in matrix), default=0.0)
    # Classic shortest augmenting path formulation for min-cost assignment.
    costs = [
        [maximum - (matrix[i][j] if i < rows and j < cols else 0.0) for j in range(size)]
        for i in range(size)
    ]
    u = [0.0] * (size + 1)
    v = [0.0] * (size + 1)
    p = [0] * (size + 1)
    way = [0] * (size + 1)
    for i in range(1, size + 1):
        p[0] = i
        j0 = 0
        minv = [math.inf] * (size + 1)
        used = [False] * (size + 1)
        while True:
            used[j0] = True
            i0 = p[j0]
            delta = math.inf
            j1 = 0
            for j in range(1, size + 1):
                if used[j]:
                    continue
                cur = costs[i0 - 1][j - 1] - u[i0] - v[j]
                if cur < minv[j]:
                    minv[j], way[j] = cur, j0
                if minv[j] < delta:
                    delta, j1 = minv[j], j
            for j in range(size + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minv[j] -= delta
            j0 = j1
            if p[j0] == 0:
                break
        while True:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1
            if j0 == 0:
                break
    pairs = [(p[j] - 1, j - 1) for j in range(1, size + 1) if p[j]]
    return sorted((i, j) for i, j in pairs if i < rows and j < cols)


def match_events(
    gold_events: Sequence[Mapping[str, Any]],
    predicted_events: Sequence[Mapping[str, Any]],
    *,
    issue_registry: Optional[Mapping[str, Any]] = None,
    transcript_structure: str | None = None,
) -> dict[str, Any]:
    """Return optimal eligible one-to-one matches and event-level counts."""

    decisions = [
        [
            event_eligibility(
                gold,
                predicted,
                issue_registry=issue_registry,
                transcript_structure=transcript_structure,
            )
            for predicted in predicted_events
        ]
        for gold in gold_events
    ]
    matrix = [[cell["weight"] for cell in row] for row in decisions]
    matches = []
    for gold_index, predicted_index in _maximum_weight_assignment(matrix):
        decision = decisions[gold_index][predicted_index]
        if decision["eligible"] and decision["weight"] > 0:
            matches.append(
                {
                    "gold_index": gold_index,
                    "predicted_index": predicted_index,
                    **decision,
                }
            )
    matched = len(matches)
    unsupported_prediction_indexes = sorted(
        {
            predicted_index
            for row in decisions
            for predicted_index, decision in enumerate(row)
            if decision.get("unsupported_attribution")
            and decision.get("evidence_overlap", 0) >= EVIDENCE_OVERLAP_FLOOR
            and decision.get("claim_text_f1", 0) >= CLAIM_TEXT_F1_FLOOR
        }
    )
    return {
        "gold_events": len(gold_events),
        "predicted_events": len(predicted_events),
        "matched_events": matched,
        "false_negatives": len(gold_events) - matched,
        "false_positives": len(predicted_events) - matched,
        "matches": matches,
        "transcript_structure": transcript_structure,
        "unsupported_attributions": len(unsupported_prediction_indexes),
    }


def scorer_specification() -> dict[str, Any]:
    return {
        "spec_version": SPEC_VERSION,
        "scorer_version": SCORER_VERSION,
        "matching": "maximum_weight_one_to_one_hungarian",
        "event_credit": "binary_after_eligibility_no_partial_tp_credit",
        "evidence_overlap": {
            "kind": "overlap_coefficient_shorter_span_denominator",
            "minimum": EVIDENCE_OVERLAP_FLOOR,
            "cross_source_numeric_spans": "ineligible",
            "text_fallback": "multiset_token_overlap_coefficient",
        },
        "matching_fields": ["evidence_overlap", "claim_text_token_f1"],
        "diagnostic_agreement_fields": [
            "speaker", "speaker_role", "subject", "issue", "stance",
            "quoted_person", "mentioned_people", "unsupported_attribution",
        ],
        "issue_policy": (
            "issue labels are extraction-time proposals and do not control event matching; "
            "raw-label agreement is diagnostic only and is excluded from the extraction "
            "composite. Stable issue identity is evaluated by the separately frozen "
            "canonicalization stage"
        ),
        "clean_event_mapping": {
            "speaker": "speaker_id",
            "speaker_role": "attribution_type",
            "issue": "issue_label with issue_id/topic_id compatibility",
            "quoted_person": "quoted_person_id when attribution_type is quoted_speech",
            "mentioned_people": "mentioned_person_ids when attribution_type is third_party_mention",
            "subject": "optional legacy compatibility field only",
        },
        "transcript_structure_policy": {
            "speaker_turn": "full_speaker_and_role_agreement",
            "paragraph": "full_speaker_and_role_agreement_when_present",
            "flattened": (
                "gold speaker is indeterminable unless named in text; any asserted unsupported "
                "speaker is an attribution error"
            ),
            "asr_diarized": (
                "gold binds to normalized transcript-surface entity forms; a canonical spelling "
                "matches only when frozen in gold entity_aliases; every third spelling is an error"
            ),
        },
        "asr_entity_matching": {
            "surface_normalization": "unicode string casefold plus collapsed whitespace",
            "gold_authority": "transcript_surface_form",
            "canonical_form": "eligible_only_when_recorded_in_gold_entity_aliases",
            "external_registry_lookup": "forbidden",
            "unrecorded_third_form": "entity_disagreement",
        },
        "claim_text": {
            "kind": "multiset_token_f1",
            "minimum": CLAIM_TEXT_F1_FLOOR,
            "purpose": "prevent different atomic propositions sharing one span from matching",
        },
        "merge_policy": "one_prediction_matches_at_most_one_gold_event",
        "split_policy": (
            "one-to-one assignment plus separate same-span multiplicity atomicity penalty"
        ),
        "duplicate_policy": "unmatched_predictions_are_false_positives",
        "weight": {"evidence_overlap": 0.70, "claim_text_token_f1": 0.30},
    }


def scorer_sha256() -> str:
    encoded = json.dumps(
        {
            "specification": scorer_specification(),
            "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _wilson_lower(successes: int, total: int, alpha: float) -> float:
    if total <= 0:
        return 0.0
    z = NormalDist().inv_cdf(1.0 - alpha)
    p = successes / total
    denominator = 1.0 + z * z / total
    center = (p + z * z / (2.0 * total)) / denominator
    margin = z * math.sqrt(p * (1.0 - p) / total + z * z / (4.0 * total * total)) / denominator
    return max(0.0, center - margin)


def qualify_scorer(decisions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Evaluate the frozen scorer against stratified adjudicated decisions.

    Each row requires ``expected_match``, ``scorer_match``, and
    ``error_family``.  The first 100 must include at least ten families and a
    40/60-or-better expected match/no-match balance.  More cases are requested
    in 50-case blocks until the 97% one-sided Wilson LCB clears or 500 cases
    have been evaluated.
    """

    total = len(decisions)
    required_keys = {"expected_match", "scorer_match", "error_family"}
    for index, row in enumerate(decisions):
        missing = required_keys - row.keys()
        if missing:
            raise SignalDeskScorerError(
                f"qualification row {index} is missing: {', '.join(sorted(missing))}"
            )
    if total < QUALIFICATION_INITIAL_CASES:
        return {
            "status": "needs_more_cases",
            "required_total": QUALIFICATION_INITIAL_CASES,
            "evaluated": total,
            "scorer_sha256": scorer_sha256(),
        }
    if total > QUALIFICATION_MAX_CASES:
        raise SignalDeskScorerError("qualification may not exceed 500 adjudicated cases")
    if total > QUALIFICATION_INITIAL_CASES and total % QUALIFICATION_CASE_BLOCK:
        raise SignalDeskScorerError("qualification expansions must use 50-case blocks")
    families = {_text(row.get("error_family")) for row in decisions if _text(row.get("error_family"))}
    expected_matches = sum(bool(row.get("expected_match")) for row in decisions)
    if len(families) < 10:
        raise SignalDeskScorerError("qualification requires at least ten error families")
    if not 0.40 <= expected_matches / total <= 0.60:
        raise SignalDeskScorerError("qualification requires balanced match/no-match decisions")
    agreements = sum(
        bool(row.get("expected_match")) == bool(row.get("scorer_match")) for row in decisions
    )
    point = agreements / total
    lower = _wilson_lower(agreements, total, QUALIFICATION_ALPHA)
    if lower >= QUALIFICATION_LCB:
        status = "qualified"
        required_total = total
    elif total >= QUALIFICATION_MAX_CASES:
        status = "failed"
        required_total = None
    else:
        status = "needs_more_cases"
        required_total = min(QUALIFICATION_MAX_CASES, total + QUALIFICATION_CASE_BLOCK)
    return {
        "status": status,
        "evaluated": total,
        "agreements": agreements,
        "point_agreement": round(point, 6),
        "agreement_lcb": round(lower, 6),
        "alpha": QUALIFICATION_ALPHA,
        "required_lcb": QUALIFICATION_LCB,
        "required_total": required_total,
        "error_families": len(families),
        "scorer_sha256": scorer_sha256(),
    }


def scorer_registry_entry(qualification: Mapping[str, Any]) -> dict[str, Any]:
    if qualification.get("status") != "qualified":
        raise SignalDeskScorerError("an unqualified scorer cannot be frozen in the registry")
    return {
        "kind": "signal_desk_rebuild_scorer",
        "version": SCORER_VERSION,
        "sha256": scorer_sha256(),
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "test_suite": ["tests/test_signal_desk_rebuild_scorer.py"],
        "specification": scorer_specification(),
        "qualification": dict(qualification),
        "frozen": True,
    }
