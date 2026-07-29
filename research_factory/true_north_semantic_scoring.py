"""Deterministic, candidate-local scoring for True North atomic outputs.

This module deliberately does *not* claim to determine semantic equivalence.
Its text and qualifier measurements are lexical heuristics/proxies.  It never
uses a model, embeddings, transcript text, or any external service.

The public campaign interface accepts the frozen consensus contract and,
optionally, the preferred adjudicated gold items:

    score_campaign(predictions, consensus_items, preferred_gold_items)

Each input may be either a sequence of candidate dictionaries or a mapping
from candidate ID to candidate dictionary.  Candidate claims are aligned
one-to-one with a deterministic maximum-weight bipartite assignment.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from difflib import SequenceMatcher
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "pif_true_north_semantic_scoring_v3"
METRIC_KIND = "deterministic_lexical_and_field_proxy"

_FIELD_NAMES = (
    "claim_text",
    "proposition_text",
    "raw_speaker",
    "reported_actor",
    "subject_text",
    "subject_type",
    "claim_type",
    "domain",
    "time_horizon",
    "certainty",
    "stance",
    "polarity",
    "position",
    "confidence",
    "evidence_text",
    "evidence_start",
    "evidence_end",
)
_UNSUPPORTED_CHECK_FIELDS = _FIELD_NAMES
_QUALIFIER_PATTERNS: tuple[tuple[str, str], ...] = (
    ("negation", r"\b(?:no|not|never|neither|nor|without|cannot|can't|won't)\b"),
    ("possibility", r"\b(?:may|might|could|possibly|potentially|perhaps)\b"),
    ("probability", r"\b(?:likely|unlikely|probably|usually|generally|often)\b"),
    ("necessity", r"\b(?:must|need(?:s|ed)? to|required|necessary)\b"),
    ("recommendation", r"\b(?:should|ought to|recommend(?:s|ed)?)\b"),
    ("conditional", r"\b(?:if|unless|provided that|depending on|when)\b"),
    ("past", r"\b(?:previously|formerly|historically|in the past|used to)\b"),
    ("present", r"\b(?:currently|now|today|at present)\b"),
    ("future", r"\b(?:future|eventually|will|going to|soon|later)\b"),
    ("scope_some", r"\b(?:some|many|several|often|sometimes)\b"),
    ("scope_all", r"\b(?:all|every|always|none|never)\b"),
    ("comparison", r"\b(?:more than|less than|compared with|relative to)\b"),
)


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().casefold().split())


def _tokens(value: Any) -> list[str]:
    return re.findall(r"[a-z0-9]+(?:'[a-z0-9]+)?", _text(value))


def _fingerprint(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:20]


def _claim_sort_key(claim: Mapping[str, Any]) -> tuple[str, str]:
    return (_text(claim.get("claim_text") or claim.get("proposition_text")), _fingerprint(claim))


def _claim_refs(claims: Sequence[Mapping[str, Any]], prefix: str) -> list[tuple[str, Mapping[str, Any]]]:
    ordered = sorted((dict(claim) for claim in claims), key=_claim_sort_key)
    # Public references intentionally reveal neither text nor a content hash.
    return [
        (f"{prefix}:{index:04d}", claim)
        for index, claim in enumerate(ordered, start=1)
    ]


def _token_f1(left: Any, right: Any) -> tuple[float, float, float]:
    left_counts = Counter(_tokens(left))
    right_counts = Counter(_tokens(right))
    if not left_counts and not right_counts:
        return (1.0, 1.0, 1.0)
    if not left_counts or not right_counts:
        return (0.0, 0.0, 0.0)
    overlap = sum((left_counts & right_counts).values())
    precision = overlap / sum(left_counts.values())
    recall = overlap / sum(right_counts.values())
    f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
    return (precision, recall, f1)


def lexical_faithfulness_proxy(predicted_text: Any, gold_text: Any) -> dict[str, float]:
    """Return lexical overlap measurements, not semantic equivalence."""

    precision, recall, token_f1 = _token_f1(predicted_text, gold_text)
    sequence_ratio = SequenceMatcher(
        None, " ".join(_tokens(predicted_text)), " ".join(_tokens(gold_text)), autojunk=False
    ).ratio()
    return {
        "score": round((0.75 * token_f1) + (0.25 * sequence_ratio), 6),
        "token_precision": round(precision, 6),
        "token_recall": round(recall, 6),
        "token_f1": round(token_f1, 6),
        "sequence_ratio": round(sequence_ratio, 6),
    }


def _exact(left: Any, right: Any) -> bool:
    return _text(left) == _text(right)


_HONORIFICS = re.compile(r"^(?:dr|mr|mrs|ms|prof)\.?\s+", re.IGNORECASE)


def _participant_key(value: Any) -> str:
    return " ".join(_HONORIFICS.sub("", _text(value)).split())


def _participant_entries(
    speaker_map: Mapping[str, Any] | Sequence[Any] | None,
) -> list[tuple[str, set[str]]]:
    if not speaker_map:
        return []
    rows: list[tuple[str | None, Any]]
    if isinstance(speaker_map, Mapping):
        rows = [(str(identifier), entry) for identifier, entry in speaker_map.items()]
    elif isinstance(speaker_map, Sequence) and not isinstance(
        speaker_map, (str, bytes)
    ):
        rows = [(None, entry) for entry in speaker_map]
    else:
        return []
    entries: list[tuple[str, set[str]]] = []
    for identifier, entry in rows:
        aliases: set[str] = set()
        display = ""
        if isinstance(entry, str):
            display = entry
        elif isinstance(entry, Mapping):
            display = str(
                entry.get("display")
                or entry.get("name")
                or entry.get("speaker")
                or ""
            )
            raw_aliases = entry.get("aliases", [])
            if isinstance(raw_aliases, Sequence) and not isinstance(
                raw_aliases, (str, bytes)
            ):
                aliases.update(_participant_key(value) for value in raw_aliases)
        canonical = identifier or _participant_key(display)
        aliases.update(
            value
            for value in (
                _participant_key(identifier),
                _participant_key(display),
            )
            if value
        )
        parts = _participant_key(display).split()
        if len(parts) >= 2:
            aliases.update((parts[0], parts[-1]))
        if canonical and aliases:
            entries.append((canonical, aliases))
    return entries


def canonicalize_participant(
    value: Any,
    speaker_map: Mapping[str, Any] | Sequence[Any] | None,
) -> str:
    """Resolve a participant alias through the frozen roster when unambiguous."""

    key = _participant_key(value)
    if not key:
        return ""
    matches = {
        canonical
        for canonical, aliases in _participant_entries(speaker_map)
        if key in aliases
    }
    return next(iter(matches)) if len(matches) == 1 else key


_ENUM_SYNONYMS: dict[str, dict[str, set[str]]] = {
    # Canonical values come from ai_discourse_v3_1, the source contract used
    # to materialize the frozen candidates.
    "certainty": {
        "low": {"low", "low confidence"},
        "medium": {
            "medium",
            "moderate",
            "likely",
            "probable",
            "probably",
        },
        "high": {"high", "certain", "high confidence", "confident"},
        "hedged": {
            "hedged",
            "uncertain",
            "speculative",
            "conditional",
            "qualified",
        },
    },
    "stance": {
        "supportive": {
            "supportive",
            "supports",
            "support",
            "endorses",
            "positive",
        },
        "skeptical": {
            "skeptical",
            "opposes",
            "opposed",
            "critical",
            "negative",
        },
        "neutral": {"neutral", "descriptive", "reporting", "reports"},
        "mixed": {"mixed", "qualified"},
        "warning": {"warning", "warns", "caution", "cautious"},
        "competitive": {"competitive"},
        "promotional": {"promotional"},
        "uncertain": {"uncertain"},
        "not_applicable": {"not applicable", "not_applicable", "n/a"},
    },
    "time_horizon": {
        "past": {"past", "historical", "formerly", "previously"},
        "present": {"present", "current", "currently", "ongoing", "recent"},
        "near_future": {
            "near future",
            "near_future",
            "very near future",
            "very_near_future",
            "next year",
            "next_year",
        },
        "long_future": {"long future", "long_future"},
        "timeless": {"timeless", "general", "historical generalization"},
        "unspecified": {"unspecified", "unknown", "not applicable", "n/a"},
    },
    # Polarity is part of the downstream atomic contract rather than v3.1.
    # The frozen gold uses positive/affirmative, negative, and neutral.
    "polarity": {
        "positive": {"positive", "affirmative", "affirms", "asserts"},
        "negative": {"negative", "denies", "denial"},
        "neutral": {"neutral"},
    },
}


def normalize_enum_field(field: str, value: Any) -> str:
    """Normalize known surface synonyms without hiding unknown disagreements."""

    normalized = _text(value).replace("-", "_")
    normalized = " ".join(normalized.split())
    table = _ENUM_SYNONYMS.get(field)
    if not table:
        return normalized
    comparison = normalized.replace("_", " ")
    for canonical, synonyms in table.items():
        if comparison in {item.replace("_", " ") for item in synonyms}:
            return canonical
    return normalized


def _field_value(
    field: str,
    value: Any,
    speaker_map: Mapping[str, Any] | Sequence[Any] | None,
) -> Any:
    if field in {"raw_speaker", "reported_actor"}:
        return canonicalize_participant(value, speaker_map)
    if field in _ENUM_SYNONYMS:
        return normalize_enum_field(field, value)
    return _text(value)


def _field_equal(
    field: str,
    left: Any,
    right: Any,
    speaker_map: Mapping[str, Any] | Sequence[Any] | None,
) -> bool:
    return _field_value(field, left, speaker_map) == _field_value(
        field, right, speaker_map
    )


def _qualifiers(value: Any) -> set[str]:
    normalized = _text(value)
    return {name for name, pattern in _QUALIFIER_PATTERNS if re.search(pattern, normalized)}


def _qualifier_proxy(predicted: Mapping[str, Any], gold: Mapping[str, Any]) -> dict[str, Any]:
    predicted_markers = _qualifiers(
        f"{predicted.get('claim_text', '')} {predicted.get('proposition_text', '')}"
    )
    gold_markers = _qualifiers(f"{gold.get('claim_text', '')} {gold.get('proposition_text', '')}")
    overlap = len(predicted_markers & gold_markers)
    precision = (
        1.0 if not predicted_markers and not gold_markers
        else (overlap / len(predicted_markers) if predicted_markers else 0.0)
    )
    recall = (
        1.0 if not predicted_markers and not gold_markers
        else (overlap / len(gold_markers) if gold_markers else 0.0)
    )
    f1 = (
        0.0
        if precision + recall == 0
        else 2 * precision * recall / (precision + recall)
    )
    additions = sorted(predicted_markers - gold_markers)
    missing = sorted(gold_markers - predicted_markers)
    return {
        "score": round(f1, 6),
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
        "gold_markers": sorted(gold_markers),
        "predicted_markers": sorted(predicted_markers),
        "missing_markers": missing,
        "added_markers": additions,
    }


def _pair_score(
    predicted: Mapping[str, Any],
    gold: Mapping[str, Any],
    *,
    speaker_map: Mapping[str, Any] | Sequence[Any] | None = None,
) -> int:
    """Integer matching weight; integer arithmetic makes ties reproducible."""

    lexical = lexical_faithfulness_proxy(
        predicted.get("claim_text") or predicted.get("proposition_text"),
        gold.get("claim_text") or gold.get("proposition_text"),
    )["score"]
    field_score = sum(
        weight
        for field, weight in (
            ("subject_text", 0.08),
            ("proposition_text", 0.05),
            ("raw_speaker", 0.04),
            ("reported_actor", 0.03),
            ("time_horizon", 0.02),
            ("certainty", 0.02),
            ("stance", 0.02),
            ("polarity", 0.02),
        )
        if _field_equal(
            field,
            predicted.get(field),
            gold.get(field),
            speaker_map,
        )
    )
    return int(round(((0.72 * lexical) + field_score) * 1_000_000))


def _maximum_assignment(weights: Sequence[Sequence[int]]) -> list[tuple[int, int]]:
    """Hungarian maximum assignment over a rectangular integer matrix."""

    rows = len(weights)
    columns = len(weights[0]) if rows else 0
    if rows == 0 or columns == 0:
        return []
    size = max(rows, columns)
    maximum = max(max(row) for row in weights)
    costs = [
        [
            maximum - (weights[i][j] if i < rows and j < columns else 0)
            for j in range(size)
        ]
        for i in range(size)
    ]

    # Standard one-indexed Hungarian minimization.  Iteration order is fixed, so
    # equal-cost solutions are deterministic after stable claim sorting.
    u = [0] * (size + 1)
    v = [0] * (size + 1)
    p = [0] * (size + 1)
    way = [0] * (size + 1)
    for i in range(1, size + 1):
        p[0] = i
        j0 = 0
        minimum = [10**30] * (size + 1)
        used = [False] * (size + 1)
        while True:
            used[j0] = True
            i0 = p[j0]
            delta = 10**30
            j1 = 0
            for j in range(1, size + 1):
                if used[j]:
                    continue
                current = costs[i0 - 1][j - 1] - u[i0] - v[j]
                if current < minimum[j]:
                    minimum[j] = current
                    way[j] = j0
                if minimum[j] < delta:
                    delta = minimum[j]
                    j1 = j
            for j in range(size + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minimum[j] -= delta
            j0 = j1
            if p[j0] == 0:
                break
        while True:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1
            if j0 == 0:
                break
    result = [
        (p[j] - 1, j - 1)
        for j in range(1, size + 1)
        if p[j] and p[j] - 1 < rows and j - 1 < columns
    ]
    return sorted(result)


def _mean(values: Iterable[float]) -> float | None:
    materialized = list(values)
    if not materialized:
        return None
    return round(sum(materialized) / len(materialized), 6)


def _field_metric(
    pairs: Sequence[dict[str, Any]],
    field: str,
    *,
    predicted_atom_count: int,
    gold_atom_count: int,
    reference_available: bool,
) -> dict[str, Any]:
    if not reference_available:
        return {
            "reference_available": False,
            "matched_pairs": len(pairs),
            "predicted_atom_count": predicted_atom_count,
            "gold_atom_count": gold_atom_count,
            "micro_denominator": None,
            "correct_pairs": 0,
            "precision": None,
            "recall": None,
            "f1": None,
            "score": None,
            "coupled_diagnostic": {
                "micro_denominator": None,
                "score": None,
            },
        }
    correct = sum(1 for pair in pairs if pair["field_exactness"][field])
    denominator = len(pairs)
    coupled_denominator = max(predicted_atom_count, gold_atom_count)
    precision = correct / predicted_atom_count if predicted_atom_count else None
    recall = correct / gold_atom_count if gold_atom_count else None
    f1 = (
        None
        if precision is None or recall is None
        else (
            0.0
            if precision + recall == 0
            else 2 * precision * recall / (precision + recall)
        )
    )
    return {
        "reference_available": True,
        "matched_pairs": len(pairs),
        "predicted_atom_count": predicted_atom_count,
        "gold_atom_count": gold_atom_count,
        "micro_denominator": denominator,
        "correct_pairs": correct,
        "precision": None if precision is None else round(precision, 6),
        "recall": None if recall is None else round(recall, 6),
        "f1": None if f1 is None else round(f1, 6),
        "score": None if denominator == 0 else round(correct / denominator, 6),
        "coupled_diagnostic": {
            "micro_denominator": coupled_denominator,
            "score": (
                None
                if coupled_denominator == 0
                else round(correct / coupled_denominator, 6)
            ),
        },
    }


def _value_state(disposition: Any) -> str:
    normalized = _text(disposition)
    if normalized in {"retain", "revise"}:
        return "value"
    if normalized == "hold":
        return "hold"
    if normalized == "reject":
        return "junk"
    return "unknown"


def _claims(value: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not value:
        return []
    claims = value.get("atomic_claims", [])
    return [dict(claim) for claim in claims if isinstance(claim, Mapping)]


def _consensus_decompositions(consensus: Mapping[str, Any]) -> list[list[dict[str, Any]]]:
    decompositions: list[list[dict[str, Any]]] = []
    for decomposition in consensus.get("decompositions", []):
        if not isinstance(decomposition, Mapping):
            continue
        texts = decomposition.get("claim_texts", [])
        if isinstance(texts, Sequence) and not isinstance(texts, (str, bytes)):
            decompositions.append([{"claim_text": str(text)} for text in texts])
    return decompositions


def _align(
    predicted_claims: Sequence[Mapping[str, Any]],
    gold_claims: Sequence[Mapping[str, Any]],
    *,
    check_reference_fields: bool = True,
    speaker_map: Mapping[str, Any] | Sequence[Any] | None = None,
) -> dict[str, Any]:
    predicted = _claim_refs(predicted_claims, "pred")
    gold = _claim_refs(gold_claims, "gold")
    weights = [
        [
            _pair_score(pred_claim, gold_claim, speaker_map=speaker_map)
            for _, gold_claim in gold
        ]
        for _, pred_claim in predicted
    ]
    assignment = _maximum_assignment(weights)
    matched_predicted: set[int] = set()
    matched_gold: set[int] = set()
    pairs: list[dict[str, Any]] = []
    unsupported_flags: list[dict[str, str]] = []

    for pred_index, gold_index in assignment:
        pred_ref, pred_claim = predicted[pred_index]
        gold_ref, gold_claim = gold[gold_index]
        matched_predicted.add(pred_index)
        matched_gold.add(gold_index)
        lexical = lexical_faithfulness_proxy(
            pred_claim.get("claim_text") or pred_claim.get("proposition_text"),
            gold_claim.get("claim_text") or gold_claim.get("proposition_text"),
        )
        qualifier = _qualifier_proxy(pred_claim, gold_claim)
        exactness = {
            field: _field_equal(
                field,
                pred_claim.get(field),
                gold_claim.get(field),
                speaker_map,
            )
            for field in _FIELD_NAMES
        }
        pair_flags: list[dict[str, str]] = []
        for field in _UNSUPPORTED_CHECK_FIELDS:
            predicted_value = _field_value(
                field, pred_claim.get(field), speaker_map
            )
            gold_value = _field_value(field, gold_claim.get(field), speaker_map)
            if check_reference_fields and predicted_value and not gold_value:
                pair_flags.append(
                    {
                        "predicted_ref": pred_ref,
                        "field": field,
                        "kind": "predicted_field_absent_from_reference",
                        "severity": (
                            "hallucination"
                            if field in {"raw_speaker", "reported_actor"}
                            else "divergence"
                        ),
                    }
                )
            elif (
                check_reference_fields
                and predicted_value
                and gold_value
                and predicted_value != gold_value
            ):
                pair_flags.append(
                    {
                        "predicted_ref": pred_ref,
                        "field": field,
                        "kind": "predicted_field_reference_mismatch_proxy",
                        "severity": "divergence",
                    }
                )
        if lexical["token_precision"] < 0.5:
            pair_flags.append(
                {
                    "predicted_ref": pred_ref,
                    "field": "claim_text",
                    "kind": "low_reference_token_precision_proxy",
                    "severity": (
                        "hallucination"
                        if lexical["token_precision"] < 0.3
                        else "divergence"
                    ),
                }
            )
        if check_reference_fields and (
            _text(gold_claim.get("evidence_text"))
            and _text(pred_claim.get("evidence_text"))
            and not _exact(pred_claim.get("evidence_text"), gold_claim.get("evidence_text"))
        ):
            pair_flags.append(
                {
                    "predicted_ref": pred_ref,
                    "field": "evidence_text",
                    "kind": "reference_evidence_mismatch",
                    "severity": "hallucination",
                }
            )
        unsupported_flags.extend(pair_flags)
        pairs.append(
            {
                "predicted_ref": pred_ref,
                "gold_ref": gold_ref,
                "alignment_weight": weights[pred_index][gold_index],
                "claim_text_faithfulness_proxy": lexical,
                "qualifier_preservation_proxy": qualifier,
                "field_exactness": exactness,
                "unsupported_field_flags": pair_flags,
            }
        )

    unmatched_predicted = [predicted[i][0] for i in range(len(predicted)) if i not in matched_predicted]
    unmatched_gold = [gold[i][0] for i in range(len(gold)) if i not in matched_gold]
    unsupported_flags.extend(
        {
            "predicted_ref": reference,
            "field": "atomic_claim",
            "kind": "unmatched_predicted_claim",
            "severity": "hallucination",
        }
        for reference in unmatched_predicted
    )
    pairs.sort(key=lambda pair: (pair["predicted_ref"], pair["gold_ref"]))
    unsupported_flags.sort(
        key=lambda flag: (flag["predicted_ref"], flag["field"], flag["kind"])
    )
    denominator = max(len(predicted), len(gold))
    faithfulness_numerator = sum(
        pair["claim_text_faithfulness_proxy"]["score"] for pair in pairs
    )
    matched_denominator = len(pairs)
    faithfulness = (
        None
        if matched_denominator == 0
        else round(faithfulness_numerator / matched_denominator, 6)
    )
    coupled_faithfulness = (
        1.0
        if denominator == 0
        else round(faithfulness_numerator / denominator, 6)
    )
    qualifier = (
        1.0
        if denominator == 0
        else round(
            sum(pair["qualifier_preservation_proxy"]["score"] for pair in pairs)
            / denominator,
            6,
        )
    )
    reported_actor_tp = sum(
        1
        for pred_index, gold_index in assignment
        if _text(predicted[pred_index][1].get("reported_actor"))
        and _field_equal(
            "reported_actor",
            predicted[pred_index][1].get("reported_actor"),
            gold[gold_index][1].get("reported_actor"),
            speaker_map,
        )
    )
    reported_actor_predicted_positive = sum(
        bool(_text(claim.get("reported_actor"))) for _, claim in predicted
    )
    reported_actor_gold_positive = sum(
        bool(_text(claim.get("reported_actor"))) for _, claim in gold
    )
    reported_actor_precision = (
        None
        if reported_actor_predicted_positive == 0
        else round(reported_actor_tp / reported_actor_predicted_positive, 6)
    )
    reported_actor_recall = (
        None
        if reported_actor_gold_positive == 0
        else round(reported_actor_tp / reported_actor_gold_positive, 6)
    )
    return {
        "pairs": pairs,
        "unmatched_predicted": unmatched_predicted,
        "unmatched_gold": unmatched_gold,
        "unsupported_field_flags": unsupported_flags,
        "predicted_atom_count": len(predicted),
        "gold_atom_count": len(gold),
        "micro_denominator": matched_denominator,
        "claim_text_faithfulness_proxy": faithfulness,
        "claim_text_faithfulness_coupled_diagnostic": {
            "score": coupled_faithfulness,
            "micro_denominator": denominator,
        },
        "qualifier_preservation_proxy": qualifier,
        "reported_actor_positive": {
            "true_positive": reported_actor_tp,
            "predicted_positive": reported_actor_predicted_positive,
            "gold_positive": reported_actor_gold_positive,
            "false_positive": reported_actor_predicted_positive - reported_actor_tp,
            "false_negative": reported_actor_gold_positive - reported_actor_tp,
            "precision": reported_actor_precision,
            "recall": reported_actor_recall,
        },
    }


def _best_consensus_text_alignment(
    predicted_claims: Sequence[Mapping[str, Any]], consensus: Mapping[str, Any]
) -> dict[str, Any]:
    alternatives = _consensus_decompositions(consensus)
    if not alternatives:
        return _align(predicted_claims, [], check_reference_fields=False)
    scored: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    for alternative in alternatives:
        alignment = _align(
            predicted_claims, alternative, check_reference_fields=False
        )
        score = alignment["claim_text_faithfulness_proxy"]
        tie_key = (
            -(score if score is not None else -1.0),
            len(alignment["unmatched_predicted"]) + len(alignment["unmatched_gold"]),
            json.dumps(alternative, sort_keys=True, separators=(",", ":")),
        )
        scored.append((tie_key, alignment))
    return min(scored, key=lambda item: item[0])[1]


def score_candidate(
    prediction: Mapping[str, Any],
    consensus: Mapping[str, Any],
    preferred_gold: Mapping[str, Any] | None = None,
    *,
    speaker_map: Mapping[str, Any] | Sequence[Any] | None = None,
) -> dict[str, Any]:
    """Score one candidate without comparing it to any other candidate."""

    candidate_id = str(consensus.get("candidate_id") or prediction.get("candidate_id") or "")
    if str(prediction.get("candidate_id") or candidate_id) != candidate_id:
        raise ValueError("prediction candidate_id does not match consensus candidate_id")
    if preferred_gold and str(preferred_gold.get("candidate_id") or candidate_id) != candidate_id:
        raise ValueError("preferred gold candidate_id does not match consensus candidate_id")

    predicted_claims = _claims(prediction)
    preferred_claims = _claims(preferred_gold)
    field_reference_available = preferred_gold is not None
    reference_claims = preferred_claims
    if preferred_gold is None:
        decompositions = _consensus_decompositions(consensus)
        reference_claims = decompositions[0] if decompositions else []

    enriched_alignment = _align(
        predicted_claims,
        reference_claims,
        check_reference_fields=field_reference_available,
        speaker_map=speaker_map,
    )
    consensus_text_alignment = _best_consensus_text_alignment(predicted_claims, consensus)
    pairs = enriched_alignment["pairs"]
    predicted_disposition = _text(prediction.get("disposition"))
    predicted_value_state = _text(prediction.get("value_state")) or _value_state(
        predicted_disposition
    )
    acceptable_dispositions = sorted(
        {_text(value) for value in consensus.get("acceptable_dispositions", [])}
    )
    acceptable_value_states = sorted(
        {_text(value) for value in consensus.get("acceptable_value_states", [])}
    )
    acceptable_counts = sorted(
        {int(value) for value in consensus.get("acceptable_atomic_counts", [])}
    )
    minimum_count = int(
        consensus.get(
            "minimum_atomic_count",
            min(acceptable_counts) if acceptable_counts else 0,
        )
    )
    maximum_count = int(
        consensus.get(
            "maximum_atomic_count",
            max(acceptable_counts) if acceptable_counts else minimum_count,
        )
    )
    interval_counts = list(range(minimum_count, maximum_count + 1))
    acceptable_count_set = set(acceptable_counts) | set(interval_counts)
    count = len(predicted_claims)
    field_metrics = {
        field: _field_metric(
            pairs,
            field,
            predicted_atom_count=len(predicted_claims),
            gold_atom_count=len(reference_claims),
            reference_available=field_reference_available,
        )
        for field in _FIELD_NAMES
    }
    preferred_disposition = _text(
        consensus.get("preferred_disposition")
        or (acceptable_dispositions[0] if len(acceptable_dispositions) == 1 else "")
    )
    preferred_value_state = _text(
        consensus.get("preferred_value_state")
        or (
            acceptable_value_states[0]
            if len(acceptable_value_states) == 1
            else _value_state(preferred_disposition)
        )
    )

    result = {
        "schema_version": SCHEMA_VERSION,
        "metric_kind": METRIC_KIND,
        "candidate_id": candidate_id,
        "consensus_state": _text(consensus.get("consensus_state")) or "unspecified",
        "strictly_scoreable": bool(consensus.get("strictly_scoreable", True)),
        "disposition": {
            "predicted": predicted_disposition,
            "acceptable": acceptable_dispositions,
            "exact_acceptable": predicted_disposition in acceptable_dispositions,
            "preferred_reference": preferred_disposition or None,
        },
        "value_state": {
            "predicted": predicted_value_state,
            "acceptable": acceptable_value_states,
            "exact_acceptable": predicted_value_state in acceptable_value_states,
            "preferred_reference": preferred_value_state or None,
        },
        "atomic_count": {
            "predicted": count,
            "acceptable": acceptable_counts,
            "minimum": minimum_count,
            "maximum": maximum_count,
            "interval_semantics": True,
            "acceptable_count": count in acceptable_count_set,
        },
        "reference_kind": (
            "preferred_adjudicated_gold" if preferred_gold is not None else "consensus_text_only"
        ),
        "claim_text_faithfulness_proxy": {
            "score": consensus_text_alignment["claim_text_faithfulness_proxy"],
            "predicted_atom_count": consensus_text_alignment["predicted_atom_count"],
            "gold_atom_count": consensus_text_alignment["gold_atom_count"],
            "micro_denominator": consensus_text_alignment["micro_denominator"],
            "reference": "best_frozen_consensus_decomposition_by_lexical_alignment",
            "semantic_equivalence_claimed": False,
            "coupled_diagnostic": consensus_text_alignment[
                "claim_text_faithfulness_coupled_diagnostic"
            ],
        },
        "field_reference_available": field_reference_available,
        "required_field_exactness": field_metrics,
        "speaker_exactness": field_metrics["raw_speaker"],
        "reported_actor_exactness": field_metrics["reported_actor"],
        "reported_actor_positive": enriched_alignment["reported_actor_positive"],
        "qualifier_preservation_proxy": {
            "score": enriched_alignment["qualifier_preservation_proxy"],
            "micro_denominator": enriched_alignment["micro_denominator"],
            "semantic_equivalence_claimed": False,
        },
        "time_horizon_exactness": field_metrics["time_horizon"],
        "certainty_exactness": field_metrics["certainty"],
        "stance_exactness": field_metrics["stance"],
        "position_exactness": field_metrics["position"],
        "alignment": pairs,
        "unmatched_predicted": enriched_alignment["unmatched_predicted"],
        "unmatched_gold": enriched_alignment["unmatched_gold"],
        "unsupported_field_flags": enriched_alignment["unsupported_field_flags"],
        "hallucination_or_unsupported_proxy": {
            "flagged": any(
                flag["severity"] == "hallucination"
                for flag in enriched_alignment["unsupported_field_flags"]
            ),
            "flag_count": sum(
                flag["severity"] == "hallucination"
                for flag in enriched_alignment["unsupported_field_flags"]
            ),
            "semantic_hallucination_claimed": False,
        },
        "field_divergence_proxy": {
            "flagged": any(
                flag["severity"] == "divergence"
                for flag in enriched_alignment["unsupported_field_flags"]
            ),
            "flag_count": sum(
                flag["severity"] == "divergence"
                for flag in enriched_alignment["unsupported_field_flags"]
            ),
            "semantic_disagreement_claimed": False,
        },
    }
    return result


def _index_items(
    items: Sequence[Mapping[str, Any]] | Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    if isinstance(items, Mapping):
        return {str(key): value for key, value in items.items()}
    result: dict[str, Mapping[str, Any]] = {}
    for item in items:
        candidate_id = str(item.get("candidate_id") or "")
        if not candidate_id:
            raise ValueError("every campaign item requires candidate_id")
        if candidate_id in result:
            raise ValueError(f"duplicate candidate_id: {candidate_id}")
        result[candidate_id] = item
    return result


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else round(numerator / denominator, 6)


def _classification_metrics(
    candidate_scores: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    labels = ("retain", "revise", "hold", "reject")
    by_class: dict[str, dict[str, Any]] = {}
    f1_values: list[float] = []
    for label in labels:
        true_positive = sum(
            score["disposition"]["predicted"] == label
            and score["disposition"]["preferred_reference"] == label
            for score in candidate_scores
        )
        false_positive = sum(
            score["disposition"]["predicted"] == label
            and score["disposition"]["preferred_reference"] != label
            for score in candidate_scores
        )
        false_negative = sum(
            score["disposition"]["predicted"] != label
            and score["disposition"]["preferred_reference"] == label
            for score in candidate_scores
        )
        precision = _safe_ratio(true_positive, true_positive + false_positive)
        recall = _safe_ratio(true_positive, true_positive + false_negative)
        support = true_positive + false_positive + false_negative
        if support == 0:
            f1 = None
        elif true_positive == 0:
            f1 = 0.0
        else:
            assert precision is not None and recall is not None
            f1 = round(2 * precision * recall / (precision + recall), 6)
        if f1 is not None:
            f1_values.append(f1)
        by_class[label] = {
            "true_positive": true_positive,
            "false_positive": false_positive,
            "false_negative": false_negative,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        }
    return {"macro_f1": _mean(f1_values), "by_class": by_class}


def _aggregate_field(
    candidate_scores: Sequence[Mapping[str, Any]], field: str
) -> dict[str, Any]:
    metrics = [
        score["required_field_exactness"][field]
        for score in candidate_scores
        if score["required_field_exactness"][field]["reference_available"]
    ]
    if not metrics:
        return {
            "reference_available": False,
            "matched_pairs": 0,
            "predicted_atom_count": 0,
            "gold_atom_count": 0,
            "micro_denominator": None,
            "correct_pairs": 0,
            "precision": None,
            "recall": None,
            "f1": None,
            "score": None,
            "coupled_diagnostic": {
                "micro_denominator": None,
                "score": None,
            },
        }
    correct = sum(metric["correct_pairs"] for metric in metrics)
    predicted = sum(metric["predicted_atom_count"] for metric in metrics)
    gold = sum(metric["gold_atom_count"] for metric in metrics)
    denominator = sum(metric["micro_denominator"] for metric in metrics)
    coupled_denominator = sum(
        metric["coupled_diagnostic"]["micro_denominator"]
        for metric in metrics
    )
    precision = _safe_ratio(correct, predicted)
    recall = _safe_ratio(correct, gold)
    f1 = (
        None
        if precision is None or recall is None
        else (
            0.0
            if precision + recall == 0
            else round(2 * precision * recall / (precision + recall), 6)
        )
    )
    return {
        "reference_available": True,
        "matched_pairs": sum(metric["matched_pairs"] for metric in metrics),
        "predicted_atom_count": predicted,
        "gold_atom_count": gold,
        "micro_denominator": denominator,
        "correct_pairs": correct,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "score": None if denominator == 0 else round(correct / denominator, 6),
        "coupled_diagnostic": {
            "micro_denominator": coupled_denominator,
            "score": (
                None
                if coupled_denominator == 0
                else round(correct / coupled_denominator, 6)
            ),
        },
    }


def score_campaign(
    predictions: Sequence[Mapping[str, Any]] | Mapping[str, Mapping[str, Any]],
    consensus_items: Sequence[Mapping[str, Any]] | Mapping[str, Mapping[str, Any]],
    preferred_gold_items: (
        Sequence[Mapping[str, Any]] | Mapping[str, Mapping[str, Any]] | None
    ) = None,
    *,
    subset_candidate_ids: Iterable[str] | None = None,
    speaker_maps_by_candidate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Score a complete campaign, or an explicit candidate-ID subset.

    Silent partial scoring is forbidden.  Callers running a bounded diagnostic
    subset must name that subset with ``subset_candidate_ids``.
    """

    predicted_by_id = _index_items(predictions)
    consensus_by_id = _index_items(consensus_items)
    gold_supplied = preferred_gold_items is not None
    gold_by_id = _index_items(preferred_gold_items or {})
    unknown = sorted(set(predicted_by_id) - set(consensus_by_id))
    if unknown:
        raise ValueError(f"predictions lack consensus contract: {unknown}")
    if subset_candidate_ids is None:
        target_ids = set(consensus_by_id)
        if set(predicted_by_id) != target_ids:
            missing = sorted(target_ids - set(predicted_by_id))
            raise ValueError(
                "incomplete campaign predictions; pass subset_candidate_ids for an "
                f"intentional subset (missing={missing})"
            )
    else:
        requested = {str(candidate_id) for candidate_id in subset_candidate_ids}
        if not requested:
            raise ValueError("subset_candidate_ids must not be empty")
        outside = sorted(requested - set(consensus_by_id))
        if outside:
            raise ValueError(f"subset lacks consensus contract: {outside}")
        if set(predicted_by_id) != requested:
            raise ValueError(
                "prediction IDs must exactly equal subset_candidate_ids"
            )
        target_ids = requested
    if gold_supplied:
        missing_gold = sorted(target_ids - set(gold_by_id))
        if missing_gold:
            raise ValueError(f"preferred gold is incomplete for campaign: {missing_gold}")

    candidate_scores = [
        score_candidate(
            predicted_by_id[candidate_id],
            consensus_by_id[candidate_id],
            gold_by_id.get(candidate_id),
            speaker_map=(
                speaker_maps_by_candidate.get(candidate_id)
                if speaker_maps_by_candidate
                else None
            ),
        )
        for candidate_id in sorted(target_ids)
    ]
    strict_scores = [score for score in candidate_scores if score["strictly_scoreable"]]

    def proportion(path: tuple[str, ...]) -> float | None:
        values: list[bool] = []
        for score in strict_scores:
            value: Any = score
            for key in path:
                value = value[key]
            values.append(bool(value))
        return _mean(float(value) for value in values)

    disposition_classification = _classification_metrics(
        [
            score
            for score in strict_scores
            if score["disposition"]["preferred_reference"] is not None
        ]
    )
    gold_value = [
        score
        for score in strict_scores
        if score["value_state"]["preferred_reference"] == "value"
    ]
    gold_junk = [
        score
        for score in strict_scores
        if score["value_state"]["preferred_reference"] == "junk"
    ]
    retained_value_count = sum(
        score["value_state"]["predicted"] == "value" for score in gold_value
    )
    junk_escape_count = sum(
        score["value_state"]["predicted"] == "value" for score in gold_junk
    )
    required_fields = {
        field: _aggregate_field(strict_scores, field) for field in _FIELD_NAMES
    }
    actor_tp = sum(
        score["reported_actor_positive"]["true_positive"] for score in strict_scores
    )
    actor_predicted = sum(
        score["reported_actor_positive"]["predicted_positive"]
        for score in strict_scores
    )
    actor_gold = sum(
        score["reported_actor_positive"]["gold_positive"] for score in strict_scores
    )
    text_numerator = sum(
        (score["claim_text_faithfulness_proxy"]["score"] or 0.0)
        * score["claim_text_faithfulness_proxy"]["micro_denominator"]
        for score in strict_scores
    )
    text_denominator = sum(
        score["claim_text_faithfulness_proxy"]["micro_denominator"]
        for score in strict_scores
    )
    coupled_text_numerator = sum(
        (
            score["claim_text_faithfulness_proxy"]["coupled_diagnostic"][
                "score"
            ]
            or 0.0
        )
        * score["claim_text_faithfulness_proxy"]["coupled_diagnostic"][
            "micro_denominator"
        ]
        for score in strict_scores
    )
    coupled_text_denominator = sum(
        score["claim_text_faithfulness_proxy"]["coupled_diagnostic"][
            "micro_denominator"
        ]
        for score in strict_scores
    )
    qualifier_numerator = sum(
        (score["qualifier_preservation_proxy"]["score"] or 0.0)
        * score["qualifier_preservation_proxy"]["micro_denominator"]
        for score in strict_scores
    )
    qualifier_denominator = sum(
        score["qualifier_preservation_proxy"]["micro_denominator"]
        for score in strict_scores
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "metric_kind": METRIC_KIND,
        "candidate_count": len(candidate_scores),
        "strictly_scoreable_candidate_count": len(strict_scores),
        "excluded_nonstrict_candidate_count": len(candidate_scores) - len(strict_scores),
        "subset_filtered": subset_candidate_ids is not None,
        "candidates": candidate_scores,
        "aggregate": {
            "disposition_accuracy": proportion(("disposition", "exact_acceptable")),
            "disposition_macro_f1": disposition_classification["macro_f1"],
            "disposition_by_class": disposition_classification["by_class"],
            "value_state_accuracy": proportion(("value_state", "exact_acceptable")),
            "retained_value_recall": _safe_ratio(
                retained_value_count, len(gold_value)
            ),
            "retained_value_true_positive": retained_value_count,
            "retained_value_gold_positive": len(gold_value),
            "junk_escape_rate": _safe_ratio(junk_escape_count, len(gold_junk)),
            "junk_escape_count": junk_escape_count,
            "junk_gold_count": len(gold_junk),
            "atomic_count_acceptability": proportion(("atomic_count", "acceptable_count")),
            "claim_text_faithfulness_proxy": (
                None
                if text_denominator == 0
                else round(text_numerator / text_denominator, 6)
            ),
            "claim_text_micro_denominator": text_denominator,
            "required_field_exactness": required_fields,
            "speaker_exactness": required_fields["raw_speaker"]["score"],
            "reported_actor_exactness": required_fields["reported_actor"]["score"],
            "coupled_diagnostics": {
                "claim_text_faithfulness_proxy": (
                    None
                    if coupled_text_denominator == 0
                    else round(
                        coupled_text_numerator / coupled_text_denominator,
                        6,
                    )
                ),
                "claim_text_micro_denominator": coupled_text_denominator,
                "speaker_exactness": required_fields["raw_speaker"][
                    "coupled_diagnostic"
                ]["score"],
                "reported_actor_exactness": required_fields[
                    "reported_actor"
                ]["coupled_diagnostic"]["score"],
            },
            "claim_text_faithfulness_proxy_coupled_diagnostic": (
                None
                if coupled_text_denominator == 0
                else round(
                    coupled_text_numerator / coupled_text_denominator,
                    6,
                )
            ),
            "speaker_exactness_coupled_diagnostic": required_fields[
                "raw_speaker"
            ]["coupled_diagnostic"]["score"],
            "reported_actor_exactness_coupled_diagnostic": required_fields[
                "reported_actor"
            ]["coupled_diagnostic"]["score"],
            "reported_actor_positive_precision": _safe_ratio(
                actor_tp, actor_predicted
            ),
            "reported_actor_positive_recall": _safe_ratio(actor_tp, actor_gold),
            "reported_actor_positive_true_positive": actor_tp,
            "reported_actor_positive_predicted": actor_predicted,
            "reported_actor_positive_gold": actor_gold,
            "qualifier_preservation_proxy": (
                1.0
                if qualifier_denominator == 0
                else round(qualifier_numerator / qualifier_denominator, 6)
            ),
            "qualifier_micro_denominator": qualifier_denominator,
            "hallucination_rate_proxy": _mean(
                float(
                    any(
                        flag["severity"] == "hallucination"
                        for pair in score["alignment"]
                        for flag in pair["unsupported_field_flags"]
                    )
                )
                for score in strict_scores
            ),
            "hallucination_rate_proxy_coupled_diagnostic": _mean(
                float(score["hallucination_or_unsupported_proxy"]["flagged"])
                for score in strict_scores
            ),
            # Compatibility name retained as the hard-gate alias for one
            # release. It now reflects only hallucination-severity flags.
            "unsupported_candidate_rate_proxy": _mean(
                float(
                    any(
                        flag["severity"] == "hallucination"
                        for pair in score["alignment"]
                        for flag in pair["unsupported_field_flags"]
                    )
                )
                for score in strict_scores
            ),
            "unsupported_candidate_rate_proxy_coupled_diagnostic": _mean(
                float(score["hallucination_or_unsupported_proxy"]["flagged"])
                for score in strict_scores
            ),
            "field_divergence_rate": _mean(
                float(score["field_divergence_proxy"]["flagged"])
                for score in strict_scores
            ),
            "unsupported_candidate_rate_proxy_legacy": _mean(
                float(bool(score["unsupported_field_flags"]))
                for score in strict_scores
            ),
            "unmatched_predicted_count": sum(
                len(score["unmatched_predicted"]) for score in strict_scores
            ),
            "unmatched_gold_count": sum(
                len(score["unmatched_gold"]) for score in strict_scores
            ),
        },
    }


__all__ = [
    "METRIC_KIND",
    "SCHEMA_VERSION",
    "canonicalize_participant",
    "lexical_faithfulness_proxy",
    "normalize_enum_field",
    "score_campaign",
    "score_candidate",
]
