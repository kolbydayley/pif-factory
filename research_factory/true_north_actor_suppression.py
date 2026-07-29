"""Zero-call actor-emission suppression frontier for Phase D."""

from __future__ import annotations

import copy
import re
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from .true_north_decoupled_rescore import (
    _speaker_maps,
    score_predictions,
)
from .true_north_semantic_scoring import score_campaign


SCHEMA_VERSION = "pif_true_north_actor_suppression_v1"
EXPERIMENT_ID = "phase-d-actor-suppression-frontier-20260729-v1"
SOURCE_RUN_ID = "task5-sol-input-split-default-20260729-v1"
SOURCE_RELATIVE = (
    "multipass/runs/"
    f"{SOURCE_RUN_ID}/outputs/composed-partial-actor-span/"
    "predictions.private.json"
)
ACTOR_GATE = 0.735385
HALLUCINATION_GATE = 0.093684
THRESHOLDS = tuple(range(-1, 8))

_CONNECTORS = frozenset({"of", "the", "and", "for", "in", "to"})
_GENERIC_ACTOR_TERMS = frozenset(
    {
        "administration",
        "agencies",
        "businesses",
        "ceos",
        "companies",
        "company",
        "developers",
        "frameworks",
        "government",
        "governments",
        "industry",
        "labs",
        "leaders",
        "models",
        "officials",
        "organizations",
        "people",
        "platforms",
        "public",
        "researcher",
        "researchers",
        "users",
        "vendors",
    }
)
_ARTIFACT_TERMS = frozenset(
    {
        "ai",
        "code",
        "fable",
        "flash",
        "framework",
        "frameworks",
        "jailbreak",
        "model",
        "models",
        "mythos",
        "paper",
        "product",
        "products",
        "spend",
        "system",
    }
)


class ActorSuppressionError(RuntimeError):
    """Raised when the frozen Phase-D scope or metrics drift."""


def _tokens(value: Any) -> list[str]:
    return re.findall(r"[a-z0-9]+", str(value or "").lower())


def actor_emission_risk(atomic: Mapping[str, Any]) -> int:
    """Return a packet-only risk score; gold is never an input."""
    actor = str(atomic.get("reported_actor") or "").strip()
    actor_tokens = _tokens(actor)
    if not actor_tokens:
        return -99
    claim_tokens = set(_tokens(atomic.get("claim_text")))
    coverage = (
        sum(token in claim_tokens for token in actor_tokens)
        / len(actor_tokens)
    )
    surface_tokens = re.findall(r"[A-Za-z0-9]+", actor)
    named_tokens = [
        token
        for token in surface_tokens
        if token.lower() not in _CONNECTORS
        and (token[:1].isupper() or token.isupper())
    ]
    risk = 0
    if coverage < 1.0:
        risk += 2
    if coverage < 0.5:
        risk += 1
    if not named_tokens:
        risk += 2
    if any(token in _GENERIC_ACTOR_TERMS for token in actor_tokens):
        risk += 1
    if any(token in _ARTIFACT_TERMS for token in actor_tokens):
        risk += 2
    evidence_tokens = _tokens(atomic.get("evidence_text"))
    if evidence_tokens[: len(actor_tokens)] == actor_tokens:
        risk -= 1
    return risk


def suppress_predictions(
    predictions: Sequence[Mapping[str, Any]],
    *,
    threshold: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    output = copy.deepcopy(list(predictions))
    risk_counts: Counter[int] = Counter()
    suppressed = 0
    emitted = 0
    for row in output:
        for atomic in row["atomic_claims"]:
            if not atomic.get("reported_actor"):
                continue
            emitted += 1
            risk = actor_emission_risk(atomic)
            risk_counts[risk] += 1
            if risk >= threshold:
                atomic["reported_actor"] = None
                suppressed += 1
    return output, {
        "threshold": threshold,
        "emitted_before": emitted,
        "suppressed": suppressed,
        "emitted_after": emitted - suppressed,
        "risk_distribution": {
            str(key): value for key, value in sorted(risk_counts.items())
        },
    }


def run_frontier(*, suite_root: str | Path) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    source_path = root / SOURCE_RELATIVE
    source = true_north._read_json(source_path)
    predictions = source["items"]
    manifest = true_north._read_json(root / "manifest.json")
    consensus = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    candidate_ids = {
        str(row["candidate_id"]) for row in predictions
    }
    speaker_maps = _speaker_maps(manifest)
    frontier: list[dict[str, Any]] = []
    variants: dict[int, list[dict[str, Any]]] = {}
    for threshold in THRESHOLDS:
        variant, accounting = suppress_predictions(
            predictions, threshold=threshold
        )
        variants[threshold] = variant
        aggregate = score_campaign(
            variant,
            consensus["items"],
            preferred["items"],
            subset_candidate_ids=candidate_ids,
            speaker_maps_by_candidate=speaker_maps,
        )["aggregate"]
        actor = float(aggregate["reported_actor_exactness"])
        hallucination = float(
            aggregate["hallucination_rate_proxy"]
        )
        frontier.append(
            {
                **accounting,
                "reported_actor_exactness": actor,
                "hallucination_rate_proxy": hallucination,
                "actor_gate_passed": actor >= ACTOR_GATE,
                "hallucination_gate_passed": (
                    hallucination <= HALLUCINATION_GATE
                ),
                "jointly_passed": (
                    actor >= ACTOR_GATE
                    and hallucination <= HALLUCINATION_GATE
                ),
            }
        )
    passing = [row for row in frontier if row["jointly_passed"]]
    if not passing:
        adopted = None
        adopted_score = None
    else:
        adopted = max(
            passing,
            key=lambda row: (
                row["reported_actor_exactness"],
                -row["hallucination_rate_proxy"],
                row["threshold"],
            ),
        )
        threshold = int(adopted["threshold"])
        adopted_predictions = variants[threshold]
        predictions_document = {
            "schema_version": SCHEMA_VERSION,
            "experiment_id": EXPERIMENT_ID,
            "source_sha256": true_north._sha256_file(source_path),
            "adopted_threshold": threshold,
            "items": adopted_predictions,
        }
        predictions_document["predictions_sha256"] = (
            true_north.sha256_text(
                true_north.dumps_json(predictions_document)
            )
        )
        predictions_path = (
            root
            / "rescoring"
            / "decoupled-v2"
            / "phase-d-actor-suppression-predictions.private.json"
        )
        true_north._write_json(
            predictions_path, predictions_document, immutable=False
        )
        adopted_score = score_predictions(
            suite_root=root,
            lane_id="phase-d-actor-suppression",
            predictions=adopted_predictions,
            source_paths=[source_path, predictions_path],
            actor_span_applied=True,
            actor_span_report={
                "suppression_rule": SCHEMA_VERSION,
                **{
                    key: value
                    for key, value in adopted.items()
                    if key
                    in {
                        "threshold",
                        "emitted_before",
                        "suppressed",
                        "emitted_after",
                    }
                },
            },
        )
    result = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "source_path": str(source_path),
        "source_sha256": true_north._sha256_file(source_path),
        "runtime_inputs": (
            "reported_actor, claim_text, and evidence_text only"
        ),
        "gold_use": "offline scoring only; never an input to the rule",
        "frontier": frontier,
        "adopted": adopted,
        "adopted_score": adopted_score,
        "provider_calls": 0,
        "provider_tokens": 0,
        "holdout_opened": False,
        "production_mutation": False,
    }
    result["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    output = (
        root
        / "diagnostics"
        / "phase-d-actor-suppression-frontier-v1.json"
    )
    true_north._write_json(output, result, immutable=False)
    return {**result, "output_path": str(output)}
