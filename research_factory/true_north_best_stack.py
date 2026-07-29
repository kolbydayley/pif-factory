"""Zero-call definitive composition and faithfulness diagnostics."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from . import true_north
from .true_north_actor_suppression import suppress_predictions
from .true_north_decoupled_rescore import _speaker_maps, score_predictions
from .true_north_dual_decomposition import _load_search_context
from .true_north_semantic_scoring import (
    _align,
    _claim_refs,
    _consensus_decompositions,
    lexical_faithfulness_proxy,
    score_campaign,
)


SCHEMA_VERSION = "pif_true_north_best_stack_v1"
LANE_ID = "c2-phase-d-best-coherent-stack"
C2_RUN_ID = "task5-c2-sol-adjudicator-schema-free-final-20260729-v1"
C2_SOURCE_RELATIVE = (
    "multipass/runs/"
    f"{C2_RUN_ID}/outputs/composed-actor-span/predictions.private.json"
)
ACTOR_SUPPRESSION_THRESHOLD = 2
FAITHFULNESS_GATE = 0.744435


class BestStackError(RuntimeError):
    """Raised when frozen artifacts cannot be composed coherently."""


def _percentile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 6)
    fraction = position - lower
    return round(
        ordered[lower] * (1 - fraction) + ordered[upper] * fraction,
        6,
    )


def distribution(values: Iterable[float]) -> dict[str, Any]:
    materialized = [float(value) for value in values]
    bins = {
        "0.00_to_lt_0.25": 0,
        "0.25_to_lt_0.50": 0,
        "0.50_to_lt_0.75": 0,
        "0.75_to_lt_0.90": 0,
        "0.90_to_1.00": 0,
    }
    for value in materialized:
        if value < 0.25:
            bins["0.00_to_lt_0.25"] += 1
        elif value < 0.50:
            bins["0.25_to_lt_0.50"] += 1
        elif value < 0.75:
            bins["0.50_to_lt_0.75"] += 1
        elif value < 0.90:
            bins["0.75_to_lt_0.90"] += 1
        else:
            bins["0.90_to_1.00"] += 1
    return {
        "count": len(materialized),
        "mean": (
            None
            if not materialized
            else round(sum(materialized) / len(materialized), 6)
        ),
        "minimum": min(materialized, default=None),
        "p10": _percentile(materialized, 0.10),
        "p25": _percentile(materialized, 0.25),
        "median": _percentile(materialized, 0.50),
        "p75": _percentile(materialized, 0.75),
        "p90": _percentile(materialized, 0.90),
        "maximum": max(materialized, default=None),
        "bins": bins,
    }


def _load_gold_pass(root: Path, pass_name: str) -> dict[str, dict[str, Any]]:
    output_root = root / "gold" / "development" / pass_name / "outputs"
    items = {
        str(item["candidate_id"]): item
        for path in sorted(output_root.glob("*/validated.private.json"))
        for item in true_north._read_json(path)["items"]
    }
    if len(items) != 1140:
        raise BestStackError(
            f"{pass_name} must contain exactly 1140 development items"
        )
    return items


def _best_consensus_alignment_with_gold(
    predicted_claims: Sequence[Mapping[str, Any]],
    consensus: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    alternatives = _consensus_decompositions(consensus)
    if not alternatives:
        return _align(
            predicted_claims, [], check_reference_fields=False
        ), []
    scored: list[
        tuple[tuple[Any, ...], dict[str, Any], list[dict[str, Any]]]
    ] = []
    for alternative in alternatives:
        alignment = _align(
            predicted_claims,
            alternative,
            check_reference_fields=False,
        )
        score = alignment["claim_text_faithfulness_proxy"]
        tie_key = (
            -(score if score is not None else -1.0),
            len(alignment["unmatched_predicted"])
            + len(alignment["unmatched_gold"]),
            json.dumps(
                alternative, sort_keys=True, separators=(",", ":")
            ),
        )
        scored.append((tie_key, alignment, alternative))
    _tie, alignment, alternative = min(
        scored, key=lambda item: item[0]
    )
    return alignment, alternative


def compose_best_stack(*, suite_root: str | Path) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    source_path = root / C2_SOURCE_RELATIVE
    source = true_north._read_json(source_path)
    predictions = source["items"]
    composed, suppression = suppress_predictions(
        predictions,
        threshold=ACTOR_SUPPRESSION_THRESHOLD,
    )
    output_document = {
        "schema_version": SCHEMA_VERSION,
        "lane_id": LANE_ID,
        "source_run_id": C2_RUN_ID,
        "source_sha256": true_north._sha256_file(source_path),
        "components": {
            "disposition": "certified_option2_ensemble",
            "decomposition": "c2_sol_adjudication_schema_free_final",
            "speaker": "frozen_candidate_prior_adoption",
            "actor": (
                "deterministic_actor_span_rule_v1_plus_"
                "phase_d_suppression_threshold_2"
            ),
        },
        "coherence": {
            "coherent": True,
            "reason": (
                "C2 source already carries the normative actor-span rule; "
                "Phase D changes reported_actor only and reads fields present "
                "on each resulting C2 atomic."
            ),
        },
        "suppression": suppression,
        "items": composed,
    }
    output_document["predictions_sha256"] = true_north.sha256_text(
        true_north.dumps_json(output_document)
    )
    output_path = (
        root
        / "rescoring"
        / "decoupled-v2"
        / "c2-phase-d-best-stack-predictions.private.json"
    )
    true_north._write_json(output_path, output_document, immutable=False)
    score = score_predictions(
        suite_root=root,
        lane_id=LANE_ID,
        predictions=composed,
        source_paths=[source_path, output_path],
        actor_span_applied=True,
        actor_span_report={
            "actor_span_rule": "deterministic_actor_span_rule_v1",
            "suppression_rule": (
                "pif_true_north_actor_suppression_v1"
            ),
            **suppression,
        },
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "lane_id": LANE_ID,
        "coherence": output_document["coherence"],
        "suppression": suppression,
        "predictions_path": str(output_path),
        "predictions_sha256": output_document["predictions_sha256"],
        "score": score,
        "provider_calls": 0,
        "provider_tokens": 0,
        "holdout_opened": False,
        "production_mutation": False,
    }


def faithfulness_headroom(
    *,
    suite_root: str | Path,
    predictions_path: str | Path,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    manifest = true_north._read_json(root / "manifest.json")
    resolved_predictions_path = Path(predictions_path).expanduser().resolve()
    predictions_document = true_north._read_json(
        resolved_predictions_path
    )
    predictions = {
        str(row["candidate_id"]): row
        for row in predictions_document["items"]
    }
    consensus = {
        str(row["candidate_id"]): row
        for row in true_north._read_json(
            root / "gold" / "development" / "final" / "consensus.private.json"
        )["items"]
    }
    gold = {
        str(row["candidate_id"]): row
        for row in true_north._read_json(
            root / "gold" / "development" / "final" / "gold.private.json"
        )["items"]
    }
    _jobs, _dispositions, candidates = _load_search_context(root, manifest)
    candidate_ids = set(predictions)
    if candidate_ids != set(candidates):
        raise BestStackError(
            "best-stack predictions do not exactly match Search candidates"
        )
    speaker_maps = _speaker_maps(manifest)
    live_score = score_campaign(
        list(predictions.values()),
        [consensus[candidate_id] for candidate_id in sorted(candidate_ids)],
        [gold[candidate_id] for candidate_id in sorted(candidate_ids)],
        subset_candidate_ids=candidate_ids,
        speaker_maps_by_candidate=speaker_maps,
    )
    emitted_scores: list[float] = []
    candidate_scores: list[float] = []
    max_wording_scores: list[float] = []
    emitted_below_candidate = 0
    emitted_below_candidate_delta = 0.0
    aligned_count_groups: dict[str, list[float]] = {
        "acceptable_atomic_count": [],
        "unacceptable_atomic_count": [],
    }
    candidate_score_by_pair: list[dict[str, Any]] = []
    live_scores_by_id = {
        str(row["candidate_id"]): row
        for row in live_score["candidates"]
    }
    for candidate_id in sorted(candidate_ids):
        candidate_score = live_scores_by_id[candidate_id]
        if not candidate_score["strictly_scoreable"]:
            continue
        candidate_id = str(candidate_score["candidate_id"])
        predicted_refs = dict(
            _claim_refs(
                predictions[candidate_id]["atomic_claims"], "pred"
            )
        )
        consensus_alignment, consensus_gold = (
            _best_consensus_alignment_with_gold(
                predictions[candidate_id]["atomic_claims"],
                consensus[candidate_id],
            )
        )
        gold_refs = dict(
            _claim_refs(consensus_gold, "gold")
        )
        candidate_text = str(candidates[candidate_id]["claim_text"])
        count_group = (
            "acceptable_atomic_count"
            if candidate_score["atomic_count"]["acceptable_count"]
            else "unacceptable_atomic_count"
        )
        for pair in consensus_alignment["pairs"]:
            emitted = float(
                pair["claim_text_faithfulness_proxy"]["score"]
            )
            gold_text = str(
                gold_refs[str(pair["gold_ref"])].get("claim_text") or ""
            )
            baseline = float(
                lexical_faithfulness_proxy(
                    candidate_text, gold_text
                )["score"]
            )
            emitted_scores.append(emitted)
            candidate_scores.append(baseline)
            max_wording_scores.append(max(emitted, baseline))
            aligned_count_groups[count_group].append(emitted)
            if emitted < baseline:
                emitted_below_candidate += 1
                emitted_below_candidate_delta += baseline - emitted
            candidate_score_by_pair.append(
                {
                    "candidate_id": candidate_id,
                    "predicted_ref": pair["predicted_ref"],
                    "gold_ref": pair["gold_ref"],
                    "emitted_score": emitted,
                    "candidate_verbatim_score": baseline,
                    "emitted_claim_text": predicted_refs[
                        str(pair["predicted_ref"])
                    ].get("claim_text"),
                }
            )
    pass_a = _load_gold_pass(root, "pass-a")
    pass_b = _load_gold_pass(root, "pass-b")
    gold_pair_scores: list[float] = []
    for candidate_id in sorted(pass_a):
        alignment = _align(
            pass_a[candidate_id]["atomic_claims"],
            pass_b[candidate_id]["atomic_claims"],
            speaker_map=speaker_maps.get(candidate_id),
        )
        gold_pair_scores.extend(
            float(pair["claim_text_faithfulness_proxy"]["score"])
            for pair in alignment["pairs"]
        )
    emitted_distribution = distribution(emitted_scores)
    candidate_distribution = distribution(candidate_scores)
    gold_distribution = distribution(gold_pair_scores)
    wording_oracle_distribution = distribution(max_wording_scores)
    aligned_distributions = {
        key: distribution(values)
        for key, values in aligned_count_groups.items()
    }
    result = {
        "schema_version": "pif_true_north_faithfulness_headroom_v1",
        "source_predictions_path": str(resolved_predictions_path),
        "source_predictions_sha256": true_north._sha256_file(
            resolved_predictions_path
        ),
        "matched_pair_count": len(emitted_scores),
        "faithfulness_gate": FAITHFULNESS_GATE,
        "candidate_verbatim_against_gold": candidate_distribution,
        "emitted_against_gold": emitted_distribution,
        "gold_a_against_gold_b": gold_distribution,
        "best_of_emitted_or_candidate_wording": (
            wording_oracle_distribution
        ),
        "emitted_below_candidate_verbatim_count": (
            emitted_below_candidate
        ),
        "emitted_below_candidate_verbatim_rate": round(
            emitted_below_candidate / len(emitted_scores), 6
        ),
        "maximum_mean_gain_from_candidate_wording_substitution": round(
            emitted_below_candidate_delta / len(emitted_scores), 6
        ),
        "alignment_conditioned": aligned_distributions,
        "finding": {
            "wording_only_reaches_gate": bool(
                wording_oracle_distribution["mean"] is not None
                and float(wording_oracle_distribution["mean"])
                >= FAITHFULNESS_GATE
            ),
            "emitted_minus_candidate_mean": round(
                float(emitted_distribution["mean"])
                - float(candidate_distribution["mean"]),
                6,
            ),
            "gold_style_headroom_above_emitted": round(
                float(gold_distribution["mean"])
                - float(emitted_distribution["mean"]),
                6,
            ),
            "alignment_penalty": round(
                float(
                    aligned_distributions["acceptable_atomic_count"]["mean"]
                )
                - float(
                    aligned_distributions[
                        "unacceptable_atomic_count"
                    ]["mean"]
                ),
                6,
            ),
        },
        "private_pair_diagnostics": candidate_score_by_pair,
        "provider_calls": 0,
        "provider_tokens": 0,
        "holdout_opened": False,
        "production_mutation": False,
    }
    result["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    output_path = (
        root
        / "diagnostics"
        / "faithfulness-headroom-best-stack-v1.private.json"
    )
    true_north._write_json(output_path, result, immutable=False)
    return {
        **{
            key: value
            for key, value in result.items()
            if key != "private_pair_diagnostics"
        },
        "output_path": str(output_path),
    }
