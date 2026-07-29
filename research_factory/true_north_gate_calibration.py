"""Inter-annotator ceiling calibration for True North semantic gates."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from .true_north_semantic_scoring import score_candidate


SCHEMA_VERSION = "pif_true_north_gate_calibration_v2"
DEFAULT_CURRENT_TARGETS = {
    "claim_text_faithfulness_proxy": 0.90,
    "speaker_exactness": 0.97,
    "reported_actor_exactness": 0.95,
    "atomic_count_exactness": 0.90,
    "disposition_exactness": 0.90,
    "value_state_exactness": 0.90,
}


class GateCalibrationError(RuntimeError):
    """Raised when calibration inputs are incomplete or inconsistent."""


def summarize_ceiling(
    values: Sequence[float],
    *,
    current_target: float,
    margin: float = 0.04,
) -> dict[str, Any]:
    """Summarize one ceiling and derive its bounded proposed threshold."""

    if not values:
        raise GateCalibrationError("ceiling values must not be empty")
    normalized = [float(value) for value in values]
    if any(value < 0.0 or value > 1.0 for value in normalized):
        raise GateCalibrationError("ceiling values must be in [0, 1]")
    mean = sum(normalized) / len(normalized)
    median = statistics.median(normalized)
    recommended = min(float(current_target), max(0.0, mean - margin))
    return {
        "sample_count": len(normalized),
        "mean": round(mean, 6),
        "median": round(median, 6),
        "fraction_at_or_above_current_target": round(
            sum(value >= current_target for value in normalized)
            / len(normalized),
            6,
        ),
        "current_target": float(current_target),
        "margin": float(margin),
        "recommended_threshold": round(recommended, 6),
    }


def _synthetic_consensus(item: Mapping[str, Any]) -> dict[str, Any]:
    disposition = str(item["disposition"])
    value_state = true_north._gold_value_state(disposition)
    atomic_count = len(item["atomic_claims"])
    return {
        "candidate_id": str(item["candidate_id"]),
        "consensus_state": "interannotator_calibration",
        "strictly_scoreable": True,
        "acceptable_dispositions": [disposition],
        "acceptable_value_states": [value_state],
        "acceptable_atomic_counts": [atomic_count],
        "minimum_atomic_count": atomic_count,
        "maximum_atomic_count": atomic_count,
        "preferred_disposition": disposition,
        "preferred_value_state": value_state,
        "decompositions": [
            {
                "source": "pass_b",
                "disposition": disposition,
                "value_state": value_state,
                "atomic_count": atomic_count,
                "claim_texts": [
                    str(atomic["claim_text"])
                    for atomic in item["atomic_claims"]
                ],
            }
        ],
    }


_GATE_DENOMINATOR_METRICS = (
    "speaker_exactness",
    "reported_actor_exactness",
    "claim_text_faithfulness_proxy",
)


def _gate_denominator_alignment(
    coupled_values: Mapping[str, Sequence[float]],
    matched_ceilings: Mapping[str, Mapping[str, Any]],
    targets: Mapping[str, float],
) -> dict[str, Any]:
    """Compare each gate's target against a ceiling on the gate's own denominator.

    `summarize_ceiling` derives its recommendation from matched atomic pairs
    only, while the live gate scores `correct / max(predicted, gold)`. When the
    two denominators disagree, a gate can sit above the agreement its own gold
    achieves and stay unpassable through recalibration. This block reports both
    numbers side by side; it changes no threshold and no gate wiring.
    """

    metrics: dict[str, Any] = {}
    for metric in _GATE_DENOMINATOR_METRICS:
        values = list(coupled_values.get(metric, ()))
        target = float(targets[metric])
        if not values:
            metrics[metric] = {
                "coupled_sample_count": 0,
                "matched_pair_ceiling_mean": matched_ceilings[metric]["mean"],
                "coupled_ceiling_mean": None,
                "live_target": target,
                "gate_exceeds_coupled_ceiling": None,
                "recommended_threshold_on_gate_denominator": None,
            }
            continue
        coupled = summarize_ceiling(values, current_target=target)
        metrics[metric] = {
            "coupled_sample_count": len(values),
            "matched_pair_ceiling_mean": matched_ceilings[metric]["mean"],
            "coupled_ceiling_mean": coupled["mean"],
            "live_target": target,
            "gate_exceeds_coupled_ceiling": coupled["mean"] < target,
            "recommended_threshold_on_gate_denominator": coupled[
                "recommended_threshold"
            ],
        }
    return {
        "live_gate_changed": False,
        "metrics": metrics,
        "rationale": (
            "A gate threshold is only meaningful against a ceiling measured on "
            "the same denominator the gate scores on. Where "
            "gate_exceeds_coupled_ceiling is true, the gold process itself "
            "cannot pass that gate, so the reading certifies nothing about the "
            "extractor. Adopting any of these thresholds is a "
            "measurement-contract change and requires explicit approval."
        ),
    }


def compute_ceiling_document(
    pass_a: Mapping[str, Mapping[str, Any]],
    pass_b: Mapping[str, Mapping[str, Any]],
    *,
    current_targets: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """Compute matched-atomic and candidate-decision agreement ceilings."""

    if not pass_a or set(pass_a) != set(pass_b):
        raise GateCalibrationError(
            "pass-A and pass-B must have the same non-empty candidate scope"
        )
    targets = dict(DEFAULT_CURRENT_TARGETS)
    targets.update(current_targets or {})
    values: dict[str, list[float]] = {
        key: [] for key in targets
    }
    campaign_faithfulness_numerator = 0.0
    campaign_faithfulness_denominator = 0
    campaign_actor_correct = 0
    campaign_actor_denominator = 0
    matched_pair_count = 0
    # Per-candidate agreement scored on the denominator the live gate itself
    # uses (correct / max(predicted_atoms, gold_atoms)), so a recommended
    # threshold is calibrated against the measurement the gate actually makes.
    coupled_values: dict[str, list[float]] = {
        metric: [] for metric in _GATE_DENOMINATOR_METRICS
    }
    for candidate_id in sorted(pass_a):
        item_a = pass_a[candidate_id]
        item_b = pass_b[candidate_id]
        scored = score_candidate(
            item_a,
            _synthetic_consensus(item_b),
            item_b,
        )
        values["disposition_exactness"].append(
            float(item_a["disposition"] == item_b["disposition"])
        )
        values["value_state_exactness"].append(
            float(
                true_north._gold_value_state(str(item_a["disposition"]))
                == true_north._gold_value_state(str(item_b["disposition"]))
            )
        )
        values["atomic_count_exactness"].append(
            float(
                len(item_a["atomic_claims"])
                == len(item_b["atomic_claims"])
            )
        )
        faith = scored["claim_text_faithfulness_proxy"]
        coupled_faith = faith["coupled_diagnostic"]
        if coupled_faith["score"] is not None:
            campaign_faithfulness_numerator += (
                float(coupled_faith["score"])
                * int(coupled_faith["micro_denominator"])
            )
            campaign_faithfulness_denominator += int(
                coupled_faith["micro_denominator"]
            )
        actor_metric = scored["reported_actor_exactness"]
        campaign_actor_correct += int(actor_metric["correct_pairs"])
        campaign_actor_denominator += int(
            actor_metric["coupled_diagnostic"]["micro_denominator"]
        )
        for metric in _GATE_DENOMINATOR_METRICS:
            coupled_score = scored[metric]["coupled_diagnostic"]["score"]
            if coupled_score is not None:
                coupled_values[metric].append(float(coupled_score))
        for pair in scored["alignment"]:
            matched_pair_count += 1
            values["claim_text_faithfulness_proxy"].append(
                float(pair["claim_text_faithfulness_proxy"]["score"])
            )
            values["speaker_exactness"].append(
                float(pair["field_exactness"]["raw_speaker"])
            )
            values["reported_actor_exactness"].append(
                float(pair["field_exactness"]["reported_actor"])
            )
    ceilings = {
        metric: summarize_ceiling(
            metric_values,
            current_target=targets[metric],
        )
        for metric, metric_values in values.items()
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate_count": len(pass_a),
        "matched_atomic_pair_count": matched_pair_count,
        "ceilings": ceilings,
        "gate_denominator_alignment": _gate_denominator_alignment(
            coupled_values,
            ceilings,
            targets,
        ),
        "coupled_diagnostics": {
            "claim_text_campaign_micro_including_unmatched_atomics": round(
                (
                    campaign_faithfulness_numerator
                    / campaign_faithfulness_denominator
                )
                if campaign_faithfulness_denominator
                else 1.0,
                6,
            ),
            "reported_actor_campaign_micro_including_unmatched_atomics": round(
                campaign_actor_correct / campaign_actor_denominator
                if campaign_actor_denominator
                else 1.0,
                6,
            ),
        },
        "proposed_faithfulness_gate": {
            "live_gate_changed": False,
            "aspirational_target": targets[
                "claim_text_faithfulness_proxy"
            ],
            "proposed_lexical_threshold": ceilings[
                "claim_text_faithfulness_proxy"
            ]["recommended_threshold"],
            "required_companion_gate": {
                "metric": "hallucination_rate_proxy",
                "comparison": "<=",
                "threshold": 0.02,
            },
            "rationale": (
                "Matched-atomic wording faithfulness is calibrated separately "
                "from missing-atomic penalties because atomic-count accuracy "
                "already gates decomposition. A ceiling above the attainable "
                "inter-annotator agreement does not certify semantic quality."
            ),
        },
    }


def compute_interannotator_ceilings(
    suite: str | Path,
) -> dict[str, Any]:
    """Compute, hash-bind, and store a proposed calibration for one suite."""

    suite_root = Path(suite).expanduser().resolve()
    base = suite_root / "gold" / "development"
    pass_a, pass_b = true_north._load_independent_atomic_gold(base)
    document = compute_ceiling_document(pass_a, pass_b)
    source_paths = sorted(
        [
            *(base / "pass-a" / "outputs").glob(
                "*/validated.private.json"
            ),
            *(base / "pass-b" / "outputs").glob(
                "*/validated.private.json"
            ),
        ]
    )
    document["suite_id"] = true_north.SUITE_ID
    document["partition"] = "development"
    document["source_artifacts"] = {
        "count": len(source_paths),
        "aggregate_sha256": true_north.sha256_text(
            true_north.dumps_json(
                [
                    {
                        "path": str(path.relative_to(suite_root)),
                        "sha256": true_north._sha256_file(path),
                    }
                    for path in source_paths
                ]
            )
        ),
    }
    document["calibration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    output_path = suite_root / "diagnostics" / "gate-calibration.json"
    true_north._write_json(output_path, document, immutable=True)
    return document


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate proposed True North inter-gold gate calibration."
    )
    parser.add_argument("--suite-root", required=True)
    args = parser.parse_args(argv)
    document = compute_interannotator_ceilings(args.suite_root)
    print(json.dumps(document, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
