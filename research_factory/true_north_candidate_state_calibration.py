"""Exact gold-vs-gold calibration for the True-North candidate-state gate.

This module deliberately uses the same three-class mapping and macro-F1
implementation as the live ``consensus_candidate_state_macro_f1`` gate. It
does not change the gate. It produces a zero-call review artifact so a
measurement-contract ruling can compare the current target with the measured
inter-annotator ceiling.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north


SCHEMA_VERSION = "pif_true_north_candidate_state_calibration_v1"
CURRENT_TARGET = 0.90
MARGIN = 0.04
LABELS = ("value", "junk", "hold")
OUTPUT_RELATIVE = Path(
    "diagnostics/candidate-state-macro-f1-calibration-v1.json"
)


class CandidateStateCalibrationError(RuntimeError):
    """Raised when the calibration scope or source artifacts are invalid."""


def _state_map(
    items: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    return {
        candidate_id: true_north._gold_value_state(
            str(item["disposition"])
        )
        for candidate_id, item in items.items()
    }


def compute_candidate_state_calibration(
    pass_a: Mapping[str, Mapping[str, Any]],
    pass_b: Mapping[str, Mapping[str, Any]],
    *,
    current_target: float = CURRENT_TARGET,
    margin: float = MARGIN,
) -> dict[str, Any]:
    """Score pass A as predictions against pass B with the live gate metric."""

    if not pass_a or set(pass_a) != set(pass_b):
        raise CandidateStateCalibrationError(
            "pass-A and pass-B must have the same non-empty candidate scope"
        )
    if not 0.0 <= current_target <= 1.0:
        raise CandidateStateCalibrationError(
            "current_target must be in [0, 1]"
        )
    if not 0.0 <= margin <= 1.0:
        raise CandidateStateCalibrationError("margin must be in [0, 1]")

    predicted_states = _state_map(pass_a)
    reference_states = _state_map(pass_b)
    macro_f1, by_class = true_north._macro_f1(
        reference_states,
        predicted_states,
        LABELS,
    )
    exact_disposition_agreement = sum(
        str(pass_a[candidate_id]["disposition"])
        == str(pass_b[candidate_id]["disposition"])
        for candidate_id in pass_a
    ) / len(pass_a)
    value_state_agreement = sum(
        predicted_states[candidate_id]
        == reference_states[candidate_id]
        for candidate_id in pass_a
    ) / len(pass_a)
    recommended_threshold = min(
        float(current_target),
        max(0.0, float(macro_f1) - float(margin)),
    )
    gate_exceeds_measured_ceiling = macro_f1 < current_target
    document = {
        "schema_version": SCHEMA_VERSION,
        "metric": "consensus_candidate_state_macro_f1",
        "scoring_contract": {
            "prediction_source": "gold_pass_a",
            "reference_source": "gold_pass_b",
            "labels": list(LABELS),
            "mapping": {
                "retain": "value",
                "revise": "value",
                "reject": "junk",
                "hold": "hold",
            },
            "implementation": "research_factory.true_north._macro_f1",
            "scope": "all_development_candidates_before_consensus_filtering",
        },
        "candidate_count": len(pass_a),
        "measured_ceiling": macro_f1,
        "by_class": by_class,
        "diagnostics": {
            "raw_disposition_exact_agreement": round(
                exact_disposition_agreement, 6
            ),
            "value_state_exact_agreement": round(
                value_state_agreement, 6
            ),
        },
        "calibration_rule": {
            "comparison": ">=",
            "current_target": float(current_target),
            "margin": float(margin),
            "formula": "min(current_target, measured_ceiling - margin)",
            "recommended_threshold": round(recommended_threshold, 6),
        },
        "finding": {
            "gate_exceeds_measured_ceiling": gate_exceeds_measured_ceiling,
            "current_gate_is_ceiling_referenced": not (
                gate_exceeds_measured_ceiling
            ),
            "review_recommendation": (
                "ruling_4_re_reference_to_recommended_threshold"
                if gate_exceeds_measured_ceiling
                else "retain_current_gate_and_treat_live_shortfall_as_real"
            ),
            "gate_changed": False,
        },
        "provider_calls": 0,
        "sealed_holdout_opened": False,
        "production_mutated": False,
    }
    return document


def run_candidate_state_calibration(
    suite_root: str | Path,
) -> dict[str, Any]:
    """Load frozen development passes, hash-bind them, and write diagnostics."""

    root = Path(suite_root).expanduser().resolve()
    base = root / "gold" / "development"
    pass_a, pass_b = true_north._load_independent_atomic_gold(base)
    document = compute_candidate_state_calibration(pass_a, pass_b)
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
    if not source_paths:
        raise CandidateStateCalibrationError(
            "no development pass artifacts were found"
        )
    document["suite_id"] = true_north.SUITE_ID
    document["partition"] = "development"
    document["source_artifacts"] = {
        "count": len(source_paths),
        "aggregate_sha256": true_north.sha256_text(
            true_north.dumps_json(
                [
                    {
                        "path": str(path.relative_to(root)),
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
    output_path = root / OUTPUT_RELATIVE
    true_north._write_json(output_path, document)
    return {**document, "output_path": str(output_path)}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Calibrate the True-North three-class candidate-state macro F1."
        )
    )
    parser.add_argument("--suite-root", required=True)
    args = parser.parse_args(argv)
    print(
        json.dumps(
            run_candidate_state_calibration(args.suite_root),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
