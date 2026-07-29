"""Zero-call gold-vs-gold calibration for the hallucination proxy."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from . import true_north
from .true_north_gate_calibration import _synthetic_consensus
from .true_north_semantic_scoring import (
    SCHEMA_VERSION as SCORER_VERSION,
    score_candidate,
)


SCHEMA_VERSION = "pif_true_north_hallucination_ceiling_v1"
CURRENT_GATE = 0.02


def _summarize_scores(
    scores: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if not scores:
        raise ValueError("hallucination ceiling requires candidate scores")
    live_flagged = {
        candidate_id
        for candidate_id, score in scores.items()
        if score["hallucination_or_unsupported_proxy"]["flagged"]
    }
    matched_flagged = {
        candidate_id
        for candidate_id, score in scores.items()
        if any(
            flag["severity"] == "hallucination"
            for pair in score["alignment"]
            for flag in pair["unsupported_field_flags"]
        )
    }
    flag_kinds: dict[str, int] = {}
    for score in scores.values():
        for flag in score["unsupported_field_flags"]:
            if flag["severity"] != "hallucination":
                continue
            kind = str(flag["kind"])
            flag_kinds[kind] = flag_kinds.get(kind, 0) + 1
    denominator = len(scores)
    live_rate = len(live_flagged) / denominator
    matched_rate = len(matched_flagged) / denominator
    return {
        "candidate_count": denominator,
        "live_proxy": {
            "flagged_candidate_count": len(live_flagged),
            "rate": live_rate,
            "includes_unmatched_predicted_claims": True,
        },
        "matched_pair_only_diagnostic": {
            "flagged_candidate_count": len(matched_flagged),
            "rate": matched_rate,
            "includes_unmatched_predicted_claims": False,
        },
        "hallucination_flag_kind_counts": dict(sorted(flag_kinds.items())),
        "current_gate": CURRENT_GATE,
        "current_gate_passes_gold_vs_gold": live_rate <= CURRENT_GATE,
        "min_current_target_and_observed_rate": min(
            CURRENT_GATE, live_rate
        ),
        "measurement_contract_finding_required": live_rate > CURRENT_GATE,
        "gate_changed": False,
    }


def compute_hallucination_ceiling(
    *,
    suite_root: str | Path,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    manifest = true_north._read_json(root / "manifest.json")
    gold_root = root / "gold" / "development"
    pass_a, pass_b = true_north._load_independent_atomic_gold(gold_root)
    scores = {
        candidate_id: score_candidate(
            pass_a[candidate_id],
            _synthetic_consensus(pass_b[candidate_id]),
            pass_b[candidate_id],
        )
        for candidate_id in sorted(pass_a)
    }
    result = {
        "schema_version": SCHEMA_VERSION,
        "scorer_version": SCORER_VERSION,
        "suite_id": root.name,
        "partition": "development",
        "direction": "pass_a_as_prediction_vs_pass_b_as_reference",
        "manifest_sha256": manifest["manifest_sha256"],
        **_summarize_scores(scores),
        "provider_calls": 0,
        "holdout_opened": False,
        "production_mutation": False,
    }
    result["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    path = (
        root
        / "calibration"
        / "hallucination-gold-vs-gold-v1.json"
    )
    true_north._write_json(path, result, immutable=False)
    return {**result, "output_path": str(path)}

