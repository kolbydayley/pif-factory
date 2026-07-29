"""Zero-model-call candidate-prior adoption floor for True North.

This module deliberately reuses only stored workhorse dispositions and frozen
candidate fields.  It never reads a gold artifact while composing predictions.
Gold is loaded only by the separate scoring entrypoint after the prediction
artifact has been fully materialized and hashed.
"""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from . import true_north_input_optimization as input_optimization
from .true_north_semantic_scoring import normalize_enum_field, score_campaign


SCHEMA_VERSION = "pif_true_north_prior_adoption_v1"
DEFAULT_VARIANT_ID = "stack-03-all-compatible"
_ABSENT_TEXT = {"", "none", "null", "n/a", "na", "unknown", "unspecified"}
_GATE_RULES = true_north.APPROVED_GATE_POLICY


class PriorAdoptionError(RuntimeError):
    """Raised when the frozen prior-adoption contract is violated."""


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _meaningful(value: Any) -> str | None:
    text = _text(value)
    return None if text.lower() in _ABSENT_TEXT else text


def _named(value: Any) -> str | None:
    if isinstance(value, Mapping):
        return _meaningful(value.get("name"))
    return _meaningful(value)


def _reported_actor(candidate: Mapping[str, Any]) -> str | None:
    """Apply the frozen candidate-prior fallback contract literally."""

    return _named(candidate.get("reported_actor")) or _meaningful(
        candidate.get("actor_name")
    )


def _atomic_from_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    claim_text = _meaningful(candidate.get("claim_text"))
    raw_speaker = _named(candidate.get("speaker")) or _meaningful(
        candidate.get("actor_name")
    )
    if claim_text is None:
        raise PriorAdoptionError(
            f"candidate lacks claim_text: {candidate.get('candidate_id')}"
        )
    if raw_speaker is None:
        raise PriorAdoptionError(
            f"candidate lacks speaker prior: {candidate.get('candidate_id')}"
        )
    subject_text = (
        _meaningful(candidate.get("candidate_concept"))
        or _meaningful(candidate.get("target_raw"))
        or claim_text
    )
    stance = normalize_enum_field("stance", candidate.get("stance") or "neutral")
    return {
        "claim_text": claim_text,
        "claim_type": normalize_enum_field(
            "claim_type", candidate.get("claim_type") or "assertion"
        ),
        "raw_speaker": raw_speaker,
        "reported_actor": _reported_actor(candidate),
        "stance": stance,
        "certainty": normalize_enum_field(
            "certainty", candidate.get("certainty") or "unspecified"
        ),
        "time_horizon": normalize_enum_field(
            "time_horizon", candidate.get("time_horizon") or "unspecified"
        ),
        "confidence": max(
            0.0, min(1.0, float(candidate.get("confidence") or 0.0))
        ),
        "subject_text": subject_text,
        "subject_type": _meaningful(candidate.get("event_type")) or "topic",
        "domain": _meaningful(candidate.get("frame")),
        "proposition_text": claim_text,
        "polarity": normalize_enum_field(
            "polarity", candidate.get("polarity") or "neutral"
        ),
        "position": stance,
        "evidence_text": str(candidate["evidence_text"]),
        "evidence_start": int(candidate["evidence_start"]),
        "evidence_end": int(candidate["evidence_end"]),
    }


def compose_prior_adoption(
    candidates: Sequence[Mapping[str, Any]],
    dispositions: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Compose one prior-derived atom for each stored retain/revise decision."""

    candidate_by_id = {
        str(candidate["candidate_id"]): candidate for candidate in candidates
    }
    if len(candidate_by_id) != len(candidates):
        raise PriorAdoptionError("candidate IDs are not unique")
    if set(candidate_by_id) != set(dispositions):
        missing = sorted(set(candidate_by_id) - set(dispositions))
        extra = sorted(set(dispositions) - set(candidate_by_id))
        raise PriorAdoptionError(
            f"candidate/disposition scope mismatch: missing={missing} extra={extra}"
        )
    items: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_id = str(candidate["candidate_id"])
        decision = dispositions[candidate_id]
        disposition = str(decision["disposition"])
        if disposition not in true_north.DISPOSITIONS:
            raise PriorAdoptionError(
                f"invalid disposition for {candidate_id}: {disposition}"
            )
        atomics = (
            [_atomic_from_candidate(candidate)]
            if disposition in {"retain", "revise"}
            else []
        )
        items.append(
            {
                "candidate_id": candidate_id,
                "disposition": disposition,
                "reason_code": _meaningful(decision.get("reason_code"))
                or f"stored_{disposition}",
                "atomic_claims": atomics,
            }
        )
    output = {
        "schema_version": true_north.WORK_OUTPUT_SCHEMA_VERSION,
        "items": items,
    }
    validation_job = {
        "input": {"candidates": copy.deepcopy(list(candidates))},
        "output_schema": true_north.atomic_output_schema(
            [str(candidate["candidate_id"]) for candidate in candidates]
        ),
    }
    true_north._validate_atomic_output(output, validation_job)
    return output


def load_stored_stack(
    campaign_dir: str | Path,
    *,
    variant_id: str = DEFAULT_VARIANT_ID,
) -> dict[str, Any]:
    """Load and cryptographically verify the selected stored stack outputs."""

    root = Path(campaign_dir).expanduser().resolve()
    registry, plan, state = input_optimization._load_campaign(root)
    rows = [
        row
        for row in plan["packets"]
        if row["phase"] == "train" and row["variant_id"] == variant_id
    ]
    if not rows:
        raise PriorAdoptionError(f"stored variant is absent: {variant_id}")
    completed = set(state["completed_locators"])
    candidates: list[dict[str, Any]] = []
    dispositions: dict[str, dict[str, Any]] = {}
    source_packets: list[dict[str, Any]] = []
    contexts_by_episode: dict[str, dict[str, Any]] = {}
    for row in rows:
        locator = "/".join(
            (
                row["factor"],
                row["variant_id"],
                row["replicate_id"],
                row["fold_id"],
                row["episode_id"],
                row["packet_id"],
            )
        )
        if locator not in completed:
            raise PriorAdoptionError(f"stored packet is not complete: {locator}")
        packet_path = Path(row["packet_path"])
        packet = _read(packet_path)
        if (
            true_north._sha256_file(packet_path) != row["packet_sha256"]
            or input_optimization._sha(packet) != row["packet_semantic_sha256"]
        ):
            raise PriorAdoptionError(f"stored packet hash mismatch: {locator}")
        result_path = Path(row["output_dir"]) / "campaign-result.private.json"
        result = _read(result_path)
        if (
            result.get("locator") != locator
            or result.get("packet_sha256") != row["packet_sha256"]
            or result.get("packet_semantic_sha256") != row["packet_semantic_sha256"]
            or result.get("output_sha256")
            != input_optimization._sha(result.get("output"))
        ):
            raise PriorAdoptionError(f"stored result identity mismatch: {locator}")
        packet_candidates = list(packet["input"]["candidates"])
        result_items = list(result["output"]["items"])
        packet_ids = {str(item["candidate_id"]) for item in packet_candidates}
        result_ids = {str(item["candidate_id"]) for item in result_items}
        if packet_ids != result_ids:
            raise PriorAdoptionError(
                f"stored result does not cover its packet: {locator}"
            )
        for context_row in packet["input"].get("episode_contexts", []):
            if context_row.get("episode_id"):
                contexts_by_episode[str(context_row["episode_id"])] = dict(
                    context_row.get("context") or {}
                )
        if packet["input"].get("episode", {}).get("episode_id"):
            contexts_by_episode.setdefault(
                str(packet["input"]["episode"]["episode_id"]),
                dict(packet["input"].get("episode_context") or {}),
            )
        for candidate in packet_candidates:
            candidate_id = str(candidate["candidate_id"])
            if candidate_id in dispositions:
                raise PriorAdoptionError(f"duplicate stored candidate: {candidate_id}")
            candidates.append(dict(candidate))
        for item in result_items:
            candidate_id = str(item["candidate_id"])
            dispositions[candidate_id] = {
                "disposition": str(item["disposition"]),
                "reason_code": str(item.get("reason_code") or ""),
            }
        source_packets.append(
            {
                "locator": locator,
                "packet_path": str(packet_path),
                "packet_sha256": row["packet_sha256"],
                "result_path": str(result_path),
                "result_sha256": true_north._sha256_file(result_path),
            }
        )
    return {
        "registry": registry,
        "variant_id": variant_id,
        "candidates": candidates,
        "dispositions": dispositions,
        "contexts_by_episode": contexts_by_episode,
        "source_packets": source_packets,
    }


def _score_scope(
    predictions: Sequence[Mapping[str, Any]],
    *,
    consensus_document: Mapping[str, Any],
    preferred_document: Mapping[str, Any],
    speaker_maps: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_ids = {str(row["candidate_id"]) for row in predictions}
    score = score_campaign(
        predictions,
        consensus_document["items"],
        preferred_document["items"],
        subset_candidate_ids=candidate_ids,
        speaker_maps_by_candidate=speaker_maps,
    )
    score["aggregate"]["schema_parse_success_rate"] = 1.0
    score["aggregate"]["terminal_validation_failure_rate"] = 0.0
    core = true_north._consensus_atomic_metrics(
        consensus_document,
        {str(row["candidate_id"]): row for row in predictions},
        require_complete_scope=False,
    )
    score["aggregate"].update(
        {str(row["metric"]): row["value"] for row in core}
    )
    checks = {
        metric: (
            float(score["aggregate"][metric]) >= threshold
            if comparison == ">="
            else float(score["aggregate"][metric]) <= threshold
        )
        for metric, (comparison, threshold) in _GATE_RULES.items()
    }
    score["gate"] = {
        "passed": all(checks.values()),
        "checks": checks,
        "rules": {
            metric: {"comparison": comparison, "threshold": threshold}
            for metric, (comparison, threshold) in _GATE_RULES.items()
        },
    }
    return score


def run_prior_adoption_floor(
    campaign_dir: str | Path,
    output_dir: str | Path,
    *,
    variant_id: str = DEFAULT_VARIANT_ID,
) -> dict[str, Any]:
    """Materialize predictions first, then score them against frozen gold."""

    loaded = load_stored_stack(campaign_dir, variant_id=variant_id)
    predictions = compose_prior_adoption(
        loaded["candidates"], loaded["dispositions"]
    )
    output_root = Path(output_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    prediction_path = output_root / "predictions.private.json"
    if prediction_path.exists():
        raise PriorAdoptionError(f"prediction artifact already exists: {prediction_path}")
    true_north._write_json(prediction_path, predictions, immutable=True)
    prediction_sha256 = true_north._sha256_file(prediction_path)

    gold_binding = loaded["registry"]["gold_binding"]
    consensus_path = Path(gold_binding["consensus_path"])
    preferred_path = Path(gold_binding["gold_path"])
    if (
        true_north._sha256_file(consensus_path)
        != gold_binding["consensus_file_sha256"]
        or true_north._sha256_file(preferred_path)
        != gold_binding["gold_file_sha256"]
    ):
        raise PriorAdoptionError("frozen gold file hash drift")
    consensus_document = _read(consensus_path)
    preferred_document = _read(preferred_path)

    candidate_by_id = {
        str(row["candidate_id"]): row for row in loaded["candidates"]
    }
    predictions_by_episode: dict[str, list[dict[str, Any]]] = {}
    speaker_maps: dict[str, Any] = {}
    for item in predictions["items"]:
        candidate_id = str(item["candidate_id"])
        candidate = candidate_by_id[candidate_id]
        episode_id = str(candidate.get("_source_episode_id") or "")
        if not episode_id:
            raise PriorAdoptionError(f"candidate lacks source episode: {candidate_id}")
        predictions_by_episode.setdefault(episode_id, []).append(item)
        context = loaded["contexts_by_episode"].get(episode_id, {})
        if context.get("speaker_map"):
            speaker_maps[candidate_id] = context["speaker_map"]

    scopes: dict[str, Any] = {}
    for episode_id, items in sorted(predictions_by_episode.items()):
        scopes[episode_id] = _score_scope(
            items,
            consensus_document=consensus_document,
            preferred_document=preferred_document,
            speaker_maps=speaker_maps,
        )
    scopes["comparison_cohort"] = _score_scope(
        predictions["items"],
        consensus_document=consensus_document,
        preferred_document=preferred_document,
        speaker_maps=speaker_maps,
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "experiment": "zero_model_call_prior_adoption_floor",
        "variant_id": variant_id,
        "model_calls": 0,
        "provider_tokens": 0,
        "prediction_path": str(prediction_path),
        "prediction_sha256": prediction_sha256,
        "candidate_count": len(predictions["items"]),
        "episode_ids": sorted(predictions_by_episode),
        "source_packets": loaded["source_packets"],
        "gold_binding": gold_binding,
        "scopes": scopes,
    }
    report["report_sha256"] = true_north.sha256_text(
        true_north.dumps_json(report)
    )
    report_path = output_root / "score.private.json"
    true_north._write_json(report_path, report, immutable=True)
    return report


def rescore_prior_adoption_floor(
    suite_root: str | Path,
    prediction_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Re-score the immutable prior predictions after an approved gold revision."""

    root = Path(suite_root).expanduser().resolve()
    predictions_path = Path(prediction_path).expanduser().resolve()
    predictions = _read(predictions_path)
    manifest = _read(root / "manifest.json")
    candidate_by_id: dict[str, dict[str, Any]] = {}
    speaker_maps: dict[str, Any] = {}
    for bundle_row in manifest["bundles"]:
        if bundle_row["partition"] != "development":
            continue
        bundle = _read(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            candidate_id = str(candidate["candidate_id"])
            candidate_by_id[candidate_id] = {
                **candidate,
                "_source_episode_id": str(bundle_row["episode_id"]),
            }
            speaker_maps[candidate_id] = bundle["episode_context"].get(
                "speaker_map", []
            )
    prediction_ids = {
        str(item["candidate_id"]) for item in predictions["items"]
    }
    if not prediction_ids <= set(candidate_by_id):
        raise PriorAdoptionError(
            "prior predictions are outside the current development manifest"
        )
    consensus_document = _read(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred_document = _read(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    if (
        consensus_document["source_gold_sha256"]
        != preferred_document["gold_sha256"]
    ):
        raise PriorAdoptionError(
            "approved consensus is not bound to approved gold"
        )
    by_episode: dict[str, list[dict[str, Any]]] = {}
    for item in predictions["items"]:
        candidate_id = str(item["candidate_id"])
        episode_id = str(
            candidate_by_id[candidate_id]["_source_episode_id"]
        )
        by_episode.setdefault(episode_id, []).append(item)
    scopes = {
        episode_id: _score_scope(
            items,
            consensus_document=consensus_document,
            preferred_document=preferred_document,
            speaker_maps=speaker_maps,
        )
        for episode_id, items in sorted(by_episode.items())
    }
    scopes["comparison_cohort"] = _score_scope(
        predictions["items"],
        consensus_document=consensus_document,
        preferred_document=preferred_document,
        speaker_maps=speaker_maps,
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "experiment": "approved_policy_prior_floor_rescore",
        "model_calls": 0,
        "prediction_path": str(predictions_path),
        "prediction_sha256": true_north._sha256_file(predictions_path),
        "manifest_sha256": manifest["manifest_sha256"],
        "gold_sha256": preferred_document["gold_sha256"],
        "consensus_sha256": consensus_document["consensus_sha256"],
        "gate_policy_version": true_north.APPROVED_GATE_POLICY_VERSION,
        "candidate_count": len(predictions["items"]),
        "scopes": scopes,
    }
    report["report_sha256"] = true_north.sha256_text(
        true_north.dumps_json(report)
    )
    destination = Path(output_dir).expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    true_north._write_json(
        destination / "score.private.json", report, immutable=True
    )
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the zero-call True North prior-adoption floor."
    )
    parser.add_argument("--campaign-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--variant-id", default=DEFAULT_VARIANT_ID)
    args = parser.parse_args(argv)
    report = run_prior_adoption_floor(
        args.campaign_dir,
        args.output_dir,
        variant_id=args.variant_id,
    )
    public = {
        "schema_version": report["schema_version"],
        "model_calls": report["model_calls"],
        "candidate_count": report["candidate_count"],
        "episode_ids": report["episode_ids"],
        "report_sha256": report["report_sha256"],
        "aggregate_by_scope": {
            scope: value["aggregate"]
            for scope, value in report["scopes"].items()
        },
        "gate_by_scope": {
            scope: value["gate"] for scope, value in report["scopes"].items()
        },
    }
    print(json.dumps(public, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
