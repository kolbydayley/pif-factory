"""Zero-call rescoring of frozen True-North predictions under contract v6."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from . import true_north
from .true_north_actor_span_rule import apply_actor_span_rule
from .true_north_decoupled_contract import CONTRACT_VERSION
from .true_north_semantic_scoring import (
    SCHEMA_VERSION as SCORER_VERSION,
    score_campaign,
)


RESCORE_VERSION = "pif_true_north_decoupled_rescore_v1"
TASK5_RUN_ID = "task5-adjudication-20260729-v1"
SPLIT_DEFAULT_RUN_ID = "task5-input-split-default-20260729-v1"
ACTOR_TWO_STAGE_RUN_ID = "task6-actor-two-stage-20260729-v1"
PRIOR_FLOOR_RELATIVE = (
    "prior-adoption/pif_true_north_prior_adoption_v1/"
    "tnpa-20260728-stack03-v1/predictions.private.json"
)


class DecoupledRescoreError(RuntimeError):
    """Raised when a frozen lane cannot be reproduced exactly."""


def _prediction_paths(
    root: Path,
    relative_glob: str,
) -> list[Path]:
    paths = sorted(root.glob(relative_glob))
    if not paths:
        raise DecoupledRescoreError(
            f"frozen prediction scope is empty: {relative_glob}"
        )
    return paths


def _load_predictions(paths: Sequence[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        document = true_north._read_json(path)
        rows.extend(copy.deepcopy(document["items"]))
    ids = [str(row["candidate_id"]) for row in rows]
    if len(ids) != len(set(ids)):
        raise DecoupledRescoreError(
            "frozen prediction scope contains duplicate candidates"
        )
    return rows


def _speaker_maps(
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for row in manifest["bundles"]:
        if row["partition"] != "development":
            continue
        bundle = true_north._read_json(Path(row["bundle_path"]))
        speaker_map = bundle["episode_context"].get("speaker_map", [])
        for candidate in bundle["candidates"]:
            result[str(candidate["candidate_id"])] = speaker_map
    return result


def _gate_table(aggregate: Mapping[str, Any]) -> list[dict[str, Any]]:
    diagnostics = aggregate["coupled_diagnostics"]
    rows: list[dict[str, Any]] = []
    for metric, (
        comparison,
        threshold,
    ) in true_north.APPROVED_GATE_POLICY.items():
        result = float(aggregate[metric])
        passed = (
            result >= threshold
            if comparison == ">="
            else result <= threshold
        )
        diagnostic = None
        if metric in {
            "speaker_exactness",
            "reported_actor_exactness",
            "claim_text_faithfulness_proxy",
        }:
            diagnostic = diagnostics[metric]
        rows.append(
            {
                "metric": metric,
                "result": result,
                "comparison": comparison,
                "gate": threshold,
                "passed": passed,
                "coupled_diagnostic": diagnostic,
            }
        )
    return rows


def score_predictions(
    *,
    suite_root: str | Path,
    lane_id: str,
    predictions: Sequence[Mapping[str, Any]],
    source_paths: Sequence[Path],
    actor_span_applied: bool = False,
    actor_span_report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    manifest = true_north._read_json(root / "manifest.json")
    contract = manifest.get("measurement_contract", {})
    if contract.get("version") != CONTRACT_VERSION:
        raise DecoupledRescoreError(
            "suite is not on the decoupled measurement contract"
        )
    consensus = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    ids = {str(row["candidate_id"]) for row in predictions}
    score = score_campaign(
        predictions,
        consensus["items"],
        preferred["items"],
        subset_candidate_ids=ids,
        speaker_maps_by_candidate=_speaker_maps(manifest),
    )
    score["aggregate"]["schema_parse_success_rate"] = 1.0
    core = true_north._consensus_atomic_metrics(
        consensus,
        {
            str(row["candidate_id"]): row for row in predictions
        },
        require_complete_scope=False,
    )
    score["aggregate"].update(
        {str(row["metric"]): row["value"] for row in core}
    )
    table = _gate_table(score["aggregate"])
    document = {
        "schema_version": RESCORE_VERSION,
        "scorer_version": SCORER_VERSION,
        "measurement_contract_version": CONTRACT_VERSION,
        "measurement_contract_sha256": contract["contract_sha256"],
        "manifest_sha256": manifest["manifest_sha256"],
        "lane_id": lane_id,
        "candidate_count": len(predictions),
        "source_artifacts": [
            {
                "path": str(path),
                "sha256": true_north._sha256_file(path),
            }
            for path in source_paths
        ],
        "actor_span_applied": actor_span_applied,
        "actor_span_report": dict(actor_span_report or {}),
        "aggregate": score["aggregate"],
        "nine_gate_table": table,
        "passed_gate_count": sum(row["passed"] for row in table),
        "passed": all(row["passed"] for row in table),
        "private_candidate_scores": score["candidates"],
        "provider_calls": 0,
        "holdout_opened": False,
        "production_mutation": False,
    }
    document["rescore_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    output = (
        root
        / "rescoring"
        / "decoupled-v2"
        / f"{lane_id}.private.json"
    )
    true_north._write_json(output, document, immutable=False)
    return {
        **{
            key: value
            for key, value in document.items()
            if key != "private_candidate_scores"
        },
        "output_path": str(output),
    }


def _apply_actor_span(
    predictions: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    output: list[dict[str, Any]] = []
    aggregate = {
        "total": 0,
        "kept": 0,
        "already_null": 0,
        "nulled_absent_span": 0,
        "nulled_speaker_self": 0,
    }
    for row in predictions:
        resolved = apply_actor_span_rule(row["atomic_claims"])
        updated = copy.deepcopy(row)
        updated["atomic_claims"] = list(resolved.atomics)
        output.append(updated)
        for key, value in resolved.report.items():
            aggregate[key] += int(value)
    return output, aggregate


def rescore_frozen_lanes(
    *,
    suite_root: str | Path,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    task5_paths = _prediction_paths(
        root,
        (
            f"multipass/runs/{TASK5_RUN_ID}/outputs/composed/"
            "*/*/validated.private.json"
        ),
    )
    split_paths = _prediction_paths(
        root,
        (
            f"multipass/runs/{SPLIT_DEFAULT_RUN_ID}/outputs/"
            "composed-partial-fallback/*/*/validated.private.json"
        ),
    )
    actor_paths = _prediction_paths(
        root,
        (
            f"multipass/runs/{ACTOR_TWO_STAGE_RUN_ID}/outputs/"
            "composed-partial-null-fallback/*/*/validated.private.json"
        ),
    )
    prior_path = root / PRIOR_FLOOR_RELATIVE
    if not prior_path.is_file():
        raise DecoupledRescoreError(
            "prior-floor prediction artifact is missing"
        )
    lanes = {
        "task5-adjudication": (task5_paths, _load_predictions(task5_paths)),
        "split-default-partial": (
            split_paths,
            _load_predictions(split_paths),
        ),
        "actor-two-stage-partial": (
            actor_paths,
            _load_predictions(actor_paths),
        ),
        "prior-floor": ([prior_path], _load_predictions([prior_path])),
    }
    results = {
        lane: score_predictions(
            suite_root=root,
            lane_id=lane,
            predictions=predictions,
            source_paths=paths,
        )
        for lane, (paths, predictions) in lanes.items()
    }
    task5_predictions = lanes["task5-adjudication"][1]
    span_predictions, span_report = _apply_actor_span(task5_predictions)
    span_path = (
        root
        / "rescoring"
        / "decoupled-v2"
        / "task5-actor-span-predictions.private.json"
    )
    span_document = {
        "schema_version": true_north.WORK_OUTPUT_SCHEMA_VERSION,
        "source_lane": "task5-adjudication",
        "actor_span_rule": "deterministic_actor_span_rule_v1",
        "items": span_predictions,
    }
    span_document["predictions_sha256"] = true_north.sha256_text(
        true_north.dumps_json(span_document)
    )
    true_north._write_json(span_path, span_document, immutable=False)
    results["task5-actor-span-baseline"] = score_predictions(
        suite_root=root,
        lane_id="task5-actor-span-baseline",
        predictions=span_predictions,
        source_paths=[*task5_paths, span_path],
        actor_span_applied=True,
        actor_span_report=span_report,
    )
    index = {
        "schema_version": RESCORE_VERSION,
        "lanes": {
            lane: {
                "rescore_sha256": result["rescore_sha256"],
                "output_path": result["output_path"],
                "passed_gate_count": result["passed_gate_count"],
            }
            for lane, result in results.items()
        },
        "provider_calls": 0,
        "holdout_opened": False,
        "production_mutation": False,
    }
    index["index_sha256"] = true_north.sha256_text(
        true_north.dumps_json(index)
    )
    path = root / "rescoring" / "decoupled-v2" / "index.json"
    true_north._write_json(path, index, immutable=False)
    return {**index, "index_path": str(path), "results": results}
