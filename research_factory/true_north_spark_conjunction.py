"""Bounded Spark measurement for conjunction-dense Stage-B candidates."""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from .true_north_dual_decomposition import (
    REFERENCE_RUN_ID,
    SEARCH_EPISODE_IDS,
    SPARK_MODEL,
    _load_search_context,
    _reference_artifacts,
)


SCHEMA_VERSION = "pif_true_north_spark_conjunction_v1"
MAX_CALLS = 12
MAX_TOKENS = 250_000
MAX_WALL_SECONDS = 2 * 60 * 60
MAX_BATCH_CANDIDATES = 15
RESERVED_TOKENS_PER_CALL = 27_000
CAMPAIGN_CALLS_BEFORE_RUN = 139
CAMPAIGN_CALL_CEILING = 160


class SparkConjunctionError(RuntimeError):
    """Raised when the bounded conjunction measurement is invalid."""


def has_conjunction(claim_text: str) -> bool:
    """Reproduce the review selector exactly, including its spacing rule."""

    text = str(claim_text).casefold()
    return " and " in text or " or " in text


def _source_packet(
    base_job: Mapping[str, Any],
    disposition: Mapping[str, Any],
    selected_ids: set[str],
) -> dict[str, Any]:
    decisions = {
        str(row["candidate_id"]): row
        for row in disposition["items"]
    }
    candidates = [
        true_north._adjudication_candidate_projection(row)
        for row in base_job["input"]["candidates"]
        if str(row["candidate_id"]) in selected_ids
    ]
    candidate_ids = [str(row["candidate_id"]) for row in candidates]
    if not candidate_ids:
        raise SparkConjunctionError("source packet selection is empty")
    return {
        "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
        "suite_id": true_north.SUITE_ID,
        "multipass_stage": "adjudication",
        "task": "Adopt, split, or minimally repair each proposed claim.",
        "instructions": [
            "Do not change disposition or attribute speakers.",
            "Adopt proposed_claim_text verbatim unless the evidence forces a change.",
            "split=false requires exactly one atomic claim; split=true requires two or more.",
            "edit_reason=none means the single claim byte-matches proposed_claim_text.",
        ],
        "output_schema": true_north.multipass_adjudication_schema(
            candidate_ids
        ),
        "input": {
            "episode": copy.deepcopy(base_job["input"]["episode"]),
            "segment": copy.deepcopy(base_job["input"]["segment"]),
            "candidates": candidates,
            "stage_a_decisions": [
                decisions[candidate_id]
                for candidate_id in candidate_ids
            ],
        },
    }


def _batch_source_packets(
    source_packets: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    batches: list[list[Mapping[str, Any]]] = []
    current: list[Mapping[str, Any]] = []
    current_count = 0
    for packet in source_packets:
        count = len(packet["input"]["candidates"])
        if current and current_count + count > MAX_BATCH_CANDIDATES:
            batches.append(current)
            current = []
            current_count = 0
        current.append(packet)
        current_count += count
    if current:
        batches.append(current)
    result: list[dict[str, Any]] = []
    for batch_index, packets in enumerate(batches):
        candidate_ids = [
            str(candidate["candidate_id"])
            for packet in packets
            for candidate in packet["input"]["candidates"]
        ]
        result.append(
            {
                "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
                "suite_id": true_north.SUITE_ID,
                "multipass_stage": "adjudication",
                "task": (
                    "Apply the frozen Stage-B contract independently to "
                    "every candidate in the enclosed source packets."
                ),
                "instructions": copy.deepcopy(
                    packets[0]["instructions"]
                ),
                "output_schema": (
                    true_north.multipass_adjudication_schema(
                        candidate_ids
                    )
                ),
                "input": {
                    "batch_id": f"conjunction-{batch_index:03d}",
                    "source_packets": copy.deepcopy(list(packets)),
                },
            }
        )
    return result


def validate_conjunction_output(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    candidates = [
        candidate
        for source in packet["input"]["source_packets"]
        for candidate in source["input"]["candidates"]
    ]
    surrogate = {
        "output_schema": packet["output_schema"],
        "input": {"candidates": candidates},
    }
    true_north.validate_multipass_adjudication(output, surrogate)


def _spark_items(
    outputs: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for output in outputs.values():
        for row in output["items"]:
            candidate_id = str(row["candidate_id"])
            if candidate_id in result:
                raise SparkConjunctionError(
                    "duplicate Spark candidate output"
                )
            result[candidate_id] = dict(row)
    return result


def run_spark_conjunction_measurement(
    *,
    suite_root: str | Path,
    run_id: str | None = None,
    workers: int = 4,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    runner: Any | None = None,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise SparkConjunctionError(
            "suite verification failed before Spark measurement"
        )
    manifest = true_north._read_json(root / "manifest.json")
    base_jobs, dispositions, candidates = _load_search_context(
        root, manifest
    )
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        reference_provenance,
    ) = _reference_artifacts(root)
    candidate_to_key: dict[str, tuple[str, str]] = {}
    for key, job in base_jobs.items():
        for candidate in job["input"]["candidates"]:
            candidate_to_key[str(candidate["candidate_id"])] = key
    selected_ids = sorted(
        candidate_id
        for candidate_id, candidate in candidates.items()
        if has_conjunction(str(candidate.get("claim_text") or ""))
    )
    if len(candidates) != 256 or len(selected_ids) != 96:
        raise SparkConjunctionError(
            "conjunction selector does not reproduce 96 of 256 candidates"
        )
    selected_set = set(selected_ids)
    source_packets = [
        _source_packet(
            base_jobs[key],
            dispositions[key],
            {
                candidate_id
                for candidate_id in selected_set
                if candidate_to_key[candidate_id] == key
            },
        )
        for key in sorted(base_jobs)
        if any(
            candidate_id in selected_set
            and candidate_to_key[candidate_id] == key
            for candidate_id in selected_ids
        )
    ]
    batches = _batch_source_packets(source_packets)
    if len(batches) > MAX_CALLS:
        raise SparkConjunctionError(
            "conjunction batches exceed the 12-call ceiling"
        )
    eligible_ids = {
        str(candidate["candidate_id"])
        for packet in reference_packets.values()
        for candidate in packet["input"]["candidates"]
    }
    selected_eligible_ids = selected_set & eligible_ids

    resolved_run_id = run_id or (
        "task5-spark-conjunction-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    run_root = root / "multipass" / "runs" / resolved_run_id
    budget = {
        "max_calls": MAX_CALLS,
        "max_tokens": MAX_TOKENS,
        "max_wall_seconds": MAX_WALL_SECONDS,
    }
    configuration = {
        "schema_version": SCHEMA_VERSION,
        "suite_id": root.name,
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "run_id": resolved_run_id,
        "episode_ids": list(SEARCH_EPISODE_IDS),
        "reference": reference_provenance,
        "model": SPARK_MODEL,
        "selector": {
            "rule": (
                "casefolded claim_text contains literal ' and ' or ' or '"
            ),
            "candidate_count": 96,
            "eligible_decomposition_candidate_count": len(
                selected_eligible_ids
            ),
            "candidate_ids": selected_ids,
        },
        "packet_count": len(batches),
        "max_batch_candidates": MAX_BATCH_CANDIDATES,
        "system_prompt_sha256": true_north.sha256_text(
            true_north.MULTIPASS_SYSTEM_PROMPTS["adjudication"]
        ),
        "budget": budget,
        "campaign_calls_before_run": CAMPAIGN_CALLS_BEFORE_RUN,
        "campaign_call_ceiling": CAMPAIGN_CALL_CEILING,
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
        "prompt_tuning_allowed": False,
    }
    configuration["configuration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(configuration)
    )
    config_path = run_root / "configuration.json"
    state_path = run_root / "state.json"
    if config_path.is_file():
        if true_north._read_json(config_path) != configuration:
            raise SparkConjunctionError(
                "resume configuration differs from declared Spark run"
            )
        state = true_north._read_json(state_path)
    else:
        true_north._write_json(
            config_path, configuration, immutable=True
        )
        state = {
            "schema_version": SCHEMA_VERSION,
            "run_id": resolved_run_id,
            "configuration_sha256": configuration[
                "configuration_sha256"
            ],
            "budget": budget,
            "completed": [],
            "usage": {
                "calls": 0,
                "tokens": 0,
                "wall_seconds": 0.0,
            },
            "complete": False,
        }
        true_north._multipass_state_write(state_path, state)
    jobs = [
        ("search", f"batch-{index:03d}", packet)
        for index, packet in enumerate(batches)
    ]
    spark_outputs = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="adjudication",
        artifact_stage="spark-conjunction",
        jobs=jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=SPARK_MODEL,
        reserved_tokens_per_call=RESERVED_TOKENS_PER_CALL,
        validator_override=validate_conjunction_output,
    )
    spark_by_candidate = _spark_items(spark_outputs)
    if set(spark_by_candidate) != selected_set:
        raise SparkConjunctionError(
            "Spark output does not cover exactly 96 selected candidates"
        )

    final_adjudication: dict[
        tuple[str, str], dict[str, Any]
    ] = {}
    for key, reference in reference_outputs.items():
        items = []
        for row in reference["items"]:
            candidate_id = str(row["candidate_id"])
            items.append(
                copy.deepcopy(
                    spark_by_candidate[candidate_id]
                    if candidate_id in selected_eligible_ids
                    else row
                )
            )
        output = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": items,
        }
        true_north.validate_multipass_adjudication(
            output, reference_packets[key]
        )
        final_adjudication[key] = output
        true_north._write_json(
            run_root
            / "outputs"
            / "adjudication-final"
            / key[0]
            / key[1]
            / "validated.private.json",
            output,
            immutable=True,
        )
    for key in sorted(base_jobs):
        output = true_north.compose_multipass_output(
            base_jobs[key],
            dispositions[key],
            final_adjudication.get(key),
            None,
            stage_b_mode="adjudication",
        )
        true_north._write_json(
            run_root
            / "outputs"
            / "composed"
            / key[0]
            / key[1]
            / "validated.private.json",
            output,
            immutable=True,
        )
    state["complete"] = True
    state["candidate_count"] = len(candidates)
    state["selected_candidate_count"] = len(selected_set)
    state["selected_eligible_candidate_count"] = len(
        selected_eligible_ids
    )
    true_north._multipass_state_write(state_path, state)
    result = {
        "schema_version": SCHEMA_VERSION,
        "run_id": resolved_run_id,
        "configuration_sha256": configuration[
            "configuration_sha256"
        ],
        "complete": True,
        "selected_candidate_count": len(selected_set),
        "selected_eligible_candidate_count": len(
            selected_eligible_ids
        ),
        "packet_count": len(batches),
        "usage": state["usage"],
        "campaign_calls_after_run": (
            CAMPAIGN_CALLS_BEFORE_RUN
            + int(state["usage"]["calls"])
        ),
        "holdout_opened": False,
        "production_mutation": False,
        "prompt_tuned": False,
    }
    result["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    true_north._write_json(
        run_root / "result.json", result, immutable=False
    )
    return {**result, "run_root": str(run_root)}


def _value_state(disposition: str) -> str:
    return (
        "value"
        if disposition in {"retain", "revise"}
        else ("junk" if disposition == "reject" else "hold")
    )


def score_spark_conjunction_measurement(
    *,
    suite_root: str | Path,
    run_id: str,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    configuration = true_north._read_json(
        run_root / "configuration.json"
    )
    if configuration.get("schema_version") != SCHEMA_VERSION:
        raise SparkConjunctionError(
            "run is not a Spark conjunction measurement"
        )
    full = true_north.score_multipass_run(
        run_id=run_id,
        output_root=root.parent,
        suite=root.name,
    )
    selected = set(configuration["selector"]["candidate_ids"])
    predictions: dict[str, dict[str, Any]] = {}
    for episode_id in configuration["episode_ids"]:
        for path in (
            run_root / "outputs" / "composed" / episode_id
        ).glob("*/validated.private.json"):
            for row in true_north._read_json(path)["items"]:
                candidate_id = str(row["candidate_id"])
                if candidate_id in selected:
                    predictions[candidate_id] = row
    consensus_document = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    core = true_north._consensus_atomic_metrics(
        {
            **consensus_document,
            "items": [
                row
                for row in consensus_document["items"]
                if str(row["candidate_id"]) in selected
            ],
        },
        predictions,
        require_complete_scope=True,
    )
    selected_metrics = {
        str(row["metric"]): row["value"] for row in core
    }
    reference = true_north._read_json(
        root
        / "multipass"
        / "runs"
        / REFERENCE_RUN_ID
        / "task5-score.private.json"
    )
    result = true_north._read_json(run_root / "result.json")
    reference_usage = reference["usage"]
    spark_usage = result["usage"]
    cost = {
        "all_glm_baseline": {
            "calls": int(reference_usage["calls"]),
            "tokens": int(reference_usage["tokens"]),
        },
        "measured_mixed_route": {
            "glm_calls": int(reference_usage["calls"]),
            "glm_tokens": int(reference_usage["tokens"]),
            "spark_calls": int(spark_usage["calls"]),
            "spark_tokens": int(spark_usage["tokens"]),
            "total_calls": (
                int(reference_usage["calls"])
                + int(spark_usage["calls"])
            ),
            "total_tokens": (
                int(reference_usage["tokens"])
                + int(spark_usage["tokens"])
            ),
        },
        "ratio_vs_all_glm": {
            "call_ratio": round(
                (
                    int(reference_usage["calls"])
                    + int(spark_usage["calls"])
                )
                / int(reference_usage["calls"]),
                6,
            ),
            "token_ratio": round(
                (
                    int(reference_usage["tokens"])
                    + int(spark_usage["tokens"])
                )
                / int(reference_usage["tokens"]),
                6,
            ),
        },
        "all_spark_call_baseline": int(reference_usage["calls"]),
        "ratio_vs_all_spark": {
            "frontier_call_ratio": round(
                int(spark_usage["calls"])
                / int(reference_usage["calls"]),
                6,
            ),
            "total_call_ratio": round(
                (
                    int(reference_usage["calls"])
                    + int(spark_usage["calls"])
                )
                / int(reference_usage["calls"]),
                6,
            ),
            "token_ratio_not_claimed": True,
        },
        "interpretation": (
            "Actual provider calls and tokens are reported. An all-Spark "
            "token or dollar ratio is not asserted without an all-Spark run "
            "or metered subscription prices."
        ),
    }
    aggregate = full["aggregate"]
    document = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "spark_subset": {
            "selected_candidate_count": len(selected),
            "selected_eligible_candidate_count": configuration[
                "selector"
            ]["eligible_decomposition_candidate_count"],
            "acceptable_atomic_count_rate": selected_metrics[
                "acceptable_atomic_count_rate"
            ],
        },
        "composed": {
            "acceptable_atomic_count_rate": aggregate[
                "acceptable_atomic_count_rate"
            ],
            "speaker_exactness": aggregate["speaker_exactness"],
            "claim_text_faithfulness": aggregate[
                "claim_text_faithfulness_proxy"
            ],
        },
        "single_pass_reference": {
            "acceptable_atomic_count_rate": reference["search_fold"][
                "atomic_count_accuracy"
            ],
            "speaker_exactness": reference["search_fold"][
                "aggregate"
            ]["speaker_exactness"],
            "claim_text_faithfulness": reference["search_fold"][
                "claim_text_faithfulness"
            ],
        },
        "cost": cost,
        "usage": spark_usage,
        "campaign_calls_after_run": result[
            "campaign_calls_after_run"
        ],
        "holdout_opened": False,
        "production_mutation": False,
        "prompt_tuned": False,
    }
    document["score_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = run_root / "spark-conjunction-score.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "score_path": str(path)}


def finalize_partial_spark_conjunction_measurement(
    *,
    suite_root: str | Path,
    run_id: str,
) -> dict[str, Any]:
    """Score validated Spark coverage and a labeled GLM-fallback composition."""

    from .true_north_semantic_scoring import score_campaign

    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    configuration = true_north._read_json(run_root / "configuration.json")
    state = true_north._read_json(run_root / "state.json")
    if configuration.get("schema_version") != SCHEMA_VERSION:
        raise SparkConjunctionError(
            "run is not a Spark conjunction measurement"
        )
    if state.get("complete") is True:
        raise SparkConjunctionError("complete Spark runs use the normal scorer")
    if int(state["usage"]["calls"]) != int(state["budget"]["max_calls"]):
        raise SparkConjunctionError("partial Spark run is not budget terminal")

    manifest = true_north._read_json(root / "manifest.json")
    base_jobs, dispositions, candidates = _load_search_context(root, manifest)
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        _reference_provenance,
    ) = _reference_artifacts(root)
    spark_outputs: dict[tuple[str, str], dict[str, Any]] = {}
    validated_batches: list[str] = []
    for path in sorted(
        (run_root / "outputs" / "spark-conjunction" / "search").glob(
            "*/validated.private.json"
        )
    ):
        batch_id = path.parent.name
        packet = true_north._read_json(
            run_root
            / "packets"
            / "spark-conjunction"
            / "search"
            / f"{batch_id}.private.json"
        )
        output = true_north._read_json(path)
        validate_conjunction_output(output, packet)
        validated_batches.append(batch_id)
        spark_outputs[("search", batch_id)] = output
    spark_by_candidate = _spark_items(spark_outputs)
    selected = set(configuration["selector"]["candidate_ids"])
    missing = sorted(selected - set(spark_by_candidate))
    if not missing:
        raise SparkConjunctionError("partial Spark finalizer found full coverage")

    disposition_by_candidate = {
        str(row["candidate_id"]): row
        for output in dispositions.values()
        for row in output["items"]
    }
    eligible = {
        candidate_id
        for candidate_id in selected
        if str(disposition_by_candidate[candidate_id]["disposition"])
        in {"retain", "revise"}
    }
    final_adjudication: dict[tuple[str, str], dict[str, Any]] = {}
    for key, reference in reference_outputs.items():
        items = []
        for row in reference["items"]:
            candidate_id = str(row["candidate_id"])
            items.append(
                copy.deepcopy(
                    spark_by_candidate[candidate_id]
                    if candidate_id in eligible
                    and candidate_id in spark_by_candidate
                    else row
                )
            )
        output = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": items,
        }
        true_north.validate_multipass_adjudication(
            output, reference_packets[key]
        )
        final_adjudication[key] = output

    fallback_predictions: list[dict[str, Any]] = []
    for key in sorted(base_jobs):
        output = true_north.compose_multipass_output(
            base_jobs[key],
            dispositions[key],
            final_adjudication.get(key),
            None,
            stage_b_mode="adjudication",
        )
        fallback_predictions.extend(output["items"])
        true_north._write_json(
            run_root
            / "outputs"
            / "composed-partial-fallback"
            / key[0]
            / key[1]
            / "validated.private.json",
            output,
            immutable=False,
        )

    consensus_document = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred_document = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    prediction_ids = {
        str(row["candidate_id"]) for row in fallback_predictions
    }
    consensus = [
        row for row in consensus_document["items"]
        if str(row["candidate_id"]) in prediction_ids
    ]
    preferred = [
        row for row in preferred_document["items"]
        if str(row["candidate_id"]) in prediction_ids
    ]
    speaker_maps: dict[str, Any] = {}
    for bundle_row in manifest["bundles"]:
        if str(bundle_row["episode_id"]) not in configuration["episode_ids"]:
            continue
        bundle = true_north._read_json(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            speaker_maps[str(candidate["candidate_id"])] = (
                bundle["episode_context"].get("speaker_map", [])
            )
    fallback_score = score_campaign(
        fallback_predictions,
        consensus,
        preferred,
        speaker_maps_by_candidate=speaker_maps,
    )
    fallback_core = true_north._consensus_atomic_metrics(
        {
            **consensus_document,
            "items": consensus,
        },
        {
            str(row["candidate_id"]): row
            for row in fallback_predictions
        },
        require_complete_scope=True,
    )
    fallback_metrics = {
        str(row["metric"]): row["value"] for row in fallback_core
    }
    validated_selected = set(spark_by_candidate)
    scored_spark_by_candidate = {
        candidate_id: {
            "disposition": disposition_by_candidate[candidate_id][
                "disposition"
            ],
            **row,
        }
        for candidate_id, row in spark_by_candidate.items()
    }
    subset_core = true_north._consensus_atomic_metrics(
        {
            **consensus_document,
            "items": [
                row for row in consensus_document["items"]
                if str(row["candidate_id"]) in validated_selected
            ],
        },
        scored_spark_by_candidate,
        require_complete_scope=True,
    )
    subset_metrics = {
        str(row["metric"]): row["value"] for row in subset_core
    }
    validated_eligible = validated_selected & eligible
    eligible_subset_core = true_north._consensus_atomic_metrics(
        {
            **consensus_document,
            "items": [
                row for row in consensus_document["items"]
                if str(row["candidate_id"]) in validated_eligible
            ],
        },
        {
            candidate_id: scored_spark_by_candidate[candidate_id]
            for candidate_id in validated_eligible
        },
        require_complete_scope=True,
    )
    eligible_subset_metrics = {
        str(row["metric"]): row["value"] for row in eligible_subset_core
    }
    reference_by_candidate = {
        str(row["candidate_id"]): row
        for output in reference_outputs.values()
        for row in output["items"]
    }
    reference_subset_core = true_north._consensus_atomic_metrics(
        {
            **consensus_document,
            "items": [
                row for row in consensus_document["items"]
                if str(row["candidate_id"]) in validated_selected
            ],
        },
        {
            candidate_id: {
                "disposition": disposition_by_candidate[candidate_id][
                    "disposition"
                ],
                **reference_by_candidate.get(
                    candidate_id,
                    {
                        "candidate_id": candidate_id,
                        "atomic_claims": [],
                    },
                ),
            }
            for candidate_id in validated_selected
        },
        require_complete_scope=True,
    )
    reference_subset_metrics = {
        str(row["metric"]): row["value"] for row in reference_subset_core
    }
    reference = true_north._read_json(
        root
        / "multipass"
        / "runs"
        / REFERENCE_RUN_ID
        / "task5-score.private.json"
    )
    reference_usage = reference["usage"]
    spark_usage = state["usage"]
    total_calls = int(reference_usage["calls"]) + int(spark_usage["calls"])
    total_tokens = int(reference_usage["tokens"]) + int(spark_usage["tokens"])
    document = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "complete": False,
        "acceptance_eligible": False,
        "terminal_reason": "workstream_call_ceiling_with_invalid_frozen_contract_batch",
        "coverage": {
            "validated_batch_count": len(validated_batches),
            "expected_batch_count": len(validated_batches) + 1,
            "validated_selected_candidate_count": len(validated_selected),
            "expected_selected_candidate_count": len(selected),
            "validated_selected_eligible_candidate_count": len(
                validated_selected & eligible
            ),
            "expected_selected_eligible_candidate_count": len(eligible),
            "coverage_rate": len(validated_selected) / len(selected),
            "missing_candidate_count": len(missing),
        },
        "validated_spark_subset": {
            "acceptable_atomic_count_rate": subset_metrics[
                "acceptable_atomic_count_rate"
            ],
            "eligible_only_acceptable_atomic_count_rate": (
                eligible_subset_metrics["acceptable_atomic_count_rate"]
            ),
            "same_candidates_glm_acceptable_atomic_count_rate": (
                reference_subset_metrics["acceptable_atomic_count_rate"]
            ),
        },
        "partial_fallback_composition": {
            "definition": (
                "validated Spark outputs on the measured conjunction subset; "
                "frozen single-pass GLM outputs for the invalid batch and "
                "unselected candidates"
            ),
            "acceptable_atomic_count_rate": fallback_metrics[
                "acceptable_atomic_count_rate"
            ],
            "speaker_exactness": fallback_score["aggregate"][
                "speaker_exactness"
            ],
            "claim_text_faithfulness": fallback_score["aggregate"][
                "claim_text_faithfulness_proxy"
            ],
        },
        "single_pass_reference": {
            "acceptable_atomic_count_rate": reference["search_fold"][
                "atomic_count_accuracy"
            ],
            "speaker_exactness": reference["search_fold"]["aggregate"][
                "speaker_exactness"
            ],
            "claim_text_faithfulness": reference["search_fold"][
                "claim_text_faithfulness"
            ],
        },
        "cost": {
            "all_glm_baseline": {
                "calls": int(reference_usage["calls"]),
                "tokens": int(reference_usage["tokens"]),
            },
            "measured_mixed_route": {
                "glm_calls": int(reference_usage["calls"]),
                "glm_tokens": int(reference_usage["tokens"]),
                "spark_calls": int(spark_usage["calls"]),
                "spark_tokens": int(spark_usage["tokens"]),
                "total_calls": total_calls,
                "total_tokens": total_tokens,
            },
            "ratio_vs_all_glm": {
                "call_ratio": round(
                    total_calls / int(reference_usage["calls"]), 6
                ),
                "token_ratio": round(
                    total_tokens / int(reference_usage["tokens"]), 6
                ),
            },
            "all_spark_call_baseline": int(reference_usage["calls"]),
            "ratio_vs_all_spark": {
                "frontier_call_ratio": round(
                    int(spark_usage["calls"])
                    / int(reference_usage["calls"]),
                    6,
                ),
                "total_call_ratio": round(
                    total_calls / int(reference_usage["calls"]), 6
                ),
                "token_ratio_not_claimed": True,
            },
            "interpretation": (
                "Calls and tokens are actual. No all-Spark token or dollar "
                "ratio is claimed without an all-Spark run or metered prices."
            ),
        },
        "usage": spark_usage,
        "campaign_calls_after_run": (
            int(configuration["campaign_calls_before_run"])
            + int(spark_usage["calls"])
        ),
        "holdout_opened": False,
        "production_mutation": False,
        "prompt_tuned": False,
    }
    document["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = run_root / "terminal-partial-result.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "result_path": str(path)}
