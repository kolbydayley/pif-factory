"""Bounded Spark probe over the frozen split-default semantic packets."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from .true_north_decoupled_contract import CONTRACT_VERSION
from .true_north_decoupled_rescore import (
    _apply_actor_span,
    score_predictions,
)
from .true_north_dual_decomposition import (
    REFERENCE_RUN_ID,
    SEARCH_EPISODE_IDS,
    SPARK_MODEL,
    _load_search_context,
    _reference_artifacts,
)
from .true_north_input_split_default import (
    FLAGGED_SYSTEM_PROMPT,
    merge_adjudication_schema,
    validate_merge_adjudication,
)
from .true_north_semantic_scoring import score_campaign


SCHEMA_VERSION = "pif_true_north_spark_split_default_v1"
EXPERIMENT_ID = "stage-b-spark-input-split-default-20260729-v1"
SOURCE_RUN_ID = "task5-input-split-default-20260729-v1"
MAX_CALLS = 14
MAX_TOKENS = 250_000
MAX_WALL_SECONDS = 2 * 60 * 60
RESERVED_TOKENS_PER_CALL = 17_000
CUMULATIVE_CALLS_BEFORE_RUN = 236
KNOWN_CUMULATIVE_TOKENS_BEFORE_RUN = 2_017_646
LEDGER_PATH = (
    Path(__file__).resolve().parent.parent
    / "config"
    / "true_north_campaign_budget_ledger.json"
)


class SparkSplitDefaultError(RuntimeError):
    """Raised when the bounded Spark probe violates its frozen contract."""


def _source_packets(
    suite_root: Path,
) -> list[dict[str, Any]]:
    run_root = suite_root / "multipass" / "runs" / SOURCE_RUN_ID
    source_configuration = true_north._read_json(
        run_root / "configuration.json"
    )
    expected_hashes = source_configuration["input_contract"][
        "flagged_packet_hashes"
    ]
    packets: list[dict[str, Any]] = []
    for path in sorted(
        (run_root / "packets" / "input-split-default").glob(
            "*/*.private.json"
        )
    ):
        packet = true_north._read_json(path)
        key = f"{path.parent.name}/{path.stem.removesuffix('.private')}"
        digest = true_north.sha256_text(true_north.dumps_json(packet))
        if expected_hashes.get(key) != digest:
            raise SparkSplitDefaultError(
                f"source split-default packet hash drift: {key}"
            )
        packets.append(
            {
                "packet_key": key,
                "packet_sha256": digest,
                "candidate_count": len(packet["input"]["candidates"]),
                "packet": packet,
            }
        )
    if len(packets) != 19 or len(expected_hashes) != 19:
        raise SparkSplitDefaultError(
            "Spark probe requires exactly 19 frozen semantic packets"
        )
    return packets


def pack_provider_envelopes(
    source_packets: Sequence[Mapping[str, Any]],
) -> list[list[dict[str, Any]]]:
    """Reduce 19 immutable packets to 14 calls without changing a packet."""

    by_episode: dict[str, list[dict[str, Any]]] = {}
    for row in source_packets:
        episode_id = str(row["packet_key"]).split("/", 1)[0]
        by_episode.setdefault(episode_id, []).append(copy.deepcopy(row))
    envelopes: list[list[dict[str, Any]]] = []
    pair_budget = len(source_packets) - MAX_CALLS
    episode_order = sorted(
        by_episode,
        key=lambda episode_id: (
            sum(
                int(row["candidate_count"])
                for row in by_episode[episode_id]
            ),
            episode_id,
        ),
    )
    for episode_id in episode_order:
        rows = sorted(
            by_episode[episode_id],
            key=lambda row: (
                int(row["candidate_count"]),
                str(row["packet_key"]),
            ),
        )
        pairs_here = min(pair_budget, len(rows) // 2)
        while pairs_here:
            envelopes.append([rows.pop(0), rows.pop(0)])
            pair_budget -= 1
            pairs_here -= 1
        envelopes.extend([[row] for row in rows])
    if pair_budget != 0 or len(envelopes) != MAX_CALLS:
        raise SparkSplitDefaultError(
            "deterministic packet packing did not produce 14 envelopes"
        )
    envelopes.sort(
        key=lambda rows: tuple(str(row["packet_key"]) for row in rows)
    )
    return envelopes


def _provider_packet(
    envelope_id: str,
    source_packets: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_ids = [
        str(candidate["candidate_id"])
        for row in source_packets
        for candidate in row["packet"]["input"]["candidates"]
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "suite_id": true_north.SUITE_ID,
        "multipass_stage": "adjudication",
        "task": (
            "Apply the frozen split-default contract independently to every "
            "candidate in each enclosed immutable semantic packet."
        ),
        "instructions": [
            "Each source_packets[].packet is an immutable semantic packet.",
            "Do not combine context or candidates across source packets.",
            "Return one flat candidate result set covering every packet.",
            "Every candidate remains governed by its own proposed_decomposition.",
        ],
        "output_schema": merge_adjudication_schema(candidate_ids),
        "input": {
            "provider_envelope_id": envelope_id,
            "source_packets": copy.deepcopy(list(source_packets)),
        },
    }


def validate_provider_output(
    output: Mapping[str, Any],
    provider_packet: Mapping[str, Any],
) -> None:
    output_by_id = {
        str(row["candidate_id"]): row for row in output.get("items", [])
    }
    expected: set[str] = set()
    for source in provider_packet["input"]["source_packets"]:
        packet = source["packet"]
        ids = {
            str(row["candidate_id"])
            for row in packet["input"]["candidates"]
        }
        expected.update(ids)
        if not ids <= set(output_by_id):
            raise SparkSplitDefaultError(
                "provider envelope omitted a source packet candidate"
            )
        subset = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                output_by_id[candidate_id]
                for candidate_id in sorted(ids)
            ],
        }
        validate_merge_adjudication(subset, packet)
    if set(output_by_id) != expected:
        raise SparkSplitDefaultError(
            "provider envelope emitted an out-of-scope candidate"
        )


def _ledger() -> tuple[dict[str, Any], str]:
    ledger = true_north._read_json(LEDGER_PATH)
    row = next(
        (
            item
            for item in ledger["declared_experiments"]
            if item["experiment_id"] == EXPERIMENT_ID
        ),
        None,
    )
    if (
        row is None
        or row.get("status") != "declared"
        or row.get("model") != SPARK_MODEL
        or int(row["max_calls"]) != MAX_CALLS
        or int(row["max_tokens"]) != MAX_TOKENS
    ):
        raise SparkSplitDefaultError(
            "Spark probe budget is not declared exactly"
        )
    return ledger, true_north.sha256_text(
        true_north.dumps_json(ledger)
    )


def run_spark_split_default_probe(
    *,
    suite_root: str | Path,
    run_id: str,
    workers: int = 3,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    runner: Any | None = None,
) -> dict[str, Any]:
    if workers < 1 or workers > 3:
        raise SparkSplitDefaultError("workers must be between 1 and 3")
    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise SparkSplitDefaultError(
            "suite verification failed before Spark probe"
        )
    manifest = true_north._read_json(root / "manifest.json")
    if manifest["measurement_contract"]["version"] != CONTRACT_VERSION:
        raise SparkSplitDefaultError(
            "Spark probe requires the decoupled measurement contract"
        )
    ledger, ledger_sha = _ledger()
    source_packets = _source_packets(root)
    envelopes = pack_provider_envelopes(source_packets)
    provider_packets = [
        _provider_packet(f"spark-split-{index:03d}", rows)
        for index, rows in enumerate(envelopes)
    ]
    run_root = root / "multipass" / "runs" / run_id
    snapshot_path = (
        root
        / "multipass"
        / "budget-ledger"
        / f"{EXPERIMENT_ID}.json"
    )
    true_north._write_json(snapshot_path, ledger, immutable=True)
    configuration = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "suite_id": root.name,
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "measurement_contract_sha256": manifest[
            "measurement_contract"
        ]["contract_sha256"],
        "run_id": run_id,
        "episode_ids": list(SEARCH_EPISODE_IDS),
        "model": SPARK_MODEL,
        "source_run_id": SOURCE_RUN_ID,
        "source_semantic_packet_count": len(source_packets),
        "source_semantic_packet_hashes": {
            str(row["packet_key"]): str(row["packet_sha256"])
            for row in source_packets
        },
        "provider_envelope_count": len(provider_packets),
        "provider_envelope_hashes": [
            true_north.sha256_text(true_north.dumps_json(packet))
            for packet in provider_packets
        ],
        "batching_contract": (
            "19 immutable semantic packets packed into 14 provider "
            "envelopes; every output revalidated per original packet"
        ),
        "system_prompt_sha256": true_north.sha256_text(
            FLAGGED_SYSTEM_PROMPT
        ),
        "unflagged_source_run_id": REFERENCE_RUN_ID,
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
            "max_wall_seconds": MAX_WALL_SECONDS,
        },
        "budget_ledger_sha256": ledger_sha,
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(configuration)
    )
    config_path = run_root / "configuration.json"
    state_path = run_root / "state.json"
    if config_path.is_file():
        if true_north._read_json(config_path) != configuration:
            raise SparkSplitDefaultError("resume configuration drift")
        state = true_north._read_json(state_path)
    else:
        true_north._write_json(
            config_path, configuration, immutable=True
        )
        state = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "configuration_sha256": configuration[
                "configuration_sha256"
            ],
            "budget": copy.deepcopy(configuration["budget"]),
            "completed": [],
            "usage": {
                "calls": 0,
                "tokens": 0,
                "wall_seconds": 0.0,
            },
            "complete": False,
        }
        true_north._multipass_state_write(state_path, state)
    outputs = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="adjudication",
        artifact_stage="spark-input-split-default",
        jobs=[
            ("search", f"envelope-{index:03d}", packet)
            for index, packet in enumerate(provider_packets)
        ],
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=SPARK_MODEL,
        reserved_tokens_per_call=RESERVED_TOKENS_PER_CALL,
        validator_override=validate_provider_output,
        system_prompt_override=FLAGGED_SYSTEM_PROMPT,
    )
    spark_by_candidate = {
        str(row["candidate_id"]): {
            key: value
            for key, value in row.items()
            if key != "merge_reason"
        }
        for output in outputs.values()
        for row in output["items"]
    }
    flagged_ids = {
        str(candidate["candidate_id"])
        for source in source_packets
        for candidate in source["packet"]["input"]["candidates"]
    }
    if set(spark_by_candidate) != flagged_ids:
        raise SparkSplitDefaultError(
            "Spark output does not cover all flagged candidates"
        )
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        _reference_provenance,
    ) = _reference_artifacts(root)
    base_jobs, dispositions, candidates = _load_search_context(
        root, manifest
    )
    final_adjudication: dict[tuple[str, str], dict[str, Any]] = {}
    for key, reference in reference_outputs.items():
        output = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                copy.deepcopy(
                    spark_by_candidate.get(
                        str(row["candidate_id"]), row
                    )
                )
                for row in reference["items"]
            ],
        }
        true_north.validate_multipass_adjudication(
            output, reference_packets[key]
        )
        final_adjudication[key] = output
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
    state["flagged_candidate_count"] = len(flagged_ids)
    state["provider_envelope_count"] = len(provider_packets)
    true_north._multipass_state_write(state_path, state)
    result = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "configuration_sha256": configuration[
            "configuration_sha256"
        ],
        "complete": True,
        "flagged_candidate_count": len(flagged_ids),
        "provider_envelope_count": len(provider_packets),
        "usage": state["usage"],
        "cumulative_calls_after_run": (
            CUMULATIVE_CALLS_BEFORE_RUN + int(state["usage"]["calls"])
        ),
        "known_cumulative_tokens_after_run": (
            KNOWN_CUMULATIVE_TOKENS_BEFORE_RUN
            + int(state["usage"]["tokens"])
        ),
        "holdout_opened": False,
        "production_mutation": False,
    }
    result["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(result)
    )
    true_north._write_json(
        run_root / "result.json", result, immutable=False
    )
    return {**result, "run_root": str(run_root)}


def _atomic_subset(
    *,
    consensus: Mapping[str, Any],
    predictions: Mapping[str, Mapping[str, Any]],
    candidate_ids: set[str],
) -> float:
    rows = true_north._consensus_atomic_metrics(
        {
            **consensus,
            "items": [
                row
                for row in consensus["items"]
                if str(row["candidate_id"]) in candidate_ids
            ],
        },
        {
            candidate_id: predictions[candidate_id]
            for candidate_id in candidate_ids
        },
        require_complete_scope=True,
    )
    metrics = {str(row["metric"]): row["value"] for row in rows}
    return float(metrics["acceptable_atomic_count_rate"])


def score_spark_split_default_probe(
    *,
    suite_root: str | Path,
    run_id: str,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    configuration = true_north._read_json(
        run_root / "configuration.json"
    )
    result = true_north._read_json(run_root / "result.json")
    paths = sorted(
        (run_root / "outputs" / "composed").glob(
            "*/*/validated.private.json"
        )
    )
    predictions = [
        row
        for path in paths
        for row in true_north._read_json(path)["items"]
    ]
    span_predictions, span_report = _apply_actor_span(predictions)
    span_path = (
        run_root
        / "outputs"
        / "composed-actor-span"
        / "predictions.private.json"
    )
    span_document = {
        "schema_version": true_north.WORK_OUTPUT_SCHEMA_VERSION,
        "actor_span_rule": "deterministic_actor_span_rule_v1",
        "items": span_predictions,
    }
    span_document["predictions_sha256"] = true_north.sha256_text(
        true_north.dumps_json(span_document)
    )
    true_north._write_json(span_path, span_document, immutable=False)
    decoupled = score_predictions(
        suite_root=root,
        lane_id=f"{run_id}-actor-span",
        predictions=span_predictions,
        source_paths=[*paths, span_path],
        actor_span_applied=True,
        actor_span_report=span_report,
    )
    private = true_north._read_json(Path(decoupled["output_path"]))
    candidate_scores = {
        str(row["candidate_id"]): row
        for row in private["private_candidate_scores"]
    }
    prediction_map = {
        str(row["candidate_id"]): row for row in span_predictions
    }
    consensus = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    manifest = true_north._read_json(root / "manifest.json")
    speaker_maps: dict[str, Any] = {}
    for bundle_row in manifest["bundles"]:
        if bundle_row["partition"] != "development":
            continue
        bundle = true_north._read_json(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            speaker_maps[str(candidate["candidate_id"])] = (
                bundle["episode_context"].get("speaker_map", [])
            )
    aligned_ids = {
        candidate_id
        for candidate_id, row in candidate_scores.items()
        if row["strictly_scoreable"]
        and row["atomic_count"]["acceptable_count"]
        and row["value_state"]["predicted"] == "value"
    }
    aligned = score_campaign(
        [
            prediction_map[candidate_id]
            for candidate_id in sorted(aligned_ids)
        ],
        consensus["items"],
        preferred["items"],
        subset_candidate_ids=aligned_ids,
        speaker_maps_by_candidate=speaker_maps,
    )
    flagged_ids = {
        str(candidate["candidate_id"])
        for packet in _source_packets(root)
        for candidate in packet["packet"]["input"]["candidates"]
    }
    spark_flagged = _atomic_subset(
        consensus=consensus,
        predictions=prediction_map,
        candidate_ids=flagged_ids,
    )
    glm_paths = sorted(
        (
            root
            / "multipass"
            / "runs"
            / SOURCE_RUN_ID
            / "outputs"
            / "composed-partial-fallback"
        ).glob("*/*/validated.private.json")
    )
    glm_map = {
        str(row["candidate_id"]): row
        for path in glm_paths
        for row in true_north._read_json(path)["items"]
    }
    validated_glm_ids = {
        str(row["candidate_id"])
        for path in (
            root
            / "multipass"
            / "runs"
            / SOURCE_RUN_ID
            / "outputs"
            / "input-split-default"
        ).glob("*/*/validated.private.json")
        for row in true_north._read_json(path)["items"]
    }
    shared_ids = flagged_ids & validated_glm_ids
    comparison = {
        "shared_candidate_count": len(shared_ids),
        "spark_acceptable_atomic_count_rate": _atomic_subset(
            consensus=consensus,
            predictions=prediction_map,
            candidate_ids=shared_ids,
        ),
        "glm_acceptable_atomic_count_rate": _atomic_subset(
            consensus=consensus,
            predictions=glm_map,
            candidate_ids=shared_ids,
        ),
        "full_flagged_spark_acceptable_atomic_count_rate": spark_flagged,
        "full_flagged_glm_with_task5_fallback_diagnostic": _atomic_subset(
            consensus=consensus,
            predictions=glm_map,
            candidate_ids=flagged_ids,
        ),
    }
    checkpoint = true_north._read_json(
        root
        / "certification"
        / true_north.TASK5_CHECKPOINT_FILENAME
    )
    aggregate = decoupled["aggregate"]
    aligned_faithfulness = aligned["aggregate"][
        "claim_text_faithfulness_proxy"
    ]
    acceptance = {
        "atomic_count_accuracy_at_least_0_90": (
            float(aggregate["acceptable_atomic_count_rate"]) >= 0.90
        ),
        "aligned_faithfulness_at_least_decoupled_gate": (
            float(aligned_faithfulness)
            >= true_north.APPROVED_GATE_POLICY[
                "claim_text_faithfulness_proxy"
            ][1]
        ),
        "intrinsic_junk_escapes_zero": (
            checkpoint["disposition_gate"][
                "intrinsic_junk_escape_count"
            ]
            == 0
        ),
        "relational_contamination_zero": (
            checkpoint["relational_merge_certification"][
                "contamination_count"
            ]
            == 0
        ),
    }
    document = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "complete": True,
        "acceptance_eligible": True,
        "composed": {
            "acceptable_atomic_count_rate": aggregate[
                "acceptable_atomic_count_rate"
            ],
            "aligned_candidate_count": len(aligned_ids),
            "aligned_claim_text_faithfulness": aligned_faithfulness,
            "nine_gate_table": decoupled["nine_gate_table"],
            "passed_gate_count": decoupled["passed_gate_count"],
        },
        "flagged_comparison": comparison,
        "junk_and_contamination": {
            "intrinsic_junk_escape_count": checkpoint[
                "disposition_gate"
            ]["intrinsic_junk_escape_count"],
            "relational_contamination_count": checkpoint[
                "relational_merge_certification"
            ]["contamination_count"],
        },
        "acceptance": acceptance,
        "passed": all(acceptance.values()),
        "stop_decomposition_lane": not all(acceptance.values()),
        "usage": result["usage"],
        "cumulative_calls_after_run": result[
            "cumulative_calls_after_run"
        ],
        "known_cumulative_tokens_after_run": result[
            "known_cumulative_tokens_after_run"
        ],
        "holdout_opened": False,
        "production_mutation": False,
    }
    document["score_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = run_root / "spark-split-default-score.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "score_path": str(path)}


def finalize_partial_spark_split_default_probe(
    *,
    suite_root: str | Path,
    run_id: str,
) -> dict[str, Any]:
    """Freeze a budget-breaching partial run with explicit Task-5 fallback."""

    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    configuration = true_north._read_json(
        run_root / "configuration.json"
    )
    state = true_north._read_json(run_root / "state.json")
    if configuration.get("schema_version") != SCHEMA_VERSION:
        raise SparkSplitDefaultError(
            "run is not a Spark split-default probe"
        )
    if state.get("complete") is True:
        raise SparkSplitDefaultError(
            "complete runs use the normal Spark scorer"
        )
    if int(state["usage"]["calls"]) != MAX_CALLS:
        raise SparkSplitDefaultError(
            "partial Spark probe has not exhausted its call ceiling"
        )
    if int(state["usage"]["tokens"]) <= MAX_TOKENS:
        raise SparkSplitDefaultError(
            "partial finalizer is reserved for the measured token breach"
        )

    valid_outputs: list[dict[str, Any]] = []
    valid_paths: list[Path] = []
    for path in sorted(
        (run_root / "outputs" / "spark-input-split-default").glob(
            "*/*/validated.private.json"
        )
    ):
        packet = true_north._read_json(
            run_root
            / "packets"
            / "spark-input-split-default"
            / "search"
            / f"{path.parent.name}.private.json"
        )
        output = true_north._read_json(path)
        validate_provider_output(output, packet)
        valid_paths.append(path)
        valid_outputs.append(output)
    spark_by_candidate = {
        str(row["candidate_id"]): {
            key: value
            for key, value in row.items()
            if key != "merge_reason"
        }
        for output in valid_outputs
        for row in output["items"]
    }
    if not spark_by_candidate:
        raise SparkSplitDefaultError(
            "partial probe has no validated Spark candidates"
        )

    manifest = true_north._read_json(root / "manifest.json")
    base_jobs, dispositions, candidates = _load_search_context(
        root, manifest
    )
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        _reference_provenance,
    ) = _reference_artifacts(root)
    predictions: list[dict[str, Any]] = []
    for key, reference in sorted(reference_outputs.items()):
        adjudication = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                copy.deepcopy(
                    spark_by_candidate.get(
                        str(row["candidate_id"]), row
                    )
                )
                for row in reference["items"]
            ],
        }
        true_north.validate_multipass_adjudication(
            adjudication, reference_packets[key]
        )
        output = true_north.compose_multipass_output(
            base_jobs[key],
            dispositions[key],
            adjudication,
            None,
            stage_b_mode="adjudication",
        )
        predictions.extend(output["items"])
        true_north._write_json(
            run_root
            / "outputs"
            / "composed-partial-task5-fallback"
            / key[0]
            / key[1]
            / "validated.private.json",
            output,
            immutable=False,
        )
    span_predictions, span_report = _apply_actor_span(predictions)
    span_path = (
        run_root
        / "outputs"
        / "composed-partial-actor-span"
        / "predictions.private.json"
    )
    span_document = {
        "schema_version": true_north.WORK_OUTPUT_SCHEMA_VERSION,
        "actor_span_rule": "deterministic_actor_span_rule_v1",
        "fallback": (
            "Spark for validated candidates; frozen Task-5 for every "
            "unvalidated flagged candidate and every unflagged candidate"
        ),
        "items": span_predictions,
    }
    span_document["predictions_sha256"] = true_north.sha256_text(
        true_north.dumps_json(span_document)
    )
    true_north._write_json(span_path, span_document, immutable=False)
    decoupled = score_predictions(
        suite_root=root,
        lane_id=f"{run_id}-partial-actor-span",
        predictions=span_predictions,
        source_paths=[*valid_paths, span_path],
        actor_span_applied=True,
        actor_span_report=span_report,
    )
    private = true_north._read_json(Path(decoupled["output_path"]))
    candidate_scores = {
        str(row["candidate_id"]): row
        for row in private["private_candidate_scores"]
    }
    prediction_map = {
        str(row["candidate_id"]): row for row in span_predictions
    }
    consensus = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    speaker_maps: dict[str, Any] = {}
    for bundle_row in manifest["bundles"]:
        if bundle_row["partition"] != "development":
            continue
        bundle = true_north._read_json(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            speaker_maps[str(candidate["candidate_id"])] = (
                bundle["episode_context"].get("speaker_map", [])
            )
    aligned_ids = {
        candidate_id
        for candidate_id, row in candidate_scores.items()
        if row["strictly_scoreable"]
        and row["atomic_count"]["acceptable_count"]
        and row["value_state"]["predicted"] == "value"
    }
    aligned = score_campaign(
        [
            prediction_map[candidate_id]
            for candidate_id in sorted(aligned_ids)
        ],
        consensus["items"],
        preferred["items"],
        subset_candidate_ids=aligned_ids,
        speaker_maps_by_candidate=speaker_maps,
    )
    measured_ids = set(spark_by_candidate)
    glm_paths = sorted(
        (
            root
            / "multipass"
            / "runs"
            / SOURCE_RUN_ID
            / "outputs"
            / "composed-partial-fallback"
        ).glob("*/*/validated.private.json")
    )
    glm_map = {
        str(row["candidate_id"]): row
        for path in glm_paths
        for row in true_north._read_json(path)["items"]
    }
    flagged_ids = {
        str(candidate["candidate_id"])
        for packet in _source_packets(root)
        for candidate in packet["packet"]["input"]["candidates"]
    }
    comparison = {
        "measured_same_candidate_count": len(measured_ids),
        "spark_acceptable_atomic_count_rate": _atomic_subset(
            consensus=consensus,
            predictions=prediction_map,
            candidate_ids=measured_ids,
        ),
        "glm_acceptable_atomic_count_rate": _atomic_subset(
            consensus=consensus,
            predictions=glm_map,
            candidate_ids=measured_ids,
        ),
        "full_flagged_partial_spark_with_task5_fallback": _atomic_subset(
            consensus=consensus,
            predictions=prediction_map,
            candidate_ids=flagged_ids,
        ),
        "full_flagged_glm_with_task5_fallback": _atomic_subset(
            consensus=consensus,
            predictions=glm_map,
            candidate_ids=flagged_ids,
        ),
    }
    checkpoint = true_north._read_json(
        root
        / "certification"
        / true_north.TASK5_CHECKPOINT_FILENAME
    )
    aggregate = decoupled["aggregate"]
    document = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "complete": False,
        "acceptance_eligible": False,
        "terminal_reason": (
            "call_ceiling_exhausted_and_actual_tokens_exceeded_declared_"
            "ceiling_after_under_reserved_provider_envelopes"
        ),
        "coverage": {
            "validated_provider_envelopes": len(valid_outputs),
            "expected_provider_envelopes": MAX_CALLS,
            "validated_spark_candidate_count": len(measured_ids),
            "flagged_candidate_count": len(flagged_ids),
        },
        "partial_fallback_composition": {
            "acceptable_atomic_count_rate": aggregate[
                "acceptable_atomic_count_rate"
            ],
            "aligned_candidate_count": len(aligned_ids),
            "aligned_claim_text_faithfulness": aligned["aggregate"][
                "claim_text_faithfulness_proxy"
            ],
            "nine_gate_table": decoupled["nine_gate_table"],
            "passed_gate_count": decoupled["passed_gate_count"],
        },
        "flagged_comparison": comparison,
        "junk_and_contamination": {
            "intrinsic_junk_escape_count": checkpoint[
                "disposition_gate"
            ]["intrinsic_junk_escape_count"],
            "relational_contamination_count": checkpoint[
                "relational_merge_certification"
            ]["contamination_count"],
        },
        "acceptance": {
            "complete_full_search_measurement": False,
            "within_declared_call_ceiling": (
                int(state["usage"]["calls"]) <= MAX_CALLS
            ),
            "within_declared_token_ceiling": False,
            "atomic_count_accuracy_at_least_0_90": (
                float(aggregate["acceptable_atomic_count_rate"]) >= 0.90
            ),
            "aligned_faithfulness_at_least_decoupled_gate": (
                float(
                    aligned["aggregate"][
                        "claim_text_faithfulness_proxy"
                    ]
                )
                >= true_north.APPROVED_GATE_POLICY[
                    "claim_text_faithfulness_proxy"
                ][1]
            ),
            "intrinsic_junk_escapes_zero": (
                checkpoint["disposition_gate"][
                    "intrinsic_junk_escape_count"
                ]
                == 0
            ),
            "relational_contamination_zero": (
                checkpoint["relational_merge_certification"][
                    "contamination_count"
                ]
                == 0
            ),
        },
        "passed": False,
        "stop_decomposition_lane": True,
        "usage": state["usage"],
        "budget_overage": {
            "calls": max(
                0, int(state["usage"]["calls"]) - MAX_CALLS
            ),
            "tokens": max(
                0, int(state["usage"]["tokens"]) - MAX_TOKENS
            ),
            "cause": (
                "reserved_tokens_per_call was set to 17000; actual Spark "
                "envelopes averaged above that reservation"
            ),
        },
        "cumulative_calls_after_run": (
            CUMULATIVE_CALLS_BEFORE_RUN
            + int(state["usage"]["calls"])
        ),
        "known_cumulative_tokens_after_run": (
            KNOWN_CUMULATIVE_TOKENS_BEFORE_RUN
            + int(state["usage"]["tokens"])
        ),
        "holdout_opened": False,
        "production_mutation": False,
    }
    document["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = run_root / "terminal-partial-result.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "result_path": str(path)}
