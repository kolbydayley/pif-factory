"""Bounded dual-pass decomposition with count-disagreement escalation."""

from __future__ import annotations

import copy
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north


SCHEMA_VERSION = "pif_true_north_dual_decomposition_v1"
REFERENCE_RUN_ID = "task5-adjudication-20260729-v1"
GLM_MODEL = "zai-coding-plan/glm-5.2"
SPARK_MODEL = "openai/gpt-5.3-codex-spark"
SEARCH_EPISODE_IDS = (
    "ep_90c3b5c995bce501c9aef55c",
    "ep_7ec9f808a3955c720aeb94ff",
)
MAX_CALLS = 30
MAX_TOKENS = 400_000
MAX_WALL_SECONDS = 2 * 60 * 60
MAX_ESCALATION_RATE = 0.25
RESERVED_TOKENS_PER_CALL = 13_000


class DualDecompositionError(RuntimeError):
    """Raised when the bounded dual-pass contract cannot be satisfied."""


def _items(output: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row["candidate_id"]): dict(row)
        for row in output["items"]
    }


def count_disagreements(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
) -> list[str]:
    first_items = _items(first)
    second_items = _items(second)
    if set(first_items) != set(second_items):
        raise DualDecompositionError(
            "decomposition passes have different candidate scope"
        )
    return sorted(
        candidate_id
        for candidate_id in first_items
        if len(first_items[candidate_id]["atomic_claims"])
        != len(second_items[candidate_id]["atomic_claims"])
    )


def compose_agreement_output(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
    escalated: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Use pass 1 on count agreement and Spark on count disagreement."""

    first_items = _items(first)
    second_items = _items(second)
    disagreements = set(count_disagreements(first, second))
    escalated_items = _items(escalated) if escalated is not None else {}
    if set(escalated_items) != disagreements:
        raise DualDecompositionError(
            "Spark output must exactly cover count disagreements"
        )
    return {
        "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
        "items": [
            copy.deepcopy(
                escalated_items[candidate_id]
                if candidate_id in disagreements
                else first_items[candidate_id]
            )
            for candidate_id in sorted(first_items)
        ],
    }


def _filtered_packet(
    packet: Mapping[str, Any],
    candidate_ids: Sequence[str],
) -> dict[str, Any]:
    selected = set(candidate_ids)
    value = copy.deepcopy(packet)
    value["output_schema"] = true_north.multipass_adjudication_schema(
        sorted(selected)
    )
    value["input"]["candidates"] = [
        row
        for row in value["input"]["candidates"]
        if str(row["candidate_id"]) in selected
    ]
    value["input"]["stage_a_decisions"] = [
        row
        for row in value["input"]["stage_a_decisions"]
        if str(row["candidate_id"]) in selected
    ]
    if {
        str(row["candidate_id"])
        for row in value["input"]["candidates"]
    } != selected:
        raise DualDecompositionError(
            "escalation candidate is absent from its source packet"
        )
    return value


def _load_search_context(
    suite_root: Path,
    manifest: Mapping[str, Any],
) -> tuple[
    dict[tuple[str, str], dict[str, Any]],
    dict[tuple[str, str], dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    development = {
        str(row["episode_id"]): row
        for row in manifest["bundles"]
        if row["partition"] == "development"
    }
    base_jobs: dict[tuple[str, str], dict[str, Any]] = {}
    candidates: dict[str, dict[str, Any]] = {}
    for episode_id in SEARCH_EPISODE_IDS:
        if episode_id not in development:
            raise DualDecompositionError(
                "Search episode is absent from development manifest"
            )
        bundle = true_north._read_json(
            Path(development[episode_id]["bundle_path"])
        )
        for candidate in bundle["candidates"]:
            candidates[str(candidate["candidate_id"])] = dict(candidate)
        for job in true_north._segment_jobs(bundle, gold=False):
            segment_id = str(job["input"]["segment"]["segment_id"])
            base_jobs[(episode_id, segment_id)] = job
    resolved, _provenance = true_north._task5_resolved_dispositions(
        suite_root=suite_root,
        manifest=manifest,
        candidate_by_id=candidates,
    )
    dispositions = {
        key: true_north._task5_disposition_document(
            base_job, resolved
        )
        for key, base_job in base_jobs.items()
    }
    return base_jobs, dispositions, candidates


def _reference_artifacts(
    suite_root: Path,
) -> tuple[
    Path,
    dict[tuple[str, str], dict[str, Any]],
    dict[tuple[str, str], dict[str, Any]],
    dict[str, Any],
]:
    reference_root = (
        suite_root / "multipass" / "runs" / REFERENCE_RUN_ID
    )
    configuration = true_north._read_json(
        reference_root / "configuration.json"
    )
    state = true_north._read_json(reference_root / "state.json")
    if (
        configuration.get("schema_version")
        != true_north.TASK5_SCHEMA_VERSION
        or state.get("complete") is not True
        or configuration.get("holdout_access_allowed") is not False
        or configuration.get("production_database_open_allowed") is not False
    ):
        raise DualDecompositionError(
            "frozen Task 5 reference is not a complete local-only run"
        )
    prompt_hash = true_north.sha256_text(
        true_north.MULTIPASS_SYSTEM_PROMPTS["adjudication"]
    )
    if configuration["system_prompt_sha256"]["adjudication"] != prompt_hash:
        raise DualDecompositionError(
            "frozen Task 5 prompt differs from current frozen prompt"
        )
    packets: dict[tuple[str, str], dict[str, Any]] = {}
    outputs: dict[tuple[str, str], dict[str, Any]] = {}
    for packet_path in sorted(
        (reference_root / "packets" / "adjudication").glob(
            "*/*.private.json"
        )
    ):
        episode_id = packet_path.parent.name
        segment_id = packet_path.stem.removesuffix(".private")
        key = (episode_id, segment_id)
        packet = true_north._read_json(packet_path)
        output_path = (
            reference_root
            / "outputs"
            / "adjudication"
            / episode_id
            / segment_id
            / "validated.private.json"
        )
        output = true_north._read_json(output_path)
        true_north.validate_multipass_adjudication(output, packet)
        packets[key] = packet
        outputs[key] = output
    if len(packets) != 21 or set(packets) != set(outputs):
        raise DualDecompositionError(
            "frozen Task 5 reference must contain exactly 21 packets"
        )
    provenance = {
        "run_id": REFERENCE_RUN_ID,
        "configuration_sha256": configuration["configuration_sha256"],
        "configuration_file_sha256": true_north._sha256_file(
            reference_root / "configuration.json"
        ),
        "state_file_sha256": true_north._sha256_file(
            reference_root / "state.json"
        ),
        "score_file_sha256": true_north._sha256_file(
            reference_root / "task5-score.private.json"
        ),
        "usage": state["usage"],
    }
    return reference_root, packets, outputs, provenance


def run_dual_decomposition(
    *,
    suite_root: str | Path,
    run_id: str | None = None,
    workers: int = 4,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    runner: Any | None = None,
) -> dict[str, Any]:
    """Run one new GLM pass and Spark only on count disagreements."""

    if workers < 1 or workers > 4:
        raise DualDecompositionError("workers must be between 1 and 4")
    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise DualDecompositionError(
            "suite verification failed before dual decomposition"
        )
    manifest = true_north._read_json(root / "manifest.json")
    base_jobs, dispositions, candidates = _load_search_context(
        root, manifest
    )
    (
        _reference_root,
        packets,
        first_outputs,
        reference_provenance,
    ) = _reference_artifacts(root)
    if set(packets) != {
        key
        for key in base_jobs
        if true_north.build_multipass_adjudication_packet(
            base_jobs[key], dispositions[key]
        )
        is not None
    }:
        raise DualDecompositionError(
            "current Search packet scope differs from frozen Task 5"
        )
    for key, packet in packets.items():
        rebuilt = true_north.build_multipass_adjudication_packet(
            base_jobs[key], dispositions[key]
        )
        if rebuilt != packet:
            raise DualDecompositionError(
                "current Stage-B packet differs from frozen Task 5"
            )

    resolved_run_id = run_id or (
        "task5-dual-decomposition-"
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
        "frozen_reference": reference_provenance,
        "models": {
            "second_pass": GLM_MODEL,
            "count_disagreement_escalation": SPARK_MODEL,
        },
        "selection_rule": {
            "same_count": "frozen_pass_1",
            "different_count": "spark",
            "semantic_disagreement_fallback": False,
        },
        "system_prompt_sha256": true_north.sha256_text(
            true_north.MULTIPASS_SYSTEM_PROMPTS["adjudication"]
        ),
        "packet_contract": "identical_frozen_stage_b_candidate_projection",
        "candidate_count": len(candidates),
        "eligible_decomposition_candidate_count": sum(
            len(packet["input"]["candidates"])
            for packet in packets.values()
        ),
        "max_escalation_rate": MAX_ESCALATION_RATE,
        "max_escalation_candidates": math.floor(
            len(candidates) * MAX_ESCALATION_RATE
        ),
        "budget": budget,
        "campaign_calls_before_run_approximate": 73,
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
            raise DualDecompositionError(
                "resume configuration differs from declared dual-pass run"
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
        (episode_id, segment_id, packets[(episode_id, segment_id)])
        for episode_id, segment_id in sorted(packets)
    ]
    second_outputs = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="adjudication",
        artifact_stage="glm-pass-2",
        jobs=jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=GLM_MODEL,
        reserved_tokens_per_call=RESERVED_TOKENS_PER_CALL,
    )

    disagreements_by_packet: dict[
        tuple[str, str], list[str]
    ] = {}
    all_disagreements: list[str] = []
    for key in sorted(packets):
        disagreement = count_disagreements(
            first_outputs[key], second_outputs[key]
        )
        if disagreement:
            disagreements_by_packet[key] = disagreement
            all_disagreements.extend(disagreement)
    all_disagreements = sorted(all_disagreements)
    max_candidates = int(
        configuration["max_escalation_candidates"]
    )
    escalation_packet_count = len(disagreements_by_packet)
    overflow_reasons: list[str] = []
    if len(all_disagreements) > max_candidates:
        overflow_reasons.append("candidate_rate_cap")
    if int(state["usage"]["calls"]) + escalation_packet_count > MAX_CALLS:
        overflow_reasons.append("call_cap")
    disagreement_report = {
        "schema_version": SCHEMA_VERSION,
        "candidate_count": len(candidates),
        "eligible_decomposition_candidate_count": configuration[
            "eligible_decomposition_candidate_count"
        ],
        "disagreement_candidate_count": len(all_disagreements),
        "disagreement_candidate_ids": all_disagreements,
        "realized_escalation_rate_all_candidates": round(
            len(all_disagreements) / len(candidates), 6
        ),
        "realized_escalation_rate_eligible_candidates": round(
            len(all_disagreements)
            / int(configuration[
                "eligible_decomposition_candidate_count"
            ]),
            6,
        ),
        "escalation_packet_count": escalation_packet_count,
        "max_escalation_candidates": max_candidates,
        "overflow": bool(overflow_reasons),
        "overflow_reasons": overflow_reasons,
    }
    true_north._write_json(
        run_root / "disagreement-ledger.json",
        disagreement_report,
        immutable=False,
    )
    if overflow_reasons:
        state["halted"] = True
        state["halt_reason"] = "escalation_overflow"
        true_north._multipass_state_write(state_path, state)
        return {
            "ok": False,
            "run_id": resolved_run_id,
            "state": "escalation_overflow",
            "disagreement": disagreement_report,
            "usage": state["usage"],
            "holdout_opened": False,
            "production_mutation": False,
        }

    escalation_jobs = [
        (
            episode_id,
            segment_id,
            _filtered_packet(
                packets[(episode_id, segment_id)],
                disagreements_by_packet[(episode_id, segment_id)],
            ),
        )
        for episode_id, segment_id in sorted(disagreements_by_packet)
    ]
    escalated_outputs = (
        true_north._multipass_execute_stage(
            run_root=run_root,
            stage="adjudication",
            artifact_stage="spark-escalation",
            jobs=escalation_jobs,
            state=state,
            workers=workers,
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
            runner=runner,
            model=SPARK_MODEL,
            reserved_tokens_per_call=RESERVED_TOKENS_PER_CALL,
        )
        if escalation_jobs
        else {}
    )

    final_outputs: dict[tuple[str, str], dict[str, Any]] = {}
    for key in sorted(packets):
        disagreement_ids = disagreements_by_packet.get(key, [])
        escalation = escalated_outputs.get(key)
        final = compose_agreement_output(
            first_outputs[key],
            second_outputs[key],
            escalation,
        )
        true_north.validate_multipass_adjudication(
            final, packets[key]
        )
        destination = (
            run_root
            / "outputs"
            / "adjudication-final"
            / key[0]
            / key[1]
            / "validated.private.json"
        )
        true_north._write_json(destination, final, immutable=True)
        final_outputs[key] = final
        if bool(disagreement_ids) != (escalation is not None):
            raise DualDecompositionError(
                "escalation presence differs from disagreement scope"
            )

    for key in sorted(base_jobs):
        output = true_north.compose_multipass_output(
            base_jobs[key],
            dispositions[key],
            final_outputs.get(key),
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
    state["stage_b_packet_count"] = len(packets)
    state["disagreement_candidate_count"] = len(all_disagreements)
    state["spark_escalation_packet_count"] = len(escalation_jobs)
    true_north._multipass_state_write(state_path, state)
    charged_glm_attempts = (
        int(state["usage"]["calls"]) - len(escalation_jobs)
    )
    cost = {
        "successful_glm_packet_calls": len(jobs),
        "charged_glm_attempts": charged_glm_attempts,
        "failed_glm_attempts": max(
            0, charged_glm_attempts - len(jobs)
        ),
        "spark_escalation_calls": len(escalation_jobs),
        "total_new_calls": int(state["usage"]["calls"]),
        "total_new_tokens": int(state["usage"]["tokens"]),
        "all_spark_baseline_calls": len(jobs),
        "total_call_ratio_vs_all_spark": round(
            int(state["usage"]["calls"]) / len(jobs), 6
        ),
        "effective_frontier_call_ratio_vs_all_spark": round(
            len(escalation_jobs) / len(jobs), 6
        ),
        "frontier_calls_avoided_vs_all_spark": (
            len(jobs) - len(escalation_jobs)
        ),
        "cost_interpretation": (
            "frontier-call exposure; GLM subscription calls are reported "
            "separately and no dollar equivalence is asserted"
        ),
    }
    result = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "run_id": resolved_run_id,
        "configuration_sha256": configuration[
            "configuration_sha256"
        ],
        "complete": True,
        "disagreement": disagreement_report,
        "cost": cost,
        "usage": state["usage"],
        "run_root": str(run_root),
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
    return result


def score_dual_decomposition(
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
        raise DualDecompositionError(
            "run is not a dual-decomposition experiment"
        )
    result = true_north._read_json(run_root / "result.json")
    score = true_north.score_multipass_run(
        run_id=run_id,
        output_root=root.parent,
        suite=root.name,
    )
    reference = true_north._read_json(
        root
        / "multipass"
        / "runs"
        / REFERENCE_RUN_ID
        / "task5-score.private.json"
    )
    aggregate = score["aggregate"]
    metrics = {
        metric: {
            "dual_pass": aggregate[metric],
            "single_pass_reference": reference["search_fold"][
                "aggregate"
            ][metric],
        }
        for metric in (
            "acceptable_atomic_count_rate",
            "speaker_exactness",
            "claim_text_faithfulness_proxy",
            "reported_actor_exactness",
            "hallucination_rate_proxy",
        )
    }
    document = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "atomic_count_accuracy": aggregate[
            "acceptable_atomic_count_rate"
        ],
        "atomic_count_gate": 0.90,
        "atomic_count_passed": (
            float(aggregate["acceptable_atomic_count_rate"]) >= 0.90
        ),
        "alignment_bound_metrics": {
            "speaker_exactness": aggregate["speaker_exactness"],
            "claim_text_faithfulness": aggregate[
                "claim_text_faithfulness_proxy"
            ],
        },
        "actor_diagnostics": {
            "reported_actor_exactness": aggregate[
                "reported_actor_exactness"
            ],
            "hallucination_rate": aggregate[
                "hallucination_rate_proxy"
            ],
        },
        "comparison": metrics,
        "disagreement": result["disagreement"],
        "cost": result["cost"],
        "usage": result["usage"],
        "holdout_opened": False,
        "production_mutation": False,
        "prompt_tuned": False,
    }
    document["score_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = run_root / "dual-score.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "score_path": str(path)}
