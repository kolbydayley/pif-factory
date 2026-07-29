"""Ruling-8 one-episode sealed-transfer execution.

The module is intentionally episode-locked.  It runs the certified GLM-only
composition before permitting any gold execution, freezes that output, and
then exposes selected-episode gold helpers.  The other sealed episode is a
hard error at every boundary.
"""

from __future__ import annotations

import copy
import json
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from .true_north_actor_span_rule import apply_actor_span_rule
from .true_north_actor_suppression import suppress_predictions
from .true_north_input_split_default import (
    FLAGGED_SYSTEM_PROMPT,
    _flagged_packet,
    compound_flag,
    validate_merge_adjudication,
)
from .true_north_option2 import apply_phase_c_hold_resolution


SCHEMA_VERSION = "pif_true_north_sealed_transfer_v1"
EXPERIMENT_ID = "ruling8-sealed-transfer-smaller-episode-20260729-v1"
AUTHORIZED_EPISODE_ID = "ep_97a45100ce0d305f58d7dd69"
FORBIDDEN_EPISODE_ID = "ep_044f1d2d020e021cfaf99e90"
RUN_ID = "ruling8-sealed-transfer-blind-20260729-v1"
MAX_CALLS = 150
MAX_TOKENS = 4_800_000
MAX_WALL_SECONDS = 12 * 60 * 60
GLM_MODEL = "zai-coding-plan/glm-5.2"
ACTOR_SUPPRESSION_THRESHOLD = 2
LEDGER_PATH = (
    Path(__file__).resolve().parent.parent
    / "config"
    / "true_north_campaign_budget_ledger.json"
)


class TransferError(RuntimeError):
    """Raised when the sealed-transfer protocol or isolation would drift."""


def _root(suite_root: str | Path) -> Path:
    root = Path(suite_root).expanduser().resolve()
    result = true_north.verify_suite(output_root=root.parent, suite=root.name)
    if not result["ok"]:
        raise TransferError("suite verification failed")
    return root


def _run_root(root: Path) -> Path:
    return root / "sealed-transfer" / "runs" / RUN_ID


def _authorized_bundle(root: Path) -> tuple[dict[str, Any], Path]:
    manifest = true_north._read_json(root / "manifest.json")
    matches = [
        row
        for row in manifest["bundles"]
        if str(row["episode_id"]) == AUTHORIZED_EPISODE_ID
    ]
    if len(matches) != 1 or matches[0]["partition"] != "holdout":
        raise TransferError("authorized sealed bundle is not uniquely bound")
    path = Path(matches[0]["bundle_path"]).expanduser().resolve()
    if FORBIDDEN_EPISODE_ID in str(path):
        raise TransferError("forbidden episode resolved as authorized bundle")
    if true_north._sha256_file(path) != matches[0]["bundle_sha256"]:
        raise TransferError("authorized bundle hash drift")
    bundle = true_north._read_json(path)
    if str(bundle["episode"]["episode_id"]) != AUTHORIZED_EPISODE_ID:
        raise TransferError("authorized bundle episode mismatch")
    return bundle, path


def _gold_state(root: Path) -> dict[str, Any]:
    base = root / "gold" / "sealed-holdout"
    return {
        "pass_a_outputs": len(
            list((base / "pass-a" / "outputs").glob(
                "*/validated.private.json"
            ))
        ),
        "pass_b_outputs": len(
            list((base / "pass-b" / "outputs").glob(
                "*/validated.private.json"
            ))
        ),
        "pass_c_exists": (base / "pass-c-adjudication").exists(),
        "final_gold_exists": (
            base / "final" / "gold.private.json"
        ).exists(),
        "final_consensus_exists": (
            base / "final" / "consensus.private.json"
        ).exists(),
    }


def _ledger_preflight() -> tuple[dict[str, Any], str]:
    ledger = true_north._read_json(LEDGER_PATH)
    rows = {
        row["experiment_id"]: row
        for row in ledger["declared_experiments"]
        if "experiment_id" in row
    }
    row = rows.get(EXPERIMENT_ID)
    if (
        row is None
        or row["status"] != "declared_before_first_call"
        or int(row["max_calls"]) != MAX_CALLS
        or int(row["max_tokens"]) != MAX_TOKENS
        or row["scope"]["authorized_episode_id"]
        != AUTHORIZED_EPISODE_ID
        or row["scope"]["forbidden_episode_id"]
        != FORBIDDEN_EPISODE_ID
    ):
        raise TransferError("Ruling-8 budget declaration is missing or drifted")
    return ledger, true_north.sha256_text(true_north.dumps_json(ledger))


def _base_jobs(bundle: Mapping[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    jobs: dict[tuple[str, str], dict[str, Any]] = {}
    for job in true_north._segment_jobs(bundle, gold=False):
        episode_id = str(job["input"]["episode"]["episode_id"])
        segment_id = str(job["input"]["segment"]["segment_id"])
        if episode_id != AUTHORIZED_EPISODE_ID:
            raise TransferError("runtime packet escaped authorized episode")
        jobs[(episode_id, segment_id)] = job
    if len(jobs) != 17:
        raise TransferError(f"expected 17 authorized segment jobs, got {len(jobs)}")
    return jobs


def _compose_disposition(
    base_jobs: Mapping[tuple[str, str], Mapping[str, Any]],
    first: Mapping[tuple[str, str], Mapping[str, Any]],
    second: Mapping[tuple[str, str], Mapping[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}
    first_items: dict[str, dict[str, Any]] = {}
    second_items: dict[str, dict[str, Any]] = {}
    candidate_to_key: dict[str, tuple[str, str]] = {}
    for key, job in base_jobs.items():
        for candidate in job["input"]["candidates"]:
            candidate_id = str(candidate["candidate_id"])
            candidates[candidate_id] = dict(candidate)
            candidate_to_key[candidate_id] = key
        first_items.update(
            {str(row["candidate_id"]): dict(row) for row in first[key]["items"]}
        )
        second_items.update(
            {str(row["candidate_id"]): dict(row) for row in second[key]["items"]}
        )
    if set(candidates) != set(first_items) or set(candidates) != set(second_items):
        raise TransferError("disposition pass scopes differ")
    composed: dict[str, dict[str, Any]] = {}
    disagreements: list[str] = []
    for candidate_id in sorted(candidates):
        earlier = first_items[candidate_id]
        current = second_items[candidate_id]
        earlier_value = (
            true_north._gold_value_state(str(earlier["disposition"])) == "value"
        )
        current_value = (
            true_north._gold_value_state(str(current["disposition"])) == "value"
        )
        if earlier_value == current_value:
            composed[candidate_id] = current
        else:
            disagreements.append(candidate_id)
            composed[candidate_id] = earlier if earlier_value else current
    intrinsic = true_north.apply_phase_c_intrinsic_composition_rules(
        composed, candidates
    )
    predictions = intrinsic["predictions"]
    held_ids = [
        candidate_id
        for candidate_id, row in predictions.items()
        if str(row["disposition"]) == "hold"
    ]
    hold = apply_phase_c_hold_resolution(
        predictions, candidates, held_candidate_ids=held_ids
    )
    by_key: dict[tuple[str, str], list[dict[str, Any]]] = {
        key: [] for key in base_jobs
    }
    for candidate_id in sorted(hold["predictions"]):
        by_key[candidate_to_key[candidate_id]].append(
            hold["predictions"][candidate_id]
        )
    outputs = {
        key: {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                {
                    "candidate_id": str(row["candidate_id"]),
                    "disposition": str(row["disposition"]),
                    "junk_reason": row.get("junk_reason"),
                }
                for row in rows
            ],
        }
        for key, rows in by_key.items()
    }
    for key, output in outputs.items():
        true_north.validate_multipass_disposition(
            output, true_north.build_multipass_disposition_packet(base_jobs[key])
        )
    return outputs, {
        "value_state_disagreement_count": len(disagreements),
        "value_state_disagreement_candidate_ids": disagreements,
        "intrinsic_rule": {
            key: value
            for key, value in intrinsic.items()
            if key != "predictions"
        },
        "hold_resolution": {
            key: value for key, value in hold.items() if key != "predictions"
        },
    }


def _account_failed_split_attempts(
    run_root: Path, state: dict[str, Any]
) -> int:
    failures = 0
    for output_dir in (
        run_root / "outputs" / "input-split-default"
        / AUTHORIZED_EPISODE_ID
    ).glob("*"):
        if (output_dir / "validated.private.json").is_file():
            continue
        for attempt_path in output_dir.glob("attempt-*.private.jsonl"):
            digest = true_north._sha256_file(attempt_path)
            marker = output_dir / f"failed-attempt-{digest}.accounted.json"
            if marker.is_file():
                continue
            _answer, finish, _events = true_north._parse_opencode_stream(
                attempt_path.read_text(encoding="utf-8")
            )
            usage = true_north._usage(finish)
            token_count = int(usage.get("total_tokens") or 32_000)
            true_north._write_json(
                marker,
                {
                    "schema_version": SCHEMA_VERSION,
                    "reason": "split_default_validation_failure_task5_fallback",
                    "attempt_sha256": digest,
                    "usage": usage,
                    "conservative_tokens_used": token_count,
                },
                immutable=True,
            )
            state["usage"]["calls"] += 1
            state["usage"]["tokens"] += token_count
            failures += 1
    true_north._multipass_state_write(run_root / "state.json", state)
    return failures


def run_blind(
    *,
    suite_root: str | Path,
    workers: int = 3,
    timeout_seconds: int = 1200,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    runner: Any | None = None,
) -> dict[str, Any]:
    """Run and freeze the episode-locked certified GLM-only stack."""
    root = _root(suite_root)
    ledger, ledger_sha = _ledger_preflight()
    gold_before = _gold_state(root)
    if any(gold_before.values()):
        raise TransferError(
            "blind run requires sealed gold outputs to be absent"
        )
    bundle, bundle_path = _authorized_bundle(root)
    base_jobs = _base_jobs(bundle)
    run_root = _run_root(root)
    freeze_path = run_root / "blind-freeze.json"
    if freeze_path.is_file():
        return true_north._read_json(freeze_path)
    configuration = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": RUN_ID,
        "authorized_episode_id": AUTHORIZED_EPISODE_ID,
        "forbidden_episode_id": FORBIDDEN_EPISODE_ID,
        "bundle_path": str(bundle_path),
        "bundle_sha256": true_north._sha256_file(bundle_path),
        "model": GLM_MODEL,
        "composition": [
            "two_glm_disposition_passes_value_state_or",
            "bracket_link_chrome_only_v1",
            "admit_hold_unless_intrinsic_v1",
            "frozen_task5_adjudication",
            "split_default_flagged_with_task5_validation_fallback",
            "deterministic_actor_span_rule_v1",
            "phase_d_actor_suppression_threshold_2",
            "speaker_prior_adoption",
        ],
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
            "max_wall_seconds": MAX_WALL_SECONDS,
        },
        "ledger_sha256": ledger_sha,
        "gold_state_before": gold_before,
        "sealed_gold_in_model_input": False,
    }
    configuration["configuration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(configuration)
    )
    true_north._write_json(
        run_root / "configuration.json", configuration, immutable=True
    )
    true_north._write_json(
        run_root / "budget-ledger-snapshot.json", ledger, immutable=True
    )
    state_path = run_root / "state.json"
    state = (
        true_north._read_json(state_path)
        if state_path.is_file()
        else {
            "schema_version": SCHEMA_VERSION,
            "run_id": RUN_ID,
            "configuration_sha256": configuration["configuration_sha256"],
            "budget": copy.deepcopy(configuration["budget"]),
            "completed": [],
            "usage": {"calls": 0, "tokens": 0, "wall_seconds": 0.0},
            "complete": False,
        }
    )
    true_north._multipass_state_write(state_path, state)
    disposition_jobs = [
        (
            key[0],
            key[1],
            true_north.build_multipass_disposition_packet(job),
        )
        for key, job in sorted(base_jobs.items())
    ]
    first = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="disposition",
        artifact_stage="disposition-a",
        jobs=disposition_jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=GLM_MODEL,
        reserved_tokens_per_call=20_000,
    )
    second = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="disposition",
        artifact_stage="disposition-b",
        jobs=disposition_jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=GLM_MODEL,
        reserved_tokens_per_call=20_000,
    )
    dispositions, disposition_report = _compose_disposition(
        base_jobs, first, second
    )
    for key, output in dispositions.items():
        true_north._write_json(
            run_root / "outputs" / "disposition-composed"
            / key[0] / key[1] / "validated.private.json",
            output,
            immutable=True,
        )
    task5_packets = {
        key: packet
        for key, job in base_jobs.items()
        if (
            packet := true_north.build_multipass_adjudication_packet(
                job, dispositions[key]
            )
        )
        is not None
    }
    task5 = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="adjudication",
        artifact_stage="task5",
        jobs=[
            (key[0], key[1], packet)
            for key, packet in sorted(task5_packets.items())
        ],
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=GLM_MODEL,
        reserved_tokens_per_call=25_000,
    )
    candidate_map = {
        str(candidate["candidate_id"]): candidate
        for job in base_jobs.values()
        for candidate in job["input"]["candidates"]
    }
    flagged_ids = {
        candidate_id
        for candidate_id, candidate in candidate_map.items()
        if compound_flag(str(candidate.get("claim_text") or ""))
        and any(
            str(row["candidate_id"]) == candidate_id
            for output in task5.values()
            for row in output["items"]
        )
    }
    split_packets: dict[tuple[str, str], dict[str, Any]] = {}
    for key, packet in task5_packets.items():
        ids = {
            str(row["candidate_id"])
            for row in packet["input"]["candidates"]
        } & flagged_ids
        if ids:
            split_packets[key] = _flagged_packet(packet, ids)
    split_error: str | None = None
    try:
        true_north._multipass_execute_stage(
            run_root=run_root,
            stage="adjudication",
            artifact_stage="input-split-default",
            jobs=[
                (key[0], key[1], packet)
                for key, packet in sorted(split_packets.items())
            ],
            state=state,
            workers=workers,
            timeout_seconds=timeout_seconds,
            opencode_binary=opencode_binary,
            runner=runner,
            model=GLM_MODEL,
            reserved_tokens_per_call=25_000,
            validator_override=validate_merge_adjudication,
            system_prompt_override=FLAGGED_SYSTEM_PROMPT,
        )
    except Exception as exc:
        split_error = f"{type(exc).__name__}: {exc}"
        _account_failed_split_attempts(run_root, state)
    split_outputs: dict[tuple[str, str], dict[str, Any]] = {}
    for key in split_packets:
        path = (
            run_root / "outputs" / "input-split-default"
            / key[0] / key[1] / "validated.private.json"
        )
        if path.is_file():
            split_outputs[key] = true_north._read_json(path)
    predictions: list[dict[str, Any]] = []
    split_used = split_fallback = 0
    for key, base_job in sorted(base_jobs.items()):
        reference = task5.get(key)
        if reference is None:
            # No retained candidates: an empty stage-B output is sufficient.
            reference = {
                "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
                "items": [],
            }
        final = copy.deepcopy(reference)
        if key in split_packets:
            replacement = {
                str(row["candidate_id"]): row
                for row in split_outputs.get(key, {"items": []})["items"]
            }
            if replacement:
                final["items"] = [
                    copy.deepcopy(
                        replacement.get(str(row["candidate_id"]), row)
                    )
                    for row in reference["items"]
                ]
                split_used += 1
            else:
                split_fallback += 1
        composed = true_north.compose_multipass_output(
            base_job,
            dispositions[key],
            final,
            None,
            stage_b_mode="adjudication",
        )
        for item in composed["items"]:
            candidate = candidate_map[str(item["candidate_id"])]
            for atomic in item["atomic_claims"]:
                atomic["evidence_text"] = str(candidate["evidence_text"])
                atomic["evidence_start"] = int(candidate["evidence_start"])
                atomic["evidence_end"] = int(candidate["evidence_end"])
            predictions.append(item)
    span_rows: list[dict[str, Any]] = []
    span_report = {
        "total": 0,
        "kept": 0,
        "already_null": 0,
        "nulled_absent_span": 0,
        "nulled_speaker_self": 0,
    }
    for row in predictions:
        result = apply_actor_span_rule(row["atomic_claims"])
        updated = copy.deepcopy(row)
        updated["atomic_claims"] = list(result.atomics)
        span_rows.append(updated)
        for key, value in result.report.items():
            span_report[key] += value
    final_predictions, suppression = suppress_predictions(
        span_rows, threshold=ACTOR_SUPPRESSION_THRESHOLD
    )
    prediction_doc = {
        "schema_version": true_north.WORK_OUTPUT_SCHEMA_VERSION,
        "suite_id": true_north.SUITE_ID,
        "partition": "sealed-holdout",
        "episode_id": AUTHORIZED_EPISODE_ID,
        "items": final_predictions,
    }
    prediction_doc["output_sha256"] = true_north.sha256_text(
        true_north.dumps_json(prediction_doc)
    )
    output_path = run_root / "blind" / "predictions.private.json"
    true_north._write_json(output_path, prediction_doc, immutable=True)
    gold_after = _gold_state(root)
    if gold_after != gold_before:
        raise TransferError("sealed gold state changed during blind extraction")
    if int(state["usage"]["calls"]) > MAX_CALLS or int(
        state["usage"]["tokens"]
    ) > MAX_TOKENS:
        raise TransferError("blind extraction exceeded Ruling-8 ceiling")
    state["complete"] = True
    state["blind_frozen"] = True
    true_north._multipass_state_write(state_path, state)
    freeze = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": RUN_ID,
        "episode_id": AUTHORIZED_EPISODE_ID,
        "forbidden_episode_id": FORBIDDEN_EPISODE_ID,
        "configuration_sha256": configuration["configuration_sha256"],
        "bundle_sha256": true_north._sha256_file(bundle_path),
        "prediction_path": str(output_path),
        "prediction_file_sha256": true_north._sha256_file(output_path),
        "prediction_semantic_sha256": prediction_doc["output_sha256"],
        "candidate_count": len(final_predictions),
        "atomic_count": sum(
            len(row["atomic_claims"]) for row in final_predictions
        ),
        "usage": copy.deepcopy(state["usage"]),
        "disposition": disposition_report,
        "split_default": {
            "flagged_candidate_count": len(flagged_ids),
            "packet_count": len(split_packets),
            "validated_packet_count": split_used,
            "task5_fallback_packet_count": split_fallback,
            "operational_error": split_error,
        },
        "actor_span": span_report,
        "actor_suppression": suppression,
        "gold_state_before": gold_before,
        "gold_state_at_freeze": gold_after,
        "gold_absent_at_freeze": not any(gold_after.values()),
        "sealed_gold_in_model_input": False,
        "frozen_at": true_north.now_iso(),
    }
    freeze["freeze_sha256"] = true_north.sha256_text(
        true_north.dumps_json(freeze)
    )
    true_north._write_json(freeze_path, freeze, immutable=True)
    return freeze


def verify_blind_freeze(*, suite_root: str | Path) -> dict[str, Any]:
    root = _root(suite_root)
    freeze_path = _run_root(root) / "blind-freeze.json"
    if not freeze_path.is_file():
        raise TransferError("blind freeze is missing")
    freeze = true_north._read_json(freeze_path)
    stored = freeze.pop("freeze_sha256")
    if true_north.sha256_text(true_north.dumps_json(freeze)) != stored:
        raise TransferError("blind freeze hash mismatch")
    freeze["freeze_sha256"] = stored
    if freeze["episode_id"] != AUTHORIZED_EPISODE_ID:
        raise TransferError("blind freeze episode mismatch")
    path = Path(freeze["prediction_path"])
    if true_north._sha256_file(path) != freeze["prediction_file_sha256"]:
        raise TransferError("frozen prediction file hash mismatch")
    return freeze
