"""Owner-authorized single-factor count-first Stage-B experiment."""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from . import true_north
from .true_north_dual_decomposition import (
    GLM_MODEL,
    REFERENCE_RUN_ID,
    SEARCH_EPISODE_IDS,
    _load_search_context,
    _reference_artifacts,
)
from .true_north_semantic_scoring import score_campaign


SCHEMA_VERSION = "pif_true_north_count_first_stage_b_v1"
EXPERIMENT_ID = "stage-b-count-first-20260729-v1"
MAX_CALLS = 25
MAX_TOKENS = 300_000
MAX_WALL_SECONDS = 2 * 60 * 60
RESERVED_TOKENS_PER_CALL = 13_000
CUMULATIVE_CALLS_BEFORE_RUN = 160
LEDGER_PATH = (
    Path(__file__).resolve().parent.parent
    / "config"
    / "true_north_campaign_budget_ledger.json"
)

_REMOVED_DEFAULT = (
    "Your default action is to adopt the proposed\n"
    "claim_text verbatim as one atomic claim; most candidates need exactly this."
)
_COUNT_FIRST_REPLACEMENT = (
    "Before drafting, first enumerate how many independently true-or-false\n"
    "conclusions the proposal contains. Then write exactly that many atomic\n"
    "claims, reusing the proposal's own wording for each part. One conclusion\n"
    "requires one claim; two or more conclusions require a split."
)


class CountFirstError(RuntimeError):
    """Raised when the count-first experiment violates its frozen contract."""


def count_first_system_prompt() -> str:
    frozen = true_north.MULTIPASS_SYSTEM_PROMPTS["adjudication"]
    if frozen.count(_REMOVED_DEFAULT) != 1:
        raise CountFirstError(
            "frozen Stage-B adoption default is absent or ambiguous"
        )
    variant = frozen.replace(
        _REMOVED_DEFAULT, _COUNT_FIRST_REPLACEMENT, 1
    )
    if "Your default action is to adopt" in variant:
        raise CountFirstError("count-first prompt retained adoption default")
    return variant


def _budget_ledger() -> tuple[dict[str, Any], str]:
    ledger = true_north._read_json(LEDGER_PATH)
    decisions = {
        str(row["decision_id"]): row
        for row in ledger.get("owner_decisions", [])
    }
    decision = decisions.get("owner-lift-campaign-stop-20260729-v1")
    if (
        decision is None
        or decision["authorizes"]["campaign_total_policy"]
        != "report_only_no_hard_stop"
        or decision["unchanged_controls"][
            "per_experiment_ceiling_required_before_first_call"
        ]
        is not True
    ):
        raise CountFirstError("owner campaign authorization is invalid")
    experiments = {
        str(row["experiment_id"]): row
        for row in ledger.get("declared_experiments", [])
    }
    declared = experiments.get(EXPERIMENT_ID)
    if declared is None:
        raise CountFirstError("count-first experiment is not declared")
    if (
        int(declared["max_calls"]) != MAX_CALLS
        or int(declared["max_tokens"]) != MAX_TOKENS
        or str(declared["model"]) != GLM_MODEL
    ):
        raise CountFirstError("count-first declared budget drifted")
    return ledger, true_north.sha256_text(
        true_north.dumps_json(ledger)
    )


def run_count_first_experiment(
    *,
    suite_root: str | Path,
    run_id: str | None = None,
    workers: int = 3,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    runner: Any | None = None,
) -> dict[str, Any]:
    """Run the full Search fold with only the Stage-B system prompt changed."""

    if workers < 1 or workers > 3:
        raise CountFirstError("count-first workers must be between 1 and 3")
    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise CountFirstError("suite verification failed")
    manifest = true_north._read_json(root / "manifest.json")
    ledger, ledger_sha = _budget_ledger()
    ledger_snapshot = (
        root
        / "multipass"
        / "budget-ledger"
        / "owner-lift-campaign-stop-20260729-v1.json"
    )
    true_north._write_json(ledger_snapshot, ledger, immutable=True)
    if true_north.sha256_text(
        true_north.dumps_json(
            true_north._read_json(ledger_snapshot)
        )
    ) != ledger_sha:
        raise CountFirstError("runtime budget-ledger snapshot hash mismatch")

    base_jobs, dispositions, candidates = _load_search_context(root, manifest)
    (
        _reference_root,
        reference_packets,
        _reference_outputs,
        reference_provenance,
    ) = _reference_artifacts(root)
    if len(reference_packets) != 21:
        raise CountFirstError("frozen Task 5 packet count is not 21")
    prompt = count_first_system_prompt()
    configuration = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "suite_id": root.name,
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "episode_ids": list(SEARCH_EPISODE_IDS),
        "model": GLM_MODEL,
        "single_factor": "stage_b_system_prompt_adoption_default_only",
        "prompt": {
            "frozen_sha256": true_north.sha256_text(
                true_north.MULTIPASS_SYSTEM_PROMPTS["adjudication"]
            ),
            "variant_sha256": true_north.sha256_text(prompt),
            "removed_text_sha256": true_north.sha256_text(
                _REMOVED_DEFAULT
            ),
            "replacement_text_sha256": true_north.sha256_text(
                _COUNT_FIRST_REPLACEMENT
            ),
        },
        "packets": {
            "source_run_id": REFERENCE_RUN_ID,
            "source_configuration_sha256": reference_provenance[
                "configuration_sha256"
            ],
            "packet_count": len(reference_packets),
            "packet_hashes": {
                f"{key[0]}/{key[1]}": true_north.sha256_text(
                    true_north.dumps_json(packet)
                )
                for key, packet in sorted(reference_packets.items())
            },
        },
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
            "max_wall_seconds": MAX_WALL_SECONDS,
        },
        "budget_ledger": {
            "owner_decision_id": "owner-lift-campaign-stop-20260729-v1",
            "ledger_sha256": ledger_sha,
            "snapshot_path": str(ledger_snapshot),
            "campaign_total_policy": "report_only_no_hard_stop",
            "cumulative_calls_before_run": CUMULATIVE_CALLS_BEFORE_RUN,
        },
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(configuration)
    )
    resolved_run_id = run_id or (
        "task5-count-first-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + configuration["configuration_sha256"][:8]
    )
    run_root = root / "multipass" / "runs" / resolved_run_id
    config_path = run_root / "configuration.json"
    state_path = run_root / "state.json"
    if config_path.is_file():
        if true_north._read_json(config_path) != configuration:
            raise CountFirstError("count-first resume configuration drift")
    else:
        true_north._write_json(config_path, configuration, immutable=True)
    if state_path.is_file():
        state = true_north._read_json(state_path)
    else:
        state = {
            "schema_version": SCHEMA_VERSION,
            "run_id": resolved_run_id,
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

    jobs = [
        (key[0], key[1], reference_packets[key])
        for key in sorted(reference_packets)
    ]
    outputs = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="adjudication",
        jobs=jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=GLM_MODEL,
        reserved_tokens_per_call=RESERVED_TOKENS_PER_CALL,
        system_prompt_override=prompt,
    )
    if set(outputs) != set(reference_packets):
        raise CountFirstError("count-first output scope is incomplete")
    for key in sorted(base_jobs):
        composed = true_north.compose_multipass_output(
            base_jobs[key],
            dispositions[key],
            outputs.get(key),
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
            composed,
            immutable=True,
        )
    state["complete"] = True
    state["packet_count"] = len(base_jobs)
    state["stage_b_packet_count"] = len(jobs)
    state["candidate_count"] = len(candidates)
    true_north._multipass_state_write(state_path, state)
    result = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": resolved_run_id,
        "configuration_sha256": configuration["configuration_sha256"],
        "complete": True,
        "candidate_count": len(candidates),
        "packet_count": len(jobs),
        "usage": state["usage"],
        "cumulative_calls_before_run": CUMULATIVE_CALLS_BEFORE_RUN,
        "cumulative_calls_after_run": (
            CUMULATIVE_CALLS_BEFORE_RUN + int(state["usage"]["calls"])
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


def _load_predictions(
    run_root: Path,
    episode_ids: list[str],
) -> list[dict[str, Any]]:
    predictions: list[dict[str, Any]] = []
    for episode_id in episode_ids:
        for path in sorted(
            (run_root / "outputs" / "composed" / episode_id).glob(
                "*/validated.private.json"
            )
        ):
            predictions.extend(true_north._read_json(path)["items"])
    return predictions


def score_count_first_experiment(
    *,
    suite_root: str | Path,
    run_id: str,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    configuration = true_north._read_json(run_root / "configuration.json")
    state = true_north._read_json(run_root / "state.json")
    result = true_north._read_json(run_root / "result.json")
    if (
        configuration.get("schema_version") != SCHEMA_VERSION
        or state.get("complete") is not True
    ):
        raise CountFirstError("count-first run is not complete")
    full = true_north.score_multipass_run(
        run_id=run_id,
        output_root=root.parent,
        suite=root.name,
    )
    predictions = _load_predictions(
        run_root, list(configuration["episode_ids"])
    )
    prediction_map = {
        str(row["candidate_id"]): row for row in predictions
    }
    private_score = true_north._read_json(
        run_root / "score.private.json"
    )
    aligned_ids = {
        str(row["candidate_id"])
        for row in private_score["private_candidate_scores"]
        if row["strictly_scoreable"]
        and row["consensus_state"] == "consensus_value"
        and row["value_state"] == "value"
        and row["atomic_count"]["acceptable_count"]
    }
    aligned_predictions = [
        prediction_map[candidate_id]
        for candidate_id in sorted(aligned_ids)
    ]
    consensus_document = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred_document = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    manifest = true_north._read_json(root / "manifest.json")
    speaker_maps: dict[str, Any] = {}
    for bundle_row in manifest["bundles"]:
        if str(bundle_row["episode_id"]) not in configuration["episode_ids"]:
            continue
        bundle = true_north._read_json(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            speaker_maps[str(candidate["candidate_id"])] = (
                bundle["episode_context"].get("speaker_map", [])
            )
    aligned = score_campaign(
        aligned_predictions,
        consensus_document["items"],
        preferred_document["items"],
        subset_candidate_ids=aligned_ids,
        speaker_maps_by_candidate=speaker_maps,
    )
    baseline = true_north._read_json(
        root
        / "multipass"
        / "runs"
        / REFERENCE_RUN_ID
        / "task5-score.private.json"
    )
    checkpoint = true_north._read_json(
        root
        / "certification"
        / true_north.TASK5_CHECKPOINT_FILENAME
    )
    aggregate = full["aggregate"]
    count_errors = {
        "under": 0,
        "over": 0,
    }
    for row in private_score["private_candidate_scores"]:
        if (
            row["strictly_scoreable"]
            and row["consensus_state"] == "consensus_value"
            and row["value_state"] == "value"
            and not row["atomic_count"]["acceptable_count"]
        ):
            predicted = int(row["atomic_count"]["predicted"])
            if predicted < int(row["atomic_count"]["minimum"]):
                count_errors["under"] += 1
            elif predicted > int(row["atomic_count"]["maximum"]):
                count_errors["over"] += 1
    aligned_aggregate = aligned["aggregate"]
    acceptance = {
        "atomic_count_accuracy_at_least_0_90": (
            float(aggregate["acceptable_atomic_count_rate"]) >= 0.90
        ),
        "aligned_faithfulness_at_least_0_75": (
            float(aligned_aggregate["claim_text_faithfulness_proxy"])
            >= 0.75
        ),
        "intrinsic_junk_escapes_zero": (
            int(checkpoint["intrinsic_disposition_gate"][
                "intrinsic_junk_escape_count"
            ])
            == 0
        ),
        "relational_contamination_zero": (
            int(checkpoint["relational_merge_gate"][
                "contamination_count"
            ])
            == 0
        ),
    }
    document = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "configuration_sha256": configuration[
            "configuration_sha256"
        ],
        "search_fold": {
            "candidate_count": full["candidate_count"],
            "acceptable_atomic_count_rate": aggregate[
                "acceptable_atomic_count_rate"
            ],
            "claim_text_faithfulness_full_fold": aggregate[
                "claim_text_faithfulness_proxy"
            ],
            "speaker_exactness_full_fold": aggregate[
                "speaker_exactness"
            ],
            "count_error_direction": count_errors,
        },
        "aligned_subset": {
            "candidate_count": len(aligned_ids),
            "claim_text_faithfulness": aligned_aggregate[
                "claim_text_faithfulness_proxy"
            ],
            "speaker_exactness": aligned_aggregate[
                "speaker_exactness"
            ],
            "hallucination_rate_proxy": aligned_aggregate[
                "hallucination_rate_proxy"
            ],
        },
        "frozen_task5_reference": {
            "acceptable_atomic_count_rate": baseline["search_fold"][
                "atomic_count_accuracy"
            ],
            "claim_text_faithfulness_full_fold": baseline[
                "search_fold"
            ]["claim_text_faithfulness"],
            "speaker_exactness_full_fold": baseline["search_fold"][
                "aggregate"
            ]["speaker_exactness"],
            "aligned_claim_text_faithfulness": 0.7449,
        },
        "junk_and_contamination": {
            "measurement_contract": (
                "Stage-A dispositions are byte-identical to the certified "
                "checkpoint; the Stage-B prompt cannot change candidate state."
            ),
            "intrinsic_junk_escape_count": checkpoint[
                "intrinsic_disposition_gate"
            ]["intrinsic_junk_escape_count"],
            "relational_contamination_count": checkpoint[
                "relational_merge_gate"
            ]["contamination_count"],
            "checkpoint_sha256": true_north._sha256_file(
                root
                / "certification"
                / true_north.TASK5_CHECKPOINT_FILENAME
            ),
        },
        "acceptance": acceptance,
        "passed": all(acceptance.values()),
        "usage": result["usage"],
        "cumulative_calls_after_run": result[
            "cumulative_calls_after_run"
        ],
        "known_cumulative_tokens_after_run": (
            1_177_814 + int(result["usage"]["tokens"])
        ),
        "known_token_accounting_excludes_actor_gold_repair": True,
        "holdout_opened": False,
        "production_mutation": False,
    }
    document["score_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = run_root / "count-first-score.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "score_path": str(path)}
