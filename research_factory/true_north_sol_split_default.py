"""Gold-author-class probe over frozen split-default packets."""

from __future__ import annotations

import copy
import time
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
    _load_search_context,
    _reference_artifacts,
)
from .true_north_input_split_default import (
    FLAGGED_SYSTEM_PROMPT,
    merge_adjudication_schema,
)
from .true_north_semantic_scoring import score_campaign
from .true_north_spark_split_default import (
    SOURCE_RUN_ID,
    _atomic_subset,
    _provider_packet,
    _source_packets,
    validate_provider_output,
)


SCHEMA_VERSION = "pif_true_north_sol_split_default_v1"
EXPERIMENT_ID = "stage-b-sol-input-split-default-20260729-v1"
MODEL = "gpt-5.6-sol"
MODEL_LANE = "codex_subscription_ephemeral"
MAX_CALLS = 16
MAX_TOKENS = 450_000
MAX_WALL_SECONDS = 4 * 60 * 60
RESERVED_TOKENS_PER_CALL = 25_000
CUMULATIVE_CALLS_BEFORE_RUN = 250
KNOWN_CUMULATIVE_TOKENS_BEFORE_RUN = 2_326_768
LEDGER_PATH = (
    Path(__file__).resolve().parent.parent
    / "config"
    / "true_north_campaign_budget_ledger.json"
)


class SolSplitDefaultError(RuntimeError):
    """Raised when the bounded sol probe violates its frozen contract."""


def pack_provider_envelopes(
    source_packets: Sequence[Mapping[str, Any]],
) -> list[list[dict[str, Any]]]:
    """Pack 19 immutable semantic packets into exactly 16 calls."""

    by_episode: dict[str, list[dict[str, Any]]] = {}
    for row in source_packets:
        episode_id = str(row["packet_key"]).split("/", 1)[0]
        by_episode.setdefault(episode_id, []).append(copy.deepcopy(row))
    envelopes: list[list[dict[str, Any]]] = []
    pair_budget = len(source_packets) - MAX_CALLS
    for episode_id in sorted(
        by_episode,
        key=lambda value: (
            sum(
                int(row["candidate_count"])
                for row in by_episode[value]
            ),
            value,
        ),
    ):
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
    if pair_budget or len(envelopes) != MAX_CALLS:
        raise SolSplitDefaultError(
            "deterministic packing did not produce 16 envelopes"
        )
    envelopes.sort(
        key=lambda rows: tuple(str(row["packet_key"]) for row in rows)
    )
    return envelopes


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
        or row.get("model") != MODEL
        or row.get("provider_lane") != MODEL_LANE
        or int(row["max_calls"]) != MAX_CALLS
        or int(row["max_tokens"]) != MAX_TOKENS
        or int(row["reserved_tokens_per_call"])
        != RESERVED_TOKENS_PER_CALL
    ):
        raise SolSplitDefaultError(
            "sol probe budget is not declared exactly"
        )
    return ledger, true_north.sha256_text(
        true_north.dumps_json(ledger)
    )


def prepare_sol_split_default_probe(
    *,
    suite_root: str | Path,
    run_id: str,
) -> dict[str, Any]:
    """Perform the whole-run preflight and freeze all call artifacts."""

    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise SolSplitDefaultError(
            "suite verification failed before sol probe"
        )
    manifest = true_north._read_json(root / "manifest.json")
    if manifest["measurement_contract"]["version"] != CONTRACT_VERSION:
        raise SolSplitDefaultError(
            "sol probe requires the decoupled measurement contract"
        )
    ledger, ledger_sha = _ledger()
    source_packets = _source_packets(root)
    envelopes = pack_provider_envelopes(source_packets)
    provider_packets = [
        _provider_packet(f"sol-split-{index:03d}", rows)
        for index, rows in enumerate(envelopes)
    ]
    flagged_ids = {
        str(candidate["candidate_id"])
        for source in source_packets
        for candidate in source["packet"]["input"]["candidates"]
    }
    reserved_total = (
        len(provider_packets) * RESERVED_TOKENS_PER_CALL
    )
    if len(flagged_ids) != 106:
        raise SolSplitDefaultError(
            "sol probe requires exactly 106 flagged candidates"
        )
    if len(provider_packets) > MAX_CALLS or reserved_total > MAX_TOKENS:
        raise SolSplitDefaultError(
            "whole-run packet-budget preflight failed"
        )
    run_root = root / "multipass" / "runs" / run_id
    packet_root = run_root / "packets" / "sol-input-split-default"
    schema_root = run_root / "schemas" / "sol-input-split-default"
    packet_hashes: list[str] = []
    for index, packet in enumerate(provider_packets):
        name = f"envelope-{index:03d}"
        packet_path = packet_root / f"{name}.private.json"
        schema_path = schema_root / f"{name}.json"
        true_north._write_json(packet_path, packet, immutable=True)
        true_north._write_json(
            schema_path, packet["output_schema"], immutable=True
        )
        packet_hashes.append(true_north._sha256_file(packet_path))
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
        "run_id": run_id,
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "measurement_contract_sha256": manifest[
            "measurement_contract"
        ]["contract_sha256"],
        "episode_ids": list(SEARCH_EPISODE_IDS),
        "model": MODEL,
        "model_lane": MODEL_LANE,
        "model_reasoning_effort": "high",
        "invocation": "codex exec --ephemeral",
        "source_run_id": SOURCE_RUN_ID,
        "source_semantic_packet_count": len(source_packets),
        "source_semantic_packet_hashes": {
            str(row["packet_key"]): str(row["packet_sha256"])
            for row in source_packets
        },
        "provider_envelope_count": len(provider_packets),
        "provider_envelope_packet_hashes": packet_hashes,
        "system_prompt_sha256": true_north.sha256_text(
            FLAGGED_SYSTEM_PROMPT
        ),
        "unflagged_source_run_id": REFERENCE_RUN_ID,
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
            "max_wall_seconds": MAX_WALL_SECONDS,
            "reserved_tokens_per_call": RESERVED_TOKENS_PER_CALL,
            "whole_run_reserved_tokens": reserved_total,
        },
        "budget_ledger_sha256": ledger_sha,
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(configuration)
    )
    config_path = run_root / "configuration.json"
    if config_path.is_file():
        if true_north._read_json(config_path) != configuration:
            raise SolSplitDefaultError("resume configuration drift")
    else:
        true_north._write_json(
            config_path, configuration, immutable=True
        )
    state_path = run_root / "state.json"
    if not state_path.is_file():
        state = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "configuration_sha256": configuration[
                "configuration_sha256"
            ],
            "budget": copy.deepcopy(configuration["budget"]),
            "completed": [],
            "failures": [],
            "usage": {
                "calls": 0,
                "tokens": 0,
                "wall_seconds": 0.0,
            },
            "complete": False,
        }
        true_north._multipass_state_write(state_path, state)
    preflight = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "model": MODEL,
        "model_lane": MODEL_LANE,
        "source_semantic_packet_count": len(source_packets),
        "flagged_candidate_count": len(flagged_ids),
        "provider_envelope_count": len(provider_packets),
        "reserved_tokens_per_call": RESERVED_TOKENS_PER_CALL,
        "whole_run_reserved_tokens": reserved_total,
        "within_call_ceiling": len(provider_packets) <= MAX_CALLS,
        "within_token_ceiling": reserved_total <= MAX_TOKENS,
        "provider_calls_made": 0,
        "holdout_opened": False,
    }
    preflight["preflight_sha256"] = true_north.sha256_text(
        true_north.dumps_json(preflight)
    )
    true_north._write_json(
        run_root / "preflight.json", preflight, immutable=False
    )
    return {**preflight, "run_root": str(run_root)}


def _usage_tokens(receipt: Mapping[str, Any]) -> int:
    usage = receipt.get("usage", {})
    return int(
        usage.get("total_tokens")
        or (
            int(usage.get("input_tokens") or 0)
            + int(usage.get("output_tokens") or 0)
        )
    )


def run_sol_split_default_probe(
    *,
    suite_root: str | Path,
    run_id: str,
    timeout_seconds: int = 1800,
    codex_binary: str = "codex",
) -> dict[str, Any]:
    """Execute one sequential, budget-enforced sol run."""

    prepared = prepare_sol_split_default_probe(
        suite_root=suite_root, run_id=run_id
    )
    root = Path(suite_root).expanduser().resolve()
    run_root = Path(prepared["run_root"])
    state_path = run_root / "state.json"
    state = true_north._read_json(state_path)
    started = time.monotonic()
    packet_paths = sorted(
        (run_root / "packets" / "sol-input-split-default").glob(
            "*.private.json"
        )
    )
    for packet_path in packet_paths:
        name = packet_path.name.removesuffix(".private.json")
        if name in set(state["completed"]):
            continue
        if int(state["usage"]["calls"]) >= MAX_CALLS:
            break
        if (
            int(state["usage"]["tokens"])
            + RESERVED_TOKENS_PER_CALL
            > MAX_TOKENS
        ):
            break
        if float(state["usage"]["wall_seconds"]) >= MAX_WALL_SECONDS:
            break
        output_dir = (
            run_root
            / "outputs"
            / "sol-input-split-default"
            / name
        )
        call_started = time.monotonic()
        failure = None
        try:
            receipt = true_north._run_codex_gold_packet(
                packet_path=packet_path,
                schema_path=(
                    run_root
                    / "schemas"
                    / "sol-input-split-default"
                    / f"{name}.json"
                ),
                output_dir=output_dir,
                timeout_seconds=timeout_seconds,
                codex_binary=codex_binary,
                validator=validate_provider_output,
                prompt_prefix=FLAGGED_SYSTEM_PROMPT,
            )
        except Exception as exc:
            failure = f"{type(exc).__name__}: {exc}"
            events_path = output_dir / "events.private.jsonl"
            events = (
                events_path.read_text(encoding="utf-8")
                if events_path.is_file()
                else ""
            )
            receipt = {
                "ok": False,
                "model": MODEL,
                "model_lane": MODEL_LANE,
                "effort": "high",
                "elapsed_seconds": round(
                    time.monotonic() - call_started, 3
                ),
                "usage": true_north._codex_usage_from_jsonl(events),
                "packet_sha256": true_north._sha256_file(packet_path),
                "failure": failure,
            }
            true_north._write_json(
                output_dir / "failure-receipt.json",
                receipt,
                immutable=True,
            )
        state["usage"]["calls"] = int(state["usage"]["calls"]) + 1
        state["usage"]["tokens"] = (
            int(state["usage"]["tokens"]) + _usage_tokens(receipt)
        )
        state["usage"]["wall_seconds"] = round(
            float(state["usage"]["wall_seconds"])
            + float(receipt.get("elapsed_seconds") or 0.0),
            3,
        )
        probe_receipt = {
            "schema_version": SCHEMA_VERSION,
            "provider_envelope": name,
            "model": MODEL,
            "model_lane": MODEL_LANE,
            "effort": "high",
            "ephemeral": True,
            "ok": bool(receipt.get("ok")),
            "usage": receipt.get("usage", {}),
            "packet_sha256": true_north._sha256_file(packet_path),
            "base_receipt_sha256": (
                true_north._sha256_file(output_dir / "receipt.json")
                if (output_dir / "receipt.json").is_file()
                else None
            ),
            "failure": failure,
        }
        true_north._write_json(
            output_dir / "probe-receipt.json",
            probe_receipt,
            immutable=True,
        )
        if receipt.get("ok"):
            state["completed"].append(name)
        else:
            state["failures"].append(
                {"provider_envelope": name, "failure": failure}
            )
        true_north._multipass_state_write(state_path, state)
        if int(state["usage"]["tokens"]) > MAX_TOKENS:
            break
        if time.monotonic() - started > MAX_WALL_SECONDS:
            break
    complete = len(state["completed"]) == len(packet_paths)
    state["complete"] = complete
    true_north._multipass_state_write(state_path, state)
    terminal = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "model": MODEL,
        "model_lane": MODEL_LANE,
        "complete": complete,
        "validated_provider_envelopes": len(state["completed"]),
        "expected_provider_envelopes": len(packet_paths),
        "failure_count": len(state["failures"]),
        "usage": state["usage"],
        "within_call_ceiling": int(state["usage"]["calls"]) <= MAX_CALLS,
        "within_token_ceiling": int(state["usage"]["tokens"]) <= MAX_TOKENS,
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
    terminal["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(terminal)
    )
    true_north._write_json(
        run_root / "result.json", terminal, immutable=False
    )
    return {**terminal, "run_root": str(run_root)}


def compose_and_score_sol_probe(
    *,
    suite_root: str | Path,
    run_id: str,
) -> dict[str, Any]:
    """Compose a complete sol run with frozen Task-5 controls and score it."""

    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    result = true_north._read_json(run_root / "result.json")
    if not result["complete"]:
        raise SolSplitDefaultError(
            "incomplete sol probe is not acceptance-eligible"
        )
    output_paths = sorted(
        (run_root / "outputs" / "sol-input-split-default").glob(
            "*/validated.private.json"
        )
    )
    sol_by_candidate = {
        str(row["candidate_id"]): {
            key: value
            for key, value in row.items()
            if key != "merge_reason"
        }
        for path in output_paths
        for row in true_north._read_json(path)["items"]
    }
    flagged_ids = {
        str(candidate["candidate_id"])
        for source in _source_packets(root)
        for candidate in source["packet"]["input"]["candidates"]
    }
    if set(sol_by_candidate) != flagged_ids:
        raise SolSplitDefaultError(
            "validated sol output lacks full flagged coverage"
        )
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        _reference_provenance,
    ) = _reference_artifacts(root)
    manifest = true_north._read_json(root / "manifest.json")
    base_jobs, dispositions, _candidates = _load_search_context(
        root, manifest
    )
    predictions: list[dict[str, Any]] = []
    composed_paths: list[Path] = []
    for key, reference in sorted(reference_outputs.items()):
        adjudication = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                copy.deepcopy(
                    sol_by_candidate.get(
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
        path = (
            run_root
            / "outputs"
            / "composed"
            / key[0]
            / key[1]
            / "validated.private.json"
        )
        true_north._write_json(path, output, immutable=True)
        composed_paths.append(path)
        predictions.extend(output["items"])
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
        source_paths=[*output_paths, *composed_paths, span_path],
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
        "flagged_candidate_count": len(flagged_ids),
        "shared_sol_glm_candidate_count": len(shared_ids),
        "sol_shared_acceptable_atomic_count_rate": _atomic_subset(
            consensus=consensus,
            predictions=prediction_map,
            candidate_ids=shared_ids,
        ),
        "glm_shared_acceptable_atomic_count_rate": _atomic_subset(
            consensus=consensus,
            predictions=glm_map,
            candidate_ids=shared_ids,
        ),
        "full_flagged_sol_acceptable_atomic_count_rate": _atomic_subset(
            consensus=consensus,
            predictions=prediction_map,
            candidate_ids=flagged_ids,
        ),
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
        "model": MODEL,
        "model_lane": MODEL_LANE,
        "complete": True,
        "acceptance_eligible": True,
        "composed": {
            "acceptable_atomic_count_rate": aggregate[
                "acceptable_atomic_count_rate"
            ],
            "aligned_candidate_count": len(aligned_ids),
            "aligned_claim_text_faithfulness": aligned_faithfulness,
            "faithfulness": aggregate[
                "claim_text_faithfulness_proxy"
            ],
            "candidate_state_macro_f1": aggregate[
                "consensus_candidate_state_macro_f1"
            ],
            "hallucination_rate_proxy": aggregate[
                "hallucination_rate_proxy"
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
    path = run_root / "sol-split-default-score.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "score_path": str(path)}


def _atomic_error_directions(
    *,
    consensus: Mapping[str, Any],
    predictions: Mapping[str, Mapping[str, Any]],
    candidate_ids: set[str],
) -> dict[str, int]:
    consensus_by_id = {
        str(row["candidate_id"]): row for row in consensus["items"]
    }
    counts = {"acceptable": 0, "under": 0, "over": 0}
    for candidate_id in sorted(candidate_ids):
        gold = consensus_by_id[candidate_id]
        predicted_count = len(
            predictions[candidate_id]["atomic_claims"]
        )
        minimum = int(gold["minimum_atomic_count"])
        maximum = int(gold["maximum_atomic_count"])
        if minimum <= predicted_count <= maximum:
            counts["acceptable"] += 1
        elif predicted_count < minimum:
            counts["under"] += 1
        else:
            counts["over"] += 1
    return counts


def finalize_partial_sol_probe(
    *,
    suite_root: str | Path,
    run_id: str,
) -> dict[str, Any]:
    """Freeze an incomplete run as a diagnostic Task-5 fallback composition."""

    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    result = true_north._read_json(run_root / "result.json")
    if result["complete"]:
        raise SolSplitDefaultError(
            "complete sol runs use the acceptance-eligible scorer"
        )
    output_paths = sorted(
        (run_root / "outputs" / "sol-input-split-default").glob(
            "*/validated.private.json"
        )
    )
    sol_by_candidate = {
        str(row["candidate_id"]): {
            key: value
            for key, value in row.items()
            if key != "merge_reason"
        }
        for path in output_paths
        for row in true_north._read_json(path)["items"]
    }
    if not sol_by_candidate:
        raise SolSplitDefaultError(
            "partial sol run has no validated candidates"
        )
    manifest = true_north._read_json(root / "manifest.json")
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        _reference_provenance,
    ) = _reference_artifacts(root)
    base_jobs, dispositions, _candidates = _load_search_context(
        root, manifest
    )
    predictions: list[dict[str, Any]] = []
    for key, reference in sorted(reference_outputs.items()):
        adjudication = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                copy.deepcopy(
                    sol_by_candidate.get(
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
            "validated sol candidates; frozen Task-5 for every "
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
        source_paths=[*output_paths, span_path],
        actor_span_applied=True,
        actor_span_report=span_report,
    )
    prediction_map = {
        str(row["candidate_id"]): row for row in span_predictions
    }
    private = true_north._read_json(Path(decoupled["output_path"]))
    candidate_scores = {
        str(row["candidate_id"]): row
        for row in private["private_candidate_scores"]
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
    measured_ids = set(sol_by_candidate)
    shared_ids = measured_ids & validated_glm_ids
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
        "model": MODEL,
        "model_lane": MODEL_LANE,
        "complete": False,
        "acceptance_eligible": False,
        "terminal_reason": (
            "actual_token_usage_exceeded_declared_ceiling_after_"
            "reserved_call_and_one_output_failed_unchanged_validator"
        ),
        "coverage": {
            "validated_provider_envelopes": result[
                "validated_provider_envelopes"
            ],
            "expected_provider_envelopes": result[
                "expected_provider_envelopes"
            ],
            "validated_sol_candidate_count": len(measured_ids),
            "flagged_candidate_count": 106,
        },
        "partial_fallback_composition": {
            "acceptable_atomic_count_rate": aggregate[
                "acceptable_atomic_count_rate"
            ],
            "aligned_candidate_count": len(aligned_ids),
            "aligned_claim_text_faithfulness": aligned["aggregate"][
                "claim_text_faithfulness_proxy"
            ],
            "faithfulness": aggregate[
                "claim_text_faithfulness_proxy"
            ],
            "candidate_state_macro_f1": aggregate[
                "consensus_candidate_state_macro_f1"
            ],
            "hallucination_rate_proxy": aggregate[
                "hallucination_rate_proxy"
            ],
            "nine_gate_table": decoupled["nine_gate_table"],
            "passed_gate_count": decoupled["passed_gate_count"],
        },
        "same_candidate_comparison": {
            "all_validated_sol_candidate_count": len(measured_ids),
            "sol_all_validated_acceptable_atomic_count_rate": (
                _atomic_subset(
                    consensus=consensus,
                    predictions=prediction_map,
                    candidate_ids=measured_ids,
                )
            ),
            "glm_task5_fallback_on_same_candidates": _atomic_subset(
                consensus=consensus,
                predictions=glm_map,
                candidate_ids=measured_ids,
            ),
            "direct_sol_glm_shared_candidate_count": len(shared_ids),
            "sol_direct_shared_acceptable_atomic_count_rate": (
                _atomic_subset(
                    consensus=consensus,
                    predictions=prediction_map,
                    candidate_ids=shared_ids,
                )
            ),
            "glm_direct_shared_acceptable_atomic_count_rate": (
                _atomic_subset(
                    consensus=consensus,
                    predictions=glm_map,
                    candidate_ids=shared_ids,
                )
            ),
            "sol_error_directions_on_validated_candidates": (
                _atomic_error_directions(
                    consensus=consensus,
                    predictions=prediction_map,
                    candidate_ids=measured_ids,
                )
            ),
        },
        "junk_and_contamination": {
            "intrinsic_junk_escape_count": checkpoint[
                "disposition_gate"
            ]["intrinsic_junk_escape_count"],
            "relational_contamination_count": checkpoint[
                "relational_merge_certification"
            ]["contamination_count"],
        },
        "usage": result["usage"],
        "token_overage": max(
            0, int(result["usage"]["tokens"]) - MAX_TOKENS
        ),
        "cumulative_calls_after_run": result[
            "cumulative_calls_after_run"
        ],
        "known_cumulative_tokens_after_run": result[
            "known_cumulative_tokens_after_run"
        ],
        "passed": False,
        "stop_decomposition_lane": True,
        "holdout_opened": False,
        "production_mutation": False,
    }
    document["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = run_root / "terminal-partial-result.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "result_path": str(path)}
