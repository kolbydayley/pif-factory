"""Bounded two-stage actor emission and value experiment."""

from __future__ import annotations

import copy
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from .true_north_actor_value import (
    _actor_claim_rows,
    _actor_diagnostics,
    _load_source_predictions,
)
from .true_north_dual_decomposition import GLM_MODEL
from .true_north_semantic_scoring import score_campaign


SCHEMA_VERSION = "pif_true_north_actor_two_stage_v1"
EXPERIMENT_ID = "actor-two-stage-20260729-v1"
SOURCE_RUN_ID = "task5-dual-decomposition-20260729-v1"
NULL_FLOOR_ID = "actor-null-floor-20260729-v1"
MAX_CALLS = 30
MAX_TOKENS = 400_000
MAX_WALL_SECONDS = 2 * 60 * 60
RESERVED_TOKENS_PER_CALL = 14_000
TARGET_PACKETS_PER_STAGE = 8
CUMULATIVE_CALLS_BEFORE_RUN = 185
KNOWN_CUMULATIVE_TOKENS_BEFORE_RUN = 1_427_310
LEDGER_PATH = (
    Path(__file__).resolve().parent.parent
    / "config"
    / "true_north_campaign_budget_ledger.json"
)

EMISSION_SYSTEM_PROMPT = """You decide only whether an atomic podcast claim has an explicitly named
focal non-speaker actor. Do not use tools. Return emit=true only when the exact
evidence names a person or organization whose action, state, finding, position,
or outcome the claim describes. A citation source, incidental mention, claim
subject inferred from context, or direct speaker speaking in their own voice
about themselves does not count. Bias strongly toward emit=false: most claims
have no focal actor. Do not choose or return an actor name in this stage.
Return only the exact schema-valid JSON requested by the packet."""

VALUE_SYSTEM_PROMPT = """You choose the exact actor value only for atomic claims already judged to
have a named focal non-speaker actor. Do not use tools. Return the shortest
exact case-sensitive substring of evidence_text that names the person or
organization whose action, state, finding, position, or outcome the claim
describes. The prior and deterministic span candidates are non-binding hints:
override either when the evidence names a better focal actor. Never return the
direct speaker merely because they uttered the claim, a citation source, or an
incidental mention. Return only the exact schema-valid JSON requested by the
packet."""


class ActorTwoStageError(RuntimeError):
    """Raised when the two-stage actor experiment violates its contract."""


def emission_schema(refs: Sequence[tuple[str, int]]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {"const": SCHEMA_VERSION},
            "items": {
                "type": "array",
                "minItems": len(refs),
                "maxItems": len(refs),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["candidate_id", "claim_index", "emit"],
                    "properties": {
                        "candidate_id": {
                            "type": "string",
                            "enum": sorted({row[0] for row in refs}),
                        },
                        "claim_index": {"type": "integer", "minimum": 0},
                        "emit": {"type": "boolean"},
                    },
                },
            },
        },
    }


def value_schema(refs: Sequence[tuple[str, int]]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {"const": SCHEMA_VERSION},
            "items": {
                "type": "array",
                "minItems": len(refs),
                "maxItems": len(refs),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "candidate_id",
                        "claim_index",
                        "reported_actor",
                    ],
                    "properties": {
                        "candidate_id": {
                            "type": "string",
                            "enum": sorted({row[0] for row in refs}),
                        },
                        "claim_index": {"type": "integer", "minimum": 0},
                        "reported_actor": {
                            "type": "string",
                            "minLength": 1,
                        },
                    },
                },
            },
        },
    }


def validate_emission_output(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    true_north._validate_schema(
        output, packet["output_schema"], path="actor_emission"
    )
    expected = {
        (str(row["candidate_id"]), int(row["claim_index"]))
        for row in packet["input"]["claims"]
    }
    seen = {
        (str(row["candidate_id"]), int(row["claim_index"]))
        for row in output["items"]
    }
    if seen != expected:
        raise ActorTwoStageError(
            "emission output does not exactly cover packet claims"
        )


def validate_value_output(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    true_north._validate_schema(
        output, packet["output_schema"], path="actor_value"
    )
    inputs = {
        (str(row["candidate_id"]), int(row["claim_index"])): row
        for row in packet["input"]["claims"]
    }
    seen: set[tuple[str, int]] = set()
    for row in output["items"]:
        ref = (str(row["candidate_id"]), int(row["claim_index"]))
        if ref not in inputs or ref in seen:
            raise ActorTwoStageError("value output claim scope is invalid")
        seen.add(ref)
        actor = str(row["reported_actor"])
        if actor not in str(inputs[ref]["evidence_text"]):
            raise ActorTwoStageError(
                "reported_actor is not an exact evidence substring"
            )
    if seen != set(inputs):
        raise ActorTwoStageError(
            "value output does not exactly cover emitted claims"
        )


def span_candidates(
    evidence_text: str,
    prior: str | None,
) -> list[str]:
    """Return deterministic exact-substring name hints, never actor truth."""

    candidates: list[str] = []
    pattern = re.compile(
        r"\b(?:[A-Z][A-Za-z0-9&.'-]*|[A-Z]{2,})"
        r"(?:\s+(?:[A-Z][A-Za-z0-9&.'-]*|[A-Z]{2,})){0,4}\b"
    )
    for match in pattern.finditer(evidence_text):
        value = match.group(0).strip()
        if value and value not in candidates:
            candidates.append(value)
    if prior:
        index = evidence_text.casefold().find(prior.casefold())
        if index >= 0:
            value = evidence_text[index : index + len(prior)]
            if value in candidates:
                candidates.remove(value)
            candidates.insert(0, value)
    return candidates[:20]


def _batch_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    stage: str,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    batch_size = math.ceil(len(rows) / TARGET_PACKETS_PER_STAGE)
    packets: list[dict[str, Any]] = []
    for start in range(0, len(rows), batch_size):
        batch = [copy.deepcopy(row) for row in rows[start : start + batch_size]]
        refs = [
            (str(row["candidate_id"]), int(row["claim_index"]))
            for row in batch
        ]
        packets.append(
            {
                "schema_version": SCHEMA_VERSION,
                "suite_id": true_north.SUITE_ID,
                "actor_stage": stage,
                "task": (
                    "Decide emit or null for every claim."
                    if stage == "emission"
                    else "Choose the exact focal actor value for every claim."
                ),
                "output_schema": (
                    emission_schema(refs)
                    if stage == "emission"
                    else value_schema(refs)
                ),
                "input": {"claims": batch},
            }
        )
    return packets


def _campaign_context(
    root: Path,
) -> tuple[
    dict[tuple[str, str], dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, Any],
]:
    _source_root, outputs, provenance = _load_source_predictions(
        root, SOURCE_RUN_ID
    )
    manifest = true_north._read_json(root / "manifest.json")
    candidates: dict[str, dict[str, Any]] = {}
    for bundle_row in manifest["bundles"]:
        if str(bundle_row["episode_id"]) not in provenance["episode_ids"]:
            continue
        bundle = true_north._read_json(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            candidates[str(candidate["candidate_id"])] = dict(candidate)
    return outputs, candidates, provenance


def _speaker_maps(
    root: Path,
    episode_ids: Sequence[str],
) -> dict[str, Any]:
    manifest = true_north._read_json(root / "manifest.json")
    result: dict[str, Any] = {}
    for bundle_row in manifest["bundles"]:
        if str(bundle_row["episode_id"]) not in episode_ids:
            continue
        bundle = true_north._read_json(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            result[str(candidate["candidate_id"])] = (
                bundle["episode_context"].get("speaker_map", [])
            )
    return result


def measure_always_null_floor(
    *,
    suite_root: str | Path,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    outputs, _candidates, provenance = _campaign_context(root)
    predictions: list[dict[str, Any]] = []
    for output in outputs.values():
        value = copy.deepcopy(output)
        for item in value["items"]:
            for atomic in item["atomic_claims"]:
                atomic["reported_actor"] = None
        predictions.extend(value["items"])
    ids = {str(row["candidate_id"]) for row in predictions}
    consensus_document = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred_document = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    consensus = [
        row for row in consensus_document["items"]
        if str(row["candidate_id"]) in ids
    ]
    preferred = [
        row for row in preferred_document["items"]
        if str(row["candidate_id"]) in ids
    ]
    score = score_campaign(
        predictions,
        consensus,
        preferred,
        speaker_maps_by_candidate=_speaker_maps(
            root, provenance["episode_ids"]
        ),
    )
    document = {
        "schema_version": SCHEMA_VERSION,
        "floor_id": NULL_FLOOR_ID,
        "source_run_id": SOURCE_RUN_ID,
        "exact_scorer_reported_actor_floor": score["aggregate"][
            "reported_actor_exactness"
        ],
        "hallucination_rate_proxy": score["aggregate"][
            "hallucination_rate_proxy"
        ],
        "repaired_gold_global_null_prevalence": 0.698217,
        "interpretation": (
            "The exact Search scorer floor is lower than global gold null "
            "prevalence because atomic-count/alignment misses remain in its "
            "reported-actor denominator."
        ),
        "model_calls": 0,
        "holdout_opened": False,
        "production_mutation": False,
    }
    document["floor_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = root / "multipass" / "diagnostics" / f"{NULL_FLOOR_ID}.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "floor_path": str(path)}


def _budget_ledger() -> tuple[dict[str, Any], str]:
    ledger = true_north._read_json(LEDGER_PATH)
    experiments = {
        str(row["experiment_id"]): row
        for row in ledger["declared_experiments"]
    }
    stage_b = experiments.get("stage-b-count-first-20260729-v1")
    actor = experiments.get(EXPERIMENT_ID)
    if (
        stage_b is None
        or stage_b.get("status") != "budget_terminal_partial"
        or actor is None
        or int(actor["max_calls"]) != MAX_CALLS
        or int(actor["max_tokens"]) != MAX_TOKENS
        or str(actor["model"]) != GLM_MODEL
    ):
        raise ActorTwoStageError(
            "budget ledger does not authorize sequenced actor run"
        )
    return ledger, true_north.sha256_text(
        true_north.dumps_json(ledger)
    )


def run_actor_two_stage_experiment(
    *,
    suite_root: str | Path,
    run_id: str | None = None,
    workers: int = 3,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    runner: Any | None = None,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise ActorTwoStageError("suite verification failed")
    ledger, ledger_sha = _budget_ledger()
    floor = measure_always_null_floor(suite_root=root)
    ledger_snapshot = (
        root
        / "multipass"
        / "budget-ledger"
        / "actor-two-stage-20260729-v1.json"
    )
    true_north._write_json(ledger_snapshot, ledger, immutable=True)
    outputs, candidates, provenance = _campaign_context(root)
    rows = _actor_claim_rows(outputs=outputs, candidates=candidates)
    emission_packets = _batch_rows(rows, stage="emission")
    configuration = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "suite_id": root.name,
        "model": GLM_MODEL,
        "source": provenance,
        "atomic_claim_count": len(rows),
        "emission_packet_count": len(emission_packets),
        "emission_prompt_sha256": true_north.sha256_text(
            EMISSION_SYSTEM_PROMPT
        ),
        "value_prompt_sha256": true_north.sha256_text(VALUE_SYSTEM_PROMPT),
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
            "max_wall_seconds": MAX_WALL_SECONDS,
        },
        "budget_ledger": {
            "ledger_sha256": ledger_sha,
            "snapshot_path": str(ledger_snapshot),
            "cumulative_calls_before_run": CUMULATIVE_CALLS_BEFORE_RUN,
            "campaign_total_policy": "report_only_no_hard_stop",
        },
        "null_floor": {
            "floor_sha256": floor["floor_sha256"],
            "exact_scorer_floor": floor[
                "exact_scorer_reported_actor_floor"
            ],
        },
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(configuration)
    )
    resolved_run_id = run_id or (
        "task6-actor-two-stage-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + configuration["configuration_sha256"][:8]
    )
    run_root = root / "multipass" / "runs" / resolved_run_id
    config_path = run_root / "configuration.json"
    state_path = run_root / "state.json"
    if config_path.is_file():
        if true_north._read_json(config_path) != configuration:
            raise ActorTwoStageError("actor resume configuration drift")
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

    emission_outputs = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="adjudication",
        artifact_stage="actor-emission",
        jobs=[
            ("search", f"batch-{index:03d}", packet)
            for index, packet in enumerate(emission_packets)
        ],
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=GLM_MODEL,
        reserved_tokens_per_call=RESERVED_TOKENS_PER_CALL,
        validator_override=validate_emission_output,
        system_prompt_override=EMISSION_SYSTEM_PROMPT,
    )
    emission: dict[tuple[str, int], bool] = {}
    for output in emission_outputs.values():
        for row in output["items"]:
            emission[(str(row["candidate_id"]), int(row["claim_index"]))] = (
                bool(row["emit"])
            )
    expected = {
        (str(row["candidate_id"]), int(row["claim_index"]))
        for row in rows
    }
    if set(emission) != expected:
        raise ActorTwoStageError("emission decisions are incomplete")

    value_rows = []
    for row in rows:
        ref = (str(row["candidate_id"]), int(row["claim_index"]))
        if not emission[ref]:
            continue
        value_rows.append(
            {
                **copy.deepcopy(row),
                "span_candidates": span_candidates(
                    str(row["evidence_text"]),
                    (
                        str(row["candidate_prior"])
                        if row.get("candidate_prior")
                        else None
                    ),
                ),
            }
        )
    value_packets = _batch_rows(value_rows, stage="value")
    value_outputs = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="adjudication",
        artifact_stage="actor-value-two-stage",
        jobs=[
            ("search", f"batch-{index:03d}", packet)
            for index, packet in enumerate(value_packets)
        ],
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=GLM_MODEL,
        reserved_tokens_per_call=RESERVED_TOKENS_PER_CALL,
        validator_override=validate_value_output,
        system_prompt_override=VALUE_SYSTEM_PROMPT,
    )
    values: dict[tuple[str, int], str] = {}
    for output in value_outputs.values():
        for row in output["items"]:
            values[(str(row["candidate_id"]), int(row["claim_index"]))] = (
                str(row["reported_actor"])
            )
    emitted = {ref for ref, decision in emission.items() if decision}
    if set(values) != emitted:
        raise ActorTwoStageError("value decisions do not cover emitted claims")

    for key, source in outputs.items():
        output = copy.deepcopy(source)
        for item in output["items"]:
            candidate_id = str(item["candidate_id"])
            for claim_index, atomic in enumerate(item["atomic_claims"]):
                ref = (candidate_id, claim_index)
                atomic["reported_actor"] = (
                    values[ref] if emission[ref] else None
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
    state["atomic_claim_count"] = len(rows)
    state["emitted_claim_count"] = len(emitted)
    state["emission_packet_count"] = len(emission_packets)
    state["value_packet_count"] = len(value_packets)
    true_north._multipass_state_write(state_path, state)
    result = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": resolved_run_id,
        "configuration_sha256": configuration[
            "configuration_sha256"
        ],
        "complete": True,
        "atomic_claim_count": len(rows),
        "emitted_claim_count": len(emitted),
        "emission_packet_count": len(emission_packets),
        "value_packet_count": len(value_packets),
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


def score_actor_two_stage_experiment(
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
        raise ActorTwoStageError("actor two-stage run is incomplete")
    predictions: list[dict[str, Any]] = []
    for episode_id in configuration["source"]["episode_ids"]:
        for path in sorted(
            (run_root / "outputs" / "composed" / episode_id).glob(
                "*/validated.private.json"
            )
        ):
            predictions.extend(true_north._read_json(path)["items"])
    ids = {str(row["candidate_id"]) for row in predictions}
    consensus_document = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred_document = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    consensus = [
        row for row in consensus_document["items"]
        if str(row["candidate_id"]) in ids
    ]
    preferred = [
        row for row in preferred_document["items"]
        if str(row["candidate_id"]) in ids
    ]
    score = score_campaign(
        predictions,
        consensus,
        preferred,
        speaker_maps_by_candidate=_speaker_maps(
            root, configuration["source"]["episode_ids"]
        ),
    )
    prediction_map = {
        str(row["candidate_id"]): row for row in predictions
    }
    preferred_map = {
        str(row["candidate_id"]): row for row in preferred
    }
    diagnostics = _actor_diagnostics(
        candidate_scores=score["candidates"],
        predictions=prediction_map,
        preferred=preferred_map,
    )
    aggregate = score["aggregate"]
    null_floor = measure_always_null_floor(suite_root=root)
    composite = float(aggregate["reported_actor_exactness"])
    document = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        **diagnostics,
        "composite_reported_actor_exactness": composite,
        "reported_actor_gate": 0.903182,
        "always_null_exact_scorer_floor": null_floor[
            "exact_scorer_reported_actor_floor"
        ],
        "beats_null_floor": (
            composite
            > float(null_floor["exact_scorer_reported_actor_floor"])
        ),
        "hallucination_rate_proxy": aggregate[
            "hallucination_rate_proxy"
        ],
        "lane_stop_required": (
            composite
            < float(null_floor["exact_scorer_reported_actor_floor"])
        ),
        "emitted_claim_count": result["emitted_claim_count"],
        "atomic_claim_count": result["atomic_claim_count"],
        "usage": result["usage"],
        "cumulative_calls_after_run": result[
            "cumulative_calls_after_run"
        ],
        "known_cumulative_tokens_after_run": result[
            "known_cumulative_tokens_after_run"
        ],
        "known_token_accounting_excludes_actor_gold_repair": True,
        "holdout_opened": False,
        "production_mutation": False,
    }
    document["score_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = run_root / "actor-two-stage-score.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "score_path": str(path)}
