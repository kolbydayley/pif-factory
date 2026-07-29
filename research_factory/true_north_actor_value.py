"""Corrected Task 6: decide a literal reported-actor value or null."""

from __future__ import annotations

import copy
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from .true_north_dual_decomposition import GLM_MODEL


SCHEMA_VERSION = "pif_true_north_actor_value_v1"
MAX_CALLS = 25
MAX_TOKENS = 350_000
MAX_WALL_SECONDS = 2 * 60 * 60
TARGET_PACKET_COUNT = 8
RESERVED_TOKENS_PER_CALL = 35_000
CAMPAIGN_CALL_CEILING = 160
ACTOR_CONTRACT = (
    "`reported_actor` is the focal actor: the named person, organization, "
    "or collective whose action, decision, state, or outcome the claim "
    "describes, when that actor is explicitly named in the evidence and is "
    "not the direct speaker speaking in their own voice about themselves. "
    "It is not restricted to sources of reported speech. If no such actor "
    "is explicitly named, the field is null."
)
SYSTEM_PROMPT = f"""You are a reported-actor field adjudicator for a private
podcast research corpus. Do not use tools. Apply this definition independently
to every atomic claim: {ACTOR_CONTRACT}

Return either null or the exact case-sensitive substring from that claim's
evidence_text that concisely names the focal actor. No other string is
emittable. Bias toward null: most claims have no reported actor. The supplied
candidate prior is a non-binding hint and is often wrong; override it whenever
the evidence and definition require. A direct speaker speaking in their own
voice about themselves has reported_actor null. Do not change the claim,
speaker, evidence, decomposition, or any other field. Return only the exact
schema-valid JSON requested by the packet."""


class ActorValueError(RuntimeError):
    """Raised when the actor-value measurement violates its contract."""


def _prior_name(value: Any) -> str | None:
    if isinstance(value, Mapping):
        value = value.get("name")
    text = " ".join(str(value or "").split())
    if not text or text.casefold() in {"none", "null", "n/a", "unknown"}:
        return None
    return text


def actor_value_schema(
    refs: Sequence[tuple[str, int]],
) -> dict[str, Any]:
    candidate_ids = sorted({candidate_id for candidate_id, _ in refs})
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": SCHEMA_VERSION,
            },
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
                            "enum": candidate_ids,
                        },
                        "claim_index": {
                            "type": "integer",
                            "minimum": 0,
                        },
                        "reported_actor": {
                            "type": ["string", "null"],
                        },
                    },
                },
            },
        },
    }


def validate_actor_value_output(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    true_north._validate_schema(
        packet["output_schema"], output, path="$"
    )
    inputs = {
        (str(row["candidate_id"]), int(row["claim_index"])): row
        for row in packet["input"]["claims"]
    }
    seen: set[tuple[str, int]] = set()
    for row in output["items"]:
        ref = (str(row["candidate_id"]), int(row["claim_index"]))
        if ref in seen or ref not in inputs:
            raise ActorValueError(
                "actor output contains duplicate or out-of-scope claim"
            )
        seen.add(ref)
        actor = row["reported_actor"]
        if actor is None:
            continue
        if not actor.strip() or actor != actor.strip():
            raise ActorValueError(
                "reported_actor must be null or a trimmed exact substring"
            )
        if actor not in str(inputs[ref]["evidence_text"]):
            raise ActorValueError(
                "reported_actor is not an exact evidence substring"
            )
    if seen != set(inputs):
        raise ActorValueError(
            "actor output does not exactly cover packet claims"
        )


def _load_source_predictions(
    root: Path,
    source_run_id: str,
) -> tuple[
    Path,
    dict[tuple[str, str], dict[str, Any]],
    dict[str, Any],
]:
    source_root = root / "multipass" / "runs" / source_run_id
    configuration = true_north._read_json(
        source_root / "configuration.json"
    )
    state = true_north._read_json(source_root / "state.json")
    result = true_north._read_json(source_root / "result.json")
    if (
        state.get("complete") is not True
        or configuration.get("holdout_access_allowed") is not False
        or configuration.get("production_database_open_allowed") is not False
        or result.get("holdout_opened") is not False
        or result.get("production_mutation") is not False
    ):
        raise ActorValueError(
            "source decomposition is not complete and local-only"
        )
    outputs: dict[tuple[str, str], dict[str, Any]] = {}
    for episode_id in configuration["episode_ids"]:
        for path in sorted(
            (
                source_root / "outputs" / "composed" / episode_id
            ).glob("*/validated.private.json")
        ):
            outputs[(episode_id, path.parent.name)] = (
                true_north._read_json(path)
            )
    if not outputs:
        raise ActorValueError(
            "source decomposition has no composed outputs"
        )
    provenance = {
        "run_id": source_run_id,
        "configuration_sha256": configuration[
            "configuration_sha256"
        ],
        "configuration_file_sha256": true_north._sha256_file(
            source_root / "configuration.json"
        ),
        "state_file_sha256": true_north._sha256_file(
            source_root / "state.json"
        ),
        "result_file_sha256": true_north._sha256_file(
            source_root / "result.json"
        ),
        "campaign_calls_after_source": (
            int(result["campaign_calls_after_run"])
            if result.get("campaign_calls_after_run") is not None
            else None
        ),
        "episode_ids": configuration["episode_ids"],
    }
    return source_root, outputs, provenance


def _actor_claim_rows(
    *,
    outputs: Mapping[tuple[str, str], Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (episode_id, segment_id), output in sorted(outputs.items()):
        for item in output["items"]:
            candidate_id = str(item["candidate_id"])
            prior = _prior_name(
                candidates[candidate_id].get("reported_actor")
            )
            for claim_index, atomic in enumerate(item["atomic_claims"]):
                rows.append(
                    {
                        "candidate_id": candidate_id,
                        "claim_index": claim_index,
                        "episode_id": episode_id,
                        "segment_id": segment_id,
                        "claim_text": str(atomic["claim_text"]),
                        "evidence_text": str(atomic["evidence_text"]),
                        "raw_speaker": str(atomic["raw_speaker"]),
                        "candidate_prior": prior,
                    }
                )
    return rows


def _actor_packets(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not rows:
        raise ActorValueError("actor stage has no atomic claims")
    batch_size = math.ceil(len(rows) / TARGET_PACKET_COUNT)
    packets: list[dict[str, Any]] = []
    for index in range(0, len(rows), batch_size):
        batch = list(rows[index : index + batch_size])
        refs = [
            (str(row["candidate_id"]), int(row["claim_index"]))
            for row in batch
        ]
        packets.append(
            {
                "schema_version": SCHEMA_VERSION,
                "suite_id": true_north.SUITE_ID,
                "multipass_stage": "actor-value",
                "task": (
                    "Return the literal focal actor or null for each claim."
                ),
                "instructions": [
                    ACTOR_CONTRACT,
                    (
                        "A non-null value must be a case-sensitive exact "
                        "substring of that claim's evidence_text."
                    ),
                    (
                        "The candidate prior is non-binding; decide from "
                        "the evidence and focal-actor definition."
                    ),
                    (
                        "Bias toward null and return null for a direct "
                        "speaker speaking about themselves."
                    ),
                ],
                "output_schema": actor_value_schema(refs),
                "input": {"claims": copy.deepcopy(batch)},
            }
        )
    if len(packets) > TARGET_PACKET_COUNT:
        raise ActorValueError(
            "actor packet builder exceeded target packet count"
        )
    return packets


def run_actor_value_measurement(
    *,
    suite_root: str | Path,
    source_run_id: str,
    run_id: str | None = None,
    campaign_calls_before_run: int | None = None,
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
        raise ActorValueError(
            "suite verification failed before actor-value stage"
        )
    manifest = true_north._read_json(root / "manifest.json")
    _source_root, source_outputs, source_provenance = (
        _load_source_predictions(root, source_run_id)
    )
    candidates: dict[str, dict[str, Any]] = {}
    for bundle_row in manifest["bundles"]:
        if bundle_row["partition"] != "development":
            continue
        bundle = true_north._read_json(
            Path(bundle_row["bundle_path"])
        )
        for candidate in bundle["candidates"]:
            candidates[str(candidate["candidate_id"])] = dict(candidate)
    rows = _actor_claim_rows(
        outputs=source_outputs, candidates=candidates
    )
    packets = _actor_packets(rows)
    source_campaign_calls = source_provenance[
        "campaign_calls_after_source"
    ]
    campaign_calls_before = (
        int(campaign_calls_before_run)
        if campaign_calls_before_run is not None
        else (
            int(source_campaign_calls)
            if source_campaign_calls is not None
            else -1
        )
    )
    if campaign_calls_before < 0:
        raise ActorValueError(
            "campaign calls before actor run must be declared"
        )
    effective_call_ceiling = min(
        MAX_CALLS,
        CAMPAIGN_CALL_CEILING - campaign_calls_before,
    )
    if len(packets) > effective_call_ceiling:
        raise ActorValueError(
            "actor packets cannot fit the remaining campaign call budget"
        )
    resolved_run_id = run_id or (
        "task6-actor-value-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    run_root = root / "multipass" / "runs" / resolved_run_id
    budget = {
        "max_calls": effective_call_ceiling,
        "workstream_max_calls": MAX_CALLS,
        "max_tokens": MAX_TOKENS,
        "max_wall_seconds": MAX_WALL_SECONDS,
    }
    configuration = {
        "schema_version": SCHEMA_VERSION,
        "suite_id": root.name,
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "run_id": resolved_run_id,
        "source": source_provenance,
        "episode_ids": source_provenance["episode_ids"],
        "model": GLM_MODEL,
        "system_prompt_sha256": true_north.sha256_text(SYSTEM_PROMPT),
        "actor_contract_sha256": true_north.sha256_text(
            ACTOR_CONTRACT
        ),
        "atomic_claim_count": len(rows),
        "packet_count": len(packets),
        "target_packet_count": TARGET_PACKET_COUNT,
        "budget": budget,
        "campaign_calls_before_run": campaign_calls_before,
        "campaign_call_ceiling": CAMPAIGN_CALL_CEILING,
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
            raise ActorValueError(
                "resume configuration differs from actor-value run"
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
        for index, packet in enumerate(packets)
    ]
    actor_outputs = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="adjudication",
        artifact_stage="actor-value",
        jobs=jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=GLM_MODEL,
        reserved_tokens_per_call=RESERVED_TOKENS_PER_CALL,
        validator_override=validate_actor_value_output,
        system_prompt_override=SYSTEM_PROMPT,
    )
    decisions: dict[tuple[str, int], str | None] = {}
    for output in actor_outputs.values():
        for row in output["items"]:
            ref = (str(row["candidate_id"]), int(row["claim_index"]))
            if ref in decisions:
                raise ActorValueError(
                    "duplicate actor decision across packets"
                )
            decisions[ref] = row["reported_actor"]
    expected = {
        (str(row["candidate_id"]), int(row["claim_index"]))
        for row in rows
    }
    if set(decisions) != expected:
        raise ActorValueError(
            "actor decisions do not cover all source atomic claims"
        )
    for key, source in source_outputs.items():
        output = copy.deepcopy(source)
        for item in output["items"]:
            candidate_id = str(item["candidate_id"])
            for claim_index, atomic in enumerate(item["atomic_claims"]):
                atomic["reported_actor"] = decisions[
                    (candidate_id, claim_index)
                ]
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
    state["packet_count"] = len(packets)
    true_north._multipass_state_write(state_path, state)
    campaign_calls_after = (
        campaign_calls_before + int(state["usage"]["calls"])
    )
    if campaign_calls_after > CAMPAIGN_CALL_CEILING:
        raise ActorValueError(
            "actor run exceeded the global campaign call ceiling"
        )
    result = {
        "schema_version": SCHEMA_VERSION,
        "run_id": resolved_run_id,
        "configuration_sha256": configuration[
            "configuration_sha256"
        ],
        "complete": True,
        "atomic_claim_count": len(rows),
        "packet_count": len(packets),
        "usage": state["usage"],
        "campaign_calls_before_run": campaign_calls_before,
        "campaign_calls_after_run": campaign_calls_after,
        "campaign_call_ceiling": CAMPAIGN_CALL_CEILING,
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
    episode_ids: Sequence[str],
) -> list[dict[str, Any]]:
    predictions: list[dict[str, Any]] = []
    for episode_id in episode_ids:
        for path in sorted(
            (
                run_root / "outputs" / "composed" / episode_id
            ).glob("*/validated.private.json")
        ):
            predictions.extend(
                true_north._read_json(path)["items"]
            )
    return predictions


def _actor_diagnostics(
    *,
    candidate_scores: Sequence[Mapping[str, Any]],
    predictions: Mapping[str, Mapping[str, Any]],
    preferred: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    emission_correct = 0
    emission_denominator = 0
    value_correct = 0
    gold_non_null = 0
    for score in candidate_scores:
        if not score["strictly_scoreable"]:
            continue
        candidate_id = str(score["candidate_id"])
        predicted_claims = predictions[candidate_id]["atomic_claims"]
        gold_claims = preferred[candidate_id]["atomic_claims"]
        emission_denominator += max(
            len(predicted_claims), len(gold_claims)
        )
        matched_gold: set[int] = set()
        for pair in score["alignment"]:
            predicted_index = int(pair["predicted_index"])
            gold_index = int(pair["gold_index"])
            matched_gold.add(gold_index)
            predicted_actor = predicted_claims[predicted_index].get(
                "reported_actor"
            )
            gold_actor = gold_claims[gold_index].get("reported_actor")
            emission_correct += int(
                (predicted_actor is None) == (gold_actor is None)
            )
            if gold_actor is not None:
                gold_non_null += 1
                value_correct += int(predicted_actor == gold_actor)
        for gold_index, gold_claim in enumerate(gold_claims):
            if (
                gold_index not in matched_gold
                and gold_claim.get("reported_actor") is not None
            ):
                gold_non_null += 1
    return {
        "emission_accuracy": (
            emission_correct / emission_denominator
            if emission_denominator
            else 1.0
        ),
        "emission_correct": emission_correct,
        "emission_denominator": emission_denominator,
        "value_accuracy_given_gold_non_null": (
            value_correct / gold_non_null
            if gold_non_null
            else 1.0
        ),
        "value_correct": value_correct,
        "gold_non_null_denominator": gold_non_null,
    }


def score_actor_value_measurement(
    *,
    suite_root: str | Path,
    run_id: str,
) -> dict[str, Any]:
    from .true_north_semantic_scoring import score_campaign

    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    configuration = true_north._read_json(
        run_root / "configuration.json"
    )
    if configuration.get("schema_version") != SCHEMA_VERSION:
        raise ActorValueError("run is not an actor-value measurement")
    source_root = (
        root
        / "multipass"
        / "runs"
        / configuration["source"]["run_id"]
    )
    episode_ids = configuration["episode_ids"]
    predictions = _load_predictions(run_root, episode_ids)
    baseline_predictions = _load_predictions(
        source_root, episode_ids
    )
    consensus_document = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred_document = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    manifest = true_north._read_json(root / "manifest.json")
    speaker_maps: dict[str, Any] = {}
    for bundle_row in manifest["bundles"]:
        if str(bundle_row["episode_id"]) not in episode_ids:
            continue
        bundle = true_north._read_json(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            speaker_maps[str(candidate["candidate_id"])] = (
                bundle["episode_context"].get("speaker_map", [])
            )
    score = score_campaign(
        predictions,
        consensus_document["items"],
        preferred_document["items"],
        speaker_maps_by_candidate=speaker_maps,
    )
    baseline = score_campaign(
        baseline_predictions,
        consensus_document["items"],
        preferred_document["items"],
        speaker_maps_by_candidate=speaker_maps,
    )
    prediction_map = {
        str(row["candidate_id"]): row for row in predictions
    }
    preferred_map = {
        str(row["candidate_id"]): row
        for row in preferred_document["items"]
        if str(row["candidate_id"]) in prediction_map
    }
    diagnostics = _actor_diagnostics(
        candidate_scores=score["candidates"],
        predictions=prediction_map,
        preferred=preferred_map,
    )
    result = true_north._read_json(run_root / "result.json")
    aggregate = score["aggregate"]
    baseline_aggregate = baseline["aggregate"]
    document = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        **diagnostics,
        "composite_reported_actor_exactness": aggregate[
            "reported_actor_exactness"
        ],
        "reported_actor_gate": 0.903182,
        "composite_gate_passed": (
            float(aggregate["reported_actor_exactness"]) >= 0.903182
        ),
        "hallucination_proxy": {
            "before": baseline_aggregate[
                "hallucination_rate_proxy"
            ],
            "after": aggregate["hallucination_rate_proxy"],
            "absolute_change": round(
                float(aggregate["hallucination_rate_proxy"])
                - float(
                    baseline_aggregate["hallucination_rate_proxy"]
                ),
                6,
            ),
        },
        "usage": result["usage"],
        "campaign_calls_after_run": result[
            "campaign_calls_after_run"
        ],
        "campaign_call_ceiling": CAMPAIGN_CALL_CEILING,
        "holdout_opened": False,
        "production_mutation": False,
    }
    document["score_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = run_root / "actor-value-score.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "score_path": str(path)}
