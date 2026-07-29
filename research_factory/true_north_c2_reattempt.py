"""Ruling-5 C2 re-attempt with a smoke-gated Codex-compatible schema."""

from __future__ import annotations

import copy
import time
from pathlib import Path
from typing import Any, Mapping

from . import true_north
from .true_north_c2_sol_adjudicator import (
    PHASE_C_RUN_ID,
    _pass_maps,
    _source_adjudication_packets,
)
from .true_north_cheap_consensus import (
    _selected_item,
    score_cheap_consensus,
)
from .true_north_dual_decomposition import (
    REFERENCE_RUN_ID,
    SEARCH_EPISODE_IDS,
    _load_search_context,
    _reference_artifacts,
)
from .true_north_ruling4_contract import CONTRACT_VERSION
from .true_north_sol_split_default import (
    MODEL as SOL_MODEL,
    MODEL_LANE as SOL_MODEL_LANE,
    _usage_tokens,
)


SCHEMA_VERSION = "pif_true_north_c2_reattempt_v1"
EXPERIMENT_ID = "phase-c2-sol-adjudicator-reattempt-20260729-v1"
RUN_ID = "task5-c2-sol-adjudicator-reattempt-20260729-v1"
MAX_CALLS = 24
MAX_TOKENS = 700_000
MAX_WALL_SECONDS = 4 * 60 * 60
RESERVED_TOKENS_PER_ENVELOPE = 35_000
CUMULATIVE_CALLS_BEFORE = 289
KNOWN_CUMULATIVE_TOKENS_BEFORE = 3_029_113
LEDGER_PATH = (
    Path(__file__).resolve().parent.parent
    / "config"
    / "true_north_campaign_budget_ledger.json"
)
SYSTEM_PROMPT = """\
You adjudicate atomic decompositions, not wording.

For every candidate, inspect the evidence and both proposed decompositions.
Return:
- decision="chose_a" only when the resulting_claim_texts exactly equal Pass A.
- decision="chose_b" only when the resulting_claim_texts exactly equal Pass B.
- decision="merged" only when selecting a non-empty, non-duplicative
  combination or subset from the supplied union that is not exactly A or B.

Every resulting claim must be copied byte-for-byte from union_claim_texts.
Never paraphrase, repair, or invent a third claim. Select the smallest set in
which every item is independently true or false and the set preserves the
evidence-supported proposition. Return only JSON matching the schema."""


class C2ReattemptError(RuntimeError):
    """Raised when the Ruling-5 re-attempt violates its frozen contract."""


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
        or row.get("mandatory_smoke_test_first") is not True
        or row.get("one_run_only") is not True
        or row.get("provider_lane") != SOL_MODEL_LANE
        or int(row.get("max_calls", -1)) != MAX_CALLS
        or int(row.get("max_tokens", -1)) != MAX_TOKENS
        or int(row.get("reserved_tokens_per_envelope", -1))
        != RESERVED_TOKENS_PER_ENVELOPE
    ):
        raise C2ReattemptError(
            "C2 re-attempt budget is not declared exactly"
        )
    return ledger, true_north.sha256_text(
        true_north.dumps_json(ledger)
    )


def flat_output_schema(
    packet: Mapping[str, Any],
) -> dict[str, Any]:
    candidates = packet["input"]["candidates"]
    candidate_ids = [
        str(row["candidate_id"]) for row in candidates
    ]
    union_claims = list(
        dict.fromkeys(
            str(claim)
            for row in candidates
            for claim in row["union_claim_texts"]
        )
    )
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "items"],
        "properties": {
            "schema_version": {"const": SCHEMA_VERSION},
            "items": {
                "type": "array",
                "minItems": len(candidate_ids),
                "maxItems": len(candidate_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "candidate_id",
                        "decision",
                        "resulting_claim_texts",
                    ],
                    "properties": {
                        "candidate_id": {
                            "type": "string",
                            "enum": candidate_ids,
                        },
                        "decision": {
                            "type": "string",
                            "enum": ["chose_a", "chose_b", "merged"],
                        },
                        "resulting_claim_texts": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "string",
                                "enum": union_claims,
                            },
                        },
                    },
                },
            },
        },
    }


def flat_packet(source: Mapping[str, Any]) -> dict[str, Any]:
    packet = copy.deepcopy(source)
    packet["schema_version"] = SCHEMA_VERSION
    packet["task"] = (
        "Adjudicate A/B decomposition using a closed decision and exact "
        "claim strings from their union."
    )
    packet["instructions"] = [
        "decision must be chose_a, chose_b, or merged.",
        "resulting_claim_texts must be non-empty and duplicate-free.",
        "Every resulting string must be copied exactly from union_claim_texts.",
        "chose_a and chose_b must exactly reproduce that proposal.",
        "merged must differ from both proposals and invent nothing.",
    ]
    packet["output_schema"] = flat_output_schema(packet)
    return packet


def validate_flat_output(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    if output.get("schema_version") != SCHEMA_VERSION:
        raise C2ReattemptError("C2 output schema version drift")
    expected = {
        str(row["candidate_id"]): row
        for row in packet["input"]["candidates"]
    }
    actual: dict[str, Mapping[str, Any]] = {}
    for row in output.get("items", []):
        candidate_id = str(row.get("candidate_id"))
        if candidate_id not in expected or candidate_id in actual:
            raise C2ReattemptError(
                "C2 output added or duplicated a candidate"
            )
        decision = str(row.get("decision"))
        claims = [
            str(value)
            for value in row.get("resulting_claim_texts", [])
        ]
        if (
            decision not in {"chose_a", "chose_b", "merged"}
            or not claims
            or len(claims) != len(set(claims))
        ):
            raise C2ReattemptError(
                "C2 output has an invalid decision or duplicate claims"
            )
        source = expected[candidate_id]
        union = set(str(value) for value in source["union_claim_texts"])
        if not set(claims) <= union:
            raise C2ReattemptError(
                "C2 output escaped the exact A/B union"
            )
        a = [
            str(item["claim_text"])
            for item in source["pass_a_glm"]["atomic_claims"]
        ]
        b = [
            str(item["claim_text"])
            for item in source["pass_b_sol"]["atomic_claims"]
        ]
        if (
            (decision == "chose_a" and claims != a)
            or (decision == "chose_b" and claims != b)
            or (decision == "merged" and (claims == a or claims == b))
        ):
            raise C2ReattemptError(
                "C2 decision does not match its resulting structure"
            )
        actual[candidate_id] = row
    if set(actual) != set(expected):
        raise C2ReattemptError(
            "C2 output omitted an adjudication candidate"
        )


def prepare_reattempt(
    *,
    suite_root: str | Path,
    run_id: str = RUN_ID,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise C2ReattemptError(
            "suite verification failed before C2 re-attempt"
        )
    manifest = true_north._read_json(root / "manifest.json")
    if manifest["measurement_contract"]["version"] != CONTRACT_VERSION:
        raise C2ReattemptError("C2 re-attempt requires contract v8")
    ledger, ledger_sha = _ledger()
    source_packets = _source_adjudication_packets(root)
    packets = {
        key: flat_packet(packet)
        for key, packet in source_packets.items()
    }
    reserved = len(packets) * RESERVED_TOKENS_PER_ENVELOPE
    if len(packets) > MAX_CALLS or reserved > MAX_TOKENS:
        raise C2ReattemptError(
            "C2 re-attempt whole-run budget preflight failed"
        )
    run_root = root / "multipass" / "runs" / run_id
    snapshot = (
        root / "multipass" / "budget-ledger" / f"{EXPERIMENT_ID}.json"
    )
    true_north._write_json(snapshot, ledger, immutable=True)
    packet_hashes = {}
    for (episode_id, segment_id), packet in sorted(packets.items()):
        packet_path = (
            run_root
            / "packets"
            / "sol-flat-adjudication"
            / episode_id
            / f"{segment_id}.private.json"
        )
        schema_path = (
            run_root
            / "schemas"
            / "sol-flat-adjudication"
            / episode_id
            / f"{segment_id}.json"
        )
        true_north._write_json(packet_path, packet, immutable=True)
        true_north._write_json(
            schema_path, packet["output_schema"], immutable=True
        )
        packet_hashes[f"{episode_id}/{segment_id}"] = (
            true_north._sha256_file(packet_path)
        )
    smoke_key = min(
        packets,
        key=lambda key: (
            len(packets[key]["input"]["candidates"]),
            key,
        ),
    )
    configuration = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "suite_id": root.name,
        "suite_manifest_sha256": manifest["manifest_sha256"],
        "measurement_contract_sha256": manifest[
            "measurement_contract"
        ]["contract_sha256"],
        "models": {
            "pass_a": "zai-coding-plan/glm-5.2_frozen_task5",
            "pass_b": "gpt-5.6-sol_completed_phase_c",
            "pass_c2": SOL_MODEL,
        },
        "model_lane": SOL_MODEL_LANE,
        "invocation": "codex exec --ephemeral",
        "source_run_id": PHASE_C_RUN_ID,
        "source_packet_hashes": packet_hashes,
        "provider_envelope_count": len(packets),
        "smoke_packet": f"{smoke_key[0]}/{smoke_key[1]}",
        "smoke_must_pass_before_batch": True,
        "system_prompt_sha256": true_north.sha256_text(SYSTEM_PROMPT),
        "provider_schema": (
            "flat decision plus claim list; no oneOf; no uniqueItems"
        ),
        "semantic_validator": (
            "local exact candidate scope, decision consistency, "
            "union membership, and uniqueness"
        ),
        "unflagged_control": REFERENCE_RUN_ID,
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
            "max_wall_seconds": MAX_WALL_SECONDS,
            "reserved_tokens_per_envelope": (
                RESERVED_TOKENS_PER_ENVELOPE
            ),
            "whole_run_reserved_tokens": reserved,
        },
        "budget_ledger_sha256": ledger_sha,
        "episode_ids": list(SEARCH_EPISODE_IDS),
        "one_run_only": True,
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(configuration)
    )
    config_path = run_root / "configuration.json"
    if config_path.is_file():
        if true_north._read_json(config_path) != configuration:
            raise C2ReattemptError(
                "C2 re-attempt resume configuration drift"
            )
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
            "smoke_packet": configuration["smoke_packet"],
            "smoke_passed": False,
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
        "flagged_candidate_count": 106,
        "provider_envelope_count": len(packets),
        "smoke_packet": configuration["smoke_packet"],
        "whole_run_reserved_calls": len(packets),
        "whole_run_reserved_tokens": reserved,
        "within_call_ceiling": len(packets) <= MAX_CALLS,
        "within_token_ceiling": reserved <= MAX_TOKENS,
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


def _execute_packet(
    *,
    run_root: Path,
    packet_path: Path,
    timeout_seconds: int,
    codex_binary: str,
) -> tuple[dict[str, Any], int, float]:
    episode_id = packet_path.parent.name
    segment_id = packet_path.stem.removesuffix(".private")
    output_dir = (
        run_root
        / "outputs"
        / "sol-flat-adjudication"
        / episode_id
        / segment_id
    )
    started = time.monotonic()
    receipt = true_north._run_codex_gold_packet(
        packet_path=packet_path,
        schema_path=(
            run_root
            / "schemas"
            / "sol-flat-adjudication"
            / episode_id
            / f"{segment_id}.json"
        ),
        output_dir=output_dir,
        timeout_seconds=timeout_seconds,
        codex_binary=codex_binary,
        validator=validate_flat_output,
        prompt_prefix=SYSTEM_PROMPT,
    )
    elapsed = time.monotonic() - started
    true_north._write_json(
        output_dir / "c2-reattempt-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "stage": "pass_c2_sol_flat_adjudication",
            "provider_model": SOL_MODEL,
            "provider_lane": SOL_MODEL_LANE,
            "usage": receipt.get("usage", {}),
            "base_receipt_sha256": true_north._sha256_file(
                output_dir / "receipt.json"
            ),
        },
        immutable=True,
    )
    return receipt, _usage_tokens(receipt), elapsed


def _record_failure(
    *,
    state: dict[str, Any],
    packet_path: Path,
    run_root: Path,
    exc: Exception,
    elapsed: float,
) -> None:
    episode_id = packet_path.parent.name
    segment_id = packet_path.stem.removesuffix(".private")
    events_path = (
        run_root
        / "outputs"
        / "sol-flat-adjudication"
        / episode_id
        / segment_id
        / "events.private.jsonl"
    )
    usage = (
        true_north._codex_usage_from_jsonl(
            events_path.read_text(encoding="utf-8")
        )
        if events_path.is_file()
        else {}
    )
    tokens = int(
        usage.get("input_tokens", 0)
        + usage.get("output_tokens", 0)
    )
    state["usage"]["calls"] += 1
    state["usage"]["tokens"] += tokens
    state["usage"]["wall_seconds"] = round(
        float(state["usage"]["wall_seconds"]) + elapsed, 3
    )
    state["failures"].append(
        {
            "packet": f"{episode_id}/{segment_id}",
            "failure": f"{type(exc).__name__}: {exc}",
            "usage": usage,
            "preinference_schema_rejection": (
                tokens == 0
                and events_path.is_file()
                and "invalid_json_schema"
                in events_path.read_text(encoding="utf-8")
            ),
        }
    )


def run_reattempt(
    *,
    suite_root: str | Path,
    run_id: str = RUN_ID,
    timeout_seconds: int = 1800,
    codex_binary: str = "codex",
    smoke_only: bool = False,
) -> dict[str, Any]:
    prepared = prepare_reattempt(
        suite_root=suite_root, run_id=run_id
    )
    root = Path(suite_root).expanduser().resolve()
    run_root = Path(prepared["run_root"])
    state_path = run_root / "state.json"
    state = true_north._read_json(state_path)
    packet_paths = sorted(
        (run_root / "packets" / "sol-flat-adjudication").glob(
            "*/*.private.json"
        )
    )
    by_key = {
        f"{path.parent.name}/{path.stem.removesuffix('.private')}": path
        for path in packet_paths
    }
    smoke_path = by_key[str(state["smoke_packet"])]
    smoke_marker = f"sol-flat-adjudication/{state['smoke_packet']}"
    if not state["smoke_passed"]:
        started = time.monotonic()
        try:
            receipt, tokens, elapsed = _execute_packet(
                run_root=run_root,
                packet_path=smoke_path,
                timeout_seconds=timeout_seconds,
                codex_binary=codex_binary,
            )
        except Exception as exc:
            _record_failure(
                state=state,
                packet_path=smoke_path,
                run_root=run_root,
                exc=exc,
                elapsed=time.monotonic() - started,
            )
            true_north._multipass_state_write(state_path, state)
            return _terminal_result(
                run_root=run_root,
                state=state,
                packet_count=len(packet_paths),
                terminal_reason=(
                    "mandatory_smoke_test_failed_no_batch_dispatched"
                ),
            )
        state["usage"]["calls"] += 1
        state["usage"]["tokens"] += tokens
        state["usage"]["wall_seconds"] = round(
            float(state["usage"]["wall_seconds"]) + elapsed, 3
        )
        state["smoke_passed"] = True
        state["smoke_receipt_sha256"] = receipt["receipt_sha256"]
        state["completed"].append(smoke_marker)
        true_north._multipass_state_write(state_path, state)
    if smoke_only:
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "smoke_passed": True,
            "smoke_packet": state["smoke_packet"],
            "usage": dict(state["usage"]),
            "batch_dispatched": False,
            "holdout_opened": False,
            "production_mutation": False,
            "run_root": str(run_root),
        }
    for packet_path in packet_paths:
        key = f"{packet_path.parent.name}/{packet_path.stem.removesuffix('.private')}"
        marker = f"sol-flat-adjudication/{key}"
        if marker in state["completed"]:
            continue
        if (
            int(state["usage"]["calls"]) + 1 > MAX_CALLS
            or int(state["usage"]["tokens"])
            + RESERVED_TOKENS_PER_ENVELOPE
            > MAX_TOKENS
        ):
            raise C2ReattemptError(
                "budget cannot reserve the next C2 batch envelope"
            )
        started = time.monotonic()
        try:
            _receipt, tokens, elapsed = _execute_packet(
                run_root=run_root,
                packet_path=packet_path,
                timeout_seconds=timeout_seconds,
                codex_binary=codex_binary,
            )
        except Exception as exc:
            _record_failure(
                state=state,
                packet_path=packet_path,
                run_root=run_root,
                exc=exc,
                elapsed=time.monotonic() - started,
            )
            true_north._multipass_state_write(state_path, state)
            break
        state["usage"]["calls"] += 1
        state["usage"]["tokens"] += tokens
        state["usage"]["wall_seconds"] = round(
            float(state["usage"]["wall_seconds"]) + elapsed, 3
        )
        state["completed"].append(marker)
        true_north._multipass_state_write(state_path, state)
    complete = len(state["completed"]) == len(packet_paths)
    if not complete:
        return _terminal_result(
            run_root=run_root,
            state=state,
            packet_count=len(packet_paths),
            terminal_reason="batch_incomplete_after_smoke_passed",
        )
    outputs = sorted(
        (
            run_root / "outputs" / "sol-flat-adjudication"
        ).glob("*/*/validated.private.json")
    )
    decisions = {
        str(row["candidate_id"]): {
            "decision": str(row["decision"]),
            "claims": list(row["resulting_claim_texts"]),
        }
        for path in outputs
        for row in true_north._read_json(path)["items"]
    }
    if len(decisions) != 106:
        raise C2ReattemptError(
            "completed C2 re-attempt lacks 106 decisions"
        )
    decision_counts = {
        decision: sum(
            row["decision"] == decision
            for row in decisions.values()
        )
        for decision in ("chose_a", "chose_b", "merged")
    }
    pass_a, pass_b = _pass_maps(root)
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        _reference_provenance,
    ) = _reference_artifacts(root)
    final_adjudication = {}
    for key, reference in reference_outputs.items():
        source_by_id = {
            str(row["candidate_id"]): row
            for row in reference_packets[key]["input"]["candidates"]
        }
        items = []
        for row in reference["items"]:
            candidate_id = str(row["candidate_id"])
            if candidate_id not in decisions:
                items.append(copy.deepcopy(row))
                continue
            items.append(
                _selected_item(
                    candidate_id=candidate_id,
                    selected=decisions[candidate_id]["claims"],
                    pass_a=pass_a[candidate_id],
                    pass_b=pass_b[candidate_id],
                    proposed_claim_text=str(
                        source_by_id[candidate_id][
                            "proposed_claim_text"
                        ]
                    ),
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
    base_jobs, dispositions, candidates = _load_search_context(
        root, true_north._read_json(root / "manifest.json")
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
    state["flagged_candidate_count"] = len(decisions)
    state["decision_distribution"] = decision_counts
    true_north._multipass_state_write(state_path, state)
    return _terminal_result(
        run_root=run_root,
        state=state,
        packet_count=len(packet_paths),
        terminal_reason="semantic_result_complete_lane_closes",
    )


def _terminal_result(
    *,
    run_root: Path,
    state: Mapping[str, Any],
    packet_count: int,
    terminal_reason: str,
) -> dict[str, Any]:
    complete = bool(state.get("complete"))
    result = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": RUN_ID,
        "complete": complete,
        "acceptance_eligible": complete,
        "terminal_reason": terminal_reason,
        "smoke_passed": bool(state.get("smoke_passed")),
        "validated_provider_envelopes": len(state["completed"]),
        "expected_provider_envelopes": packet_count,
        "failure_count": len(state["failures"]),
        "flagged_candidate_count": (
            int(state["flagged_candidate_count"])
            if complete
            else None
        ),
        "decision_distribution": (
            dict(state["decision_distribution"])
            if complete
            else None
        ),
        "usage": dict(state["usage"]),
        "within_call_ceiling": int(state["usage"]["calls"]) <= MAX_CALLS,
        "within_token_ceiling": int(state["usage"]["tokens"]) <= MAX_TOKENS,
        "cumulative_calls_after_run": (
            CUMULATIVE_CALLS_BEFORE + int(state["usage"]["calls"])
        ),
        "known_cumulative_tokens_after_run": (
            KNOWN_CUMULATIVE_TOKENS_BEFORE
            + int(state["usage"]["tokens"])
        ),
        "decomposition_lane_closed_permanently": complete,
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


def score_reattempt(
    *,
    suite_root: str | Path,
    run_id: str = RUN_ID,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    result = true_north._read_json(run_root / "result.json")
    if not result["complete"]:
        raise C2ReattemptError(
            "incomplete C2 re-attempt is not acceptance-eligible"
        )
    score = score_cheap_consensus(
        suite_root=root,
        run_id=run_id,
        experiment_id=EXPERIMENT_ID,
    )
    score["schema_version"] = SCHEMA_VERSION
    score["smoke_passed"] = True
    score["decision_distribution"] = result[
        "decision_distribution"
    ]
    score["decomposition_lane_closed_permanently"] = True
    score.pop("score_sha256", None)
    score.pop("score_path", None)
    score["score_sha256"] = true_north.sha256_text(
        true_north.dumps_json(score)
    )
    path = run_root / "score.private.json"
    true_north._write_json(path, score, immutable=False)
    return {**score, "score_path": str(path)}
