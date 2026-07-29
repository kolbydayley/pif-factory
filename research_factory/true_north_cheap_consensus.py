"""One-run A/B/C cheap-consensus decomposition experiment."""

from __future__ import annotations

import copy
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from .true_north_decoupled_rescore import _apply_actor_span, score_predictions
from .true_north_dual_decomposition import (
    GLM_MODEL,
    REFERENCE_RUN_ID,
    SEARCH_EPISODE_IDS,
    _load_search_context,
    _reference_artifacts,
)
from .true_north_input_split_default import (
    FLAGGED_SYSTEM_PROMPT,
    merge_adjudication_schema,
    validate_merge_adjudication,
)
from .true_north_ruling4_contract import CONTRACT_VERSION
from .true_north_semantic_scoring import score_campaign
from .true_north_sol_split_default import (
    MODEL as SOL_MODEL,
    MODEL_LANE as SOL_MODEL_LANE,
    _usage_tokens,
)
from .true_north_spark_split_default import (
    _source_packets,
)


SCHEMA_VERSION = "pif_true_north_cheap_consensus_v1"
EXPERIMENT_ID = "phase-c-ab-c-cheap-consensus-20260729-v1"
RUN_ID = "task5-ab-c-cheap-consensus-20260729-v1"
SOURCE_SOL_RUN_ID = "task5-sol-input-split-default-20260729-v1"
MAX_CALLS = 40
MAX_TOKENS = 900_000
MAX_WALL_SECONDS = 4 * 60 * 60
SOL_RESERVED_TOKENS_PER_ENVELOPE = 35_000
GLM_RESERVED_TOKENS_PER_PACKET = 30_000
CUMULATIVE_CALLS_BEFORE = 264
KNOWN_CUMULATIVE_TOKENS_BEFORE = 2_782_670
LEDGER_PATH = (
    Path(__file__).resolve().parent.parent
    / "config"
    / "true_north_campaign_budget_ledger.json"
)
ADJUDICATOR_SYSTEM_PROMPT = """\
You adjudicate atomic decompositions, not wording.

For every candidate, inspect the evidence and the two independent proposed
decompositions. Return a non-empty ordered selection of exact claim_text
strings copied from the union_claim_texts supplied for that candidate.

You may select all claims from proposal A, all from proposal B, or a
non-duplicative subset/combination from their union. Do not write, paraphrase,
repair, or invent any third claim text. Select the smallest set in which each
item is independently true or false and together preserves the supported
proposition. Prefer retaining a qualification with the claim it qualifies.
Return only JSON matching the schema."""


class CheapConsensusError(RuntimeError):
    """Raised when the bounded consensus contract is violated."""


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
        or int(row.get("max_calls", -1)) != MAX_CALLS
        or int(row.get("max_tokens", -1)) != MAX_TOKENS
        or int(row.get("reserved_sol_tokens_per_envelope", -1))
        != SOL_RESERVED_TOKENS_PER_ENVELOPE
        or row.get("one_run_only") is not True
    ):
        raise CheapConsensusError(
            "Phase C budget is not declared exactly before execution"
        )
    return ledger, true_north.sha256_text(
        true_north.dumps_json(ledger)
    )


def _validated_sol_map(root: Path) -> dict[str, dict[str, Any]]:
    source = (
        root
        / "multipass"
        / "runs"
        / SOURCE_SOL_RUN_ID
        / "outputs"
        / "sol-input-split-default"
    )
    return {
        str(row["candidate_id"]): {
            key: copy.deepcopy(value)
            for key, value in row.items()
            if key != "merge_reason"
        }
        for path in sorted(source.glob("*/validated.private.json"))
        for row in true_north._read_json(path)["items"]
    }


def _filtered_sol_packet(
    source: Mapping[str, Any],
    missing_ids: set[str],
) -> dict[str, Any]:
    packet = copy.deepcopy(source["packet"])
    candidates = [
        row
        for row in packet["input"]["candidates"]
        if str(row["candidate_id"]) in missing_ids
    ]
    if not candidates:
        raise CheapConsensusError("missing-sol packet is empty")
    ids = [str(row["candidate_id"]) for row in candidates]
    decisions = {
        str(row["candidate_id"]): row
        for row in packet["input"]["stage_a_decisions"]
    }
    packet["input"]["candidates"] = candidates
    packet["input"]["stage_a_decisions"] = [
        copy.deepcopy(decisions[candidate_id]) for candidate_id in ids
    ]
    packet["output_schema"] = merge_adjudication_schema(ids)
    return packet


def _completion_packets(
    root: Path,
) -> tuple[list[dict[str, Any]], set[str]]:
    sources = _source_packets(root)
    flagged_ids = {
        str(candidate["candidate_id"])
        for source in sources
        for candidate in source["packet"]["input"]["candidates"]
    }
    existing = set(_validated_sol_map(root))
    missing = flagged_ids - existing
    packets = []
    for source in sources:
        source_ids = {
            str(row["candidate_id"])
            for row in source["packet"]["input"]["candidates"]
        }
        selected = source_ids & missing
        if selected:
            packets.append(
                {
                    "packet_key": str(source["packet_key"]),
                    "candidate_ids": sorted(selected),
                    "packet": _filtered_sol_packet(source, selected),
                }
            )
    if set(
        candidate_id
        for row in packets
        for candidate_id in row["candidate_ids"]
    ) != missing:
        raise CheapConsensusError(
            "Sol completion packets do not exactly cover missing candidates"
        )
    return packets, flagged_ids


def _candidate_schema(
    candidate_id: str,
    allowed_claims: Sequence[str],
) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["candidate_id", "selected_claim_texts"],
        "properties": {
            "candidate_id": {"const": candidate_id},
            "selected_claim_texts": {
                "type": "array",
                "minItems": 1,
                "uniqueItems": True,
                "items": {"type": "string", "enum": list(allowed_claims)},
            },
        },
    }


def build_adjudication_packet(
    *,
    source_packet: Mapping[str, Any],
    pass_a: Mapping[str, Mapping[str, Any]],
    pass_b: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidates = []
    schemas = []
    for source in source_packet["input"]["candidates"]:
        candidate_id = str(source["candidate_id"])
        a = copy.deepcopy(pass_a[candidate_id])
        b = copy.deepcopy(pass_b[candidate_id])
        union = list(
            dict.fromkeys(
                [
                    str(row["claim_text"])
                    for row in [
                        *a["atomic_claims"],
                        *b["atomic_claims"],
                    ]
                ]
            )
        )
        if not union:
            raise CheapConsensusError(
                f"empty A/B claim union: {candidate_id}"
            )
        candidates.append(
            {
                "candidate_id": candidate_id,
                "evidence_text": source["evidence_text"],
                "proposed_claim_text": source["proposed_claim_text"],
                "pass_a_glm": a,
                "pass_b_sol": b,
                "union_claim_texts": union,
            }
        )
        schemas.append(_candidate_schema(candidate_id, union))
    return {
        "schema_version": SCHEMA_VERSION,
        "suite_id": true_north.SUITE_ID,
        "multipass_stage": "adjudication",
        "task": (
            "Adjudicate A/B decomposition strictly within their claim union."
        ),
        "instructions": [
            "Choose exact claim_text strings only from union_claim_texts.",
            "Never invent a third structure or paraphrase a claim.",
            "Return every candidate exactly once.",
        ],
        "output_schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["schema_version", "items"],
            "properties": {
                "schema_version": {"const": SCHEMA_VERSION},
                "items": {
                    "type": "array",
                    "minItems": len(candidates),
                    "maxItems": len(candidates),
                    "items": {"oneOf": schemas},
                },
            },
        },
        "input": {
            "episode": copy.deepcopy(source_packet["input"]["episode"]),
            "segment": copy.deepcopy(source_packet["input"]["segment"]),
            "candidates": candidates,
        },
    }


def validate_adjudication_output(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    if output.get("schema_version") != SCHEMA_VERSION:
        raise CheapConsensusError("adjudicator schema version drift")
    expected = {
        str(row["candidate_id"]): set(row["union_claim_texts"])
        for row in packet["input"]["candidates"]
    }
    actual: dict[str, list[str]] = {}
    for row in output.get("items", []):
        candidate_id = str(row.get("candidate_id"))
        selected = [str(value) for value in row.get("selected_claim_texts", [])]
        if (
            candidate_id not in expected
            or candidate_id in actual
            or not selected
            or len(selected) != len(set(selected))
            or not set(selected) <= expected[candidate_id]
        ):
            raise CheapConsensusError(
                "adjudicator output escaped the A/B union contract"
            )
        actual[candidate_id] = selected
    if set(actual) != set(expected):
        raise CheapConsensusError(
            "adjudicator output omitted or added a candidate"
        )


def _selected_item(
    *,
    candidate_id: str,
    selected: Sequence[str],
    pass_a: Mapping[str, Any],
    pass_b: Mapping[str, Any],
    proposed_claim_text: str,
) -> dict[str, Any]:
    a_texts = [
        str(row["claim_text"]) for row in pass_a["atomic_claims"]
    ]
    b_texts = [
        str(row["claim_text"]) for row in pass_b["atomic_claims"]
    ]
    if list(selected) == a_texts:
        return copy.deepcopy(dict(pass_a))
    if list(selected) == b_texts:
        return copy.deepcopy(dict(pass_b))
    count = len(selected)
    return {
        "candidate_id": candidate_id,
        "split": count > 1,
        "edit_reason": (
            "none"
            if count == 1 and selected[0] == proposed_claim_text
            else ("compound_split" if count > 1 else "qualifier_repair")
        ),
        "atomic_claims": [
            {"claim_text": claim_text} for claim_text in selected
        ],
    }


def prepare_cheap_consensus(
    *,
    suite_root: str | Path,
    run_id: str = RUN_ID,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise CheapConsensusError(
            "suite verification failed before Phase C"
        )
    manifest = true_north._read_json(root / "manifest.json")
    if manifest["measurement_contract"]["version"] != CONTRACT_VERSION:
        raise CheapConsensusError("Phase C requires contract v8")
    ledger, ledger_sha = _ledger()
    completion_packets, flagged_ids = _completion_packets(root)
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        reference_provenance,
    ) = _reference_artifacts(root)
    pass_a = {
        str(row["candidate_id"]): copy.deepcopy(row)
        for output in reference_outputs.values()
        for row in output["items"]
    }
    if not flagged_ids <= set(pass_a):
        raise CheapConsensusError("frozen Pass A lacks flagged coverage")
    flagged_packet_count = sum(
        bool(
            flagged_ids
            & {
                str(row["candidate_id"])
                for row in packet["input"]["candidates"]
            }
        )
        for packet in reference_packets.values()
    )
    reserved = (
        len(completion_packets) * SOL_RESERVED_TOKENS_PER_ENVELOPE
        + flagged_packet_count * GLM_RESERVED_TOKENS_PER_PACKET
    )
    calls = len(completion_packets) + flagged_packet_count
    if calls > MAX_CALLS or reserved > MAX_TOKENS:
        raise CheapConsensusError(
            "whole-run Phase C packet-budget preflight failed"
        )
    run_root = root / "multipass" / "runs" / run_id
    snapshot = (
        root / "multipass" / "budget-ledger" / f"{EXPERIMENT_ID}.json"
    )
    true_north._write_json(snapshot, ledger, immutable=True)
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
            "pass_a": GLM_MODEL,
            "pass_b": SOL_MODEL,
            "pass_c": GLM_MODEL,
        },
        "pass_a_run_id": REFERENCE_RUN_ID,
        "pass_b_reuse_run_id": SOURCE_SOL_RUN_ID,
        "flagged_candidate_count": len(flagged_ids),
        "sol_reused_candidate_count": len(
            _validated_sol_map(root)
        ),
        "sol_missing_candidate_count": sum(
            len(row["candidate_ids"]) for row in completion_packets
        ),
        "sol_completion_packet_count": len(completion_packets),
        "adjudication_packet_count": flagged_packet_count,
        "unflagged_control": REFERENCE_RUN_ID,
        "reference_provenance": reference_provenance,
        "system_prompt_sha256": {
            "sol_completion": true_north.sha256_text(
                FLAGGED_SYSTEM_PROMPT
            ),
            "glm_adjudication": true_north.sha256_text(
                ADJUDICATOR_SYSTEM_PROMPT
            ),
        },
        "union_contract": (
            "selected claim texts must be a non-empty subset of the "
            "exact A/B union; third structures are rejected"
        ),
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
            "max_wall_seconds": MAX_WALL_SECONDS,
            "reserved_sol_tokens_per_envelope": (
                SOL_RESERVED_TOKENS_PER_ENVELOPE
            ),
            "reserved_glm_tokens_per_packet": (
                GLM_RESERVED_TOKENS_PER_PACKET
            ),
            "whole_run_reserved_tokens": reserved,
            "whole_run_reserved_calls": calls,
        },
        "budget_ledger_sha256": ledger_sha,
        "episode_ids": list(SEARCH_EPISODE_IDS),
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
        "one_run_only": True,
    }
    configuration["configuration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(configuration)
    )
    config_path = run_root / "configuration.json"
    if config_path.is_file():
        if true_north._read_json(config_path) != configuration:
            raise CheapConsensusError("Phase C resume configuration drift")
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
    for index, row in enumerate(completion_packets):
        name = f"missing-{index:03d}"
        true_north._write_json(
            run_root
            / "packets"
            / "sol-completion"
            / f"{name}.private.json",
            row["packet"],
            immutable=True,
        )
        true_north._write_json(
            run_root / "schemas" / "sol-completion" / f"{name}.json",
            row["packet"]["output_schema"],
            immutable=True,
        )
    preflight = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "flagged_candidate_count": len(flagged_ids),
        "sol_reused_candidate_count": len(_validated_sol_map(root)),
        "sol_missing_candidate_count": configuration[
            "sol_missing_candidate_count"
        ],
        "sol_completion_calls_reserved": len(completion_packets),
        "glm_adjudication_calls_reserved": flagged_packet_count,
        "whole_run_reserved_calls": calls,
        "whole_run_reserved_tokens": reserved,
        "within_call_ceiling": calls <= MAX_CALLS,
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


def run_cheap_consensus(
    *,
    suite_root: str | Path,
    run_id: str = RUN_ID,
    workers: int = 3,
    timeout_seconds: int = 1800,
    codex_binary: str = "codex",
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    runner: Any | None = None,
) -> dict[str, Any]:
    """Execute missing Sol coverage, then one GLM A/B adjudication pass."""

    prepared = prepare_cheap_consensus(
        suite_root=suite_root, run_id=run_id
    )
    root = Path(suite_root).expanduser().resolve()
    run_root = Path(prepared["run_root"])
    state_path = run_root / "state.json"
    state = true_north._read_json(state_path)
    started = time.monotonic()
    sol_packet_paths = sorted(
        (run_root / "packets" / "sol-completion").glob(
            "*.private.json"
        )
    )
    for packet_path in sol_packet_paths:
        name = packet_path.name.removesuffix(".private.json")
        marker = f"sol-completion/{name}"
        if marker in state["completed"]:
            continue
        if (
            int(state["usage"]["calls"]) + 1 > MAX_CALLS
            or int(state["usage"]["tokens"])
            + SOL_RESERVED_TOKENS_PER_ENVELOPE
            > MAX_TOKENS
        ):
            raise CheapConsensusError(
                "budget cannot reserve the next Sol completion envelope"
            )
        output_dir = run_root / "outputs" / "sol-completion" / name
        call_started = time.monotonic()
        last_message = output_dir / "last-message.private.txt"
        events_path = output_dir / "events.private.jsonl"
        recovered_attempt = (
            last_message.is_file() and events_path.is_file()
        )
        if recovered_attempt:
            output = true_north._read_json(last_message)
            validate_merge_adjudication(
                output, true_north._read_json(packet_path)
            )
            true_north._write_json(
                output_dir / "validated.private.json",
                output,
                immutable=True,
            )
            usage = true_north._codex_usage_from_jsonl(
                events_path.read_text(encoding="utf-8")
            )
            receipt = {
                "ok": True,
                "model": SOL_MODEL,
                "model_lane": SOL_MODEL_LANE,
                "effort": "high",
                "elapsed_seconds": 0.0,
                "usage": usage,
                "packet_sha256": true_north._sha256_file(packet_path),
                "recovered_after_local_validator_shape_defect": True,
            }
            receipt["receipt_sha256"] = true_north.sha256_text(
                true_north.dumps_json(receipt)
            )
            true_north._write_json(
                output_dir / "receipt.json",
                receipt,
                immutable=True,
            )
        else:
            receipt = true_north._run_codex_gold_packet(
                packet_path=packet_path,
                schema_path=(
                    run_root
                    / "schemas"
                    / "sol-completion"
                    / f"{name}.json"
                ),
                output_dir=output_dir,
                timeout_seconds=timeout_seconds,
                codex_binary=codex_binary,
                validator=validate_merge_adjudication,
                prompt_prefix=FLAGGED_SYSTEM_PROMPT,
            )
        state["completed"].append(marker)
        state["usage"]["calls"] += 1
        state["usage"]["tokens"] += _usage_tokens(receipt)
        state["usage"]["wall_seconds"] = round(
            float(state["usage"]["wall_seconds"])
            + time.monotonic()
            - call_started,
            3,
        )
        true_north._write_json(
            output_dir / "phase-c-receipt.json",
            {
                "schema_version": SCHEMA_VERSION,
                "stage": "pass_b_sol_missing_coverage",
                "provider_model": SOL_MODEL,
                "provider_lane": SOL_MODEL_LANE,
                "usage": receipt.get("usage", {}),
                "recovered_attempt": recovered_attempt,
                "base_receipt_sha256": true_north._sha256_file(
                    output_dir / "receipt.json"
                ),
            },
            immutable=True,
        )
        true_north._multipass_state_write(state_path, state)
        if int(state["usage"]["tokens"]) > MAX_TOKENS:
            raise CheapConsensusError(
                "actual Sol usage exceeded the Phase C token ceiling"
            )
    pass_b = _validated_sol_map(root)
    for path in sorted(
        (run_root / "outputs" / "sol-completion").glob(
            "*/validated.private.json"
        )
    ):
        for row in true_north._read_json(path)["items"]:
            pass_b[str(row["candidate_id"])] = {
                key: copy.deepcopy(value)
                for key, value in row.items()
                if key != "merge_reason"
            }
    source_packets = _source_packets(root)
    flagged_ids = {
        str(candidate["candidate_id"])
        for source in source_packets
        for candidate in source["packet"]["input"]["candidates"]
    }
    if set(pass_b) != flagged_ids:
        raise CheapConsensusError(
            "Pass B still lacks exact flagged coverage"
        )
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        _reference_provenance,
    ) = _reference_artifacts(root)
    pass_a = {
        str(row["candidate_id"]): copy.deepcopy(row)
        for output in reference_outputs.values()
        for row in output["items"]
    }
    jobs = []
    packet_by_key = {
        str(source["packet_key"]): source["packet"]
        for source in source_packets
    }
    for packet_key, source_packet in sorted(packet_by_key.items()):
        episode_id, segment_id = packet_key.split("/", 1)
        jobs.append(
            (
                episode_id,
                segment_id,
                build_adjudication_packet(
                    source_packet=source_packet,
                    pass_a=pass_a,
                    pass_b=pass_b,
                ),
            )
        )
    glm_outputs = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="adjudication",
        artifact_stage="glm-ab-adjudication",
        jobs=jobs,
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=GLM_MODEL,
        reserved_tokens_per_call=GLM_RESERVED_TOKENS_PER_PACKET,
        validator_override=validate_adjudication_output,
        system_prompt_override=ADJUDICATOR_SYSTEM_PROMPT,
    )
    selected: dict[str, list[str]] = {
        str(row["candidate_id"]): list(row["selected_claim_texts"])
        for output in glm_outputs.values()
        for row in output["items"]
    }
    if set(selected) != flagged_ids:
        raise CheapConsensusError(
            "Pass C does not exactly cover flagged candidates"
        )
    base_jobs, dispositions, candidates = _load_search_context(
        root, true_north._read_json(root / "manifest.json")
    )
    final_adjudication: dict[tuple[str, str], dict[str, Any]] = {}
    for key, reference in reference_outputs.items():
        source_by_id = {
            str(row["candidate_id"]): row
            for row in reference_packets[key]["input"]["candidates"]
        }
        items = []
        for row in reference["items"]:
            candidate_id = str(row["candidate_id"])
            if candidate_id not in selected:
                items.append(copy.deepcopy(row))
                continue
            items.append(
                _selected_item(
                    candidate_id=candidate_id,
                    selected=selected[candidate_id],
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
    state["usage"]["wall_seconds"] = round(
        float(state["usage"]["wall_seconds"])
        + max(0.0, time.monotonic() - started),
        3,
    )
    true_north._multipass_state_write(state_path, state)
    result = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "complete": True,
        "flagged_candidate_count": len(flagged_ids),
        "sol_reused_candidate_count": prepared[
            "sol_reused_candidate_count"
        ],
        "sol_completed_candidate_count": prepared[
            "sol_missing_candidate_count"
        ],
        "sol_completion_calls": len(sol_packet_paths),
        "glm_adjudication_calls": len(jobs),
        "usage": state["usage"],
        "within_call_ceiling": int(state["usage"]["calls"]) <= MAX_CALLS,
        "within_token_ceiling": int(state["usage"]["tokens"]) <= MAX_TOKENS,
        "cumulative_calls_after_run": (
            CUMULATIVE_CALLS_BEFORE + int(state["usage"]["calls"])
        ),
        "known_cumulative_tokens_after_run": (
            KNOWN_CUMULATIVE_TOKENS_BEFORE
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


def score_cheap_consensus(
    *,
    suite_root: str | Path,
    run_id: str = RUN_ID,
    experiment_id: str = EXPERIMENT_ID,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    result = true_north._read_json(run_root / "result.json")
    if not result["complete"]:
        raise CheapConsensusError(
            "incomplete Phase C run is not acceptance-eligible"
        )
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
    span_doc = {
        "schema_version": true_north.WORK_OUTPUT_SCHEMA_VERSION,
        "actor_span_rule": "deterministic_actor_span_rule_v1",
        "items": span_predictions,
    }
    span_doc["predictions_sha256"] = true_north.sha256_text(
        true_north.dumps_json(span_doc)
    )
    true_north._write_json(span_path, span_doc, immutable=False)
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
    aligned_ids = {
        candidate_id
        for candidate_id, row in candidate_scores.items()
        if row["strictly_scoreable"]
        and row["atomic_count"]["acceptable_count"]
        and row["value_state"]["predicted"] == "value"
    }
    manifest = true_north._read_json(root / "manifest.json")
    consensus = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    speaker_maps = {}
    for bundle_row in manifest["bundles"]:
        if bundle_row["partition"] != "development":
            continue
        bundle = true_north._read_json(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            speaker_maps[str(candidate["candidate_id"])] = bundle[
                "episode_context"
            ].get("speaker_map", [])
    prediction_map = {
        str(row["candidate_id"]): row for row in span_predictions
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
    source_packets = _source_packets(root)
    flagged_ids = {
        str(candidate["candidate_id"])
        for source in source_packets
        for candidate in source["packet"]["input"]["candidates"]
    }
    flagged_atomic = true_north._consensus_atomic_metrics(
        {
            **consensus,
            "items": [
                row
                for row in consensus["items"]
                if str(row["candidate_id"]) in flagged_ids
            ],
        },
        {
            candidate_id: prediction_map[candidate_id]
            for candidate_id in flagged_ids
        },
        require_complete_scope=True,
    )
    flagged_metrics = {
        str(row["metric"]): row["value"] for row in flagged_atomic
    }
    checkpoint = true_north._read_json(
        root
        / "certification"
        / true_north.TASK5_CHECKPOINT_FILENAME
    )
    aggregate = decoupled["aggregate"]
    acceptance = {
        "atomic_count_at_least_0_90": (
            float(aggregate["acceptable_atomic_count_rate"]) >= 0.90
        ),
        "matched_faithfulness_at_least_0_744435": (
            float(aggregate["claim_text_faithfulness_proxy"])
            >= 0.744435
        ),
        "matched_hallucination_at_most_0_093684": (
            float(aggregate["hallucination_rate_proxy"])
            <= 0.093684
        ),
        "macro_f1_at_least_0_790664": (
            float(aggregate["consensus_candidate_state_macro_f1"])
            >= 0.790664
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
        "experiment_id": experiment_id,
        "run_id": run_id,
        "complete": True,
        "acceptance_eligible": True,
        "composed": {
            "acceptable_atomic_count_rate": aggregate[
                "acceptable_atomic_count_rate"
            ],
            "flagged_acceptable_atomic_count_rate": flagged_metrics[
                "acceptable_atomic_count_rate"
            ],
            "matched_faithfulness": aggregate[
                "claim_text_faithfulness_proxy"
            ],
            "aligned_faithfulness": aligned["aggregate"][
                "claim_text_faithfulness_proxy"
            ],
            "speaker_exactness": aggregate["speaker_exactness"],
            "reported_actor_exactness": aggregate[
                "reported_actor_exactness"
            ],
            "candidate_state_macro_f1": aggregate[
                "consensus_candidate_state_macro_f1"
            ],
            "hallucination_rate_proxy": aggregate[
                "hallucination_rate_proxy"
            ],
            "hallucination_live_coupled_diagnostic": aggregate[
                "hallucination_rate_proxy_coupled_diagnostic"
            ],
            "nine_gate_table": decoupled["nine_gate_table"],
            "passed_gate_count": decoupled["passed_gate_count"],
        },
        "flagged_subset": {
            "candidate_count": len(flagged_ids),
            "acceptable_atomic_count_rate": flagged_metrics[
                "acceptable_atomic_count_rate"
            ],
        },
        "junk_and_contamination": {
            "intrinsic_junk_escape_count": checkpoint[
                "disposition_gate"
            ]["intrinsic_junk_escape_count"],
            "relational_contamination_count": checkpoint[
                "relational_merge_certification"
            ]["contamination_count"],
            "materialized_merge_count": checkpoint[
                "relational_merge_certification"
            ]["canonical_merge_count"],
        },
        "acceptance": acceptance,
        "passed": all(acceptance.values()),
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
    path = run_root / "score.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "score_path": str(path)}
