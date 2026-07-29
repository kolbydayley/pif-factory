"""Single authorized C2 revision with sol in the adjudicator seat."""

from __future__ import annotations

import copy
import time
from pathlib import Path
from typing import Any, Mapping

from . import true_north
from .true_north_cheap_consensus import (
    ADJUDICATOR_SYSTEM_PROMPT,
    RUN_ID as PHASE_C_RUN_ID,
    _selected_item,
    _validated_sol_map,
    score_cheap_consensus,
    validate_adjudication_output,
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
from .true_north_spark_split_default import _source_packets


SCHEMA_VERSION = "pif_true_north_c2_sol_adjudicator_v1"
EXPERIMENT_ID = "phase-c2-sol-adjudicator-20260729-v1"
RUN_ID = "task5-c2-sol-adjudicator-20260729-v1"
MAX_CALLS = 24
MAX_TOKENS = 700_000
MAX_WALL_SECONDS = 4 * 60 * 60
RESERVED_TOKENS_PER_ENVELOPE = 35_000
CUMULATIVE_CALLS_BEFORE = 287
KNOWN_CUMULATIVE_TOKENS_BEFORE = 3_029_113
LEDGER_PATH = (
    Path(__file__).resolve().parent.parent
    / "config"
    / "true_north_campaign_budget_ledger.json"
)


class C2SolAdjudicatorError(RuntimeError):
    """Raised when C2 violates its single-revision contract."""


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
        or row.get("one_run_only") is not True
        or row.get("provider_lane") != SOL_MODEL_LANE
        or int(row.get("max_calls", -1)) != MAX_CALLS
        or int(row.get("max_tokens", -1)) != MAX_TOKENS
        or int(row.get("reserved_tokens_per_envelope", -1))
        != RESERVED_TOKENS_PER_ENVELOPE
    ):
        raise C2SolAdjudicatorError(
            "C2 budget is not declared exactly before execution"
        )
    return ledger, true_north.sha256_text(
        true_north.dumps_json(ledger)
    )


def _source_adjudication_packets(
    root: Path,
) -> dict[tuple[str, str], dict[str, Any]]:
    phase_c_root = root / "multipass" / "runs" / PHASE_C_RUN_ID
    configuration = true_north._read_json(
        phase_c_root / "configuration.json"
    )
    result = true_north._read_json(phase_c_root / "result.json")
    if (
        result.get("complete") is not True
        or configuration.get("one_run_only") is not True
        or configuration.get("union_contract")
        != (
            "selected claim texts must be a non-empty subset of the "
            "exact A/B union; third structures are rejected"
        )
    ):
        raise C2SolAdjudicatorError(
            "Phase C source run is not the completed frozen union contract"
        )
    packets = {
        (path.parent.name, path.stem.removesuffix(".private")): (
            true_north._read_json(path)
        )
        for path in sorted(
            (
                phase_c_root
                / "packets"
                / "glm-ab-adjudication"
            ).glob("*/*.private.json")
        )
    }
    if len(packets) != 19:
        raise C2SolAdjudicatorError(
            "C2 requires exactly 19 frozen Phase C adjudication packets"
        )
    for packet in packets.values():
        if (
            packet.get("schema_version")
            != "pif_true_north_cheap_consensus_v1"
        ):
            raise C2SolAdjudicatorError(
                "Phase C adjudication packet schema drift"
            )
    return packets


def codex_compatible_output_schema(
    packet: Mapping[str, Any],
) -> dict[str, Any]:
    """Flatten Phase C's oneOf schema for Codex response-format support.

    The provider schema is intentionally broader across candidates. The
    unchanged semantic validator still enforces each candidate's exact union.
    """

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
            "schema_version": {"const": packet["schema_version"]},
            "items": {
                "type": "array",
                "minItems": len(candidate_ids),
                "maxItems": len(candidate_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "candidate_id",
                        "selected_claim_texts",
                    ],
                    "properties": {
                        "candidate_id": {
                            "type": "string",
                            "enum": candidate_ids,
                        },
                        "selected_claim_texts": {
                            "type": "array",
                            "minItems": 1,
                            "uniqueItems": True,
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


def prepare_c2(
    *,
    suite_root: str | Path,
    run_id: str = RUN_ID,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise C2SolAdjudicatorError(
            "suite verification failed before C2"
        )
    manifest = true_north._read_json(root / "manifest.json")
    if manifest["measurement_contract"]["version"] != CONTRACT_VERSION:
        raise C2SolAdjudicatorError("C2 requires contract v8")
    ledger, ledger_sha = _ledger()
    source_packets = _source_adjudication_packets(root)
    reserved_tokens = (
        len(source_packets) * RESERVED_TOKENS_PER_ENVELOPE
    )
    if (
        len(source_packets) > MAX_CALLS
        or reserved_tokens > MAX_TOKENS
    ):
        raise C2SolAdjudicatorError(
            "whole-run C2 packet-budget preflight failed"
        )
    source_sol = _validated_sol_map(root)
    for path in sorted(
        (
            root
            / "multipass"
            / "runs"
            / PHASE_C_RUN_ID
            / "outputs"
            / "sol-completion"
        ).glob("*/validated.private.json")
    ):
        for row in true_north._read_json(path)["items"]:
            source_sol[str(row["candidate_id"])] = {
                key: copy.deepcopy(value)
                for key, value in row.items()
                if key != "merge_reason"
            }
    if len(source_sol) != 106:
        raise C2SolAdjudicatorError(
            "completed Phase C Sol pass does not cover 106 candidates"
        )
    run_root = root / "multipass" / "runs" / run_id
    snapshot = (
        root / "multipass" / "budget-ledger" / f"{EXPERIMENT_ID}.json"
    )
    true_north._write_json(snapshot, ledger, immutable=True)
    packet_hashes = {}
    for (episode_id, segment_id), packet in sorted(
        source_packets.items()
    ):
        destination = (
            run_root
            / "packets"
            / "sol-ab-adjudication"
            / episode_id
            / f"{segment_id}.private.json"
        )
        schema_path = (
            run_root
            / "schemas"
            / "sol-ab-adjudication"
            / episode_id
            / f"{segment_id}.json"
        )
        true_north._write_json(destination, packet, immutable=True)
        true_north._write_json(
            schema_path,
            codex_compatible_output_schema(packet),
            immutable=False,
        )
        packet_hashes[f"{episode_id}/{segment_id}"] = (
            true_north._sha256_file(destination)
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
        "pass_a_run_id": REFERENCE_RUN_ID,
        "flagged_candidate_count": len(source_sol),
        "provider_envelope_count": len(source_packets),
        "system_prompt_sha256": true_north.sha256_text(
            ADJUDICATOR_SYSTEM_PROMPT
        ),
        "union_contract": (
            "identical Phase C non-empty exact A/B claim-string union; "
            "third structures rejected"
        ),
        "unflagged_control": REFERENCE_RUN_ID,
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
            "max_wall_seconds": MAX_WALL_SECONDS,
            "reserved_tokens_per_envelope": (
                RESERVED_TOKENS_PER_ENVELOPE
            ),
            "whole_run_reserved_tokens": reserved_tokens,
        },
        "budget_ledger_sha256": ledger_sha,
        "episode_ids": list(SEARCH_EPISODE_IDS),
        "one_run_only": True,
        "terminal_after_run_regardless_of_outcome": True,
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(configuration)
    )
    config_path = run_root / "configuration.json"
    if config_path.is_file():
        if true_north._read_json(config_path) != configuration:
            raise C2SolAdjudicatorError("C2 resume configuration drift")
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
        "flagged_candidate_count": 106,
        "provider_envelope_count": len(source_packets),
        "whole_run_reserved_calls": len(source_packets),
        "whole_run_reserved_tokens": reserved_tokens,
        "within_call_ceiling": len(source_packets) <= MAX_CALLS,
        "within_token_ceiling": reserved_tokens <= MAX_TOKENS,
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


def _reconcile_preinference_schema_rejection(
    *,
    run_root: Path,
    state: dict[str, Any],
) -> None:
    """Correct conservative reservation accounting for a proven 400 reject."""

    marker = run_root / "provider-schema-rejection-accounting.json"
    if marker.is_file() or not state.get("failures"):
        return
    first = state["failures"][0]
    if first.get("usage") != {}:
        return
    output_dir = (
        run_root
        / "outputs"
        / "sol-ab-adjudication"
        / "ep_7ec9f808a3955c720aeb94ff"
        / "seg_00334a52223e256c48ca81c8"
    )
    events_path = output_dir / "events.private.jsonl"
    if not events_path.is_file():
        return
    events = events_path.read_text(encoding="utf-8")
    if (
        "invalid_json_schema" not in events
        or "'oneOf' is not permitted" not in events
    ):
        return
    if int(state["usage"]["tokens"]) < RESERVED_TOKENS_PER_ENVELOPE:
        raise C2SolAdjudicatorError(
            "schema-rejection accounting cannot remove reservation"
        )
    state["usage"]["tokens"] -= RESERVED_TOKENS_PER_ENVELOPE
    first["classification"] = (
        "provider_400_before_model_execution_no_inference_tokens"
    )
    first["charged_calls"] = 1
    first["charged_tokens"] = 0
    accounting = {
        "schema_version": SCHEMA_VERSION,
        "reason": (
            "Codex response-format API rejected Phase C's oneOf schema "
            "before model execution; packet and semantic validator unchanged"
        ),
        "charged_calls": 1,
        "charged_tokens": 0,
        "events_sha256": true_north._sha256_file(events_path),
        "provider_schema_adapter": (
            "flattened candidate schema with exact-union semantic "
            "validation retained"
        ),
    }
    accounting["accounting_sha256"] = true_north.sha256_text(
        true_north.dumps_json(accounting)
    )
    true_north._write_json(marker, accounting, immutable=True)
    true_north._multipass_state_write(run_root / "state.json", state)


def _pass_maps(
    root: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    (
        _reference_root,
        _reference_packets,
        reference_outputs,
        _reference_provenance,
    ) = _reference_artifacts(root)
    pass_a = {
        str(row["candidate_id"]): copy.deepcopy(row)
        for output in reference_outputs.values()
        for row in output["items"]
    }
    pass_b = _validated_sol_map(root)
    for path in sorted(
        (
            root
            / "multipass"
            / "runs"
            / PHASE_C_RUN_ID
            / "outputs"
            / "sol-completion"
        ).glob("*/validated.private.json")
    ):
        for row in true_north._read_json(path)["items"]:
            pass_b[str(row["candidate_id"])] = {
                key: copy.deepcopy(value)
                for key, value in row.items()
                if key != "merge_reason"
            }
    return pass_a, pass_b


def adjudicator_statistics(
    *,
    pass_a: Mapping[str, Mapping[str, Any]],
    pass_b: Mapping[str, Mapping[str, Any]],
    selected: Mapping[str, list[str]],
    prior_selected: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    counts = {
        "proposals_identical": 0,
        "chose_a": 0,
        "chose_b": 0,
        "merged_union": 0,
    }
    for candidate_id, chosen in selected.items():
        a = [
            str(row["claim_text"])
            for row in pass_a[candidate_id]["atomic_claims"]
        ]
        b = [
            str(row["claim_text"])
            for row in pass_b[candidate_id]["atomic_claims"]
        ]
        if a == b and chosen == a:
            counts["proposals_identical"] += 1
        elif chosen == a:
            counts["chose_a"] += 1
        elif chosen == b:
            counts["chose_b"] += 1
        else:
            counts["merged_union"] += 1
    total = len(selected)
    contested = total - counts["proposals_identical"]
    result: dict[str, Any] = {
        "candidate_count": total,
        **counts,
        "contested_candidate_count": contested,
        "contested_choice_rates": {
            key: (
                counts[key] / contested if contested else 0.0
            )
            for key in ("chose_a", "chose_b", "merged_union")
        },
    }
    if prior_selected is not None:
        shared = set(selected) & set(prior_selected)
        disagreement = sum(
            selected[candidate_id] != prior_selected[candidate_id]
            for candidate_id in shared
        )
        result["vs_phase_c_glm_adjudicator"] = {
            "shared_candidate_count": len(shared),
            "exact_selection_agreement_count": len(shared) - disagreement,
            "exact_selection_disagreement_count": disagreement,
            "exact_selection_disagreement_rate": (
                disagreement / len(shared) if shared else 0.0
            ),
        }
    return result


def run_c2(
    *,
    suite_root: str | Path,
    run_id: str = RUN_ID,
    timeout_seconds: int = 1800,
    codex_binary: str = "codex",
) -> dict[str, Any]:
    """Run the one authorized sequential Sol adjudication pass."""

    prepared = prepare_c2(suite_root=suite_root, run_id=run_id)
    root = Path(suite_root).expanduser().resolve()
    run_root = Path(prepared["run_root"])
    state_path = run_root / "state.json"
    state = true_north._read_json(state_path)
    _reconcile_preinference_schema_rejection(
        run_root=run_root, state=state
    )
    packet_paths = sorted(
        (run_root / "packets" / "sol-ab-adjudication").glob(
            "*/*.private.json"
        )
    )
    for packet_path in packet_paths:
        episode_id = packet_path.parent.name
        segment_id = packet_path.stem.removesuffix(".private")
        marker = f"sol-ab-adjudication/{episode_id}/{segment_id}"
        if marker in state["completed"]:
            continue
        if (
            int(state["usage"]["calls"]) + 1 > MAX_CALLS
            or int(state["usage"]["tokens"])
            + RESERVED_TOKENS_PER_ENVELOPE
            > MAX_TOKENS
        ):
            raise C2SolAdjudicatorError(
                "budget cannot reserve the next C2 envelope"
            )
        output_dir = (
            run_root
            / "outputs"
            / "sol-ab-adjudication"
            / episode_id
            / segment_id
        )
        call_started = time.monotonic()
        try:
            receipt = true_north._run_codex_gold_packet(
                packet_path=packet_path,
                schema_path=(
                    run_root
                    / "schemas"
                    / "sol-ab-adjudication"
                    / episode_id
                    / f"{segment_id}.json"
                ),
                output_dir=output_dir,
                timeout_seconds=timeout_seconds,
                codex_binary=codex_binary,
                validator=validate_adjudication_output,
                prompt_prefix=ADJUDICATOR_SYSTEM_PROMPT,
            )
        except Exception as exc:
            events_path = output_dir / "events.private.jsonl"
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
            state["usage"]["tokens"] += (
                tokens or RESERVED_TOKENS_PER_ENVELOPE
            )
            state["usage"]["wall_seconds"] = round(
                float(state["usage"]["wall_seconds"])
                + time.monotonic()
                - call_started,
                3,
            )
            state["failures"].append(
                {
                    "envelope": marker,
                    "failure": f"{type(exc).__name__}: {exc}",
                    "usage": usage,
                }
            )
            true_north._multipass_state_write(state_path, state)
            break
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
            output_dir / "c2-receipt.json",
            {
                "schema_version": SCHEMA_VERSION,
                "stage": "pass_c2_sol_adjudication",
                "provider_model": SOL_MODEL,
                "provider_lane": SOL_MODEL_LANE,
                "usage": receipt.get("usage", {}),
                "base_receipt_sha256": true_north._sha256_file(
                    output_dir / "receipt.json"
                ),
            },
            immutable=True,
        )
        true_north._multipass_state_write(state_path, state)
        if int(state["usage"]["tokens"]) > MAX_TOKENS:
            raise C2SolAdjudicatorError(
                "actual C2 usage exceeded the token ceiling"
            )
    complete = len(state["completed"]) == len(packet_paths)
    if complete:
        selected = {
            str(row["candidate_id"]): list(
                row["selected_claim_texts"]
            )
            for path in sorted(
                (
                    run_root
                    / "outputs"
                    / "sol-ab-adjudication"
                ).glob("*/*/validated.private.json")
            )
            for row in true_north._read_json(path)["items"]
        }
        pass_a, pass_b = _pass_maps(root)
        phase_c_selected = {
            str(row["candidate_id"]): list(
                row["selected_claim_texts"]
            )
            for path in sorted(
                (
                    root
                    / "multipass"
                    / "runs"
                    / PHASE_C_RUN_ID
                    / "outputs"
                    / "glm-ab-adjudication"
                ).glob("*/*/validated.private.json")
            )
            for row in true_north._read_json(path)["items"]
        }
        stats = adjudicator_statistics(
            pass_a=pass_a,
            pass_b=pass_b,
            selected=selected,
            prior_selected=phase_c_selected,
        )
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
        manifest = true_north._read_json(root / "manifest.json")
        base_jobs, dispositions, candidates = _load_search_context(
            root, manifest
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
        state["candidate_count"] = len(candidates)
        state["flagged_candidate_count"] = len(selected)
    else:
        stats = {}
    state["complete"] = complete
    true_north._multipass_state_write(state_path, state)
    result = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "complete": complete,
        "validated_provider_envelopes": len(state["completed"]),
        "expected_provider_envelopes": len(packet_paths),
        "failure_count": len(state["failures"]),
        "flagged_candidate_count": (
            106 if complete else None
        ),
        "adjudicator_statistics": stats,
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
        "decomposition_lane_closed_permanently": True,
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


def score_c2(
    *,
    suite_root: str | Path,
    run_id: str = RUN_ID,
) -> dict[str, Any]:
    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    result = true_north._read_json(run_root / "result.json")
    if not result["complete"]:
        raise C2SolAdjudicatorError(
            "incomplete C2 run is not acceptance-eligible"
        )
    score = score_cheap_consensus(
        suite_root=root,
        run_id=run_id,
        experiment_id=EXPERIMENT_ID,
    )
    score["schema_version"] = SCHEMA_VERSION
    score["adjudicator_statistics"] = result[
        "adjudicator_statistics"
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


def finalize_c2_operational_failure(
    *,
    suite_root: str | Path,
    run_id: str = RUN_ID,
) -> dict[str, Any]:
    """Freeze the exhausted C2 schema-rejection path with exact accounting."""

    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    state_path = run_root / "state.json"
    state = true_north._read_json(state_path)
    if state.get("complete"):
        raise C2SolAdjudicatorError(
            "completed C2 runs use the semantic scorer"
        )
    if len(state.get("failures", [])) != 2:
        raise C2SolAdjudicatorError(
            "C2 operational finalizer requires exactly two failures"
        )
    second = state["failures"][1]
    events_path = (
        run_root
        / "outputs"
        / "sol-ab-adjudication"
        / "ep_7ec9f808a3955c720aeb94ff"
        / "seg_00334a52223e256c48ca81c8"
        / "events.private.jsonl"
    )
    events = events_path.read_text(encoding="utf-8")
    marker = run_root / "provider-schema-rejection-2-accounting.json"
    if not marker.is_file():
        if (
            second.get("usage") != {}
            or "invalid_json_schema" not in events
            or "'uniqueItems' is not permitted" not in events
            or int(state["usage"]["tokens"])
            < RESERVED_TOKENS_PER_ENVELOPE
        ):
            raise C2SolAdjudicatorError(
                "second C2 failure is not the proven pre-inference reject"
            )
        state["usage"]["tokens"] -= RESERVED_TOKENS_PER_ENVELOPE
        second["classification"] = (
            "provider_400_before_model_execution_no_inference_tokens"
        )
        second["charged_calls"] = 1
        second["charged_tokens"] = 0
        accounting = {
            "schema_version": SCHEMA_VERSION,
            "reason": (
                "Codex response-format API rejected uniqueItems before "
                "model execution; no semantic output exists"
            ),
            "charged_calls": 1,
            "charged_tokens": 0,
            "events_sha256": true_north._sha256_file(events_path),
        }
        accounting["accounting_sha256"] = true_north.sha256_text(
            true_north.dumps_json(accounting)
        )
        true_north._write_json(marker, accounting, immutable=True)
        true_north._multipass_state_write(state_path, state)
    terminal = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "complete": False,
        "acceptance_eligible": False,
        "terminal_reason": (
            "provider_response_schema_incompatibility_before_model_"
            "execution_after_one_bounded_adapter_retry"
        ),
        "validated_provider_envelopes": 0,
        "expected_provider_envelopes": 19,
        "failure_count": 2,
        "flagged_candidate_count": 106,
        "flagged_subset_accuracy": None,
        "adjudicator_statistics": None,
        "semantic_metrics": None,
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
        "decomposition_lane_closed_permanently": True,
        "holdout_opened": False,
        "production_mutation": False,
    }
    terminal["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(terminal)
    )
    true_north._write_json(
        run_root / "result.json", terminal, immutable=False
    )
    return {**terminal, "result_path": str(run_root / "result.json")}
