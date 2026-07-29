"""Input-side split-default inversion for flagged Stage-B candidates."""

from __future__ import annotations

import copy
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import true_north
from .true_north_dual_decomposition import (
    GLM_MODEL,
    REFERENCE_RUN_ID,
    SEARCH_EPISODE_IDS,
    _load_search_context,
    _reference_artifacts,
)
from .true_north_semantic_scoring import score_campaign


SCHEMA_VERSION = "pif_true_north_input_split_default_v1"
EXPERIMENT_ID = "stage-b-input-split-default-20260729-v1"
MAX_CALLS = 30
MAX_TOKENS = 350_000
MAX_WALL_SECONDS = 2 * 60 * 60
RESERVED_TOKENS_PER_CALL = 12_000
CUMULATIVE_CALLS_BEFORE_RUN = 208
KNOWN_CUMULATIVE_TOKENS_BEFORE_RUN = 1_688_964
LEDGER_PATH = (
    Path(__file__).resolve().parent.parent
    / "config"
    / "true_north_campaign_budget_ledger.json"
)

MERGE_REASONS = (
    "none",
    "fragment_not_independently_assertable",
    "shared_subject_or_predicate",
    "mechanism_reason_condition_or_qualification",
    "single_causal_contrast_or_comparison",
)
_BOUNDARY_RE = re.compile(
    r"\s*(?P<comma>,)\s*"
    r"|\s+(?P<coordinator>and|or|but|yet|nor|so)\s+",
    re.IGNORECASE,
)
_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")

FLAGGED_SYSTEM_PROMPT = """You adjudicate deterministic over-segmentations of podcast claim proposals.
Do not use tools. Each flagged candidate arrives with one original proposal and
an input-default decomposition into clauses. Treat those clauses as the
starting claims. For each clause ask whether it is independently true or false
and meaningful when cited alone. Keep independently assertable clauses
separate. Merge only fragments that share an inseparable subject or predicate,
or clauses that merely supply a mechanism, reason, condition, qualification,
single causal relation, contrast, or comparison for one conclusion.
Over-segmentation is expected; under-merging is safer than collapsing distinct
conclusions. Preserve the proposal's wording per retained piece and change only
what grammar requires. The original Stage-B edit_reason closed set still
applies. Set merge_reason=none when no proposed pieces were merged; otherwise
choose the one closed-set reason that explains the merge. Return only the exact
schema-valid JSON requested by the packet."""

UNFLAGGED_SYSTEM_PROMPT = true_north.MULTIPASS_SYSTEM_PROMPTS["adjudication"]


class InputSplitDefaultError(RuntimeError):
    """Raised when the split-default experiment violates its contract."""


def compound_flag(claim_text: str) -> bool:
    text = str(claim_text)
    folded = text.casefold()
    return " and " in folded or " or " in folded or "," in text


def _segments_with_boundaries(text: str) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    cursor = 0
    preceding_boundary: str | None = None
    for match in _BOUNDARY_RE.finditer(text):
        value = text[cursor : match.start()].strip()
        if value:
            start = text.find(value, cursor, match.start())
            segments.append(
                {
                    "text": value,
                    "start": start,
                    "end": start + len(value),
                    "preceding_boundary": preceding_boundary,
                }
            )
        preceding_boundary = match.group(0).strip()
        cursor = match.end()
    value = text[cursor:].strip()
    if value:
        start = text.find(value, cursor)
        segments.append(
            {
                "text": value,
                "start": start,
                "end": start + len(value),
                "preceding_boundary": preceding_boundary,
            }
        )
    return segments


def deterministic_clause_segmentation(
    *,
    proposed_claim_text: str,
    evidence_text: str,
) -> list[dict[str, Any]]:
    proposal_segments = _segments_with_boundaries(proposed_claim_text)
    evidence_segments = _segments_with_boundaries(evidence_text)
    if len(proposal_segments) < 2:
        raise InputSplitDefaultError(
            "flagged proposal did not produce at least two clauses"
        )
    if not evidence_segments:
        evidence_segments = [
            {
                "text": evidence_text,
                "start": 0,
                "end": len(evidence_text),
                "preceding_boundary": None,
            }
        ]

    def tokens(value: str) -> set[str]:
        return {
            token.casefold() for token in _TOKEN_RE.findall(value)
        }

    result: list[dict[str, Any]] = []
    for index, proposal in enumerate(proposal_segments):
        proposal_tokens = tokens(str(proposal["text"]))
        ranked = []
        for evidence_index, evidence in enumerate(evidence_segments):
            evidence_tokens = tokens(str(evidence["text"]))
            overlap = len(proposal_tokens & evidence_tokens)
            union = len(proposal_tokens | evidence_tokens)
            ranked.append(
                (
                    overlap / union if union else 0.0,
                    overlap,
                    -evidence_index,
                    evidence,
                )
            )
        aligned = max(ranked, key=lambda row: row[:3])[3]
        result.append(
            {
                "clause_index": index,
                "proposed_clause_text": proposal["text"],
                "proposal_start": proposal["start"],
                "proposal_end": proposal["end"],
                "preceding_boundary": proposal["preceding_boundary"],
                "evidence_aligned_clause_text": aligned["text"],
                "evidence_relative_start": aligned["start"],
                "evidence_relative_end": aligned["end"],
            }
        )
    return result


def merge_adjudication_schema(
    candidate_ids: Sequence[str],
) -> dict[str, Any]:
    schema = true_north.multipass_adjudication_schema(candidate_ids)
    item = schema["properties"]["items"]["items"]
    item["required"].append("merge_reason")
    item["properties"]["merge_reason"] = {
        "type": "string",
        "enum": list(MERGE_REASONS),
    }
    return schema


def _flagged_packet(
    reference_packet: Mapping[str, Any],
    flagged_ids: set[str],
) -> dict[str, Any]:
    candidates = []
    for source in reference_packet["input"]["candidates"]:
        candidate_id = str(source["candidate_id"])
        if candidate_id not in flagged_ids:
            continue
        candidate = copy.deepcopy(source)
        candidate["proposed_decomposition"] = {
            "default_action": "keep_independently_assertable_pieces_and_merge_fragments",
            "segmentation_rule": (
                "coordinating_conjunctions_and_clause_boundary_commas_v1"
            ),
            "clauses": deterministic_clause_segmentation(
                proposed_claim_text=str(source["proposed_claim_text"]),
                evidence_text=str(source["evidence_text"]),
            ),
        }
        candidates.append(candidate)
    if not candidates:
        raise InputSplitDefaultError("flagged packet is empty")
    candidate_ids = [
        str(candidate["candidate_id"]) for candidate in candidates
    ]
    decisions = {
        str(row["candidate_id"]): copy.deepcopy(row)
        for row in reference_packet["input"]["stage_a_decisions"]
    }
    return {
        "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
        "suite_id": true_north.SUITE_ID,
        "multipass_stage": "adjudication",
        "task": (
            "Keep or merge the deterministic proposed clause decomposition."
        ),
        "instructions": [
            "Do not change disposition or attribute speakers.",
            "Treat proposed_decomposition.clauses as the input default.",
            "Merge only pieces that are not independently assertable.",
            "Reuse original proposal wording per retained piece.",
            "merge_reason=none requires output count equal proposed clause count.",
        ],
        "output_schema": merge_adjudication_schema(candidate_ids),
        "input": {
            "episode": copy.deepcopy(
                reference_packet["input"]["episode"]
            ),
            "segment": copy.deepcopy(
                reference_packet["input"]["segment"]
            ),
            "candidates": candidates,
            "stage_a_decisions": [
                decisions[candidate_id]
                for candidate_id in candidate_ids
            ],
        },
    }


def validate_merge_adjudication(
    output: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> None:
    true_north.validate_multipass_adjudication(output, packet)
    clause_counts = {
        str(row["candidate_id"]): len(
            row["proposed_decomposition"]["clauses"]
        )
        for row in packet["input"]["candidates"]
    }
    for item in output["items"]:
        candidate_id = str(item["candidate_id"])
        output_count = len(item["atomic_claims"])
        proposed_count = clause_counts[candidate_id]
        if output_count > proposed_count:
            raise InputSplitDefaultError(
                "merge-default output introduced more pieces than input"
            )
        merged = output_count < proposed_count
        if (str(item["merge_reason"]) == "none") == merged:
            raise InputSplitDefaultError(
                "merge_reason does not match proposed/output count"
            )


def _ledger() -> tuple[dict[str, Any], str]:
    ledger = true_north._read_json(LEDGER_PATH)
    experiments = {
        str(row["experiment_id"]): row
        for row in ledger["declared_experiments"]
    }
    declared = experiments.get(EXPERIMENT_ID)
    if (
        declared is None
        or declared.get("status") != "declared"
        or int(declared["max_calls"]) != MAX_CALLS
        or int(declared["max_tokens"]) != MAX_TOKENS
        or str(declared["model"]) != GLM_MODEL
    ):
        raise InputSplitDefaultError(
            "input split-default budget declaration is invalid"
        )
    return ledger, true_north.sha256_text(
        true_north.dumps_json(ledger)
    )


def run_input_split_default_experiment(
    *,
    suite_root: str | Path,
    run_id: str | None = None,
    workers: int = 3,
    timeout_seconds: int = 900,
    opencode_binary: str = "/opt/homebrew/bin/opencode",
    runner: Any | None = None,
) -> dict[str, Any]:
    if workers < 1 or workers > 3:
        raise InputSplitDefaultError("workers must be between 1 and 3")
    root = Path(suite_root).expanduser().resolve()
    verification = true_north.verify_suite(
        output_root=root.parent, suite=root.name
    )
    if not verification["ok"]:
        raise InputSplitDefaultError("suite verification failed")
    ledger, ledger_sha = _ledger()
    manifest = true_north._read_json(root / "manifest.json")
    base_jobs, dispositions, candidates = _load_search_context(root, manifest)
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        reference_provenance,
    ) = _reference_artifacts(root)
    eligible_ids = {
        str(row["candidate_id"])
        for packet in reference_packets.values()
        for row in packet["input"]["candidates"]
    }
    flagged_ids = {
        candidate_id
        for candidate_id in eligible_ids
        if compound_flag(str(candidates[candidate_id]["claim_text"]))
    }
    if not flagged_ids:
        raise InputSplitDefaultError("compound screen selected nothing")
    flagged_packets: dict[tuple[str, str], dict[str, Any]] = {}
    for key, packet in sorted(reference_packets.items()):
        packet_ids = {
            str(row["candidate_id"])
            for row in packet["input"]["candidates"]
        }
        selected = flagged_ids & packet_ids
        if selected:
            flagged_packets[key] = _flagged_packet(packet, selected)
    if len(flagged_packets) > MAX_CALLS:
        raise InputSplitDefaultError(
            "flagged packets exceed declared call ceiling"
        )
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
        "episode_ids": list(SEARCH_EPISODE_IDS),
        "model": GLM_MODEL,
        "selector": {
            "rule": (
                "casefolded claim_text contains literal ' and ' or ' or ', "
                "or claim_text contains at least one comma"
            ),
            "eligible_candidate_count": len(eligible_ids),
            "flagged_candidate_count": len(flagged_ids),
            "unflagged_candidate_count": len(eligible_ids - flagged_ids),
            "flagged_candidate_ids": sorted(flagged_ids),
        },
        "input_contract": {
            "segmentation_rule": (
                "coordinating_conjunctions_and_clause_boundary_commas_v1"
            ),
            "merge_reasons": list(MERGE_REASONS),
            "flagged_packet_count": len(flagged_packets),
            "flagged_packet_hashes": {
                f"{key[0]}/{key[1]}": true_north.sha256_text(
                    true_north.dumps_json(packet)
                )
                for key, packet in sorted(flagged_packets.items())
            },
            "unflagged_source_run_id": REFERENCE_RUN_ID,
            "unflagged_source_configuration_sha256": (
                reference_provenance["configuration_sha256"]
            ),
        },
        "prompts": {
            "flagged_sha256": true_north.sha256_text(
                FLAGGED_SYSTEM_PROMPT
            ),
            "unflagged_sha256": true_north.sha256_text(
                UNFLAGGED_SYSTEM_PROMPT
            ),
        },
        "budget": {
            "max_calls": MAX_CALLS,
            "max_tokens": MAX_TOKENS,
            "max_wall_seconds": MAX_WALL_SECONDS,
        },
        "budget_ledger": {
            "ledger_sha256": ledger_sha,
            "snapshot_path": str(snapshot_path),
            "cumulative_calls_before_run": CUMULATIVE_CALLS_BEFORE_RUN,
            "campaign_total_policy": "report_only_no_hard_stop",
        },
        "holdout_access_allowed": False,
        "production_database_open_allowed": False,
    }
    configuration["configuration_sha256"] = true_north.sha256_text(
        true_north.dumps_json(configuration)
    )
    resolved_run_id = run_id or (
        "task5-input-split-default-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + configuration["configuration_sha256"][:8]
    )
    run_root = root / "multipass" / "runs" / resolved_run_id
    config_path = run_root / "configuration.json"
    state_path = run_root / "state.json"
    if config_path.is_file():
        if true_north._read_json(config_path) != configuration:
            raise InputSplitDefaultError("resume configuration drift")
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
    flagged_outputs = true_north._multipass_execute_stage(
        run_root=run_root,
        stage="adjudication",
        artifact_stage="input-split-default",
        jobs=[
            (key[0], key[1], packet)
            for key, packet in sorted(flagged_packets.items())
        ],
        state=state,
        workers=workers,
        timeout_seconds=timeout_seconds,
        opencode_binary=opencode_binary,
        runner=runner,
        model=GLM_MODEL,
        reserved_tokens_per_call=RESERVED_TOKENS_PER_CALL,
        validator_override=validate_merge_adjudication,
        system_prompt_override=FLAGGED_SYSTEM_PROMPT,
    )
    flagged_by_candidate = {
        str(row["candidate_id"]): {
            key: value
            for key, value in row.items()
            if key != "merge_reason"
        }
        for output in flagged_outputs.values()
        for row in output["items"]
    }
    if set(flagged_by_candidate) != flagged_ids:
        raise InputSplitDefaultError("flagged output scope is incomplete")

    final_adjudication: dict[tuple[str, str], dict[str, Any]] = {}
    for key, reference in reference_outputs.items():
        items = []
        for row in reference["items"]:
            candidate_id = str(row["candidate_id"])
            items.append(
                copy.deepcopy(
                    flagged_by_candidate[candidate_id]
                    if candidate_id in flagged_ids
                    else row
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
        true_north._write_json(
            run_root
            / "outputs"
            / "adjudication-final"
            / key[0]
            / key[1]
            / "validated.private.json",
            output,
            immutable=True,
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
    state["eligible_candidate_count"] = len(eligible_ids)
    state["flagged_candidate_count"] = len(flagged_ids)
    state["flagged_packet_count"] = len(flagged_packets)
    true_north._multipass_state_write(state_path, state)
    result = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": resolved_run_id,
        "configuration_sha256": configuration[
            "configuration_sha256"
        ],
        "complete": True,
        "flagged_candidate_count": len(flagged_ids),
        "unflagged_candidate_count": len(eligible_ids - flagged_ids),
        "flagged_packet_count": len(flagged_packets),
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


def _subset_atomic_metrics(
    *,
    consensus_document: Mapping[str, Any],
    predictions: Mapping[str, Mapping[str, Any]],
    candidate_ids: set[str],
) -> dict[str, Any]:
    core = true_north._consensus_atomic_metrics(
        {
            **consensus_document,
            "items": [
                row for row in consensus_document["items"]
                if str(row["candidate_id"]) in candidate_ids
            ],
        },
        {
            candidate_id: predictions[candidate_id]
            for candidate_id in candidate_ids
        },
        require_complete_scope=True,
    )
    return {str(row["metric"]): row["value"] for row in core}


def score_input_split_default_experiment(
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
        raise InputSplitDefaultError("experiment run is incomplete")
    full = true_north.score_multipass_run(
        run_id=run_id,
        output_root=root.parent,
        suite=root.name,
    )
    predictions: list[dict[str, Any]] = []
    for episode_id in configuration["episode_ids"]:
        for path in sorted(
            (run_root / "outputs" / "composed" / episode_id).glob(
                "*/validated.private.json"
            )
        ):
            predictions.extend(true_north._read_json(path)["items"])
    prediction_map = {
        str(row["candidate_id"]): row for row in predictions
    }
    flagged_ids = set(
        configuration["selector"]["flagged_candidate_ids"]
    )
    eligible_ids = {
        candidate_id
        for candidate_id, row in prediction_map.items()
        if str(row["disposition"]) in {"retain", "revise"}
    }
    unflagged_ids = eligible_ids - flagged_ids
    consensus_document = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    flagged_metrics = _subset_atomic_metrics(
        consensus_document=consensus_document,
        predictions=prediction_map,
        candidate_ids=flagged_ids,
    )
    unflagged_metrics = _subset_atomic_metrics(
        consensus_document=consensus_document,
        predictions=prediction_map,
        candidate_ids=unflagged_ids,
    )
    private_score = true_north._read_json(
        run_root / "score.private.json"
    )
    candidate_scores = {
        str(row["candidate_id"]): row
        for row in private_score["private_candidate_scores"]
    }
    aligned_ids = {
        candidate_id
        for candidate_id, row in candidate_scores.items()
        if row["strictly_scoreable"]
        and row["consensus_state"] == "consensus_value"
        and row["value_state"]["predicted"] == "value"
        and row["atomic_count"]["acceptable_count"]
    }
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
        [
            prediction_map[candidate_id]
            for candidate_id in sorted(aligned_ids)
        ],
        consensus_document["items"],
        preferred_document["items"],
        subset_candidate_ids=aligned_ids,
        speaker_maps_by_candidate=speaker_maps,
    )
    error_direction = {
        "full": {"under": 0, "over": 0},
        "flagged": {"under": 0, "over": 0},
        "unflagged": {"under": 0, "over": 0},
    }
    for candidate_id, row in candidate_scores.items():
        if (
            not row["strictly_scoreable"]
            or row["consensus_state"] != "consensus_value"
            or row["value_state"]["predicted"] != "value"
            or row["atomic_count"]["acceptable_count"]
        ):
            continue
        direction = (
            "under"
            if int(row["atomic_count"]["predicted"])
            < int(row["atomic_count"]["minimum"])
            else "over"
        )
        error_direction["full"][direction] += 1
        group = "flagged" if candidate_id in flagged_ids else "unflagged"
        error_direction[group][direction] += 1
    baseline = true_north._read_json(
        root
        / "multipass"
        / "runs"
        / REFERENCE_RUN_ID
        / "task5-score.private.json"
    )
    checkpoint_path = (
        root / "certification" / true_north.TASK5_CHECKPOINT_FILENAME
    )
    checkpoint = true_north._read_json(checkpoint_path)
    aggregate = full["aggregate"]
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
            int(
                checkpoint["disposition_gate"][
                    "intrinsic_junk_escape_count"
                ]
            )
            == 0
        ),
        "relational_contamination_zero": (
            int(
                checkpoint["relational_merge_certification"][
                    "contamination_count"
                ]
            )
            == 0
        ),
    }
    document = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
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
            "aligned_candidate_count": len(aligned_ids),
            "aligned_claim_text_faithfulness": aligned_aggregate[
                "claim_text_faithfulness_proxy"
            ],
            "aligned_speaker_exactness": aligned_aggregate[
                "speaker_exactness"
            ],
        },
        "subsets": {
            "flagged": {
                "candidate_count": len(flagged_ids),
                "acceptable_atomic_count_rate": flagged_metrics[
                    "acceptable_atomic_count_rate"
                ],
            },
            "unflagged": {
                "candidate_count": len(unflagged_ids),
                "acceptable_atomic_count_rate": unflagged_metrics[
                    "acceptable_atomic_count_rate"
                ],
            },
        },
        "error_direction": error_direction,
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
            "intrinsic_junk_escape_count": checkpoint[
                "disposition_gate"
            ]["intrinsic_junk_escape_count"],
            "relational_contamination_count": checkpoint[
                "relational_merge_certification"
            ]["contamination_count"],
            "checkpoint_sha256": true_north._sha256_file(
                checkpoint_path
            ),
        },
        "acceptance": acceptance,
        "passed": all(acceptance.values()),
        "stop_if_failed": not all(acceptance.values()),
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
    path = run_root / "input-split-default-score.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "score_path": str(path)}


def finalize_partial_input_split_default_experiment(
    *,
    suite_root: str | Path,
    run_id: str,
) -> dict[str, Any]:
    """Freeze an ineligible partial run with explicit Task-5 fallbacks."""

    root = Path(suite_root).expanduser().resolve()
    run_root = root / "multipass" / "runs" / run_id
    configuration = true_north._read_json(run_root / "configuration.json")
    state = true_north._read_json(run_root / "state.json")
    if configuration.get("schema_version") != SCHEMA_VERSION:
        raise InputSplitDefaultError("run is not input split-default")
    if state.get("complete") is True:
        raise InputSplitDefaultError("complete run uses the normal scorer")
    remaining_calls = (
        int(state["budget"]["max_calls"]) - int(state["usage"]["calls"])
    )
    remaining_tokens = (
        int(state["budget"]["max_tokens"]) - int(state["usage"]["tokens"])
    )

    manifest = true_north._read_json(root / "manifest.json")
    base_jobs, dispositions, candidates = _load_search_context(root, manifest)
    (
        _reference_root,
        reference_packets,
        reference_outputs,
        _reference_provenance,
    ) = _reference_artifacts(root)
    valid_flagged: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted(
        (run_root / "outputs" / "input-split-default").glob(
            "*/*/validated.private.json"
        )
    ):
        key = (path.parents[1].name, path.parent.name)
        packet = true_north._read_json(
            run_root
            / "packets"
            / "input-split-default"
            / key[0]
            / f"{key[1]}.private.json"
        )
        output = true_north._read_json(path)
        validate_merge_adjudication(output, packet)
        valid_flagged[key] = output
    expected_flagged_keys = {
        (
            path.parent.name,
            path.stem.removesuffix(".private"),
        )
        for path in (
            run_root / "packets" / "input-split-default"
        ).glob("*/*.private.json")
    }
    missing_keys = sorted(expected_flagged_keys - set(valid_flagged))
    if not missing_keys:
        raise InputSplitDefaultError("partial finalizer found complete coverage")
    if (
        remaining_calls >= len(missing_keys)
        and remaining_tokens
        >= len(missing_keys) * RESERVED_TOKENS_PER_CALL
    ):
        raise InputSplitDefaultError(
            "partial run still has budget for every missing packet"
        )

    flagged_ids = set(
        configuration["selector"]["flagged_candidate_ids"]
    )
    measured_flagged_ids: set[str] = set()
    final_adjudication: dict[tuple[str, str], dict[str, Any]] = {}
    for key, reference in sorted(reference_outputs.items()):
        replacements = {
            str(row["candidate_id"]): {
                field: value
                for field, value in row.items()
                if field != "merge_reason"
            }
            for row in valid_flagged.get(key, {}).get("items", [])
        }
        measured_flagged_ids.update(replacements)
        output = {
            "schema_version": true_north.MULTIPASS_SCHEMA_VERSION,
            "items": [
                copy.deepcopy(
                    replacements.get(str(row["candidate_id"]), row)
                )
                for row in reference["items"]
            ],
        }
        true_north.validate_multipass_adjudication(
            output, reference_packets[key]
        )
        final_adjudication[key] = output

    predictions: list[dict[str, Any]] = []
    for key in sorted(base_jobs):
        output = true_north.compose_multipass_output(
            base_jobs[key],
            dispositions[key],
            final_adjudication.get(key),
            None,
            stage_b_mode="adjudication",
        )
        predictions.extend(output["items"])
        true_north._write_json(
            run_root
            / "outputs"
            / "composed-partial-fallback"
            / key[0]
            / key[1]
            / "validated.private.json",
            output,
            immutable=False,
        )

    consensus_document = true_north._read_json(
        root / "gold" / "development" / "final" / "consensus.private.json"
    )
    preferred_document = true_north._read_json(
        root / "gold" / "development" / "final" / "gold.private.json"
    )
    prediction_map = {
        str(row["candidate_id"]): row for row in predictions
    }
    prediction_ids = set(prediction_map)
    consensus = [
        row for row in consensus_document["items"]
        if str(row["candidate_id"]) in prediction_ids
    ]
    preferred = [
        row for row in preferred_document["items"]
        if str(row["candidate_id"]) in prediction_ids
    ]
    speaker_maps: dict[str, Any] = {}
    for bundle_row in manifest["bundles"]:
        if str(bundle_row["episode_id"]) not in configuration["episode_ids"]:
            continue
        bundle = true_north._read_json(Path(bundle_row["bundle_path"]))
        for candidate in bundle["candidates"]:
            speaker_maps[str(candidate["candidate_id"])] = (
                bundle["episode_context"].get("speaker_map", [])
            )
    score = score_campaign(
        predictions,
        consensus,
        preferred,
        speaker_maps_by_candidate=speaker_maps,
    )
    core = true_north._consensus_atomic_metrics(
        {**consensus_document, "items": consensus},
        prediction_map,
        require_complete_scope=True,
    )
    score["aggregate"].update(
        {str(row["metric"]): row["value"] for row in core}
    )
    candidate_scores = {
        str(row["candidate_id"]): row for row in score["candidates"]
    }
    eligible_ids = {
        candidate_id
        for candidate_id, row in prediction_map.items()
        if str(row["disposition"]) in {"retain", "revise"}
    }
    unflagged_ids = eligible_ids - flagged_ids
    flagged_metrics = _subset_atomic_metrics(
        consensus_document=consensus_document,
        predictions=prediction_map,
        candidate_ids=flagged_ids,
    )
    unflagged_metrics = _subset_atomic_metrics(
        consensus_document=consensus_document,
        predictions=prediction_map,
        candidate_ids=unflagged_ids,
    )
    aligned_ids = {
        candidate_id
        for candidate_id, row in candidate_scores.items()
        if row["strictly_scoreable"]
        and row["consensus_state"] == "consensus_value"
        and row["value_state"]["predicted"] == "value"
        and row["atomic_count"]["acceptable_count"]
    }
    aligned = score_campaign(
        [
            prediction_map[candidate_id]
            for candidate_id in sorted(aligned_ids)
        ],
        consensus_document["items"],
        preferred_document["items"],
        subset_candidate_ids=aligned_ids,
        speaker_maps_by_candidate=speaker_maps,
    )
    error_direction = {
        "full": {"under": 0, "over": 0},
        "flagged": {"under": 0, "over": 0},
        "unflagged": {"under": 0, "over": 0},
    }
    for candidate_id, row in candidate_scores.items():
        if (
            not row["strictly_scoreable"]
            or row["consensus_state"] != "consensus_value"
            or row["value_state"]["predicted"] != "value"
            or row["atomic_count"]["acceptable_count"]
        ):
            continue
        direction = (
            "under"
            if int(row["atomic_count"]["predicted"])
            < int(row["atomic_count"]["minimum"])
            else "over"
        )
        error_direction["full"][direction] += 1
        group = "flagged" if candidate_id in flagged_ids else "unflagged"
        error_direction[group][direction] += 1

    baseline = true_north._read_json(
        root
        / "multipass"
        / "runs"
        / REFERENCE_RUN_ID
        / "task5-score.private.json"
    )
    baseline_private = true_north._read_json(
        root
        / "multipass"
        / "runs"
        / REFERENCE_RUN_ID
        / "score.private.json"
    )
    baseline_scores = {
        str(row["candidate_id"]): row
        for row in baseline_private["private_candidate_scores"]
    }
    transitions = {
        "improved_wrong_to_acceptable": 0,
        "regressed_acceptable_to_wrong": 0,
        "remained_acceptable": 0,
        "remained_wrong": 0,
        "predicted_count_changed": 0,
    }
    for candidate_id in sorted(measured_flagged_ids):
        current = candidate_scores[candidate_id]
        prior = baseline_scores[candidate_id]
        current_ok = bool(current["atomic_count"]["acceptable_count"])
        prior_ok = bool(prior["atomic_count"]["acceptable_count"])
        if not prior_ok and current_ok:
            transitions["improved_wrong_to_acceptable"] += 1
        elif prior_ok and not current_ok:
            transitions["regressed_acceptable_to_wrong"] += 1
        elif current_ok:
            transitions["remained_acceptable"] += 1
        else:
            transitions["remained_wrong"] += 1
        transitions["predicted_count_changed"] += int(
            int(current["atomic_count"]["predicted"])
            != int(prior["atomic_count"]["predicted"])
        )

    checkpoint_path = (
        root / "certification" / true_north.TASK5_CHECKPOINT_FILENAME
    )
    checkpoint = true_north._read_json(checkpoint_path)
    aggregate = score["aggregate"]
    aligned_aggregate = aligned["aggregate"]
    document = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "run_id": run_id,
        "complete": False,
        "acceptance_eligible": False,
        "terminal_reason": (
            "experiment_token_ceiling_with_reproducible_local_validation_failures"
        ),
        "coverage": {
            "validated_flagged_packet_count": len(valid_flagged),
            "expected_flagged_packet_count": len(expected_flagged_keys),
            "measured_flagged_candidate_count": len(measured_flagged_ids),
            "flagged_candidate_count": len(flagged_ids),
            "eligible_candidate_count": len(eligible_ids),
            "search_candidate_count": len(candidates),
            "missing_flagged_packet_count": len(missing_keys),
            "missing_flagged_packet_keys": [
                f"{key[0]}/{key[1]}" for key in missing_keys
            ],
        },
        "partial_fallback_composition": {
            "definition": (
                "split-default output for validated flagged packets, frozen "
                "Task-5 output for two invalid flagged packets and all "
                "unflagged candidates"
            ),
            "acceptable_atomic_count_rate": aggregate[
                "acceptable_atomic_count_rate"
            ],
            "claim_text_faithfulness_full_fold": aggregate[
                "claim_text_faithfulness_proxy"
            ],
            "speaker_exactness_full_fold": aggregate["speaker_exactness"],
            "aligned_candidate_count": len(aligned_ids),
            "aligned_claim_text_faithfulness": aligned_aggregate[
                "claim_text_faithfulness_proxy"
            ],
            "aligned_speaker_exactness": aligned_aggregate[
                "speaker_exactness"
            ],
            "flagged_acceptable_atomic_count_rate": flagged_metrics[
                "acceptable_atomic_count_rate"
            ],
            "unflagged_acceptable_atomic_count_rate": unflagged_metrics[
                "acceptable_atomic_count_rate"
            ],
            "count_error_direction": error_direction,
            "measured_flagged_candidate_count_transition": transitions,
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
            "intrinsic_junk_escape_count": checkpoint[
                "disposition_gate"
            ]["intrinsic_junk_escape_count"],
            "relational_contamination_count": checkpoint[
                "relational_merge_certification"
            ]["contamination_count"],
            "checkpoint_sha256": true_north._sha256_file(
                checkpoint_path
            ),
        },
        "acceptance": {
            "complete_full_search_measurement": False,
            "atomic_count_accuracy_at_least_0_90": (
                float(aggregate["acceptable_atomic_count_rate"]) >= 0.90
            ),
            "aligned_faithfulness_at_least_0_75": (
                float(
                    aligned_aggregate["claim_text_faithfulness_proxy"]
                )
                >= 0.75
            ),
            "intrinsic_junk_escapes_zero": (
                int(
                    checkpoint["disposition_gate"][
                        "intrinsic_junk_escape_count"
                    ]
                )
                == 0
            ),
            "relational_contamination_zero": (
                int(
                    checkpoint["relational_merge_certification"][
                        "contamination_count"
                    ]
                )
                == 0
            ),
        },
        "passed": False,
        "stop_if_failed": True,
        "usage": state["usage"],
        "cumulative_calls_after_run": (
            CUMULATIVE_CALLS_BEFORE_RUN + int(state["usage"]["calls"])
        ),
        "known_cumulative_tokens_after_run": (
            KNOWN_CUMULATIVE_TOKENS_BEFORE_RUN
            + int(state["usage"]["tokens"])
        ),
        "known_token_accounting_excludes_actor_gold_repair": True,
        "holdout_opened": False,
        "production_mutation": False,
    }
    document["result_sha256"] = true_north.sha256_text(
        true_north.dumps_json(document)
    )
    path = run_root / "terminal-partial-result.private.json"
    true_north._write_json(path, document, immutable=False)
    return {**document, "result_path": str(path)}
