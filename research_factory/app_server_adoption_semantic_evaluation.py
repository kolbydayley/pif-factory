from __future__ import annotations

"""Evaluate the immutable adoption-only output with the frozen LLM judges."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_adoption_join as adoption
from . import app_server_capacity_reserve as reserve
from . import app_server_configured_experiment as configured
from . import app_server_judge_v5 as judge
from . import app_server_judge_v5_selection_v276_frozen_support as v276
from . import app_server_judge_v5_selection_v277_frozen_alignment as v277
from . import app_server_runtime_verifier as runtime_verifier
from . import codex_app_server
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_adoption_semantic_evaluation_v1"
SUPPORT_LOCK_VERSION = "pif_adoption_semantic_support_runtime_lock_v1"
ALIGNMENT_LOCK_VERSION = "pif_adoption_semantic_alignment_runtime_lock_v1"
TERMINAL_VERSION = "pif_adoption_semantic_evaluation_terminal_v1"
WINNER_VERSION = "pif_adoption_semantic_development_winner_v1"
OVERRIDE_VERSION = "pif_adoption_semantic_proxy_override_v1"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
).resolve()
ADOPTION_ROOT = (
    PIPELINE_ROOT
    / "development-canary-adoption-only-global-join-prepared-2026-07-17"
).resolve()
PHASE_BOUNDARY_ROOT = (
    PIPELINE_ROOT / "phase-boundary-v275-v277-2026-07-17"
).resolve()
SHARED_REFERENCE = (
    PIPELINE_ROOT
    / "development-selection-v5_4-v220-fresh-integrated-base-design"
    / "shared-reference-seed-v1.json"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-adoption-output-semantic-evaluation-v1"
).resolve()
SUPPORT_TURN_NAME = "adoption_existing_output_pointwise_support"
ALIGNMENT_TURN_NAMES = (
    "adoption_existing_output_alignment_base",
    "adoption_existing_output_alignment_balanced_canary",
)
ALIGNMENT_PERMUTATIONS = ("base", "balanced_canary")
SUPPORT_MODEL = v276.MODEL
SUPPORT_EFFORT = v276.EFFORT
SUPPORT_MAX_TOKENS = v276.MAX_TOTAL_TOKENS
ALIGNMENT_MODEL = v277.MODEL
ALIGNMENT_EFFORT = v277.EFFORT
ALIGNMENT_MAX_TOKENS = v277.MAX_TOTAL_TOKENS_PER_TURN
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_REFERENCE_WITNESSES = 27
EXPECTED_CANDIDATE_WITNESSES = 25
EXPECTED_DENSE_CANDIDATES = 24
EXPECTED_NO_SIGNAL_CANDIDATES = 1
QUALITY_THRESHOLD = 0.97
PRODUCTION_TOKEN_RATIO = 0.259107
TOKEN_RATIO_TARGET = 0.28
USAGE_FIELDS = adoption.USAGE_FIELDS
HASH_CACHE = runtime_verifier.GLOBAL_CONTENT_HASH_CACHE
ADOPTION_CLOSURE_RECEIPT = ADOPTION_ROOT / "runtime-lock-closure.json"
V277_CLOSURE_RECEIPT = (
    PHASE_BOUNDARY_ROOT / "runtime-verifier-closure-receipt.json"
)


class AdoptionSemanticEvaluationError(RuntimeError):
    """The immutable-output semantic evaluation cannot proceed safely."""


class AdoptionSemanticQualityStop(AdoptionSemanticEvaluationError):
    """A frozen semantic quality gate did not pass."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdoptionSemanticEvaluationError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise AdoptionSemanticEvaluationError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise AdoptionSemanticEvaluationError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    return HASH_CACHE.record(path)


def _verify_record(record: Mapping[str, Any]) -> bool:
    return HASH_CACHE.verify_record(record)


def _source_path() -> Path:
    config = _load_json(ADOPTION_ROOT / "experiment-config.json", "adoption config")
    return Path(str(config["source_input"]["path"])).expanduser().resolve()


def _lineage_paths() -> dict[str, Path]:
    turn = ADOPTION_ROOT / "turns" / "adoption-only-episode-global-owner-join"
    return {
        "adoption_config": ADOPTION_ROOT / "experiment-config.json",
        "adoption_runtime_lock": ADOPTION_ROOT / "runtime-lock.json",
        "adoption_authorization": ADOPTION_ROOT / "operator-authorization.json",
        "adoption_capacity": turn / "capacity.json",
        "adoption_sidecar": turn / "sidecar.json",
        "adoption_output": turn / "output.private.json",
        "adoption_schema": turn / "schema.json",
        "adoption_terminal": ADOPTION_ROOT / "terminal.json",
        "adoption_audit": PHASE_BOUNDARY_ROOT
        / "adoption-only-join-terminal-audit-2026-07-17.json",
        "post_adoption_blocker": PHASE_BOUNDARY_ROOT
        / "post-adoption-development-blocker-2026-07-17.json",
        "source_input": _source_path(),
        "shared_reference": SHARED_REFERENCE,
        "frozen_support_runtime_lock": v276.DEFAULT_OUTPUT_ROOT / "runtime-lock.json",
        "frozen_support_instructions": v276.V223_ROOT
        / "support-instructions.private.md",
        "frozen_alignment_runtime_lock": v277.DEFAULT_OUTPUT_ROOT / "runtime-lock.json",
        "frozen_alignment_instructions": v277.v246.v224.DEFAULT_OUTPUT_ROOT
        / "alignment-instructions.private.md",
    }


def _predecessor_manifests() -> list[dict[str, Any]]:
    adoption_receipt = _load_json(ADOPTION_CLOSURE_RECEIPT, "adoption closure receipt")
    v277_receipt = _load_json(V277_CLOSURE_RECEIPT, "v277 closure receipt")
    configured_rows = {
        str((ADOPTION_ROOT / "runtime-lock.json").resolve()): {
            "manifest": adoption_receipt["manifest"],
            "closure_digest": adoption_receipt["closure_digest"],
            "source_receipt": _record(ADOPTION_CLOSURE_RECEIPT),
        },
        str((v277.DEFAULT_OUTPUT_ROOT / "runtime-lock.json").resolve()): {
            "manifest": v277_receipt["manifest"],
            "closure_digest": v277_receipt["closure_digest"],
            "source_receipt": _record(V277_CLOSURE_RECEIPT),
        },
    }
    rows = []
    for path in (
        ADOPTION_ROOT / "runtime-lock.json",
        v276.DEFAULT_OUTPUT_ROOT / "runtime-lock.json",
        v277.DEFAULT_OUTPUT_ROOT / "runtime-lock.json",
    ):
        resolved = path.resolve()
        configured_row = configured_rows.get(str(resolved))
        manifest = _record(resolved)
        closure_digest = runtime_verifier._metadata_closure_digest(
            resolved, cache=HASH_CACHE, memo={}, active=set()
        )
        if configured_row is not None and (
            configured_row["manifest"] != manifest
            or configured_row["closure_digest"] != closure_digest
        ):
            raise AdoptionSemanticEvaluationError(
                "recorded predecessor closure receipt drifted"
            )
        rows.append(
            {
                "manifest": manifest,
                "closure_digest": closure_digest,
                "source_receipt": (
                    configured_row["source_receipt"]
                    if configured_row is not None
                    else None
                ),
            }
        )
    return rows


def _verify_predecessor_manifests(rows: Sequence[Mapping[str, Any]]) -> None:
    expected_paths = {
        str((ADOPTION_ROOT / "runtime-lock.json").resolve()),
        str((v276.DEFAULT_OUTPUT_ROOT / "runtime-lock.json").resolve()),
        str((v277.DEFAULT_OUTPUT_ROOT / "runtime-lock.json").resolve()),
    }
    if {str(row["manifest"]["path"]) for row in rows} != expected_paths:
        raise AdoptionSemanticEvaluationError("predecessor runtime manifest set drifted")
    for row in rows:
        manifest_path = Path(str(row["manifest"]["path"])).resolve()
        if _record(manifest_path) != row["manifest"]:
            raise AdoptionSemanticEvaluationError(
                "immediate predecessor manifest record drifted"
            )
        observed_closure = runtime_verifier._metadata_closure_digest(
            manifest_path, cache=HASH_CACHE, memo={}, active=set()
        )
        if observed_closure != row["closure_digest"]:
            raise AdoptionSemanticEvaluationError(
                "immediate predecessor metadata closure drifted"
            )
        source_receipt = row.get("source_receipt")
        if source_receipt is not None and not _verify_record(source_receipt):
            raise AdoptionSemanticEvaluationError(
                "predecessor closure source receipt drifted"
            )


def _project_candidate() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    paths = _lineage_paths()
    source = _load_json(paths["source_input"], "source input")
    raw = _load_json(paths["adoption_output"], "adoption output")
    schema = _load_json(paths["adoption_schema"], "adoption schema")
    try:
        configured._validate_schema(schema, raw, path="$")
    except Exception as exc:
        raise AdoptionSemanticEvaluationError("adoption output schema drifted") from exc
    source_segments = list(source.get("segments") or [])
    raw_segments = list(raw.get("segments") or [])
    segment_ids = [str(row["segment_id"]) for row in source_segments]
    if raw.get("episode_id") != source.get("episode_id") or [
        str(row["segment_id"]) for row in raw_segments
    ] != segment_ids:
        raise AdoptionSemanticEvaluationError("adoption episode or segment order drifted")

    core_packet = _load_json(ADOPTION_ROOT / "adopted-core-packet.private.json", "core packet")
    core_to_segment = {
        str(event["core_id"]): str(segment["segment_id"])
        for segment in core_packet["segments"]
        for event in segment["events"]
    }
    accounted: list[str] = []
    source_by_id = {str(row["segment_id"]): row for row in source_segments}
    normalized_rows = []
    provenance = []
    diagnostics = []
    seen_identities: set[str] = set()
    duplicate_count = 0
    metric_substring_error_count = 0
    metric_applicability_mismatches = []
    semantic_projection_pairs = []

    for raw_row in raw_segments:
        segment_id = str(raw_row["segment_id"])
        source_row = source_by_id[segment_id]
        events = list(raw_row.get("events") or [])
        if (raw_row.get("status") == "coded") != bool(events):
            raise AdoptionSemanticEvaluationError("adoption status does not match events")
        if len(events) > 32:
            raise AdoptionSemanticEvaluationError("adoption event cap exceeded")
        units = list(source_row["units"])
        unit_index = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
        prior_start = -1
        normalized_events = []
        for event_index, raw_event in enumerate(events):
            event = copy.deepcopy(dict(raw_event))
            core_ids = [str(value) for value in event.pop("core_ids")]
            if any(core_to_segment.get(core_id) != segment_id for core_id in core_ids):
                raise AdoptionSemanticEvaluationError("core event moved across segments")
            accounted.extend(core_ids)
            start_id = str(event.pop("evidence_start_unit_id"))
            end_id = str(event.pop("evidence_end_unit_id"))
            evidence, start_char, end_char, window_id = configured._evidence(
                source_row, start_id, end_id
            )
            if unit_index[start_id] < prior_start:
                raise AdoptionSemanticEvaluationError("adoption event order drifted")
            prior_start = unit_index[start_id]
            metric_fields = (
                "metric_value",
                "metric_unit",
                "metric_comparator",
                "metric_raw_text",
            )
            metric_values = [str(event.get(field) or "") for field in metric_fields]
            missing_literals = [value for value in metric_values if value and value not in evidence]
            if missing_literals:
                metric_substring_error_count += 1
            metric_present = any(metric_values)
            direction_not_applicable = event.get("metric_direction") == "not_applicable"
            if metric_present == direction_not_applicable:
                metric_applicability_mismatches.append(
                    {
                        "segment_id": segment_id,
                        "event_index": event_index,
                        "metric_direction": event.get("metric_direction"),
                        "metric_present": metric_present,
                    }
                )
            semantic_before = _canonical_json(event)
            identity = _canonical_json(
                {field: event.get(field) for field in configured.IDENTITY_FIELDS}
            )
            if identity in seen_identities:
                duplicate_count += 1
            seen_identities.add(identity)
            event["window_id"] = window_id
            event["evidence"] = evidence
            semantic_after = _canonical_json(
                {key: value for key, value in event.items() if key not in {"window_id", "evidence"}}
            )
            if semantic_before != semantic_after:
                raise AdoptionSemanticEvaluationError("semantic field changed during projection")
            normalized_events.append(event)
            semantic_projection_pairs.append(
                {
                    "segment_id": segment_id,
                    "event_index": event_index,
                    "semantic_sha256": sha256_text(semantic_before),
                }
            )
            provenance.append(
                {
                    "segment_id": segment_id,
                    "event_index": event_index,
                    "evidence_start_unit_id": start_id,
                    "evidence_end_unit_id": end_id,
                    "start_char": start_char,
                    "end_char": end_char,
                    "window_id": window_id,
                    "evidence_sha256": sha256_text(evidence),
                }
            )
        normalized_rows.append(
            {
                "segment_id": segment_id,
                "status": raw_row["status"],
                "segment_source_context": raw_row["segment_source_context"],
                "no_signal_reason": raw_row["no_signal_reason"],
                "events": normalized_events,
            }
        )
        diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source_row["density_stratum"],
                "source_unit_count": len(units),
                "reviewed_source_unit_count": len(units),
                "event_count": len(normalized_events),
            }
        )

    accounted.extend(str(row["core_id"]) for row in raw.get("dropped_core_events") or [])
    if set(accounted) != set(core_to_segment) or len(accounted) != len(set(accounted)):
        raise AdoptionSemanticEvaluationError("core accounting is not an exact partition")
    counts = [row["event_count"] for row in diagnostics]
    if counts != [EXPECTED_DENSE_CANDIDATES, EXPECTED_NO_SIGNAL_CANDIDATES]:
        raise AdoptionSemanticEvaluationError("adoption candidate membership drifted")
    if metric_substring_error_count or duplicate_count:
        raise AdoptionSemanticEvaluationError(
            "non-overridden exact grounding or identity invariant failed"
        )
    if len(metric_applicability_mismatches) != 2:
        raise AdoptionSemanticEvaluationError("metric applicability diagnostic drifted")
    return (
        {"episode_id": source["episode_id"], "segments": normalized_rows},
        {
            "schema_version": SCHEMA_VERSION,
            "episode_id": source["episode_id"],
            "events": provenance,
        },
        {
            "schema_version": SCHEMA_VERSION,
            "diagnostics": diagnostics,
            "candidate_event_count": sum(counts),
            "dense_event_count": counts[0],
            "nominal_no_signal_event_count": counts[1],
            "exact_evidence_rate": 1.0,
            "metric_substring_error_count": metric_substring_error_count,
            "metric_applicability_mismatch_count": len(metric_applicability_mismatches),
            "metric_applicability_mismatches": metric_applicability_mismatches,
            "exact_identity_duplicate_count": duplicate_count,
            "semantic_projection_pairs": semantic_projection_pairs,
            "semantic_fields_changed": False,
            "dense_event_count_used_as_semantic_gate": False,
        },
    )


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    predecessors = _predecessor_manifests()
    adoption_config = _load_json(paths["adoption_config"], "adoption config")
    adoption._load_operator_authorization(
        {
            "root": ADOPTION_ROOT,
            "runtime_lock": paths["adoption_runtime_lock"],
            "config": adoption_config,
        }
    )
    terminal = _load_json(paths["adoption_terminal"], "adoption terminal")
    sidecar = _load_json(paths["adoption_sidecar"], "adoption sidecar")
    audit = _load_json(paths["adoption_audit"], "adoption audit")
    support_instructions = paths["frozen_support_instructions"].read_text(encoding="utf-8")
    alignment_instructions = paths["frozen_alignment_instructions"].read_text(encoding="utf-8")
    if (
        terminal.get("terminal_reason")
        != "adoption_join_structural_or_cost_gate_not_passed"
        or terminal.get("error_class") != "AdoptionJoinOutputContractError"
        or terminal.get("semantic_attempt_count") != 1
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("accounting_complete") is not True
        or terminal.get("production_mutated") is not False
        or (terminal.get("new_join_usage") or {}).get("total_tokens") != 36_663
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != "gpt-5.6-sol"
        or sidecar.get("effort") != "low"
        or audit.get("production_amortized_total_token_ratio") != PRODUCTION_TOKEN_RATIO
        or audit.get("dense_event_count") != EXPECTED_DENSE_CANDIDATES
        or audit.get("metric_applicability_mismatch_count") != 2
        or support_instructions != v276.v245.v143.support_base_instructions_v143()
        or alignment_instructions != v277.v246.v130.alignment_instructions_v130()
    ):
        raise AdoptionSemanticEvaluationError("adoption or frozen judge lineage drifted")
    candidate, provenance, diagnostics = _project_candidate()
    return {
        "paths": paths,
        "records": records,
        "predecessors": predecessors,
        "candidate": candidate,
        "provenance": provenance,
        "diagnostics": diagnostics,
    }


def _verify_override(root: Path, lineage: Mapping[str, Any]) -> dict[str, Any]:
    path = root / "operator-override-audit-v1.json"
    value = _load_json(path, "operator override")
    expected_records = {
        name: lineage["records"][name]
        for name in (
            "adoption_terminal",
            "adoption_output",
            "adoption_sidecar",
            "adoption_authorization",
            "adoption_audit",
            "post_adoption_blocker",
        )
    }
    if (
        value.get("schema_version") != OVERRIDE_VERSION
        or value.get("authority") != "direct_user_instruction"
        or value.get("scope")
        != "evaluate_existing_adoption_output_support_then_neutral_alignment"
        or value.get("extraction_replay_authorized") is not False
        or value.get("semantic_repair_authorized") is not False
        or value.get("dense_event_count_proxy_stop_overridden") is not True
        or value.get("metric_applicability_mismatches_are_diagnostics") is not True
        or value.get("frozen_quality_threshold") != QUALITY_THRESHOLD
        or value.get("prompt_model_rubric_changed") is not False
        or value.get("holdout_authorized_before_alignment") is not False
        or value.get("production_mutation_allowed") is not False
        or value.get("records") != expected_records
    ):
        raise AdoptionSemanticEvaluationError("operator override contract drifted")
    return value


def _event_claim(event: Mapping[str, Any]) -> str:
    claim = event.get("claim_text")
    if not isinstance(claim, str) or not claim.strip():
        raise AdoptionSemanticEvaluationError("support witness has no claim text")
    return claim


def build_support_bundle(lineage: Mapping[str, Any]) -> dict[str, Any]:
    source = _load_json(lineage["paths"]["source_input"], "source input")
    reference = _load_json(lineage["paths"]["shared_reference"], "shared reference")
    candidate = lineage["candidate"]
    source_by_id = {str(row["segment_id"]): row for row in source["segments"]}
    candidate_by_id = {str(row["segment_id"]): row for row in candidate["segments"]}
    reference_by_id = {str(row["segment_id"]): row for row in reference["references"]}
    segment_ids = [str(row["segment_id"]) for row in source["segments"]]
    cases = []
    units = []
    origins = []
    for segment_id in segment_ids:
        source_row = source_by_id[segment_id]
        excerpt = "\n".join(str(unit["text"]) for unit in source_row["units"])
        reference_row = reference_by_id[segment_id]
        if sha256_text(excerpt) != reference_row["text_sha256"]:
            raise AdoptionSemanticEvaluationError("support source text hash drifted")
        density = str(source_row["density_stratum"])
        case_id = "case_" + sha256_text(f"adoption-support-v1|{segment_id}")[:24]
        reference_events = list(reference_row["golden_output"].get("discourse_events") or [])
        candidate_events = list(candidate_by_id[segment_id].get("events") or [])
        witnesses = []
        for origin, events in (("reference", reference_events), ("candidate", candidate_events)):
            for event_index, event in enumerate(events):
                event_hash = sha256_text(_canonical_json(event))
                witness_id = "w_" + sha256_text(
                    f"adoption-support-v1|{case_id}|{origin}|{event_index}|{event_hash}"
                )[:24]
                witnesses.append({"witness_id": witness_id, "event": dict(event)})
                units.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "proposition": {"claim_text": _event_claim(event)},
                        "source_excerpt": excerpt,
                    }
                )
                origins.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "segment_id": segment_id,
                        "density_stratum": density,
                        "origin": origin,
                        "event_index": event_index,
                        "event_sha256": event_hash,
                    }
                )
        cases.append(
            {
                "case_id": case_id,
                "segment_id": segment_id,
                "density_stratum": density,
                "source_excerpt": excerpt,
                "witnesses": sorted(witnesses, key=lambda row: str(row["witness_id"])),
                "reference_event_count": len(reference_events),
                "candidate_event_count": len(candidate_events),
            }
        )
    cases.sort(key=lambda row: str(row["case_id"]))
    units.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    origins.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    counts = Counter(str(row["origin"]) for row in origins)
    if counts != Counter(
        {"reference": EXPECTED_REFERENCE_WITNESSES, "candidate": EXPECTED_CANDIDATE_WITNESSES}
    ):
        raise AdoptionSemanticEvaluationError("support witness membership drifted")
    support_value = v276.v245.v143._support_input(units)
    prompt = v276.v245.v175.compact_support_prompt(units)
    schema = v276.v245.v143.support_output_schema(support_value)
    if (
        len(prompt.encode("utf-8")) > v276.MAX_PROMPT_BYTES
        or len(_canonical_json(schema).encode("utf-8")) > v276.MAX_SCHEMA_BYTES
    ):
        raise AdoptionSemanticEvaluationError("support request size cap exceeded")
    return {
        "cases": cases,
        "units": units,
        "origins": origins,
        "support_value": support_value,
        "prompt": prompt,
        "schema": schema,
    }


def _capacity_policy(
    root: Path,
    *,
    phase_id: str,
    turn_names: Sequence[str],
    maximum_total_tokens_per_turn: int,
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(turn_names) * maximum_total_tokens_per_turn
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    _write_stable_time(
        audit_path,
        {
            "schema_version": "pif_app_server_capacity_policy_audit_v20",
            "phase_id": phase_id,
            "created_at": now_iso(),
            "production_mutation_performed": False,
            "measured_basis": {
                "declared_turn_count": len(turn_names),
                "maximum_total_tokens_per_turn": maximum_total_tokens_per_turn,
                "phase_total_token_bound": bound,
                "projected_phase_quota_points": projected,
                "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
            },
        },
        "created_at",
    )
    _write_stable_time(
        policy_path,
        {
            "schema_version": "pif_app_server_capacity_policy_v20",
            "phase_id": phase_id,
            "created_at": now_iso(),
            "managed_chatgpt_auth_only": True,
            "official_persistent_codex_app_server_only": True,
            "retry_count_per_turn": 0,
            "production_mutation_allowed": False,
            "rate_limit_reached_type_must_be_null": True,
            "unknown_usage_hard_stop": True,
            "ordered_turn_names": list(turn_names),
            "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
            "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
            "maximum_total_tokens_per_turn": maximum_total_tokens_per_turn,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "semantic_output_root": str(root),
            "audit": _record(audit_path),
        },
        "created_at",
    )
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized.private.json",
    }


def _usage(
    path: Path, *, model: str, effort: str, maximum_total_tokens: int
) -> dict[str, int]:
    sidecar = _load_json(path, "judge sidecar")
    values = sidecar.get("usage") or {}
    try:
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
    except (KeyError, TypeError, ValueError) as exc:
        raise AdoptionSemanticEvaluationError("judge usage is incomplete") from exc
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != model
        or sidecar.get("effort") != effort
        or sidecar.get("error_class") is not None
        or usage["total_tokens"] > maximum_total_tokens
    ):
        raise AdoptionSemanticEvaluationError("judge accounting or auth contract failed")
    return usage


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(adoption.PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _lock_records(lock: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return runtime_verifier.manifest_records(lock)


def _verify_lock_records(lock: Mapping[str, Any]) -> None:
    if any(not _verify_record(record) for record in _lock_records(lock)):
        raise AdoptionSemanticEvaluationError("runtime lock record drifted")


def freeze_support(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    support_root = root / "support"
    if (support_root / "runtime-lock.json").is_file():
        verify_support_lock(support_root / "runtime-lock.json")
        return _load_support_frozen(root)
    allowed = {"operator-override-audit-v1.json"}
    if {path.name for path in root.iterdir()} - allowed:
        raise AdoptionSemanticEvaluationError("unfinished semantic evaluation root is not replayable")
    lineage = _validate_lineage()
    override = _verify_override(root, lineage)
    bundle = build_support_bundle(lineage)
    support_root.mkdir(parents=True, exist_ok=True)
    paths = _turn_paths(support_root, SUPPORT_TURN_NAME)
    paths["root"].mkdir(parents=True, exist_ok=True)
    capacity = _capacity_policy(
        support_root,
        phase_id="adoption_existing_output_pointwise_support_v1",
        turn_names=[SUPPORT_TURN_NAME],
        maximum_total_tokens_per_turn=SUPPORT_MAX_TOKENS,
    )
    candidate_path = root / "candidate-evaluation-view.private.json"
    provenance_path = root / "candidate-evidence-provenance.private.json"
    diagnostics_path = root / "candidate-projection-diagnostics.json"
    _write_immutable(candidate_path, lineage["candidate"])
    _write_immutable(provenance_path, lineage["provenance"])
    _write_immutable(diagnostics_path, lineage["diagnostics"])
    pool_path = support_root / "support-pool.private.json"
    origin_path = support_root / "origin-map.private.json"
    instructions_path = support_root / "instructions.private.md"
    _write_immutable(pool_path, {"schema_version": SCHEMA_VERSION, "cases": bundle["cases"]})
    _write_immutable(origin_path, {"schema_version": SCHEMA_VERSION, "rows": bundle["origins"]})
    _write_private_text(instructions_path, v276.v245.v143.support_base_instructions_v143())
    _write_immutable(paths["input"], bundle["support_value"])
    _write_private_text(paths["prompt"], bundle["prompt"])
    _write_immutable(paths["schema"], bundle["schema"])
    spec_path = support_root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "state": "frozen_before_one_turn_pointwise_support",
            "declared_turn_count": 1,
            "turn_name": SUPPORT_TURN_NAME,
            "model": SUPPORT_MODEL,
            "effort": SUPPORT_EFFORT,
            "retry_count": 0,
            "witness_count": EXPECTED_REFERENCE_WITNESSES + EXPECTED_CANDIDATE_WITNESSES,
            "candidate_membership_count": EXPECTED_CANDIDATE_WITNESSES,
            "prompt_model_rubric_changed": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    lock = {
        "schema_version": SUPPORT_LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": "adoption_existing_output_pointwise_support_v1",
        "model": SUPPORT_MODEL,
        "effort": SUPPORT_EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "frozen_quality_threshold": QUALITY_THRESHOLD,
        "dense_event_proxy_stop_overridden": True,
        "metric_applicability_mismatches_are_diagnostics": True,
        "prompt_model_rubric_changed": False,
        "runtime_adapter": _record(Path(__file__).resolve()),
        "pinned_codex_cli": _record(adoption.PINNED_CODEX),
        "predecessor_manifests": lineage["predecessors"],
        "direct_lineage": list(lineage["records"].values()),
        "override": _record(root / "operator-override-audit-v1.json"),
        "candidate": _record(candidate_path),
        "provenance": _record(provenance_path),
        "projection_diagnostics": _record(diagnostics_path),
        "pool": _record(pool_path),
        "origin_map": _record(origin_path),
        "instructions": _record(instructions_path),
        "input": _record(paths["input"]),
        "prompt": _record(paths["prompt"]),
        "schema": _record(paths["schema"]),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity["audit"]),
        "capacity_policy": _record(capacity["policy"]),
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(support_root / "runtime-lock.json", lock, "frozen_at")
    verify_support_lock(support_root / "runtime-lock.json")
    return _load_support_frozen(root)


def verify_support_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "support runtime lock")
    root = path.parent.parent.resolve()
    if (
        lock.get("schema_version") != SUPPORT_LOCK_VERSION
        or lock.get("model") != SUPPORT_MODEL
        or lock.get("effort") != SUPPORT_EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or lock.get("frozen_quality_threshold") != QUALITY_THRESHOLD
        or lock.get("dense_event_proxy_stop_overridden") is not True
        or lock.get("metric_applicability_mismatches_are_diagnostics") is not True
        or lock.get("prompt_model_rubric_changed") is not False
        or lock.get("runtime_adapter") != _record(Path(__file__).resolve())
        or lock.get("pinned_codex_cli") != _record(adoption.PINNED_CODEX)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise AdoptionSemanticEvaluationError("support runtime lock contract drifted")
    _verify_predecessor_manifests(lock.get("predecessor_manifests") or [])
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise AdoptionSemanticEvaluationError("support direct lineage set drifted")
    _verify_override(root, lineage)
    _verify_lock_records(lock)
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_support_frozen(root: Path) -> dict[str, Any]:
    support_root = root / "support"
    paths = _turn_paths(support_root, SUPPORT_TURN_NAME)
    return {
        "root": root,
        "support_root": support_root,
        "runtime_lock": support_root / "runtime-lock.json",
        "capacity_policy": support_root / "capacity-policy.json",
        "spec": _load_json(support_root / "attempt-spec.json", "support spec"),
        "support_value": _load_json(paths["input"], "support input"),
        "origin_rows": _load_json(support_root / "origin-map.private.json", "origin map")["rows"],
        "prompt": paths["prompt"].read_text(encoding="utf-8"),
        "schema": _load_json(paths["schema"], "support schema"),
        "paths": paths,
    }


def _phase_failure_terminal(
    *,
    root: Path,
    phase: str,
    exc: BaseException,
    turn_names: Sequence[str],
    model: str,
    effort: str,
    maximum_total_tokens: int,
    runtime_lock: Path,
) -> dict[str, Any]:
    phase_root = root / phase
    attempted = unknown = 0
    usage = {field: 0 for field in USAGE_FIELDS}
    sidecars = []
    for turn_name in turn_names:
        paths = _turn_paths(phase_root, turn_name)
        if not paths["capacity"].exists() and not paths["sidecar"].exists():
            continue
        attempted += 1
        if paths["sidecar"].is_file():
            sidecars.append(_record(paths["sidecar"]))
            try:
                row_usage = _usage(
                    paths["sidecar"],
                    model=model,
                    effort=effort,
                    maximum_total_tokens=maximum_total_tokens,
                )
                for field in USAGE_FIELDS:
                    usage[field] += row_usage[field]
            except Exception:
                unknown += 1
        else:
            unknown += 1
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "semantic_quality_gate_not_passed"
            if isinstance(exc, AdoptionSemanticQualityStop)
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "attempted_turn_count": attempted,
        "unknown_usage_turn_count": unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": usage,
        "sidecars": sidecars,
        "development_quality_passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(runtime_lock),
    }
    _write_stable_time(phase_root / "terminal.json", terminal, "terminal_at")
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_support(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "evaluation terminal")
    frozen = freeze_support(output_dir=root)
    support_root = frozen["support_root"]
    if (support_root / "terminal.json").is_file():
        return _load_json(support_root / "terminal.json", "support terminal")
    verify_support_lock(frozen["runtime_lock"])
    if (support_root / "launch-receipt.json").exists():
        return _phase_failure_terminal(
            root=root,
            phase="support",
            exc=AdoptionSemanticEvaluationError("support launch exists; replay prohibited"),
            turn_names=[SUPPORT_TURN_NAME],
            model=SUPPORT_MODEL,
            effort=SUPPORT_EFFORT,
            maximum_total_tokens=SUPPORT_MAX_TOKENS,
            runtime_lock=frozen["runtime_lock"],
        )
    _write_stable_time(
        support_root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "declared_turn_count": 1,
            "retry_count": 0,
            "model": SUPPORT_MODEL,
            "effort": SUPPORT_EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "extraction_replayed": False,
            "managed_chatgpt_auth_only": True,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "launched_at",
    )
    started = time.monotonic()
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=SUPPORT_MODEL,
                effort=SUPPORT_EFFORT,
                base_instructions=v276.v245.v143.support_base_instructions_v143(),
                prompt=frozen["prompt"],
                output_schema=frozen["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=frozen["paths"]["sidecar"],
                output_path=frozen["paths"]["output"],
                batch_size=EXPECTED_REFERENCE_WITNESSES + EXPECTED_CANDIDATE_WITNESSES,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=frozen["paths"]["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise AdoptionSemanticEvaluationError("support turn did not complete")
        usage = _usage(
            frozen["paths"]["sidecar"],
            model=SUPPORT_MODEL,
            effort=SUPPORT_EFFORT,
            maximum_total_tokens=SUPPORT_MAX_TOKENS,
        )
        audit, private_score = v276.v245.v223.score_support_output(
            output=result.output,
            support_value=frozen["support_value"],
            origin_rows=frozen["origin_rows"],
        )
        receipts = v276.v245.v223._support_receipts(result.output)
        audit_path = support_root / "support-audit.json"
        score_path = support_root / "support-score.private.json"
        receipts_path = support_root / "support-receipts.private.json"
        _write_immutable(audit_path, audit)
        _write_immutable(score_path, private_score)
        _write_immutable(receipts_path, receipts)
        authorized = audit.get("alignment_audit_authorized") is True
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "completed" if authorized else "inactive_incomplete_recovery_required",
            "terminal_reason": (
                "adoption_existing_output_support_completed_alignment_authorized"
                if authorized
                else "adoption_existing_output_support_quality_gate_not_passed"
            ),
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "support_status_counts": audit["support_status_counts"],
            "no_signal_candidate_event_support_status": audit[
                "no_signal_candidate_event_support_status"
            ],
            "alignment_audit_authorized": authorized,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "wall_seconds": round(time.monotonic() - started, 6),
            "support_audit": _record(audit_path),
            "support_score": _record(score_path),
            "support_receipts": _record(receipts_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "sidecar": _record(frozen["paths"]["sidecar"]),
        }
        _write_stable_time(support_root / "terminal.json", terminal, "terminal_at")
        if not authorized:
            _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (support_root / "terminal.json").is_file():
            return _load_json(support_root / "terminal.json", "support terminal")
        return _phase_failure_terminal(
            root=root,
            phase="support",
            exc=exc,
            turn_names=[SUPPORT_TURN_NAME],
            model=SUPPORT_MODEL,
            effort=SUPPORT_EFFORT,
            maximum_total_tokens=SUPPORT_MAX_TOKENS,
            runtime_lock=frozen["runtime_lock"],
        )


def _opaque_id(kind: str, value: str) -> str:
    prefix = "jcase_" if kind == "case" else "wit_"
    return prefix + sha256_text(f"adoption-semantic-v1|{kind}|{value}")[:24]


def build_alignment_bundle(root: Path) -> dict[str, Any]:
    support_root = root / "support"
    raw_pool = _load_json(support_root / "support-pool.private.json", "support pool")
    origin_rows = _load_json(support_root / "origin-map.private.json", "support origin map")[
        "rows"
    ]
    score_rows = _load_json(support_root / "support-score.private.json", "support score")[
        "rows"
    ]
    receipts = _load_json(support_root / "support-receipts.private.json", "support receipts")
    origin_by_id = {str(row["witness_id"]): row for row in origin_rows}
    status_by_id = {str(row["witness_id"]): str(row["support_status"]) for row in score_rows}
    receipt_by_id = {str(row["witness_id"]): row for row in receipts["units"]}
    if set(origin_by_id) != set(status_by_id) or set(origin_by_id) != set(receipt_by_id):
        raise AdoptionSemanticEvaluationError("support receipt coverage drifted")
    dense_case = next(case for case in raw_pool["cases"] if case["density_stratum"] == "dense")
    no_signal_case = next(
        case for case in raw_pool["cases"] if case["density_stratum"] == "no_signal"
    )
    dense_case_id = _opaque_id("case", str(dense_case["case_id"]))
    id_map = {
        str(witness["witness_id"]): _opaque_id("witness", str(witness["witness_id"]))
        for witness in dense_case["witnesses"]
        if status_by_id[str(witness["witness_id"])] == "supported"
    }
    rendered: dict[str, list[dict[str, Any]]] = {"reference": [], "candidate": []}
    mapping_rows = []
    for witness in dense_case["witnesses"]:
        legacy_id = str(witness["witness_id"])
        metadata = origin_by_id[legacy_id]
        status = status_by_id[legacy_id]
        new_id = id_map.get(legacy_id)
        mapping_rows.append(
            {
                "case_id": dense_case_id if new_id else None,
                "legacy_case_id": dense_case["case_id"],
                "witness_id": new_id,
                "legacy_witness_id": legacy_id,
                "origin": metadata["origin"],
                "support_status": status,
                "segment_id": metadata["segment_id"],
                "density_stratum": metadata["density_stratum"],
                "event_sha256": metadata["event_sha256"],
            }
        )
        if new_id:
            rendered[str(metadata["origin"])].append(
                {"witness_id": new_id, "event": witness["event"]}
            )
    no_signal_rows = [
        {
            **dict(origin_by_id[str(witness["witness_id"])]),
            "support_status": status_by_id[str(witness["witness_id"])],
        }
        for witness in no_signal_case["witnesses"]
    ]
    dense_reference_total = sum(row["origin"] == "reference" for row in mapping_rows)
    dense_candidate_total = sum(row["origin"] == "candidate" for row in mapping_rows)
    if (
        dense_reference_total != EXPECTED_REFERENCE_WITNESSES
        or dense_candidate_total != EXPECTED_DENSE_CANDIDATES
        or len(no_signal_rows) != EXPECTED_NO_SIGNAL_CANDIDATES
        or any(row["origin"] != "candidate" for row in no_signal_rows)
        or not rendered["reference"]
        or not rendered["candidate"]
    ):
        raise AdoptionSemanticEvaluationError("alignment membership drifted")
    pool = {
        "schema_version": v277.app_server_llm_judge.SHARED_WITNESS_POOL_VERSION,
        "seed_sha256": sha256_text("adoption-existing-output-supported-dense-pool-v1"),
        "cases": [
            {
                "case_id": dense_case_id,
                "source_excerpt": dense_case["source_excerpt"],
                "event_set_a": sorted(rendered["reference"], key=lambda row: row["witness_id"]),
                "event_set_b": sorted(rendered["candidate"], key=lambda row: row["witness_id"]),
            }
        ],
        "privacy": "private_analysis_only_blinded_no_origin_provenance",
    }
    errors = judge.validate_shared_witness_pool(pool)
    if errors:
        raise AdoptionSemanticEvaluationError(
            "normalized alignment pool is invalid: " + "; ".join(errors)
        )
    filtered_receipts = {
        **receipts,
        "units": [
            {
                **receipt_by_id[legacy_id],
                "case_id": dense_case_id,
                "witness_id": new_id,
            }
            for legacy_id, new_id in sorted(id_map.items(), key=lambda row: row[1])
        ],
    }
    base = judge.build_neutral_alignment_input(pool, filtered_receipts, permutation="base")
    canary = judge.build_neutral_alignment_input(
        pool, filtered_receipts, permutation="balanced_canary"
    )
    base_ids = [row["witness_id"] for row in base["cases"][0]["witnesses"]]
    canary_ids = [row["witness_id"] for row in canary["cases"][0]["witnesses"]]
    if canary_ids != list(reversed(base_ids)):
        raise AdoptionSemanticEvaluationError("balanced alignment permutation drifted")
    turns = []
    for turn_name, permutation, value in zip(
        ALIGNMENT_TURN_NAMES, ALIGNMENT_PERMUTATIONS, (base, canary)
    ):
        prompt = v277.v246.v130.alignment_prompt_v130(value)
        schema = judge.neutral_alignment_output_schema(value)
        if (
            len(prompt.encode("utf-8")) > v277.MAX_PROMPT_BYTES
            or len(_canonical_json(schema).encode("utf-8")) > v277.MAX_SCHEMA_BYTES
        ):
            raise AdoptionSemanticEvaluationError("alignment request size cap exceeded")
        turns.append(
            {
                "turn_name": turn_name,
                "permutation": permutation,
                "value": value,
                "prompt": prompt,
                "schema": schema,
            }
        )
    return {
        "pool": pool,
        "receipts": filtered_receipts,
        "mapping": {
            "schema_version": SCHEMA_VERSION,
            "rows": sorted(mapping_rows, key=lambda row: str(row["legacy_witness_id"])),
            "no_signal_rows": no_signal_rows,
        },
        "turns": turns,
    }


def _alignment_request_records(root: Path) -> list[dict[str, Any]]:
    records = []
    for turn_name in ALIGNMENT_TURN_NAMES:
        paths = _turn_paths(root / "alignment", turn_name)
        records.extend(_record(paths[name]) for name in ("input", "prompt", "schema"))
    return records


def freeze_alignment(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    alignment_root = root / "alignment"
    if (alignment_root / "runtime-lock.json").is_file():
        verify_alignment_lock(alignment_root / "runtime-lock.json")
        return _load_alignment_frozen(root)
    if (root / "terminal.json").is_file():
        raise AdoptionSemanticEvaluationError("evaluation already has a terminal")
    verify_support_lock(root / "support" / "runtime-lock.json")
    support_terminal = _load_json(root / "support" / "terminal.json", "support terminal")
    if (
        support_terminal.get("terminal_reason")
        != "adoption_existing_output_support_completed_alignment_authorized"
        or support_terminal.get("alignment_audit_authorized") is not True
        or support_terminal.get("accounting_complete") is not True
    ):
        raise AdoptionSemanticEvaluationError("support did not authorize alignment")
    lineage = _validate_lineage()
    _verify_override(root, lineage)
    bundle = build_alignment_bundle(root)
    alignment_root.mkdir(parents=True, exist_ok=True)
    capacity = _capacity_policy(
        alignment_root,
        phase_id="adoption_existing_output_neutral_alignment_v1",
        turn_names=ALIGNMENT_TURN_NAMES,
        maximum_total_tokens_per_turn=ALIGNMENT_MAX_TOKENS,
    )
    pool_path = alignment_root / "shared-witness-pool.private.json"
    receipts_path = alignment_root / "support-receipts.private.json"
    mapping_path = alignment_root / "origin-map.private.json"
    instructions_path = alignment_root / "instructions.private.md"
    _write_immutable(pool_path, bundle["pool"])
    _write_immutable(receipts_path, bundle["receipts"])
    _write_immutable(mapping_path, bundle["mapping"])
    _write_private_text(instructions_path, v277.v246.v130.alignment_instructions_v130())
    for turn in bundle["turns"]:
        paths = _turn_paths(alignment_root, turn["turn_name"])
        paths["root"].mkdir(parents=True, exist_ok=True)
        _write_immutable(paths["input"], turn["value"])
        _write_private_text(paths["prompt"], turn["prompt"])
        _write_immutable(paths["schema"], turn["schema"])
    supported_count = len(bundle["receipts"]["units"])
    spec_path = alignment_root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "state": "frozen_before_two_turn_neutral_alignment",
            "declared_turn_count": 2,
            "turn_names": list(ALIGNMENT_TURN_NAMES),
            "permutations": list(ALIGNMENT_PERMUTATIONS),
            "model": ALIGNMENT_MODEL,
            "effort": ALIGNMENT_EFFORT,
            "retry_count": 0,
            "supported_alignment_witness_count": supported_count,
            "frozen_quality_threshold": QUALITY_THRESHOLD,
            "prompt_model_rubric_changed": False,
            "holdout_authorized_before_score": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    support_records = {
        name: _record(root / "support" / name)
        for name in (
            "runtime-lock.json",
            "terminal.json",
            "support-pool.private.json",
            "origin-map.private.json",
            "support-score.private.json",
            "support-receipts.private.json",
            "support-audit.json",
        )
    }
    lock = {
        "schema_version": ALIGNMENT_LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": "adoption_existing_output_neutral_alignment_v1",
        "model": ALIGNMENT_MODEL,
        "effort": ALIGNMENT_EFFORT,
        "declared_turn_count": 2,
        "retry_count": 0,
        "frozen_quality_threshold": QUALITY_THRESHOLD,
        "production_amortized_total_token_ratio": PRODUCTION_TOKEN_RATIO,
        "prompt_model_rubric_changed": False,
        "runtime_adapter": _record(Path(__file__).resolve()),
        "pinned_codex_cli": _record(adoption.PINNED_CODEX),
        "predecessor_manifests": lineage["predecessors"],
        "direct_lineage": list(lineage["records"].values()),
        "override": _record(root / "operator-override-audit-v1.json"),
        "candidate": _record(root / "candidate-evaluation-view.private.json"),
        "projection_diagnostics": _record(root / "candidate-projection-diagnostics.json"),
        "support_records": support_records,
        "pool": _record(pool_path),
        "receipts": _record(receipts_path),
        "mapping": _record(mapping_path),
        "instructions": _record(instructions_path),
        "frozen_requests": _alignment_request_records(root),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity["audit"]),
        "capacity_policy": _record(capacity["policy"]),
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(alignment_root / "runtime-lock.json", lock, "frozen_at")
    verify_alignment_lock(alignment_root / "runtime-lock.json")
    return _load_alignment_frozen(root)


def verify_alignment_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "alignment runtime lock")
    root = path.parent.parent.resolve()
    if (
        lock.get("schema_version") != ALIGNMENT_LOCK_VERSION
        or lock.get("model") != ALIGNMENT_MODEL
        or lock.get("effort") != ALIGNMENT_EFFORT
        or lock.get("declared_turn_count") != 2
        or lock.get("retry_count") != 0
        or lock.get("frozen_quality_threshold") != QUALITY_THRESHOLD
        or lock.get("production_amortized_total_token_ratio") != PRODUCTION_TOKEN_RATIO
        or lock.get("prompt_model_rubric_changed") is not False
        or lock.get("runtime_adapter") != _record(Path(__file__).resolve())
        or lock.get("pinned_codex_cli") != _record(adoption.PINNED_CODEX)
        or lock.get("holdout_authorized_before_score") is not False
        or lock.get("production_mutation_allowed") is not False
        or {row["path"] for row in lock.get("frozen_requests") or []}
        != {row["path"] for row in _alignment_request_records(root)}
    ):
        raise AdoptionSemanticEvaluationError("alignment runtime lock contract drifted")
    verify_support_lock(root / "support" / "runtime-lock.json")
    _verify_predecessor_manifests(lock.get("predecessor_manifests") or [])
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise AdoptionSemanticEvaluationError("alignment direct lineage set drifted")
    _verify_override(root, lineage)
    _verify_lock_records(lock)
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_alignment_frozen(root: Path) -> dict[str, Any]:
    alignment_root = root / "alignment"
    turns = []
    for turn_name, permutation in zip(ALIGNMENT_TURN_NAMES, ALIGNMENT_PERMUTATIONS):
        paths = _turn_paths(alignment_root, turn_name)
        turns.append(
            {
                "turn_name": turn_name,
                "permutation": permutation,
                "value": _load_json(paths["input"], f"{turn_name} input"),
                "prompt": paths["prompt"].read_text(encoding="utf-8"),
                "schema": _load_json(paths["schema"], f"{turn_name} schema"),
                "paths": paths,
            }
        )
    return {
        "root": root,
        "alignment_root": alignment_root,
        "runtime_lock": alignment_root / "runtime-lock.json",
        "capacity_policy": alignment_root / "capacity-policy.json",
        "mapping": _load_json(alignment_root / "origin-map.private.json", "alignment map"),
        "instructions": (alignment_root / "instructions.private.md").read_text(
            encoding="utf-8"
        ),
        "turns": turns,
    }


def _f1(precision: float, recall: float) -> float:
    return 1.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)


def score_alignment(
    *, base: Mapping[str, Any], canary: Mapping[str, Any], mapping: Mapping[str, Any]
) -> dict[str, Any]:
    base_rows = {str(row["case_id"]): row for row in base["cases"]}
    canary_rows = {str(row["case_id"]): row for row in canary["cases"]}
    if set(base_rows) != set(canary_rows) or len(base_rows) != 1:
        raise AdoptionSemanticEvaluationError("alignment case coverage drifted")
    disagreement_case_ids = sorted(
        case_id
        for case_id in base_rows
        if v277.v246.v130._owner_projection(base_rows[case_id])
        != v277.v246.v130._owner_projection(canary_rows[case_id])
    )
    consistency_issues = v277.v246.v224._alignment_consistency_issues(base)
    abstention_case_ids = sorted(
        case_id
        for case_id, row in base_rows.items()
        if v277.v246.v224._case_has_abstention(row)
    )
    mapping_rows = mapping["rows"]
    supported_mapping = [row for row in mapping_rows if row["witness_id"]]
    origin_by_id = {str(row["witness_id"]): str(row["origin"]) for row in supported_mapping}
    row = next(iter(base_rows.values()))
    groups = row["equivalence_groups"]
    reference_groups = {
        index
        for index, group in enumerate(groups)
        if any(origin_by_id[witness_id] == "reference" for witness_id in group)
    }
    strict_represented = {
        index
        for index, group in enumerate(groups)
        if {origin_by_id[witness_id] for witness_id in group} == {"reference", "candidate"}
    }
    reference_count = len(reference_groups)
    represented_count = len(reference_groups & strict_represented)
    dense_candidate_total = sum(row["origin"] == "candidate" for row in mapping_rows)
    dense_candidate_supported = sum(
        row["origin"] == "candidate" and row["support_status"] == "supported"
        for row in mapping_rows
    )
    dense_precision = dense_candidate_supported / dense_candidate_total
    dense_recall = represented_count / reference_count if reference_count else 1.0
    dense_f1 = _f1(dense_precision, dense_recall)
    no_signal_rows = mapping["no_signal_rows"]
    no_signal_unsupported = sum(row["support_status"] != "supported" for row in no_signal_rows)
    no_signal_f1 = 1.0 if no_signal_unsupported == 0 else 0.0
    macro_f1 = (dense_f1 + no_signal_f1) / 2
    mismatch_counts = Counter(
        field
        for pair in row["alignment_pairs"]
        for field in pair.get("mismatch_fields") or []
    )
    checks = {
        "permutation_projection_exact": not disagreement_case_ids,
        "primary_abstention_count_0": not abstention_case_ids,
        "alignment_partition_relation_consistent": not consistency_issues,
        "supported_reference_witness_count_27": reference_count
        == EXPECTED_REFERENCE_WITNESSES,
        "total_candidate_witness_count_25": dense_candidate_total + len(no_signal_rows)
        == EXPECTED_CANDIDATE_WITNESSES,
        "strict_full_field_macro_f1_gte_0_97": macro_f1 >= QUALITY_THRESHOLD,
        "no_material_source_macro_regression": macro_f1 >= QUALITY_THRESHOLD,
        "no_signal_source_unsupported_count_0": no_signal_unsupported == 0,
        "exact_evidence_rate_1": True,
        "metric_applicability_mismatches_recorded_2": True,
        "production_amortized_total_token_ratio_lte_0_28": PRODUCTION_TOKEN_RATIO
        <= TOKEN_RATIO_TARGET,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "metrics": {
            "alignment_case_count": len(base_rows),
            "permutation_exact_case_count": len(base_rows) - len(disagreement_case_ids),
            "abstention_case_count": len(abstention_case_ids),
            "alignment_consistency_issue_count": len(consistency_issues),
            "reference_semantic_unit_count": reference_count,
            "strictly_equivalent_reference_semantic_unit_count": represented_count,
            "dense_candidate_event_count": dense_candidate_total,
            "dense_source_supported_candidate_event_count": dense_candidate_supported,
            "dense_source_supported_candidate_precision": round(dense_precision, 6),
            "dense_strict_full_field_recall": round(dense_recall, 6),
            "dense_strict_full_field_f1": round(dense_f1, 6),
            "no_signal_source_supported_f1": round(no_signal_f1, 6),
            "development_strict_full_field_macro_f1": round(macro_f1, 6),
            "baseline_reference_macro_f1": 1.0,
            "delta_vs_reference": round(macro_f1 - 1.0, 6),
            "production_amortized_total_token_ratio": PRODUCTION_TOKEN_RATIO,
            "metric_applicability_mismatch_count": 2,
        },
        "mismatch_field_counts": dict(sorted(mismatch_counts.items())),
        "permutation_disagreement_case_ids": disagreement_case_ids,
        "abstention_case_ids": abstention_case_ids,
        "alignment_consistency_issues": consistency_issues,
        "development_winner_frozen": not failed,
        "holdout_authorized": not failed,
        "overall_evaluation_complete": False,
        "production_mutated": False,
        "privacy": "sanitized counts metrics and opaque ids no source or event text",
    }


def _winner(root: Path, score_path: Path) -> dict[str, Any]:
    lineage = _lineage_paths()
    return {
        "schema_version": WINNER_VERSION,
        "frozen_at": now_iso(),
        "system_id": "adoption_existing_output_semantically_accepted_v1",
        "configuration": {
            "extraction_replayed": False,
            "raw_output": _record(lineage["adoption_output"]),
            "raw_sidecar": _record(lineage["adoption_sidecar"]),
            "raw_terminal": _record(lineage["adoption_terminal"]),
            "raw_runtime_lock": _record(lineage["adoption_runtime_lock"]),
            "candidate_evaluation_view": _record(
                root / "candidate-evaluation-view.private.json"
            ),
            "evidence_provenance": _record(root / "candidate-evidence-provenance.private.json"),
            "projection_diagnostics": _record(root / "candidate-projection-diagnostics.json"),
            "operator_override": _record(root / "operator-override-audit-v1.json"),
        },
        "development_evidence": {
            "support_terminal": _record(root / "support" / "terminal.json"),
            "alignment_score": _record(score_path),
        },
        "development_quality_passed": True,
        "frozen_quality_threshold": QUALITY_THRESHOLD,
        "production_amortized_total_token_ratio": PRODUCTION_TOKEN_RATIO,
        "production_amortized_token_target_passed": True,
        "untouched_holdout_authorized": True,
        "overall_evaluation_complete": False,
        "production_mutated": False,
        "privacy": "hashes counts metrics and configuration no source or event text",
    }


async def run_alignment(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "evaluation terminal")
    frozen = freeze_alignment(output_dir=root)
    alignment_root = frozen["alignment_root"]
    if (alignment_root / "terminal.json").is_file():
        return _load_json(alignment_root / "terminal.json", "alignment terminal")
    verify_alignment_lock(frozen["runtime_lock"])
    if (alignment_root / "launch-receipt.json").exists():
        return _phase_failure_terminal(
            root=root,
            phase="alignment",
            exc=AdoptionSemanticEvaluationError("alignment launch exists; replay prohibited"),
            turn_names=ALIGNMENT_TURN_NAMES,
            model=ALIGNMENT_MODEL,
            effort=ALIGNMENT_EFFORT,
            maximum_total_tokens=ALIGNMENT_MAX_TOKENS,
            runtime_lock=frozen["runtime_lock"],
        )
    _write_stable_time(
        alignment_root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "declared_turn_count": 2,
            "retry_count": 0,
            "model": ALIGNMENT_MODEL,
            "effort": ALIGNMENT_EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "prompt_model_rubric_changed": False,
            "managed_chatgpt_auth_only": True,
            "holdout_authorized_before_score": False,
            "production_mutation_allowed": False,
        },
        "launched_at",
    )
    started = time.monotonic()
    try:
        normalized = []
        usages = []
        async with client_factory(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                paths = turn["paths"]
                result = await client.run_ephemeral_structured_turn(
                    model=ALIGNMENT_MODEL,
                    effort=ALIGNMENT_EFFORT,
                    base_instructions=frozen["instructions"],
                    prompt=turn["prompt"],
                    output_schema=turn["schema"],
                    cwd=PROJECT_ROOT,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=len(turn["value"]["cases"][0]["witnesses"]),
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=paths["capacity"],
                )
                if result.status_ok is not True or not isinstance(result.output, Mapping):
                    raise AdoptionSemanticEvaluationError(
                        f"alignment turn did not complete: {turn['turn_name']}"
                    )
                usages.append(
                    _usage(
                        paths["sidecar"],
                        model=ALIGNMENT_MODEL,
                        effort=ALIGNMENT_EFFORT,
                        maximum_total_tokens=ALIGNMENT_MAX_TOKENS,
                    )
                )
                errors = judge.validate_neutral_alignment_output(result.output, turn["value"])
                if errors:
                    raise AdoptionSemanticEvaluationError(
                        "post-return neutral alignment validation failed: "
                        + "; ".join(sorted(set(errors)))
                    )
                projected = judge.normalize_neutral_alignment_output(
                    result.output, turn["value"]
                )
                _write_immutable(paths["normalized"], projected)
                normalized.append(projected)
        score = score_alignment(
            base=normalized[0], canary=normalized[1], mapping=frozen["mapping"]
        )
        score_path = alignment_root / "alignment-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        winner_path = root / "development-winner.json"
        winner_record = None
        if passed:
            _write_stable_time(winner_path, _winner(root, score_path), "frozen_at")
            winner_record = _record(winner_path)
        combined_usage = {
            field: sum(item[field] for item in usages) for field in USAGE_FIELDS
        }
        support_terminal = _load_json(root / "support" / "terminal.json", "support terminal")
        support_usage = support_terminal["usage"]
        total_judge_usage = {
            field: int(support_usage[field]) + combined_usage[field] for field in USAGE_FIELDS
        }
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "completed" if passed else "inactive_incomplete_recovery_required",
            "terminal_reason": (
                "adoption_semantic_quality_passed_development_winner_frozen_holdout_authorized"
                if passed
                else "adoption_semantic_quality_gate_not_passed"
            ),
            "attempted_turn_count": 3,
            "measured_turn_count": 3,
            "unknown_usage_turn_count": 0,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "support_usage": support_usage,
            "alignment_usage": combined_usage,
            "total_judge_usage": total_judge_usage,
            "development_quality_passed": passed,
            "development_winner_frozen": passed,
            "holdout_authorized": passed,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "production_amortized_total_token_ratio": PRODUCTION_TOKEN_RATIO,
            "wall_seconds": round(time.monotonic() - started, 6),
            "score": _record(score_path),
            "development_winner": winner_record,
            "support_terminal": _record(root / "support" / "terminal.json"),
            "sidecars": [
                _record(turn["paths"]["sidecar"]) for turn in frozen["turns"]
            ],
            "runtime_lock": _record(frozen["runtime_lock"]),
            "exact_next_action": (
                "freeze and execute the already-authorized untouched holdout gates"
                if passed
                else "freeze a semantic-quality blocker; do not replay or repair extraction"
            ),
        }
        _write_stable_time(alignment_root / "terminal.json", terminal, "terminal_at")
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (alignment_root / "terminal.json").is_file():
            return _load_json(alignment_root / "terminal.json", "alignment terminal")
        return _phase_failure_terminal(
            root=root,
            phase="alignment",
            exc=exc,
            turn_names=ALIGNMENT_TURN_NAMES,
            model=ALIGNMENT_MODEL,
            effort=ALIGNMENT_EFFORT,
            maximum_total_tokens=ALIGNMENT_MAX_TOKENS,
            runtime_lock=frozen["runtime_lock"],
        )


async def run_all(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "evaluation terminal")
    support = await run_support(output_dir=root, timeout_seconds=timeout_seconds)
    if support.get("alignment_audit_authorized") is not True:
        return support
    return await run_alignment(output_dir=root, timeout_seconds=timeout_seconds)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate immutable adoption output with frozen support/alignment judges"
    )
    parser.add_argument(
        "action",
        choices=("freeze-support", "run-support", "freeze-alignment", "run-alignment", "run"),
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    root = Path(args.output_dir)
    if args.action == "freeze-support":
        frozen = freeze_support(output_dir=root)
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "witness_count": frozen["spec"]["witness_count"],
        }
    elif args.action == "run-support":
        result = asyncio.run(
            run_support(output_dir=root, timeout_seconds=args.timeout_seconds)
        )
    elif args.action == "freeze-alignment":
        frozen = freeze_alignment(output_dir=root)
        result = {
            "state": "frozen_before_two_turn_neutral_alignment",
            "root": str(frozen["root"]),
            "turn_count": len(frozen["turns"]),
        }
    elif args.action == "run-alignment":
        result = asyncio.run(
            run_alignment(output_dir=root, timeout_seconds=args.timeout_seconds)
        )
    else:
        result = asyncio.run(run_all(output_dir=root, timeout_seconds=args.timeout_seconds))
    sanitized = {
        key: result.get(key)
        for key in (
            "state",
            "terminal_reason",
            "usage_status",
            "support_status_counts",
            "alignment_audit_authorized",
            "development_quality_passed",
            "development_winner_frozen",
            "holdout_authorized",
            "production_mutated",
            "root",
            "witness_count",
            "turn_count",
        )
        if key in result
    }
    print(json.dumps(sanitized, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
