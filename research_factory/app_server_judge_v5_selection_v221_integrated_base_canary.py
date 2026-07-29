from __future__ import annotations

"""Run the frozen v220 fresh integrated base canary exactly once."""

import argparse
import asyncio
import hashlib
import json
import math
import sqlite3
import statistics
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_evaluation as app_eval
from . import codex_app_server
from . import app_server_judge_v5_calibration_v25_diagnostic as v25
from . import app_server_judge_v5_calibration_v26_diagnostic as v26
from . import app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic as v86
from . import app_server_judge_v5_diagnostic as diagnostic
from . import app_server_judge_v5_selection_v220_integrated_base_design as v220
from . import paths as paths_module
from . import util as util_module
from .app_server_capacity_reserve import (
    ReserveCapacityGatedCodexAppServerClient,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
    PINNED_CODEX_0_144_1,
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import USAGE_FIELDS, _validate_usage
from .labels import ValidationError, _validate_schema
from .paths import db_path
from .util import now_iso, sha256_text


V221_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V221_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V221_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v221_spec_v1"
V221_RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_4_selection_v221_runtime_lock_v1"
)
V221_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v221_launch_v1"
V221_REPORT_VERSION = "pif_app_server_judge_v5_4_selection_v221_report_v1"
V221_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v221_gate_v1"
V221_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v221_failure_v1"
V221_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v221_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v221_fresh_integrated_base_canary"
DEFAULT_OUTPUT_ROOT = (
    v220.PIPELINE_ROOT
    / "development-selection-v5_4-v221-fresh-integrated-base-canary"
).resolve()
MINIMUM_REMAINING_RESERVE_PERCENT = 20
TIMEOUT_SECONDS = 1200.0


class JudgeV5SelectionV221Error(RuntimeError):
    """The v221 fresh integrated canary cannot run or score safely."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise JudgeV5SelectionV221Error(f"frozen {path.name} drifted")
        return
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _error_receipt(exc: BaseException) -> dict[str, Any]:
    message = str(exc).encode("utf-8", errors="replace")
    return {
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
    }


def _v220_paths() -> dict[str, Path]:
    root = v220.DEFAULT_OUTPUT_ROOT
    return {
        "terminal": root / "terminal.json",
        "runtime_lock": root / "runtime-lock.json",
        "spec": root / "attempt-spec.json",
        "design": root / "integrated-base-design.json",
        "selection": root / "fresh-development-selection-audit.json",
        "manifest": root / "manifest-v4.json",
        "reference": root / "shared-reference-seed-v1.json",
        "reference_noise": root / "reference-noise-v1.json",
        "request_fingerprints": root / "request-fingerprints.private.json",
        "instructions": root / "integrated-core-instructions.private.md",
    }


def _validate_v220_authorization() -> dict[str, Any]:
    root = v220.DEFAULT_OUTPUT_ROOT
    paths = _v220_paths()
    expected = {path.resolve() for path in paths.values()}
    actual = {path.resolve() for path in root.iterdir() if path.is_file()}
    if actual != expected:
        raise JudgeV5SelectionV221Error("v220 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV221Error("v220 immutable artifact drifted")
    v220.verify_runtime_lock(paths["runtime_lock"])
    values = {
        name: _load_json(path, f"v220 {name}")
        for name, path in paths.items()
        if name != "instructions"
    }
    instructions = paths["instructions"].read_text(encoding="utf-8")
    terminal = values["terminal"]
    spec = values["spec"]
    design = values["design"]
    manifest = values["manifest"]
    fingerprints = values["request_fingerprints"]
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v220_fresh_nonreplay_integrated_base_canary_authorized"
        or terminal.get("semantic_attempt_authorized") is not True
        or terminal.get("authorized_turn_count") != v220.TURN_COUNT
        or terminal.get("authorized_model") != v220.MODEL
        or terminal.get("authorized_effort") != v220.EFFORT
        or terminal.get("fresh_nonreplay_extraction_authorized") is not True
        or terminal.get("extraction_rerun_authorized") is not False
        or terminal.get("batch_5_replayed") is not False
        or terminal.get("prior_episode_or_text_replayed") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or spec.get("semantic_model_calls_declared") != v220.TURN_COUNT
        or spec.get("semantic_model_calls_started") != 0
        or spec.get("retry_count_per_turn") != 0
        or spec.get("fresh_nonreplay_extraction_authorized") is not True
        or spec.get("extraction_rerun_authorized") is not False
        or design.get("strategy")
        != "fresh_nonreplay_integrated_extraction_and_completeness_receipt"
        or design.get("prior_episode_or_text_replayed") is not False
        or design.get("cost_bound", {}).get(
            "base_plus_followup_passes_lte_0_28"
        )
        is not True
        or manifest.get("episode_count") != v220.EPISODE_COUNT
        or manifest.get("segment_count")
        != v220.EPISODE_COUNT * v220.SEGMENTS_PER_EPISODE
        or manifest.get("density_counts") != {"dense": 4, "no_signal": 4}
        or len(fingerprints.get("turns") or []) != v220.TURN_COUNT
        or sha256_text(instructions)
        != spec.get("frozen_inputs", {}).get("instructions", {}).get("sha256")
    ):
        raise JudgeV5SelectionV221Error("v220 authorization contract drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "instructions": instructions,
        **values,
    }


def _build_turn_requests(
    conn: sqlite3.Connection,
    *,
    predecessor: Mapping[str, Any],
) -> list[dict[str, Any]]:
    episodes = app_eval._load_prepared_episodes(
        conn,
        manifest=predecessor["manifest"],
        window_count=v220.WINDOW_COUNT,
        context_chars=v220.CONTEXT_CHARS,
    )
    fingerprints = predecessor["request_fingerprints"]["turns"]
    by_episode = {str(row["episode_id"]): row for row in fingerprints}
    core, guideline_sha = v220.integrated_core_instructions()
    if (
        core != predecessor["instructions"]
        or guideline_sha
        != predecessor["request_fingerprints"]["guideline_sha256"]
    ):
        raise JudgeV5SelectionV221Error("v220 integrated instructions drifted")
    requests = []
    for episode in episodes:
        episode_id = str(episode["episode_id"])
        segments = list(episode["segments"])
        segment_ids = [str(row["segment_id"]) for row in segments]
        base = app_eval.build_episode_base_instructions(
            core_instructions=core,
            episode=episode,
            episode_context=episode["episode_context"],
        )
        prompt = app_eval.build_episode_batch_prompt(segments=segments)
        schema = v220.integrated_episode_batch_schema(
            episode_id=episode_id,
            segment_ids=segment_ids,
        )
        schema_text = json.dumps(
            schema, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        expected = by_episode.get(episode_id)
        observed = {
            "episode_id": episode_id,
            "segment_ids": segment_ids,
            "base_instructions_sha256": sha256_text(base),
            "base_instructions_bytes": len(base.encode("utf-8")),
            "prompt_sha256": sha256_text(prompt),
            "prompt_bytes": len(prompt.encode("utf-8")),
            "schema_sha256": sha256_text(schema_text),
            "schema_bytes": len(schema_text.encode("utf-8")),
            "total_request_bytes": len(base.encode("utf-8"))
            + len(prompt.encode("utf-8"))
            + len(schema_text.encode("utf-8")),
        }
        if not isinstance(expected, Mapping) or any(
            expected.get(key) != value for key, value in observed.items()
        ):
            raise JudgeV5SelectionV221Error("v220 request fingerprint drifted")
        requests.append(
            {
                **observed,
                "turn_name": str(expected["turn_name"]),
                "episode": episode,
                "segments": segments,
                "base_instructions": base,
                "prompt": prompt,
                "schema": schema,
            }
        )
    expected_order = [str(row["episode_id"]) for row in fingerprints]
    if (
        [row["episode_id"] for row in requests] != expected_order
        or len(requests) != v220.TURN_COUNT
    ):
        raise JudgeV5SelectionV221Error("v221 request order drifted")
    return requests


def _build_capacity_policy(
    *, root: Path, predecessor: Mapping[str, Any], turn_names: Sequence[str]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    total_bound = len(turn_names) * v220.MAXIMUM_BASE_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(
        total_bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": V221_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v220_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": predecessor[
                "terminal"
            ]["cumulative_known_usage_lower_bound"]["total_tokens"],
            "predecessor_unknown_usage_turn_count": predecessor["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "predecessor_unknown_usage_upper_bound": predecessor["terminal"][
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "declared_turn_count": len(turn_names),
            "maximum_total_tokens_per_turn": (
                v220.MAXIMUM_BASE_TOTAL_TOKENS_PER_TURN
            ),
            "phase_total_token_bound": total_bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": (
                MINIMUM_REMAINING_RESERVE_PERCENT
            ),
        },
    }
    _write_immutable(audit_path, audit)
    policy = {
        "schema_version": V221_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(turn_names),
        "minimum_remaining_reserve_percent": MINIMUM_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": (
            v220.MAXIMUM_BASE_TOTAL_TOKENS_PER_TURN
        ),
        "phase_total_token_bound": total_bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_immutable(policy_path, policy)
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            set(v220._expected_runtime_paths())
            | {
                Path(__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(v25.__file__).resolve(),
                Path(v26.__file__).resolve(),
                Path(v86.__file__).resolve(),
                Path(diagnostic.__file__).resolve(),
                Path(paths_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
            },
            key=str,
        )
    )


def _turn_artifact_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "base_instructions": turn_root / "base-instructions.private.md",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "input": turn_root / "input.private.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized-output.private.json",
        "diagnostics": turn_root / "diagnostics.private.json",
    }


def _freeze_request_artifacts(
    *, root: Path, requests: Sequence[Mapping[str, Any]]
) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for request in requests:
        turn_name = str(request["turn_name"])
        paths = _turn_artifact_paths(root, turn_name)
        paths["root"].mkdir(parents=True, exist_ok=True)
        _write_private_text(
            paths["base_instructions"], str(request["base_instructions"])
        )
        _write_private_text(paths["prompt"], str(request["prompt"]))
        _write_immutable(paths["schema"], dict(request["schema"]))
        _write_immutable(
            paths["input"],
            {
                "schema_version": "pif_app_server_v221_turn_input_v1",
                "turn_name": turn_name,
                "episode_id": request["episode_id"],
                "segment_ids": request["segment_ids"],
                "base_instructions_sha256": request[
                    "base_instructions_sha256"
                ],
                "prompt_sha256": request["prompt_sha256"],
                "schema_sha256": request["schema_sha256"],
                "total_request_bytes": request["total_request_bytes"],
                "privacy": "ids_hashes_and_sizes_only_source_text_in_private_prompt",
            },
        )
        records[turn_name] = {
            key: _record(path)
            for key, path in paths.items()
            if key in {"base_instructions", "prompt", "schema", "input"}
        }
    return records


def _freeze_runtime_lock(
    *,
    root: Path,
    predecessor: Mapping[str, Any],
    spec_path: Path,
    capacity_paths: Mapping[str, Path],
    request_records: Mapping[str, Mapping[str, Any]],
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V221_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v220_attempt": list(predecessor["records"].values()),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "turn_requests": dict(request_records),
        "managed_chatgpt_auth_only": True,
        "fresh_nonreplay_extraction_only": True,
        "extraction_rerun_allowed": False,
        "batch_5_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_immutable(path, lock)
    verify_runtime_lock(path, predecessor=predecessor)
    return path


def verify_runtime_lock(
    path: Path, *, predecessor: Optional[Mapping[str, Any]] = None
) -> dict[str, Any]:
    lock = _load_json(path, "v221 runtime lock")
    checked = predecessor or _validate_v220_authorization()
    expected_runtime = {str(item) for item in _expected_runtime_paths()}
    actual_runtime = {
        str(Path(record["path"]).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping) and isinstance(record.get("path"), str)
    }
    if (
        lock.get("schema_version") != V221_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or actual_runtime != expected_runtime
        or lock.get("v220_attempt") != list(checked["records"].values())
        or lock.get("managed_chatgpt_auth_only") is not True
        or lock.get("fresh_nonreplay_extraction_only") is not True
        or lock.get("extraction_rerun_allowed") is not False
        or lock.get("batch_5_replay_allowed") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise JudgeV5SelectionV221Error("v221 runtime lock drifted")
    request_records = [
        record
        for group in (lock.get("turn_requests") or {}).values()
        for record in group.values()
    ]
    records = [
        lock.get("pinned_codex_cli"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("runtime_files") or []),
        *(lock.get("v220_attempt") or []),
        *request_records,
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV221Error("v221 runtime lock record drifted")
    reserve.load_reserve_capacity_policy(
        Path(lock["capacity_policy"]["path"])
    )
    return lock


def freeze_v221(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    database_path: Optional[Path] = None,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return {
            "root": root,
            "terminal": _load_json(terminal_path, "v221 terminal"),
        }
    if any(root.iterdir()):
        spec_path = root / "attempt-spec.json"
        lock_path = root / "runtime-lock.json"
        if not spec_path.exists() or not lock_path.exists():
            raise JudgeV5SelectionV221Error(
                "v221 root is nonempty without a complete presemantic freeze"
            )
        if (root / "launch-receipt.json").exists():
            raise JudgeV5SelectionV221Error(
                "v221 launch exists without terminal; replay is prohibited"
            )
        predecessor = _validate_v220_authorization()
        verify_runtime_lock(lock_path, predecessor=predecessor)
        spec = _load_json(spec_path, "v221 spec")
        return {
            "root": root,
            "predecessor": predecessor,
            "spec": spec,
            "spec_path": spec_path,
            "runtime_lock": lock_path,
            "capacity_policy": root / "capacity-policy.json",
            "capacity_audit": root / "capacity-policy-audit.json",
            "turn_names": list(spec["turn_plan"]),
        }

    predecessor = _validate_v220_authorization()
    source_db = (database_path or db_path()).expanduser().resolve()
    conn = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        requests = _build_turn_requests(conn, predecessor=predecessor)
    finally:
        conn.close()
    turn_names = [str(row["turn_name"]) for row in requests]
    request_records = _freeze_request_artifacts(root=root, requests=requests)
    capacity_paths = _build_capacity_policy(
        root=root, predecessor=predecessor, turn_names=turn_names
    )
    spec = {
        "schema_version": V221_SPEC_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_semantic_attempt",
        "turn_plan": turn_names,
        "declared_turn_count": v220.TURN_COUNT,
        "model": v220.MODEL,
        "reasoning_effort": v220.EFFORT,
        "timeout_seconds_per_turn": timeout_seconds,
        "maximum_total_tokens_per_turn": (
            v220.MAXIMUM_BASE_TOTAL_TOKENS_PER_TURN
        ),
        "retry_count_per_turn": 0,
        "fresh_nonreplay_extraction_only": True,
        "extraction_rerun_allowed": False,
        "batch_5_replay_allowed": False,
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "api_key_billing_allowed": False,
        "raw_session_token_access_allowed": False,
        "codex_exec_semantic_calls_allowed": False,
        "semantic_regex_or_keyword_pruning_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity_paths["policy"]),
        "capacity_audit": _record(capacity_paths["audit"]),
        "v220_attempt": predecessor["records"],
        "turn_requests": request_records,
        "privacy": "private_prompts_outputs_and_ids_sanitized_reports_only",
    }
    spec_path = root / "attempt-spec.json"
    _write_immutable(spec_path, spec)
    lock_path = _freeze_runtime_lock(
        root=root,
        predecessor=predecessor,
        spec_path=spec_path,
        capacity_paths=capacity_paths,
        request_records=request_records,
    )
    return {
        "root": root,
        "predecessor": predecessor,
        "spec": spec,
        "spec_path": spec_path,
        "runtime_lock": lock_path,
        "capacity_policy": capacity_paths["policy"],
        "capacity_audit": capacity_paths["audit"],
        "turn_names": turn_names,
        "requests": requests,
    }


def _receipt_errors(
    *,
    receipt: Mapping[str, Any],
    status: str,
    output_event_count: int,
    segment_text: str,
) -> list[str]:
    coverage = receipt.get("coverage_status")
    followup = receipt.get("followup_required")
    gap_types = receipt.get("gap_event_types")
    gap_evidence = receipt.get("gap_evidence")
    rationale = receipt.get("rationale")
    errors = []
    if not isinstance(rationale, str) or not rationale.strip():
        errors.append("empty_rationale")
    if not isinstance(gap_types, list) or len(gap_types) != len(set(gap_types)):
        errors.append("invalid_or_duplicate_gap_event_types")
        gap_types = []
    if not isinstance(gap_evidence, list) or len(gap_evidence) != len(
        set(gap_evidence)
    ):
        errors.append("invalid_or_duplicate_gap_evidence")
        gap_evidence = []
    if any(
        not isinstance(span, str) or not span or span not in segment_text
        for span in gap_evidence
    ):
        errors.append("nonexact_gap_evidence")
    if coverage == "complete":
        if followup is not False or gap_types or gap_evidence:
            errors.append("complete_receipt_has_followup_or_gaps")
        if output_event_count < 1 or status != "coded":
            errors.append("complete_receipt_without_coded_events")
    elif coverage == "no_eligible_events":
        if followup is not False or gap_types or gap_evidence:
            errors.append("no_eligible_receipt_has_followup_or_gaps")
        if output_event_count != 0 or status == "coded":
            errors.append("no_eligible_receipt_has_coded_events")
    elif coverage == "material_gaps":
        if followup is not True or not gap_types or not gap_evidence:
            errors.append("material_gap_receipt_is_not_actionable")
    elif coverage == "abstain":
        if followup is not True:
            errors.append("abstain_receipt_does_not_require_followup")
    else:
        errors.append("unknown_coverage_status")
    return errors


def validate_and_normalize_output(
    *, payload: Mapping[str, Any], request: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not isinstance(payload, dict):
        raise JudgeV5SelectionV221Error("v221 output is not an object")
    schema = request["schema"]
    _validate_schema(schema, payload, path="$")
    rows = payload.get("segments") or []
    expected_ids = list(request["segment_ids"])
    if [row.get("segment_id") for row in rows] != expected_ids:
        raise JudgeV5SelectionV221Error("v221 output segment order drifted")
    receipts = {}
    receipt_diagnostics = []
    prepared_by_id = {
        str(row["segment_id"]): row for row in request["segments"]
    }
    core_payload = {"episode_id": payload["episode_id"], "segments": []}
    for row in rows:
        segment_id = str(row["segment_id"])
        receipt = row.get("coverage_receipt")
        if not isinstance(receipt, Mapping):
            raise JudgeV5SelectionV221Error("v221 coverage receipt is malformed")
        receipts[segment_id] = dict(receipt)
        projected = dict(row)
        projected.pop("coverage_receipt", None)
        core_payload["segments"].append(projected)
    try:
        normalized, diagnostics_rows = app_eval.normalize_episode_batch_output(
            core_payload,
            episode_id=str(request["episode_id"]),
            prepared_segments=list(request["segments"]),
            max_events_per_segment=v220.MAX_EVENTS_PER_SEGMENT,
        )
    except (ValidationError, ValueError) as exc:
        raise JudgeV5SelectionV221Error(
            "v221 core output failed grounding validation"
        ) from exc
    diagnostics_by_id = {
        str(row["segment_id"]): row for row in diagnostics_rows
    }
    for row in normalized["segments"]:
        segment_id = str(row["segment_id"])
        receipt = receipts[segment_id]
        errors = _receipt_errors(
            receipt=receipt,
            status=str(row["status"]),
            output_event_count=len(row.get("events") or []),
            segment_text=str(prepared_by_id[segment_id]["segment_text"]),
        )
        row["coverage_receipt"] = receipt
        receipt_diagnostics.append(
            {
                **dict(diagnostics_by_id[segment_id]),
                "coverage_status": receipt["coverage_status"],
                "followup_required": receipt["followup_required"],
                "gap_event_type_count": len(receipt["gap_event_types"]),
                "gap_evidence_count": len(receipt["gap_evidence"]),
                "coverage_receipt_errors": errors,
                "coverage_receipt_valid": not errors,
            }
        )
    if any(not row["coverage_receipt_valid"] for row in receipt_diagnostics):
        raise JudgeV5SelectionV221Error("v221 coverage receipt contract failed")
    return normalized, receipt_diagnostics


def _production_cost(
    *, measured_base_tokens: int, flagged_episode_count: int
) -> dict[str, Any]:
    segments = v220.EPISODE_COUNT * v220.SEGMENTS_PER_EPISODE
    scaled_base = math.ceil(
        measured_base_tokens * v220.BASELINE_SEGMENT_SCOPE / segments
    )
    bounded_followup = (
        flagged_episode_count
        * v220.MAXIMUM_FOLLOWUP_TOTAL_TOKENS_PER_EPISODE
    )
    scaled_joint = math.ceil(
        (measured_base_tokens + bounded_followup)
        * v220.BASELINE_SEGMENT_SCOPE
        / segments
    )
    base_total = scaled_base + v220.PRODUCTION_AMORTIZED_CONTEXT_TOKENS
    joint_total = scaled_joint + v220.PRODUCTION_AMORTIZED_CONTEXT_TOKENS
    return {
        "measured_base_development_tokens": measured_base_tokens,
        "flagged_episode_count": flagged_episode_count,
        "bounded_followup_development_tokens": bounded_followup,
        "base_production_amortized_total_tokens": base_total,
        "base_production_amortized_total_token_ratio": round(
            base_total / v220.BASELINE_END_TO_END_TOKENS, 6
        ),
        "base_plus_bounded_followup_production_amortized_total_tokens": (
            joint_total
        ),
        "base_plus_bounded_followup_production_amortized_total_token_ratio": (
            round(joint_total / v220.BASELINE_END_TO_END_TOKENS, 6)
        ),
    }


def evaluate_structural_gate(
    *,
    predecessor: Mapping[str, Any],
    diagnostics_rows: Sequence[Mapping[str, Any]],
    usage: Mapping[str, int],
    measured_turn_count: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = predecessor["manifest"]
    metadata = {
        str(segment["segment_id"]): {
            "episode_id": str(episode["episode_id"]),
            "source_id": str(episode["source_id"]),
            "density_stratum": str(segment["density_stratum"]),
            "golden_event_count": int(segment["golden_event_count"]),
        }
        for episode in manifest.get("episodes") or []
        for segment in episode.get("segments") or []
    }
    if set(metadata) != {str(row["segment_id"]) for row in diagnostics_rows}:
        raise JudgeV5SelectionV221Error("v221 diagnostic segment coverage drifted")
    rows = []
    for diagnostic_row in diagnostics_rows:
        segment_id = str(diagnostic_row["segment_id"])
        item = {**metadata[segment_id], **dict(diagnostic_row)}
        golden = item["golden_event_count"]
        item["candidate_to_reference_event_count_ratio"] = (
            round(int(item["output_events"]) / golden, 6) if golden else None
        )
        rows.append(item)
    dense = [row for row in rows if row["density_stratum"] == "dense"]
    no_signal = [
        row for row in rows if row["density_stratum"] == "no_signal"
    ]
    if len(dense) != 4 or len(no_signal) != 4:
        raise JudgeV5SelectionV221Error("v221 density coverage drifted")
    dense_median = round(
        float(
            statistics.median(
                row["candidate_to_reference_event_count_ratio"] for row in dense
            )
        ),
        6,
    )
    unflagged_shortfalls = [
        row
        for row in dense
        if row["candidate_to_reference_event_count_ratio"] < 0.75
        and row["coverage_status"] == "complete"
    ]
    flagged_episode_ids = {
        row["episode_id"] for row in rows if row["followup_required"] is True
    }
    total_input_events = sum(int(row["input_events"]) for row in rows)
    total_output_events = sum(int(row["output_events"]) for row in rows)
    exact_rate = (
        round(total_output_events / total_input_events, 6)
        if total_input_events
        else 1.0
    )
    cost = _production_cost(
        measured_base_tokens=int(usage["total_tokens"]),
        flagged_episode_count=len(flagged_episode_ids),
    )
    checks = {
        "all_8_segments_validated": len(rows) == 8,
        "all_4_turns_usage_measured": measured_turn_count == v220.TURN_COUNT,
        "normalized_exact_evidence_rate_1": exact_rate == 1.0,
        "no_signal_candidate_positive_segments_0": all(
            int(row["output_events"]) == 0 for row in no_signal
        ),
        "metric_grounding_error_events_0": all(
            int(row["metric_grounding_error_events"]) == 0 for row in rows
        ),
        "dense_median_event_count_ratio_gte_0_75": dense_median >= 0.75,
        "unflagged_dense_coverage_shortfall_count_0": not unflagged_shortfalls,
        "coverage_receipt_contract_valid": all(
            row["coverage_receipt_valid"] is True for row in rows
        ),
        "base_production_amortized_total_token_ratio_lte_0_18": (
            cost["base_production_amortized_total_token_ratio"] <= 0.18
        ),
        "base_plus_bounded_followup_total_token_ratio_lte_0_28": (
            cost[
                "base_plus_bounded_followup_production_amortized_total_token_ratio"
            ]
            <= 0.28
        ),
    }
    gate = {
        "schema_version": V221_GATE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": sorted(name for name, passed in checks.items() if not passed),
        "segment_count": len(rows),
        "dense_segment_count": len(dense),
        "no_signal_segment_count": len(no_signal),
        "dense_median_candidate_to_reference_event_count_ratio": dense_median,
        "unflagged_dense_coverage_shortfall_count": len(unflagged_shortfalls),
        "coverage_status_counts": dict(
            sorted(Counter(str(row["coverage_status"]) for row in rows).items())
        ),
        "followup_required_segment_count": sum(
            row["followup_required"] is True for row in rows
        ),
        "followup_required_episode_count": len(flagged_episode_ids),
        "normalized_exact_evidence_rate": exact_rate,
        "cost": cost,
        "semantic_quality_scored": False,
        "fresh_frozen_judge_authorized": all(checks.values()),
        "holdout_authorized": False,
        "production_mutated": False,
    }
    private = {
        "schema_version": V221_REPORT_VERSION,
        "cases": rows,
        "flagged_episode_ids": sorted(flagged_episode_ids),
        "usage": dict(usage),
        "privacy": "private_ids_and_case_diagnostics_no_source_text",
    }
    return gate, private


def _sidecar_accounting(root: Path) -> dict[str, Any]:
    turn_roots = sorted((root / "turns").glob("*"))
    attempted = 0
    measured = 0
    unknown = 0
    usage = {field: 0 for field in USAGE_FIELDS}
    for turn_root in turn_roots:
        if not turn_root.is_dir():
            continue
        sidecar_path = turn_root / "sidecar.json"
        capacity_path = turn_root / "capacity.json"
        if not sidecar_path.exists() and not capacity_path.exists():
            continue
        attempted += 1
        try:
            sidecar = _load_json(sidecar_path, "v221 sidecar")
            item = _validate_usage(sidecar)
            if (
                sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
            ):
                raise JudgeV5SelectionV221Error("v221 sidecar usage is incomplete")
        except Exception:
            unknown += 1
            continue
        measured += 1
        for field in USAGE_FIELDS:
            usage[field] += int(item[field])
    status = (
        "complete"
        if attempted and unknown == 0
        else "partial_unknown"
        if measured
        else "unknown"
        if attempted
        else "not_started"
    )
    return {
        "attempted_turn_count": attempted,
        "measured_turn_count": measured,
        "unknown_usage_turn_count": unknown,
        "usage_status": status,
        "accounting_complete": attempted == v220.TURN_COUNT and unknown == 0,
        "usage": usage,
    }


def _write_failure_terminal(
    *, root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    accounting = _sidecar_accounting(root)
    error = _error_receipt(exc)
    failure = {
        "schema_version": V221_FAILURE_VERSION,
        "failed_at": now_iso(),
        **error,
        **accounting,
        "semantic_retry_allowed": False,
        "fresh_judge_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "error_class_hash_length_and_aggregate_usage_no_private_text",
    }
    failure_path = root / "failure.json"
    _write_stable_time(failure_path, failure, "failed_at")
    predecessor_terminal = frozen["predecessor"]["terminal"]
    known = dict(predecessor_terminal["cumulative_known_usage_lower_bound"])
    for field in USAGE_FIELDS:
        known[field] += int(accounting["usage"].get(field) or 0)
    unknown_turns = int(
        predecessor_terminal["cumulative_unknown_usage_turn_count"]
    ) + int(accounting["unknown_usage_turn_count"])
    unknown_upper = int(
        predecessor_terminal["cumulative_conservative_unknown_usage_upper_bound"]
    ) + int(accounting["unknown_usage_turn_count"]) * int(
        v220.MAXIMUM_BASE_TOTAL_TOKENS_PER_TURN
    )
    terminal = {
        "schema_version": V221_TERMINAL_VERSION,
        "state": "failed",
        "terminal_at": now_iso(),
        "terminal_reason": "infrastructure_or_extraction_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "development_winner_frozen": False,
        "fresh_judge_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "semantic_retry_allowed": False,
        "usage_status": accounting["usage_status"],
        "accounting_complete": accounting["accounting_complete"],
        "usage": accounting["usage"],
        "cumulative_known_usage_lower_bound": known,
        "cumulative_unknown_usage_turn_count": unknown_turns,
        "cumulative_conservative_unknown_usage_upper_bound": unknown_upper,
        "failure": _record(failure_path),
        "spec": _record(frozen["spec_path"]),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "launch_receipt": _record(root / "launch-receipt.json"),
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[
            str(PINNED_CODEX_0_144_1),
            "app-server",
            "--stdio",
            "--strict-config",
        ]
    )


def _default_client_factory(
    policy_path: Path,
) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path,
        inner_factory=_inner_factory,
    )


async def run_v221(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    database_path: Optional[Path] = None,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _default_client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v221 terminal")
    frozen = freeze_v221(
        output_dir=root,
        database_path=database_path,
        timeout_seconds=timeout_seconds,
    )
    verify_runtime_lock(
        frozen["runtime_lock"], predecessor=frozen["predecessor"]
    )
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise JudgeV5SelectionV221Error(
            "v221 launch receipt exists; semantic replay is prohibited"
        )
    launch = {
        "schema_version": V221_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "launched_at": now_iso(),
        "declared_turn_count": v220.TURN_COUNT,
        "retry_count_per_turn": 0,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "managed_chatgpt_auth_only": True,
        "fresh_nonreplay_extraction_only": True,
        "semantic_thread_or_turn_started_before_receipt": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_immutable(launch_path, launch)
    source_db = (database_path or db_path()).expanduser().resolve()
    conn = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    started = time.monotonic()
    diagnostics_rows = []
    try:
        requests = _build_turn_requests(conn, predecessor=frozen["predecessor"])
        async with client_factory(frozen["capacity_policy"]) as client:
            for request in requests:
                paths = _turn_artifact_paths(root, str(request["turn_name"]))
                result = await client.run_ephemeral_structured_turn(
                    model=v220.MODEL,
                    effort=v220.EFFORT,
                    base_instructions=str(request["base_instructions"]),
                    prompt=str(request["prompt"]),
                    output_schema=dict(request["schema"]),
                    cwd=v220.PROJECT_ROOT,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=v220.SEGMENTS_PER_EPISODE,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=paths["capacity"],
                )
                if result.status_ok is not True or not isinstance(
                    result.output, dict
                ):
                    raise JudgeV5SelectionV221Error(
                        "v221 semantic turn did not return structured output"
                    )
                normalized, turn_diagnostics = validate_and_normalize_output(
                    payload=result.output,
                    request=request,
                )
                _write_immutable(paths["normalized"], normalized)
                _write_immutable(
                    paths["diagnostics"],
                    {
                        "schema_version": "pif_app_server_v221_turn_diagnostics_v1",
                        "turn_name": request["turn_name"],
                        "episode_id": request["episode_id"],
                        "segments": turn_diagnostics,
                        "privacy": "private_ids_and_metrics_no_source_text",
                    },
                )
                diagnostics_rows.extend(turn_diagnostics)
        accounting = _sidecar_accounting(root)
        if (
            accounting["accounting_complete"] is not True
            or accounting["measured_turn_count"] != v220.TURN_COUNT
            or accounting["unknown_usage_turn_count"] != 0
        ):
            raise JudgeV5SelectionV221Error("v221 usage accounting is incomplete")
        gate, private = evaluate_structural_gate(
            predecessor=frozen["predecessor"],
            diagnostics_rows=diagnostics_rows,
            usage=accounting["usage"],
            measured_turn_count=accounting["measured_turn_count"],
        )
        gate["wall_time_seconds"] = round(time.monotonic() - started, 3)
        gate_path = root / "structural-gate.json"
        private_path = root / "structural-gate.private.json"
        _write_immutable(gate_path, gate)
        _write_immutable(private_path, private)
        predecessor_terminal = frozen["predecessor"]["terminal"]
        known = dict(predecessor_terminal["cumulative_known_usage_lower_bound"])
        for field in USAGE_FIELDS:
            known[field] += int(accounting["usage"][field])
        passed = gate["passed"] is True
        flagged = int(gate["followup_required_episode_count"])
        next_name = (
            "development-selection-v5_4-v222-integrated-gap-followup"
            if flagged
            else "development-selection-v5_4-v222-integrated-fresh-judge"
        )
        terminal = {
            "schema_version": V221_TERMINAL_VERSION,
            "state": "completed" if passed else "failed",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v221_integrated_base_structural_gate_passed"
                if passed
                else "v221_integrated_base_structural_quality_gate_not_passed"
            ),
            "terminal_classification": (
                "active_development_recovery_required"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "overall_evaluation_complete": False,
            "development_winner_frozen": False,
            "fresh_judge_authorized": passed,
            "bounded_gap_followup_authorized": passed and flagged > 0,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "semantic_retry_allowed": False,
            "fresh_nonreplay_extraction_only": True,
            "extraction_rerun_performed": False,
            "batch_5_replayed": False,
            "usage_status": accounting["usage_status"],
            "accounting_complete": accounting["accounting_complete"],
            "usage": accounting["usage"],
            "cumulative_known_usage_lower_bound": known,
            "cumulative_unknown_usage_turn_count": predecessor_terminal[
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_conservative_unknown_usage_upper_bound": (
                predecessor_terminal[
                    "cumulative_conservative_unknown_usage_upper_bound"
                ]
            ),
            "production_amortized_total_token_ratio": gate["cost"][
                "base_production_amortized_total_token_ratio"
            ],
            "base_plus_bounded_followup_total_token_ratio": gate["cost"][
                "base_plus_bounded_followup_production_amortized_total_token_ratio"
            ],
            "structural_gate": _record(gate_path),
            "private_score": _record(private_path),
            "spec": _record(frozen["spec_path"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "launch_receipt": _record(launch_path),
            "required_next_artifact_path": (
                str(v220.PIPELINE_ROOT / next_name / "terminal.json")
                if passed
                else None
            ),
        }
        _write_immutable(root / "terminal.json", terminal)
        return terminal
    except asyncio.CancelledError as exc:
        _write_failure_terminal(root=root, frozen=frozen, exc=exc)
        raise
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return _write_failure_terminal(root=root, frozen=frozen, exc=exc)
    finally:
        conn.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the v221 fresh integrated base canary"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--database", default=str(db_path()))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v221(
            output_dir=Path(args.output_dir),
            database_path=Path(args.database),
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "usage_status": terminal["usage_status"],
                "accounting_complete": terminal["accounting_complete"],
                "fresh_judge_authorized": terminal["fresh_judge_authorized"],
                "bounded_gap_followup_authorized": terminal.get(
                    "bounded_gap_followup_authorized", False
                ),
                "holdout_authorized": terminal["holdout_authorized"],
                "production_mutated": terminal["production_mutated"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
