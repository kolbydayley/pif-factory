from __future__ import annotations

"""Run the frozen side-free support judge over the two v229 additions."""

import argparse
import asyncio
import hashlib
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5_selection_v223_frozen_support_audit as v223
from . import app_server_judge_v5_selection_v229_metric_grounding_repair as v229
from . import codex_app_server
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v230_support_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v230_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v230_terminal_v1"
PHASE_ID = "development_selection_v5_4_v230_incremental_support"
TURN_NAME = "v230_incremental_support"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
MAX_NEW_TOKENS = 50_000
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
TIMEOUT_SECONDS = 1200.0
PINNED_CODEX_0_144_1 = v229.PINNED_CODEX_0_144_1
USAGE_FIELDS = v229.USAGE_FIELDS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = v229.PIPELINE_ROOT
V229_ROOT = v229.DEFAULT_OUTPUT_ROOT
V229_TURN_ROOT = V229_ROOT / "turns/v229-metric-grounding-repair"
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v230-incremental-support"
).resolve()
FROZEN_SUPPORT_INSTRUCTION_HASH = (
    "09e646f13513d3088089da0244968461ae31f7454a9243bae7c5c5a487de8e1a"
)
EXPECTED_V229_HASHES = {
    "attempt_spec": "4c26cb7cb25c8a5d73c21e9909207efe635d87f891bc75b4d5d8e7d47f9fb6f4",
    "runtime_lock": "afab7c3609b3e11d3889f7d1c79b88ede6d6f7f4c71cafbcf586fc3a9bb35438",
    "terminal": "6ead7bb87f266c55e14c0a001463cc449314733044675dcda2a02f81a9fd2329",
    "normalized": "a785052356fa74925ea7d65da7be8f56866f27a05d20cc8d58b88235795d9cbd",
    "gate": "d7063a4cc5b51b1ca1c238924687bb63e0234f3d8f93c3b5ec750aa70399d2f9",
    "sidecar": "1a1029b3be69271cbc3ba84da746060cf0fd534312e88906c3efc98c52391628",
}


class V230SupportError(RuntimeError):
    """The incremental support audit cannot be frozen or executed safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise V230SupportError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, indent=2
    ) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V230SupportError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V230SupportError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    data = resolved.read_bytes()
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        return _record(Path(str(record["path"]))) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _v229_paths() -> dict[str, Path]:
    return {
        "attempt_spec": V229_ROOT / "attempt-spec.json",
        "runtime_lock": V229_ROOT / "runtime-lock.json",
        "terminal": V229_ROOT / "terminal.json",
        "normalized": V229_TURN_ROOT / "normalized.private.json",
        "gate": V229_TURN_ROOT / "metric-repair-gate.json",
        "sidecar": V229_TURN_ROOT / "sidecar.json",
    }


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _v229_paths()
    records = {name: _record(path) for name, path in paths.items()}
    for name, expected in EXPECTED_V229_HASHES.items():
        if records[name]["sha256"] != expected:
            raise V230SupportError(f"v229 {name} drifted")
    v229.verify_runtime_lock(paths["runtime_lock"])
    terminal = _load_json(paths["terminal"], "v229 terminal")
    gate = _load_json(paths["gate"], "v229 gate")
    sidecar = _load_json(paths["sidecar"], "v229 sidecar")
    normalized = _load_json(paths["normalized"], "v229 normalized")
    if (
        terminal.get("terminal_reason") != "v229_structural_cost_gate_passed"
        or terminal.get("support_alignment_authorized") is not True
        or gate.get("passed") is not True
        or gate.get("failed_checks") != []
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sha256_text(v223.v143.support_base_instructions_v143())
        != FROZEN_SUPPORT_INSTRUCTION_HASH
    ):
        raise V230SupportError("v229 support authorization drifted")
    return {
        "records": records,
        "terminal": terminal,
        "gate": gate,
        "sidecar": sidecar,
        "normalized": normalized,
    }


def _build_pool(lineage: Mapping[str, Any]) -> dict[str, Any]:
    normalized = lineage["normalized"]
    repaired_rows = normalized["repair"]["segments"]
    second_rows = normalized["second_episode"]["segments"]
    events_by_segment: dict[str, list[dict[str, Any]]] = {}
    for row in [*repaired_rows, *second_rows]:
        events = [dict(event) for event in row.get("events") or []]
        if events:
            events_by_segment[str(row["segment_id"])] = events
    if len(events_by_segment) != 2 or any(
        len(events) != 1 for events in events_by_segment.values()
    ):
        raise V230SupportError("v229 addition coverage drifted")
    predecessor_frozen = v229._load_frozen(V229_ROOT)["predecessor_frozen"]
    sources = {
        str(predecessor_frozen["repair_source"]["segment_id"]): str(
            predecessor_frozen["repair_source"]["segment_text"]
        )
    }
    second_input = predecessor_frozen["predecessor_input"][
        "second_episode_input"
    ]
    density_by_segment = {}
    for row in second_input["normalization_segments"]:
        sources[str(row["segment_id"])] = str(row["segment_text"])
        density_by_segment[str(row["segment_id"])] = str(
            row["density_stratum"]
        )
    density_by_segment[
        str(predecessor_frozen["repair_source"]["segment_id"])
    ] = str(predecessor_frozen["repair_source"]["density_stratum"])
    units = []
    witnesses = []
    origins = []
    for segment_id, events in sorted(events_by_segment.items()):
        if segment_id not in sources:
            raise V230SupportError("v229 source coverage drifted")
        case_id = "case_" + sha256_text(f"v230|{segment_id}")[:24]
        event = events[0]
        claim = event.get("claim_text")
        evidence = event.get("evidence")
        if (
            not isinstance(claim, str)
            or not claim.strip()
            or not isinstance(evidence, str)
            or not evidence
            or evidence not in sources[segment_id]
        ):
            raise V230SupportError("v229 witness grounding drifted")
        event_hash = sha256_text(_canonical_json(event))
        witness_id = "w_" + sha256_text(
            f"v230|{case_id}|{event_hash}"
        )[:24]
        units.append(
            {
                "case_id": case_id,
                "witness_id": witness_id,
                "proposition": {"claim_text": claim},
                "source_excerpt": sources[segment_id],
            }
        )
        witnesses.append(
            {
                "case_id": case_id,
                "witness_id": witness_id,
                "segment_id": segment_id,
                "density_stratum": density_by_segment[segment_id],
                "event": event,
                "source_excerpt": sources[segment_id],
            }
        )
        origins.append(
            {
                "case_id": case_id,
                "witness_id": witness_id,
                "segment_id": segment_id,
                "density_stratum": density_by_segment[segment_id],
                "origin": "candidate",
                "event_sha256": event_hash,
            }
        )
    units.sort(key=lambda row: (row["case_id"], row["witness_id"]))
    witnesses.sort(key=lambda row: (row["case_id"], row["witness_id"]))
    origins.sort(key=lambda row: (row["case_id"], row["witness_id"]))
    support_value = v223.v143._support_input(units)
    prompt = v223.v175.compact_support_prompt(units)
    schema = v223.v143.support_output_schema(support_value)
    return {
        "units": units,
        "witnesses": witnesses,
        "origins": origins,
        "support_value": support_value,
        "prompt": prompt,
        "schema": schema,
    }


def _score_support(
    output: Mapping[str, Any], pool: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    errors = v223.v143.validate_support_output(
        output, pool["support_value"]
    )
    if errors:
        raise V230SupportError("frozen support output validation failed")
    origin_by_id = {
        str(row["witness_id"]): row for row in pool["origins"]
    }
    decisions = {str(row["witness_id"]): row for row in output["units"]}
    if set(decisions) != set(origin_by_id):
        raise V230SupportError("support witness coverage drifted")
    rows = []
    for witness_id, decision in sorted(decisions.items()):
        rows.append(
            {
                **origin_by_id[witness_id],
                "support_status": decision["support_status"],
                "source_evidence_span_count": len(
                    decision["source_evidence_spans"]
                ),
            }
        )
    counts = Counter(str(row["support_status"]) for row in rows)
    passed = counts == Counter({"supported": len(rows)})
    audit = {
        "schema_version": SCHEMA_VERSION,
        "passed": passed,
        "witness_count": len(rows),
        "support_status_counts": dict(sorted(counts.items())),
        "all_new_witnesses_supported": passed,
        "alignment_audit_authorized": passed,
        "support_protocol_instruction_hash": FROZEN_SUPPORT_INSTRUCTION_HASH,
        "side_free": True,
        "semantic_quality_passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "privacy": "sanitized counts and statuses only",
    }
    private = {
        "schema_version": SCHEMA_VERSION,
        "rows": rows,
        "receipts": v223._support_receipts(output),
    }
    return audit, private


def _capacity_policy(root: Path, lineage: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    points = math.ceil(
        MAX_NEW_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v229_terminal": lineage["records"]["terminal"],
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_NEW_TOKENS,
            "phase_total_token_bound": MAX_NEW_TOKENS,
            "projected_phase_quota_points": points,
            "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": "pif_app_server_capacity_policy_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_NEW_TOKENS,
        "phase_total_token_bound": MAX_NEW_TOKENS,
        "projected_phase_quota_points": points,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                Path(v223.__file__).resolve(),
                *v229._runtime_files(),
                *v223._expected_runtime_paths(),
            },
            key=str,
        )
    )


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v230 runtime lock")
    expected_runtime = {str(item) for item in _runtime_files()}
    actual_runtime = {
        str(Path(row["path"]).expanduser().resolve())
        for row in lock.get("runtime_files") or []
    }
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or actual_runtime != expected_runtime
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V230SupportError("runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V230SupportError("runtime lock record drifted")
    _validate_lineage()
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    pool = _load_json(root / "support-pool.private.json", "support pool")
    support_value = _load_json(
        root / "support-input.private.json", "support input"
    )
    pool["support_value"] = support_value
    return {
        "root": root,
        "paths": _turn_paths(root),
        "spec": _load_json(root / "attempt-spec.json", "v230 spec"),
        "spec_path": root / "attempt-spec.json",
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "pool": pool,
        "support_value": support_value,
        "prompt": (root / "support-prompt.private.md").read_text(),
        "base": (root / "support-instructions.private.md").read_text(),
        "schema": _load_json(root / "support-schema.json", "support schema"),
    }


def freeze_v230(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V230SupportError("unfinished v230 root is not replayable")
        if (root / "launch-receipt.json").exists():
            raise V230SupportError("v230 launch exists; replay prohibited")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    pool = _build_pool(lineage)
    base = v223.v143.support_base_instructions_v143()
    if sha256_text(base) != FROZEN_SUPPORT_INSTRUCTION_HASH:
        raise V230SupportError("frozen support instructions drifted")
    pool_path = root / "support-pool.private.json"
    input_path = root / "support-input.private.json"
    prompt_path = root / "support-prompt.private.md"
    base_path = root / "support-instructions.private.md"
    schema_path = root / "support-schema.json"
    _turn_paths(root)["capacity"].parent.mkdir(parents=True, exist_ok=True)
    _write_immutable(pool_path, {k: pool[k] for k in ("units", "witnesses", "origins")})
    _write_immutable(input_path, pool["support_value"])
    _write_private_text(prompt_path, pool["prompt"])
    _write_private_text(base_path, base)
    _write_immutable(schema_path, pool["schema"])
    capacity_paths = _capacity_policy(root, lineage)
    frozen_request = [
        _record(pool_path),
        _record(input_path),
        _record(prompt_path),
        _record(base_path),
        _record(schema_path),
    ]
    spec = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "state": "frozen_before_single_semantic_turn",
        "declared_turn_count": 1,
        "retry_count": 0,
        "model": MODEL,
        "effort": EFFORT,
        "maximum_new_turn_tokens": MAX_NEW_TOKENS,
        "strategy": "frozen_side_free_support_over_two_v229_additions",
        "support_instruction_hash": FROZEN_SUPPORT_INSTRUCTION_HASH,
        "side_labels_in_model_input": False,
        "witness_count": len(pool["units"]),
        "v229_replayed": False,
        "alignment_allowed_after_pass": True,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "direct_lineage": lineage["records"],
        "frozen_request": frozen_request,
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "prompt_bytes": len(pool["prompt"].encode()),
        "schema_bytes": len(_canonical_json(pool["schema"]).encode()),
        "privacy": "private sources and events with sanitized reports",
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "frozen_request": frozen_request,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(root / "runtime-lock.json", lock, "created_at")
    verify_runtime_lock(root / "runtime-lock.json")
    return _load_frozen(root)


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    try:
        return v229._usage(sidecar)
    except Exception as exc:
        raise V230SupportError("support usage is incomplete") from exc


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[
            str(PINNED_CODEX_0_144_1),
            "app-server",
            "--stdio",
            "--strict-config",
        ]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    paths = frozen["paths"]
    usage = {field: 0 for field in USAGE_FIELDS}
    usage_status = "unknown"
    accounting_complete = False
    sidecar_record = None
    if paths["sidecar"].is_file():
        sidecar = _load_json(paths["sidecar"], "v230 sidecar")
        sidecar_record = _record(paths["sidecar"])
        try:
            usage = _usage(sidecar)
            usage_status = str(sidecar.get("usage_status"))
            accounting_complete = sidecar.get("usage_complete") is True
        except Exception:
            pass
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": "v230_support_attempt_failed",
        "error_class": type(exc).__name__,
        "error_message_sha256": sha256_text(message.decode(errors="replace")),
        "error_message_bytes": len(message),
        "semantic_attempt_count": 1 if paths["capacity"].exists() else 0,
        "semantic_retry_count": 0,
        "usage_status": usage_status,
        "accounting_complete": accounting_complete,
        "usage": usage,
        "sidecar": sidecar_record,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "alignment_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v230(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v230 terminal")
    frozen = freeze_v230(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = root / "launch-receipt.json"
    if launch_path.exists():
        raise V230SupportError("v230 launch exists; replay prohibited")
    _write_immutable(
        launch_path,
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 1,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "managed_chatgpt_auth_only": True,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
    )
    started = time.monotonic()
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=frozen["base"],
                prompt=frozen["prompt"],
                output_schema=frozen["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=frozen["paths"]["sidecar"],
                output_path=frozen["paths"]["output"],
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=frozen["paths"]["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise V230SupportError("support turn did not complete")
        sidecar = _load_json(frozen["paths"]["sidecar"], "v230 sidecar")
        usage = _usage(sidecar)
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("plan_type") != "pro"
            or sidecar.get("model") != MODEL
            or sidecar.get("effort") != EFFORT
            or sidecar.get("error_class") is not None
            or usage["total_tokens"] > MAX_NEW_TOKENS
        ):
            raise V230SupportError("measured support sidecar contract failed")
        audit, private = _score_support(result.output, frozen["pool"])
        audit_path = root / "support-audit.json"
        private_path = root / "support-score.private.json"
        receipts_path = root / "frozen-support-receipts.private.json"
        _write_immutable(audit_path, audit)
        _write_immutable(private_path, private)
        _write_immutable(receipts_path, private["receipts"])
        passed = bool(audit["passed"])
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": (
                "v230_incremental_support_passed"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "terminal_reason": (
                "v230_support_passed_alignment_authorized"
                if passed
                else "v230_new_witness_support_gate_not_passed"
            ),
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "support_passed": passed,
            "alignment_authorized": passed,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "wall_seconds": round(time.monotonic() - started, 6),
            "support_audit": _record(audit_path),
            "support_receipts": _record(receipts_path),
            "support_score_private": _record(private_path),
            "sidecar": _record(frozen["paths"]["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": (
                "align support-positive additions against the shared reference"
                if passed
                else "audit the unsupported addition before a new version"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v230 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v230 incremental support")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v230(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "prompt_bytes": frozen["spec"]["prompt_bytes"],
            "schema_bytes": frozen["spec"]["schema_bytes"],
        }
    else:
        terminal = asyncio.run(
            run_v230(
                output_dir=Path(args.output_dir),
                timeout_seconds=args.timeout_seconds,
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "support_passed": terminal.get("support_passed", False),
            "alignment_authorized": terminal.get("alignment_authorized", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
