from __future__ import annotations

"""Run the unchanged frozen side-free support judge over v265."""

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
from . import app_server_judge_v5_selection_v250_frozen_support as v250
from . import app_server_judge_v5_selection_v265_segment_isolation as v265
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v266_frozen_support_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v266_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v266_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v266_frozen_support"
TURN_NAME = "v266_frozen_support_v265_candidate"
MODEL = v250.MODEL
EFFORT = v250.EFFORT
MAX_TOTAL_TOKENS = 70_000
MAX_PROMPT_BYTES = 130_000
MAX_SCHEMA_BYTES = 55_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_CASE_COUNT = 2
EXPECTED_REFERENCE_WITNESSES = 27
EXPECTED_CANDIDATE_WITNESSES = 32
EXPECTED_WITNESS_COUNT = EXPECTED_REFERENCE_WITNESSES + EXPECTED_CANDIDATE_WITNESSES
USAGE_FIELDS = v265.USAGE_FIELDS
PROJECT_ROOT = v265.PROJECT_ROOT
PIPELINE_ROOT = v265.PIPELINE_ROOT
V265_ROOT = v265.DEFAULT_OUTPUT_ROOT
V223_ROOT = v250.V223_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v266-frozen-support-v265"
).resolve()
PINNED_CODEX_0_144_1 = v265.PINNED_CODEX_0_144_1


class V266FrozenSupportError(RuntimeError):
    """The v266 frozen support gate cannot proceed or be adopted safely."""


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V266FrozenSupportError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V266FrozenSupportError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V266FrozenSupportError(f"frozen {path.name} drifted")
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


def _v265_turn_roots() -> list[Path]:
    roots = sorted(V265_ROOT.glob("turns/v265-segment-*"))
    if len(roots) != 2:
        raise V266FrozenSupportError("v265 turn membership drifted")
    return roots


def _lineage_paths() -> dict[str, Path]:
    turn_roots = _v265_turn_roots()
    return {
        "v265_terminal": V265_ROOT / "terminal.json",
        "v265_gate": V265_ROOT / "architecture-structural-gate.json",
        "v265_runtime_lock": V265_ROOT / "runtime-lock.json",
        "v265_sidecar_0": turn_roots[0] / "sidecar.json",
        "v265_sidecar_1": turn_roots[1] / "sidecar.json",
        "v265_normalized": V265_ROOT / "normalized-output.private.json",
        "v265_provenance": V265_ROOT / "evidence-provenance.private.json",
        "v265_input_0": turn_roots[0] / "input.private.json",
        "v265_input_1": turn_roots[1] / "input.private.json",
        "shared_reference": v250.v245.v220.DEFAULT_OUTPUT_ROOT
        / "shared-reference-seed-v1.json",
        "v223_runtime_lock": V223_ROOT / "runtime-lock.json",
        "v223_instructions": V223_ROOT / "support-instructions.private.md",
        "v223_design": V223_ROOT / "support-design.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    v265.verify_runtime_lock(paths["v265_runtime_lock"])
    v250.v245.v223.verify_runtime_lock(paths["v223_runtime_lock"])
    terminal = _load_json(paths["v265_terminal"], "v265 terminal")
    gate = _load_json(paths["v265_gate"], "v265 gate")
    sidecars = [
        _load_json(paths[f"v265_sidecar_{index}"], f"v265 sidecar {index}")
        for index in range(2)
    ]
    design = _load_json(paths["v223_design"], "v223 design")
    instructions = paths["v223_instructions"].read_text(encoding="utf-8")
    frozen_instructions = v250.v245.v143.support_base_instructions_v143()
    if (
        terminal.get("terminal_reason")
        != "v265_segment_isolation_structural_cost_gate_passed"
        or terminal.get("support_alignment_authorized") is not True
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or gate.get("passed") is not True
        or gate.get("dense_event_count") != 31
        or gate.get("candidate_only_nominal_no_signal_event_count") != 1
        or gate.get("production_amortized_total_token_ratio") != 0.274543
        or sum((sidecar.get("usage") or {}).get("total_tokens", 0) for sidecar in sidecars)
        != 72_095
        or any(sidecar.get("usage_status") != "measured" for sidecar in sidecars)
        or any(sidecar.get("usage_complete") is not True for sidecar in sidecars)
        or instructions != frozen_instructions
        or design.get("support_instruction_hash") != sha256_text(frozen_instructions)
        or design.get("support_model") != MODEL
        or design.get("reasoning_effort") != EFFORT
    ):
        raise V266FrozenSupportError("v266 frozen support lineage or protocol drifted")
    return {"paths": paths, "records": records}


def _event_claim(event: Mapping[str, Any]) -> str:
    claim = event.get("claim_text")
    if not isinstance(claim, str) or not claim.strip():
        raise V266FrozenSupportError("support witness has no claim text")
    return claim


def build_support_pool(lineage: Mapping[str, Any]) -> dict[str, Any]:
    source_inputs = [
        _load_json(lineage["paths"][f"v265_input_{index}"], f"v265 source input {index}")
        for index in range(2)
    ]
    source = {
        "episode_id": source_inputs[0]["episode_id"],
        "segments": [
            segment for source_input in source_inputs for segment in source_input["segments"]
        ],
    }
    candidate = _load_json(
        lineage["paths"]["v265_normalized"], "v265 normalized output"
    )
    reference = _load_json(
        lineage["paths"]["shared_reference"], "shared reference"
    )
    source_by_id = {str(row["segment_id"]): row for row in source["segments"]}
    candidate_by_id = {str(row["segment_id"]): row for row in candidate["segments"]}
    reference_by_id = {str(row["segment_id"]): row for row in reference["references"]}
    segment_ids = [v265.v249.v239.DENSE_SEGMENT_ID, v265.v249.v239.NO_SIGNAL_SEGMENT_ID]
    if any(
        segment_id not in source_by_id
        or segment_id not in candidate_by_id
        or segment_id not in reference_by_id
        for segment_id in segment_ids
    ):
        raise V266FrozenSupportError("support pool segment coverage drifted")
    cases: list[dict[str, Any]] = []
    units: list[dict[str, Any]] = []
    origin_rows: list[dict[str, Any]] = []
    for segment_id in segment_ids:
        source_row = source_by_id[segment_id]
        source_excerpt = str(source_row["segment_text"])
        reference_row = reference_by_id[segment_id]
        if sha256_text(source_excerpt) != reference_row["text_sha256"]:
            raise V266FrozenSupportError("support source text hash drifted")
        density = str(source_row["density_stratum"])
        case_id = "case_" + sha256_text(f"v266|{segment_id}")[:24]
        reference_events = list(
            reference_row["golden_output"].get("discourse_events") or []
        )
        candidate_events = list(candidate_by_id[segment_id].get("events") or [])
        witnesses: list[dict[str, Any]] = []
        for origin, events in (
            ("reference", reference_events),
            ("candidate", candidate_events),
        ):
            for event_index, event in enumerate(events):
                event_hash = sha256_text(v265.v249._canonical_json(event))
                witness_id = "w_" + sha256_text(
                    f"v266|{case_id}|{origin}|{event_index}|{event_hash}"
                )[:24]
                witnesses.append({"witness_id": witness_id, "event": dict(event)})
                units.append(
                    {
                        "case_id": case_id,
                        "witness_id": witness_id,
                        "proposition": {"claim_text": _event_claim(event)},
                        "source_excerpt": source_excerpt,
                    }
                )
                origin_rows.append(
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
        witnesses.sort(key=lambda row: str(row["witness_id"]))
        cases.append(
            {
                "case_id": case_id,
                "segment_id": segment_id,
                "density_stratum": density,
                "source_excerpt": source_excerpt,
                "witnesses": witnesses,
                "reference_event_count": len(reference_events),
                "candidate_event_count": len(candidate_events),
            }
        )
    cases.sort(key=lambda row: str(row["case_id"]))
    units.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    origin_rows.sort(key=lambda row: (str(row["case_id"]), str(row["witness_id"])))
    counts = Counter(str(row["origin"]) for row in origin_rows)
    if (
        len(cases) != EXPECTED_CASE_COUNT
        or len(units) != EXPECTED_WITNESS_COUNT
        or counts
        != Counter(
            {
                "reference": EXPECTED_REFERENCE_WITNESSES,
                "candidate": EXPECTED_CANDIDATE_WITNESSES,
            }
        )
    ):
        raise V266FrozenSupportError("support pool witness coverage drifted")
    support_value = v250.v245.v143._support_input(units)
    prompt = v250.v245.v175.compact_support_prompt(units)
    schema = v250.v245.v143.support_output_schema(support_value)
    schema_text = v265.v249._canonical_json(schema)
    if (
        len(prompt.encode("utf-8")) > MAX_PROMPT_BYTES
        or len(schema_text.encode("utf-8")) > MAX_SCHEMA_BYTES
    ):
        raise V266FrozenSupportError("support request size cap exceeded")
    return {
        "schema_version": SCHEMA_VERSION,
        "cases": cases,
        "units": units,
        "origin_rows": origin_rows,
        "support_value": support_value,
        "prompt": prompt,
        "schema": schema,
        "prompt_bytes": len(prompt.encode("utf-8")),
        "schema_bytes": len(schema_text.encode("utf-8")),
    }


def _capacity_policy(root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
            "phase_total_token_bound": MAX_TOTAL_TOKENS,
            "projected_phase_quota_points": projected,
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
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
        "phase_total_token_bound": MAX_TOTAL_TOKENS,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "root": turn_root,
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                *v250._runtime_files(),
                *v265._runtime_files(),
                Path(__file__).resolve(),
                Path(v250.__file__).resolve(),
                Path(v265.__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
            },
            key=str,
        )
    )


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v266 runtime lock")
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(file) for file in _runtime_files()}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V266FrozenSupportError("v266 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("design"),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        lock.get("pool"),
        lock.get("support_input"),
        lock.get("origin_map"),
        lock.get("instructions"),
        lock.get("prompt"),
        lock.get("schema"),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V266FrozenSupportError("v266 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V266FrozenSupportError("v266 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    return {
        "root": root,
        "spec_path": spec_path,
        "spec": _load_json(spec_path, "v266 spec"),
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "support_value": _load_json(root / "support-input.private.json", "support input"),
        "origin_rows": _load_json(root / "origin-map.private.json", "origin map")["rows"],
        "prompt": (root / "support-prompt.private.md").read_text(encoding="utf-8"),
        "schema": _load_json(root / "support-schema.json", "support schema"),
    }


def freeze_v266(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v266 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V266FrozenSupportError("unfinished v266 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    pool = build_support_pool(lineage)
    paths = _turn_paths(root)
    paths["root"].mkdir(parents=True, exist_ok=True)
    capacity_paths = _capacity_policy(root)
    design_path = root / "support-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "strategy": "unchanged_frozen_side_free_support_over_v265_and_shared_reference",
            "support_instruction_hash": sha256_text(
                v250.v245.v143.support_base_instructions_v143()
            ),
            "support_model": MODEL,
            "reasoning_effort": EFFORT,
            "case_count": EXPECTED_CASE_COUNT,
            "reference_witness_count": EXPECTED_REFERENCE_WITNESSES,
            "candidate_witness_count": EXPECTED_CANDIDATE_WITNESSES,
            "origin_labels_visible_to_model": False,
            "prompt_or_rubric_changed": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    pool_path = root / "support-pool.private.json"
    input_path = root / "support-input.private.json"
    origin_path = root / "origin-map.private.json"
    instructions_path = root / "support-instructions.private.md"
    prompt_path = root / "support-prompt.private.md"
    schema_path = root / "support-schema.json"
    _write_immutable(pool_path, {"schema_version": SCHEMA_VERSION, "cases": pool["cases"]})
    _write_immutable(input_path, pool["support_value"])
    _write_immutable(
        origin_path, {"schema_version": SCHEMA_VERSION, "rows": pool["origin_rows"]}
    )
    _write_private_text(
        instructions_path, v250.v245.v143.support_base_instructions_v143()
    )
    _write_private_text(prompt_path, pool["prompt"])
    _write_immutable(schema_path, pool["schema"])
    spec_path = root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "state": "frozen_before_one_turn_side_free_support",
            "declared_turn_count": 1,
            "turn_name": TURN_NAME,
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "witness_count": EXPECTED_WITNESS_COUNT,
            "prompt_bytes": pool["prompt_bytes"],
            "schema_bytes": pool["schema_bytes"],
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    lock_path = root / "runtime-lock.json"
    lock = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": PHASE_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(file) for file in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "pool": _record(pool_path),
        "support_input": _record(input_path),
        "origin_map": _record(origin_path),
        "instructions": _record(instructions_path),
        "prompt": _record(prompt_path),
        "schema": _record(schema_path),
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    values = sidecar.get("usage") or {}
    try:
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
    except (KeyError, TypeError, ValueError) as exc:
        raise V266FrozenSupportError("v266 sidecar usage incomplete") from exc
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or usage["total_tokens"] > MAX_TOTAL_TOKENS
    ):
        raise V266FrozenSupportError("v266 sidecar accounting or auth contract failed")
    return usage


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    paths = _turn_paths(root)
    attempted = int(paths["capacity"].exists())
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = attempted
    sidecar_record = None
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            sidecar = _load_json(paths["sidecar"], "v266 sidecar")
            usage = {
                field: int((sidecar.get("usage") or {})[field]) for field in USAGE_FIELDS
            }
            unknown = int(
                sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
            )
        except Exception:
            unknown = 1
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": attempted,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": usage,
        "unknown_usage_attempt_count": unknown,
        "sidecar": sidecar_record,
        "alignment_audit_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "exact_next_action": "audit immutable v266 judge attempt; no retry",
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v266(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v266 terminal")
    frozen = freeze_v266(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V266FrozenSupportError("launch exists; replay prohibited")
        )
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 1,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "managed_chatgpt_auth_only": True,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
    )
    started = time.monotonic()
    paths = _turn_paths(root)
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=v250.v245.v143.support_base_instructions_v143(),
                prompt=frozen["prompt"],
                output_schema=frozen["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                batch_size=EXPECTED_WITNESS_COUNT,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=paths["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise V266FrozenSupportError("v266 support turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v266 sidecar"))
        audit, private_score = v250.v245.v223.score_support_output(
            output=result.output,
            support_value=frozen["support_value"],
            origin_rows=frozen["origin_rows"],
        )
        receipts = v250.v245.v223._support_receipts(result.output)
        audit_path = root / "support-audit.json"
        score_path = root / "support-score.private.json"
        receipts_path = root / "support-receipts.private.json"
        _write_immutable(audit_path, audit)
        _write_immutable(score_path, private_score)
        _write_immutable(receipts_path, receipts)
        authorized = audit["alignment_audit_authorized"] is True
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "completed" if authorized else "inactive_incomplete_recovery_required",
            "terminal_reason": (
                "v266_frozen_support_completed_alignment_authorized"
                if authorized
                else "v266_frozen_support_quality_gate_not_passed"
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
            "semantic_quality_passed": False,
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
            "attempt_spec": _record(frozen["spec_path"]),
            "sidecar": _record(paths["sidecar"]),
            "exact_next_action": (
                "freeze and run the unchanged two-permutation neutral alignment gate"
                if authorized
                else "reject v265 on source support and advance to the next distinct architecture"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v266 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run frozen support over v265")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v266(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "witness_count": frozen["spec"]["witness_count"],
            "prompt_bytes": frozen["spec"]["prompt_bytes"],
            "schema_bytes": frozen["spec"]["schema_bytes"],
        }
    else:
        terminal = asyncio.run(
            run_v266(
                output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
            "support_status_counts": terminal.get("support_status_counts"),
            "no_signal_candidate_event_support_status": terminal.get(
                "no_signal_candidate_event_support_status"
            ),
            "alignment_audit_authorized": terminal.get(
                "alignment_audit_authorized", False
            ),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
