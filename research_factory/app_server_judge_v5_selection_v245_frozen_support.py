from __future__ import annotations

"""Run the frozen side-free support judge over the immutable v244 candidate."""

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
from . import app_server_judge_v5_calibration_v143_corrected_layered_diagnostic as v143
from . import app_server_judge_v5_selection_v175_support as v175
from . import app_server_judge_v5_selection_v220_integrated_base_design as v220
from . import app_server_judge_v5_selection_v223_frozen_support_audit as v223
from . import app_server_judge_v5_selection_v244_source_indexed_graph as v244
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v245_frozen_support_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v245_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v245_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v245_frozen_support"
TURN_NAME = "v245_frozen_support_v244_candidate"
MODEL = v223.MODEL
EFFORT = v223.EFFORT
MAX_TOTAL_TOKENS = 70_000
MAX_PROMPT_BYTES = 120_000
MAX_SCHEMA_BYTES = 50_000
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_CASE_COUNT = 2
EXPECTED_REFERENCE_WITNESSES = 27
EXPECTED_CANDIDATE_WITNESSES = 32
EXPECTED_WITNESS_COUNT = EXPECTED_REFERENCE_WITNESSES + EXPECTED_CANDIDATE_WITNESSES
USAGE_FIELDS = v244.USAGE_FIELDS
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = v244.PIPELINE_ROOT
V244_ROOT = v244.DEFAULT_OUTPUT_ROOT
V223_ROOT = v223.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v245-frozen-support-v244"
).resolve()
PINNED_CODEX_0_144_1 = v244.PINNED_CODEX_0_144_1


class V245FrozenSupportError(RuntimeError):
    """The frozen support audit cannot proceed or be adopted safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V245FrozenSupportError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V245FrozenSupportError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V245FrozenSupportError(f"frozen {path.name} drifted")
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


def _lineage_paths() -> dict[str, Path]:
    turn_root = next(V244_ROOT.glob("turns/*"))
    return {
        "v244_terminal": V244_ROOT / "terminal.json",
        "v244_gate": V244_ROOT / "architecture-structural-gate.json",
        "v244_runtime_lock": V244_ROOT / "runtime-lock.json",
        "v244_sidecar": turn_root / "sidecar.json",
        "v244_normalized": turn_root / "normalized-output.private.json",
        "v244_provenance": turn_root / "evidence-provenance.private.json",
        "v244_input": turn_root / "input.private.json",
        "shared_reference": v220.DEFAULT_OUTPUT_ROOT / "shared-reference-seed-v1.json",
        "v223_runtime_lock": V223_ROOT / "runtime-lock.json",
        "v223_instructions": V223_ROOT / "support-instructions.private.md",
        "v223_design": V223_ROOT / "support-design.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    v244.verify_runtime_lock(paths["v244_runtime_lock"])
    v223.verify_runtime_lock(paths["v223_runtime_lock"])
    terminal = _load_json(paths["v244_terminal"], "v244 terminal")
    gate = _load_json(paths["v244_gate"], "v244 gate")
    sidecar = _load_json(paths["v244_sidecar"], "v244 sidecar")
    design = _load_json(paths["v223_design"], "v223 design")
    instructions = paths["v223_instructions"].read_text(encoding="utf-8")
    if (
        terminal.get("terminal_reason")
        != "v244_source_indexed_graph_structural_cost_gate_passed"
        or terminal.get("support_alignment_authorized") is not True
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or gate.get("passed") is not True
        or gate.get("dense_event_count") != 31
        or gate.get("candidate_only_nominal_no_signal_event_count") != 1
        or (sidecar.get("usage") or {}).get("total_tokens") != 38_162
        or instructions != v143.support_base_instructions_v143()
        or design.get("support_instruction_hash")
        != sha256_text(v143.support_base_instructions_v143())
        or design.get("support_model") != MODEL
        or design.get("reasoning_effort") != EFFORT
    ):
        raise V245FrozenSupportError("frozen support lineage or protocol drifted")
    return {"paths": paths, "records": records}


def _event_claim(event: Mapping[str, Any]) -> str:
    claim = event.get("claim_text")
    if not isinstance(claim, str) or not claim.strip():
        raise V245FrozenSupportError("support witness has no claim text")
    return claim


def build_support_pool(lineage: Mapping[str, Any]) -> dict[str, Any]:
    source = _load_json(lineage["paths"]["v244_input"], "v244 source input")
    candidate = _load_json(lineage["paths"]["v244_normalized"], "v244 normalized output")
    reference = _load_json(lineage["paths"]["shared_reference"], "shared reference")
    source_by_id = {str(row["segment_id"]): row for row in source["segments"]}
    candidate_by_id = {str(row["segment_id"]): row for row in candidate["segments"]}
    reference_by_id = {str(row["segment_id"]): row for row in reference["references"]}
    segment_ids = [v244.DENSE_SEGMENT_ID, v244.NO_SIGNAL_SEGMENT_ID]
    if any(
        segment_id not in source_by_id
        or segment_id not in candidate_by_id
        or segment_id not in reference_by_id
        for segment_id in segment_ids
    ):
        raise V245FrozenSupportError("support pool segment coverage drifted")
    cases = []
    units = []
    origin_rows = []
    for segment_id in segment_ids:
        source_row = source_by_id[segment_id]
        source_excerpt = str(source_row["segment_text"])
        reference_row = reference_by_id[segment_id]
        if sha256_text(source_excerpt) != reference_row["text_sha256"]:
            raise V245FrozenSupportError("support source text hash drifted")
        density = str(source_row["density_stratum"])
        case_id = "case_" + sha256_text(f"v245|{segment_id}")[:24]
        reference_events = list(
            reference_row["golden_output"].get("discourse_events") or []
        )
        candidate_events = list(candidate_by_id[segment_id].get("events") or [])
        witnesses = []
        for origin, events in (("reference", reference_events), ("candidate", candidate_events)):
            for event_index, event in enumerate(events):
                event_hash = sha256_text(_canonical_json(event))
                witness_id = "w_" + sha256_text(
                    f"v245|{case_id}|{origin}|{event_index}|{event_hash}"
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
        raise V245FrozenSupportError("support pool witness coverage drifted")
    support_value = v143._support_input(units)
    prompt = v175.compact_support_prompt(units)
    schema = v143.support_output_schema(support_value)
    schema_text = _canonical_json(schema)
    if (
        len(prompt.encode("utf-8")) > MAX_PROMPT_BYTES
        or len(schema_text.encode("utf-8")) > MAX_SCHEMA_BYTES
    ):
        raise V245FrozenSupportError("support request size cap exceeded")
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
    projected = math.ceil(MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
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
                *v223._expected_runtime_paths(),
                *v244._runtime_files(),
                Path(__file__).resolve(),
                Path(v244.__file__).resolve(),
                Path(v223.__file__).resolve(),
                Path(v175.__file__).resolve(),
                Path(v143.__file__).resolve(),
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
    lock = _load_json(path, "v245 runtime lock")
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(value) for value in _runtime_files()}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise V245FrozenSupportError("v245 runtime lock contract drifted")
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
    if any(not _verify_record(row or {}) for row in records):
        raise V245FrozenSupportError("v245 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V245FrozenSupportError("v245 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    return {
        "root": root,
        "spec_path": spec_path,
        "spec": _load_json(spec_path, "v245 spec"),
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "support_value": _load_json(root / "support-input.private.json", "support input"),
        "origin_rows": _load_json(root / "origin-map.private.json", "origin map")["rows"],
        "prompt": (root / "support-prompt.private.md").read_text(encoding="utf-8"),
        "schema": _load_json(root / "support-schema.json", "support schema"),
    }


def freeze_v245(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v245 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V245FrozenSupportError("unfinished v245 root is not replayable")
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
            "strategy": "frozen_v174_side_free_support_over_v244_and_shared_reference",
            "support_instruction_hash": sha256_text(v143.support_base_instructions_v143()),
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
    _write_immutable(origin_path, {"schema_version": SCHEMA_VERSION, "rows": pool["origin_rows"]})
    _write_private_text(instructions_path, v143.support_base_instructions_v143())
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
        "runtime_files": [_record(path) for path in _runtime_files()],
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
        raise V245FrozenSupportError("v245 sidecar usage incomplete") from exc
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
        raise V245FrozenSupportError("v245 sidecar accounting or auth contract failed")
    return usage


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    paths = _turn_paths(root)
    attempted = int(paths["capacity"].exists())
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = attempted
    sidecar_record = None
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        try:
            sidecar = _load_json(paths["sidecar"], "v245 sidecar")
            values = sidecar.get("usage") or {}
            usage = {field: int(values[field]) for field in USAGE_FIELDS}
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
        "exact_next_action": "audit immutable judge attempt; no retry",
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v245(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v245 terminal")
    frozen = freeze_v245(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(root, frozen, V245FrozenSupportError("launch exists; replay prohibited"))
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
                base_instructions=v143.support_base_instructions_v143(),
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
            raise V245FrozenSupportError("v245 support turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v245 sidecar"))
        audit, private_score = v223.score_support_output(
            output=result.output,
            support_value=frozen["support_value"],
            origin_rows=frozen["origin_rows"],
        )
        receipts = v223._support_receipts(result.output)
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
                "v245_frozen_support_completed_alignment_authorized"
                if authorized
                else "v245_frozen_support_quality_gate_not_passed"
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
                else "reject v244 on source support and select the next distinct architecture"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v245 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run frozen support over v244")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v245(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "witness_count": frozen["spec"]["witness_count"],
        }
    else:
        terminal = asyncio.run(
            run_v245(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
            "alignment_audit_authorized": terminal.get("alignment_audit_authorized", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
