from __future__ import annotations

"""Capacity-only recovery for the frozen adoption-output alignment turns."""

import argparse
import asyncio
import hashlib
import json
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_adoption_semantic_evaluation as semantic
from . import app_server_capacity_reserve as reserve
from .util import now_iso


SCHEMA_VERSION = "pif_adoption_alignment_capacity_recovery_v1"
LOCK_VERSION = "pif_adoption_alignment_capacity_recovery_runtime_lock_v1"
TERMINAL_VERSION = "pif_adoption_alignment_capacity_recovery_terminal_v1"
PROJECT_ROOT = semantic.PROJECT_ROOT
PIPELINE_ROOT = semantic.PIPELINE_ROOT
SOURCE_ROOT = semantic.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v5_4-adoption-output-semantic-evaluation-v1-alignment-capacity-recovery-v2"
).resolve()
TURN_NAMES = semantic.ALIGNMENT_TURN_NAMES
PERMUTATIONS = semantic.ALIGNMENT_PERMUTATIONS
MODEL = semantic.ALIGNMENT_MODEL
EFFORT = semantic.ALIGNMENT_EFFORT
MAX_TOTAL_TOKENS_PER_TURN = semantic.ALIGNMENT_MAX_TOKENS
TIMEOUT_SECONDS = semantic.TIMEOUT_SECONDS
USAGE_FIELDS = semantic.USAGE_FIELDS


class AdoptionAlignmentRecoveryError(RuntimeError):
    """The frozen alignment recovery cannot proceed safely."""


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdoptionAlignmentRecoveryError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise AdoptionAlignmentRecoveryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    return semantic._record(path)


def _verify_record(record: Mapping[str, Any]) -> bool:
    return semantic._verify_record(record)


def _source_paths() -> dict[str, Path]:
    alignment = SOURCE_ROOT / "alignment"
    support = SOURCE_ROOT / "support"
    paths = {
        "source_parent_terminal": SOURCE_ROOT / "terminal.json",
        "source_alignment_terminal": alignment / "terminal.json",
        "source_alignment_launch": alignment / "launch-receipt.json",
        "source_alignment_runtime_lock": alignment / "runtime-lock.json",
        "source_support_terminal": support / "terminal.json",
        "source_support_runtime_lock": support / "runtime-lock.json",
        "source_override": SOURCE_ROOT / "operator-override-audit-v1.json",
        "source_candidate": SOURCE_ROOT / "candidate-evaluation-view.private.json",
        "source_projection_diagnostics": SOURCE_ROOT
        / "candidate-projection-diagnostics.json",
        "source_pool": alignment / "shared-witness-pool.private.json",
        "source_receipts": alignment / "support-receipts.private.json",
        "source_mapping": alignment / "origin-map.private.json",
        "source_instructions": alignment / "instructions.private.md",
    }
    for turn_name in TURN_NAMES:
        turn = semantic._turn_paths(alignment, turn_name)
        key = turn_name.replace("adoption_existing_output_", "")
        paths[f"{key}_input"] = turn["input"]
        paths[f"{key}_prompt"] = turn["prompt"]
        paths[f"{key}_schema"] = turn["schema"]
    return paths


def _source_records() -> dict[str, dict[str, Any]]:
    return {name: _record(path) for name, path in _source_paths().items()}


def _zero_usage() -> dict[str, int]:
    return {field: 0 for field in USAGE_FIELDS}


def validate_source_failure() -> dict[str, Any]:
    paths = _source_paths()
    source_lock = semantic.verify_alignment_lock(paths["source_alignment_runtime_lock"])
    parent = _load_json(paths["source_parent_terminal"], "source parent terminal")
    alignment = _load_json(
        paths["source_alignment_terminal"], "source alignment terminal"
    )
    if parent != alignment:
        raise AdoptionAlignmentRecoveryError("source terminal copies drifted")
    if (
        parent.get("state") != "inactive_incomplete_recovery_required"
        or parent.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or parent.get("error_class") != "ReserveCapacityError"
        or parent.get("attempted_turn_count") != 0
        or parent.get("unknown_usage_turn_count") != 0
        or parent.get("semantic_retry_count") != 0
        or parent.get("usage_status") != "complete"
        or parent.get("accounting_complete") is not True
        or parent.get("usage") != _zero_usage()
        or parent.get("sidecars") != []
        or parent.get("development_quality_passed") is not False
        or parent.get("holdout_authorized") is not False
        or parent.get("production_mutated") is not False
        or parent.get("runtime_lock") != _record(paths["source_alignment_runtime_lock"])
    ):
        raise AdoptionAlignmentRecoveryError(
            "source failure is not the frozen zero-turn capacity stop"
        )
    support = _load_json(paths["source_support_terminal"], "source support terminal")
    if (
        support.get("terminal_reason")
        != "adoption_existing_output_support_completed_alignment_authorized"
        or support.get("alignment_audit_authorized") is not True
        or support.get("usage_status") != "complete"
        or support.get("accounting_complete") is not True
        or support.get("semantic_retry_count") != 0
        or support.get("production_mutated") is not False
    ):
        raise AdoptionAlignmentRecoveryError("source support is not reusable")
    frozen_request_records = {
        (str(row["path"]), str(row["sha256"]), int(row["size_bytes"]))
        for row in source_lock.get("frozen_requests") or []
    }
    observed_request_records = set()
    for turn_name in TURN_NAMES:
        turn = semantic._turn_paths(SOURCE_ROOT / "alignment", turn_name)
        for field in ("input", "prompt", "schema"):
            row = _record(turn[field])
            observed_request_records.add(
                (str(row["path"]), str(row["sha256"]), int(row["size_bytes"]))
            )
        for forbidden in ("capacity", "sidecar", "output", "normalized"):
            if turn[forbidden].exists():
                raise AdoptionAlignmentRecoveryError(
                    "source alignment turn artifact exists despite zero attempts"
                )
    if observed_request_records != frozen_request_records:
        raise AdoptionAlignmentRecoveryError("source frozen request records drifted")
    if (SOURCE_ROOT / "development-winner.json").exists():
        raise AdoptionAlignmentRecoveryError(
            "source capacity stop unexpectedly froze a development winner"
        )
    predecessors = source_lock.get("predecessor_manifests") or []
    semantic._verify_predecessor_manifests(predecessors)
    return {
        "source_lock": source_lock,
        "parent_terminal": parent,
        "support_terminal": support,
        "records": _source_records(),
        "predecessor_manifests": predecessors,
    }


def _bindings(source: Mapping[str, Any]) -> dict[str, Any]:
    paths = _source_paths()
    turns = []
    for turn_name, permutation in zip(TURN_NAMES, PERMUTATIONS):
        turn = semantic._turn_paths(SOURCE_ROOT / "alignment", turn_name)
        turns.append(
            {
                "turn_name": turn_name,
                "permutation": permutation,
                "input": _record(turn["input"]),
                "prompt": _record(turn["prompt"]),
                "schema": _record(turn["schema"]),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "source_root": str(SOURCE_ROOT),
        "source_alignment_runtime_lock": source["records"][
            "source_alignment_runtime_lock"
        ],
        "source_zero_turn_terminal": source["records"]["source_parent_terminal"],
        "source_support_terminal": source["records"]["source_support_terminal"],
        "instructions": _record(paths["source_instructions"]),
        "mapping": _record(paths["source_mapping"]),
        "turns": turns,
        "request_bytes_changed": False,
        "support_replayed": False,
        "extraction_replayed": False,
        "semantic_repair_performed": False,
    }


def _binding_records(binding: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = [
        binding["source_alignment_runtime_lock"],
        binding["source_zero_turn_terminal"],
        binding["source_support_terminal"],
        binding["instructions"],
        binding["mapping"],
    ]
    for turn in binding["turns"]:
        rows.extend(turn[field] for field in ("input", "prompt", "schema"))
    return rows


def freeze_recovery(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    alignment_root = root / "alignment"
    lock_path = alignment_root / "runtime-lock.json"
    if lock_path.is_file():
        verify_recovery_lock(lock_path)
        return _load_frozen(root)
    if root.exists() and any(root.iterdir()):
        raise AdoptionAlignmentRecoveryError("recovery root is not empty")
    root.mkdir(parents=True, exist_ok=True)
    source = validate_source_failure()
    alignment_root.mkdir(parents=True, exist_ok=True)
    for turn_name in TURN_NAMES:
        semantic._turn_paths(alignment_root, turn_name)["root"].mkdir(
            parents=True, exist_ok=True
        )
    binding_path = alignment_root / "request-bindings.json"
    _write_immutable(binding_path, _bindings(source))
    spec_path = alignment_root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "state": "frozen_before_two_turn_alignment_capacity_recovery",
            "recovery_scope": "capacity_only_after_zero_semantic_turns",
            "declared_turn_count": 2,
            "turn_names": list(TURN_NAMES),
            "permutations": list(PERMUTATIONS),
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "source_support_reused": True,
            "source_support_replayed": False,
            "source_extraction_replayed": False,
            "request_bytes_changed": False,
            "frozen_quality_threshold": semantic.QUALITY_THRESHOLD,
            "holdout_authorized_before_score": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    failure_audit_path = alignment_root / "source-capacity-stop-audit.json"
    _write_stable_time(
        failure_audit_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "classification": "presemantic_external_capacity_policy_stop",
            "source_terminal": source["records"]["source_parent_terminal"],
            "source_attempted_turn_count": 0,
            "source_unknown_usage_turn_count": 0,
            "source_usage": _zero_usage(),
            "source_request_replay_performed": False,
            "production_mutated": False,
        },
        "created_at",
    )
    capacity = semantic._capacity_policy(
        alignment_root,
        phase_id="adoption_existing_output_neutral_alignment_capacity_recovery_v2",
        turn_names=TURN_NAMES,
        maximum_total_tokens_per_turn=MAX_TOTAL_TOKENS_PER_TURN,
    )
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": "adoption_existing_output_neutral_alignment_capacity_recovery_v2",
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 2,
        "retry_count": 0,
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
        "frozen_quality_threshold": semantic.QUALITY_THRESHOLD,
        "production_amortized_total_token_ratio": semantic.PRODUCTION_TOKEN_RATIO,
        "runtime_adapter": _record(Path(__file__).resolve()),
        "base_semantic_adapter": _record(Path(semantic.__file__).resolve()),
        "pinned_codex_cli": _record(semantic.adoption.PINNED_CODEX),
        "predecessor_manifests": source["predecessor_manifests"],
        "source_records": source["records"],
        "request_bindings": _record(binding_path),
        "attempt_spec": _record(spec_path),
        "source_failure_audit": _record(failure_audit_path),
        "capacity_audit": _record(capacity["audit"]),
        "capacity_policy": _record(capacity["policy"]),
        "support_replayed": False,
        "extraction_replayed": False,
        "request_bytes_changed": False,
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_recovery_lock(lock_path)
    return _load_frozen(root)


def verify_recovery_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "recovery runtime lock")
    alignment_root = path.parent.resolve()
    if (
        lock.get("schema_version") != LOCK_VERSION
        or lock.get("phase_id")
        != "adoption_existing_output_neutral_alignment_capacity_recovery_v2"
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 2
        or lock.get("retry_count") != 0
        or lock.get("maximum_total_tokens_per_turn") != MAX_TOTAL_TOKENS_PER_TURN
        or lock.get("frozen_quality_threshold") != semantic.QUALITY_THRESHOLD
        or lock.get("production_amortized_total_token_ratio")
        != semantic.PRODUCTION_TOKEN_RATIO
        or lock.get("runtime_adapter") != _record(Path(__file__).resolve())
        or lock.get("base_semantic_adapter")
        != _record(Path(semantic.__file__).resolve())
        or lock.get("pinned_codex_cli") != _record(semantic.adoption.PINNED_CODEX)
        or lock.get("support_replayed") is not False
        or lock.get("extraction_replayed") is not False
        or lock.get("request_bytes_changed") is not False
        or lock.get("holdout_authorized_before_score") is not False
        or lock.get("production_mutation_allowed") is not False
    ):
        raise AdoptionAlignmentRecoveryError("recovery runtime lock contract drifted")
    source = validate_source_failure()
    if lock.get("predecessor_manifests") != source["predecessor_manifests"]:
        raise AdoptionAlignmentRecoveryError("recovery predecessor manifests drifted")
    if lock.get("source_records") != source["records"]:
        raise AdoptionAlignmentRecoveryError("recovery source records drifted")
    semantic._verify_predecessor_manifests(lock["predecessor_manifests"])
    binding_path = alignment_root / "request-bindings.json"
    binding = _load_json(binding_path, "request bindings")
    if (
        lock.get("request_bindings") != _record(binding_path)
        or binding.get("request_bytes_changed") is not False
        or binding.get("support_replayed") is not False
        or binding.get("extraction_replayed") is not False
        or binding.get("semantic_repair_performed") is not False
        or any(not _verify_record(row) for row in _binding_records(binding))
    ):
        raise AdoptionAlignmentRecoveryError("recovery request bindings drifted")
    for field in (
        "attempt_spec",
        "source_failure_audit",
        "capacity_audit",
        "capacity_policy",
    ):
        if not _verify_record(lock[field]):
            raise AdoptionAlignmentRecoveryError(f"recovery {field} drifted")
    policy_path = Path(str(lock["capacity_policy"]["path"])).resolve()
    if policy_path.parent != alignment_root:
        raise AdoptionAlignmentRecoveryError("recovery capacity policy root drifted")
    policy = reserve.load_reserve_capacity_policy(policy_path)
    if (
        Path(str(policy["semantic_output_root"])).resolve() != alignment_root
        or tuple(policy["ordered_turn_names"]) != tuple(TURN_NAMES)
    ):
        raise AdoptionAlignmentRecoveryError("recovery capacity path contract drifted")
    for turn_name in TURN_NAMES:
        capacity_path = semantic._turn_paths(alignment_root, turn_name)["capacity"]
        expected = (
            alignment_root
            / "turns"
            / turn_name.replace("_", "-")
            / "capacity.json"
        )
        if capacity_path != expected:
            raise AdoptionAlignmentRecoveryError("recovery turn path contract drifted")
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    alignment_root = root / "alignment"
    binding = _load_json(alignment_root / "request-bindings.json", "request bindings")
    turns = []
    for binding_turn in binding["turns"]:
        turn_name = str(binding_turn["turn_name"])
        source_paths = semantic._turn_paths(SOURCE_ROOT / "alignment", turn_name)
        turns.append(
            {
                "turn_name": turn_name,
                "permutation": binding_turn["permutation"],
                "value": _load_json(source_paths["input"], f"{turn_name} input"),
                "prompt": source_paths["prompt"].read_text(encoding="utf-8"),
                "schema": _load_json(source_paths["schema"], f"{turn_name} schema"),
                "target_paths": semantic._turn_paths(alignment_root, turn_name),
            }
        )
    return {
        "root": root,
        "alignment_root": alignment_root,
        "runtime_lock": alignment_root / "runtime-lock.json",
        "capacity_policy": alignment_root / "capacity-policy.json",
        "instructions": _source_paths()["source_instructions"].read_text(
            encoding="utf-8"
        ),
        "mapping": _load_json(_source_paths()["source_mapping"], "source mapping"),
        "turns": turns,
    }


def _failure_terminal(
    *, root: Path, exc: BaseException, runtime_lock: Path
) -> dict[str, Any]:
    alignment_root = root / "alignment"
    attempted = unknown = 0
    usage = _zero_usage()
    sidecars = []
    for turn_name in TURN_NAMES:
        paths = semantic._turn_paths(alignment_root, turn_name)
        if not paths["capacity"].exists() and not paths["sidecar"].exists():
            continue
        attempted += 1
        if paths["sidecar"].is_file():
            sidecars.append(_record(paths["sidecar"]))
            try:
                row_usage = semantic._usage(
                    paths["sidecar"],
                    model=MODEL,
                    effort=EFFORT,
                    maximum_total_tokens=MAX_TOTAL_TOKENS_PER_TURN,
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
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "source_presemantic_attempted_turn_count": 0,
        "recovery_attempted_turn_count": attempted,
        "recovery_unknown_usage_turn_count": unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "recovery_usage": usage,
        "sidecars": sidecars,
        "support_replayed": False,
        "extraction_replayed": False,
        "development_quality_passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(runtime_lock),
    }
    _write_stable_time(alignment_root / "terminal.json", terminal, "terminal_at")
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_recovery(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = semantic._client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "recovery terminal")
    frozen = freeze_recovery(output_dir=root)
    alignment_root = frozen["alignment_root"]
    verify_recovery_lock(frozen["runtime_lock"])
    launch_path = alignment_root / "launch-receipt.json"
    if launch_path.exists():
        return _failure_terminal(
            root=root,
            exc=AdoptionAlignmentRecoveryError(
                "alignment recovery launch exists; replay prohibited"
            ),
            runtime_lock=frozen["runtime_lock"],
        )
    _write_stable_time(
        launch_path,
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "declared_turn_count": 2,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "source_support_reused": True,
            "source_support_replayed": False,
            "source_extraction_replayed": False,
            "request_bytes_changed": False,
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
                paths = turn["target_paths"]
                result = await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
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
                    raise AdoptionAlignmentRecoveryError(
                        f"alignment recovery turn did not complete: {turn['turn_name']}"
                    )
                usages.append(
                    semantic._usage(
                        paths["sidecar"],
                        model=MODEL,
                        effort=EFFORT,
                        maximum_total_tokens=MAX_TOTAL_TOKENS_PER_TURN,
                    )
                )
                errors = semantic.judge.validate_neutral_alignment_output(
                    result.output, turn["value"]
                )
                if errors:
                    raise AdoptionAlignmentRecoveryError(
                        "post-return neutral alignment validation failed: "
                        + "; ".join(sorted(set(errors)))
                    )
                projected = semantic.judge.normalize_neutral_alignment_output(
                    result.output, turn["value"]
                )
                _write_immutable(paths["normalized"], projected)
                normalized.append(projected)
        score = semantic.score_alignment(
            base=normalized[0], canary=normalized[1], mapping=frozen["mapping"]
        )
        score_path = alignment_root / "alignment-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        winner_path = root / "development-winner.json"
        winner_record = None
        if passed:
            winner = semantic._winner(SOURCE_ROOT, score_path)
            winner["schema_version"] = "pif_adoption_semantic_development_winner_v2"
            winner["configuration"]["alignment_capacity_recovery_runtime_lock"] = _record(
                frozen["runtime_lock"]
            )
            winner["development_evidence"]["source_support_terminal"] = _record(
                SOURCE_ROOT / "support" / "terminal.json"
            )
            _write_stable_time(winner_path, winner, "frozen_at")
            winner_record = _record(winner_path)
        alignment_usage = {
            field: sum(row[field] for row in usages) for field in USAGE_FIELDS
        }
        support_terminal = _load_json(
            SOURCE_ROOT / "support" / "terminal.json", "source support terminal"
        )
        support_usage = support_terminal["usage"]
        total_judge_usage = {
            field: int(support_usage[field]) + alignment_usage[field]
            for field in USAGE_FIELDS
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
            "source_presemantic_attempted_turn_count": 0,
            "recovery_attempted_turn_count": 2,
            "measured_alignment_turn_count": 2,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "support_usage": support_usage,
            "alignment_usage": alignment_usage,
            "total_judge_usage": total_judge_usage,
            "support_replayed": False,
            "extraction_replayed": False,
            "development_quality_passed": passed,
            "development_winner_frozen": passed,
            "holdout_authorized": passed,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "production_amortized_total_token_ratio": semantic.PRODUCTION_TOKEN_RATIO,
            "wall_seconds": round(time.monotonic() - started, 6),
            "score": _record(score_path),
            "development_winner": winner_record,
            "source_support_terminal": _record(
                SOURCE_ROOT / "support" / "terminal.json"
            ),
            "sidecars": [
                _record(turn["target_paths"]["sidecar"])
                for turn in frozen["turns"]
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
            return _load_json(alignment_root / "terminal.json", "recovery terminal")
        return _failure_terminal(root=root, exc=exc, runtime_lock=frozen["runtime_lock"])


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Recover only the frozen adoption-output alignment turns after capacity"
    )
    parser.add_argument("action", choices=("freeze", "verify", "run"))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    root = Path(args.output_dir)
    if args.action == "freeze":
        frozen = freeze_recovery(output_dir=root)
        result = {
            "state": "frozen_before_two_turn_alignment_capacity_recovery",
            "root": str(frozen["root"]),
            "turn_count": len(frozen["turns"]),
        }
    elif args.action == "verify":
        verify_recovery_lock(root / "alignment" / "runtime-lock.json")
        result = {"state": "verified", "root": str(root)}
    else:
        result = asyncio.run(
            run_recovery(output_dir=root, timeout_seconds=args.timeout_seconds)
        )
    sanitized = {
        key: result.get(key)
        for key in (
            "state",
            "terminal_reason",
            "usage_status",
            "development_quality_passed",
            "development_winner_frozen",
            "holdout_authorized",
            "production_mutated",
            "root",
            "turn_count",
        )
        if key in result
    }
    print(json.dumps(sanitized, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
