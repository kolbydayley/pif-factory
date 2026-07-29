from __future__ import annotations

"""Runtime lock for reference-bound judge-v5 five-arm selection."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_capacity_probe import LAUNCH_CAPACITY_VERSION
from .app_server_judge_v5_diagnostic import _record, _sha256_file, _write_immutable_json
from .app_server_judge_v5_selection import (
    DEFAULT_CONTINUATION_TERMINAL,
    DEFAULT_FRESH_CALIBRATION_ROOT,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_REUSE_CONTRACT,
    verify_fresh_calibration_for_selection,
)
from .app_server_runtime_lock_v17 import (
    CURRENT_RUNTIME_FILES as V17_RUNTIME_FILES,
    DEFAULT_CONTROL_ROOT as V17_CONTROL_ROOT,
    DEFAULT_MANIFEST as V17_MANIFEST,
    verify_runtime_lock_v17,
)
from .app_server_v5_reuse import verify_v5_reuse_contract
from .util import now_iso


RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v18"
WAIT_INTENT_VERSION = "pif_app_server_calibration_to_selection_wait_intent_v18"
LAUNCH_RECEIPT_VERSION = "pif_app_server_five_arm_selection_launch_receipt_v18"
SELECTION_RUNTIME_FILES = (
    "research_factory/app_server_capacity_probe.py",
    "research_factory/app_server_checkpoint.py",
    "research_factory/app_server_interrupted_arm_recovery.py",
    "research_factory/app_server_evaluation.py",
    "research_factory/app_server_dev_selection.py",
    "research_factory/app_server_holdout.py",
    "research_factory/efficient_backtest.py",
    "research_factory/labels.py",
    "research_factory/paths.py",
    "research_factory/worker.py",
    "research_factory/db.py",
    "research_factory/app_server_judge_v5_selection.py",
    "research_factory/app_server_runtime_lock_v18.py",
    "research_factory/app_server_judge_v5_selection_continuation.py",
    "automation/resume-app-server-evaluation-v18.sh",
    "label_packs/ai_discourse_v3_1/prompt.md",
    "label_packs/ai_discourse_v3_1/schema.json",
    "label_packs/ai_discourse_v3_1/codebook.md",
    "research_factory/prompt_guidelines/windowed_event_core_v1.json",
    "work/app-server-development-v2/run-spec-v2.json",
    "work/app-server-development-v2/manifest.json",
)
CURRENT_RUNTIME_FILES = tuple(dict.fromkeys(V17_RUNTIME_FILES + SELECTION_RUNTIME_FILES))
DEFAULT_CONTROL_ROOT = Path(
    "work/app-server-development-v2/unattended-control-v18"
).resolve()
DEFAULT_MANIFEST = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v18.json"
).resolve()
DEFAULT_WAIT_INTENT = DEFAULT_CONTROL_ROOT / "wait-intent-v18.json"
DEFAULT_LAUNCH_CAPACITY = DEFAULT_CONTROL_ROOT / "prelaunch-capacity.json"
DEFAULT_LAUNCH_RECEIPT = DEFAULT_CONTROL_ROOT / "launch-receipt-v18.json"


class RuntimeLockV18Error(RuntimeError):
    """The calibration-to-selection runtime boundary is unsafe."""


def _load(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeLockV18Error(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise RuntimeLockV18Error(f"{purpose} is not an object")
    return value


def _repo_record(repo_root: Path, relative: str) -> dict[str, Any]:
    path = (repo_root / relative).resolve()
    try:
        path.relative_to(repo_root)
    except ValueError as exc:
        raise RuntimeLockV18Error("runtime file escapes repository") from exc
    if not path.is_file():
        raise RuntimeLockV18Error(f"runtime file is missing: {relative}")
    return {
        "path": relative,
        "sha256": _sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _verify_record(record: Any, *, expected_path: Optional[Path] = None) -> Path:
    if not isinstance(record, Mapping):
        raise RuntimeLockV18Error("runtime-lock record is malformed")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if expected_path is not None and path != expected_path.expanduser().resolve():
        raise RuntimeLockV18Error("runtime-lock record path drifted")
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise RuntimeLockV18Error("runtime-lock record content drifted")
    return path


def _validate_launch_capacity(path: Path) -> dict[str, Any]:
    value = _load(path, "selection launch capacity")
    used = value.get("primary_used_percent")
    if (
        value.get("schema_version") != LAUNCH_CAPACITY_VERSION
        or value.get("managed_chatgpt_auth_verified") is not True
        or value.get("plan_type") != "pro"
        or value.get("maximum_primary_used_percent") != 20
        or value.get("cleared_for_semantic_work") is not True
        or value.get("thread_started") is not False
        or value.get("turn_started") is not False
        or isinstance(used, bool)
        or not isinstance(used, (int, float))
        or used > 20
    ):
        raise RuntimeLockV18Error("selection launch capacity is unsafe")
    return value


def prepare_wait_intent(
    *,
    repo_root: Path,
    output_path: Path = DEFAULT_WAIT_INTENT,
    selection_root: Path = DEFAULT_OUTPUT_ROOT,
    calibration_continuation_terminal: Path = DEFAULT_CONTINUATION_TERMINAL,
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    target = output_path.expanduser().resolve()
    if target.exists():
        return verify_wait_intent(
            repo_root=root,
            path=target,
            expected_selection_root=selection_root,
            expected_calibration_terminal=calibration_continuation_terminal,
        )
    selection = selection_root.expanduser().resolve()
    predecessor_terminal = calibration_continuation_terminal.expanduser().resolve()
    if selection.exists():
        raise RuntimeLockV18Error("selection root exists before v18 wait intent")
    records = {
        "runtime_lock_v16": _record(
            root / "work/app-server-development-v2/unattended-runtime-lock-v16.json"
        ),
        "wait_intent_v17": _record(
            root
            / "work/app-server-development-v2/unattended-control-v17/wait-intent-v17.json"
        ),
        "reuse_contract_v4": _record(
            root / DEFAULT_REUSE_CONTRACT.relative_to(Path.cwd())
        ),
        "selection_adapter": _record(
            root / "research_factory/app_server_judge_v5_selection.py"
        ),
        "runtime_lock_builder": _record(
            root / "research_factory/app_server_runtime_lock_v18.py"
        ),
        "selection_continuation": _record(
            root / "research_factory/app_server_judge_v5_selection_continuation.py"
        ),
        "launcher": _record(root / "automation/resume-app-server-evaluation-v18.sh"),
    }
    payload = {
        "schema_version": WAIT_INTENT_VERSION,
        "created_at": now_iso(),
        "status": "waiting_for_fresh_calibration_selection_authorization",
        "fresh_calibration_terminal_path": str(
            predecessor_terminal
        ),
        "fresh_calibration_terminal_absent": not predecessor_terminal.exists(),
        "selection_root": str(selection),
        "selection_root_absent": True,
        "semantic_attempt_started": False,
        "records": records,
        "policy": {
            "managed_chatgpt_auth_only": True,
            "maximum_primary_used_percent": 20,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "retry_count_per_turn": 0,
            "fresh_calibration_must_pass_every_frozen_gate": True,
            "five_clean_arms_reused_without_extraction": True,
            "interrupted_batch_5_same_thread_nonselectable": True,
            "old_judge_outputs_reused": False,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "extraction_model_calls_allowed": False,
            "production_mutation_allowed": False,
        },
    }
    _write_immutable_json(target, payload)
    return verify_wait_intent(
        repo_root=root,
        path=target,
        expected_selection_root=selection,
        expected_calibration_terminal=predecessor_terminal,
    )


def verify_wait_intent(
    *,
    repo_root: Path,
    path: Path,
    expected_selection_root: Path = DEFAULT_OUTPUT_ROOT,
    expected_calibration_terminal: Path = DEFAULT_CONTINUATION_TERMINAL,
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    value = _load(path.expanduser().resolve(), "v18 wait intent")
    expected_policy = {
        "managed_chatgpt_auth_only": True,
        "maximum_primary_used_percent": 20,
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "retry_count_per_turn": 0,
        "fresh_calibration_must_pass_every_frozen_gate": True,
        "five_clean_arms_reused_without_extraction": True,
        "interrupted_batch_5_same_thread_nonselectable": True,
        "old_judge_outputs_reused": False,
        "api_key_billing_allowed": False,
        "raw_session_token_access_allowed": False,
        "codex_exec_semantic_calls_allowed": False,
        "extraction_model_calls_allowed": False,
        "production_mutation_allowed": False,
    }
    if (
        value.get("schema_version") != WAIT_INTENT_VERSION
        or value.get("status")
        != "waiting_for_fresh_calibration_selection_authorization"
        or value.get("selection_root_absent") is not True
        or value.get("semantic_attempt_started") is not False
        or value.get("policy") != expected_policy
    ):
        raise RuntimeLockV18Error("v18 wait intent drifted")
    records = value.get("records")
    expected_records = {
        "runtime_lock_v16",
        "wait_intent_v17",
        "reuse_contract_v4",
        "selection_adapter",
        "runtime_lock_builder",
        "selection_continuation",
        "launcher",
    }
    if not isinstance(records, Mapping) or set(records) != expected_records:
        raise RuntimeLockV18Error("v18 wait-intent coverage drifted")
    for record in records.values():
        _verify_record(record)
    expected_selection = expected_selection_root.expanduser().resolve()
    if Path(str(value.get("selection_root") or "")).resolve() != expected_selection:
        raise RuntimeLockV18Error("v18 wait-intent selection path drifted")
    expected_terminal = expected_calibration_terminal.expanduser().resolve()
    if (
        Path(str(value.get("fresh_calibration_terminal_path") or "")).resolve()
        != expected_terminal
    ):
        raise RuntimeLockV18Error("v18 wait-intent calibration terminal path drifted")
    return value


def _superseded_artifacts(
    *,
    repo_root: Path,
    fresh: Mapping[str, Any],
    contract: Mapping[str, Any],
    wait_intent_path: Path,
    launch_capacity_path: Path,
    reuse_contract_path: Path,
) -> list[dict[str, Any]]:
    root = repo_root.expanduser().resolve()
    rows = [
        {"label": "runtime_lock_v17", **_record(root / V17_MANIFEST.relative_to(Path.cwd()))},
        {"label": "wait_intent_v17", **_record(root / V17_CONTROL_ROOT.relative_to(Path.cwd()) / "wait-intent-v17.json")},
        {"label": "launch_capacity_v17", **_record(root / V17_CONTROL_ROOT.relative_to(Path.cwd()) / "prelaunch-capacity.json")},
        {"label": "launch_receipt_v17", **_record(root / V17_CONTROL_ROOT.relative_to(Path.cwd()) / "launch-receipt-v17.json")},
        {"label": "continuation_terminal_v17", **fresh["records"]["continuation_terminal"]},
        {"label": "fresh_outer_terminal", **fresh["records"]["outer_terminal"]},
        {"label": "fresh_inner_terminal", **fresh["records"]["inner_terminal"]},
        {"label": "fresh_inner_truth", **fresh["records"]["inner_truth"]},
        {"label": "fresh_score", **fresh["records"]["score"]},
        {"label": "fresh_spec", **fresh["records"]["fresh_spec"]},
        {"label": "fresh_inner_spec", **fresh["records"]["inner_spec"]},
        {"label": "wait_intent_v18", **_record(wait_intent_path)},
        {"label": "launch_capacity_v18", **_record(launch_capacity_path)},
        {"label": "reuse_contract_v4", **_record(reuse_contract_path)},
        {"label": "interrupted_batch_5_same_thread", **contract["interrupted_batch_5_same_thread"]},
    ]
    for key, record in sorted(contract["selection_inputs"].items()):
        rows.append({"label": f"selection_input_{key}", **record})
    for arm in contract["clean_arms"]:
        rows.append(
            {
                "label": f"clean_arm_batch_{arm['batch_size']}_{arm['thread_mode']}",
                **arm["report"],
            }
        )
    for attempt in fresh["terminal"]["attempts"]:
        for kind in ("capacity", "output", "sidecar"):
            rows.append(
                {"label": f"fresh_{attempt['turn_name']}_{kind}", **attempt[kind]}
            )
        turn_root = Path(str(attempt["sidecar"]["path"])).expanduser().resolve().parent
        for kind, filename in (
            ("input", "input.private.json"),
            ("prompt", "prompt.private.md"),
            ("schema", "schema.json"),
        ):
            rows.append(
                {
                    "label": f"fresh_{attempt['turn_name']}_{kind}",
                    **_record(turn_root / filename),
                }
            )
    labels = [row["label"] for row in rows]
    if len(labels) != len(set(labels)):
        raise RuntimeLockV18Error("runtime-lock predecessor labels overlap")
    return rows


def build_runtime_lock_v18(
    *,
    repo_root: Path,
    manifest_path: Path = DEFAULT_MANIFEST,
    wait_intent_path: Path = DEFAULT_WAIT_INTENT,
    launch_capacity_path: Path = DEFAULT_LAUNCH_CAPACITY,
    calibration_root: Path = DEFAULT_FRESH_CALIBRATION_ROOT,
    continuation_terminal_path: Path = DEFAULT_CONTINUATION_TERMINAL,
    reuse_contract_path: Path = DEFAULT_REUSE_CONTRACT,
    selection_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    if manifest.exists():
        return verify_runtime_lock_v18(repo_root=root, manifest_path=manifest)
    verify_wait_intent(
        repo_root=root,
        path=wait_intent_path,
        expected_selection_root=selection_root,
        expected_calibration_terminal=continuation_terminal_path,
    )
    verify_runtime_lock_v17(
        repo_root=root,
        manifest_path=(root / V17_MANIFEST.relative_to(Path.cwd())).resolve(),
    )
    _validate_launch_capacity(launch_capacity_path.expanduser().resolve())
    fresh = verify_fresh_calibration_for_selection(
        calibration_root=calibration_root,
        continuation_terminal_path=continuation_terminal_path,
    )
    contract = verify_v5_reuse_contract(reuse_contract_path.expanduser().resolve())
    if selection_root.expanduser().resolve().exists():
        raise RuntimeLockV18Error("selection root exists before v18 runtime lock")
    artifacts = _superseded_artifacts(
        repo_root=root,
        fresh=fresh,
        contract=contract,
        wait_intent_path=wait_intent_path.expanduser().resolve(),
        launch_capacity_path=launch_capacity_path.expanduser().resolve(),
        reuse_contract_path=reuse_contract_path.expanduser().resolve(),
    )
    payload = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": "Run one reference-bound judge-v5 attempt over five clean development arms.",
        "supersedes": {
            "prior_runtime_lock_version": "pif_app_server_unattended_runtime_lock_v17",
            "fresh_calibration_terminal_reason": fresh["terminal"]["terminal_reason"],
            "fresh_calibration_passed": True,
            "fresh_calibration_turn_count": fresh["terminal"]["turn_count"],
            "fresh_calibration_retry_count": 0,
            "clean_arm_count": len(contract["clean_arms"]),
            "interrupted_batch_5_same_thread_nonselectable": True,
            "artifacts": artifacts,
        },
        "policy": {
            "execution_purpose": "reference_bound_five_arm_development_selection",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "timeout_seconds": 1200.0,
            "case_count": 32,
            "witness_count": 1960,
            "pointwise_turn_count": 28,
            "alignment_cases_per_turn_maximum": 4,
            "permutation_canary_case_count": 8,
            "minimum_turn_count": 38,
            "maximum_turn_count": 69,
            "adjudication_call_cap": 1,
            "retry_count_per_turn": 0,
            "managed_chatgpt_auth_only": True,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "official_persistent_codex_app_server_only": True,
            "semantic_turn_maximum_primary_used_percent": 20,
            "per_turn_capacity_checkpoint_required": True,
            "fresh_calibration_output_reused_only_as_gate": True,
            "old_selection_judge_outputs_reused": False,
            "extraction_model_calls_allowed": False,
            "batch_5_same_thread_replay_allowed": False,
            "production_mutation_allowed": False,
        },
        "files": [_repo_record(root, relative) for relative in CURRENT_RUNTIME_FILES],
    }
    _write_immutable_json(manifest, payload)
    return verify_runtime_lock_v18(repo_root=root, manifest_path=manifest)


def verify_runtime_lock_v18(
    *, repo_root: Path, manifest_path: Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    value = _load(manifest_path.expanduser().resolve(), "runtime lock v18")
    policy = value.get("policy") or {}
    if (
        value.get("schema_version") != RUNTIME_LOCK_VERSION
        or policy.get("execution_purpose")
        != "reference_bound_five_arm_development_selection"
        or policy.get("retry_count_per_turn") != 0
        or policy.get("managed_chatgpt_auth_only") is not True
        or policy.get("extraction_model_calls_allowed") is not False
        or policy.get("batch_5_same_thread_replay_allowed") is not False
        or policy.get("production_mutation_allowed") is not False
    ):
        raise RuntimeLockV18Error("runtime lock v18 policy drifted")
    files = value.get("files")
    if not isinstance(files, list) or len(files) != len(CURRENT_RUNTIME_FILES):
        raise RuntimeLockV18Error("runtime lock v18 file coverage drifted")
    by_path = {str(row.get("path") or ""): row for row in files if isinstance(row, Mapping)}
    if set(by_path) != set(CURRENT_RUNTIME_FILES):
        raise RuntimeLockV18Error("runtime lock v18 file identities drifted")
    for relative in CURRENT_RUNTIME_FILES:
        record = by_path[relative]
        path = (root / relative).resolve()
        if (
            not path.is_file()
            or record.get("sha256") != _sha256_file(path)
            or record.get("size_bytes") != path.stat().st_size
        ):
            raise RuntimeLockV18Error(f"runtime file drifted: {relative}")
    artifacts = (value.get("supersedes") or {}).get("artifacts")
    turn_count = (value.get("supersedes") or {}).get("fresh_calibration_turn_count")
    if (
        not isinstance(artifacts, list)
        or turn_count not in {23, 24}
        or len(artifacts) != 27 + 6 * turn_count
    ):
        raise RuntimeLockV18Error("runtime lock v18 predecessor coverage drifted")
    labels = [str(row.get("label") or "") for row in artifacts]
    if len(labels) != len(set(labels)):
        raise RuntimeLockV18Error("runtime lock v18 predecessor labels drifted")
    for artifact in artifacts:
        _verify_record(artifact)
    return value


def write_launch_receipt(
    *,
    output_path: Path,
    manifest_path: Path,
    capacity_path: Path,
    supervisor_pid: int,
) -> dict[str, Any]:
    target = output_path.expanduser().resolve()
    if target.exists():
        value = _load(target, "v18 launch receipt")
        if (
            value.get("schema_version") != LAUNCH_RECEIPT_VERSION
            or value.get("status") != "five_arm_selection_declared_before_semantic_calls"
            or value.get("semantic_attempt_started") is not False
            or value.get("retry_count_per_turn") != 0
            or value.get("production_mutation_performed") is not False
        ):
            raise RuntimeLockV18Error("v18 launch receipt drifted")
        _verify_record(value.get("runtime_lock"), expected_path=manifest_path)
        _verify_record(value.get("launch_capacity"), expected_path=capacity_path)
        return value
    payload = {
        "schema_version": LAUNCH_RECEIPT_VERSION,
        "created_at": now_iso(),
        "status": "five_arm_selection_declared_before_semantic_calls",
        "supervisor_pid": supervisor_pid,
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "semantic_attempt_started": False,
        "runtime_lock": _record(manifest_path),
        "launch_capacity": _record(capacity_path),
        "retry_count_per_turn": 0,
        "extraction_model_calls_allowed": False,
        "production_mutation_performed": False,
    }
    _write_immutable_json(target, payload)
    return payload


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Manage runtime-lock v18")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--wait-intent", default=str(DEFAULT_WAIT_INTENT))
    parser.add_argument("--launch-capacity", default=str(DEFAULT_LAUNCH_CAPACITY))
    parser.add_argument("--calibration-root", default=str(DEFAULT_FRESH_CALIBRATION_ROOT))
    parser.add_argument("--continuation-terminal", default=str(DEFAULT_CONTINUATION_TERMINAL))
    parser.add_argument("--reuse-contract", default=str(DEFAULT_REUSE_CONTRACT))
    parser.add_argument("--selection-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--prepare-wait-intent", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)
    root = Path(args.repo_root)
    if args.prepare_wait_intent:
        value = prepare_wait_intent(
            repo_root=root,
            output_path=Path(args.wait_intent),
            selection_root=Path(args.selection_root),
            calibration_continuation_terminal=Path(args.continuation_terminal),
        )
    elif args.verify_only:
        value = verify_runtime_lock_v18(repo_root=root, manifest_path=Path(args.manifest))
    else:
        value = build_runtime_lock_v18(
            repo_root=root,
            manifest_path=Path(args.manifest),
            wait_intent_path=Path(args.wait_intent),
            launch_capacity_path=Path(args.launch_capacity),
            calibration_root=Path(args.calibration_root),
            continuation_terminal_path=Path(args.continuation_terminal),
            reuse_contract_path=Path(args.reuse_contract),
            selection_root=Path(args.selection_root),
        )
    print(json.dumps({"ok": True, "schema_version": value["schema_version"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
