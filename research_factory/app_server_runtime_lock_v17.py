from __future__ import annotations

"""Runtime lock for reference-bound pipeline-v5 fresh calibration."""

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_capacity_probe import LAUNCH_CAPACITY_VERSION
from .app_server_judge_v5_diagnostic import _record, _sha256_file, _write_immutable_json
from .app_server_judge_v5_fresh_calibration import load_frozen_fixture_reference
from .app_server_runtime_lock import verify_runtime_lock
from .app_server_runtime_lock_v16 import CURRENT_RUNTIME_FILES as V16_RUNTIME_FILES
from .util import now_iso


RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v17"
WAIT_INTENT_VERSION = "pif_app_server_reference_to_calibration_wait_intent_v17"
LAUNCH_RECEIPT_VERSION = "pif_app_server_reference_calibration_launch_receipt_v17"
CURRENT_RUNTIME_FILES = V16_RUNTIME_FILES + (
    "research_factory/app_server_judge_v5_fresh_calibration.py",
    "research_factory/app_server_runtime_lock_v17.py",
    "research_factory/app_server_judge_v5_continuation.py",
)
DEFAULT_PIPELINE_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DEFAULT_REFERENCE_ROOT = (
    DEFAULT_PIPELINE_ROOT / "fixture-reference-adjudication-luna-v4"
)
DEFAULT_CONTROL_ROOT = Path(
    "work/app-server-development-v2/unattended-control-v17"
).resolve()
DEFAULT_MANIFEST = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v17.json"
).resolve()
DEFAULT_V16_MANIFEST = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v16.json"
).resolve()
DEFAULT_V16_WAIT_INTENT = Path(
    "work/app-server-development-v2/unattended-control-v16/waiting-intent-v16.json"
).resolve()
DEFAULT_V16_BLOCKED_CAPACITY = Path(
    "work/app-server-development-v2/unattended-control-v16/blocked-capacity-snapshot.json"
).resolve()


class RuntimeLockV17Error(RuntimeError):
    """The reference-to-calibration runtime boundary is unsafe."""


def _load(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeLockV17Error(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise RuntimeLockV17Error(f"{purpose} is not an object")
    return value


def _repo_record(repo_root: Path, relative: str) -> dict[str, Any]:
    path = (repo_root / relative).resolve()
    try:
        path.relative_to(repo_root)
    except ValueError as exc:
        raise RuntimeLockV17Error("runtime file escapes repository") from exc
    if not path.is_file():
        raise RuntimeLockV17Error(f"runtime file is missing: {relative}")
    return {
        "path": relative,
        "sha256": _sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _verify_record(record: Any, *, expected_path: Optional[Path] = None) -> Path:
    if not isinstance(record, Mapping):
        raise RuntimeLockV17Error("runtime-lock record is malformed")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if expected_path is not None and path != expected_path.expanduser().resolve():
        raise RuntimeLockV17Error("runtime-lock record path drifted")
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise RuntimeLockV17Error("runtime-lock record content drifted")
    return path


def prepare_wait_intent(
    *, repo_root: Path, output_path: Path = DEFAULT_CONTROL_ROOT / "wait-intent-v17.json"
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    target = output_path.expanduser().resolve()
    if target.exists():
        return verify_wait_intent(repo_root=root, path=target)
    reference_root = (root / DEFAULT_REFERENCE_ROOT.relative_to(Path.cwd())).resolve()
    if reference_root.exists():
        raise RuntimeLockV17Error("reference root already exists before wait intent")
    records = {
        "runtime_lock_v16": _record(
            (root / DEFAULT_V16_MANIFEST.relative_to(Path.cwd())).resolve()
        ),
        "wait_intent_v16": _record(
            (root / DEFAULT_V16_WAIT_INTENT.relative_to(Path.cwd())).resolve()
        ),
        "blocked_capacity_v16": _record(
            (root / DEFAULT_V16_BLOCKED_CAPACITY.relative_to(Path.cwd())).resolve()
        ),
        "fresh_calibration_adapter": _record(
            root / "research_factory/app_server_judge_v5_fresh_calibration.py"
        ),
        "runtime_lock_builder": _record(
            root / "research_factory/app_server_runtime_lock_v17.py"
        ),
        "continuation_supervisor": _record(
            root / "research_factory/app_server_judge_v5_continuation.py"
        ),
    }
    payload = {
        "schema_version": WAIT_INTENT_VERSION,
        "created_at": now_iso(),
        "status": "waiting_for_fixture_reference_v2",
        "reference_root": str(reference_root),
        "reference_root_absent": True,
        "semantic_attempt_started": False,
        "automatic_fresh_calibration_after_reference": True,
        "records": records,
        "policy": {
            "managed_chatgpt_auth_only": True,
            "maximum_primary_used_percent": 20,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "retry_count_per_turn": 0,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "production_mutation_allowed": False,
        },
    }
    _write_immutable_json(target, payload)
    return verify_wait_intent(repo_root=root, path=target)


def verify_wait_intent(*, repo_root: Path, path: Path) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    value = _load(path.expanduser().resolve(), "v17 wait intent")
    if (
        value.get("schema_version") != WAIT_INTENT_VERSION
        or value.get("status") != "waiting_for_fixture_reference_v2"
        or value.get("reference_root_absent") is not True
        or value.get("semantic_attempt_started") is not False
        or value.get("automatic_fresh_calibration_after_reference") is not True
        or value.get("policy")
        != {
            "managed_chatgpt_auth_only": True,
            "maximum_primary_used_percent": 20,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "retry_count_per_turn": 0,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "production_mutation_allowed": False,
        }
    ):
        raise RuntimeLockV17Error("v17 wait intent drifted")
    records = value.get("records")
    if not isinstance(records, Mapping) or set(records) != {
        "runtime_lock_v16",
        "wait_intent_v16",
        "blocked_capacity_v16",
        "fresh_calibration_adapter",
        "runtime_lock_builder",
        "continuation_supervisor",
    }:
        raise RuntimeLockV17Error("v17 wait-intent coverage drifted")
    for record in records.values():
        _verify_record(record)
    expected_reference = (root / DEFAULT_REFERENCE_ROOT.relative_to(Path.cwd())).resolve()
    if Path(str(value.get("reference_root") or "")).resolve() != expected_reference:
        raise RuntimeLockV17Error("v17 wait-intent reference path drifted")
    return value


def _validate_launch_capacity(path: Path) -> dict[str, Any]:
    value = _load(path, "fresh-calibration launch capacity")
    used = value.get("primary_used_percent")
    if (
        value.get("schema_version") != LAUNCH_CAPACITY_VERSION
        or value.get("managed_chatgpt_auth_verified") is not True
        or value.get("maximum_primary_used_percent") != 20
        or value.get("cleared_for_semantic_work") is not True
        or value.get("thread_started") is not False
        or value.get("turn_started") is not False
        or isinstance(used, bool)
        or not isinstance(used, int)
        or used > 20
    ):
        raise RuntimeLockV17Error("fresh-calibration launch capacity is unsafe")
    return value


def _superseded_artifacts(
    *, repo_root: Path, reference: Mapping[str, Any], launch_capacity_path: Path
) -> list[dict[str, Any]]:
    root = repo_root.expanduser().resolve()
    rows = [
        {
            "label": "runtime_lock_v16",
            **_record(root / DEFAULT_V16_MANIFEST.relative_to(Path.cwd())),
        },
        {
            "label": "wait_intent_v16",
            **_record(root / DEFAULT_V16_WAIT_INTENT.relative_to(Path.cwd())),
        },
        {
            "label": "blocked_capacity_v16",
            **_record(root / DEFAULT_V16_BLOCKED_CAPACITY.relative_to(Path.cwd())),
        },
        {"label": "fresh_calibration_launch_capacity", **_record(launch_capacity_path)},
    ]
    for label, record in reference["records"].items():
        rows.append({"label": f"reference_{label}", **dict(record)})
    for attempt in reference["terminal"]["attempts"]:
        for kind in ("capacity", "sidecar", "output"):
            rows.append(
                {
                    "label": f"{attempt['turn_name']}_{kind}",
                    **dict(attempt[kind]),
                }
            )
    labels = [row["label"] for row in rows]
    if len(labels) != len(set(labels)):
        raise RuntimeLockV17Error("runtime-lock predecessor labels overlap")
    return rows


def build_runtime_lock_v17(
    *,
    repo_root: Path,
    manifest_path: Path = DEFAULT_MANIFEST,
    wait_intent_path: Path = DEFAULT_CONTROL_ROOT / "wait-intent-v17.json",
    launch_capacity_path: Path = DEFAULT_CONTROL_ROOT / "prelaunch-capacity.json",
    reference_root: Path = DEFAULT_REFERENCE_ROOT,
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    if manifest.exists():
        return verify_runtime_lock_v17(repo_root=root, manifest_path=manifest)
    verify_runtime_lock(
        repo_root=root,
        manifest_path=(root / DEFAULT_V16_MANIFEST.relative_to(Path.cwd())).resolve(),
    )
    wait_intent = verify_wait_intent(repo_root=root, path=wait_intent_path)
    _validate_launch_capacity(launch_capacity_path.expanduser().resolve())
    reference = load_frozen_fixture_reference(reference_root)
    artifacts = _superseded_artifacts(
        repo_root=root,
        reference=reference,
        launch_capacity_path=launch_capacity_path.expanduser().resolve(),
    )
    payload = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": "Run one fresh judge-v5.4 calibration against frozen fixture reference v2.",
        "supersedes": {
            "prior_runtime_lock_version": "pif_app_server_unattended_runtime_lock_v16",
            "wait_intent_sha256": _sha256_file(wait_intent_path),
            "reference_terminal_reason": reference["terminal"]["terminal_reason"],
            "reference_frozen": True,
            "fresh_calibration_authorized": True,
            "reference_turn_count": 12,
            "reference_retry_count": 0,
            "artifacts": artifacts,
        },
        "policy": {
            "execution_purpose": "fresh_reference_bound_judge_calibration",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "timeout_seconds": 1200.0,
            "case_count": 66,
            "witness_count": 182,
            "cases_per_shard": 6,
            "minimum_turn_count": 23,
            "maximum_turn_count": 24,
            "retry_count_per_turn": 0,
            "managed_chatgpt_auth_only": True,
            "api_key_billing_allowed": False,
            "raw_session_token_access_allowed": False,
            "codex_exec_semantic_calls_allowed": False,
            "official_persistent_codex_app_server_only": True,
            "semantic_turn_maximum_primary_used_percent": 20,
            "per_turn_capacity_checkpoint_required": True,
            "prior_calibration_output_reuse_allowed": False,
            "reference_truth_exposed_to_model": False,
            "production_mutation_allowed": False,
        },
        "files": [_repo_record(root, relative) for relative in CURRENT_RUNTIME_FILES],
    }
    _write_immutable_json(manifest, payload)
    return verify_runtime_lock_v17(repo_root=root, manifest_path=manifest)


def verify_runtime_lock_v17(
    *, repo_root: Path, manifest_path: Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    value = _load(manifest_path.expanduser().resolve(), "runtime lock v17")
    if (
        value.get("schema_version") != RUNTIME_LOCK_VERSION
        or value.get("policy", {}).get("execution_purpose")
        != "fresh_reference_bound_judge_calibration"
        or value.get("policy", {}).get("retry_count_per_turn") != 0
        or value.get("policy", {}).get("managed_chatgpt_auth_only") is not True
        or value.get("policy", {}).get("production_mutation_allowed") is not False
    ):
        raise RuntimeLockV17Error("runtime lock v17 policy drifted")
    files = value.get("files")
    if not isinstance(files, list) or len(files) != len(CURRENT_RUNTIME_FILES):
        raise RuntimeLockV17Error("runtime lock v17 file coverage drifted")
    by_path = {str(row.get("path") or ""): row for row in files if isinstance(row, Mapping)}
    if set(by_path) != set(CURRENT_RUNTIME_FILES):
        raise RuntimeLockV17Error("runtime lock v17 file identities drifted")
    for relative in CURRENT_RUNTIME_FILES:
        record = by_path[relative]
        path = (root / relative).resolve()
        if (
            not path.is_file()
            or record.get("sha256") != _sha256_file(path)
            or record.get("size_bytes") != path.stat().st_size
        ):
            raise RuntimeLockV17Error(f"runtime file drifted: {relative}")
    artifacts = (value.get("supersedes") or {}).get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 47:
        raise RuntimeLockV17Error("runtime lock v17 predecessor coverage drifted")
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
    payload = {
        "schema_version": LAUNCH_RECEIPT_VERSION,
        "created_at": now_iso(),
        "status": "fresh_calibration_declared_before_semantic_calls",
        "supervisor_pid": supervisor_pid,
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "semantic_attempt_started": False,
        "runtime_lock": _record(manifest_path),
        "launch_capacity": _record(capacity_path),
        "retry_count_per_turn": 0,
        "production_mutation_performed": False,
    }
    _write_immutable_json(output_path, payload)
    return payload


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Manage runtime-lock v17")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--wait-intent", default=str(DEFAULT_CONTROL_ROOT / "wait-intent-v17.json"))
    parser.add_argument("--launch-capacity", default=str(DEFAULT_CONTROL_ROOT / "prelaunch-capacity.json"))
    parser.add_argument("--reference-root", default=str(DEFAULT_REFERENCE_ROOT))
    parser.add_argument("--prepare-wait-intent", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)
    root = Path(args.repo_root)
    if args.prepare_wait_intent:
        value = prepare_wait_intent(repo_root=root, output_path=Path(args.wait_intent))
    elif args.verify_only:
        value = verify_runtime_lock_v17(repo_root=root, manifest_path=Path(args.manifest))
    else:
        value = build_runtime_lock_v17(
            repo_root=root,
            manifest_path=Path(args.manifest),
            wait_intent_path=Path(args.wait_intent),
            launch_capacity_path=Path(args.launch_capacity),
            reference_root=Path(args.reference_root),
        )
    print(json.dumps({"ok": True, "schema_version": value["schema_version"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
