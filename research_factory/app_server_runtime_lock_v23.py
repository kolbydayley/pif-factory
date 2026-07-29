from __future__ import annotations

"""Layered runtime lock for the v23 pre-validation ordering recovery."""

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_capacity_policy_v23 import (
    DEFAULT_CONTROL_ROOT,
    DEFAULT_FAILURE_AUDIT,
    DEFAULT_POLICY,
    PRESEMANTIC_FAILURE_AUDIT_VERSION,
    SOURCE_POLICY,
    V22_RUNTIME_LOCK,
    build_v22_presemantic_failure_audit,
)
from .app_server_capacity_reserve import load_reserve_capacity_policy
from .app_server_judge_v5_fresh_calibration_v23 import DEFAULT_OUTPUT_ROOT
from .app_server_runtime_lock_v22 import (
    EXPECTED_DELTA_RUNTIME_FILES as V22_DELTA_RUNTIME_FILES,
    _base_runtime_files as _v22_base_runtime_files,
    verify_runtime_lock_v22,
)
from .util import now_iso


RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v23"
DEFAULT_MANIFEST = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v23.json"
).resolve()
BASE_RUNTIME_LOCK_RELATIVE = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v22.json"
)
EXPECTED_DELTA_RUNTIME_FILES = (
    "research_factory/app_server_capacity_policy_v23.py",
    "research_factory/app_server_judge_v5_fresh_calibration_v23.py",
    "research_factory/app_server_runtime_lock_v23.py",
    "automation/resume-app-server-evaluation-v23.sh",
)
RUNTIME_ENTRY_MODULES = (
    "app_server_capacity_policy_v23",
    "app_server_judge_v5_fresh_calibration_v23",
    "app_server_runtime_lock_v23",
)
LINEAGE_ARTIFACTS = (
    ("runtime_lock_v22", "work/app-server-development-v2/unattended-runtime-lock-v22.json"),
    (
        "v22_authorization_audit",
        "work/app-server-development-v2/unattended-control-v22/"
        "calibration-authorization-audit-v22.json",
    ),
    (
        "v22_capacity_policy",
        "work/app-server-development-v2/unattended-control-v22/capacity-policy-v22.json",
    ),
    (
        "v22_launch_receipt",
        "work/app-server-development-v2/unattended-control-v22/launch-receipt-v22.json",
    ),
    (
        "v23_failure_audit",
        "work/app-server-development-v2/unattended-control-v23/"
        "presemantic-failure-audit-v22.json",
    ),
    (
        "v23_capacity_policy",
        "work/app-server-development-v2/unattended-control-v23/capacity-policy-v23.json",
    ),
)


class RuntimeLockV23Error(RuntimeError):
    """The v23 recovery runtime or lineage drifted."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeLockV23Error(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise RuntimeLockV23Error(f"{purpose} is not an object")
    return value


def _relative_record(root: Path, relative: str) -> dict[str, Any]:
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockV23Error("v23 lock path escapes repository") from exc
    if not path.is_file():
        raise RuntimeLockV23Error(f"v23 lock input is missing: {relative}")
    return {
        "path": relative,
        "sha256": _sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _verify_record(root: Path, record: Any) -> Path:
    if not isinstance(record, Mapping):
        raise RuntimeLockV23Error("v23 lock record is malformed")
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise RuntimeLockV23Error("v23 lock record path is invalid")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockV23Error("v23 lock record escapes repository") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise RuntimeLockV23Error("immutable v23 lock record drifted")
    return path


def _discover_python_dependencies(root: Path) -> set[str]:
    package = root / "research_factory"
    pending = list(RUNTIME_ENTRY_MODULES)
    discovered: set[str] = set()
    while pending:
        module_name = pending.pop()
        if module_name in discovered:
            continue
        module_path = package / (module_name.replace(".", "/") + ".py")
        if not module_path.is_file():
            raise RuntimeLockV23Error("v23 runtime dependency is missing")
        try:
            tree = ast.parse(module_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, SyntaxError) as exc:
            raise RuntimeLockV23Error("v23 runtime dependency is unreadable") from exc
        discovered.add(module_name)
        current_package = ["research_factory", *module_name.split(".")[:-1]]
        for node in ast.walk(tree):
            candidates: list[str] = []
            if isinstance(node, ast.ImportFrom):
                if node.level:
                    keep = len(current_package) - (node.level - 1)
                    if keep < 1:
                        raise RuntimeLockV23Error("v23 import escapes package")
                    base = current_package[:keep]
                    if node.module:
                        candidates.append(".".join([*base, *node.module.split(".")]))
                    else:
                        candidates.extend(
                            ".".join([*base, alias.name]) for alias in node.names
                        )
                elif node.module:
                    candidates.append(node.module)
            elif isinstance(node, ast.Import):
                candidates.extend(alias.name for alias in node.names)
            for candidate in candidates:
                if not candidate.startswith("research_factory."):
                    continue
                local = candidate.removeprefix("research_factory.")
                candidate_path = package / (local.replace(".", "/") + ".py")
                if candidate_path.is_file() and local not in discovered:
                    pending.append(local)
    return {
        "research_factory/" + name.replace(".", "/") + ".py"
        for name in discovered
    }


def _base_runtime_files(root: Path) -> set[str]:
    return _v22_base_runtime_files(root) | set(V22_DELTA_RUNTIME_FILES)


def _verify_delta_files(root: Path, records: Any) -> None:
    if not isinstance(records, list):
        raise RuntimeLockV23Error("v23 delta runtime files are malformed")
    observed = [
        record.get("path") if isinstance(record, Mapping) else None
        for record in records
    ]
    if observed != list(EXPECTED_DELTA_RUNTIME_FILES):
        raise RuntimeLockV23Error("v23 delta runtime coverage is not exact")
    allowed = _base_runtime_files(root) | set(EXPECTED_DELTA_RUNTIME_FILES)
    if not _discover_python_dependencies(root).issubset(allowed):
        raise RuntimeLockV23Error("v23 imported runtime dependency coverage drifted")
    for record in records:
        _verify_record(root, record)


def _verify_lineage(root: Path, artifacts: Any) -> dict[str, Path]:
    if not isinstance(artifacts, list):
        raise RuntimeLockV23Error("v23 lineage artifacts are malformed")
    expected = list(LINEAGE_ARTIFACTS)
    observed = [
        (record.get("label"), record.get("path"))
        if isinstance(record, Mapping)
        else (None, None)
        for record in artifacts
    ]
    if observed != expected:
        raise RuntimeLockV23Error("v23 lineage coverage is not exact")
    return {record["label"]: _verify_record(root, record) for record in artifacts}


def _verify_recovery_contract(records: Mapping[str, Path]) -> None:
    failure = _load(records["v23_failure_audit"], "v23 failure audit")
    policy = load_reserve_capacity_policy(records["v23_capacity_policy"])
    fresh = build_v22_presemantic_failure_audit()
    stable_fields = (
        "state",
        "terminal_reason",
        "classification",
        "error_class",
        "root_cause_code",
        "failure_reproduced_offline",
        "failure_reproduction_result",
        "v22_output_root",
        "v22_output_root_absent",
        "semantic_attempt_started",
        "thread_started",
        "turn_started",
        "capacity_checkpoint_count",
        "sidecar_count",
        "semantic_output_count",
        "usage_status",
        "usage",
        "retry_allowed_in_v22",
        "required_next_version",
        "production_mutated",
    )
    if (
        failure.get("schema_version") != PRESEMANTIC_FAILURE_AUDIT_VERSION
        or any(failure.get(field) != fresh.get(field) for field in stable_fields)
        or policy.get("phase_id") != "fresh_judge_v5_4_calibration_v23"
        or policy.get("ordered_turn_names") != fresh_policy_turn_names()
        or policy.get("phase_total_token_bound") != 2448000
        or policy.get("projected_phase_quota_points") != 42
        or Path(policy["phase_output_root"]).resolve() != DEFAULT_OUTPUT_ROOT
        or Path(policy["semantic_output_root"]).resolve()
        != DEFAULT_OUTPUT_ROOT / "fresh-attempt"
    ):
        raise RuntimeLockV23Error("v23 recovery contract drifted")
    recovery = policy.get("recovery")
    if not isinstance(recovery, Mapping):
        raise RuntimeLockV23Error("v23 recovery binding is missing")
    for key, expected in (
        ("source_policy", records["v22_capacity_policy"]),
        ("v22_runtime_lock", records["runtime_lock_v22"]),
        ("v22_launch_receipt", records["v22_launch_receipt"]),
        ("v22_failure_audit", records["v23_failure_audit"]),
    ):
        record = recovery.get(key)
        if (
            not isinstance(record, Mapping)
            or Path(str(record.get("path") or "")).resolve() != expected
            or record.get("sha256") != _sha256_file(expected)
            or record.get("size_bytes") != expected.stat().st_size
        ):
            raise RuntimeLockV23Error("v23 recovery cross-link drifted")


def fresh_policy_turn_names() -> list[str]:
    return list(load_reserve_capacity_policy(SOURCE_POLICY)["ordered_turn_names"])


def build_runtime_lock_v23(
    *, repo_root: Path, manifest_path: Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    if Path.cwd().resolve() != root:
        raise RuntimeLockV23Error("v23 lock must be built from repository root")
    if manifest.exists():
        verify_runtime_lock_v23(repo_root=root, manifest_path=manifest)
        return _load(manifest, "existing v23 runtime lock")
    if DEFAULT_OUTPUT_ROOT.exists():
        raise RuntimeLockV23Error("v23 output root exists before runtime lock")
    base_report = verify_runtime_lock_v22(
        repo_root=root, manifest_path=(root / BASE_RUNTIME_LOCK_RELATIVE).resolve()
    )
    artifacts = [
        {"label": label, **_relative_record(root, relative)}
        for label, relative in LINEAGE_ARTIFACTS
    ]
    records = _verify_lineage(root, artifacts)
    _verify_recovery_contract(records)
    payload = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": "Run v23 after preserving the pre-semantic v22 ordering failure.",
        "base_runtime_lock": {
            **_relative_record(root, str(BASE_RUNTIME_LOCK_RELATIVE)),
            "verification": base_report,
        },
        "lineage": {
            "v22_semantic_attempt_started": False,
            "v22_retry_allowed": False,
            "v23_attempt_count": 1,
            "retry_count_per_turn": 0,
            "selection_authorized_before_calibration": False,
            "holdout_authorized": False,
            "artifacts": artifacts,
        },
        "policy": {
            "phase_id": "fresh_judge_v5_4_calibration_v23",
            "phase_output_root": str(DEFAULT_OUTPUT_ROOT),
            "phase_output_root_absent_at_lock": True,
            "reference_validated_before_checkpoint_validator_installation": True,
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
            "minimum_turn_count": 23,
            "maximum_turn_count": 24,
            "retry_count_per_turn": 0,
            "managed_chatgpt_auth_only": True,
            "production_mutation_allowed": False,
        },
        "delta_files": [
            _relative_record(root, relative)
            for relative in EXPECTED_DELTA_RUNTIME_FILES
        ],
    }
    manifest.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = manifest.open("xb")
    except FileExistsError as exc:
        raise RuntimeLockV23Error("v23 runtime lock already exists") from exc
    with handle:
        handle.write(
            (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            )
        )
        handle.flush()
        os.fsync(handle.fileno())
    verify_runtime_lock_v23(repo_root=root, manifest_path=manifest)
    return payload


def verify_runtime_lock_v23(
    *, repo_root: Path, manifest_path: Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    base_report = verify_runtime_lock_v22(
        repo_root=root, manifest_path=(root / BASE_RUNTIME_LOCK_RELATIVE).resolve()
    )
    manifest = _load(manifest_path, "v23 runtime lock")
    lineage = manifest.get("lineage")
    policy = manifest.get("policy")
    if (
        manifest.get("schema_version") != RUNTIME_LOCK_VERSION
        or not isinstance(lineage, Mapping)
        or lineage.get("v22_semantic_attempt_started") is not False
        or lineage.get("v22_retry_allowed") is not False
        or lineage.get("v23_attempt_count") != 1
        or lineage.get("retry_count_per_turn") != 0
        or lineage.get("selection_authorized_before_calibration") is not False
        or lineage.get("holdout_authorized") is not False
        or not isinstance(policy, Mapping)
        or policy.get("phase_id") != "fresh_judge_v5_4_calibration_v23"
        or policy.get("retry_count_per_turn") != 0
        or policy.get("production_mutation_allowed") is not False
    ):
        raise RuntimeLockV23Error("v23 runtime-lock contract drifted")
    base_record = manifest.get("base_runtime_lock")
    base_path = _verify_record(root, base_record)
    if (
        base_path != (root / BASE_RUNTIME_LOCK_RELATIVE).resolve()
        or (base_record or {}).get("verification", {}).get("manifest_sha256")
        != base_report["manifest_sha256"]
    ):
        raise RuntimeLockV23Error("v23 base runtime-lock cross-link drifted")
    records = _verify_lineage(root, lineage.get("artifacts"))
    _verify_recovery_contract(records)
    delta = manifest.get("delta_files")
    _verify_delta_files(root, delta)
    return {
        "ok": True,
        "schema_version": RUNTIME_LOCK_VERSION,
        "manifest_path": str(Path(manifest_path).expanduser().resolve()),
        "manifest_sha256": _sha256_file(Path(manifest_path).expanduser().resolve()),
        "verified_base_file_count": base_report["verified_base_file_count"]
        + base_report["verified_delta_file_count"],
        "verified_delta_file_count": len(delta),
        "verified_predecessor_artifact_count": len(records),
        "production_mutation_performed": False,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build or verify v23 runtime lock")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)
    if args.verify_only:
        report = verify_runtime_lock_v23(
            repo_root=Path(args.repo_root), manifest_path=Path(args.manifest)
        )
    else:
        payload = build_runtime_lock_v23(
            repo_root=Path(args.repo_root), manifest_path=Path(args.manifest)
        )
        report = {
            "ok": True,
            "schema_version": payload["schema_version"],
            "delta_file_count": len(payload["delta_files"]),
            "predecessor_artifact_count": len(payload["lineage"]["artifacts"]),
        }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
