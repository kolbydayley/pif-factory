from __future__ import annotations

"""Layered immutable runtime lock for the v21 reference recovery."""

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_capacity_policy_v21 import (
    DEFAULT_CONTROL_ROOT,
    DEFAULT_SEMANTIC_ROOT,
    DEFAULT_SOURCE_POLICY,
    DEFAULT_V20_ROOT,
    RECOVERY_AUDIT_VERSION,
    audit_v20_checkpoint_validator_failure,
)
from .app_server_capacity_reserve import load_reserve_capacity_policy
from .app_server_runtime_lock_v20 import verify_runtime_lock_v20
from .util import now_iso


RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v21"
DEFAULT_MANIFEST = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v21.json"
).resolve()
BASE_RUNTIME_LOCK_RELATIVE = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v20-r2.json"
)
BASE_RUNTIME_LOCK = BASE_RUNTIME_LOCK_RELATIVE.resolve()
DEFAULT_POLICY = DEFAULT_CONTROL_ROOT / "capacity-policy-v21.json"
DEFAULT_RECOVERY_AUDIT = DEFAULT_CONTROL_ROOT / "recovery-audit-v21.json"

EXPECTED_DELTA_RUNTIME_FILES = (
    "research_factory/app_server_capacity_policy_v21.py",
    "research_factory/app_server_judge_v5_reference_adjudication_v21.py",
    "research_factory/app_server_runtime_lock_v21.py",
    "automation/resume-app-server-evaluation-v21.sh",
)
RUNTIME_ENTRY_MODULES = (
    "app_server_capacity_policy_v21",
    "app_server_judge_v5_reference_adjudication_v21",
    "app_server_runtime_lock_v21",
)
PREDECESSOR_ARTIFACTS = (
    (
        "runtime_lock_v20_r2",
        "work/app-server-development-v2/unattended-runtime-lock-v20-r2.json",
    ),
    (
        "v20_launch_receipt",
        "work/app-server-development-v2/unattended-control-v20/launch-receipt-v20.json",
    ),
    (
        "v20_terminal",
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-reference-adjudication-luna-v5-capacity-v20/terminal.json",
    ),
    (
        "v20_reference_spec",
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-reference-adjudication-luna-v5-capacity-v20/"
        "reference-adjudication-spec.json",
    ),
    (
        "v20_disagreement_manifest",
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-reference-adjudication-luna-v5-capacity-v20/"
        "disagreement-manifest.private.json",
    ),
    (
        "v20_turn_00_input",
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-reference-adjudication-luna-v5-capacity-v20/turns/"
        "reference-pointwise-shard-00/input.private.json",
    ),
    (
        "v20_turn_00_prompt",
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-reference-adjudication-luna-v5-capacity-v20/turns/"
        "reference-pointwise-shard-00/prompt.private.md",
    ),
    (
        "v20_turn_00_schema",
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-reference-adjudication-luna-v5-capacity-v20/turns/"
        "reference-pointwise-shard-00/schema.json",
    ),
    (
        "v20_turn_00_capacity",
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-reference-adjudication-luna-v5-capacity-v20/turns/"
        "reference-pointwise-shard-00/capacity.json",
    ),
    (
        "v20_turn_00_sidecar",
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-reference-adjudication-luna-v5-capacity-v20/turns/"
        "reference-pointwise-shard-00/sidecar.json",
    ),
    (
        "v20_turn_00_output",
        "work/app-server-development-v2/unattended-pipeline-v5/"
        "fixture-reference-adjudication-luna-v5-capacity-v20/turns/"
        "reference-pointwise-shard-00/output.private.json",
    ),
    (
        "v21_recovery_audit",
        "work/app-server-development-v2/unattended-control-v21/"
        "recovery-audit-v21.json",
    ),
    (
        "v21_capacity_policy",
        "work/app-server-development-v2/unattended-control-v21/"
        "capacity-policy-v21.json",
    ),
)


class RuntimeLockV21Error(RuntimeError):
    """The v21 layered runtime or immutable lineage drifted."""


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
        raise RuntimeLockV21Error("%s is missing or invalid" % purpose) from exc
    if not isinstance(value, dict):
        raise RuntimeLockV21Error("%s is not an object" % purpose)
    return value


def _relative_record(root: Path, relative: str) -> dict[str, Any]:
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockV21Error("v21 lock path escapes repository") from exc
    if not path.is_file():
        raise RuntimeLockV21Error("v21 lock input is missing: %s" % relative)
    return {
        "path": relative,
        "sha256": _sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _verify_record(root: Path, record: Any) -> Path:
    if not isinstance(record, Mapping):
        raise RuntimeLockV21Error("v21 lock record is malformed")
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise RuntimeLockV21Error("v21 lock record path is invalid")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockV21Error("v21 lock record escapes repository") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise RuntimeLockV21Error("immutable v21 lock record drifted")
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
            raise RuntimeLockV21Error("v21 runtime dependency is missing")
        try:
            tree = ast.parse(module_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, SyntaxError) as exc:
            raise RuntimeLockV21Error("v21 runtime dependency is unreadable") from exc
        discovered.add(module_name)
        current_package = ["research_factory", *module_name.split(".")[:-1]]
        for node in ast.walk(tree):
            candidates: list[str] = []
            if isinstance(node, ast.ImportFrom):
                if node.level:
                    keep = len(current_package) - (node.level - 1)
                    if keep < 1:
                        raise RuntimeLockV21Error("v21 relative import escapes package")
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
    base = _load(root / BASE_RUNTIME_LOCK_RELATIVE, "v20-r2 lock")
    records = base.get("files")
    if not isinstance(records, list):
        raise RuntimeLockV21Error("v20-r2 runtime coverage is malformed")
    paths = {
        record.get("path") for record in records if isinstance(record, Mapping)
    }
    if None in paths or len(paths) != len(records):
        raise RuntimeLockV21Error("v20-r2 runtime coverage drifted")
    return {str(path) for path in paths}


def _verify_delta_files(root: Path, records: Any) -> list[Path]:
    if not isinstance(records, list):
        raise RuntimeLockV21Error("v21 delta runtime files are malformed")
    observed = [
        record.get("path") if isinstance(record, Mapping) else None
        for record in records
    ]
    expected = list(EXPECTED_DELTA_RUNTIME_FILES)
    if observed != expected:
        raise RuntimeLockV21Error("v21 delta runtime coverage is not exact")
    allowed = _base_runtime_files(root) | set(expected)
    if not _discover_python_dependencies(root).issubset(allowed):
        raise RuntimeLockV21Error("v21 imported runtime dependency coverage drifted")
    return [_verify_record(root, record) for record in records]


def _verify_lineage(root: Path, artifacts: Any) -> dict[str, Path]:
    if not isinstance(artifacts, list):
        raise RuntimeLockV21Error("v21 predecessor artifacts are malformed")
    expected_labels = [label for label, _relative in PREDECESSOR_ARTIFACTS]
    observed_labels = [
        record.get("label") if isinstance(record, Mapping) else None
        for record in artifacts
    ]
    if observed_labels != expected_labels:
        raise RuntimeLockV21Error("v21 predecessor coverage is not exact")
    return {
        record["label"]: _verify_record(root, record) for record in artifacts
    }


def _verify_recovery_contract(root: Path, records: Mapping[str, Path]) -> None:
    audit = _load(records["v21_recovery_audit"], "v21 recovery audit")
    policy = load_reserve_capacity_policy(records["v21_capacity_policy"])
    if (
        audit.get("schema_version") != RECOVERY_AUDIT_VERSION
        or audit.get("classification")
        != "repairable_local_checkpoint_validator_schema_mismatch"
        or audit.get("semantic_output_validator_error_count") != 0
        or audit.get("reserve_validator_accepts_completed_turn") is not True
        or audit.get("v20_replay_allowed") is not False
        or audit.get("production_mutated") is not False
        or policy.get("phase_id") != "fixture_reference_adjudication_v21"
        or Path(policy["semantic_output_root"]).resolve() != DEFAULT_SEMANTIC_ROOT
        or policy.get("ordered_turn_names", [None])[0]
        != "reference_pointwise_shard_01"
        or len(policy.get("ordered_turn_names", [])) != 11
        or (policy.get("recovery") or {}).get("v20_replay_allowed") is not False
        or ((policy.get("recovery") or {}).get("adopted_completed_turn") or {}).get(
            "turn_name"
        )
        != "reference_pointwise_shard_00"
        or ((policy.get("recovery") or {}).get("adopted_completed_turn") or {}).get(
            "adoption_is_not_retry"
        )
        is not True
    ):
        raise RuntimeLockV21Error("v21 recovery contract drifted")
    fresh = audit_v20_checkpoint_validator_failure(
        v20_root=DEFAULT_V20_ROOT, source_policy_path=DEFAULT_SOURCE_POLICY
    )
    stable_fields = (
        "classification",
        "completed_semantic_turn_count",
        "semantic_retry_count",
        "semantic_output_validator_error_count",
        "legacy_failure_reproduced",
        "reserve_validator_accepts_completed_turn",
        "usage_status",
        "usage",
        "v20_replay_allowed",
        "production_mutated",
    )
    if any(audit.get(field) != fresh.get(field) for field in stable_fields):
        raise RuntimeLockV21Error("v21 recovery audit no longer reproduces")
    for label, record in (
        ("v20_terminal", audit.get("v20_terminal")),
        ("v20_reference_spec", audit.get("v20_reference_spec")),
    ):
        expected = records[label]
        if (
            not isinstance(record, Mapping)
            or Path(str(record.get("path"))).resolve() != expected
            or record.get("sha256") != _sha256_file(expected)
            or record.get("size_bytes") != expected.stat().st_size
        ):
            raise RuntimeLockV21Error("v21 recovery audit cross-link drifted")
    recovery = policy.get("recovery")
    if not isinstance(recovery, Mapping):
        raise RuntimeLockV21Error("v21 policy recovery binding is missing")
    for key, expected in (
        ("source_policy", DEFAULT_SOURCE_POLICY),
        ("v20_terminal", records["v20_terminal"]),
        ("recovery_audit", records["v21_recovery_audit"]),
    ):
        record = recovery.get(key)
        expected_path = expected.expanduser().resolve()
        if (
            not isinstance(record, Mapping)
            or Path(str(record.get("path"))).expanduser().resolve() != expected_path
            or record.get("sha256") != _sha256_file(expected_path)
            or record.get("size_bytes") != expected_path.stat().st_size
        ):
            raise RuntimeLockV21Error("v21 policy recovery cross-link drifted")


def build_runtime_lock_v21(
    *, repo_root: Path, manifest_path: Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    if Path.cwd().resolve() != root:
        raise RuntimeLockV21Error("v21 lock must be built from repository root")
    manifest = manifest_path.expanduser().resolve()
    if manifest.exists():
        verify_runtime_lock_v21(repo_root=root, manifest_path=manifest)
        return _load(manifest, "existing v21 runtime lock")
    if DEFAULT_SEMANTIC_ROOT.exists():
        raise RuntimeLockV21Error("v21 semantic root exists before runtime lock")
    base_report = verify_runtime_lock_v20(
        repo_root=root,
        manifest_path=(root / BASE_RUNTIME_LOCK_RELATIVE).resolve(),
    )
    artifacts = [
        {"label": label, **_relative_record(root, relative)}
        for label, relative in PREDECESSOR_ARTIFACTS
    ]
    records = _verify_lineage(root, artifacts)
    _verify_recovery_contract(root, records)
    payload = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": "Run one fresh v21 reference after the v20 local checkpoint-validator failure.",
        "base_runtime_lock": {
            **_relative_record(
                root,
                "work/app-server-development-v2/unattended-runtime-lock-v20-r2.json",
            ),
            "verification": base_report,
        },
        "lineage": {
            "v20_terminal_state": "failed",
            "v20_failure_class": "repairable_local_checkpoint_validator_schema_mismatch",
            "v20_completed_semantic_turn_count": 1,
            "v20_replay_allowed": False,
            "v21_attempt_count": 1,
            "artifacts": artifacts,
        },
        "policy": {
            "phase_id": "fixture_reference_adjudication_v21",
            "semantic_output_root": str(DEFAULT_SEMANTIC_ROOT),
            "semantic_output_root_absent_at_lock": True,
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
        raise RuntimeLockV21Error("v21 runtime lock already exists") from exc
    with handle:
        handle.write(
            (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            )
        )
        handle.flush()
        os.fsync(handle.fileno())
    verify_runtime_lock_v21(repo_root=root, manifest_path=manifest)
    return payload


def verify_runtime_lock_v21(
    *, repo_root: Path, manifest_path: Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    base_report = verify_runtime_lock_v20(
        repo_root=root,
        manifest_path=(root / BASE_RUNTIME_LOCK_RELATIVE).resolve(),
    )
    manifest = _load(manifest_path, "v21 runtime lock")
    lineage = manifest.get("lineage")
    policy = manifest.get("policy")
    if (
        manifest.get("schema_version") != RUNTIME_LOCK_VERSION
        or not isinstance(lineage, Mapping)
        or lineage.get("v20_terminal_state") != "failed"
        or lineage.get("v20_replay_allowed") is not False
        or lineage.get("v21_attempt_count") != 1
        or not isinstance(policy, Mapping)
        or policy.get("phase_id") != "fixture_reference_adjudication_v21"
        or policy.get("retry_count_per_turn") != 0
        or policy.get("production_mutation_allowed") is not False
    ):
        raise RuntimeLockV21Error("v21 runtime-lock contract drifted")
    base_record = manifest.get("base_runtime_lock")
    base_path = _verify_record(root, base_record)
    if (
        base_path != (root / BASE_RUNTIME_LOCK_RELATIVE).resolve()
        or (base_record or {}).get("verification", {}).get("manifest_sha256")
        != base_report["manifest_sha256"]
    ):
        raise RuntimeLockV21Error("v21 base runtime-lock cross-link drifted")
    records = _verify_lineage(root, lineage.get("artifacts"))
    _verify_recovery_contract(root, records)
    delta = manifest.get("delta_files")
    _verify_delta_files(root, delta)
    return {
        "ok": True,
        "schema_version": RUNTIME_LOCK_VERSION,
        "manifest_path": str(Path(manifest_path).expanduser().resolve()),
        "manifest_sha256": _sha256_file(Path(manifest_path).expanduser().resolve()),
        "verified_base_file_count": base_report["verified_file_count"],
        "verified_delta_file_count": len(delta),
        "verified_predecessor_artifact_count": len(records),
        "production_mutation_performed": False,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build or verify v21 runtime lock")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)
    if args.verify_only:
        report = verify_runtime_lock_v21(
            repo_root=Path(args.repo_root), manifest_path=Path(args.manifest)
        )
    else:
        payload = build_runtime_lock_v21(
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
