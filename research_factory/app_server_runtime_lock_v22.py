from __future__ import annotations

"""Layered immutable runtime lock for fresh v22 calibration."""

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_capacity_policy_v22 import (
    AUTHORIZATION_AUDIT_VERSION,
    CALIBRATION_TURN_NAMES,
    DEFAULT_AUTHORIZATION_AUDIT,
    DEFAULT_CONTROL_ROOT,
    DEFAULT_POLICY,
    build_calibration_authorization_audit,
)
from .app_server_capacity_reserve import load_reserve_capacity_policy
from .app_server_judge_v5_fresh_calibration_v22 import (
    DEFAULT_OUTPUT_ROOT,
    REFERENCE_ROOT,
    load_frozen_fixture_reference_v21,
)
from .app_server_runtime_lock_v21 import verify_runtime_lock_v21
from .util import now_iso


RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v22"
DEFAULT_MANIFEST = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v22.json"
).resolve()
BASE_RUNTIME_LOCK_RELATIVE = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v21.json"
)
V21_ROOT_RELATIVE = Path(
    "work/app-server-development-v2/unattended-pipeline-v5/"
    "fixture-reference-adjudication-luna-v6-capacity-v21"
)
EXPECTED_DELTA_RUNTIME_FILES = (
    "research_factory/app_server_capacity_policy_v22.py",
    "research_factory/app_server_judge_v5_fresh_calibration_v22.py",
    "research_factory/app_server_runtime_lock_v22.py",
    "automation/resume-app-server-evaluation-v22.sh",
)
RUNTIME_ENTRY_MODULES = (
    "app_server_capacity_policy_v22",
    "app_server_judge_v5_fresh_calibration_v22",
    "app_server_runtime_lock_v22",
)


class RuntimeLockV22Error(RuntimeError):
    """The v22 runtime or frozen calibration lineage drifted."""


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
        raise RuntimeLockV22Error(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise RuntimeLockV22Error(f"{purpose} is not an object")
    return value


def _relative_record(root: Path, relative: str) -> dict[str, Any]:
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockV22Error("v22 lock path escapes repository") from exc
    if not path.is_file():
        raise RuntimeLockV22Error(f"v22 lock input is missing: {relative}")
    return {
        "path": relative,
        "sha256": _sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _verify_record(root: Path, record: Any) -> Path:
    if not isinstance(record, Mapping):
        raise RuntimeLockV22Error("v22 lock record is malformed")
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise RuntimeLockV22Error("v22 lock record path is invalid")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockV22Error("v22 lock record escapes repository") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise RuntimeLockV22Error("immutable v22 lock record drifted")
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
            raise RuntimeLockV22Error("v22 runtime dependency is missing")
        try:
            tree = ast.parse(module_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, SyntaxError) as exc:
            raise RuntimeLockV22Error("v22 runtime dependency is unreadable") from exc
        discovered.add(module_name)
        current_package = ["research_factory", *module_name.split(".")[:-1]]
        for node in ast.walk(tree):
            candidates: list[str] = []
            if isinstance(node, ast.ImportFrom):
                if node.level:
                    keep = len(current_package) - (node.level - 1)
                    if keep < 1:
                        raise RuntimeLockV22Error("v22 import escapes package")
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
    v21 = _load(root / BASE_RUNTIME_LOCK_RELATIVE, "v21 runtime lock")
    delta = v21.get("delta_files")
    base_record = v21.get("base_runtime_lock")
    if not isinstance(delta, list) or not isinstance(base_record, Mapping):
        raise RuntimeLockV22Error("v21 runtime coverage is malformed")
    v20 = _load(root / str(base_record.get("path") or ""), "v20-r2 runtime lock")
    base_files = v20.get("files")
    if not isinstance(base_files, list):
        raise RuntimeLockV22Error("v20-r2 runtime coverage is malformed")
    paths = {
        str(record.get("path"))
        for record in [*base_files, *delta]
        if isinstance(record, Mapping)
    }
    if len(paths) != len(base_files) + len(delta):
        raise RuntimeLockV22Error("layered base runtime coverage overlaps")
    return paths


def _lineage_paths(root: Path) -> list[tuple[str, str]]:
    v21_root = root / V21_ROOT_RELATIVE
    terminal = _load(v21_root / "terminal.json", "v21 reference terminal")
    spec = _load(v21_root / "reference-adjudication-spec.json", "v21 reference spec")
    rows = [
        ("runtime_lock_v21", str(BASE_RUNTIME_LOCK_RELATIVE)),
        (
            "v21_recovery_audit",
            "work/app-server-development-v2/unattended-control-v21/recovery-audit-v21.json",
        ),
        (
            "v21_capacity_policy",
            "work/app-server-development-v2/unattended-control-v21/capacity-policy-v21.json",
        ),
        (
            "v21_launch_receipt",
            "work/app-server-development-v2/unattended-control-v21/launch-receipt-v21.json",
        ),
        ("v21_terminal", str(V21_ROOT_RELATIVE / "terminal.json")),
        (
            "v21_reference_receipt",
            str(V21_ROOT_RELATIVE / "fixture-reference-v2-receipt.json"),
        ),
        (
            "v21_reference",
            str(V21_ROOT_RELATIVE / "fixture-reference-v2.private.json"),
        ),
        (
            "v21_unresolved",
            str(V21_ROOT_RELATIVE / "reference-unresolved.private.json"),
        ),
        (
            "v21_disagreement_manifest",
            str(V21_ROOT_RELATIVE / "disagreement-manifest.private.json"),
        ),
        (
            "v21_change_summary",
            str(V21_ROOT_RELATIVE / "reference-change-summary.json"),
        ),
        (
            "v21_reference_spec",
            str(V21_ROOT_RELATIVE / "reference-adjudication-spec.json"),
        ),
        (
            "v21_adoption_receipt",
            str(V21_ROOT_RELATIVE / "completed-turn-adoption.json"),
        ),
        (
            "v22_authorization_audit",
            str(DEFAULT_AUTHORIZATION_AUDIT.relative_to(Path.cwd())),
        ),
        (
            "v22_capacity_policy",
            str(DEFAULT_POLICY.relative_to(Path.cwd())),
        ),
    ]
    for attempt in terminal.get("attempts") or []:
        if not isinstance(attempt, Mapping):
            raise RuntimeLockV22Error("v21 attempt coverage is malformed")
        name = str(attempt.get("turn_name") or "")
        for kind in ("capacity", "sidecar", "output"):
            record = attempt.get(kind)
            if record is None:
                continue
            path = Path(str((record or {}).get("path") or "")).resolve()
            rows.append((f"v21_{name}_{kind}", str(path.relative_to(root))))
    for request in spec.get("pointwise_requests") or []:
        name = str((request or {}).get("turn_name") or "")
        for kind in ("input", "prompt", "schema"):
            record = (request or {}).get(kind)
            path = Path(str((record or {}).get("path") or "")).resolve()
            rows.append((f"v21_{name}_{kind}", str(path.relative_to(root))))
    labels = [label for label, _path in rows]
    paths = [path for _label, path in rows]
    if len(labels) != len(set(labels)) or len(paths) != len(set(paths)):
        raise RuntimeLockV22Error("v22 lineage coverage overlaps")
    return rows


def _verify_lineage(root: Path, artifacts: Any) -> dict[str, Path]:
    if not isinstance(artifacts, list):
        raise RuntimeLockV22Error("v22 lineage artifacts are malformed")
    expected = _lineage_paths(root)
    observed = [
        (record.get("label"), record.get("path"))
        if isinstance(record, Mapping)
        else (None, None)
        for record in artifacts
    ]
    if observed != expected:
        raise RuntimeLockV22Error("v22 lineage coverage is not exact")
    return {record["label"]: _verify_record(root, record) for record in artifacts}


def _verify_delta_files(root: Path, records: Any) -> None:
    if not isinstance(records, list):
        raise RuntimeLockV22Error("v22 delta runtime files are malformed")
    observed = [
        record.get("path") if isinstance(record, Mapping) else None
        for record in records
    ]
    if observed != list(EXPECTED_DELTA_RUNTIME_FILES):
        raise RuntimeLockV22Error("v22 delta runtime coverage is not exact")
    allowed = _base_runtime_files(root) | set(EXPECTED_DELTA_RUNTIME_FILES)
    if not _discover_python_dependencies(root).issubset(allowed):
        raise RuntimeLockV22Error("v22 imported runtime dependency coverage drifted")
    for record in records:
        _verify_record(root, record)


def _verify_authorization_contract(records: Mapping[str, Path]) -> None:
    audit = _load(records["v22_authorization_audit"], "v22 authorization audit")
    policy = load_reserve_capacity_policy(records["v22_capacity_policy"])
    fresh = build_calibration_authorization_audit()
    stable_fields = (
        "status",
        "reference_turn_count",
        "reference_new_turn_count",
        "reference_completed_checkpoint_adoption_count",
        "reference_usage",
        "calibration_model",
        "calibration_reasoning_effort",
        "calibration_case_count",
        "calibration_witness_count",
        "calibration_turn_count_minimum",
        "calibration_turn_count_maximum",
        "ordered_turn_names",
        "semantic_retry_count_per_turn",
        "prior_calibration_output_reuse_allowed",
        "selection_authorized_before_calibration",
        "holdout_authorized",
        "production_mutated",
    )
    if (
        audit.get("schema_version") != AUTHORIZATION_AUDIT_VERSION
        or any(audit.get(field) != fresh.get(field) for field in stable_fields)
        or policy.get("phase_id") != "fresh_judge_v5_4_calibration_v22"
        or policy.get("ordered_turn_names") != list(CALIBRATION_TURN_NAMES)
        or policy.get("phase_total_token_bound") != 2448000
        or policy.get("projected_phase_quota_points") != 42
        or Path(policy["phase_output_root"]).resolve() != DEFAULT_OUTPUT_ROOT
        or Path(policy["semantic_output_root"]).resolve()
        != DEFAULT_OUTPUT_ROOT / "fresh-attempt"
        or (policy.get("authorization") or {}).get(
            "selection_authorized_before_calibration"
        )
        is not False
        or (policy.get("authorization") or {}).get("holdout_authorized") is not False
    ):
        raise RuntimeLockV22Error("v22 calibration authorization drifted")
    reference_labels = {
        "terminal": "v21_terminal",
        "receipt": "v21_reference_receipt",
        "reference": "v21_reference",
        "unresolved": "v21_unresolved",
        "disagreement_manifest": "v21_disagreement_manifest",
        "change_summary": "v21_change_summary",
        "reference_spec": "v21_reference_spec",
        "completed_turn_adoption": "v21_adoption_receipt",
    }
    reference_records = audit.get("reference_records")
    if not isinstance(reference_records, Mapping) or set(reference_records) != set(
        reference_labels
    ):
        raise RuntimeLockV22Error("v22 authorization reference coverage drifted")
    for label, record in reference_records.items():
        expected = records[reference_labels[label]]
        if (
            Path(str(record.get("path") or "")).resolve() != expected
            or record.get("sha256") != _sha256_file(expected)
            or record.get("size_bytes") != expected.stat().st_size
        ):
            raise RuntimeLockV22Error("v22 authorization reference cross-link drifted")
    authorization = policy.get("authorization")
    if not isinstance(authorization, Mapping):
        raise RuntimeLockV22Error("v22 capacity authorization binding is missing")
    for key, expected in (
        ("source_policy", records["v21_capacity_policy"]),
        ("reference_terminal", records["v21_terminal"]),
        ("reference_receipt", records["v21_reference_receipt"]),
        ("authorization_audit", records["v22_authorization_audit"]),
    ):
        record = authorization.get(key)
        if (
            not isinstance(record, Mapping)
            or Path(str(record.get("path") or "")).resolve() != expected
            or record.get("sha256") != _sha256_file(expected)
            or record.get("size_bytes") != expected.stat().st_size
        ):
            raise RuntimeLockV22Error("v22 capacity authorization cross-link drifted")
    load_frozen_fixture_reference_v21(REFERENCE_ROOT)


def build_runtime_lock_v22(
    *, repo_root: Path, manifest_path: Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest = manifest_path.expanduser().resolve()
    if Path.cwd().resolve() != root:
        raise RuntimeLockV22Error("v22 lock must be built from repository root")
    if manifest.exists():
        verify_runtime_lock_v22(repo_root=root, manifest_path=manifest)
        return _load(manifest, "existing v22 runtime lock")
    if DEFAULT_OUTPUT_ROOT.exists():
        raise RuntimeLockV22Error("v22 calibration root exists before runtime lock")
    base_report = verify_runtime_lock_v21(
        repo_root=root, manifest_path=(root / BASE_RUNTIME_LOCK_RELATIVE).resolve()
    )
    artifacts = [
        {"label": label, **_relative_record(root, relative)}
        for label, relative in _lineage_paths(root)
    ]
    records = _verify_lineage(root, artifacts)
    _verify_authorization_contract(records)
    payload = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": "Run one fresh reference-bound judge calibration under reserve capacity.",
        "base_runtime_lock": {
            **_relative_record(root, str(BASE_RUNTIME_LOCK_RELATIVE)),
            "verification": base_report,
        },
        "lineage": {
            "v21_reference_frozen": True,
            "fresh_calibration_authorized": True,
            "selection_authorized_before_calibration": False,
            "holdout_authorized": False,
            "v22_attempt_count": 1,
            "retry_count_per_turn": 0,
            "artifacts": artifacts,
        },
        "policy": {
            "phase_id": "fresh_judge_v5_4_calibration_v22",
            "phase_output_root": str(DEFAULT_OUTPUT_ROOT),
            "phase_output_root_absent_at_lock": True,
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
        raise RuntimeLockV22Error("v22 runtime lock already exists") from exc
    with handle:
        handle.write(
            (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            )
        )
        handle.flush()
        os.fsync(handle.fileno())
    verify_runtime_lock_v22(repo_root=root, manifest_path=manifest)
    return payload


def verify_runtime_lock_v22(
    *, repo_root: Path, manifest_path: Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    base_report = verify_runtime_lock_v21(
        repo_root=root, manifest_path=(root / BASE_RUNTIME_LOCK_RELATIVE).resolve()
    )
    manifest = _load(manifest_path, "v22 runtime lock")
    lineage = manifest.get("lineage")
    policy = manifest.get("policy")
    if (
        manifest.get("schema_version") != RUNTIME_LOCK_VERSION
        or not isinstance(lineage, Mapping)
        or lineage.get("v21_reference_frozen") is not True
        or lineage.get("fresh_calibration_authorized") is not True
        or lineage.get("selection_authorized_before_calibration") is not False
        or lineage.get("holdout_authorized") is not False
        or lineage.get("v22_attempt_count") != 1
        or lineage.get("retry_count_per_turn") != 0
        or not isinstance(policy, Mapping)
        or policy.get("phase_id") != "fresh_judge_v5_4_calibration_v22"
        or policy.get("retry_count_per_turn") != 0
        or policy.get("production_mutation_allowed") is not False
    ):
        raise RuntimeLockV22Error("v22 runtime-lock contract drifted")
    base_record = manifest.get("base_runtime_lock")
    base_path = _verify_record(root, base_record)
    if (
        base_path != (root / BASE_RUNTIME_LOCK_RELATIVE).resolve()
        or (base_record or {}).get("verification", {}).get("manifest_sha256")
        != base_report["manifest_sha256"]
    ):
        raise RuntimeLockV22Error("v22 base runtime-lock cross-link drifted")
    records = _verify_lineage(root, lineage.get("artifacts"))
    _verify_authorization_contract(records)
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
    parser = argparse.ArgumentParser(description="Build or verify v22 runtime lock")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)
    if args.verify_only:
        report = verify_runtime_lock_v22(
            repo_root=Path(args.repo_root), manifest_path=Path(args.manifest)
        )
    else:
        payload = build_runtime_lock_v22(
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
