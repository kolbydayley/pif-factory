from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import stat
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_development_matrix as canonical_matrix
from . import app_server_canonical_v31_episode_batch as adapter
from . import app_server_expanded_cap_development_matrix as legacy_matrix
from . import app_server_runtime_lock_v20 as runtime_lock_v20
from .app_server_capacity_reserve import (
    ReserveCapacityError,
    load_reserve_capacity_policy,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "canonical-v31-epoch7-capacity-readiness-v1"
)
DEFAULT_AUDIT_PATH = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-control-v20"
    / "capacity-policy-audit-v20.json"
)
DEFAULT_AUDIT_SHA256 = (
    "945d50f734263ee99f64e9924386b7057bc2f4ad087b966ff062f43796fb7ae8"
)
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-control-v20"
    / "capacity-policy-v20.json"
)
DEFAULT_POLICY_SHA256 = (
    "07f2dfc6029cc072fe2e0d91557c219820a35bad064715a7e1580526b3c5e64f"
)
DEFAULT_MANIFEST_PATH = (
    PROJECT_ROOT / "work" / "app-server-development-v2" / "manifest.json"
)
DEFAULT_MANIFEST_SHA256 = (
    "a25d1e9189e13aa6faeba48ea97da5e5702361b741f5d2970a97e5e8af674391"
)

READINESS_VERSION = "pif_canonical_v31_epoch7_capacity_readiness_v1"
RECEIPT_VERSION = "pif_canonical_v31_epoch7_capacity_readiness_receipt_v1"
READINESS_FILENAME = "capacity-readiness.json"
RECEIPT_FILENAME = "capacity-readiness-receipt.json"
TERMINAL_FILENAME = "terminal.json"
MINIMUM_POSITIVE_SAFETY_MARGIN_PERCENT = 1


class CanonicalV31Epoch7CapacityReadinessError(RuntimeError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _pretty_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _safe_path(
    path: Path,
    *,
    project_root: Path,
    label: str,
    require_file: bool = False,
) -> Path:
    project = project_root.expanduser().resolve()
    candidate = Path(os.path.abspath(os.path.expanduser(str(path))))
    try:
        candidate.relative_to(project)
    except ValueError as exc:
        raise CanonicalV31Epoch7CapacityReadinessError(
            f"{label} is outside the project root"
        ) from exc
    cursor = candidate
    while cursor != project:
        try:
            mode = cursor.lstat().st_mode
        except FileNotFoundError:
            mode = None
        except OSError as exc:
            raise CanonicalV31Epoch7CapacityReadinessError(
                f"{label} metadata is unavailable"
            ) from exc
        if mode is not None and stat.S_ISLNK(mode):
            raise CanonicalV31Epoch7CapacityReadinessError(
                f"{label} traverses a symlink"
            )
        cursor = cursor.parent
    if require_file:
        try:
            mode = candidate.lstat().st_mode
        except OSError as exc:
            raise CanonicalV31Epoch7CapacityReadinessError(
                f"{label} is unavailable"
            ) from exc
        if not stat.S_ISREG(mode):
            raise CanonicalV31Epoch7CapacityReadinessError(
                f"{label} is not a regular file"
            )
    return candidate


def _record(path: Path, *, project_root: Path) -> dict[str, Any]:
    resolved = _safe_path(
        path,
        project_root=project_root,
        label="artifact record",
        require_file=True,
    )
    payload = resolved.read_bytes()
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
    }


def _verify_record(
    value: Any,
    *,
    label: str,
    project_root: Path,
) -> Path:
    if not isinstance(value, Mapping) or set(value) != {
        "path",
        "sha256",
        "size_bytes",
    }:
        raise CanonicalV31Epoch7CapacityReadinessError(f"{label} record is malformed")
    path_value = value.get("path")
    size = value.get("size_bytes")
    if (
        not isinstance(path_value, str)
        or not _is_sha256(value.get("sha256"))
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise CanonicalV31Epoch7CapacityReadinessError(
            f"{label} record fields drifted"
        )
    path = Path(path_value).expanduser().resolve()
    if _record(path, project_root=project_root) != dict(value):
        raise CanonicalV31Epoch7CapacityReadinessError(f"{label} record drifted")
    return path


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31Epoch7CapacityReadinessError(f"{label} is malformed") from exc
    if not isinstance(value, dict):
        raise CanonicalV31Epoch7CapacityReadinessError(f"{label} is not an object")
    return value


def _write_immutable(path: Path, value: Any, *, project_root: Path) -> dict[str, Any]:
    payload = _pretty_json(value).encode("ascii")
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise CanonicalV31Epoch7CapacityReadinessError(
            f"immutable artifact already exists: {path}"
        ) from exc
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink()
        except OSError:
            pass
        raise
    return _record(path, project_root=project_root)


def _exact_call_inventory(manifest: Mapping[str, Any]) -> dict[str, Any]:
    episodes = manifest.get("episodes")
    if not isinstance(episodes, list) or not episodes:
        raise CanonicalV31Epoch7CapacityReadinessError(
            "development manifest episode inventory is absent"
        )
    arms: list[dict[str, Any]] = []
    total = 0
    for batch_size in adapter.SUPPORTED_BATCH_SIZES:
        for thread_mode in adapter.SUPPORTED_THREAD_MODES:
            request_count = 0
            for episode in episodes:
                segments = episode.get("segments") if isinstance(episode, Mapping) else None
                if not isinstance(segments, list) or not segments:
                    raise CanonicalV31Epoch7CapacityReadinessError(
                        "development manifest segment inventory is absent"
                    )
                request_count += math.ceil(len(segments) / int(batch_size))
            total += request_count
            arms.append(
                {
                    "batch_size": int(batch_size),
                    "thread_mode": str(thread_mode),
                    "request_count": request_count,
                }
            )
    expected_arms = [
        {"batch_size": size, "thread_mode": mode}
        for size, mode in canonical_matrix.EXPECTED_ARMS
    ]
    if (
        [(row["batch_size"], row["thread_mode"]) for row in arms]
        != [(row["batch_size"], row["thread_mode"]) for row in expected_arms]
        or total < 1
    ):
        raise CanonicalV31Epoch7CapacityReadinessError(
            "canonical matrix call inventory drifted"
        )
    return {
        "episode_count": len(episodes),
        "case_count": sum(len(episode["segments"]) for episode in episodes),
        "arms": arms,
        "exact_model_call_count": total,
    }


def _measured_sidecar_summary(
    audit: Mapping[str, Any], *, project_root: Path
) -> dict[str, Any]:
    records = audit.get("measured_evidence_records")
    if not isinstance(records, list):
        raise CanonicalV31Epoch7CapacityReadinessError(
            "measured evidence records are absent"
        )
    sidecars: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, Mapping) or record.get("kind") != "sidecar":
            continue
        path = _verify_record(
            {
                "path": record.get("path"),
                "sha256": record.get("sha256"),
                "size_bytes": record.get("size_bytes"),
            },
            label="measured sidecar",
            project_root=project_root,
        )
        payload = _load_object(path, label="measured sidecar")
        usage = payload.get("usage")
        total_tokens = usage.get("total_tokens") if isinstance(usage, Mapping) else None
        wall = payload.get("wall_elapsed_seconds")
        if (
            isinstance(total_tokens, bool)
            or not isinstance(total_tokens, int)
            or total_tokens < 0
            or isinstance(wall, bool)
            or not isinstance(wall, (int, float))
            or not math.isfinite(float(wall))
            or float(wall) < 0
            or payload.get("usage_complete") is not True
            or payload.get("usage_status") != "measured"
        ):
            raise CanonicalV31Epoch7CapacityReadinessError(
                "measured sidecar usage or wall telemetry drifted"
            )
        sidecars.append(
            {"total_tokens": total_tokens, "wall_elapsed_seconds": float(wall)}
        )
    if not sidecars:
        raise CanonicalV31Epoch7CapacityReadinessError(
            "measured sidecar set is empty"
        )
    return {
        "measured_sidecar_count": len(sidecars),
        "measured_total_tokens": sum(row["total_tokens"] for row in sidecars),
        "measured_maximum_turn_tokens": max(row["total_tokens"] for row in sidecars),
        "measured_maximum_wall_seconds": max(
            row["wall_elapsed_seconds"] for row in sidecars
        ),
    }


def build_readiness_analysis(
    *,
    audit_path: Path = DEFAULT_AUDIT_PATH,
    audit_sha256: str = DEFAULT_AUDIT_SHA256,
    policy_path: Path = DEFAULT_POLICY_PATH,
    policy_sha256: str = DEFAULT_POLICY_SHA256,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    manifest_sha256: str = DEFAULT_MANIFEST_SHA256,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    project = project_root.expanduser().resolve()
    audit_file = _safe_path(
        audit_path,
        project_root=project,
        label="measured capacity audit",
        require_file=True,
    )
    policy_file = _safe_path(
        policy_path,
        project_root=project,
        label="measured capacity policy",
        require_file=True,
    )
    manifest_file = _safe_path(
        manifest_path,
        project_root=project,
        label="development manifest",
        require_file=True,
    )
    for observed, expected, label in (
        (_record(audit_file, project_root=project)["sha256"], audit_sha256, "audit"),
        (_record(policy_file, project_root=project)["sha256"], policy_sha256, "policy"),
        (
            _record(manifest_file, project_root=project)["sha256"],
            manifest_sha256,
            "manifest",
        ),
    ):
        if not _is_sha256(expected) or observed != expected:
            raise CanonicalV31Epoch7CapacityReadinessError(
                f"{label} checksum binding failed"
            )
    audit = _load_object(audit_file, label="measured capacity audit")
    try:
        source_policy = load_reserve_capacity_policy(policy_file)
        runtime_lock_v20._verify_measured_audit_records(project, audit)
        manifest_info = legacy_matrix.verify_development_manifest(
            manifest_file,
            expected_sha256=manifest_sha256,
        )
    except (
        ReserveCapacityError,
        runtime_lock_v20.RuntimeLockV20Error,
        legacy_matrix.ExpandedCapDevelopmentMatrixError,
    ) as exc:
        raise CanonicalV31Epoch7CapacityReadinessError(
            "measured capacity lineage verification failed"
        ) from exc
    embedded_audit = source_policy.get("audit")
    audit_record = _record(audit_file, project_root=project)
    if not isinstance(embedded_audit, Mapping) or dict(embedded_audit) != audit_record:
        raise CanonicalV31Epoch7CapacityReadinessError(
            "source capacity policy/audit cross-link drifted"
        )
    measured = _measured_sidecar_summary(audit, project_root=project)
    if (
        audit.get("schema_version") != "pif_app_server_capacity_policy_audit_v20"
        or audit.get("production_mutation_performed") is not False
        or audit.get("measured_turn_count") != measured["measured_sidecar_count"]
        or audit.get("measured_total_tokens") != measured["measured_total_tokens"]
        or audit.get("measured_maximum_turn_tokens")
        != measured["measured_maximum_turn_tokens"]
        or source_policy.get("maximum_total_tokens_per_turn")
        != audit.get("frozen_maximum_total_tokens_per_turn")
        or source_policy.get("quota_points_per_million_tokens")
        != audit.get("frozen_quota_points_per_million_tokens")
        or source_policy.get("minimum_remaining_reserve_percent")
        != audit.get("minimum_remaining_reserve_percent")
    ):
        raise CanonicalV31Epoch7CapacityReadinessError(
            "measured capacity summary drifted"
        )
    inventory = _exact_call_inventory(manifest_info["payload"])
    turn_bound = int(source_policy["maximum_total_tokens_per_turn"])
    quota_rate = int(source_policy["quota_points_per_million_tokens"])
    reserve = int(source_policy["minimum_remaining_reserve_percent"])
    call_count = int(inventory["exact_model_call_count"])
    phase_bound = call_count * turn_bound
    projected_points = math.ceil(phase_bound * quota_rate / 1_000_000)
    maximum_usable = 100 - reserve - MINIMUM_POSITIVE_SAFETY_MARGIN_PERCENT
    maximum_admissible_per_turn = math.floor(
        maximum_usable * 1_000_000 / (quota_rate * call_count)
    )
    source_bound_fits = projected_points <= maximum_usable
    return {
        "schema_version": READINESS_VERSION,
        "state": "ready" if source_bound_fits else "waiting",
        "terminal_reason": (
            "canonical_epoch7_capacity_policy_can_be_frozen_from_measured_evidence"
            if source_bound_fits
            else "canonical_epoch7_source_turn_bound_cannot_fit_reserve_contract"
        ),
        "source_audit": audit_record,
        "source_policy": _record(policy_file, project_root=project),
        "development_manifest": _record(manifest_file, project_root=project),
        "measured_evidence_record_count": len(audit["measured_evidence_records"]),
        **measured,
        **inventory,
        "minimum_remaining_reserve_percent": reserve,
        "minimum_positive_safety_margin_percent": (
            MINIMUM_POSITIVE_SAFETY_MARGIN_PERCENT
        ),
        "quota_points_per_million_tokens": quota_rate,
        "source_maximum_total_tokens_per_turn": turn_bound,
        "source_phase_total_token_bound": phase_bound,
        "source_projected_phase_quota_points": projected_points,
        "maximum_possible_usable_quota_points": maximum_usable,
        "maximum_admissible_total_tokens_per_turn": maximum_admissible_per_turn,
        "required_per_turn_bound_reduction_tokens": max(
            0, turn_bound - maximum_admissible_per_turn
        ),
        "source_bound_fits_with_reserve_and_positive_safety_margin": source_bound_fits,
        "canonical_policy_emitted": False,
        "required_next_evidence": (
            []
            if source_bound_fits
            else [
                (
                    "checksum-bound representative full-canonical arm measurements "
                    f"supporting a per-turn bound <= {maximum_admissible_per_turn} tokens"
                ),
                (
                    "or a separately audited multi-window phase policy with explicit reset, "
                    "wall-deadline, global-budget, and no-replay semantics"
                ),
            ]
        ),
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "capacity_probe_performed": False,
        "operator_authorization_present": False,
        "extraction_authorized": False,
        "quality_authorized": False,
        "holdout_inspected": False,
        "production_mutated": False,
    }


def freeze_readiness_receipt(
    *,
    root: Path = DEFAULT_ROOT,
    audit_path: Path = DEFAULT_AUDIT_PATH,
    audit_sha256: str = DEFAULT_AUDIT_SHA256,
    policy_path: Path = DEFAULT_POLICY_PATH,
    policy_sha256: str = DEFAULT_POLICY_SHA256,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    manifest_sha256: str = DEFAULT_MANIFEST_SHA256,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    project = project_root.expanduser().resolve()
    output_root = _safe_path(
        root, project_root=project, label="capacity readiness root"
    )
    if output_root.exists():
        raise CanonicalV31Epoch7CapacityReadinessError(
            "capacity readiness root must be fresh and absent"
        )
    analysis = build_readiness_analysis(
        audit_path=audit_path,
        audit_sha256=audit_sha256,
        policy_path=policy_path,
        policy_sha256=policy_sha256,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
        project_root=project,
    )
    output_root.mkdir(parents=True, exist_ok=False)
    analysis_record = _write_immutable(
        output_root / READINESS_FILENAME,
        analysis,
        project_root=project,
    )
    receipt = {
        "schema_version": RECEIPT_VERSION,
        "state": analysis["state"],
        "terminal_reason": analysis["terminal_reason"],
        "scope": "canonical_v31_epoch7_capacity_readiness_only",
        "output_root": str(output_root),
        "capacity_readiness": analysis_record,
        "canonical_policy_emitted": False,
        "semantic_model_call_count": 0,
        "semantic_retry_count": 0,
        "capacity_probe_performed": False,
        "operator_authorization_present": False,
        "extraction_authorized": False,
        "quality_authorized": False,
        "holdout_inspected": False,
        "production_mutated": False,
    }
    raw = _pretty_json(receipt).encode("ascii")
    for name in (RECEIPT_FILENAME, TERMINAL_FILENAME):
        path = output_root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    return verify_readiness_receipt(output_root, project_root=project)


def verify_readiness_receipt(
    root: Path = DEFAULT_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    project = project_root.expanduser().resolve()
    output_root = _safe_path(
        root, project_root=project, label="capacity readiness root"
    )
    if not output_root.is_dir() or output_root.is_symlink():
        raise CanonicalV31Epoch7CapacityReadinessError(
            "capacity readiness root is unavailable"
        )
    expected = {READINESS_FILENAME, RECEIPT_FILENAME, TERMINAL_FILENAME}
    if {path.name for path in output_root.iterdir()} != expected:
        raise CanonicalV31Epoch7CapacityReadinessError(
            "capacity readiness artifact set drifted"
        )
    receipt_path = output_root / RECEIPT_FILENAME
    terminal_path = output_root / TERMINAL_FILENAME
    receipt = _load_object(receipt_path, label="capacity readiness receipt")
    terminal = _load_object(terminal_path, label="capacity readiness terminal")
    if receipt != terminal or receipt_path.read_bytes() != terminal_path.read_bytes():
        raise CanonicalV31Epoch7CapacityReadinessError(
            "capacity readiness receipt mirrors drifted"
        )
    analysis_path = _verify_record(
        receipt.get("capacity_readiness"),
        label="capacity readiness",
        project_root=project,
    )
    if analysis_path.parent != output_root:
        raise CanonicalV31Epoch7CapacityReadinessError(
            "capacity readiness record escaped its root"
        )
    analysis = _load_object(analysis_path, label="capacity readiness")
    recomputed = build_readiness_analysis(
        audit_path=Path(analysis["source_audit"]["path"]),
        audit_sha256=analysis["source_audit"]["sha256"],
        policy_path=Path(analysis["source_policy"]["path"]),
        policy_sha256=analysis["source_policy"]["sha256"],
        manifest_path=Path(analysis["development_manifest"]["path"]),
        manifest_sha256=analysis["development_manifest"]["sha256"],
        project_root=project,
    )
    if analysis != recomputed:
        raise CanonicalV31Epoch7CapacityReadinessError(
            "capacity readiness arithmetic drifted"
        )
    if (
        receipt.get("schema_version") != RECEIPT_VERSION
        or receipt.get("state") != analysis["state"]
        or receipt.get("terminal_reason") != analysis["terminal_reason"]
        or receipt.get("scope") != "canonical_v31_epoch7_capacity_readiness_only"
        or receipt.get("output_root") != str(output_root)
        or receipt.get("canonical_policy_emitted") is not False
        or receipt.get("semantic_model_call_count") != 0
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("capacity_probe_performed") is not False
        or receipt.get("operator_authorization_present") is not False
        or receipt.get("extraction_authorized") is not False
        or receipt.get("quality_authorized") is not False
        or receipt.get("holdout_inspected") is not False
        or receipt.get("production_mutated") is not False
    ):
        raise CanonicalV31Epoch7CapacityReadinessError(
            "capacity readiness receipt contract drifted"
        )
    return copy.deepcopy(receipt)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit canonical epoch-7 capacity policy readiness without probing."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            result = freeze_readiness_receipt(root=args.root)
        else:
            result = verify_readiness_receipt(args.root)
    except CanonicalV31Epoch7CapacityReadinessError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(_pretty_json(result), end="")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "CanonicalV31Epoch7CapacityReadinessError",
    "DEFAULT_ROOT",
    "build_readiness_analysis",
    "freeze_readiness_receipt",
    "main",
    "verify_readiness_receipt",
]
