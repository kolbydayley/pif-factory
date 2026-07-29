from __future__ import annotations

"""Immutable runtime lock for the v20 reserve-bounded reference phase."""

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .app_server_capacity_policy_v20 import (
    CAPACITY_AUDIT_VERSION,
    DEFAULT_CONTROL_ROOT,
    DEFAULT_SEMANTIC_ROOT,
)
from .app_server_capacity_reserve import (
    RESERVE_CAPACITY_POLICY_VERSION,
    load_reserve_capacity_policy,
)
from .util import now_iso


RUNTIME_LOCK_VERSION = "pif_app_server_unattended_runtime_lock_v20_r2"
DEFAULT_MANIFEST = Path(
    "work/app-server-development-v2/unattended-runtime-lock-v20-r2.json"
).resolve()
DEFAULT_POLICY = DEFAULT_CONTROL_ROOT / "capacity-policy-v20.json"
DEFAULT_AUDIT = DEFAULT_CONTROL_ROOT / "capacity-policy-audit-v20.json"
DEFAULT_PREPOLICY_CAPACITY = DEFAULT_CONTROL_ROOT / "prepolicy-capacity.json"

EXPECTED_RUNTIME_FILES = (
    "research_factory/__init__.py",
    "research_factory/app_server_capacity.py",
    "research_factory/app_server_capacity_policy_v20.py",
    "research_factory/app_server_capacity_probe.py",
    "research_factory/app_server_capacity_reserve.py",
    "research_factory/app_server_dev_selection.py",
    "research_factory/app_server_evaluation.py",
    "research_factory/app_server_holdout.py",
    "research_factory/app_server_interrupted_arm_recovery.py",
    "research_factory/app_server_judge_v5.py",
    "research_factory/app_server_judge_v5_calibration.py",
    "research_factory/app_server_judge_v5_calibration_runner.py",
    "research_factory/app_server_judge_v5_continuation.py",
    "research_factory/app_server_judge_v5_diagnostic.py",
    "research_factory/app_server_judge_v5_diagnostic_audit.py",
    "research_factory/app_server_judge_v5_diagnostic_v2_audit.py",
    "research_factory/app_server_judge_v5_diagnostic_v3_audit.py",
    "research_factory/app_server_judge_v5_diagnostic_v4_audit.py",
    "research_factory/app_server_judge_v5_diagnostic_v5_audit.py",
    "research_factory/app_server_judge_v5_fixture.py",
    "research_factory/app_server_judge_v5_fixture_audit_recovery.py",
    "research_factory/app_server_judge_v5_fresh_calibration.py",
    "research_factory/app_server_judge_v5_reference_adjudication.py",
    "research_factory/app_server_judge_v5_reference_adjudication_v20.py",
    "research_factory/app_server_judge_v5_selection.py",
    "research_factory/app_server_llm_judge.py",
    "research_factory/app_server_runtime_lock.py",
    "research_factory/app_server_runtime_lock_v10.py",
    "research_factory/app_server_runtime_lock_v11.py",
    "research_factory/app_server_runtime_lock_v12.py",
    "research_factory/app_server_runtime_lock_v13.py",
    "research_factory/app_server_runtime_lock_v14.py",
    "research_factory/app_server_runtime_lock_v15.py",
    "research_factory/app_server_runtime_lock_v16.py",
    "research_factory/app_server_runtime_lock_v17.py",
    "research_factory/app_server_runtime_lock_v20.py",
    "research_factory/app_server_v2_reuse.py",
    "research_factory/app_server_v5_diagnostic_reuse.py",
    "research_factory/app_server_v5_diagnostic_v3_reuse.py",
    "research_factory/app_server_v5_diagnostic_v4_reuse.py",
    "research_factory/app_server_v5_diagnostic_v5_reuse.py",
    "research_factory/app_server_v5_diagnostic_v6_reuse.py",
    "research_factory/app_server_v5_reuse.py",
    "research_factory/codex_app_server.py",
    "research_factory/db.py",
    "research_factory/discourse.py",
    "research_factory/efficient_backtest.py",
    "research_factory/ingest.py",
    "research_factory/labels.py",
    "research_factory/paths.py",
    "research_factory/prep.py",
    "research_factory/sources.py",
    "research_factory/text.py",
    "research_factory/transcript_strategies.py",
    "research_factory/transcription.py",
    "research_factory/unattended_app_server_eval.py",
    "research_factory/util.py",
    "research_factory/windowed_evaluation.py",
    "research_factory/worker.py",
    "research_factory/evaluation/judge_calibration_v2.json",
    "research_factory/evaluation/judge_fixture_truth_audit_v3.json",
    "research_factory/evaluation/judge_v5_diagnostic_v1.json",
    "research_factory/evaluation/judge_v5_diagnostic_v1_truth_audit.json",
    "research_factory/evaluation/judge_v5_diagnostic_v2.json",
    "research_factory/evaluation/judge_v5_diagnostic_v2_attempt_audit.json",
    "research_factory/evaluation/judge_v5_diagnostic_v3.json",
    "research_factory/evaluation/judge_v5_diagnostic_v3_quality_audit.json",
    "research_factory/evaluation/judge_v5_diagnostic_v4_fixture_audit.json",
    "research_factory/evaluation/judge_v5_diagnostic_v5_quality_audit.json",
    "research_factory/protocol/codex_app_server_0_144_1/codex_app_server_protocol.v2.schemas.json",
    "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v4.json",
    "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v5.json",
    "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v6.json",
    "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v7.json",
    "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v8.json",
    "work/app-server-development-v2/unattended-pipeline-v5/reuse-contract-v9.json",
    "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-receipt-v1.json",
    "work/app-server-development-v2/unattended-pipeline-v5/diagnostic-v1-truth-audit-receipt-v1.json",
    "work/app-server-development-v2/unattended-pipeline-v5/diagnostic-v2-attempt-audit-receipt-v1.json",
    "work/app-server-development-v2/unattended-pipeline-v5/diagnostic-v3-quality-audit-receipt-v1.json",
    "work/app-server-development-v2/unattended-pipeline-v5/diagnostic-v4-fixture-audit-receipt-v1.json",
    "work/app-server-development-v2/unattended-pipeline-v5/diagnostic-v5-quality-audit-receipt-v1.json",
    "work/app-server-development-v2/unattended-pipeline-v5/judge-v5_4-freeze-receipt-v1.json",
    "work/app-server-development-v2/unattended-pipeline-v5/calibration-v1-failure-audit-receipt-v1.json",
    "automation/resume-app-server-evaluation-v20.sh",
)

RUNTIME_ENTRY_MODULES = (
    "app_server_capacity_policy_v20",
    "app_server_judge_v5_reference_adjudication_v20",
    "app_server_runtime_lock_v20",
)

PREDECESSOR_ARTIFACTS = (
    (
        "runtime_lock_v20_prelaunch_drifted",
        "work/app-server-development-v2/unattended-runtime-lock-v20.json",
    ),
    (
        "v20_prelaunch_failure",
        "work/app-server-development-v2/unattended-control-v20/prelaunch-failure-v20.json",
    ),
    (
        "runtime_lock_v16",
        "work/app-server-development-v2/unattended-runtime-lock-v16.json",
    ),
    (
        "v16_waiting_intent",
        "work/app-server-development-v2/unattended-control-v16/waiting-intent-v16.json",
    ),
    (
        "v16_blocked_capacity_snapshot",
        "work/app-server-development-v2/unattended-control-v16/blocked-capacity-snapshot.json",
    ),
    (
        "v17_wait_intent",
        "work/app-server-development-v2/unattended-control-v17/wait-intent-v17.json",
    ),
    (
        "v17_state",
        "work/app-server-development-v2/unattended-control-v17/state.json",
    ),
    (
        "v18_wait_intent",
        "work/app-server-development-v2/unattended-control-v18/wait-intent-v18.json",
    ),
    (
        "v18_state",
        "work/app-server-development-v2/unattended-control-v18/state.json",
    ),
    (
        "v19_recovery_steering",
        "work/app-server-development-v2/unattended-control-v19/recovery-steering-20260713T1030EDT.md",
    ),
    (
        "fixture_audit_v2_terminal",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/terminal.json",
    ),
    (
        "fixture_audit_v3_terminal",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v3/terminal.json",
    ),
    (
        "fixture_audit_v2_shared_witness_pool",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/shared-witness-pool.private.json",
    ),
    (
        "fixture_audit_v2_pointwise_input",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/pointwise-input-full.private.json",
    ),
    (
        "fixture_audit_v2_pointwise_output",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/pointwise-output-full.private.json",
    ),
    (
        "fixture_audit_v2_provisional_truth",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/provisional-calibration-truth.private.json",
    ),
    (
        "fixture_audit_v3_reconciled_alignment",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v3/reconciled-alignment.private.json",
    ),
)


class RuntimeLockV20Error(RuntimeError):
    """The v20 runtime or immutable predecessor chain drifted."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _relative_record(root: Path, relative: str) -> dict[str, Any]:
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockV20Error("runtime-lock path escapes repository") from exc
    if not path.is_file():
        raise RuntimeLockV20Error("runtime-lock input is missing: %s" % relative)
    return {
        "path": relative,
        "sha256": _sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _load(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeLockV20Error("%s is missing or invalid" % purpose) from exc
    if not isinstance(value, dict):
        raise RuntimeLockV20Error("%s is not an object" % purpose)
    return value


def _verify_record(root: Path, record: Any) -> Path:
    if not isinstance(record, Mapping):
        raise RuntimeLockV20Error("runtime-lock record is malformed")
    relative = record.get("path")
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise RuntimeLockV20Error("runtime-lock record path is invalid")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockV20Error("runtime-lock record escapes repository") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise RuntimeLockV20Error("immutable runtime-lock record drifted")
    return path


def _verify_absolute_record(root: Path, record: Any) -> Path:
    if not isinstance(record, Mapping):
        raise RuntimeLockV20Error("absolute audit record is malformed")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockV20Error("absolute audit record escapes repository") from exc
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise RuntimeLockV20Error("absolute audit record drifted")
    return path


def _discover_runtime_python_dependencies(root: Path) -> set[str]:
    package = root / "research_factory"
    pending = list(RUNTIME_ENTRY_MODULES)
    discovered: set[str] = set()
    while pending:
        module_name = pending.pop()
        if module_name in discovered:
            continue
        module_path = package / (module_name.replace(".", "/") + ".py")
        if not module_path.is_file():
            raise RuntimeLockV20Error("v20 runtime dependency is missing")
        try:
            tree = ast.parse(module_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, SyntaxError) as exc:
            raise RuntimeLockV20Error("v20 runtime dependency is unreadable") from exc
        discovered.add(module_name)
        current_package = ["research_factory", *module_name.split(".")[:-1]]
        for node in ast.walk(tree):
            candidates: list[str] = []
            if isinstance(node, ast.ImportFrom):
                if node.level:
                    keep = len(current_package) - (node.level - 1)
                    if keep < 1:
                        raise RuntimeLockV20Error("v20 relative import escapes package")
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
                local_name = candidate.removeprefix("research_factory.")
                candidate_path = package / (local_name.replace(".", "/") + ".py")
                if candidate_path.is_file() and local_name not in discovered:
                    pending.append(local_name)
    return {
        "research_factory/" + module.replace(".", "/") + ".py"
        for module in discovered
    }


def _verify_runtime_file_records(root: Path, records: Any) -> list[Path]:
    if not isinstance(records, list):
        raise RuntimeLockV20Error("v20 runtime files are malformed")
    expected = list(dict.fromkeys(EXPECTED_RUNTIME_FILES))
    observed = [
        record.get("path") if isinstance(record, Mapping) else None
        for record in records
    ]
    if observed != expected:
        raise RuntimeLockV20Error("v20 runtime file coverage is not exact")
    discovered = _discover_runtime_python_dependencies(root)
    if not discovered.issubset(set(expected)):
        raise RuntimeLockV20Error("v20 imported runtime dependency coverage drifted")
    return [_verify_record(root, record) for record in records]


def _verify_measured_audit_records(root: Path, audit: Mapping[str, Any]) -> None:
    records = audit.get("measured_evidence_records")
    if not isinstance(records, list) or len(records) != 48:
        raise RuntimeLockV20Error("v20 measured evidence coverage drifted")
    expected_paths = set()
    for relative_root in (
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v2/turns",
        "work/app-server-development-v2/unattended-pipeline-v5/fixture-truth-audit-gpt55-v3/turns",
    ):
        turn_root = root / relative_root
        expected_paths.update(path.resolve() for path in turn_root.glob("*/capacity.json"))
        expected_paths.update(path.resolve() for path in turn_root.glob("*/sidecar.json"))
    if len(expected_paths) != 48:
        raise RuntimeLockV20Error("live measured evidence file count drifted")
    observed_paths = set()
    for record in records:
        path = _verify_absolute_record(root, record)
        kind = record.get("kind") if isinstance(record, Mapping) else None
        if (
            (kind == "capacity" and path.name != "capacity.json")
            or (kind == "sidecar" and path.name != "sidecar.json")
            or kind not in {"capacity", "sidecar"}
        ):
            raise RuntimeLockV20Error("measured evidence kind drifted")
        observed_paths.add(path)
    if observed_paths != expected_paths:
        raise RuntimeLockV20Error("measured evidence membership drifted")


def _artifact_records_by_label(
    root: Path, artifacts: Any
) -> dict[str, tuple[Mapping[str, Any], Path]]:
    if not isinstance(artifacts, list):
        raise RuntimeLockV20Error("v20 predecessor artifacts are malformed")
    rows: dict[str, tuple[Mapping[str, Any], Path]] = {}
    for record in artifacts:
        label = record.get("label") if isinstance(record, Mapping) else None
        if not isinstance(label, str) or not label or label in rows:
            raise RuntimeLockV20Error("v20 predecessor label is invalid")
        rows[label] = (record, _verify_record(root, record))
    return rows


def _validate_policy_audit_link(
    *,
    root: Path,
    policy: Mapping[str, Any],
    audit_record: Mapping[str, Any],
    audit_path: Path,
    audit: Mapping[str, Any],
) -> None:
    embedded = policy.get("audit")
    relative = audit_record.get("path")
    if not isinstance(relative, str) or Path(relative).is_absolute():
        raise RuntimeLockV20Error("v20 policy/audit cross-link drifted")
    pinned_path = (root / relative).resolve()
    try:
        pinned_path.relative_to(root)
    except ValueError as exc:
        raise RuntimeLockV20Error("v20 policy/audit cross-link drifted") from exc
    if (
        not isinstance(embedded, Mapping)
        or pinned_path != audit_path
        or Path(str(embedded.get("path") or "")).expanduser().resolve()
        != audit_path
        or embedded.get("sha256") != audit_record.get("sha256")
        or embedded.get("size_bytes") != audit_record.get("size_bytes")
        or audit.get("schema_version") != CAPACITY_AUDIT_VERSION
    ):
        raise RuntimeLockV20Error("v20 policy/audit cross-link drifted")


def _verify_policy_audit_cross_link(
    *,
    root: Path,
    artifacts_by_label: Mapping[str, tuple[Mapping[str, Any], Path]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        policy_record, policy_path = artifacts_by_label["v20_capacity_policy"]
        audit_record, audit_path = artifacts_by_label["v20_capacity_audit"]
    except KeyError as exc:
        raise RuntimeLockV20Error("v20 policy or audit pin is missing") from exc
    policy = load_reserve_capacity_policy(policy_path)
    audit = _load(audit_path, "v20 capacity audit")
    if policy_record.get("sha256") != _sha256_file(policy_path):
        raise RuntimeLockV20Error("v20 policy/audit cross-link drifted")
    _validate_policy_audit_link(
        root=root,
        policy=policy,
        audit_record=audit_record,
        audit_path=audit_path,
        audit=audit,
    )
    _verify_measured_audit_records(root, audit)
    return policy, audit


def _validate_predecessor_states(root: Path) -> None:
    v16 = _load(
        root
        / "work/app-server-development-v2/unattended-control-v16/waiting-intent-v16.json",
        "v16 waiting intent",
    )
    v17 = _load(
        root / "work/app-server-development-v2/unattended-control-v17/state.json",
        "v17 state",
    )
    v18 = _load(
        root / "work/app-server-development-v2/unattended-control-v18/state.json",
        "v18 state",
    )
    if (
        v16.get("semantic_attempt_started") is not False
        or v16.get("thread_started") is not False
        or v16.get("turn_started") is not False
        or v17.get("status") != "waiting_for_fixture_reference_v2"
        or v17.get("semantic_attempt_started") is not False
        or v18.get("status")
        != "waiting_for_fresh_calibration_selection_authorization"
        or v18.get("semantic_attempt_started") is not False
    ):
        raise RuntimeLockV20Error("obsolete waiting supervisor state drifted")
    forbidden = (
        root
        / "work/app-server-development-v2/unattended-pipeline-v5/fixture-reference-adjudication-luna-v4/terminal.json",
        root
        / "work/app-server-development-v2/unattended-pipeline-v5/judge-calibration-v5_4-reference-v2-v1/terminal.json",
        root
        / "work/app-server-development-v2/unattended-pipeline-v5/development-selection-v5-reference-v2/v5-selection-terminal.json",
        root / "work/app-server-development-v2/unattended-control-v17/terminal.json",
        root / "work/app-server-development-v2/unattended-control-v18/terminal.json",
    )
    if any(path.exists() for path in forbidden):
        raise RuntimeLockV20Error(
            "a predecessor terminal appeared; v20 cannot supersede it"
        )


def build_runtime_lock_v20(
    *,
    repo_root: Path,
    manifest_path: Path = DEFAULT_MANIFEST,
    policy_path: Path = DEFAULT_POLICY,
    audit_path: Path = DEFAULT_AUDIT,
    prepolicy_capacity_path: Path = DEFAULT_PREPOLICY_CAPACITY,
    semantic_root: Path = DEFAULT_SEMANTIC_ROOT,
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    if Path.cwd().resolve() != root:
        raise RuntimeLockV20Error("v20 runtime lock must be built from repository root")
    manifest = manifest_path.expanduser().resolve()
    if manifest.exists():
        verify_runtime_lock_v20(repo_root=root, manifest_path=manifest)
        return _load(manifest, "existing v20 runtime lock")
    semantic = semantic_root.expanduser().resolve()
    if semantic.exists():
        raise RuntimeLockV20Error("v20 semantic root exists before runtime lock")
    _validate_predecessor_states(root)
    policy = load_reserve_capacity_policy(policy_path)
    audit = _load(audit_path.expanduser().resolve(), "v20 capacity audit")
    if (
        audit.get("schema_version") != CAPACITY_AUDIT_VERSION
        or policy.get("schema_version") != RESERVE_CAPACITY_POLICY_VERSION
        or Path(policy["semantic_output_root"]).resolve() != semantic
        or policy.get("minimum_remaining_reserve_percent") != 20
        or policy.get("retry_count_per_turn") != 0
        or len(policy.get("ordered_turn_names") or []) != 12
    ):
        raise RuntimeLockV20Error("v20 policy or audit contract drifted")
    policy_relative = str(policy_path.expanduser().resolve().relative_to(root))
    audit_relative = str(audit_path.expanduser().resolve().relative_to(root))
    snapshot_relative = str(
        prepolicy_capacity_path.expanduser().resolve().relative_to(root)
    )
    audit_record = _relative_record(root, audit_relative)
    _validate_policy_audit_link(
        root=root,
        policy=policy,
        audit_record=audit_record,
        audit_path=audit_path.expanduser().resolve(),
        audit=audit,
    )
    _verify_measured_audit_records(root, audit)
    predecessor_records = [
        {"label": label, **_relative_record(root, relative)}
        for label, relative in PREDECESSOR_ARTIFACTS
    ]
    predecessor_records.extend(
        [
            {"label": "v20_prepolicy_capacity", **_relative_record(root, snapshot_relative)},
            {"label": "v20_capacity_audit", **audit_record},
            {"label": "v20_capacity_policy", **_relative_record(root, policy_relative)},
        ]
    )
    labels = [record["label"] for record in predecessor_records]
    if len(labels) != len(set(labels)):
        raise RuntimeLockV20Error("v20 predecessor labels overlap")
    payload = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "created_at": now_iso(),
        "purpose": "Run only the 12-turn fixture reference under a measured remaining-reserve policy.",
        "supersedes": {
            "schema_version": "pif_app_server_capacity_wait_supersession_v20_r2",
            "legacy_used_percent_ceiling": 20,
            "legacy_ceiling_replaced": True,
            "minimum_remaining_reserve_percent": 20,
            "v16_v17_v18_semantic_attempt_started": False,
            "predecessor_replay_allowed": False,
            "artifacts": predecessor_records,
        },
        "policy": {
            "phase_id": policy["phase_id"],
            "semantic_output_root": str(semantic),
            "semantic_output_root_absent_at_lock": True,
            "ordered_turn_count": 12,
            "projected_phase_quota_points": policy[
                "projected_phase_quota_points"
            ],
            "minimum_remaining_reserve_percent": 20,
            "reprobe_before_every_semantic_turn": True,
            "execute_turns_serially": True,
            "retry_count_per_turn": 0,
            "unknown_usage_hard_stop": True,
            "rate_limit_reached_type_hard_stop": True,
            "auth_drift_hard_stop": True,
            "production_mutation_allowed": False,
        },
        "files": [
            _relative_record(root, relative)
            for relative in dict.fromkeys(EXPECTED_RUNTIME_FILES)
        ],
    }
    manifest.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = manifest.open("xb")
    except FileExistsError as exc:
        raise RuntimeLockV20Error("v20 runtime lock already exists") from exc
    with handle:
        handle.write(
            (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            )
        )
        handle.flush()
        os.fsync(handle.fileno())
    verify_runtime_lock_v20(repo_root=root, manifest_path=manifest)
    return payload


def verify_runtime_lock_v20(
    *, repo_root: Path, manifest_path: Path = DEFAULT_MANIFEST
) -> dict[str, Any]:
    root = repo_root.expanduser().resolve()
    manifest_file = manifest_path.expanduser().resolve()
    _validate_predecessor_states(root)
    manifest = _load(manifest_file, "v20 runtime lock")
    supersedes = manifest.get("supersedes")
    policy = manifest.get("policy")
    if (
        manifest.get("schema_version") != RUNTIME_LOCK_VERSION
        or not isinstance(supersedes, Mapping)
        or supersedes.get("schema_version")
        != "pif_app_server_capacity_wait_supersession_v20_r2"
        or supersedes.get("legacy_ceiling_replaced") is not True
        or supersedes.get("minimum_remaining_reserve_percent") != 20
        or supersedes.get("v16_v17_v18_semantic_attempt_started") is not False
        or supersedes.get("predecessor_replay_allowed") is not False
        or not isinstance(policy, Mapping)
        or policy.get("minimum_remaining_reserve_percent") != 20
        or policy.get("ordered_turn_count") != 12
        or policy.get("retry_count_per_turn") != 0
        or policy.get("production_mutation_allowed") is not False
    ):
        raise RuntimeLockV20Error("v20 runtime-lock contract drifted")
    artifacts = supersedes.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != len(PREDECESSOR_ARTIFACTS) + 3:
        raise RuntimeLockV20Error("v20 predecessor coverage drifted")
    artifacts_by_label = _artifact_records_by_label(root, artifacts)
    labels = set(artifacts_by_label)
    expected_labels = {label for label, _path in PREDECESSOR_ARTIFACTS} | {
        "v20_prepolicy_capacity",
        "v20_capacity_audit",
        "v20_capacity_policy",
    }
    if labels != expected_labels:
        raise RuntimeLockV20Error("v20 predecessor labels drifted")
    _verify_policy_audit_cross_link(
        root=root, artifacts_by_label=artifacts_by_label
    )
    files = manifest.get("files")
    _verify_runtime_file_records(root, files)
    return {
        "ok": True,
        "schema_version": RUNTIME_LOCK_VERSION,
        "manifest_path": str(manifest_file),
        "manifest_sha256": _sha256_file(manifest_file),
        "verified_file_count": len(files),
        "verified_predecessor_artifact_count": len(artifacts),
        "production_mutation_performed": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build or verify runtime-lock v20")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--verify-only", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    if args.verify_only:
        report = verify_runtime_lock_v20(
            repo_root=Path(args.repo_root), manifest_path=Path(args.manifest)
        )
    else:
        payload = build_runtime_lock_v20(
            repo_root=Path(args.repo_root), manifest_path=Path(args.manifest)
        )
        report = {
            "ok": True,
            "schema_version": payload["schema_version"],
            "file_count": len(payload["files"]),
            "predecessor_artifact_count": len(
                payload["supersedes"]["artifacts"]
            ),
        }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
