from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_canonical_v31_development_matrix as matrix
from . import app_server_canonical_v31_episode_batch as adapter
from . import app_server_canonical_v31_epoch7_controller as controller
from . import app_server_canonical_v31_epoch7_input_authority_plan as authority_plan
from . import app_server_canonical_v31_epoch7_input_authority_runtime as authority_runtime
from . import app_server_canonical_v31_epoch7_input_package as input_package
from . import app_server_expanded_cap_development_matrix as legacy_matrix
from .labels import ValidationError, validate_label_output


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
)
DEFAULT_PLAN_ROOT = PIPELINE_ROOT / "canonical-v31-epoch7-authority-bridge-plan-v1"
DEFAULT_MATERIALIZATION_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch7-authority-bridge-materialization-v1"
)
DEFAULT_INPUT_PACKAGE_ROOT = DEFAULT_MATERIALIZATION_ROOT / "input-package"
DEFAULT_FUTURE_CONTROLLER_ROOT = PIPELINE_ROOT / "canonical-v31-epoch7-controller-v1"
DEFAULT_FUTURE_EXTRACTION_ROOT = (
    PIPELINE_ROOT / "canonical-v31-epoch7-six-arm-extraction-v1"
)

PLAN_CONTRACT_VERSION = "pif_canonical_v31_epoch7_authority_bridge_plan_v1"
PLAN_RECEIPT_VERSION = "pif_canonical_v31_epoch7_authority_bridge_plan_receipt_v1"
BRIDGED_SOURCE_VERSION = (
    "pif_canonical_v31_epoch7_authority_repaired_source_binding_v1"
)
HANDOFF_VERSION = "pif_canonical_v31_epoch7_controller_input_handoff_v1"
MATERIALIZATION_RECEIPT_VERSION = (
    "pif_canonical_v31_epoch7_authority_bridge_materialization_receipt_v1"
)

PLAN_CONTRACT_FILENAME = "bridge-plan-contract.json"
PLAN_RECEIPT_FILENAME = "bridge-plan-receipt.json"
PLAN_TERMINAL_FILENAME = "terminal.json"
HANDOFF_FILENAME = "controller-input-handoff.json"
MATERIALIZATION_RECEIPT_FILENAME = "materialization-receipt.json"
MATERIALIZATION_TERMINAL_FILENAME = "terminal.json"


class CanonicalV31Epoch7AuthorityBridgeError(RuntimeError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _pretty_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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
        raise CanonicalV31Epoch7AuthorityBridgeError(
            f"{label} is outside the project root"
        ) from exc
    cursor = candidate
    while cursor != project:
        try:
            mode = cursor.lstat().st_mode
        except FileNotFoundError:
            mode = None
        except OSError as exc:
            raise CanonicalV31Epoch7AuthorityBridgeError(
                f"{label} metadata is unavailable"
            ) from exc
        if mode is not None and stat.S_ISLNK(mode):
            raise CanonicalV31Epoch7AuthorityBridgeError(
                f"{label} traverses a symlink"
            )
        cursor = cursor.parent
    if require_file:
        try:
            mode = candidate.lstat().st_mode
        except OSError as exc:
            raise CanonicalV31Epoch7AuthorityBridgeError(
                f"{label} is unavailable"
            ) from exc
        if not stat.S_ISREG(mode):
            raise CanonicalV31Epoch7AuthorityBridgeError(
                f"{label} is not a regular file"
            )
    return candidate


def _record(path: Path, *, allowed_root: Path | None = None) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if allowed_root is not None:
        try:
            resolved.relative_to(allowed_root.expanduser().resolve())
        except ValueError as exc:
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "artifact record escaped its allowed root"
            ) from exc
    try:
        mode = resolved.lstat().st_mode
        payload = resolved.read_bytes()
    except OSError as exc:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            f"required artifact is unavailable: {resolved}"
        ) from exc
    if not stat.S_ISREG(mode) or resolved.is_symlink():
        raise CanonicalV31Epoch7AuthorityBridgeError(
            f"required artifact is not a real file: {resolved}"
        )
    return {
        "path": str(resolved),
        "sha256": _sha256_bytes(payload),
        "size_bytes": len(payload),
    }


def _verify_record(
    value: Any,
    *,
    label: str,
    allowed_root: Path | None = None,
) -> Path:
    if not isinstance(value, Mapping) or set(value) != {
        "path",
        "sha256",
        "size_bytes",
    }:
        raise CanonicalV31Epoch7AuthorityBridgeError(f"{label} record is malformed")
    path_value = value.get("path")
    size = value.get("size_bytes")
    if (
        not isinstance(path_value, str)
        or not _is_sha256(value.get("sha256"))
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            f"{label} record fields drifted"
        )
    path = Path(path_value).expanduser().resolve()
    if _record(path, allowed_root=allowed_root) != dict(value):
        raise CanonicalV31Epoch7AuthorityBridgeError(f"{label} record drifted")
    return path


def _load_json(path: Path, *, label: str) -> Any:
    try:
        return json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalV31Epoch7AuthorityBridgeError(f"{label} is malformed") from exc


def _load_object(path: Path, *, label: str) -> dict[str, Any]:
    value = _load_json(path, label=label)
    if not isinstance(value, dict):
        raise CanonicalV31Epoch7AuthorityBridgeError(f"{label} is not an object")
    return value


def _load_array(path: Path, *, label: str) -> list[Any]:
    value = _load_json(path, label=label)
    if not isinstance(value, list):
        raise CanonicalV31Epoch7AuthorityBridgeError(f"{label} is not an array")
    return value


def _write_immutable_bytes(path: Path, payload: bytes) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise CanonicalV31Epoch7AuthorityBridgeError(
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
    return _record(path)


def _write_immutable_json(path: Path, value: Any) -> dict[str, Any]:
    return _write_immutable_bytes(path, _pretty_json(value).encode("ascii"))


def _runtime_dependencies() -> dict[str, dict[str, Any]]:
    return {
        "authority_bridge": _record(Path(__file__)),
        "authority_plan": _record(Path(authority_plan.__file__)),
        "authority_runtime": _record(Path(authority_runtime.__file__)),
        "canonical_adapter": _record(Path(adapter.__file__)),
        "canonical_matrix": _record(Path(matrix.__file__)),
        "epoch7_controller": _record(Path(controller.__file__)),
        "input_package": _record(Path(input_package.__file__)),
        "legacy_matrix": _record(Path(legacy_matrix.__file__)),
    }


def _verify_runtime_dependencies(value: Any) -> None:
    expected = _runtime_dependencies()
    if not isinstance(value, Mapping) or dict(value) != expected:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority bridge runtime dependency binding drifted"
        )
    for label, record in expected.items():
        _verify_record(record, label=f"runtime dependency {label}")


def _verify_authority_preauthorization(
    *,
    authority_root: Path,
    input_receipt_record: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        preauthorization = authority_runtime.verify_preauthorization(authority_root)
    except authority_runtime.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority runtime preauthorization failed verification"
        ) from exc
    contract_path = authority_root / authority_runtime.CONTRACT_FILENAME
    lock_path = authority_root / authority_runtime.RUNTIME_LOCK_FILENAME
    preauthorization_path = authority_root / authority_runtime.PREAUTHORIZATION_FILENAME
    contract = _load_object(contract_path, label="authority runtime contract")
    authority_plan_path = _verify_record(
        contract.get("authority_plan"), label="authority plan"
    )
    plan = _load_object(authority_plan_path, label="authority plan")
    if (
        preauthorization.get("state")
        != "passed_zero_call_preauthorization_only"
        or preauthorization.get("semantic_model_call_count") != 0
        or preauthorization.get("operator_authorization_present") is not False
        or preauthorization.get("runtime_contract") != _record(contract_path)
        or preauthorization.get("runtime_lock") != _record(lock_path)
        or plan.get("input_package_receipt") != dict(input_receipt_record)
        or contract.get("exact_turn_count") != plan.get("exact_turn_count")
        or contract.get("authority_plan") != _record(authority_plan_path)
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority preauthorization lineage drifted"
        )
    return preauthorization, contract, plan


def _plan_receipt_payload(
    *, root: Path, contract_record: Mapping[str, Any], contract: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": PLAN_RECEIPT_VERSION,
        "state": "waiting",
        "terminal_reason": "passed_authority_execution_receipt_required",
        "plan_root": str(root),
        "bridge_plan_contract": copy.deepcopy(dict(contract_record)),
        "expected_authority_execution_receipt_path": contract[
            "expected_authority_execution_receipt_path"
        ],
        "expected_merged_authority_output_path": contract[
            "expected_merged_authority_output_path"
        ],
        "future_materialization_root": contract["future_materialization_root"],
        "future_input_package_root": contract["future_input_package_root"],
        "future_controller_root": contract["future_controller_root"],
        "future_extraction_root": contract["future_extraction_root"],
        "semantic_model_call_count_started_by_bridge": 0,
        "semantic_retry_count_started_by_bridge": 0,
        "extraction_authorized": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def freeze_bridge_plan(
    *,
    plan_root: Path = DEFAULT_PLAN_ROOT,
    materialization_root: Path = DEFAULT_MATERIALIZATION_ROOT,
    future_input_package_root: Path = DEFAULT_INPUT_PACKAGE_ROOT,
    future_controller_root: Path = DEFAULT_FUTURE_CONTROLLER_ROOT,
    future_extraction_root: Path = DEFAULT_FUTURE_EXTRACTION_ROOT,
    database_path: Path = input_package.DEFAULT_DATABASE_PATH,
    input_root: Path = input_package.DEFAULT_ROOT,
    authority_root: Path = authority_runtime.DEFAULT_ROOT,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    project = project_root.expanduser().resolve()
    root = _safe_path(plan_root, project_root=project, label="bridge plan root")
    materialization = _safe_path(
        materialization_root,
        project_root=project,
        label="bridge materialization root",
    )
    package_root = _safe_path(
        future_input_package_root,
        project_root=project,
        label="future input package root",
    )
    future_controller = _safe_path(
        future_controller_root,
        project_root=project,
        label="future controller root",
    )
    future_extraction = _safe_path(
        future_extraction_root,
        project_root=project,
        label="future extraction root",
    )
    database = _safe_path(
        database_path,
        project_root=project,
        label="development database",
        require_file=True,
    )
    source_root = _safe_path(
        input_root,
        project_root=project,
        label="waiting input package root",
    )
    authority = _safe_path(
        authority_root,
        project_root=project,
        label="authority runtime root",
    )
    if root.exists():
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge plan root must be fresh and absent"
        )
    if materialization.exists():
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge materialization root must be fresh and absent"
        )
    if package_root.parent != materialization:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "future input package must be a direct child of materialization root"
        )
    roots = {root, materialization, future_controller, future_extraction}
    if len(roots) != 4:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge and future runtime roots must be distinct"
        )
    try:
        input_receipt = input_package.verify_input_package(
            source_root, project_root=project
        )
    except input_package.CanonicalV31Epoch7InputPackageError as exc:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "waiting input package failed verification"
        ) from exc
    if (
        input_receipt.get("state") != "waiting"
        or input_receipt.get("semantic_model_call_count") != 0
        or input_receipt.get("production_mutated") is not False
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority bridge requires the immutable waiting input package"
        )
    input_receipt_record = _record(source_root / input_package.RECEIPT_FILENAME)
    source_binding_path = _verify_record(
        input_receipt.get("source_binding"),
        label="waiting input source binding",
        allowed_root=source_root,
    )
    source_binding = _load_object(source_binding_path, label="waiting source binding")
    preauthorization, authority_contract, authority_plan_payload = (
        _verify_authority_preauthorization(
            authority_root=authority,
            input_receipt_record=input_receipt_record,
        )
    )
    if (
        authority_contract.get("exact_turn_count")
        != source_binding.get("episode_count")
        or authority_plan_payload.get("invalid_reference_count")
        != sum(
            row.get("canonical_v31_valid") is not True
            for row in source_binding.get("canonical_reference_validation", [])
        )
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority plan does not cover the waiting source"
        )
    capacity_record = source_binding.get("capacity_policy")
    _verify_record(capacity_record, label="source capacity policy")
    contract = {
        "schema_version": PLAN_CONTRACT_VERSION,
        "state": "frozen_zero_call_waiting_for_authority_execution",
        "project_root": str(project),
        "plan_root": str(root),
        "future_materialization_root": str(materialization),
        "future_input_package_root": str(package_root),
        "future_controller_root": str(future_controller),
        "future_extraction_root": str(future_extraction),
        "development_database_path": str(database),
        "waiting_input_package_receipt": input_receipt_record,
        "waiting_source_binding": _record(source_binding_path),
        "authority_preauthorization_receipt": _record(
            authority / authority_runtime.PREAUTHORIZATION_FILENAME
        ),
        "authority_runtime_contract": _record(
            authority / authority_runtime.CONTRACT_FILENAME
        ),
        "authority_runtime_lock": _record(
            authority / authority_runtime.RUNTIME_LOCK_FILENAME
        ),
        "authority_plan": copy.deepcopy(authority_contract["authority_plan"]),
        "capacity_policy": copy.deepcopy(dict(capacity_record)),
        "expected_authority_execution_receipt_path": str(
            (authority / authority_runtime.EXECUTION_RECEIPT_FILENAME).resolve()
        ),
        "expected_merged_authority_output_path": str(
            (authority / authority_runtime.MERGED_OUTPUT_FILENAME).resolve()
        ),
        "episode_count": int(source_binding["episode_count"]),
        "case_count": int(source_binding["case_count"]),
        "invalid_reference_count": int(
            authority_plan_payload["invalid_reference_count"]
        ),
        "preserved_reference_count": int(
            source_binding["case_count"]
            - authority_plan_payload["invalid_reference_count"]
        ),
        "runtime_dependencies": _runtime_dependencies(),
        "database_access": "sqlite_uri_mode_ro_query_only",
        "authority_receipt_state_required": "passed",
        "controller_authorization_present": False,
        "extraction_authorized": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "semantic_model_call_count_started_by_bridge": 0,
        "semantic_retry_count_started_by_bridge": 0,
    }
    root.mkdir(parents=True, exist_ok=False)
    contract_record = _write_immutable_json(root / PLAN_CONTRACT_FILENAME, contract)
    receipt = _plan_receipt_payload(
        root=root, contract_record=contract_record, contract=contract
    )
    raw = _pretty_json(receipt).encode("ascii")
    _write_immutable_bytes(root / PLAN_RECEIPT_FILENAME, raw)
    _write_immutable_bytes(root / PLAN_TERMINAL_FILENAME, raw)
    return verify_bridge_plan(root, project_root=project)["receipt"]


def verify_bridge_plan(
    plan_root: Path = DEFAULT_PLAN_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    project = project_root.expanduser().resolve()
    root = _safe_path(plan_root, project_root=project, label="bridge plan root")
    if not root.is_dir() or root.is_symlink():
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge plan root is unavailable"
        )
    if {path.name for path in root.iterdir()} != {
        PLAN_CONTRACT_FILENAME,
        PLAN_RECEIPT_FILENAME,
        PLAN_TERMINAL_FILENAME,
    }:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge plan artifact set drifted"
        )
    contract_path = root / PLAN_CONTRACT_FILENAME
    receipt_path = root / PLAN_RECEIPT_FILENAME
    terminal_path = root / PLAN_TERMINAL_FILENAME
    contract = _load_object(contract_path, label="bridge plan contract")
    receipt = _load_object(receipt_path, label="bridge plan receipt")
    terminal = _load_object(terminal_path, label="bridge plan terminal")
    if receipt != terminal or receipt_path.read_bytes() != terminal_path.read_bytes():
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge plan receipt mirrors drifted"
        )
    expected_contract_keys = {
        "schema_version",
        "state",
        "project_root",
        "plan_root",
        "future_materialization_root",
        "future_input_package_root",
        "future_controller_root",
        "future_extraction_root",
        "development_database_path",
        "waiting_input_package_receipt",
        "waiting_source_binding",
        "authority_preauthorization_receipt",
        "authority_runtime_contract",
        "authority_runtime_lock",
        "authority_plan",
        "capacity_policy",
        "expected_authority_execution_receipt_path",
        "expected_merged_authority_output_path",
        "episode_count",
        "case_count",
        "invalid_reference_count",
        "preserved_reference_count",
        "runtime_dependencies",
        "database_access",
        "authority_receipt_state_required",
        "controller_authorization_present",
        "extraction_authorized",
        "quality_authorized",
        "holdout_authorized",
        "production_mutation_allowed",
        "semantic_model_call_count_started_by_bridge",
        "semantic_retry_count_started_by_bridge",
    }
    if (
        set(contract) != expected_contract_keys
        or contract.get("schema_version") != PLAN_CONTRACT_VERSION
        or contract.get("state")
        != "frozen_zero_call_waiting_for_authority_execution"
        or contract.get("project_root") != str(project)
        or contract.get("plan_root") != str(root)
        or contract.get("database_access") != "sqlite_uri_mode_ro_query_only"
        or contract.get("authority_receipt_state_required") != "passed"
        or contract.get("controller_authorization_present") is not False
        or contract.get("extraction_authorized") is not False
        or contract.get("quality_authorized") is not False
        or contract.get("holdout_authorized") is not False
        or contract.get("production_mutation_allowed") is not False
        or contract.get("semantic_model_call_count_started_by_bridge") != 0
        or contract.get("semantic_retry_count_started_by_bridge") != 0
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge plan contract drifted"
        )
    _verify_runtime_dependencies(contract.get("runtime_dependencies"))
    database = _safe_path(
        Path(str(contract["development_database_path"])),
        project_root=project,
        label="development database",
        require_file=True,
    )
    input_receipt_path = _verify_record(
        contract.get("waiting_input_package_receipt"),
        label="waiting input package receipt",
    )
    input_root = input_receipt_path.parent
    try:
        input_receipt = input_package.verify_input_package(
            input_root, project_root=project
        )
    except input_package.CanonicalV31Epoch7InputPackageError as exc:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "waiting input package failed re-verification"
        ) from exc
    if input_receipt.get("state") != "waiting":
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge predecessor input package is no longer waiting"
        )
    source_path = _verify_record(
        contract.get("waiting_source_binding"),
        label="waiting source binding",
        allowed_root=input_root,
    )
    if input_receipt.get("source_binding") != _record(source_path):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "waiting source binding receipt lineage drifted"
        )
    authority_contract_path = _verify_record(
        contract.get("authority_runtime_contract"),
        label="authority runtime contract",
    )
    authority_root = authority_contract_path.parent
    preauthorization, authority_contract, authority_plan_payload = (
        _verify_authority_preauthorization(
            authority_root=authority_root,
            input_receipt_record=contract["waiting_input_package_receipt"],
        )
    )
    if (
        contract.get("authority_preauthorization_receipt")
        != _record(authority_root / authority_runtime.PREAUTHORIZATION_FILENAME)
        or contract.get("authority_runtime_lock")
        != _record(authority_root / authority_runtime.RUNTIME_LOCK_FILENAME)
        or contract.get("authority_plan") != authority_contract["authority_plan"]
        or contract.get("episode_count") != authority_contract["exact_turn_count"]
        or contract.get("invalid_reference_count")
        != authority_plan_payload["invalid_reference_count"]
        or contract.get("case_count")
        != contract.get("invalid_reference_count")
        + contract.get("preserved_reference_count")
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge authority lineage drifted"
        )
    del preauthorization
    capacity_path = _verify_record(
        contract.get("capacity_policy"), label="capacity policy"
    )
    source = _load_object(source_path, label="waiting source binding")
    if (
        source.get("capacity_policy") != _record(capacity_path)
        or source.get("episode_count") != contract.get("episode_count")
        or source.get("case_count") != contract.get("case_count")
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge source-count or capacity lineage drifted"
        )
    expected_authority_receipt = (
        authority_root / authority_runtime.EXECUTION_RECEIPT_FILENAME
    ).resolve()
    expected_merged_output = (
        authority_root / authority_runtime.MERGED_OUTPUT_FILENAME
    ).resolve()
    materialization_root = _safe_path(
        Path(str(contract["future_materialization_root"])),
        project_root=project,
        label="future materialization root",
    )
    package_root = _safe_path(
        Path(str(contract["future_input_package_root"])),
        project_root=project,
        label="future input package root",
    )
    if (
        contract.get("expected_authority_execution_receipt_path")
        != str(expected_authority_receipt)
        or contract.get("expected_merged_authority_output_path")
        != str(expected_merged_output)
        or package_root.parent != materialization_root
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge future-path contract drifted"
        )
    contract_record = _record(contract_path, allowed_root=root)
    expected_receipt = _plan_receipt_payload(
        root=root, contract_record=contract_record, contract=contract
    )
    if receipt != expected_receipt:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "bridge plan receipt drifted"
        )
    return {
        "contract": copy.deepcopy(contract),
        "receipt": copy.deepcopy(receipt),
        "contract_record": contract_record,
        "project_root": project,
        "database_path": database,
        "input_root": input_root,
        "source_binding": source,
        "source_binding_path": source_path,
        "authority_root": authority_root,
        "materialization_root": materialization_root,
        "input_package_root": package_root,
    }


def _load_passed_authority(
    plan: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    authority_root = Path(plan["authority_root"])
    try:
        receipt = authority_runtime.verify_execution_receipt(authority_root)
    except authority_runtime.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority execution receipt failed verification"
        ) from exc
    contract = plan["contract"]
    if (
        receipt.get("state") != "passed"
        or receipt.get("failed_checks") != []
        or receipt.get("unknown_usage_turn_count") != 0
        or receipt.get("semantic_retry_count") != 0
        or receipt.get("semantic_model_call_count") != contract["episode_count"]
        or receipt.get("completed_validated_turn_count") != contract["episode_count"]
        or receipt.get("extraction_plan_rebuild_required") is not True
        or receipt.get("quality_authorized") is not False
        or receipt.get("holdout_authorized") is not False
        or receipt.get("production_mutated") is not False
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority execution is not a complete passing predecessor"
        )
    receipt_path = authority_root / authority_runtime.EXECUTION_RECEIPT_FILENAME
    if str(receipt_path.resolve()) != contract[
        "expected_authority_execution_receipt_path"
    ]:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority execution receipt path drifted"
        )
    merged_path = _verify_record(
        receipt.get("merged_authority_output"), label="merged authority output"
    )
    if str(merged_path) != contract["expected_merged_authority_output_path"]:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "merged authority output path drifted"
        )
    merged = _load_object(merged_path, label="merged authority output")
    if (
        merged.get("schema_version") != authority_runtime.MERGED_OUTPUT_VERSION
        or merged.get("state") != "complete_development_authority_only"
        or merged.get("episode_count") != contract["episode_count"]
        or merged.get("reference_label_count") != contract["case_count"]
        or merged.get("repaired_reference_count")
        != contract["invalid_reference_count"]
        or merged.get("preserved_reference_count")
        != contract["preserved_reference_count"]
        or merged.get("deterministic_semantic_mutation") is not False
        or merged.get("quality_authorized") is not False
        or merged.get("holdout_authorized") is not False
        or merged.get("production_mutation_allowed") is not False
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "merged authority output contract drifted"
        )
    return receipt, merged, {
        "receipt": _record(receipt_path),
        "merged_output": _record(merged_path),
    }


def _rebuild_original_source_private(
    plan: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    contract = plan["contract"]
    source = plan["source_binding"]
    legacy_record = source.get("legacy_manifest")
    legacy_path = _verify_record(legacy_record, label="legacy development manifest")
    try:
        legacy_info = legacy_matrix.verify_development_manifest(
            legacy_path, expected_sha256=legacy_record["sha256"]
        )
    except legacy_matrix.ExpandedCapDevelopmentMatrixError as exc:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "legacy development manifest verification failed"
        ) from exc
    connection = input_package._open_read_only_database(plan["database_path"])
    try:
        try:
            prepared = legacy_matrix.prepare_development_episodes(
                connection,
                manifest=legacy_info["payload"],
                manifest_rows=legacy_info["rows"],
            )
        except legacy_matrix.ExpandedCapDevelopmentMatrixError as exc:
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "read-only development source preparation failed"
            ) from exc
    finally:
        connection.close()
    capacity_path = _verify_record(
        contract["capacity_policy"], label="capacity policy"
    )
    rebuilt_source, private = input_package._source_binding(
        legacy_info=legacy_info,
        prepared_episodes=prepared,
        capacity_policy_path=capacity_path,
        capacity_policy_sha256=contract["capacity_policy"]["sha256"],
        project_root=plan["project_root"],
    )
    if _canonical_json(rebuilt_source) != _canonical_json(source):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "read-only predecessor source no longer reproduces its frozen binding"
        )
    return rebuilt_source, private


def _apply_authority_output(
    *,
    source: Mapping[str, Any],
    private: Mapping[str, Any],
    merged: Mapping[str, Any],
    authority_records: Mapping[str, Any],
    authority_receipt: Mapping[str, Any],
    predecessor_source_record: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source_episode_ids = list(source["episode_ids"])
    merged_episodes = merged.get("episodes")
    if (
        not isinstance(merged_episodes, list)
        or [row.get("episode_id") for row in merged_episodes] != source_episode_ids
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "merged authority episode order drifted"
        )
    prepared_episodes = copy.deepcopy(list(private["prepared_episodes"]))
    prepared_by_id = {str(row["episode_id"]): row for row in prepared_episodes}
    rows = private["rows"]
    text_by_segment: dict[str, str] = {}
    for prepared in prepared_episodes:
        for segment in prepared["segments"]:
            text_by_segment[str(segment["segment_id"])] = str(
                segment["segment_text"]
            )
    diagnostics = {
        str(row["segment_id"]): row
        for row in source["canonical_reference_validation"]
    }
    original_valid_labels = private["validated_labels"]
    labels_by_segment: dict[str, dict[str, Any]] = {}
    repaired_ids: list[str] = []
    preserved_ids: list[str] = []
    context_hashes: dict[str, str] = {}
    for authority_episode in merged_episodes:
        episode_id = str(authority_episode["episode_id"])
        prepared = prepared_by_id.get(episode_id)
        if prepared is None:
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "authority episode escaped the prepared source"
            )
        original_context = prepared.get("episode_context")
        repaired_context = authority_episode.get("episode_context")
        if not isinstance(original_context, Mapping) or not isinstance(
            repaired_context, Mapping
        ):
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "authority context is malformed"
            )
        expected_context_keys = set(original_context) | {"excluded_source_context"}
        if (
            "excluded_source_context" in original_context
            or set(repaired_context) != expected_context_keys
            or any(
                _canonical_json(repaired_context.get(key))
                != _canonical_json(original_context.get(key))
                for key in original_context
            )
            or not input_package._context_value_valid(
                "excluded_source_context",
                repaired_context.get("excluded_source_context"),
            )
        ):
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "authority context changed fields outside the declared gap"
            )
        context_sha = _sha256_bytes(
            _canonical_json(repaired_context).encode("ascii")
        )
        if authority_episode.get("episode_context_sha256") != context_sha:
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "authority context hash drifted"
            )
        context_hashes[episode_id] = context_sha
        prepared["episode_context"] = copy.deepcopy(dict(repaired_context))
        labels = authority_episode.get("reference_labels")
        if not isinstance(labels, list):
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "authority reference-label list is malformed"
            )
        label_sha = _sha256_bytes(_canonical_json(labels).encode("ascii"))
        if authority_episode.get("reference_labels_sha256") != label_sha:
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "authority reference-label hash drifted"
            )
        episode_repaired: list[str] = []
        episode_preserved: list[str] = []
        for label in labels:
            if not isinstance(label, dict):
                raise CanonicalV31Epoch7AuthorityBridgeError(
                    "authority reference label is malformed"
                )
            segment_id = str(label.get("segment_id") or "")
            diagnostic = diagnostics.get(segment_id)
            text = text_by_segment.get(segment_id)
            if (
                diagnostic is None
                or diagnostic.get("episode_id") != episode_id
                or not isinstance(text, str)
                or segment_id in labels_by_segment
            ):
                raise CanonicalV31Epoch7AuthorityBridgeError(
                    "authority reference membership drifted"
                )
            try:
                validate_label_output(
                    adapter.CANONICAL_LABEL_PACK,
                    label,
                    segment_text=text,
                )
            except ValidationError as exc:
                raise CanonicalV31Epoch7AuthorityBridgeError(
                    "authority reference label failed canonical validation"
                ) from exc
            if diagnostic.get("canonical_v31_valid") is True:
                if _canonical_json(label) != _canonical_json(
                    original_valid_labels.get(segment_id)
                ):
                    raise CanonicalV31Epoch7AuthorityBridgeError(
                        "authority changed a previously valid reference label"
                    )
                episode_preserved.append(segment_id)
                preserved_ids.append(segment_id)
            else:
                episode_repaired.append(segment_id)
                repaired_ids.append(segment_id)
            labels_by_segment[segment_id] = copy.deepcopy(label)
        if (
            authority_episode.get("repaired_reference_segment_ids")
            != episode_repaired
            or authority_episode.get("preserved_valid_reference_segment_ids")
            != episode_preserved
        ):
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "authority reference partition drifted"
            )
    expected_segment_order = [str(row["segment_id"]) for row in rows]
    if list(labels_by_segment) != expected_segment_order:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority reference order or coverage drifted"
        )
    expected_repaired = [
        segment_id
        for segment_id in expected_segment_order
        if diagnostics[segment_id].get("canonical_v31_valid") is not True
    ]
    expected_preserved = [
        segment_id
        for segment_id in expected_segment_order
        if diagnostics[segment_id].get("canonical_v31_valid") is True
    ]
    if repaired_ids != expected_repaired or preserved_ids != expected_preserved:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority global reference partition drifted"
        )

    repaired_source = copy.deepcopy(dict(source))
    repaired_source.update(
        {
            "schema_version": BRIDGED_SOURCE_VERSION,
            "state": "verified_read_only_source_plus_checksum_bound_llm_authority",
            "predecessor_source_binding": copy.deepcopy(
                dict(predecessor_source_record)
            ),
            "authority_execution_receipt": copy.deepcopy(
                dict(authority_records["receipt"])
            ),
            "merged_authority_output": copy.deepcopy(
                dict(authority_records["merged_output"])
            ),
            "predecessor_authority_semantic_model_call_count": int(
                authority_receipt["semantic_model_call_count"]
            ),
            "predecessor_authority_semantic_retry_count": int(
                authority_receipt["semantic_retry_count"]
            ),
        }
    )
    repaired_context_rows = []
    for row in source["context_authority"]:
        updated = copy.deepcopy(dict(row))
        updated["pre_authority_missing_required_fields"] = copy.deepcopy(
            row["missing_required_fields"]
        )
        updated["pre_authority_invalid_required_fields"] = copy.deepcopy(
            row["invalid_required_fields"]
        )
        updated["missing_required_fields"] = []
        updated["invalid_required_fields"] = []
        updated["authority_episode_context_sha256"] = context_hashes[
            str(row["episode_id"])
        ]
        repaired_context_rows.append(updated)
    repaired_source["context_authority"] = repaired_context_rows
    repaired_reference_rows = []
    for row in source["canonical_reference_validation"]:
        updated = copy.deepcopy(dict(row))
        updated["pre_authority_canonical_v31_valid"] = row["canonical_v31_valid"]
        updated["pre_authority_diagnostic_path"] = row["diagnostic_path"]
        updated["canonical_v31_valid"] = True
        updated["diagnostic_path"] = None
        updated["authority_label_sha256"] = _sha256_bytes(
            _canonical_json(labels_by_segment[str(row["segment_id"])]).encode("ascii")
        )
        repaired_reference_rows.append(updated)
    repaired_source["canonical_reference_validation"] = repaired_reference_rows
    if input_package._blocking_conditions(repaired_source):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority-repaired source remains blocked"
        )
    repaired_private = copy.deepcopy(dict(private))
    repaired_private["prepared_episodes"] = prepared_episodes
    repaired_private["prepared_by_episode"] = prepared_by_id
    repaired_private["validated_labels"] = labels_by_segment
    preservation = {
        "episode_ids": source_episode_ids,
        "segment_ids": expected_segment_order,
        "repaired_reference_segment_ids": repaired_ids,
        "preserved_valid_reference_segment_ids": preserved_ids,
        "context_episode_ids": list(context_hashes),
        "repaired_reference_count": len(repaired_ids),
        "preserved_reference_count": len(preserved_ids),
        "semantic_deterministic_defaults": {},
        "semantic_deterministic_pruning": False,
    }
    return repaired_source, repaired_private, preservation


def _verify_materialized_package(
    *,
    plan: Mapping[str, Any],
    package_root: Path,
    repaired_source: Mapping[str, Any],
    repaired_private: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        receipt = input_package.verify_input_package(
            package_root, project_root=plan["project_root"]
        )
    except input_package.CanonicalV31Epoch7InputPackageError as exc:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "materialized input package failed verification"
        ) from exc
    if (
        receipt.get("state") != "passed"
        or receipt.get("case_count") != plan["contract"]["case_count"]
        or receipt.get("episode_count") != plan["contract"]["episode_count"]
        or receipt.get("semantic_model_call_count") != 0
        or receipt.get("extraction_authorized") is not False
        or receipt.get("production_mutated") is not False
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "materialized input package contract drifted"
        )
    source_path = _verify_record(
        receipt.get("source_binding"),
        label="materialized source binding",
        allowed_root=package_root,
    )
    if _load_object(source_path, label="materialized source binding") != dict(
        repaired_source
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "materialized source binding differs from authority merge"
        )
    references = _load_object(
        _verify_record(
            receipt.get("shared_reference_seed"),
            label="materialized shared reference",
            allowed_root=package_root,
        ),
        label="materialized shared reference",
    )
    expected_labels = repaired_private["validated_labels"]
    observed_ids = [str(row.get("segment_id") or "") for row in references["references"]]
    if observed_ids != list(expected_labels) or any(
        _canonical_json(row.get("label"))
        != _canonical_json(expected_labels[str(row["segment_id"])])
        for row in references["references"]
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "materialized shared reference differs from authority merge"
        )
    episodes = _load_array(
        _verify_record(
            receipt.get("episodes"),
            label="materialized episodes",
            allowed_root=package_root,
        ),
        label="materialized episodes",
    )
    prepared_by_id = repaired_private["prepared_by_episode"]
    for episode in episodes:
        episode_id = str(episode["episode_id"])
        prepared = prepared_by_id.get(episode_id)
        if prepared is None or any(
            _canonical_json(episode.get(field))
            != _canonical_json(
                input_package._canonical_context(
                    legacy_episode=next(
                        row
                        for row in repaired_private["manifest"]["episodes"]
                        if str(row["episode_id"]) == episode_id
                    ),
                    prepared_episode=prepared,
                )[field]
            )
            for field in input_package._CONTEXT_FIELDS
        ):
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "materialized episode context differs from authority merge"
            )
    return receipt


def _handoff_payload(
    *,
    plan: Mapping[str, Any],
    authority_records: Mapping[str, Any],
    package_receipt: Mapping[str, Any],
    preservation: Mapping[str, Any],
) -> dict[str, Any]:
    contract = plan["contract"]
    package_root = plan["input_package_root"]
    return {
        "schema_version": HANDOFF_VERSION,
        "state": "ready_for_separately_authorized_epoch7_controller_plan",
        "bridge_plan_contract": copy.deepcopy(plan["contract_record"]),
        "authority_execution_receipt": copy.deepcopy(authority_records["receipt"]),
        "merged_authority_output": copy.deepcopy(
            authority_records["merged_output"]
        ),
        "input_package_receipt": _record(
            package_root / input_package.RECEIPT_FILENAME
        ),
        "manifest": copy.deepcopy(package_receipt["canonical_manifest"]),
        "episodes": copy.deepcopy(package_receipt["episodes"]),
        "capacity_policy": copy.deepcopy(package_receipt["capacity_policy"]),
        "dry_preflight": copy.deepcopy(package_receipt["dry_preflight"]),
        "precommit": copy.deepcopy(package_receipt["precommit"]),
        "preservation_receipt": copy.deepcopy(dict(preservation)),
        "future_controller_root": contract["future_controller_root"],
        "future_extraction_root": contract["future_extraction_root"],
        "controller_module": copy.deepcopy(
            contract["runtime_dependencies"]["epoch7_controller"]
        ),
        "controller_authorization_present": False,
        "extraction_authorized": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_model_call_count_started_by_bridge": 0,
        "semantic_retry_count_started_by_bridge": 0,
    }


def _materialization_receipt_payload(
    *,
    plan: Mapping[str, Any],
    authority_receipt: Mapping[str, Any],
    authority_records: Mapping[str, Any],
    handoff_record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": MATERIALIZATION_RECEIPT_VERSION,
        "state": "passed",
        "terminal_reason": "authority_output_materialized_into_verified_epoch7_inputs",
        "materialization_root": str(plan["materialization_root"]),
        "bridge_plan_contract": copy.deepcopy(plan["contract_record"]),
        "authority_execution_receipt": copy.deepcopy(authority_records["receipt"]),
        "merged_authority_output": copy.deepcopy(
            authority_records["merged_output"]
        ),
        "input_package_receipt": _record(
            plan["input_package_root"] / input_package.RECEIPT_FILENAME
        ),
        "controller_input_handoff": copy.deepcopy(dict(handoff_record)),
        "predecessor_authority_semantic_model_call_count": int(
            authority_receipt["semantic_model_call_count"]
        ),
        "predecessor_authority_semantic_retry_count": int(
            authority_receipt["semantic_retry_count"]
        ),
        "predecessor_authority_measured_usage": copy.deepcopy(
            authority_receipt["measured_usage"]
        ),
        "semantic_model_call_count_started_by_bridge": 0,
        "semantic_retry_count_started_by_bridge": 0,
        "controller_authorization_present": False,
        "extraction_authorized": False,
        "quality_authorized": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def materialize_authority_inputs(
    *,
    plan_root: Path = DEFAULT_PLAN_ROOT,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    plan = verify_bridge_plan(plan_root, project_root=project_root)
    authority_receipt, merged, authority_records = _load_passed_authority(plan)
    source, private = _rebuild_original_source_private(plan)
    repaired_source, repaired_private, preservation = _apply_authority_output(
        source=source,
        private=private,
        merged=merged,
        authority_records=authority_records,
        authority_receipt=authority_receipt,
        predecessor_source_record=plan["contract"]["waiting_source_binding"],
    )
    materialization_root = plan["materialization_root"]
    package_root = plan["input_package_root"]
    if materialization_root.exists() and (
        materialization_root.is_symlink() or not materialization_root.is_dir()
    ):
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "materialization root is not a real directory"
        )
    materialization_root.mkdir(parents=True, exist_ok=True)
    if not package_root.exists():
        input_package._build_ready_package(
            root=package_root,
            source=repaired_source,
            private=repaired_private,
            capacity_policy_path=_verify_record(
                plan["contract"]["capacity_policy"], label="capacity policy"
            ),
            project_root=plan["project_root"],
        )
    package_receipt = _verify_materialized_package(
        plan=plan,
        package_root=package_root,
        repaired_source=repaired_source,
        repaired_private=repaired_private,
    )
    handoff = _handoff_payload(
        plan=plan,
        authority_records=authority_records,
        package_receipt=package_receipt,
        preservation=preservation,
    )
    handoff_path = materialization_root / HANDOFF_FILENAME
    if handoff_path.is_file():
        if _load_object(handoff_path, label="controller input handoff") != handoff:
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "existing controller input handoff drifted"
            )
    else:
        _write_immutable_json(handoff_path, handoff)
    handoff_record = _record(handoff_path, allowed_root=materialization_root)
    receipt = _materialization_receipt_payload(
        plan=plan,
        authority_receipt=authority_receipt,
        authority_records=authority_records,
        handoff_record=handoff_record,
    )
    receipt_path = materialization_root / MATERIALIZATION_RECEIPT_FILENAME
    terminal_path = materialization_root / MATERIALIZATION_TERMINAL_FILENAME
    if receipt_path.is_file() != terminal_path.is_file():
        source_path = receipt_path if receipt_path.is_file() else terminal_path
        target_path = terminal_path if receipt_path.is_file() else receipt_path
        if _load_object(source_path, label="single materialization terminal") != receipt:
            raise CanonicalV31Epoch7AuthorityBridgeError(
                "single materialization terminal drifted"
            )
        _write_immutable_bytes(target_path, source_path.read_bytes())
    elif not receipt_path.is_file():
        raw = _pretty_json(receipt).encode("ascii")
        _write_immutable_bytes(receipt_path, raw)
        _write_immutable_bytes(terminal_path, raw)
    return verify_materialization(plan_root, project_root=project_root)["receipt"]


def verify_materialization(
    plan_root: Path = DEFAULT_PLAN_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    plan = verify_bridge_plan(plan_root, project_root=project_root)
    authority_receipt, merged, authority_records = _load_passed_authority(plan)
    materialization_root = plan["materialization_root"]
    if not materialization_root.is_dir() or materialization_root.is_symlink():
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "materialization root is unavailable"
        )
    required = {
        input_package.RECEIPT_FILENAME,
        input_package.TERMINAL_FILENAME,
        input_package.SOURCE_BINDING_FILENAME,
        input_package.EPISODES_FILENAME,
        input_package.MANIFEST_FILENAME,
        input_package.REFERENCE_FILENAME,
        input_package.CAPACITY_POLICY_FILENAME,
        input_package.PREFLIGHT_FILENAME,
        input_package.PRECOMMIT_FILENAME,
        input_package.CONTEXT_DIRECTORY,
    }
    if {path.name for path in plan["input_package_root"].iterdir()} != required:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "materialized input package artifact set drifted"
        )
    if {path.name for path in materialization_root.iterdir()} != {
        "input-package",
        HANDOFF_FILENAME,
        MATERIALIZATION_RECEIPT_FILENAME,
        MATERIALIZATION_TERMINAL_FILENAME,
    }:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority materialization artifact set drifted"
        )
    source, private = _rebuild_original_source_private(plan)
    repaired_source, repaired_private, preservation = _apply_authority_output(
        source=source,
        private=private,
        merged=merged,
        authority_records=authority_records,
        authority_receipt=authority_receipt,
        predecessor_source_record=plan["contract"]["waiting_source_binding"],
    )
    package_receipt = _verify_materialized_package(
        plan=plan,
        package_root=plan["input_package_root"],
        repaired_source=repaired_source,
        repaired_private=repaired_private,
    )
    handoff_path = materialization_root / HANDOFF_FILENAME
    handoff_record = _record(handoff_path, allowed_root=materialization_root)
    expected_handoff = _handoff_payload(
        plan=plan,
        authority_records=authority_records,
        package_receipt=package_receipt,
        preservation=preservation,
    )
    if _load_object(handoff_path, label="controller input handoff") != expected_handoff:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "controller input handoff drifted"
        )
    receipt_path = materialization_root / MATERIALIZATION_RECEIPT_FILENAME
    terminal_path = materialization_root / MATERIALIZATION_TERMINAL_FILENAME
    receipt = _load_object(receipt_path, label="materialization receipt")
    terminal = _load_object(terminal_path, label="materialization terminal")
    if receipt != terminal or receipt_path.read_bytes() != terminal_path.read_bytes():
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "materialization terminal mirrors drifted"
        )
    expected_receipt = _materialization_receipt_payload(
        plan=plan,
        authority_receipt=authority_receipt,
        authority_records=authority_records,
        handoff_record=handoff_record,
    )
    if receipt != expected_receipt:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "materialization receipt drifted"
        )
    return {
        "receipt": copy.deepcopy(receipt),
        "handoff": copy.deepcopy(expected_handoff),
        "input_package_receipt": copy.deepcopy(package_receipt),
    }


def status_bridge(
    plan_root: Path = DEFAULT_PLAN_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    try:
        plan = verify_bridge_plan(plan_root, project_root=project_root)
    except CanonicalV31Epoch7AuthorityBridgeError as exc:
        return {
            "schema_version": PLAN_RECEIPT_VERSION,
            "state": "absent_or_invalid",
            "reason": str(exc),
            "semantic_model_call_count_started_by_bridge": 0,
            "production_mutated": False,
        }
    if (plan["materialization_root"] / MATERIALIZATION_RECEIPT_FILENAME).is_file():
        result = verify_materialization(plan_root, project_root=project_root)
        return {
            "schema_version": MATERIALIZATION_RECEIPT_VERSION,
            "state": result["receipt"]["state"],
            "terminal_reason": result["receipt"]["terminal_reason"],
            "semantic_model_call_count_started_by_bridge": 0,
            "controller_authorization_present": False,
            "production_mutated": False,
        }
    authority_receipt_path = Path(
        plan["contract"]["expected_authority_execution_receipt_path"]
    )
    if not authority_receipt_path.is_file():
        return {
            "schema_version": PLAN_RECEIPT_VERSION,
            "state": "waiting",
            "reason": "passed_authority_execution_receipt_required",
            "semantic_model_call_count_started_by_bridge": 0,
            "controller_authorization_present": False,
            "production_mutated": False,
        }
    try:
        authority_receipt = authority_runtime.verify_execution_receipt(
            plan["authority_root"]
        )
    except authority_runtime.CanonicalV31Epoch7AuthorityRuntimeError as exc:
        raise CanonicalV31Epoch7AuthorityBridgeError(
            "authority execution receipt failed status verification"
        ) from exc
    return {
        "schema_version": PLAN_RECEIPT_VERSION,
        "state": (
            "ready_for_zero_call_materialization"
            if authority_receipt.get("state") == "passed"
            else "waiting"
        ),
        "reason": (
            "passed_authority_receipt_available"
            if authority_receipt.get("state") == "passed"
            else "authority_execution_not_passed"
        ),
        "authority_state": authority_receipt.get("state"),
        "semantic_model_call_count_started_by_bridge": 0,
        "controller_authorization_present": False,
        "production_mutated": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Freeze and verify the zero-call bridge from canonical epoch-7 input "
            "authority to controller inputs."
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    plan.add_argument(
        "--materialization-root", type=Path, default=DEFAULT_MATERIALIZATION_ROOT
    )
    plan.add_argument("--input-package-root", type=Path, default=DEFAULT_INPUT_PACKAGE_ROOT)
    plan.add_argument("--controller-root", type=Path, default=DEFAULT_FUTURE_CONTROLLER_ROOT)
    plan.add_argument("--extraction-root", type=Path, default=DEFAULT_FUTURE_EXTRACTION_ROOT)
    plan.add_argument("--database", type=Path, default=input_package.DEFAULT_DATABASE_PATH)
    plan.add_argument("--waiting-input-root", type=Path, default=input_package.DEFAULT_ROOT)
    plan.add_argument("--authority-root", type=Path, default=authority_runtime.DEFAULT_ROOT)
    status = commands.add_parser("status")
    status.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    materialize = commands.add_parser("materialize")
    materialize.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    verify = commands.add_parser("verify")
    verify.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "plan":
            result = freeze_bridge_plan(
                plan_root=args.plan_root,
                materialization_root=args.materialization_root,
                future_input_package_root=args.input_package_root,
                future_controller_root=args.controller_root,
                future_extraction_root=args.extraction_root,
                database_path=args.database,
                input_root=args.waiting_input_root,
                authority_root=args.authority_root,
            )
        elif args.command == "status":
            result = status_bridge(args.plan_root)
        elif args.command == "materialize":
            result = materialize_authority_inputs(plan_root=args.plan_root)
        else:
            result = verify_materialization(args.plan_root)
    except CanonicalV31Epoch7AuthorityBridgeError as exc:
        print(_pretty_json({"ok": False, "error": str(exc)}), end="", file=sys.stderr)
        return 2
    print(_pretty_json({"ok": True, "result": result}), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
