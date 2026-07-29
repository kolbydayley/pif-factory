from __future__ import annotations

"""Strict provenance validation for managed app-server semantic checkpoints."""

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from .codex_app_server import (
    APP_SERVER_CLIENT_VERSION,
    PINNED_CODEX_CLI_VERSION,
    PROTOCOL_SCHEMA_SHA256,
    TURN_SIDECAR_SCHEMA_VERSION,
)
from .util import sha256_text


_USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)

INSTRUCTION_CONTRACT_ARTIFACT_VERSION = "pif_app_server_instruction_contract_v2"
HOLDOUT_LEAF_BINDING_VERSION = "pif_app_server_holdout_leaf_binding_v1"
CURRENT_INSTRUCTION_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "work"
    / "app-server-development-v2"
    / "instruction-contract-v2.json"
)
# Filled from the checked-in canonical artifact.  These values are deliberately
# code-bound so changing the contract requires an explicit versioned code edit.
CURRENT_INSTRUCTION_CONTRACT_SHA256 = (
    "f4a161e2453e54a286c1835441275299ef7eabc9c0931ad02dc9e36fd71df4f3"
)
CURRENT_INSTRUCTION_CONTRACT_SIZE_BYTES = 3033


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _valid_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def verify_instruction_contract(
    run_spec_path: Path | str | None = None,
    *,
    expected_artifact_sha256: str | None = None,
    expected_artifact_size_bytes: int | None = None,
) -> dict[str, Any]:
    """Verify one exact instruction contract and its model-visible boundary.

    The default is the current standalone, versioned contract.  An explicit
    path may still point at a historical run spec because those artifacts are
    immutable evidence, but the historical ``run-spec-v2.json`` is no longer
    the implicit source of current holdout instructions.
    """

    uses_current_default = run_spec_path is None
    path = Path(
        CURRENT_INSTRUCTION_CONTRACT_PATH if uses_current_default else run_spec_path
    ).expanduser().resolve()
    if uses_current_default:
        if expected_artifact_sha256 is None:
            expected_artifact_sha256 = CURRENT_INSTRUCTION_CONTRACT_SHA256
        if expected_artifact_size_bytes is None:
            expected_artifact_size_bytes = CURRENT_INSTRUCTION_CONTRACT_SIZE_BYTES
    if (expected_artifact_sha256 is None) != (expected_artifact_size_bytes is None):
        raise ValueError(
            "instruction contract artifact hash and size must be provided together"
        )
    if expected_artifact_sha256 is not None and not _valid_sha256(
        expected_artifact_sha256
    ):
        raise ValueError("instruction contract artifact hash must be lowercase SHA-256")
    if expected_artifact_size_bytes is not None and (
        isinstance(expected_artifact_size_bytes, bool)
        or not isinstance(expected_artifact_size_bytes, int)
        or expected_artifact_size_bytes < 1
    ):
        raise ValueError("instruction contract artifact size must be a positive integer")
    if not path.is_file():
        raise ValueError("instruction contract artifact is missing")

    observed_artifact_sha256 = sha256_file(path)
    observed_artifact_size_bytes = path.stat().st_size
    if (
        expected_artifact_sha256 is not None
        and observed_artifact_sha256 != expected_artifact_sha256
    ):
        raise ValueError("instruction contract artifact hash drift")
    if (
        expected_artifact_size_bytes is not None
        and observed_artifact_size_bytes != expected_artifact_size_bytes
    ):
        raise ValueError("instruction contract artifact size drift")
    try:
        spec = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("instruction contract artifact is not valid JSON") from exc
    if not isinstance(spec, dict):
        raise ValueError("instruction contract artifact must be a JSON object")
    if (
        uses_current_default
        and spec.get("schema_version") != INSTRUCTION_CONTRACT_ARTIFACT_VERSION
    ):
        raise ValueError("current instruction contract artifact schema is unsupported")
    schema_version = spec.get("schema_version")
    contract = spec.get("instruction_contract") if isinstance(spec, dict) else None
    sources = contract.get("sources") if isinstance(contract, dict) else None
    if not isinstance(sources, list) or not sources:
        raise ValueError("instruction contract artifact has no instruction sources")
    paths = []
    source_content_sha256s: list[str] = []
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError("frozen instruction source is malformed")
        source_path_value = source.get("path")
        if not isinstance(source_path_value, str) or not source_path_value:
            raise ValueError("frozen instruction source path is malformed")
        source_path = Path(source_path_value).expanduser().resolve()
        if not source_path.is_file():
            raise ValueError("frozen instruction source path is missing")
        if schema_version == INSTRUCTION_CONTRACT_ARTIFACT_VERSION:
            # The strict overlay gives project documents a zero-byte inclusion
            # budget.  The historical checksum remains useful provenance, but
            # re-hashing mutable ambient policy would validate bytes that the
            # model can never see and would make an immutable semantic request
            # depend on unrelated operations-policy edits.
            expected_sha = source.get("historical_content_sha256")
            expected_size = source.get("historical_size_bytes")
            if (
                source.get("content_byte_budget") != 0
                or source.get("content_verification")
                != "historical_metadata_only_zero_byte_budget"
                or not _valid_sha256(expected_sha)
                or isinstance(expected_size, bool)
                or not isinstance(expected_size, int)
                or expected_size < 0
            ):
                raise ValueError("zero-byte instruction source metadata is malformed")
        else:
            # Explicit historical v1/run-spec contracts remain verifiable for
            # evidence readers, including their original content binding.
            expected_sha = source.get("content_sha256")
            expected_size = source.get("size_bytes")
            if (
                not _valid_sha256(expected_sha)
                or sha256_file(source_path) != expected_sha
                or isinstance(expected_size, bool)
                or not isinstance(expected_size, int)
                or expected_size < 0
                or source_path.stat().st_size != expected_size
            ):
                raise ValueError("frozen instruction source content drift")
        paths.append(str(source_path))
        source_content_sha256s.append(str(expected_sha))
    if len(paths) != len(set(paths)):
        raise ValueError("frozen instruction source paths are duplicated")
    observed_set_sha = sha256_text(canonical_json(paths))
    expected_set_sha = contract.get("expected_path_set_sha256")
    if not _valid_sha256(expected_set_sha) or observed_set_sha != expected_set_sha:
        raise ValueError("frozen instruction source path set drift")

    strict_config_overlay: dict[str, Any] | None = None
    strict_overlay_record: dict[str, Any] | None = None
    if schema_version == INSTRUCTION_CONTRACT_ARTIFACT_VERSION:
        if (
            contract.get("project_doc_max_bytes") != 0
            or contract.get("model_visible_project_instruction_bytes") != 0
        ):
            raise ValueError("current instruction contract is not a zero-byte overlay")
        overlay_record = contract.get("strict_config_overlay")
        if not isinstance(overlay_record, dict):
            raise ValueError("current instruction contract has no strict overlay")
        overlay_path_value = overlay_record.get("artifact_path")
        if not isinstance(overlay_path_value, str) or not overlay_path_value:
            raise ValueError("strict overlay artifact path is malformed")
        overlay_path = Path(overlay_path_value).expanduser()
        if not overlay_path.is_absolute():
            overlay_path = Path(__file__).resolve().parents[1] / overlay_path
        overlay_path = overlay_path.resolve()
        overlay_sha = overlay_record.get("artifact_sha256")
        overlay_size = overlay_record.get("size_bytes")
        if (
            not overlay_path.is_file()
            or not _valid_sha256(overlay_sha)
            or sha256_file(overlay_path) != overlay_sha
            or isinstance(overlay_size, bool)
            or not isinstance(overlay_size, int)
            or overlay_size < 1
            or overlay_path.stat().st_size != overlay_size
        ):
            raise ValueError("strict overlay artifact drift")
        try:
            overlay_value = json.loads(overlay_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("strict overlay artifact is not valid JSON") from exc
        if (
            not isinstance(overlay_value, dict)
            or overlay_value.get("project_doc_max_bytes") != 0
            or overlay_value != overlay_record.get("config")
        ):
            raise ValueError("strict overlay zero-byte configuration drift")
        strict_config_overlay = overlay_value
        strict_overlay_record = {
            "artifact_path": str(overlay_path),
            "artifact_sha256": str(overlay_sha),
            "size_bytes": int(overlay_size),
        }

        provenance = contract.get("historical_source_metadata_provenance")
        if not isinstance(provenance, dict):
            raise ValueError("historical instruction metadata provenance is missing")
        provenance_path_value = provenance.get("artifact_path")
        if not isinstance(provenance_path_value, str) or not provenance_path_value:
            raise ValueError("historical instruction metadata path is malformed")
        provenance_path = Path(provenance_path_value).expanduser()
        if not provenance_path.is_absolute():
            provenance_path = Path(__file__).resolve().parents[1] / provenance_path
        provenance_path = provenance_path.resolve()
        provenance_sha = provenance.get("artifact_sha256")
        provenance_size = provenance.get("size_bytes")
        if (
            not provenance_path.is_file()
            or not _valid_sha256(provenance_sha)
            or sha256_file(provenance_path) != provenance_sha
            or isinstance(provenance_size, bool)
            or not isinstance(provenance_size, int)
            or provenance_size < 1
            or provenance_path.stat().st_size != provenance_size
        ):
            raise ValueError("historical instruction metadata provenance drift")
        try:
            historical = json.loads(provenance_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("historical instruction metadata is not valid JSON") from exc
        expected_historical_records = [
            {
                "path": path,
                "sha256": source_content_sha256s[index],
                "size_bytes": int(sources[index]["historical_size_bytes"]),
            }
            for index, path in enumerate(paths)
        ]
        if (
            provenance.get("record_field") != "effective_instruction_source_records"
            or not isinstance(historical, dict)
            or historical.get("effective_instruction_source_paths") != paths
            or historical.get("effective_instruction_source_records")
            != expected_historical_records
            or historical.get("effective_instruction_sources_sha256")
            != observed_set_sha
        ):
            raise ValueError("historical instruction source metadata drift")
    return {
        "artifact_schema_version": schema_version,
        "contract_artifact_path": str(path),
        "contract_artifact_sha256": observed_artifact_sha256,
        "contract_artifact_size_bytes": observed_artifact_size_bytes,
        # Retained for older readers that named the enclosing contract a run spec.
        "run_spec_path": str(path),
        "run_spec_sha256": observed_artifact_sha256,
        "expected_path_set_sha256": observed_set_sha,
        "instruction_sources_count": len(paths),
        "instruction_source_paths": paths,
        "source_content_sha256s": source_content_sha256s,
        "project_doc_max_bytes": (
            0 if schema_version == INSTRUCTION_CONTRACT_ARTIFACT_VERSION else None
        ),
        "model_visible_project_instruction_bytes": (
            0 if schema_version == INSTRUCTION_CONTRACT_ARTIFACT_VERSION else None
        ),
        "strict_config_overlay": strict_config_overlay,
        "strict_config_overlay_artifact": strict_overlay_record,
    }


def verify_label_pack_contract(label_pack: str = "ai_discourse_v3_1") -> dict[str, Any]:
    if not label_pack or "/" in label_pack or ".." in label_pack:
        raise ValueError("label-pack contract name is invalid")
    root = Path(__file__).resolve().parents[1] / "label_packs" / label_pack
    artifacts = {}
    for name in ("prompt.md", "schema.json", "codebook.md"):
        path = (root / name).resolve()
        if not path.is_file():
            raise ValueError(f"frozen label-pack artifact is missing: {name}")
        artifacts[name] = {
            "path": str(path),
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
    return {"label_pack": label_pack, "artifacts": artifacts}


def _valid_usage(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        raise ValueError("managed app-server sidecar has no measured usage")
    usage = {}
    for field in _USAGE_FIELDS:
        item = value.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise ValueError(f"managed app-server usage is invalid: {field}")
        usage[field] = item
    if usage["cached_input_tokens"] > usage["input_tokens"]:
        raise ValueError("managed app-server cached input exceeds input")
    if usage["reasoning_output_tokens"] > usage["output_tokens"]:
        raise ValueError("managed app-server reasoning output exceeds output")
    if usage["total_tokens"] != usage["input_tokens"] + usage["output_tokens"]:
        raise ValueError("managed app-server total usage is inconsistent")
    return usage


def _artifact_record(path: Path) -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve(strict=True)
    if not resolved.is_file():
        raise ValueError(f"managed app-server leaf artifact is missing: {resolved}")
    return {
        "path": str(resolved),
        "sha256": sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def validate_managed_sidecar_execution_lineage(
    *,
    sidecar: Mapping[str, Any],
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any],
) -> None:
    """Validate exact overlay, instruction-source, runtime, CLI, and protocol evidence."""

    contract_binding = execution_lineage.get("instruction_contract")
    overlay_binding = execution_lineage.get("strict_config_overlay")
    runtime_binding = execution_lineage.get("runtime")
    auth_binding = execution_lineage.get("managed_chatgpt_auth")
    overlay = instruction_contract.get("strict_config_overlay")
    overlay_artifact = instruction_contract.get("strict_config_overlay_artifact")
    if (
        not isinstance(contract_binding, Mapping)
        or not isinstance(overlay_binding, Mapping)
        or not isinstance(runtime_binding, Mapping)
        or not isinstance(auth_binding, Mapping)
        or not isinstance(overlay, Mapping)
        or not isinstance(overlay_artifact, Mapping)
        or contract_binding.get("schema_version")
        != instruction_contract.get("artifact_schema_version")
        or contract_binding.get("artifact_sha256")
        != instruction_contract.get("contract_artifact_sha256")
        or contract_binding.get("artifact_size_bytes")
        != instruction_contract.get("contract_artifact_size_bytes")
        or contract_binding.get("instruction_sources_sha256")
        != instruction_contract.get("expected_path_set_sha256")
        or contract_binding.get("instruction_sources_count")
        != instruction_contract.get("instruction_sources_count")
        or overlay_binding.get("project_doc_max_bytes") != 0
        or overlay_binding.get("model_visible_project_instruction_bytes") != 0
        or instruction_contract.get("project_doc_max_bytes") != 0
        or instruction_contract.get("model_visible_project_instruction_bytes") != 0
        or overlay.get("project_doc_max_bytes") != 0
        or overlay_binding.get("config_sha256")
        != hashlib.sha256(canonical_json(overlay).encode("utf-8")).hexdigest()
        or overlay_binding.get("artifact_path")
        != overlay_artifact.get("artifact_path")
        or overlay_binding.get("artifact_sha256")
        != overlay_artifact.get("artifact_sha256")
        or overlay_binding.get("artifact_size_bytes")
        != overlay_artifact.get("size_bytes")
        or runtime_binding.get("pinned_codex_cli_version")
        != PINNED_CODEX_CLI_VERSION
        or runtime_binding.get("app_server_client_version")
        != APP_SERVER_CLIENT_VERSION
        or runtime_binding.get("protocol_schema_sha256")
        != PROTOCOL_SCHEMA_SHA256
        or auth_binding
        != {
            "account_type": "chatgpt",
            "plan_type": "pro",
            "required_before_thread_or_turn": True,
        }
    ):
        raise ValueError("managed app-server expected execution lineage is malformed")

    required = {
        "client_version": APP_SERVER_CLIENT_VERSION,
        "cli_version": PINNED_CODEX_CLI_VERSION,
        "protocol_schema_sha256": PROTOCOL_SCHEMA_SHA256,
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "instruction_sources_sha256": instruction_contract.get(
            "expected_path_set_sha256"
        ),
        "instruction_sources_count": instruction_contract.get(
            "instruction_sources_count"
        ),
        "holdout_execution_lineage": dict(execution_lineage),
    }
    for key, expected in required.items():
        if sidecar.get(key) != expected:
            raise ValueError(f"managed app-server sidecar lineage mismatch: {key}")


def validate_completed_managed_sidecar(
    *,
    sidecar_path: Path,
    raw_output_path: Path,
    model: str,
    effort: str,
    thread_mode: str,
    batch_size: int,
    prompt: str,
    output_schema: Mapping[str, Any],
    base_instructions: str,
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, int]]:
    sidecar_file = Path(sidecar_path).expanduser().resolve()
    output_file = Path(raw_output_path).expanduser().resolve()
    try:
        sidecar = json.loads(sidecar_file.read_text(encoding="utf-8"))
        output_text = output_file.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("managed app-server checkpoint is unreadable") from exc
    if not isinstance(sidecar, dict):
        raise ValueError("managed app-server sidecar is not an object")
    required = {
        "schema_version": TURN_SIDECAR_SCHEMA_VERSION,
        "state": "completed",
        "status": "completed",
        "client_version": APP_SERVER_CLIENT_VERSION,
        "transport": "stdio",
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "model": model,
        "effort": effort,
        "thread_mode": thread_mode,
        "batch_size": batch_size,
        "prompt_sha256": sha256_text(prompt),
        "prompt_bytes": len(prompt.encode("utf-8")),
        "output_schema_sha256": sha256_text(canonical_json(output_schema)),
        "output_schema_bytes": len(canonical_json(output_schema).encode("utf-8")),
        "base_instructions_sha256": sha256_text(base_instructions),
        "base_instructions_bytes": len(base_instructions.encode("utf-8")),
        "instruction_sources_sha256": instruction_contract.get(
            "expected_path_set_sha256"
        ),
        "instruction_sources_count": instruction_contract.get(
            "instruction_sources_count"
        ),
        "usage_complete": True,
    }
    for key, expected in required.items():
        if sidecar.get(key) != expected:
            raise ValueError(f"managed app-server sidecar mismatch: {key}")
    for key in ("thread_id", "turn_id", "app_server_user_agent"):
        if not isinstance(sidecar.get(key), str) or not sidecar.get(key):
            raise ValueError(f"managed app-server sidecar lacks exact {key}")
    if execution_lineage is not None:
        validate_managed_sidecar_execution_lineage(
            sidecar=sidecar,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
    declared_output_path = sidecar.get("output_path")
    if not isinstance(declared_output_path, str) or Path(declared_output_path).resolve() != output_file:
        raise ValueError("managed app-server sidecar output path drift")
    acceptable_hashes = {sha256_text(output_text)}
    if output_text.endswith("\n"):
        acceptable_hashes.add(sha256_text(output_text[:-1]))
    if sidecar.get("output_sha256") not in acceptable_hashes:
        raise ValueError("managed app-server sidecar output hash drift")
    usage = _valid_usage(sidecar.get("usage"))
    return sidecar, usage


def build_holdout_leaf_binding(
    *,
    sidecar_path: Path,
    prompt_path: Path,
    output_schema_path: Path,
    base_instructions_path: Path,
    raw_output_path: Path,
    model: str,
    effort: str,
    thread_mode: str,
    batch_size: int,
    prompt: str,
    output_schema: Mapping[str, Any],
    base_instructions: str,
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate and freeze the exact identity of one completed semantic leaf."""

    prompt_file = Path(prompt_path).expanduser().resolve(strict=True)
    schema_file = Path(output_schema_path).expanduser().resolve(strict=True)
    base_file = Path(base_instructions_path).expanduser().resolve(strict=True)
    if prompt_file.read_text(encoding="utf-8") != prompt:
        raise ValueError("managed app-server leaf prompt file differs from frozen prompt")
    try:
        stored_schema = json.loads(schema_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("managed app-server leaf schema file is unreadable") from exc
    if stored_schema != dict(output_schema):
        raise ValueError("managed app-server leaf schema file differs from frozen schema")
    if base_file.read_text(encoding="utf-8") != base_instructions:
        raise ValueError(
            "managed app-server leaf base-instructions file differs from frozen instructions"
        )
    sidecar, usage = validate_completed_managed_sidecar(
        sidecar_path=sidecar_path,
        raw_output_path=raw_output_path,
        model=model,
        effort=effort,
        thread_mode=thread_mode,
        batch_size=batch_size,
        prompt=prompt,
        output_schema=output_schema,
        base_instructions=base_instructions,
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    artifacts = {
        "sidecar": _artifact_record(sidecar_path),
        "prompt": _artifact_record(prompt_file),
        "structured_schema": _artifact_record(schema_file),
        "base_instructions": _artifact_record(base_file),
        "output": _artifact_record(raw_output_path),
    }
    identity = {
        "sidecar_path": artifacts["sidecar"]["path"],
        "prompt_path": artifacts["prompt"]["path"],
        "structured_schema_path": artifacts["structured_schema"]["path"],
        "base_instructions_path": artifacts["base_instructions"]["path"],
        "output_path": artifacts["output"]["path"],
        "thread_id": sidecar["thread_id"],
        "turn_id": sidecar["turn_id"],
    }
    return {
        "schema_version": HOLDOUT_LEAF_BINDING_VERSION,
        "leaf_identity_sha256": hashlib.sha256(
            canonical_json(identity).encode("utf-8")
        ).hexdigest(),
        "model": model,
        "effort": effort,
        "thread_mode": thread_mode,
        "batch_size": batch_size,
        "auth_type": "chatgpt",
        "plan_type": "pro",
        "client_version": APP_SERVER_CLIENT_VERSION,
        "cli_version": PINNED_CODEX_CLI_VERSION,
        "protocol_schema_sha256": PROTOCOL_SCHEMA_SHA256,
        "runtime": dict(execution_lineage["runtime"]),
        "strict_config_overlay": dict(execution_lineage["strict_config_overlay"]),
        "app_server_user_agent": sidecar["app_server_user_agent"],
        "thread_id": sidecar["thread_id"],
        "turn_id": sidecar["turn_id"],
        "prompt_sha256": sha256_text(prompt),
        "structured_schema_sha256": sha256_text(canonical_json(output_schema)),
        "base_instructions_sha256": sha256_text(base_instructions),
        "output_message_sha256": sidecar["output_sha256"],
        "instruction_sources_sha256": instruction_contract[
            "expected_path_set_sha256"
        ],
        "instruction_sources_count": instruction_contract[
            "instruction_sources_count"
        ],
        "instruction_contract_artifact_sha256": instruction_contract[
            "contract_artifact_sha256"
        ],
        "execution_lineage_sha256": hashlib.sha256(
            canonical_json(execution_lineage).encode("utf-8")
        ).hexdigest(),
        "usage": usage,
        "artifacts": artifacts,
    }


def validate_holdout_leaf_binding(
    binding: Mapping[str, Any],
    *,
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any],
    expected_model: str | None = None,
    expected_effort: str | None = None,
) -> dict[str, Any]:
    """Re-derive one leaf binding from its immutable artifact paths."""

    if not isinstance(binding, Mapping) or binding.get("schema_version") != HOLDOUT_LEAF_BINDING_VERSION:
        raise ValueError("managed app-server holdout leaf binding is malformed")
    if expected_model is not None and binding.get("model") != expected_model:
        raise ValueError("managed app-server holdout leaf model drift")
    if expected_effort is not None and binding.get("effort") != expected_effort:
        raise ValueError("managed app-server holdout leaf effort drift")
    artifacts = binding.get("artifacts")
    if not isinstance(artifacts, Mapping) or set(artifacts) != {
        "sidecar",
        "prompt",
        "structured_schema",
        "base_instructions",
        "output",
    }:
        raise ValueError("managed app-server holdout leaf artifact set is incomplete")
    paths: dict[str, Path] = {}
    for name, record in artifacts.items():
        if not isinstance(record, Mapping):
            raise ValueError("managed app-server holdout leaf artifact record is malformed")
        path = Path(str(record.get("path") or "")).expanduser().resolve()
        if (
            not path.is_file()
            or record.get("sha256") != sha256_file(path)
            or record.get("size_bytes") != path.stat().st_size
        ):
            raise ValueError(f"managed app-server holdout leaf artifact drift: {name}")
        paths[name] = path
    try:
        schema = json.loads(paths["structured_schema"].read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("managed app-server holdout leaf schema is unreadable") from exc
    rebuilt = build_holdout_leaf_binding(
        sidecar_path=paths["sidecar"],
        prompt_path=paths["prompt"],
        output_schema_path=paths["structured_schema"],
        base_instructions_path=paths["base_instructions"],
        raw_output_path=paths["output"],
        model=str(binding.get("model") or ""),
        effort=str(binding.get("effort") or ""),
        thread_mode=str(binding.get("thread_mode") or ""),
        batch_size=int(binding.get("batch_size") or 0),
        prompt=paths["prompt"].read_text(encoding="utf-8"),
        output_schema=schema,
        base_instructions=paths["base_instructions"].read_text(encoding="utf-8"),
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    if dict(binding) != rebuilt:
        raise ValueError("managed app-server holdout leaf binding drift")
    return rebuilt
