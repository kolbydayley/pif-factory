from __future__ import annotations

"""Verified managed-client construction for every frozen holdout phase.

This module deliberately imports no holdout phase or judge module.  It is the
single non-cyclic boundary that binds the current instruction-contract-v2,
epoch-4 zero-byte overlay, pinned Codex runtime, and protocol schema before a
default managed app-server process can be constructed.
"""

import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping

from . import app_server_expanded_cap_episode_batch as expanded_cap
from . import codex_app_server
from .app_server_capacity import (
    CapacityGatedCodexAppServerClient,
    capacity_gated_client_factory,
)
from .app_server_checkpoint import (
    INSTRUCTION_CONTRACT_ARTIFACT_VERSION,
    verify_instruction_contract,
)


HOLDOUT_EXECUTION_LINEAGE_VERSION = "pif_app_server_holdout_execution_lineage_v1"
HOLDOUT_SIDECAR_LINEAGE_FIELD = "holdout_execution_lineage"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.expanduser().resolve().open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def verified_holdout_execution_lineage(
    instruction_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return exact holdout process lineage after re-verifying every artifact."""

    verified = verify_instruction_contract()
    if instruction_contract is not None and dict(instruction_contract) != verified:
        raise ValueError(
            "holdout instruction contract differs from current verified contract"
        )
    if (
        verified.get("artifact_schema_version")
        != INSTRUCTION_CONTRACT_ARTIFACT_VERSION
        or verified.get("project_doc_max_bytes") != 0
        or verified.get("model_visible_project_instruction_bytes") != 0
    ):
        raise ValueError("holdout requires the exact zero-byte instruction-contract-v2")

    overlay = verified.get("strict_config_overlay")
    overlay_artifact = verified.get("strict_config_overlay_artifact")
    if (
        not isinstance(overlay, dict)
        or overlay.get("project_doc_max_bytes") != 0
        or not isinstance(overlay_artifact, dict)
    ):
        raise ValueError("holdout strict overlay is missing its zero-byte boundary")

    # _epoch4_snapshot checksum-verifies the immutable runtime lock, request
    # templates, overlay, and historical instruction-source path contract.
    snapshot = expanded_cap._epoch4_snapshot()  # noqa: SLF001
    source_contract = expanded_cap._epoch4_instruction_source_contract()  # noqa: SLF001
    epoch4_overlay = expanded_cap.verified_context_control_overlay()
    runtime_lock_path = Path(snapshot["lock_path"]).expanduser().resolve(strict=True)
    runtime_lock = snapshot["lock"]
    if _sha256_file(runtime_lock_path) != expanded_cap.EPOCH4_RUNTIME_LOCK_SHA256:
        raise ValueError("holdout epoch-4 runtime lock drift")
    runtime_overlay_record = runtime_lock.get("config_overlay")
    expected_overlay_record = {
        "path": overlay_artifact.get("artifact_path"),
        "sha256": overlay_artifact.get("artifact_sha256"),
        "size_bytes": overlay_artifact.get("size_bytes"),
    }
    if (
        overlay != epoch4_overlay
        or runtime_overlay_record != expected_overlay_record
        or verified.get("expected_path_set_sha256")
        != source_contract.get("effective_instruction_sources_sha256")
        or verified.get("instruction_sources_count")
        != source_contract.get("effective_instruction_sources_count")
        or verified.get("instruction_source_paths") != snapshot.get("source_paths")
    ):
        raise ValueError("holdout instruction contract differs from pinned epoch-4")

    pinned_codex = Path(expanded_cap.epoch4.PINNED_CODEX).expanduser().resolve(strict=True)
    pinned_record = _record(pinned_codex)
    if runtime_lock.get("pinned_codex_cli") != pinned_record:
        raise ValueError("holdout pinned Codex runtime drift")

    protocol_path = codex_app_server.PROTOCOL_SCHEMA_PATH.expanduser().resolve(strict=True)
    protocol_record = _record(protocol_path)
    if (
        codex_app_server.verify_protocol_schema() != protocol_record["sha256"]
        or protocol_record["sha256"] != codex_app_server.PROTOCOL_SCHEMA_SHA256
    ):
        raise ValueError("holdout app-server protocol schema drift")

    return {
        "schema_version": HOLDOUT_EXECUTION_LINEAGE_VERSION,
        "managed_chatgpt_auth": {
            "account_type": "chatgpt",
            "plan_type": "pro",
            "required_before_thread_or_turn": True,
        },
        "instruction_contract": {
            "schema_version": verified["artifact_schema_version"],
            "artifact_sha256": verified["contract_artifact_sha256"],
            "artifact_size_bytes": verified["contract_artifact_size_bytes"],
            "instruction_sources_sha256": verified["expected_path_set_sha256"],
            "instruction_sources_count": verified["instruction_sources_count"],
        },
        "strict_config_overlay": {
            "project_doc_max_bytes": 0,
            "model_visible_project_instruction_bytes": 0,
            "config_sha256": hashlib.sha256(
                _canonical_json(overlay).encode("utf-8")
            ).hexdigest(),
            "artifact_path": overlay_artifact["artifact_path"],
            "artifact_sha256": overlay_artifact["artifact_sha256"],
            "artifact_size_bytes": overlay_artifact["size_bytes"],
        },
        "runtime": {
            "epoch4_runtime_lock_path": str(runtime_lock_path),
            "epoch4_runtime_lock_sha256": expanded_cap.EPOCH4_RUNTIME_LOCK_SHA256,
            "epoch4_runtime_lock_size_bytes": runtime_lock_path.stat().st_size,
            "pinned_codex_path": pinned_record["path"],
            "pinned_codex_sha256": pinned_record["sha256"],
            "pinned_codex_size_bytes": pinned_record["size_bytes"],
            "pinned_codex_cli_version": codex_app_server.PINNED_CODEX_CLI_VERSION,
            "app_server_client_version": codex_app_server.APP_SERVER_CLIENT_VERSION,
            "protocol_schema_path": protocol_record["path"],
            "protocol_schema_sha256": protocol_record["sha256"],
            "protocol_schema_size_bytes": protocol_record["size_bytes"],
            "command": [
                pinned_record["path"],
                "app-server",
                "--stdio",
                "--strict-config",
            ],
        },
    }


class HoldoutExpandedCapCodexAppServerClient(
    expanded_cap.ExpandedCapBatchCodexAppServerClient
):
    """Pinned overlay-aware client that records the verified holdout lineage."""

    def __init__(
        self,
        *,
        instruction_contract: Mapping[str, Any],
        execution_lineage: Mapping[str, Any],
        **kwargs: Any,
    ) -> None:
        self._holdout_instruction_contract = copy.deepcopy(dict(instruction_contract))
        self._holdout_execution_lineage = copy.deepcopy(dict(execution_lineage))
        super().__init__(**kwargs)

    @staticmethod
    def _require_managed_chatgpt_pro(account: Any) -> None:
        if (
            not isinstance(account, Mapping)
            or account.get("type") != "chatgpt"
            or account.get("plan_type") != "pro"
        ):
            raise codex_app_server.AppServerAuthError(
                "holdout requires exact managed ChatGPT Pro auth"
            )

    async def start(self) -> None:
        await super().start()
        try:
            self._require_managed_chatgpt_pro(self.account_summary)
        except BaseException:
            await self.close()
            raise

    async def _request(self, method: str, params: dict[str, Any]) -> Any:
        if method in {"thread/start", "turn/start"}:
            self._require_managed_chatgpt_pro(self.account_summary)
        return await super()._request(method, params)

    def _write_sidecar(self, path: Path, payload: dict[str, Any]) -> None:
        enriched = dict(payload)
        enriched[HOLDOUT_SIDECAR_LINEAGE_FIELD] = copy.deepcopy(
            self._holdout_execution_lineage
        )
        super()._write_sidecar(path, enriched)


def resolve_holdout_client_factory(
    client_factory: Callable[[], Any],
    instruction_contract: Mapping[str, Any],
) -> Callable[[], Any]:
    """Resolve only the production default; preserve explicit factories exactly."""

    if client_factory not in (
        CapacityGatedCodexAppServerClient,
        capacity_gated_client_factory,
    ):
        return client_factory

    # Verify now, before the caller can enter a client context.
    frozen_contract = copy.deepcopy(dict(instruction_contract))
    frozen_lineage = verified_holdout_execution_lineage(frozen_contract)

    def inner_factory() -> HoldoutExpandedCapCodexAppServerClient:
        # Re-verify immediately before construction so no process can start
        # from a contract or pinned runtime that drifted after planning.
        current_lineage = verified_holdout_execution_lineage(frozen_contract)
        if current_lineage != frozen_lineage:
            raise ValueError("holdout execution lineage drift before client start")
        return HoldoutExpandedCapCodexAppServerClient(
            instruction_contract=frozen_contract,
            execution_lineage=frozen_lineage,
            config_overlay=copy.deepcopy(frozen_contract["strict_config_overlay"]),
            command=list(frozen_lineage["runtime"]["command"]),
        )

    def managed_factory() -> CapacityGatedCodexAppServerClient:
        return CapacityGatedCodexAppServerClient(inner_factory=inner_factory)

    return managed_factory


class LazyClientSession:
    """Enter a resolved client only when a fresh semantic attempt needs it."""

    def __init__(self, client_factory: Callable[[], Any]) -> None:
        self.client_factory = client_factory
        self.context: Any | None = None
        self.client: Any | None = None

    async def __aenter__(self) -> "LazyClientSession":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        if self.context is not None:
            await self.context.__aexit__(exc_type, exc, traceback)
        self.context = None
        self.client = None
        return False

    async def _acquire(self) -> Any:
        if self.client is None:
            self.context = self.client_factory()
            self.client = await self.context.__aenter__()
        return self.client

    async def wait_for_semantic_capacity(self) -> Any:
        client = await self._acquire()
        wait = getattr(client, "wait_for_semantic_capacity", None)
        return await wait() if callable(wait) else None

    async def run_ephemeral_structured_turn(self, **kwargs: Any) -> Any:
        client = await self._acquire()
        return await client.run_ephemeral_structured_turn(**kwargs)
