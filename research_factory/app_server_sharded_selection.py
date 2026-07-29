from __future__ import annotations

"""Hash-bound five-arm selection with sharded calibration over app-server only."""

import argparse
import asyncio
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from .app_server_capacity import CapacityGatedCodexAppServerClient
from .app_server_dev_selection import (
    DEV_SELECTION_VERSION,
    _blocked_result,
    run_app_server_dev_selection,
)
from .app_server_llm_judge import run_app_server_semantic_judge, write_immutable_json
from .app_server_sharded_calibration import (
    SHARDED_CALIBRATION_VERSION,
    run_app_server_sharded_judge_calibration,
)
from .app_server_v2_reuse import (
    ReuseContractError,
    verify_selection_reuse_contract,
)
from .codex_app_server import CodexAppServerClient
from .paths import db_path


SHARDED_SELECTION_VERSION = "pif_app_server_sharded_selection_v3"


class ShardedSelectionError(ValueError):
    """The v2 selection invocation does not match its immutable reuse contract."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


class _BorrowedClientContext:
    def __init__(self, client: CodexAppServerClient):
        self.client = client

    async def __aenter__(self) -> CodexAppServerClient:
        return self.client

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        return False


async def run_app_server_sharded_selection(
    conn: sqlite3.Connection,
    *,
    reuse_contract_path: Path,
    output_dir: Path,
    selection_output_path: Optional[Path] = None,
    judge_model: str = "gpt-5.6-sol",
    judge_reasoning_effort: str = "high",
    judge_timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
    calibration_judge_runner: Callable[..., Any] = run_app_server_semantic_judge,
    full_judge_runner: Callable[..., Any] = run_app_server_semantic_judge,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = Path(selection_output_path or root / "selection-result.json").expanduser().resolve()
    try:
        reuse_path = reuse_contract_path.expanduser().resolve()
        contract = verify_selection_reuse_contract(reuse_path)
        target_root = Path(str(contract["target_pipeline_root"])).expanduser().resolve()
        source_roots = [
            Path(str(contract[key])).expanduser().resolve()
            for key in (
                "source_pipeline_root",
                "source_pipeline_v1_root",
                "source_pipeline_v2_root",
                "source_pipeline_v3_root",
            )
            if isinstance(contract.get(key), str)
        ]
        if (
            not _inside(root, target_root)
            or any(root == source or _inside(root, source) for source in source_roots)
            or any(
                terminal_path == source or _inside(terminal_path, source)
                for source in source_roots
            )
        ):
            raise ShardedSelectionError("sharded selection root is not version isolated")
        inputs = contract["selection_inputs"]
        manifest_path = Path(inputs["development_manifest"]["path"])
        context_path = Path(inputs["context_usage_recovery"]["path"])
        run_spec_path = Path(inputs["extraction_run_spec"]["path"])
        witness_root = Path(inputs["preassembled_assembly_report"]["path"]).parent
        itt_path = Path(contract["interrupted_batch_5_same_thread"]["path"])
        arm_paths = [Path(item["report"]["path"]) for item in contract["clean_arms"]]
        selection_context = {
            "schema_version": SHARDED_SELECTION_VERSION,
            "reuse_contract_path": str(reuse_path),
            "reuse_contract_sha256": _sha256_file(reuse_path),
            "reuse_contract_schema_version": contract["schema_version"],
            "calibration_schema_version": SHARDED_CALIBRATION_VERSION,
            "source_pipeline_v1_replay_allowed": False,
            "source_pipeline_v2_replay_allowed": False,
            "source_pipeline_v3_replay_allowed": False,
            "extraction_model_calls_allowed": False,
            "failed_v1_ab_retry_allowed": False,
            "failed_v2_shard_retry_allowed": False,
            "failed_v3_shard_retry_allowed": False,
            "pipeline_v3_partial_calibration_scoring_allowed": False,
            "full_fresh_calibration_required": True,
            "batch_5_same_thread_retry_allowed": False,
            "preassembled_witness_pool_sha256": inputs["preassembled_witness_pool"][
                "sha256"
            ],
            "verified_provenance_sha256": {
                name: record["sha256"]
                for name, record in sorted(contract["verified_provenance"].items())
            },
        }
        root.mkdir(parents=True, exist_ok=True)
        async with client_factory() as persistent_client:
            borrowed_factory = lambda: _BorrowedClientContext(persistent_client)

            async def calibration_runner(**kwargs: Any) -> dict[str, Any]:
                call_kwargs = dict(kwargs)
                call_kwargs["client_factory"] = borrowed_factory
                call_kwargs["judge_runner"] = calibration_judge_runner
                return await run_app_server_sharded_judge_calibration(
                    **call_kwargs,
                )

            return await run_app_server_dev_selection(
                conn,
                manifest_path=manifest_path,
                arm_report_paths=arm_paths,
                context_usage_recovery_path=context_path,
                output_dir=root,
                selection_output_path=terminal_path,
                run_spec_path=run_spec_path,
                interrupted_arm_provenance_path=itt_path,
                judge_model=judge_model,
                judge_reasoning_effort=judge_reasoning_effort,
                judge_timeout_seconds=judge_timeout_seconds,
                preassembled_witness_root=witness_root,
                selection_context=selection_context,
                client_factory=borrowed_factory,
                calibration_runner=calibration_runner,
                judge_runner=full_judge_runner,
            )
    except (ReuseContractError, ShardedSelectionError) as exc:
        result = _blocked_result(
            reasons=["versioned_reuse_contract_failed"],
            stage="preflight_contract",
            details={"error_class": type(exc).__name__},
            terminal_classification="preflight_contract_failed",
        )
        if not terminal_path.exists():
            terminal_path.parent.mkdir(parents=True, exist_ok=True)
            write_immutable_json(terminal_path, result)
        return result
    except Exception as exc:  # noqa: BLE001 - unattended transport must terminate honestly
        result = _blocked_result(
            reasons=["infrastructure_or_judge_attempt_failed"],
            stage="persistent_app_server_transport",
            details={"error_class": type(exc).__name__},
            terminal_classification="infrastructure_or_judge_attempt_failed",
        )
        if not terminal_path.exists():
            terminal_path.parent.mkdir(parents=True, exist_ok=True)
            write_immutable_json(terminal_path, result)
        return result


def _read_only_connection(path: Path) -> sqlite3.Connection:
    resolved = path.expanduser().resolve()
    connection = sqlite3.connect("file:%s?mode=ro" % resolved.as_posix(), uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run hash-bound sharded development selection")
    parser.add_argument("--reuse-contract", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--selection-output")
    parser.add_argument("--judge-model", default="gpt-5.6-sol")
    parser.add_argument("--judge-reasoning-effort", default="high")
    parser.add_argument("--judge-timeout-seconds", type=float, default=1200.0)
    parser.add_argument("--database", default=str(db_path()))
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    conn = _read_only_connection(Path(args.database))
    try:
        result = asyncio.run(
            run_app_server_sharded_selection(
                conn,
                reuse_contract_path=Path(args.reuse_contract),
                output_dir=Path(args.output_dir),
                selection_output_path=(
                    Path(args.selection_output) if args.selection_output else None
                ),
                judge_model=args.judge_model,
                judge_reasoning_effort=args.judge_reasoning_effort,
                judge_timeout_seconds=args.judge_timeout_seconds,
            )
        )
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if result.get("selection_status") == "frozen_winner" else 2


if __name__ == "__main__":
    raise SystemExit(main())
