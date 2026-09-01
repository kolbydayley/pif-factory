"""Ten-window Codex app-server protocol qualification for rebuild gold authoring.

This is a transport canary, not a semantic benchmark run. It sends ten real,
hash-frozen visible benchmark windows through the exact rebuild output schema,
then freezes a sanitized receipt covering schema validity, envelope shape,
token accounting, and runtime error classes.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .codex_app_server import CodexAppServerClient
from .signal_desk_rebuild_contracts import event_schema, validate_output
from .signal_desk_rebuild_gold import verify_frozen_manifest
from .util import now_iso, write_text_atomic


CANARY_SCHEMA_VERSION = "pif_signal_desk_rebuild_codex_protocol_canary_v1"
CANARY_CLI_VERSION = "0.147.0"
CANARY_WINDOW_COUNT = 10
EXPECTED_ERROR_CLASSES = frozenset(
    {
        "AppServerProtocolError",
        "AppServerRPCError",
        "AppServerProcessDied",
        "AppServerAuthError",
        "AppServerRecoveryRequired",
        "AppServerTurnTimeout",
        "AppServerStructuredOutputError",
    }
)


class ProtocolCanaryError(RuntimeError):
    pass


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_json(value: Any) -> str:
    return _sha_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def _shape(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _shape(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return ["list", _shape(value[0])] if value else ["list", "empty"]
    if value is None:
        return "null"
    return type(value).__name__


def select_canary_windows(manifest: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    verify_frozen_manifest(manifest)
    visible = [
        row
        for row in manifest["windows"]
        if row.get("split") in {"development", "validation"}
    ]
    visible.sort(
        key=lambda row: (
            str(row.get("transcript_structure")),
            str(row.get("show_id")),
            str(row.get("episode_id")),
            int(row.get("window_index", 0)),
        )
    )
    if len(visible) < CANARY_WINDOW_COUNT:
        raise ProtocolCanaryError("fewer than ten visible frozen windows")
    # Round-robin structures so a flattened-heavy corpus cannot erase the
    # speaker-turn transport shape (and vice versa).
    by_structure: dict[str, list[Mapping[str, Any]]] = {}
    for row in visible:
        by_structure.setdefault(str(row["transcript_structure"]), []).append(row)
    selected: list[Mapping[str, Any]] = []
    while len(selected) < CANARY_WINDOW_COUNT:
        progressed = False
        for structure in sorted(by_structure):
            if by_structure[structure] and len(selected) < CANARY_WINDOW_COUNT:
                selected.append(by_structure[structure].pop(0))
                progressed = True
        if not progressed:
            break
    if len(selected) != CANARY_WINDOW_COUNT:
        raise ProtocolCanaryError("could not select ten structure-stratified windows")
    return tuple(selected)


def _window_text(window: Mapping[str, Any], project_root: Path) -> str:
    path = Path(str(window["transcript_path"])).expanduser()
    if not path.is_absolute():
        path = project_root / path
    text = path.resolve().read_text(encoding="utf-8")
    digest = _sha_bytes(text.encode())
    if digest != window["transcript_sha256"]:
        raise ProtocolCanaryError("frozen transcript bytes changed before protocol canary")
    start, end = int(window["start_char"]), int(window["end_char"])
    excerpt = text[start:end]
    if _sha_bytes(excerpt.encode()) != window["text_sha256"]:
        raise ProtocolCanaryError("frozen window bytes changed before protocol canary")
    return excerpt


async def run_canary(
    *,
    manifest_path: Path,
    project_root: Path,
    schema_path: Path,
    schema_sha256: str,
    output_root: Path,
    binary: str = "codex",
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    windows = select_canary_windows(manifest)
    output_root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    async with CodexAppServerClient(
        command=[binary, "app-server", "--stdio", "--strict-config"],
        expected_cli_version=CANARY_CLI_VERSION,
        protocol_schema_path=schema_path,
        expected_protocol_schema_sha256=schema_sha256,
    ) as client:
        for index, window in enumerate(windows):
            text = _window_text(window, project_root)
            prompt = (
                "Transport qualification only. Return a schema-valid object for the supplied "
                f"window_id {window['window_id']!r}. Set window_disposition to "
                "no_consequential_claims and events to an empty array. Do not analyze or quote "
                "the transcript.\n\nTRANSCRIPT WINDOW:\n" + text
            )
            sidecar = output_root / "turns" / f"{index:02d}-{window['window_id']}.sidecar.json"
            output = output_root / "turns" / f"{index:02d}-{window['window_id']}.output.json"
            try:
                result = await client.run_ephemeral_structured_turn(
                    model="gpt-5.6-sol",
                    effort="medium",
                    base_instructions=(
                        "You are a deterministic structured-output transport canary. Follow the "
                        "requested disposition exactly and emit only the supplied JSON schema."
                    ),
                    prompt=prompt,
                    output_schema=event_schema(),
                    cwd=project_root,
                    sidecar_path=sidecar,
                    output_path=output,
                    timeout_seconds=300,
                )
                if not result.status_ok or result.output is None:
                    raise ProtocolCanaryError(
                        f"app-server turn failed with result error class: {result.error_class}"
                    )
                validate_output(
                    result.output,
                    transcript_window=text,
                    expected_window_id=str(window["window_id"]),
                )
                sidecar_payload = json.loads(sidecar.read_text(encoding="utf-8"))
                rows.append(
                    {
                        "window_id": window["window_id"],
                        "transcript_structure": window["transcript_structure"],
                        "schema_valid": True,
                        "status": result.status,
                        "status_ok": result.status_ok,
                        "error_class": result.error_class,
                        "envelope_shape_sha256": _sha_json(_shape(sidecar_payload)),
                        "usage": (
                            {
                                "input_tokens": result.usage.input_tokens,
                                "cached_input_tokens": result.usage.cached_input_tokens,
                                "output_tokens": result.usage.output_tokens,
                                "reasoning_output_tokens": result.usage.reasoning_output_tokens,
                                "total_tokens": result.usage.total_tokens,
                            }
                            if result.usage
                            else None
                        ),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                error_class = type(exc).__name__
                rows.append(
                    {
                        "window_id": window["window_id"],
                        "transcript_structure": window["transcript_structure"],
                        "schema_valid": False,
                        "status": "failed",
                        "status_ok": False,
                        "error_class": error_class,
                        "envelope_shape_sha256": None,
                        "usage": None,
                    }
                )
                if error_class not in EXPECTED_ERROR_CLASSES:
                    raise ProtocolCanaryError(f"unexpected app-server error class: {error_class}") from exc

    shapes = {row["envelope_shape_sha256"] for row in rows if row["envelope_shape_sha256"]}
    schema_valid = sum(bool(row["schema_valid"]) for row in rows)
    token_accounted = sum(bool(row["usage"]) for row in rows)
    passed = (
        len(rows) == CANARY_WINDOW_COUNT
        and schema_valid == CANARY_WINDOW_COUNT
        and token_accounted == CANARY_WINDOW_COUNT
        and len(shapes) == 1
        and all(row["status_ok"] and row["error_class"] is None for row in rows)
    )
    receipt = {
        "schema_version": CANARY_SCHEMA_VERSION,
        "created_at": now_iso(),
        "passed": passed,
        "cli_version": CANARY_CLI_VERSION,
        "protocol_schema_sha256": schema_sha256,
        "manifest_sha256": manifest["manifest_sha256"],
        "sample_size": len(rows),
        "assertions": {
            "schema_valid": schema_valid == CANARY_WINDOW_COUNT,
            "deterministic_envelope_shape": len(shapes) == 1,
            "token_accounting_complete": token_accounted == CANARY_WINDOW_COUNT,
            "error_classes_compatible": all(
                row["error_class"] is None or row["error_class"] in EXPECTED_ERROR_CLASSES
                for row in rows
            ),
        },
        "counts": {
            "schema_valid": schema_valid,
            "token_accounted": token_accounted,
            "envelope_shapes": len(shapes),
            "errors": sum(row["error_class"] is not None for row in rows),
        },
        "windows": rows,
        "privacy": "sanitized_ids_hashes_usage_and_error_classes_no_transcript_or_output_text",
    }
    receipt["receipt_sha256"] = _sha_json(receipt)
    write_text_atomic(
        output_root / "receipt.json",
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
    )
    if not passed:
        raise ProtocolCanaryError("Codex 0.147.0 protocol canary failed closed")
    return receipt


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--schema", required=True, type=Path)
    parser.add_argument("--schema-sha256", required=True)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--binary", default="codex")
    args = parser.parse_args(argv)
    receipt = asyncio.run(
        run_canary(
            manifest_path=args.manifest.resolve(),
            project_root=args.project_root.resolve(),
            schema_path=args.schema.resolve(),
            schema_sha256=args.schema_sha256,
            output_root=args.output_root.resolve(),
            binary=args.binary,
        )
    )
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
