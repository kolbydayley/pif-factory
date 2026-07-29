"""Lab-only Kimi Code workhorse controlled by the compact PIF CLI.

The module deliberately consumes immutable file manifests and never opens the
canonical queue database.  A dry run is the default; only ``--execute`` may
cross the hosted-model boundary.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .kimi_code_runner import (
    EXPECTED_KIMI_CODE_VERSION,
    KimiCodeRunnerError,
    inspect_kimi_code_provider_profile,
    load_authorization_receipt,
    prepare_kimi_code_home,
    run_kimi_code_job,
    serialize_kimi_code_job,
)
from .labels import load_label_pack, validate_label_output


MANIFEST_SCHEMA_VERSION = "pif_kimi_workhorse_manifest_v1"
REQUEST_SCHEMA_VERSION = "pif_kimi_workhorse_request_v1"
PRIVACY_TIER = "full_text_allowed"
ALLOWED_MODEL_IDS = frozenset({"k3", "kimi-for-coding", "kimi-for-coding-highspeed"})
KIMI_CODE_MODEL_ALIAS_PREFIX = "kimi-code/"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINARY = Path("/opt/homebrew/bin/kimi")
DEFAULT_KIMI_HOME = Path.home() / "Library" / "Application Support" / "Podcast Intelligence Factory" / "kimi-code"
DEFAULT_JOB_ROOT = Path.home() / "Library" / "Caches" / "Podcast Intelligence Factory" / "kimi-code-jobs"
DEFAULT_AUTHORIZATION_RECEIPT = PROJECT_ROOT / "config" / "kimi_code_automation_authorization.json"
MAX_MANIFEST_BYTES = 256 * 1024
MAX_PROMPT_BYTES = 64 * 1024
MAX_SCHEMA_BYTES = 32 * 1024
MAX_SEGMENT_BYTES = 8 * 1024 * 1024
MAX_OUTPUT_BYTES = 4 * 1024 * 1024
_JOB_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class KimiWorkhorseError(RuntimeError):
    """A fail-closed manifest or lab-orchestration error."""

    def __init__(self, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = dict(details or {})


@dataclass(frozen=True)
class KimiWorkhorseManifest:
    path: Path
    manifest_sha256: str
    job_id: str
    privacy_tier: str
    model: str
    prompt_path: Path
    prompt_sha256: str
    prompt: str
    schema_path: Path
    schema_sha256: str
    schema: dict[str, Any]
    output_path: Path
    label_pack: str
    segment_text_path: Path
    segment_text_sha256: str
    segment_text: str


def _print(value: Mapping[str, Any], *, stream: Any | None = None) -> None:
    print(json.dumps(dict(value), ensure_ascii=True, indent=2, sort_keys=True), file=stream or sys.stdout)


def _nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise KimiWorkhorseError(f"manifest {field} must be a non-empty string")
    return value.strip()


def _expected_sha256(value: Any, field: str) -> str:
    digest = _nonempty_string(value, field).lower()
    if not _SHA256.fullmatch(digest):
        raise KimiWorkhorseError(f"manifest {field} must be a lowercase SHA-256 digest")
    return digest


def _resolve_path(base: Path, value: Any, field: str) -> Path:
    text = _nonempty_string(value, field)
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    return candidate.resolve()


def _read_bounded(path: Path, limit: int, field: str) -> bytes:
    try:
        if not path.is_file():
            raise KimiWorkhorseError(f"manifest {field} does not name a regular file")
        size = path.stat().st_size
        if size > limit:
            raise KimiWorkhorseError(f"manifest {field} exceeds its safety limit")
        data = path.read_bytes()
    except KimiWorkhorseError:
        raise
    except OSError as exc:
        raise KimiWorkhorseError(f"manifest {field} could not be read") from exc
    if len(data) > limit:
        raise KimiWorkhorseError(f"manifest {field} exceeds its safety limit")
    return data


def _decode_utf8(data: bytes, field: str) -> str:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise KimiWorkhorseError(f"manifest {field} must be UTF-8 text") from exc


def _verify_digest(data: bytes, expected: str, field: str) -> None:
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected:
        raise KimiWorkhorseError(f"manifest {field} content hash does not match")


def _is_object_schema(schema: Mapping[str, Any]) -> bool:
    declared = schema.get("type")
    return declared == "object" or isinstance(declared, list) and "object" in declared


def load_manifest(path: Path) -> KimiWorkhorseManifest:
    """Load and hash-bind one immutable, file-only shadow job."""
    manifest_path = Path(path).expanduser().resolve()
    raw_manifest = _read_bounded(manifest_path, MAX_MANIFEST_BYTES, "path")
    try:
        raw = json.loads(_decode_utf8(raw_manifest, "path"))
    except json.JSONDecodeError as exc:
        raise KimiWorkhorseError("manifest is not valid JSON") from exc
    if not isinstance(raw, dict):
        raise KimiWorkhorseError("manifest must be a JSON object")
    allowed_fields = {
        "schema_version",
        "job_id",
        "privacy_tier",
        "model",
        "prompt_path",
        "prompt_sha256",
        "schema_path",
        "schema_sha256",
        "output_path",
        "label_pack",
        "segment_text_path",
        "segment_text_sha256",
    }
    unknown = sorted(set(raw) - allowed_fields)
    if unknown:
        raise KimiWorkhorseError("manifest contains unsupported fields")
    if raw.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise KimiWorkhorseError("manifest schema_version is not accepted")
    job_id = _nonempty_string(raw.get("job_id"), "job_id")
    if not _JOB_ID.fullmatch(job_id):
        raise KimiWorkhorseError("manifest job_id contains unsupported characters")
    privacy_tier = _nonempty_string(raw.get("privacy_tier"), "privacy_tier")
    if privacy_tier != PRIVACY_TIER:
        raise KimiWorkhorseError("manifest privacy_tier is not authorized for hosted Kimi inference")
    model = _nonempty_string(raw.get("model"), "model")
    if model not in ALLOWED_MODEL_IDS:
        raise KimiWorkhorseError("manifest model is not an accepted Kimi Code model ID")

    base = manifest_path.parent
    prompt_path = _resolve_path(base, raw.get("prompt_path"), "prompt_path")
    prompt_sha256 = _expected_sha256(raw.get("prompt_sha256"), "prompt_sha256")
    prompt_bytes = _read_bounded(prompt_path, MAX_PROMPT_BYTES, "prompt_path")
    _verify_digest(prompt_bytes, prompt_sha256, "prompt_path")
    prompt = _decode_utf8(prompt_bytes, "prompt_path")
    if not prompt.strip():
        raise KimiWorkhorseError("manifest prompt_path is empty")

    schema_path = _resolve_path(base, raw.get("schema_path"), "schema_path")
    schema_sha256 = _expected_sha256(raw.get("schema_sha256"), "schema_sha256")
    schema_bytes = _read_bounded(schema_path, MAX_SCHEMA_BYTES, "schema_path")
    _verify_digest(schema_bytes, schema_sha256, "schema_path")
    try:
        schema = json.loads(_decode_utf8(schema_bytes, "schema_path"))
    except json.JSONDecodeError as exc:
        raise KimiWorkhorseError("manifest schema_path is not valid JSON") from exc
    if not isinstance(schema, dict) or not _is_object_schema(schema):
        raise KimiWorkhorseError("manifest schema_path must contain an object JSON Schema")

    output_path = _resolve_path(base, raw.get("output_path"), "output_path")
    if output_path.exists():
        raise KimiWorkhorseError("manifest output_path already exists")

    label_pack = _nonempty_string(raw.get("label_pack"), "label_pack")
    segment_path_raw = raw.get("segment_text_path")
    segment_hash_raw = raw.get("segment_text_sha256")
    if segment_path_raw is None or segment_hash_raw is None:
        raise KimiWorkhorseError("manifest label_pack requires hash-bound segment_text_path")
    segment_text_path = _resolve_path(base, segment_path_raw, "segment_text_path")
    segment_text_sha256 = _expected_sha256(segment_hash_raw, "segment_text_sha256")
    segment_bytes = _read_bounded(segment_text_path, MAX_SEGMENT_BYTES, "segment_text_path")
    _verify_digest(segment_bytes, segment_text_sha256, "segment_text_path")
    segment_text = _decode_utf8(segment_bytes, "segment_text_path")
    try:
        pack = load_label_pack(label_pack)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise KimiWorkhorseError("manifest label_pack could not be loaded") from exc
    if pack.schema != schema:
        raise KimiWorkhorseError("manifest schema_path does not match the named PIF label pack")

    return KimiWorkhorseManifest(
        path=manifest_path,
        manifest_sha256=hashlib.sha256(raw_manifest).hexdigest(),
        job_id=job_id,
        privacy_tier=privacy_tier,
        model=model,
        prompt_path=prompt_path,
        prompt_sha256=prompt_sha256,
        prompt=prompt,
        schema_path=schema_path,
        schema_sha256=schema_sha256,
        schema=schema,
        output_path=output_path,
        label_pack=label_pack,
        segment_text_path=segment_text_path,
        segment_text_sha256=segment_text_sha256,
        segment_text=segment_text,
    )


def _manifest_summary(manifest: KimiWorkhorseManifest) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "job_id": manifest.job_id,
        "manifest_path": str(manifest.path),
        "manifest_sha256": manifest.manifest_sha256,
        "privacy_tier": manifest.privacy_tier,
        "model": manifest.model,
        "prompt_sha256": manifest.prompt_sha256,
        "prompt_bytes": len(manifest.prompt.encode("utf-8")),
        "schema_sha256": manifest.schema_sha256,
        "output_path": str(manifest.output_path),
        "label_pack": manifest.label_pack,
        "segment_text_sha256": manifest.segment_text_sha256,
        "segment_text_bytes": len(manifest.segment_text.encode("utf-8")),
    }


def freeze_manifest(
    *,
    manifest_path: Path,
    job_id: str,
    model: str,
    prompt_path: Path,
    schema_path: Path,
    output_path: Path,
    label_pack: str,
    segment_text_path: Path,
) -> dict[str, Any]:
    """Create one hash-bound manifest without making a hosted-model call."""
    target = Path(manifest_path).expanduser().resolve()
    prompt = Path(prompt_path).expanduser().resolve()
    schema = Path(schema_path).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    segment = Path(segment_text_path).expanduser().resolve()
    if target.exists():
        raise KimiWorkhorseError("manifest destination already exists")
    if output.exists():
        raise KimiWorkhorseError("manifest output_path already exists")
    if _attempt_receipt_path(output).exists():
        raise KimiWorkhorseError("manifest output_path already has an execution-attempt receipt")
    if target == output or target in {prompt, schema, segment}:
        raise KimiWorkhorseError("manifest destination must be separate from every job input and output")
    prompt_bytes = _read_bounded(prompt, MAX_PROMPT_BYTES, "prompt_path")
    schema_bytes = _read_bounded(schema, MAX_SCHEMA_BYTES, "schema_path")
    segment_bytes = _read_bounded(segment, MAX_SEGMENT_BYTES, "segment_text_path")
    payload: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "job_id": job_id,
        "privacy_tier": PRIVACY_TIER,
        "model": model,
        "prompt_path": str(prompt),
        "prompt_sha256": hashlib.sha256(prompt_bytes).hexdigest(),
        "schema_path": str(schema),
        "schema_sha256": hashlib.sha256(schema_bytes).hexdigest(),
        "output_path": str(output),
    }
    payload.update(
        {
            "label_pack": label_pack,
            "segment_text_path": str(segment),
            "segment_text_sha256": hashlib.sha256(segment_bytes).hexdigest(),
        }
    )
    rendered = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if len(rendered.encode("utf-8")) > MAX_MANIFEST_BYTES:
        raise KimiWorkhorseError("manifest exceeds its safety limit")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            os.chmod(temporary, 0o600)
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        frozen = load_manifest(temporary)
        try:
            os.link(temporary, target)
        except FileExistsError as exc:
            raise KimiWorkhorseError("manifest destination already exists") from exc
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return {
        "ok": True,
        "status": "frozen",
        "model_call_made": False,
        "canonical_db_opened": False,
        "canonical_mutation": False,
        **_manifest_summary(replace(frozen, path=target)),
    }


def _validator(manifest: KimiWorkhorseManifest) -> Callable[[dict[str, Any]], None]:
    label_pack = manifest.label_pack
    segment_text = manifest.segment_text

    def validate_pack(value: dict[str, Any]) -> None:
        validate_label_output(label_pack, value, segment_text=segment_text)

    return validate_pack


def _json_string_chunks(text: str, *, encoded_limit: int = 1_200) -> list[str]:
    """Split text so each chunk remains below Kimi Read's per-line cap."""
    chunks: list[str] = []
    current: list[str] = []
    current_size = 0
    for character in text:
        encoded_character = json.dumps(character, ensure_ascii=False)[1:-1].encode("utf-8")
        character_size = len(encoded_character)
        if character_size > encoded_limit:
            raise KimiWorkhorseError("manifest text contains an unsupported encoded character")
        if current and current_size + character_size > encoded_limit:
            chunks.append("".join(current))
            current = []
            current_size = 0
        current.append(character)
        current_size += character_size
    if current or not chunks:
        chunks.append("".join(current))
    return chunks


def _request_payload(manifest: KimiWorkhorseManifest) -> dict[str, Any]:
    schema_text = json.dumps(manifest.schema, ensure_ascii=False, sort_keys=True)
    return {
        "schema_version": REQUEST_SCHEMA_VERSION,
        "job_id": manifest.job_id,
        "task": (
            "Concatenate prompt_chunks as the labeling instructions and context. Concatenate segment_text_chunks "
            "as the complete authoritative segment to label. Return exactly one JSON object matching the schema "
            "formed by concatenating and parsing output_schema_json_chunks."
        ),
        "prompt_chunks": _json_string_chunks(manifest.prompt),
        "segment_text_chunks": _json_string_chunks(manifest.segment_text),
        "segment_text_sha256": manifest.segment_text_sha256,
        "output_schema_json_chunks": _json_string_chunks(schema_text),
    }


def _cli_model_alias(model_id: str) -> str:
    if model_id not in ALLOWED_MODEL_IDS:
        raise KimiWorkhorseError("model is not an accepted Kimi Code model ID")
    return f"{KIMI_CODE_MODEL_ALIAS_PREFIX}{model_id}"


def _attempt_receipt_path(output_path: Path) -> Path:
    return output_path.with_name(f".{output_path.name}.kimi-attempt.json")


def _attempt_payload(manifest: KimiWorkhorseManifest, *, state: str, **extra: Any) -> dict[str, Any]:
    return {
        "schema_version": "pif_kimi_workhorse_attempt_v1",
        "state": state,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "job_id": manifest.job_id,
        "manifest_sha256": manifest.manifest_sha256,
        "requested_model_id": manifest.model,
        "cli_model_alias": _cli_model_alias(manifest.model),
        "prompt_sha256": manifest.prompt_sha256,
        "schema_sha256": manifest.schema_sha256,
        "segment_text_sha256": manifest.segment_text_sha256,
        **extra,
    }


def _write_attempt_receipt(path: Path, payload: Mapping[str, Any], *, create: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(dict(payload), ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    failure_details = {
        "model_call_started": bool(payload.get("model_call_started")),
        "may_have_consumed_quota": bool(payload.get("may_have_consumed_quota")),
        "attempt_receipt_path": str(path),
        "external_model_call_completed": bool(payload.get("external_model_call_completed")),
        "output_published": bool(payload.get("output_published")),
    }
    for field in ("output_path", "output_sha256", "output_bytes"):
        if payload.get(field) is not None:
            failure_details[field] = payload[field]
    if create:
        try:
            with path.open("x", encoding="utf-8") as handle:
                os.chmod(path, 0o600)
                handle.write(rendered)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError as exc:
            raise KimiWorkhorseError(
                "manifest output already has an execution-attempt receipt; freeze a new job_id and output path",
                details={
                    "model_call_started": False,
                    "may_have_consumed_quota": False,
                    "attempt_receipt_path": str(path),
                },
            ) from exc
        except OSError as exc:
            raise KimiWorkhorseError(
                "execution-attempt receipt could not be claimed",
                details=failure_details,
            ) from exc
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            os.chmod(temporary, 0o600)
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except OSError as exc:
        raise KimiWorkhorseError(
            "execution-attempt receipt could not be updated",
            details=failure_details,
        ) from exc
    finally:
        try:
            temporary.unlink()
        except OSError:
            pass


def run_manifest(
    manifest_path: Path,
    *,
    execute: bool,
    authorization_receipt_path: Path = DEFAULT_AUTHORIZATION_RECEIPT,
    job_root: Path = DEFAULT_JOB_ROOT,
    kimi_home: Path = DEFAULT_KIMI_HOME,
    command: Sequence[str] = (str(DEFAULT_BINARY),),
    timeout_seconds: int = 900,
    expected_cli_version: str | None = EXPECTED_KIMI_CODE_VERSION,
    runner: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Plan or execute one manifest without touching the canonical PIF DB."""
    manifest = load_manifest(manifest_path)
    summary = _manifest_summary(manifest)
    payload = _request_payload(manifest)
    serialized_job = serialize_kimi_code_job(payload)
    attempt_path = _attempt_receipt_path(manifest.output_path)
    if attempt_path.exists():
        raise KimiWorkhorseError(
            "manifest output already has an execution-attempt receipt; freeze a new job_id and output path",
            details={
                "model_call_started": False,
                "may_have_consumed_quota": False,
                "attempt_receipt_path": str(attempt_path),
            },
        )
    envelope = {
        "job_document_bytes": len(serialized_job.encode("utf-8")),
        "job_document_lines": len(serialized_job.splitlines()),
        "attempt_receipt_path": str(attempt_path),
    }
    if not execute:
        return {
            "ok": True,
            "status": "planned",
            "external_model_call": False,
            "canonical_db_opened": False,
            "canonical_mutation": False,
            **summary,
            **envelope,
        }
    if runner is None:
        # A failed device login can leave an OAuth artifact before Kimi's
        # membership-backed /models check fails.  Require the local managed
        # provider/model provisioning that is written only after a successful
        # login, before claiming an immutable attempt or starting inference.
        load_authorization_receipt(Path(authorization_receipt_path))
        prepare_kimi_code_home(
            Path(kimi_home),
            command=tuple(command),
            expected_version=expected_cli_version,
        )
        if not _expected_credential_file_present(Path(kimi_home)):
            raise KimiWorkhorseError(
                "Kimi login credential is absent from the dedicated profile",
                details={
                    "model_call_started": False,
                    "may_have_consumed_quota": False,
                    "login_provisioning_complete": False,
                },
            )
        provisioning = inspect_kimi_code_provider_profile(Path(kimi_home), command=tuple(command))
        if not provisioning["login_provisioning_complete"]:
            raise KimiWorkhorseError(
                "Kimi login did not complete managed provider/model provisioning",
                details={
                    "model_call_started": False,
                    "may_have_consumed_quota": False,
                    "login_provisioning_complete": False,
                },
            )
    _write_attempt_receipt(
        attempt_path,
        _attempt_payload(
            manifest,
            state="dispatch_claimed",
            model_call_started=False,
            may_have_consumed_quota=False,
        ),
        create=True,
    )
    dispatch = runner or run_kimi_code_job
    try:
        result = dispatch(
            payload=payload,
            output_path=manifest.output_path,
            authorization_receipt_path=Path(authorization_receipt_path),
            job_root=Path(job_root),
            kimi_home=Path(kimi_home),
            model=_cli_model_alias(manifest.model),
            command=tuple(command),
            timeout_seconds=timeout_seconds,
            schema_validator=_validator(manifest),
            expected_cli_version=expected_cli_version,
            retain_job_directory=False,
        )
        output_bytes = _read_bounded(manifest.output_path, MAX_OUTPUT_BYTES, "output_path")
    except KimiCodeRunnerError as exc:
        started = bool(exc.details.get("model_call_started"))
        may_have_consumed = bool(exc.details.get("may_have_consumed_quota"))
        _write_attempt_receipt(
            attempt_path,
            _attempt_payload(
                manifest,
                state="failed_after_dispatch" if started else "failed_before_model_call",
                model_call_started=started,
                may_have_consumed_quota=may_have_consumed,
                error_class=str(exc.details.get("error_class") or type(exc).__name__),
                duration_ms=exc.details.get("duration_ms"),
            ),
            create=False,
        )
        exc.details.update(
            {
                "model_call_started": started,
                "may_have_consumed_quota": may_have_consumed,
                "attempt_receipt_path": str(attempt_path),
            }
        )
        raise
    except Exception as exc:
        _write_attempt_receipt(
            attempt_path,
            _attempt_payload(
                manifest,
                state="failed_after_dispatch_unknown",
                model_call_started=True,
                may_have_consumed_quota=True,
                error_class=type(exc).__name__,
            ),
            create=False,
        )
        raise KimiWorkhorseError(
            "Kimi dispatch failed after the immutable attempt was claimed",
            details={
                "model_call_started": True,
                "may_have_consumed_quota": True,
                "attempt_receipt_path": str(attempt_path),
            },
        ) from exc
    _write_attempt_receipt(
        attempt_path,
        _attempt_payload(
            manifest,
            state="completed",
            model_call_started=True,
            may_have_consumed_quota=True,
            external_model_call_completed=True,
            output_published=True,
            output_path=str(manifest.output_path),
            output_sha256=hashlib.sha256(output_bytes).hexdigest(),
            output_bytes=len(output_bytes),
            duration_ms=result.get("duration_ms"),
            retry_count=result.get("retry_count"),
            actual_model_verified=result.get("actual_model_verified", False),
            actual_model_id=result.get("actual_model_id"),
        ),
        create=False,
    )
    return {
        "ok": True,
        "status": "completed",
        "external_model_call": True,
        "canonical_db_opened": False,
        "canonical_mutation": False,
        **summary,
        **envelope,
        "output_sha256": hashlib.sha256(output_bytes).hexdigest(),
        "output_bytes": len(output_bytes),
        "schema_validation": result.get("schema_validation"),
        "duration_ms": result.get("duration_ms"),
        "retry_count": result.get("retry_count"),
        "requested_model_id": manifest.model,
        "cli_model_alias": _cli_model_alias(manifest.model),
        "actual_model_verified": result.get("actual_model_verified", False),
        "actual_model_id": result.get("actual_model_id"),
        "authorization": result.get("authorization"),
        "diagnostics": result.get("diagnostics"),
    }


def _credential_file_count(kimi_home: Path) -> int:
    credentials = kimi_home / "credentials"
    if not credentials.is_dir():
        return 0
    try:
        return sum(1 for path in credentials.glob("*.json") if path.is_file())
    except OSError:
        return 0


def _expected_credential_file_present(kimi_home: Path) -> bool:
    try:
        return (kimi_home / "credentials" / "kimi-code.json").is_file()
    except OSError:
        return False


def _login_command(binary: Path, kimi_home: Path) -> str:
    return f"KIMI_CODE_HOME={shlex.quote(str(kimi_home))} {shlex.quote(str(binary))} login"


def _absolute_binary(value: str) -> Path:
    binary = Path(value).expanduser()
    return binary if binary.is_absolute() else (Path.cwd() / binary).absolute()


def setup_profile(*, binary: Path, kimi_home: Path, expected_version: str | None) -> dict[str, Any]:
    prepared = prepare_kimi_code_home(kimi_home, command=(str(binary),), expected_version=expected_version)
    return {
        "ok": True,
        "status": "configured",
        "model_call_made": False,
        "version": prepared["version"],
        "kimi_home": prepared["kimi_home"],
        "config_path": prepared["config_path"],
        "skills_dir": prepared["skills_dir"],
        "mcp_path": prepared["mcp_path"],
        "login_command": _login_command(binary, kimi_home),
    }


def profile_status(*, binary: Path, kimi_home: Path, expected_version: str | None, receipt: Path) -> dict[str, Any]:
    profile_error: str | None = None
    profile_error_details: dict[str, Any] = {}
    prepared: dict[str, Any] | None = None
    try:
        prepared = prepare_kimi_code_home(kimi_home, command=(str(binary),), expected_version=expected_version)
    except KimiCodeRunnerError as exc:
        profile_error = str(exc)
        profile_error_details = dict(exc.details)
    authorization: dict[str, Any] | None = None
    authorization_error: str | None = None
    try:
        authorization = load_authorization_receipt(receipt).sanitized_metadata()
    except KimiCodeRunnerError as exc:
        authorization_error = str(exc)
    credential_count = _credential_file_count(kimi_home)
    expected_credential_present = _expected_credential_file_present(kimi_home)
    provider_probe_error: str | None = None
    provider_probe_error_details: dict[str, Any] = {}
    provisioning = {
        "provider_probe_ok": False,
        "managed_oauth_provider_provisioned": False,
        "managed_model_count": 0,
        "login_provisioning_complete": False,
    }
    if prepared is not None:
        try:
            provisioning = inspect_kimi_code_provider_profile(kimi_home, command=(str(binary),))
        except KimiCodeRunnerError as exc:
            provider_probe_error = str(exc)
            provider_probe_error_details = dict(exc.details)

    ready = (
        prepared is not None
        and authorization is not None
        and expected_credential_present
        and provider_probe_error is None
        and bool(provisioning["login_provisioning_complete"])
    )
    if prepared is None:
        status = (
            "profile_probe_failed"
            if profile_error_details.get("error_class") == "preflight_timeout"
            else "setup_required"
        )
        readiness_error = profile_error
    elif authorization is None:
        status = "authorization_required"
        readiness_error = authorization_error
    elif not expected_credential_present:
        status = "login_required"
        readiness_error = "The expected Kimi Code OAuth credential artifact is absent."
    elif provider_probe_error is not None:
        status = "provider_probe_failed"
        readiness_error = provider_probe_error
    elif not provisioning["login_provisioning_complete"]:
        status = "membership_or_login_provisioning_required"
        readiness_error = (
            "A credential artifact exists, but Kimi did not provision its managed OAuth provider and models; "
            "the prior login did not complete."
        )
    else:
        status = "ready"
        readiness_error = None
    return {
        "ok": ready,
        "status_check_completed": prepared is not None and provider_probe_error is None,
        "status": status,
        "model_call_made": False,
        "binary": str(binary),
        "expected_version": expected_version,
        "kimi_home": str(kimi_home),
        "profile_valid": (
            None if profile_error_details.get("error_class") == "preflight_timeout" else prepared is not None
        ),
        "profile_error": profile_error,
        "profile_error_class": profile_error_details.get("error_class"),
        "profile_error_stage": profile_error_details.get("preflight_stage"),
        "profile_error_timeout_seconds": profile_error_details.get("timeout_seconds"),
        "profile_error_duration_ms": profile_error_details.get("duration_ms"),
        "authorization": authorization,
        "authorization_error": authorization_error,
        "credential_files_present": credential_count > 0,
        "credential_file_count": credential_count,
        "expected_credential_file_present": expected_credential_present,
        "provider_probe_ok": bool(provisioning["provider_probe_ok"]),
        "provider_probe_error": provider_probe_error,
        "provider_probe_error_class": provider_probe_error_details.get("error_class"),
        "provider_probe_error_stage": provider_probe_error_details.get("preflight_stage"),
        "provider_probe_error_timeout_seconds": provider_probe_error_details.get("timeout_seconds"),
        "provider_probe_error_duration_ms": provider_probe_error_details.get("duration_ms"),
        "managed_oauth_provider_provisioned": bool(provisioning["managed_oauth_provider_provisioned"]),
        "managed_model_count": int(provisioning["managed_model_count"]),
        "login_provisioning_complete": bool(provisioning["login_provisioning_complete"]),
        "live_membership_check_made": False,
        "network_call_made": False,
        "authentication_currently_valid": None,
        "membership_currently_valid": None,
        "readiness_semantics": "local_prerequisites_and_prior_login_provisioning_only",
        "readiness_error": readiness_error,
        "local_prerequisites_ready": ready,
        "ready_for_live_call": ready,
        "production_enabled": False,
        "accepted_model_ids": sorted(ALLOWED_MODEL_IDS),
        "accepted_model_ids_scope": "wrapper_allowlist_not_account_entitlement",
        "login_command": _login_command(binary, kimi_home),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pif lab kimi-workhorse", description="Bounded native Kimi Code shadow workhorse.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_profile_options(command: argparse.ArgumentParser) -> None:
        command.add_argument("--binary", default=str(DEFAULT_BINARY))
        command.add_argument("--kimi-home", default=str(DEFAULT_KIMI_HOME))
        command.add_argument("--expected-version", default=EXPECTED_KIMI_CODE_VERSION)

    setup = subparsers.add_parser("setup", help="Create and locally validate the isolated profile without a model call.")
    add_profile_options(setup)
    status = subparsers.add_parser("status", help="Report sanitized local readiness without a model call.")
    add_profile_options(status)
    status.add_argument("--authorization-receipt", default=str(DEFAULT_AUTHORIZATION_RECEIPT))
    freeze = subparsers.add_parser("freeze", help="Create a hash-bound immutable shadow manifest without a model call.")
    freeze.add_argument("--manifest", required=True)
    freeze.add_argument("--job-id", required=True)
    freeze.add_argument("--model", required=True)
    freeze.add_argument("--prompt", required=True)
    freeze.add_argument("--output", required=True)
    freeze.add_argument("--label-pack", required=True)
    freeze.add_argument("--segment-text", required=True)
    run = subparsers.add_parser("run", help="Plan by default; add --execute for one hosted inference call.")
    run.add_argument("--manifest", required=True)
    run.add_argument("--execute", action="store_true")
    run.add_argument("--authorization-receipt", default=str(DEFAULT_AUTHORIZATION_RECEIPT))
    run.add_argument("--job-root", default=str(DEFAULT_JOB_ROOT))
    run.add_argument("--kimi-home", default=str(DEFAULT_KIMI_HOME))
    run.add_argument("--binary", default=str(DEFAULT_BINARY))
    run.add_argument("--expected-version", default=EXPECTED_KIMI_CODE_VERSION)
    run.add_argument("--timeout-seconds", type=int, default=900)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "setup":
            result = setup_profile(
                binary=_absolute_binary(args.binary),
                kimi_home=Path(args.kimi_home).expanduser().resolve(),
                expected_version=args.expected_version,
            )
        elif args.command == "status":
            result = profile_status(
                binary=_absolute_binary(args.binary),
                kimi_home=Path(args.kimi_home).expanduser().resolve(),
                expected_version=args.expected_version,
                receipt=Path(args.authorization_receipt).expanduser().resolve(),
            )
        elif args.command == "freeze":
            schema_path = PROJECT_ROOT / "label_packs" / str(args.label_pack) / "schema.json"
            result = freeze_manifest(
                manifest_path=Path(args.manifest),
                job_id=args.job_id,
                model=args.model,
                prompt_path=Path(args.prompt),
                schema_path=schema_path,
                output_path=Path(args.output),
                label_pack=args.label_pack,
                segment_text_path=Path(args.segment_text),
            )
        else:
            if args.timeout_seconds <= 0:
                raise KimiWorkhorseError("timeout_seconds must be positive")
            result = run_manifest(
                Path(args.manifest),
                execute=bool(args.execute),
                authorization_receipt_path=Path(args.authorization_receipt),
                job_root=Path(args.job_root),
                kimi_home=Path(args.kimi_home),
                command=(str(_absolute_binary(args.binary)),),
                timeout_seconds=args.timeout_seconds,
                expected_cli_version=args.expected_version,
            )
    except (KimiWorkhorseError, KimiCodeRunnerError, OSError, ValueError) as exc:
        details = dict(getattr(exc, "details", {}) or {})
        _print(
            {
                "ok": False,
                "error": type(exc).__name__,
                "message": str(exc),
                "external_model_call_completed": bool(details.get("external_model_call_completed")),
                "external_model_call_started": bool(details.get("model_call_started")),
                "may_have_consumed_quota": bool(details.get("may_have_consumed_quota")),
                "output_published": bool(details.get("output_published")),
                "output_path": details.get("output_path"),
                "output_sha256": details.get("output_sha256"),
                "attempt_receipt_path": details.get("attempt_receipt_path"),
                "canonical_mutation": False,
            },
            stream=sys.stderr,
        )
        return 2
    _print(result)
    return 0 if result.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
