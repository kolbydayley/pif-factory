"""Bounded native Kimi Code runner for local, Codex-orchestrated jobs.

This module intentionally owns only the transport boundary.  Callers remain
responsible for queue leases and domain validation.  A Kimi process is never
started without a local authorization receipt, and unvalidated model text is
never written to the requested output artifact.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


AUTHORIZATION_SCHEMA_VERSION = "pif_kimi_code_automation_authorization_v1"
AUTHORIZATION_SCOPE = "local_codex_orchestrated_podcast_labeling"
AUTHORIZATION_BASES = frozenset({"moonshot_written_exception", "kimi_platform_payg"})
JOB_SCHEMA_VERSION = "pif_kimi_code_job_v1"
EXPECTED_KIMI_CODE_VERSION = "0.29.0"
DEFAULT_MODEL: str | None = None
MAX_EVENT_BYTES = 4 * 1024 * 1024
MAX_DIAGNOSTIC_BYTES = 8 * 1024
# Kimi's Read tool returns at most 100 KiB, 1,000 lines, and 2,000 characters
# per line.  Keep the complete request below all three limits with margin for
# rendered line numbers and tool metadata; otherwise the model could label a
# silently truncated prompt.
MAX_JOB_BYTES = 80 * 1024
MAX_JOB_LINES = 900
MAX_JOB_LINE_BYTES = 1_600
MAX_PROVIDER_LIST_BYTES = 64 * 1024
DEFAULT_PREFLIGHT_TIMEOUT_SECONDS = 15
_MANAGED_KIMI_PROVIDER_LINE = re.compile(
    r"^managed:kimi-code\s+type=kimi\s+models=(\d+)\s+source=oauth\s*$"
)


class KimiCodeRunnerError(RuntimeError):
    """A fail-closed Kimi transport failure with sanitized diagnostics."""

    def __init__(self, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = dict(details or {})


class KimiAuthorizationError(KimiCodeRunnerError):
    """Raised before a remote Kimi process is started."""


@dataclass(frozen=True)
class KimiAuthorizationReceipt:
    """The safe subset of a written provider-approval receipt."""

    receipt_id: str
    provider: str
    scope: str
    approved_at: str
    authorization_basis: str

    def sanitized_metadata(self) -> dict[str, str]:
        return {
            "receipt_id": self.receipt_id,
            "provider": self.provider,
            "scope": self.scope,
            "approved_at": self.approved_at,
            "authorization_basis": self.authorization_basis,
        }


def _nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise KimiAuthorizationError(f"Kimi authorization receipt {field} must be a non-empty string")
    return value.strip()


def load_authorization_receipt(path: Path) -> KimiAuthorizationReceipt:
    """Load a narrow local receipt without returning its raw approval evidence.

    The receipt is deliberately separate from environment variables so an
    unattended invocation cannot be enabled by an inherited shell setting.
    Required JSON fields are ``schema_version``, ``approved``, ``receipt_id``,
    ``provider``, ``scope``, ``approved_at``, and ``authorization_basis``.  The
    optional evidence field is intentionally not read into returned metadata
    or diagnostics.
    """
    receipt_path = Path(path).expanduser()
    try:
        raw = json.loads(receipt_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise KimiAuthorizationError("Kimi authorization receipt is required before dispatch") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise KimiAuthorizationError("Kimi authorization receipt is unreadable or invalid JSON") from exc
    if not isinstance(raw, dict):
        raise KimiAuthorizationError("Kimi authorization receipt must be a JSON object")
    if raw.get("schema_version") != AUTHORIZATION_SCHEMA_VERSION:
        raise KimiAuthorizationError("Kimi authorization receipt schema_version is not accepted")
    if raw.get("approved") is not True:
        raise KimiAuthorizationError("Kimi authorization receipt is not approved")
    scope = _nonempty_string(raw.get("scope"), "scope")
    if scope != AUTHORIZATION_SCOPE:
        raise KimiAuthorizationError("Kimi authorization receipt scope is not accepted")
    provider = _nonempty_string(raw.get("provider"), "provider")
    if provider.casefold() not in {"kimi", "moonshot", "moonshot ai", "moonshot-ai"}:
        raise KimiAuthorizationError("Kimi authorization receipt provider is not accepted")
    authorization_basis = _nonempty_string(raw.get("authorization_basis"), "authorization_basis")
    if authorization_basis not in AUTHORIZATION_BASES:
        raise KimiAuthorizationError("Kimi authorization receipt basis is not accepted")
    return KimiAuthorizationReceipt(
        receipt_id=_nonempty_string(raw.get("receipt_id"), "receipt_id"),
        provider=provider,
        scope=scope,
        approved_at=_nonempty_string(raw.get("approved_at"), "approved_at"),
        authorization_basis=authorization_basis,
    )


def _safe_environment(kimi_home: Path, extra_env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return a restrictive, explicit environment for a noninteractive run."""
    inherited_names = (
        "HOME",
        "PATH",
        "SHELL",
        "TMPDIR",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "NODE_EXTRA_CA_CERTS",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "no_proxy",
    )
    environment = {name: os.environ[name] for name in inherited_names if name in os.environ}
    environment.update(
        {
            "KIMI_CODE_HOME": str(kimi_home),
            "KIMI_DISABLE_TELEMETRY": "1",
            "KIMI_CODE_NO_AUTO_UPDATE": "1",
            "KIMI_DISABLE_CRON": "1",
            "KIMI_LOG_LEVEL": "off",
            "KIMI_MODEL_OUTPUT_FORMAT": "stream-json",
            "KIMI_CODE_AGENT_SWARM_MAX_CONCURRENCY": "1",
            "KIMI_CODE_BACKGROUND_KEEP_ALIVE_ON_EXIT": "0",
            "KIMI_CODE_BACKGROUND_MAX_RUNNING_TASKS": "1",
            "KIMI_LOOP_MAX_STEPS_PER_TURN": "3",
            "KIMI_LOOP_MAX_RETRIES_PER_STEP": "2",
            "KIMI_SUBAGENT_TIMEOUT_MS": "60000",
            "CI": "1",
            "NO_COLOR": "1",
        }
    )
    if extra_env:
        rejected = [str(key) for key in extra_env if not str(key).startswith("PIF_KIMI_TEST_")]
        if rejected:
            raise ValueError("extra_env is reserved for PIF_KIMI_TEST_* test controls")
        environment.update({str(key): str(value) for key, value in extra_env.items()})
    return environment


_DENIED_TOOLS = (
    "Write",
    "Edit",
    "Grep",
    "Glob",
    "Bash",
    "TaskList",
    "TaskOutput",
    "TaskStop",
    "CronCreate",
    "CronList",
    "CronDelete",
    "ReadMediaFile",
    "TodoList",
    "Skill",
    "WebSearch",
    "Agent",
    "AgentSwarm",
    "FetchURL",
    "AskUserQuestion",
    "EnterPlanMode",
    "ExitPlanMode",
    "CreateGoal",
    "GetGoal",
    "SetGoalBudget",
    "UpdateGoal",
    "select_tools",
    "Read(!job.json)",
    "mcp__*",
)


def _restricted_config_text() -> str:
    rules = "\n\n".join(
        "[[permission.rules]]\ndecision = \"deny\"\npattern = " + json.dumps(tool) for tool in _DENIED_TOOLS
    )
    return (
        "# Managed by Podcast Intelligence Factory; do not broaden.\n"
        "merge_all_available_skills = false\n"
        "telemetry = false\n\n"
        "[tools]\n"
        'enabled = ["Read"]\n\n'
        "[loop_control]\n"
        "max_steps_per_turn = 3\n"
        "max_retries_per_step = 2\n"
        "reserved_context_size = 8192\n\n"
        "[background]\n"
        "max_running_tasks = 1\n"
        "keep_alive_on_exit = false\n"
        "bash_auto_background_on_timeout = false\n"
        "bash_task_timeout_s = 60\n"
        "print_background_mode = \"exit\"\n"
        "print_wait_ceiling_s = 120\n"
        "print_max_turns = 1\n\n"
        "[subagent]\n"
        "timeout_ms = 60000\n\n"
        "[permission]\n"
        f"{rules}\n"
    )


def _configured_permission_rules(config_text: str) -> list[tuple[str, str]]:
    rules: list[tuple[str, str]] = []
    for block in config_text.split("[[permission.rules]]")[1:]:
        decision = re.search(r'^decision\s*=\s*"([^"]+)"\s*$', block, flags=re.MULTILINE)
        pattern = re.search(r'^pattern\s*=\s*("(?:\\.|[^"])*")\s*$', block, flags=re.MULTILINE)
        if not decision or not pattern:
            continue
        try:
            value = json.loads(pattern.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(value, str):
            rules.append((decision.group(1), value))
    return rules


def _has_read_only_tool_allowlist(config_text: str) -> bool:
    section = re.search(
        r"(?ms)^\[tools\]\s*(.*?)(?=^\[|\Z)",
        config_text,
    )
    if section is None:
        return False
    enabled = re.search(r'^enabled\s*=\s*\[\s*"Read"\s*\]\s*$', section.group(1), flags=re.MULTILINE)
    return enabled is not None


def _validate_restricted_config_text(config_text: str) -> None:
    required_settings = {
        "merge_all_available_skills = false",
        "telemetry = false",
        "max_steps_per_turn = 3",
        "max_retries_per_step = 2",
        'print_background_mode = "exit"',
        "print_max_turns = 1",
    }
    lines = {line.strip() for line in config_text.splitlines()}
    if not required_settings.issubset(lines):
        raise KimiCodeRunnerError("Dedicated Kimi home does not contain the required restricted policy")
    if not _has_read_only_tool_allowlist(config_text):
        raise KimiCodeRunnerError("Dedicated Kimi home does not contain the Read-only tool allowlist")
    expected_rules = [("deny", pattern) for pattern in _DENIED_TOOLS]
    if _configured_permission_rules(config_text) != expected_rules:
        raise KimiCodeRunnerError("Dedicated Kimi home permission rules do not match the pinned restricted policy")
    if re.search(r"(?m)^\s*(?:allow|ask|deny)\s*=", config_text):
        raise KimiCodeRunnerError("Dedicated Kimi home contains unsupported legacy permission rules")
    if re.search(r"(?m)^\s*(?:\[{1,2}hooks(?:\.|\])|hooks\s*=)", config_text):
        raise KimiCodeRunnerError("Dedicated Kimi home must not configure executable hooks")


def _ensure_restricted_config(kimi_home: Path) -> Path:
    """Install or verify the dedicated home's least-privilege tool policy."""
    config_path = kimi_home / "config.toml"
    if config_path.exists():
        try:
            existing = config_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise KimiCodeRunnerError("Kimi home config could not be read") from exc
        _validate_restricted_config_text(existing)
        return config_path
    config_text = _restricted_config_text()
    _validate_restricted_config_text(config_text)
    _write_private_text_atomic(config_path, config_text)
    return config_path


def _ensure_empty_mcp_config(kimi_home: Path) -> Path:
    mcp_path = kimi_home / "mcp.json"
    if mcp_path.exists():
        try:
            value = json.loads(mcp_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise KimiCodeRunnerError("Dedicated Kimi home MCP config is unreadable") from exc
        if value != {"mcpServers": {}}:
            raise KimiCodeRunnerError("Dedicated Kimi home must not configure MCP servers")
        return mcp_path
    _write_private_text_atomic(mcp_path, '{"mcpServers": {}}\n')
    return mcp_path


def _write_private_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            os.chmod(temporary, 0o600)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _run_preflight(
    argv: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    timeout_seconds: float = DEFAULT_PREFLIGHT_TIMEOUT_SECONDS,
    stage: str = "preflight",
) -> tuple[int, str, str]:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    started_at = time.monotonic()
    try:
        proc = subprocess.Popen(
            list(argv),
            cwd=str(cwd),
            env=dict(environment),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
    except OSError as exc:
        raise KimiCodeRunnerError(
            f"Kimi {stage} process could not be started",
            details={
                "error_class": "preflight_start",
                "preflight_stage": stage,
                "model_call_started": False,
                "may_have_consumed_quota": False,
                "network_call_made": False,
            },
        ) from exc
    try:
        stdout, stderr = proc.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        _kill_process_group(proc)
        try:
            proc.communicate(timeout=0.5)
        except subprocess.TimeoutExpired:
            pass
        raise KimiCodeRunnerError(
            f"Kimi {stage} process exceeded its {timeout_seconds:g}-second hard deadline",
            details={
                "error_class": "preflight_timeout",
                "preflight_stage": stage,
                "timeout_seconds": timeout_seconds,
                "duration_ms": int((time.monotonic() - started_at) * 1000),
                "model_call_started": False,
                "may_have_consumed_quota": False,
                "network_call_made": False,
            },
        ) from exc
    return int(proc.returncode or 0), stdout, stderr


def _verify_cli_profile(
    command: Sequence[str],
    *,
    config_path: Path,
    cwd: Path,
    environment: Mapping[str, str],
    expected_version: str | None,
) -> None:
    if expected_version is not None:
        exit_code, stdout, _stderr = _run_preflight(
            [*command, "--version"],
            cwd=cwd,
            environment=environment,
            stage="version check",
        )
        if exit_code != 0 or stdout.strip() != expected_version:
            raise KimiCodeRunnerError("Kimi Code version does not match the pinned compatible version")
    exit_code, _stdout, _stderr = _run_preflight(
        [*command, "doctor", "config", str(config_path)],
        cwd=cwd,
        environment=environment,
        stage="configuration check",
    )
    if exit_code != 0:
        raise KimiCodeRunnerError("Kimi rejected the dedicated restricted config")


def prepare_kimi_code_home(
    kimi_home: Path,
    *,
    command: Sequence[str] = ("kimi",),
    expected_version: str | None = EXPECTED_KIMI_CODE_VERSION,
) -> dict[str, Any]:
    """Prepare and locally validate the dedicated Kimi profile without a model call.

    This is safe for a lab ``setup`` or ``status`` command: it creates only
    local private directories/configuration and invokes ``--version`` plus
    ``doctor config``.  It neither authenticates nor sends a prompt.
    """
    if not command or any(not isinstance(part, str) or not part for part in command):
        raise ValueError("command must be a non-empty argv sequence")
    home = Path(kimi_home).expanduser().resolve()
    home.mkdir(parents=True, exist_ok=True)
    os.chmod(home, 0o700)
    skills_dir = home / "empty-skills"
    skills_dir.mkdir(mode=0o700, exist_ok=True)
    os.chmod(skills_dir, 0o700)
    environment = _safe_environment(home)
    config_path = _ensure_restricted_config(home)
    mcp_path = _ensure_empty_mcp_config(home)
    _verify_cli_profile(
        command,
        config_path=config_path,
        cwd=home,
        environment=environment,
        expected_version=expected_version,
    )
    return {
        "ok": True,
        "version": expected_version,
        "kimi_home": str(home),
        "config_path": str(config_path),
        "skills_dir": str(skills_dir),
        "mcp_path": str(mcp_path),
        "config_valid": True,
    }


def _read_limited(path: Path, limit: int) -> str:
    try:
        with path.open("rb") as handle:
            raw = handle.read(limit + 1)
    except OSError:
        return ""
    if len(raw) > limit:
        raw = raw[:limit] + b"\n[truncated]"
    return raw.decode("utf-8", errors="replace")


def _read_events(path: Path) -> str:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise KimiCodeRunnerError("Kimi did not produce a readable event stream") from exc
    if size > MAX_EVENT_BYTES:
        raise KimiCodeRunnerError("Kimi event stream exceeded the configured safety limit")
    return _read_limited(path, MAX_EVENT_BYTES)


def _diagnostic_summary(stderr: str) -> dict[str, Any]:
    """Keep caller-visible failures useful without exporting private job text."""
    encoded = stderr.encode("utf-8", errors="replace")
    return {
        "stderr_sha256": hashlib.sha256(encoded).hexdigest(),
        "stderr_chars": len(stderr),
        "stderr_truncated": stderr.endswith("\n[truncated]"),
    }


def inspect_kimi_code_provider_profile(
    kimi_home: Path,
    *,
    command: Sequence[str] = ("kimi",),
    extra_env: Mapping[str, str] | None = None,
    timeout_seconds: float = DEFAULT_PREFLIGHT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Inspect completed managed-login provisioning without network or inference.

    Kimi Code stores a newly acquired OAuth credential before it checks the
    account's managed ``/models`` endpoint.  A failed membership check can
    therefore leave a credential file behind.  On successful login, Kimi also
    provisions the ``managed:kimi-code`` OAuth provider and its model aliases.

    ``kimi provider list`` reads that local configuration only.  Its plain
    output exposes provider type, model count, and credential source, but not
    credential material.  This probe intentionally does not claim that an
    older token or membership is still valid now.
    """
    if not command or any(not isinstance(part, str) or not part for part in command):
        raise ValueError("command must be a non-empty argv sequence")
    home = Path(kimi_home).expanduser().resolve()
    environment = _safe_environment(home, extra_env)
    exit_code, stdout, stderr = _run_preflight(
        [*command, "provider", "list"],
        cwd=home,
        environment=environment,
        timeout_seconds=timeout_seconds,
        stage="provider profile check",
    )
    if len(stdout.encode("utf-8", errors="replace")) > MAX_PROVIDER_LIST_BYTES:
        raise KimiCodeRunnerError(
            "Kimi provider profile output exceeded the safety limit",
            details={"error_class": "provider_profile_output_limit"},
        )
    if exit_code != 0:
        raise KimiCodeRunnerError(
            "Kimi provider profile could not be inspected",
            details={
                "error_class": "provider_profile_nonzero_exit",
                "exit_code": exit_code,
                "diagnostics": _diagnostic_summary(stderr),
            },
        )
    managed_model_count = 0
    managed_provider_provisioned = False
    for line in stdout.splitlines():
        match = _MANAGED_KIMI_PROVIDER_LINE.fullmatch(line.strip())
        if match is None:
            continue
        managed_provider_provisioned = True
        managed_model_count = int(match.group(1))
        break
    return {
        "provider_probe_ok": True,
        "managed_oauth_provider_provisioned": managed_provider_provisioned,
        "managed_model_count": managed_model_count,
        "login_provisioning_complete": managed_provider_provisioned and managed_model_count > 0,
        "network_call_made": False,
        "model_call_made": False,
    }


def _content_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        return None
    pieces: list[str] = []
    for block in value:
        if isinstance(block, str):
            pieces.append(block)
        elif isinstance(block, dict):
            text = block.get("text")
            if isinstance(text, str):
                pieces.append(text)
            elif isinstance(block.get("content"), str):
                pieces.append(str(block["content"]))
    return "".join(pieces) if pieces else None


def _assistant_text(event: Mapping[str, Any]) -> str | None:
    """Extract text only from an event explicitly attributed to the assistant."""
    containers: list[Mapping[str, Any]] = [event]
    for name in ("message", "data", "response"):
        nested = event.get(name)
        if isinstance(nested, dict):
            containers.append(nested)
    for container in containers:
        role = container.get("role")
        event_type = str(container.get("type") or event.get("type") or "").casefold()
        if role != "assistant" and "assistant" not in event_type:
            continue
        for field in ("content", "text", "delta", "output_text"):
            text = _content_text(container.get(field))
            if text is not None:
                return text
    return None


def _event_is_assistant(event: Mapping[str, Any]) -> bool:
    containers: list[Mapping[str, Any]] = [event]
    for name in ("message", "data", "response"):
        nested = event.get(name)
        if isinstance(nested, dict):
            containers.append(nested)
    return any(
        container.get("role") == "assistant"
        or "assistant" in str(container.get("type") or event.get("type") or "").casefold()
        for container in containers
    )


def _event_has_role(event: Mapping[str, Any], roles: set[str]) -> bool:
    containers: list[Mapping[str, Any]] = [event]
    for name in ("message", "data", "response"):
        nested = event.get(name)
        if isinstance(nested, dict):
            containers.append(nested)
    return any(container.get("role") in roles for container in containers)


def _assistant_event_has_tool_calls(event: Mapping[str, Any]) -> bool:
    containers: list[Mapping[str, Any]] = [event]
    for name in ("message", "data", "response"):
        nested = event.get(name)
        if isinstance(nested, dict):
            containers.append(nested)
    return any(
        bool(container.get(field))
        for container in containers
        for field in ("tool_calls", "toolCalls", "tool_call", "toolCall")
    )


def _json_object_from_text(text: str) -> dict[str, Any] | None:
    candidate = text.strip()
    if candidate.startswith("```"):
        newline = candidate.find("\n")
        if newline >= 0 and candidate.endswith("```"):
            candidate = candidate[newline + 1 : -3].strip()
    try:
        value, ending = json.JSONDecoder().raw_decode(candidate)
    except json.JSONDecodeError:
        return None
    if candidate[ending:].strip() or not isinstance(value, dict):
        return None
    return value


def _parse_stream_json(stream: str) -> tuple[dict[str, Any], dict[str, int]]:
    """Parse Kimi's JSONL stream and return its final assistant JSON object.

    Kimi has used both whole-message and delta event variants.  We prefer the
    last standalone assistant payload, then try the concatenated deltas.  Bad
    JSONL or a non-JSON final response fails closed instead of guessing.
    """
    final_assistant_text: str | None = None
    final_assistant_has_tool_calls = False
    deltas: list[str] = []
    saw_event = False
    retry_count = 0
    for line_number, line in enumerate(stream.splitlines(), start=1):
        if not line.strip():
            continue
        saw_event = True
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise KimiCodeRunnerError(f"Kimi emitted invalid JSONL at event {line_number}") from exc
        if not isinstance(event, dict):
            continue
        if event.get("role") == "meta" and event.get("type") == "turn.step.retrying":
            retry_count += 1
        if _event_has_role(event, {"tool", "user"}):
            # A tool result or new user turn closes any prior assistant delta
            # stream.  If no later assistant response arrives, there is no
            # final label to publish.
            deltas = []
            final_assistant_text = None
            final_assistant_has_tool_calls = False
            continue
        if not _event_is_assistant(event):
            continue
        text = _assistant_text(event)
        event_type = str(event.get("type") or "").casefold()
        if "delta" in event_type:
            if text is not None:
                deltas.append(text)
                final_assistant_text = "".join(deltas)
            final_assistant_has_tool_calls = final_assistant_has_tool_calls or _assistant_event_has_tool_calls(event)
        else:
            deltas = []
            final_assistant_text = text
            final_assistant_has_tool_calls = _assistant_event_has_tool_calls(event)
    if not saw_event:
        raise KimiCodeRunnerError("Kimi emitted no stream events")
    if final_assistant_has_tool_calls:
        raise KimiCodeRunnerError("Kimi's final assistant response still requested a tool call")
    if final_assistant_text is not None:
        parsed = _json_object_from_text(final_assistant_text)
        if parsed is not None:
            return parsed, {"retry_count": retry_count}
    raise KimiCodeRunnerError("Kimi did not emit a final assistant JSON object")


def parse_stream_json_output(stream: str) -> dict[str, Any]:
    """Return the final assistant JSON object while hiding transport metrics."""
    parsed, _metrics = _parse_stream_json(stream)
    return parsed


def _validate_output(
    output: dict[str, Any],
    *,
    schema_validator: Callable[[dict[str, Any]], Any] | None,
    json_schema: Mapping[str, Any] | None,
) -> str:
    if schema_validator is not None and json_schema is not None:
        raise ValueError("pass schema_validator or json_schema, not both")
    if schema_validator is not None:
        try:
            schema_validator(output)
        except Exception as exc:
            raise KimiCodeRunnerError("Kimi output failed caller schema validation") from exc
        return "caller"
    if json_schema is not None:
        try:
            import jsonschema  # type: ignore[import-not-found]
        except ImportError as exc:
            raise KimiCodeRunnerError("jsonschema is required when json_schema validation is requested") from exc
        try:
            jsonschema.validate(instance=output, schema=dict(json_schema))
        except Exception as exc:
            raise KimiCodeRunnerError("Kimi output failed JSON Schema validation") from exc
        return "jsonschema"
    return "not_requested"


def serialize_kimi_code_job(payload: Mapping[str, Any]) -> str:
    """Render a complete request that Kimi's bounded Read tool can see."""
    try:
        rendered = json.dumps(
            {"schema_version": JOB_SCHEMA_VERSION, "payload": dict(payload)},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n"
    except (TypeError, ValueError) as exc:
        raise KimiCodeRunnerError("Kimi job payload is not JSON serializable") from exc
    encoded = rendered.encode("utf-8")
    if len(encoded) > MAX_JOB_BYTES:
        raise KimiCodeRunnerError("Kimi job payload exceeds the Read-safe byte limit")
    lines = rendered.split("\n")
    if len(lines) > MAX_JOB_LINES:
        raise KimiCodeRunnerError("Kimi job payload exceeds the Read-safe line limit")
    if any(len(line.encode("utf-8")) > MAX_JOB_LINE_BYTES for line in lines):
        raise KimiCodeRunnerError("Kimi job payload contains a line that the Read tool would truncate")
    return rendered


def _write_json_new_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    """Create a JSON artifact atomically without replacing an existing file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            os.chmod(temporary, 0o600)
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise KimiCodeRunnerError("Kimi output path already exists") from exc
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _kill_process_group(proc: subprocess.Popen[Any]) -> None:
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        try:
            proc.kill()
        except OSError:
            pass


def run_kimi_code_job(
    *,
    payload: Mapping[str, Any],
    output_path: Path,
    authorization_receipt_path: Path,
    job_root: Path,
    kimi_home: Path | None = None,
    model: str | None = DEFAULT_MODEL,
    command: Sequence[str] = ("kimi",),
    timeout_seconds: int = 900,
    schema_validator: Callable[[dict[str, Any]], Any] | None = None,
    json_schema: Mapping[str, Any] | None = None,
    extra_env: Mapping[str, str] | None = None,
    expected_cli_version: str | None = EXPECTED_KIMI_CODE_VERSION,
    retain_job_directory: bool = False,
) -> dict[str, Any]:
    """Run one native Kimi Code job with a hard deadline and fail-closed output.

    ``command`` is dependency-injection friendly for tests (for example,
    ``(sys.executable, fake_kimi_path)``).  It is always passed to ``Popen`` as
    an argv list, never through a shell.  The only prompt text in argv is a
    fixed instruction; the payload stays in ``job.json`` in an isolated mode
    0700 directory.
    """
    if not isinstance(payload, Mapping):
        raise TypeError("payload must be a mapping")
    if not command or any(not isinstance(part, str) or not part for part in command):
        raise ValueError("command must be a non-empty argv sequence")
    if model is not None and (not isinstance(model, str) or not model.strip() or "\x00" in model):
        raise ValueError("model must be None or a non-empty string")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    # This is deliberately first: a missing or malformed receipt must never
    # create a job directory or start a process that could consume quota.
    authorization = load_authorization_receipt(Path(authorization_receipt_path))

    resolved_output_path = Path(output_path).expanduser().resolve()
    if resolved_output_path.exists():
        raise KimiCodeRunnerError("Kimi output path already exists")
    serialized_job = serialize_kimi_code_job(payload)

    root = Path(job_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    home = Path(kimi_home).expanduser().resolve() if kimi_home else root / "kimi-code-home"
    prepared_home = prepare_kimi_code_home(
        home,
        command=command,
        expected_version=expected_cli_version,
    )
    job_dir = Path(tempfile.mkdtemp(prefix="kimi-code-job-", dir=str(root)))
    os.chmod(job_dir, 0o700)
    try:
        job_path = job_dir / "job.json"
        stdout_path = job_dir / "events.jsonl"
        stderr_path = job_dir / "stderr.log"
        _write_private_text_atomic(job_path, serialized_job)
        environment = _safe_environment(home, extra_env)
        skills_dir = Path(str(prepared_home["skills_dir"]))
        prompt = "Read job.json in the current working directory and return only its requested JSON object."
        argv = [
            *command,
            "--skills-dir",
            str(skills_dir),
            "-p",
            prompt,
            "--output-format",
            "stream-json",
        ]
        if model is not None:
            argv.extend(["-m", model.strip()])
        dispatch_started_at: float | None = None
        try:
            with stdout_path.open("xb") as stdout_handle, stderr_path.open("xb") as stderr_handle:
                proc = subprocess.Popen(
                    argv,
                    cwd=str(job_dir),
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    start_new_session=True,
                )
                dispatch_started_at = time.monotonic()
                try:
                    exit_code = proc.wait(timeout=timeout_seconds)
                except subprocess.TimeoutExpired as exc:
                    _kill_process_group(proc)
                    try:
                        proc.wait(timeout=0.5)
                    except subprocess.TimeoutExpired:
                        pass
                    raise KimiCodeRunnerError(
                        "Kimi process exceeded its hard deadline",
                        details={
                            "error_class": "process_timeout",
                            "timeout_seconds": timeout_seconds,
                            "model_call_started": True,
                            "may_have_consumed_quota": True,
                            "duration_ms": int((time.monotonic() - dispatch_started_at) * 1000),
                        },
                    ) from exc
        except FileNotFoundError as exc:
            raise KimiCodeRunnerError(
                "Kimi executable was not found",
                details={
                    "error_class": "process_start",
                    "model_call_started": False,
                    "may_have_consumed_quota": False,
                },
            ) from exc
        except OSError as exc:
            raise KimiCodeRunnerError(
                "Kimi process could not be started",
                details={
                    "error_class": "process_start",
                    "model_call_started": False,
                    "may_have_consumed_quota": False,
                },
            ) from exc

        stderr = _read_limited(stderr_path, MAX_DIAGNOSTIC_BYTES)
        duration_ms = int((time.monotonic() - dispatch_started_at) * 1000) if dispatch_started_at is not None else 0
        if exit_code != 0:
            raise KimiCodeRunnerError(
                "Kimi process exited unsuccessfully",
                details={
                    "error_class": "nonzero_exit",
                    "exit_code": int(exit_code),
                    "diagnostics": _diagnostic_summary(stderr),
                    "model_call_started": True,
                    "may_have_consumed_quota": True,
                    "duration_ms": duration_ms,
                },
            )
        try:
            parsed, stream_metrics = _parse_stream_json(_read_events(stdout_path))
            validation_mode = _validate_output(parsed, schema_validator=schema_validator, json_schema=json_schema)
            _write_json_new_atomic(resolved_output_path, parsed)
        except KimiCodeRunnerError as exc:
            raise KimiCodeRunnerError(
                str(exc),
                details={
                    **exc.details,
                    "model_call_started": True,
                    "may_have_consumed_quota": True,
                    "duration_ms": int((time.monotonic() - dispatch_started_at) * 1000),
                },
            ) from exc
        except Exception as exc:
            raise KimiCodeRunnerError(
                "Kimi output could not be safely published",
                details={
                    "error_class": "post_dispatch_publication",
                    "model_call_started": True,
                    "may_have_consumed_quota": True,
                    "duration_ms": int((time.monotonic() - dispatch_started_at) * 1000),
                },
            ) from exc
        duration_ms = int((time.monotonic() - dispatch_started_at) * 1000)
        return {
            "ok": True,
            "model": model.strip() if model is not None else None,
            "job_dir": str(job_dir) if retain_job_directory else None,
            "job_dir_retained": retain_job_directory,
            "output_path": str(resolved_output_path),
            "exit_code": int(exit_code),
            "schema_validation": validation_mode,
            "duration_ms": duration_ms,
            "retry_count": stream_metrics["retry_count"],
            "actual_model_verified": False,
            "actual_model_id": None,
            "authorization": authorization.sanitized_metadata(),
            "diagnostics": _diagnostic_summary(stderr),
        }
    finally:
        if not retain_job_directory:
            shutil.rmtree(job_dir, ignore_errors=True)
