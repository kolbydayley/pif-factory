from __future__ import annotations

import asyncio
import hashlib
import importlib.resources
import json
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .util import now_iso, sha256_text, write_text_atomic


PINNED_CODEX_CLI_VERSION = "0.147.0"
APP_SERVER_CLIENT_VERSION = "pif-codex-app-server-v4"
TURN_SIDECAR_SCHEMA_VERSION = "pif_codex_app_server_turn_v2"
PROTOCOL_SCHEMA_SHA256 = "ff10829cd75b67297019b39ab508ac699198574663579aa18336b7dc55ea178f"
PROTOCOL_SCHEMA_PATH = (
    Path(__file__).resolve().parent
    / "protocol"
    / "codex_app_server_0_147_0"
    / "codex_app_server_protocol.v2.schemas.json"
)
PROTOCOL_SCHEMA_RESOURCE = (
    "protocol/codex_app_server_0_147_0/"
    "codex_app_server_protocol.v2.schemas.json"
)
THREAD_GOAL_STATUSES = frozenset(
    {"active", "paused", "blocked", "usageLimited", "budgetLimited", "complete"}
)


class AppServerError(RuntimeError):
    pass


class AppServerProtocolError(AppServerError):
    pass


class AppServerRPCError(AppServerError):
    def __init__(self, method: str, code: Any):
        self.method = method
        self.code = code
        super().__init__(f"app-server method {method} failed with code {code}")


class AppServerProcessDied(AppServerError):
    pass


class AppServerAuthError(AppServerError):
    pass


class AppServerRecoveryRequired(AppServerError):
    pass


class AppServerTurnTimeout(AppServerError):
    pass


class AppServerStructuredOutputError(AppServerError):
    pass


@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int
    cached_input_tokens: int
    output_tokens: int
    reasoning_output_tokens: int
    total_tokens: int

    @classmethod
    def from_protocol(cls, value: Any) -> TokenUsage:
        if not isinstance(value, dict):
            raise AppServerProtocolError("token usage is not an object")
        fields = {
            "input_tokens": value.get("inputTokens"),
            "cached_input_tokens": value.get("cachedInputTokens"),
            "output_tokens": value.get("outputTokens"),
            "reasoning_output_tokens": value.get("reasoningOutputTokens"),
            "total_tokens": value.get("totalTokens"),
        }
        if any(isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in fields.values()):
            raise AppServerProtocolError("token usage contains an invalid count")
        usage = cls(**fields)
        if usage.cached_input_tokens > usage.input_tokens:
            raise AppServerProtocolError("cached input tokens exceed input tokens")
        if usage.reasoning_output_tokens > usage.output_tokens:
            raise AppServerProtocolError("reasoning output tokens exceed output tokens")
        if usage.total_tokens != usage.input_tokens + usage.output_tokens:
            raise AppServerProtocolError("total tokens do not equal input plus output")
        return usage


@dataclass(frozen=True)
class AppServerThread:
    thread_id: str
    model: str
    cwd: str
    ephemeral: bool
    instruction_sources_sha256: str
    instruction_sources_count: int
    base_instructions_sha256: str
    base_instructions_bytes: int
    reasoning_effort: str | None = None
    persisted_path_sha256: str | None = None
    prior_completed_turn_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class AppServerThreadArchiveState:
    thread_id: str
    state: str
    active_match_count: int
    archived_match_count: int


@dataclass(frozen=True)
class AppServerThreadGoal:
    thread_id: str
    objective: str
    status: str
    token_budget: int | None
    tokens_used: int
    time_used_seconds: int
    created_at: int
    updated_at: int

    @classmethod
    def from_protocol(cls, value: Any) -> AppServerThreadGoal:
        if not isinstance(value, dict):
            raise AppServerProtocolError("thread goal is not an object")
        thread_id = value.get("threadId")
        objective = value.get("objective")
        status = value.get("status")
        if not isinstance(thread_id, str) or not thread_id:
            raise AppServerProtocolError("thread goal has an invalid thread id")
        if not isinstance(objective, str):
            raise AppServerProtocolError("thread goal has an invalid objective")
        if not isinstance(status, str) or status not in THREAD_GOAL_STATUSES:
            raise AppServerProtocolError("thread goal has an invalid status")

        integer_fields = {
            "tokens_used": value.get("tokensUsed"),
            "time_used_seconds": value.get("timeUsedSeconds"),
            "created_at": value.get("createdAt"),
            "updated_at": value.get("updatedAt"),
        }
        if any(
            isinstance(item, bool) or not isinstance(item, int) or item < 0
            for item in integer_fields.values()
        ):
            raise AppServerProtocolError("thread goal contains an invalid integer field")
        token_budget = value.get("tokenBudget")
        if token_budget is not None and (
            isinstance(token_budget, bool) or not isinstance(token_budget, int) or token_budget < 0
        ):
            raise AppServerProtocolError("thread goal has an invalid token budget")
        return cls(
            thread_id=thread_id,
            objective=objective,
            status=status,
            token_budget=token_budget,
            **integer_fields,
        )


@dataclass(frozen=True)
class AppServerTurnResult:
    thread_id: str
    turn_id: str
    status: str
    status_ok: bool
    output: Any
    output_text: str | None
    output_sha256: str | None
    usage: TokenUsage | None
    thread_total_usage: TokenUsage | None
    wall_elapsed_seconds: float
    error_class: str | None
    sidecar_path: str


class _TurnState:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.completed = loop.create_future()
        self.usage_arrived = asyncio.Event()
        self.agent_messages: list[dict[str, Any]] = []
        self.last_usage: TokenUsage | None = None
        self.total_usage: TokenUsage | None = None

    def final_message(self) -> str | None:
        final = [item for item in self.agent_messages if item.get("phase") == "final_answer"]
        selected = final[-1] if final else (self.agent_messages[-1] if self.agent_messages else None)
        text = selected.get("text") if selected else None
        return text if isinstance(text, str) else None


def _protocol_schema_bytes(path: str | Path | None = None) -> bytes:
    if path is not None:
        return Path(path).expanduser().resolve().read_bytes()
    return (
        importlib.resources.files("research_factory")
        .joinpath(PROTOCOL_SCHEMA_RESOURCE)
        .read_bytes()
    )


def protocol_schema_sha256(path: str | Path | None = None) -> str:
    return hashlib.sha256(_protocol_schema_bytes(path)).hexdigest()


def verify_protocol_schema(
    path: str | Path | None = None,
    *,
    expected_sha256: str = PROTOCOL_SCHEMA_SHA256,
) -> str:
    observed = protocol_schema_sha256(path)
    if observed != expected_sha256:
        raise AppServerProtocolError(
            f"app-server protocol schema drift: expected {expected_sha256}, observed {observed}"
        )
    return observed


def installed_codex_version(binary: str = "codex") -> str:
    result = subprocess.run(
        [binary, "--version"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    if result.returncode != 0:
        raise AppServerProtocolError("could not read Codex CLI version")
    prefix = "codex-cli "
    version = result.stdout.strip()
    if not version.startswith(prefix):
        raise AppServerProtocolError("unexpected Codex CLI version response")
    return version[len(prefix) :]


def verify_codex_version(
    binary: str = "codex",
    *,
    expected_version: str = PINNED_CODEX_CLI_VERSION,
) -> str:
    observed = installed_codex_version(binary)
    if observed != expected_version:
        raise AppServerProtocolError(
            f"Codex CLI drift: expected {expected_version}, observed {observed}"
        )
    return observed


def _text_metadata(prefix: str, value: str | None, *, include_text: bool) -> dict[str, Any]:
    text = str(value or "")
    result = {
        f"{prefix}_sha256": sha256_text(text),
        f"{prefix}_bytes": len(text.encode("utf-8")),
    }
    if include_text:
        result[f"{prefix}_text"] = text[:2000]
    return result


def finalize_cancelled_turn_sidecar(
    path: str | Path,
    *,
    interruption_method: str,
    parent_exit_code: int | None,
    protocol_interrupt_sent: bool,
) -> dict[str, Any]:
    sidecar_path = Path(path).expanduser().resolve()
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    if payload.get("state") not in {"started", "in_progress"}:
        raise AppServerRecoveryRequired("only a started or in-progress sidecar can be finalized as cancelled")
    usage_complete = bool(payload.get("usage_complete")) and isinstance(payload.get("usage"), dict)
    payload.update(
        {
            "state": "cancelled",
            "status": "cancelled",
            "error_class": "operator_cancelled",
            "finished_at": now_iso(),
            "usage_complete": usage_complete,
            "usage_status": "measured" if usage_complete else "unknown",
            "interrupt_attempted": True,
            "interrupt_acknowledged": None,
            "protocol_interrupt_sent": protocol_interrupt_sent,
            "interruption_method": interruption_method,
            "parent_exit_code": parent_exit_code,
            "recovery_reran_model": False,
        }
    )
    if not usage_complete:
        payload["usage"] = None
        payload["thread_total_usage"] = None
    write_text_atomic(
        sidecar_path,
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
    )
    return payload


class CodexAppServerClient:
    def __init__(
        self,
        *,
        command: list[str] | None = None,
        verify_cli: bool = True,
        request_timeout_seconds: float = 30.0,
        interrupt_timeout_seconds: float = 10.0,
        usage_grace_seconds: float = 1.0,
        max_message_bytes: int = 32 * 1024 * 1024,
        synthetic_debug_errors: bool = False,
        expected_cli_version: str = PINNED_CODEX_CLI_VERSION,
        protocol_schema_path: str | Path | None = None,
        expected_protocol_schema_sha256: str = PROTOCOL_SCHEMA_SHA256,
    ):
        if max_message_bytes < 64 * 1024:
            raise ValueError("app-server message limit must be at least 64 KiB")
        self.command = command or ["codex", "app-server", "--stdio", "--strict-config"]
        self.verify_cli = verify_cli
        self.request_timeout_seconds = request_timeout_seconds
        self.interrupt_timeout_seconds = interrupt_timeout_seconds
        self.usage_grace_seconds = usage_grace_seconds
        self.max_message_bytes = max_message_bytes
        self.synthetic_debug_errors = synthetic_debug_errors
        self.expected_cli_version = expected_cli_version
        self.protocol_schema_path = protocol_schema_path
        self.expected_protocol_schema_sha256 = expected_protocol_schema_sha256
        self.process: asyncio.subprocess.Process | None = None
        self.account_summary: dict[str, Any] | None = None
        self.models: list[dict[str, Any]] = []
        self.cli_version: str | None = None
        self.app_server_user_agent: str | None = None
        self.protocol_schema_sha256: str | None = None
        self._request_id = 0
        self._pending: dict[int, tuple[str, asyncio.Future]] = {}
        self._turn_states: dict[tuple[str, str], _TurnState] = {}
        self._thread_locks: dict[str, asyncio.Lock] = {}
        self._write_lock = asyncio.Lock()
        self._reader_task: asyncio.Task | None = None
        self._wait_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None
        self._closing = False
        self._fatal_error: AppServerError | None = None
        self._stderr_hash = hashlib.sha256()
        self._stderr_bytes = 0

    async def __aenter__(self) -> CodexAppServerClient:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:  # noqa: ANN001
        await self.close()

    async def start(self) -> None:
        if self.process is not None:
            return
        self.protocol_schema_sha256 = verify_protocol_schema(
            self.protocol_schema_path,
            expected_sha256=self.expected_protocol_schema_sha256,
        )
        if self.verify_cli:
            self.cli_version = await asyncio.to_thread(
                verify_codex_version,
                self.command[0],
                expected_version=self.expected_cli_version,
            )
        else:
            self.cli_version = self.expected_cli_version
        self.process = await asyncio.create_subprocess_exec(
            *self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=self.max_message_bytes,
        )
        self._reader_task = asyncio.create_task(self._reader_loop())
        self._wait_task = asyncio.create_task(self._wait_loop())
        self._stderr_task = asyncio.create_task(self._stderr_loop())
        try:
            initialize_result = await self._request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "podcast-intelligence-factory",
                        "title": "Podcast Intelligence Factory",
                        "version": APP_SERVER_CLIENT_VERSION,
                    },
                    "capabilities": {"experimentalApi": True},
                },
            )
            if not isinstance(initialize_result, dict) or not isinstance(
                initialize_result.get("userAgent"), str
            ):
                raise AppServerProtocolError("initialize response is malformed")
            self.app_server_user_agent = initialize_result["userAgent"]
            await self._notify("initialized")
            account_result = await self._request("account/read", {"refreshToken": False})
            account = account_result.get("account") if isinstance(account_result, dict) else None
            if not isinstance(account, dict) or account.get("type") != "chatgpt":
                raise AppServerAuthError("app-server must use a managed ChatGPT account")
            self.account_summary = {
                "type": "chatgpt",
                "plan_type": account.get("planType"),
                "requires_openai_auth": bool(account_result.get("requiresOpenaiAuth")),
            }
            self.models = await self._read_models()
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        if self.process is None:
            return
        self._closing = True
        if self.process.stdin is not None:
            self.process.stdin.close()
        try:
            await asyncio.wait_for(self.process.wait(), timeout=2)
        except asyncio.TimeoutError:
            try:
                self.process.terminate()
            except ProcessLookupError:
                # The child may exit between wait_for timing out and the
                # termination signal.  Treat that cleanup race as closed.
                pass
            try:
                await asyncio.wait_for(self.process.wait(), timeout=2)
            except asyncio.TimeoutError:
                try:
                    self.process.kill()
                except ProcessLookupError:
                    pass
                await self.process.wait()
        current = asyncio.current_task()
        for task in (self._reader_task, self._wait_task, self._stderr_task):
            if task is not None and task is not current and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._reader_task, self._wait_task, self._stderr_task) if task is not None),
            return_exceptions=True,
        )
        self.process = None

    async def start_thread(
        self,
        *,
        model: str,
        base_instructions: str,
        cwd: str | Path,
        ephemeral: bool = True,
    ) -> AppServerThread:
        self._require_started()
        if not self._model_available(model):
            raise AppServerProtocolError(f"model is not advertised by app-server: {model}")
        resolved_cwd = str(Path(cwd).expanduser().resolve())
        result = await self._request(
            "thread/start",
            {
                "model": model,
                "cwd": resolved_cwd,
                "baseInstructions": base_instructions,
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "ephemeral": ephemeral,
                "dynamicTools": [],
                "environments": [],
                "allowProviderModelFallback": False,
                "experimentalRawEvents": False,
                "serviceName": "podcast-intelligence-factory",
            },
        )
        return self._thread_from_response(
            result,
            method="thread/start",
            expected_thread_id=None,
            model=model,
            base_instructions=base_instructions,
            resolved_cwd=resolved_cwd,
            ephemeral=ephemeral,
            expected_effort=None,
            expected_instruction_sources_sha256=None,
            expected_instruction_sources_count=None,
            expected_completed_turn_ids=(),
        )

    async def resume_thread(
        self,
        *,
        thread_id: str,
        model: str,
        effort: str,
        base_instructions: str,
        cwd: str | Path,
        expected_instruction_sources_sha256: str,
        expected_instruction_sources_count: int,
        expected_completed_turn_ids: tuple[str, ...],
    ) -> AppServerThread:
        """Resume one exact durable thread without replaying completed turns."""

        self._require_started()
        validated_thread_id = self._validate_thread_id(thread_id)
        if not self._model_available(model):
            raise AppServerProtocolError(f"model is not advertised by app-server: {model}")
        if not isinstance(effort, str) or not effort:
            raise ValueError("reasoning effort must be nonempty")
        if (
            not isinstance(expected_instruction_sources_sha256, str)
            or len(expected_instruction_sources_sha256) != 64
            or isinstance(expected_instruction_sources_count, bool)
            or not isinstance(expected_instruction_sources_count, int)
            or expected_instruction_sources_count < 0
        ):
            raise ValueError("expected instruction-source metadata is invalid")
        if (
            not isinstance(expected_completed_turn_ids, tuple)
            or not expected_completed_turn_ids
            or any(not isinstance(value, str) or not value for value in expected_completed_turn_ids)
            or len(set(expected_completed_turn_ids)) != len(expected_completed_turn_ids)
        ):
            raise ValueError("expected completed turn lineage is invalid")
        resolved_cwd = str(Path(cwd).expanduser().resolve())
        result = await self._request(
            "thread/resume",
            {
                "threadId": validated_thread_id,
                "model": model,
                "cwd": resolved_cwd,
                "baseInstructions": base_instructions,
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "excludeTurns": False,
            },
        )
        return self._thread_from_response(
            result,
            method="thread/resume",
            expected_thread_id=validated_thread_id,
            model=model,
            base_instructions=base_instructions,
            resolved_cwd=resolved_cwd,
            ephemeral=False,
            expected_effort=effort,
            expected_instruction_sources_sha256=expected_instruction_sources_sha256,
            expected_instruction_sources_count=expected_instruction_sources_count,
            expected_completed_turn_ids=expected_completed_turn_ids,
        )

    async def archive_thread(self, thread_id: str) -> None:
        self._require_started()
        validated_thread_id = self._validate_thread_id(thread_id)
        result = await self._request(
            "thread/archive", {"threadId": validated_thread_id}
        )
        if result != {}:
            raise AppServerProtocolError("thread/archive response is malformed")

    async def thread_archive_state(
        self, thread_id: str
    ) -> AppServerThreadArchiveState:
        """Observe one durable thread in both active and archived listings."""

        self._require_started()
        validated_thread_id = self._validate_thread_id(thread_id)
        active_match_count = await self._thread_list_match_count(
            validated_thread_id, archived=False
        )
        archived_match_count = await self._thread_list_match_count(
            validated_thread_id, archived=True
        )
        if active_match_count == 1 and archived_match_count == 0:
            state = "active"
        elif active_match_count == 0 and archived_match_count == 1:
            state = "archived"
        elif active_match_count == 0 and archived_match_count == 0:
            state = "absent"
        else:
            raise AppServerProtocolError(
                "thread archive state is duplicated or contradictory"
            )
        return AppServerThreadArchiveState(
            thread_id=validated_thread_id,
            state=state,
            active_match_count=active_match_count,
            archived_match_count=archived_match_count,
        )

    async def _thread_list_match_count(
        self, thread_id: str, *, archived: bool
    ) -> int:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        match_count = 0
        for _page in range(10_000):
            params: dict[str, Any] = {
                "archived": archived,
                "limit": 100,
                "sourceKinds": ["appServer"],
            }
            if cursor is not None:
                params["cursor"] = cursor
            result = await self._request("thread/list", params)
            if not isinstance(result, dict) or not isinstance(
                result.get("data"), list
            ):
                raise AppServerProtocolError("thread/list response is malformed")
            for item in result["data"]:
                if not isinstance(item, dict) or not isinstance(item.get("id"), str):
                    raise AppServerProtocolError(
                        "thread/list contains a malformed thread"
                    )
                if item["id"] == thread_id:
                    match_count += 1
            next_cursor = result.get("nextCursor")
            if next_cursor is None:
                return match_count
            if (
                not isinstance(next_cursor, str)
                or not next_cursor
                or next_cursor in seen_cursors
            ):
                raise AppServerProtocolError("thread/list cursor is malformed")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise AppServerProtocolError("thread/list pagination exceeded its bound")

    def _thread_from_response(
        self,
        result: Any,
        *,
        method: str,
        expected_thread_id: str | None,
        model: str,
        base_instructions: str,
        resolved_cwd: str,
        ephemeral: bool,
        expected_effort: str | None,
        expected_instruction_sources_sha256: str | None,
        expected_instruction_sources_count: int | None,
        expected_completed_turn_ids: tuple[str, ...],
    ) -> AppServerThread:
        thread = result.get("thread") if isinstance(result, dict) else None
        if not isinstance(thread, dict):
            raise AppServerProtocolError(f"{method} response is malformed")
        thread_id = thread.get("id")
        if (
            not isinstance(thread_id, str)
            or not thread_id
            or (expected_thread_id is not None and thread_id != expected_thread_id)
            or result.get("model") != model
            or result.get("cwd") != resolved_cwd
            or thread.get("cwd") != resolved_cwd
            or thread.get("ephemeral") is not ephemeral
            or result.get("approvalPolicy") != "never"
            or not isinstance(result.get("sandbox"), dict)
            or result["sandbox"].get("type") != "readOnly"
        ):
            raise AppServerProtocolError(
                f"{method} did not preserve the requested thread configuration"
            )
        persisted_path = thread.get("path")
        if ephemeral:
            if persisted_path is not None:
                raise AppServerProtocolError(f"{method} materialized an ephemeral thread")
        elif not isinstance(persisted_path, str) or not persisted_path:
            raise AppServerProtocolError(f"{method} did not return a persisted thread path")
        returned_effort = result.get("reasoningEffort")
        if expected_effort is not None and returned_effort != expected_effort:
            raise AppServerProtocolError(
                f"{method} did not preserve the expected reasoning effort"
            )
        instruction_sources = result.get("instructionSources") or []
        if not isinstance(instruction_sources, list) or not all(
            isinstance(item, str) for item in instruction_sources
        ):
            raise AppServerProtocolError(f"{method} instructionSources is malformed")
        instruction_sources_json = json.dumps(
            instruction_sources,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        instruction_sources_sha256 = sha256_text(instruction_sources_json)
        if (
            expected_instruction_sources_sha256 is not None
            and instruction_sources_sha256 != expected_instruction_sources_sha256
        ) or (
            expected_instruction_sources_count is not None
            and len(instruction_sources) != expected_instruction_sources_count
        ):
            raise AppServerProtocolError(
                f"{method} instruction sources drifted from the durable thread"
            )
        returned_completed_turn_ids: tuple[str, ...] = ()
        if expected_completed_turn_ids:
            turns = thread.get("turns")
            if not isinstance(turns, list):
                raise AppServerProtocolError(f"{method} did not return prior turns")
            completed = []
            for turn in turns:
                if not isinstance(turn, dict):
                    raise AppServerProtocolError(f"{method} prior turn is malformed")
                turn_id = turn.get("id")
                if not isinstance(turn_id, str) or not turn_id:
                    raise AppServerProtocolError(f"{method} prior turn id is malformed")
                if turn.get("status") == "completed":
                    completed.append(turn_id)
            returned_completed_turn_ids = tuple(completed)
            if returned_completed_turn_ids != expected_completed_turn_ids:
                raise AppServerProtocolError(
                    f"{method} completed-turn lineage drifted"
                )
        self._thread_locks.setdefault(thread_id, asyncio.Lock())
        return AppServerThread(
            thread_id=thread_id,
            model=model,
            cwd=resolved_cwd,
            ephemeral=ephemeral,
            instruction_sources_sha256=instruction_sources_sha256,
            instruction_sources_count=len(instruction_sources),
            base_instructions_sha256=sha256_text(base_instructions),
            base_instructions_bytes=len(base_instructions.encode("utf-8")),
            reasoning_effort=(
                str(returned_effort) if isinstance(returned_effort, str) else None
            ),
            persisted_path_sha256=(
                sha256_text(persisted_path) if isinstance(persisted_path, str) else None
            ),
            prior_completed_turn_ids=returned_completed_turn_ids,
        )

    async def get_thread_goal(self, thread_id: str) -> AppServerThreadGoal | None:
        self._require_started()
        validated_thread_id = self._validate_thread_id(thread_id)
        result = await self._request("thread/goal/get", {"threadId": validated_thread_id})
        if not isinstance(result, dict):
            raise AppServerProtocolError("thread/goal/get response is malformed")
        goal_value = result.get("goal")
        if goal_value is None:
            return None
        goal = AppServerThreadGoal.from_protocol(goal_value)
        if goal.thread_id != validated_thread_id:
            raise AppServerProtocolError("thread/goal/get returned a different thread id")
        return goal

    async def set_thread_goal_status(
        self,
        thread_id: str,
        status: str,
    ) -> AppServerThreadGoal:
        self._require_started()
        validated_thread_id = self._validate_thread_id(thread_id)
        if not isinstance(status, str) or status not in THREAD_GOAL_STATUSES:
            allowed = ", ".join(sorted(THREAD_GOAL_STATUSES))
            raise ValueError(f"thread goal status must be one of: {allowed}")
        result = await self._request(
            "thread/goal/set",
            {"threadId": validated_thread_id, "status": status},
        )
        goal_value = result.get("goal") if isinstance(result, dict) else None
        if goal_value is None:
            raise AppServerProtocolError("thread/goal/set response is malformed")
        goal = AppServerThreadGoal.from_protocol(goal_value)
        if goal.thread_id != validated_thread_id:
            raise AppServerProtocolError("thread/goal/set returned a different thread id")
        if goal.status != status:
            raise AppServerProtocolError("thread/goal/set did not preserve the requested status")
        return goal

    async def read_weekly_rate_limit(self) -> dict[str, Any]:
        """Read the live managed-account weekly window without starting a model turn.

        Workload governors must not infer capacity from an old transcript of a
        UI session.  The app-server protocol exposes this read directly; keep
        only numeric capacity metadata so callers cannot accidentally persist
        account identity or other account fields.
        """

        self._require_started()
        result = await self._request("account/rateLimits/read", None)
        snapshot = result.get("rateLimits") if isinstance(result, dict) else None
        primary = snapshot.get("primary") if isinstance(snapshot, dict) else None
        if not isinstance(primary, dict):
            raise AppServerProtocolError("account/rateLimits/read has no primary window")
        used = primary.get("usedPercent")
        resets_at = primary.get("resetsAt")
        if isinstance(used, bool) or not isinstance(used, (int, float)):
            raise AppServerProtocolError("rate-limit used percentage is invalid")
        if isinstance(resets_at, bool) or not isinstance(resets_at, int) or resets_at <= 0:
            raise AppServerProtocolError("rate-limit reset timestamp is invalid")
        duration = primary.get("windowDurationMins")
        if duration is not None and (isinstance(duration, bool) or not isinstance(duration, int)):
            raise AppServerProtocolError("rate-limit window duration is invalid")
        return {
            "used_percent": float(used),
            "resets_at": resets_at,
            "window_minutes": duration,
            "source": "app_server_live",
        }

    async def run_ephemeral_structured_turn(
        self,
        *,
        model: str,
        effort: str,
        base_instructions: str,
        prompt: str,
        output_schema: dict[str, Any],
        cwd: str | Path,
        sidecar_path: str | Path,
        output_path: str | Path | None = None,
        batch_size: int = 1,
        thread_mode: str = "new_thread",
        timeout_seconds: float = 600.0,
    ) -> AppServerTurnResult:
        sidecar_file = Path(sidecar_path).expanduser().resolve()
        self._assert_new_sidecar(sidecar_file)
        thread = await self.start_thread(
            model=model,
            base_instructions=base_instructions,
            cwd=cwd,
            ephemeral=True,
        )
        return await self.run_structured_turn(
            thread=thread,
            effort=effort,
            prompt=prompt,
            output_schema=output_schema,
            sidecar_path=sidecar_file,
            output_path=output_path,
            batch_size=batch_size,
            thread_mode=thread_mode,
            timeout_seconds=timeout_seconds,
            sidecar_prechecked=True,
        )

    async def run_structured_turn(
        self,
        *,
        thread: AppServerThread,
        effort: str,
        prompt: str,
        output_schema: dict[str, Any],
        sidecar_path: str | Path,
        output_path: str | Path | None = None,
        batch_size: int = 1,
        thread_mode: str = "same_thread",
        timeout_seconds: float = 600.0,
        sidecar_prechecked: bool = False,
    ) -> AppServerTurnResult:
        self._require_started()
        if batch_size < 1:
            raise ValueError("batch size must be positive")
        sidecar_file = Path(sidecar_path).expanduser().resolve()
        if not sidecar_prechecked:
            self._assert_new_sidecar(sidecar_file)
        output_file = Path(output_path).expanduser().resolve() if output_path is not None else None
        initial = {
            "schema_version": TURN_SIDECAR_SCHEMA_VERSION,
            "state": "started",
            "started_at": now_iso(),
            "client_version": APP_SERVER_CLIENT_VERSION,
            "cli_version": self.cli_version,
            "app_server_user_agent": self.app_server_user_agent,
            "protocol_schema_sha256": self.protocol_schema_sha256,
            "transport": "stdio",
            "max_message_bytes": self.max_message_bytes,
            "synthetic_debug_errors": self.synthetic_debug_errors,
            "auth_type": (self.account_summary or {}).get("type"),
            "plan_type": (self.account_summary or {}).get("plan_type"),
            "thread_id": thread.thread_id,
            "turn_id": None,
            "model": thread.model,
            "effort": effort,
            "batch_size": batch_size,
            "thread_mode": thread_mode,
            "prompt_sha256": sha256_text(prompt),
            "prompt_bytes": len(prompt.encode("utf-8")),
            "base_instructions_sha256": thread.base_instructions_sha256,
            "base_instructions_bytes": thread.base_instructions_bytes,
            "instruction_sources_sha256": thread.instruction_sources_sha256,
            "instruction_sources_count": thread.instruction_sources_count,
            "output_schema_sha256": sha256_text(
                json.dumps(output_schema, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            ),
            "output_schema_bytes": len(
                json.dumps(
                    output_schema,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ),
            "output_path": str(output_file) if output_file is not None else None,
            "privacy": "telemetry_hashes_and_ids_no_prompt_transcript_response_email_or_credentials",
        }
        self._write_sidecar(sidecar_file, initial)
        wall_started = time.monotonic()
        lock = self._thread_locks.setdefault(thread.thread_id, asyncio.Lock())
        async with lock:
            turn_id = None
            state = None
            try:
                response = await self._request(
                    "turn/start",
                    {
                        "threadId": thread.thread_id,
                        "input": [{"type": "text", "text": prompt, "text_elements": []}],
                        "effort": effort,
                        "approvalPolicy": "never",
                        "sandboxPolicy": {"type": "readOnly", "networkAccess": False},
                        "environments": [],
                        "outputSchema": output_schema,
                    },
                )
                turn = response.get("turn") if isinstance(response, dict) else None
                turn_id = turn.get("id") if isinstance(turn, dict) else None
                if not isinstance(turn_id, str):
                    raise AppServerProtocolError("turn/start response is malformed")
                initial["turn_id"] = turn_id
                initial["state"] = "in_progress"
                self._write_sidecar(sidecar_file, initial)
                state = self._turn_state(thread.thread_id, turn_id)
                if self._fatal_error is not None:
                    raise self._fatal_error
                try:
                    terminal = await asyncio.wait_for(
                        asyncio.shield(state.completed),
                        timeout=timeout_seconds,
                    )
                except asyncio.TimeoutError as exc:
                    interrupt_error = None
                    try:
                        await asyncio.wait_for(
                            self._request(
                                "turn/interrupt",
                                {"threadId": thread.thread_id, "turnId": turn_id},
                            ),
                            timeout=self.interrupt_timeout_seconds,
                        )
                    except Exception as interrupt_exc:  # noqa: BLE001
                        interrupt_error = type(interrupt_exc).__name__
                    report = self._terminal_sidecar(
                        initial,
                        state="interrupted",
                        status="timeout",
                        error_class="turn_timeout",
                        wall_started=wall_started,
                        turn_state=state,
                        output_text=state.final_message(),
                    )
                    report["interrupt_error_class"] = interrupt_error
                    self._write_sidecar(sidecar_file, report)
                    raise AppServerTurnTimeout(f"turn timed out; recovery required: {sidecar_file}") from exc
                if state.last_usage is None:
                    try:
                        await asyncio.wait_for(
                            state.usage_arrived.wait(),
                            timeout=self.usage_grace_seconds,
                        )
                    except asyncio.TimeoutError:
                        pass
                return self._finish_turn(
                    sidecar_file=sidecar_file,
                    initial=initial,
                    terminal=terminal,
                    turn_state=state,
                    output_file=output_file,
                    wall_started=wall_started,
                )
            except (AppServerTurnTimeout, AppServerStructuredOutputError):
                raise
            except asyncio.CancelledError:
                interrupt_attempted = turn_id is not None
                interrupt_acknowledged = False
                interrupt_error_class = None
                terminal_notification_observed = bool(state is not None and state.completed.done())
                server_terminal_status = None
                if turn_id is not None:
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(
                                self._request(
                                    "turn/interrupt",
                                    {"threadId": thread.thread_id, "turnId": turn_id},
                                )
                            ),
                            timeout=self.interrupt_timeout_seconds,
                        )
                        interrupt_acknowledged = True
                        if state is not None and not state.completed.done():
                            try:
                                terminal_after_interrupt = await asyncio.wait_for(
                                    asyncio.shield(state.completed),
                                    timeout=self.interrupt_timeout_seconds,
                                )
                                terminal_notification_observed = True
                                terminal_turn = (
                                    terminal_after_interrupt.get("turn")
                                    if isinstance(terminal_after_interrupt, dict)
                                    else None
                                )
                                if isinstance(terminal_turn, dict):
                                    server_terminal_status = terminal_turn.get("status")
                            except (asyncio.TimeoutError, AppServerError):
                                pass
                    except Exception as interrupt_exc:  # noqa: BLE001
                        interrupt_error_class = type(interrupt_exc).__name__
                report = self._terminal_sidecar(
                    initial,
                    state="cancelled",
                    status="cancelled",
                    error_class="client_cancelled",
                    wall_started=wall_started,
                    turn_state=state,
                    output_text=state.final_message() if state is not None else None,
                )
                report.update(
                    {
                        "interrupt_attempted": interrupt_attempted,
                        "interrupt_acknowledged": interrupt_acknowledged,
                        "interrupt_error_class": interrupt_error_class,
                        "protocol_interrupt_sent": interrupt_attempted,
                        "terminal_notification_observed": terminal_notification_observed,
                        "server_terminal_status": server_terminal_status,
                        "usage_status": "measured" if report["usage_complete"] else "unknown",
                    }
                )
                self._write_sidecar(sidecar_file, report)
                raise
            except Exception as exc:
                report = self._terminal_sidecar(
                    initial,
                    state="failed",
                    status="client_error",
                    error_class=type(exc).__name__,
                    wall_started=wall_started,
                    turn_state=state,
                    output_text=state.final_message() if state is not None else None,
                )
                if turn_id is not None:
                    report["turn_id"] = turn_id
                if isinstance(exc, AppServerError):
                    report.update(
                        _text_metadata(
                            "error_detail",
                            str(exc),
                            include_text=self.synthetic_debug_errors,
                        )
                    )
                self._write_sidecar(sidecar_file, report)
                raise
            finally:
                if turn_id is not None:
                    self._turn_states.pop((thread.thread_id, turn_id), None)

    def _finish_turn(
        self,
        *,
        sidecar_file: Path,
        initial: dict[str, Any],
        terminal: dict[str, Any],
        turn_state: _TurnState,
        output_file: Path | None,
        wall_started: float,
    ) -> AppServerTurnResult:
        turn = terminal.get("turn") if isinstance(terminal, dict) else None
        status = turn.get("status") if isinstance(turn, dict) else None
        if not isinstance(status, str):
            raise AppServerProtocolError("turn/completed notification is malformed")
        output_text = turn_state.final_message()
        output_sha256 = sha256_text(output_text) if output_text is not None else None
        error_class = None
        output_value = None
        status_ok = status == "completed"
        if status_ok:
            if output_text is None:
                error_class = "agent_message_missing"
                status_ok = False
            elif turn_state.last_usage is None:
                error_class = "token_usage_missing"
                status_ok = False
            else:
                try:
                    output_value = json.loads(output_text)
                except json.JSONDecodeError:
                    error_class = "structured_output_invalid"
                    status_ok = False
        else:
            error_class = f"turn_{status}"
        if status_ok and output_file is not None:
            write_text_atomic(output_file, output_text + ("" if output_text.endswith("\n") else "\n"))
        sidecar_state = "completed" if status_ok else ("interrupted" if status == "interrupted" else "failed")
        report = self._terminal_sidecar(
            initial,
            state=sidecar_state,
            status=status,
            error_class=error_class,
            wall_started=wall_started,
            turn_state=turn_state,
            output_text=output_text,
        )
        turn_error = turn.get("error") if isinstance(turn, dict) else None
        if isinstance(turn_error, dict):
            codex_error_info = turn_error.get("codexErrorInfo")
            report["turn_error"] = {
                "codex_error_info": codex_error_info,
                **_text_metadata(
                    "message",
                    turn_error.get("message"),
                    include_text=self.synthetic_debug_errors,
                ),
                **_text_metadata(
                    "additional_details",
                    turn_error.get("additionalDetails"),
                    include_text=self.synthetic_debug_errors,
                ),
            }
            # Capacity messages are short provider diagnostics, not model
            # output. Preserve them verbatim so model-pool saturation can be
            # distinguished from quota, transport, and local-process errors.
            backend_message = turn_error.get("message")
            if (
                codex_error_info in {
                    "serverOverloaded", "modelCapacityExceeded", "capacityExceeded"
                }
                and isinstance(backend_message, str)
                and 0 < len(backend_message) <= 500
            ):
                report["turn_error"]["backend_message"] = backend_message
        self._write_sidecar(sidecar_file, report)
        wall_elapsed = float(report["wall_elapsed_seconds"])
        if status == "completed" and error_class == "structured_output_invalid":
            raise AppServerStructuredOutputError(
                f"turn completed without valid structured output; recovery required: {sidecar_file}"
            )
        return AppServerTurnResult(
            thread_id=str(report["thread_id"]),
            turn_id=str(report["turn_id"]),
            status=status,
            status_ok=status_ok,
            output=output_value,
            output_text=output_text,
            output_sha256=output_sha256,
            usage=turn_state.last_usage,
            thread_total_usage=turn_state.total_usage,
            wall_elapsed_seconds=wall_elapsed,
            error_class=error_class,
            sidecar_path=str(sidecar_file),
        )

    def _terminal_sidecar(
        self,
        initial: dict[str, Any],
        *,
        state: str,
        status: str,
        error_class: str | None,
        wall_started: float,
        turn_state: _TurnState | None,
        output_text: str | None,
    ) -> dict[str, Any]:
        usage = turn_state.last_usage if turn_state is not None else None
        total_usage = turn_state.total_usage if turn_state is not None else None
        return {
            **initial,
            "state": state,
            "finished_at": now_iso(),
            "status": status,
            "error_class": error_class,
            "wall_elapsed_seconds": round(time.monotonic() - wall_started, 3),
            "usage": asdict(usage) if usage is not None else None,
            "thread_total_usage": asdict(total_usage) if total_usage is not None else None,
            "usage_complete": usage is not None,
            "usage_status": "measured" if usage is not None else "unknown",
            "output_sha256": sha256_text(output_text) if output_text is not None else None,
            "stderr_sha256": self._stderr_hash.hexdigest(),
            "stderr_bytes": self._stderr_bytes,
            "recovery_reran_model": False,
        }

    async def _read_models(self) -> list[dict[str, Any]]:
        models = []
        cursor = None
        while True:
            result = await self._request(
                "model/list",
                {"cursor": cursor, "limit": 100, "includeHidden": True},
            )
            if not isinstance(result, dict) or not isinstance(result.get("data"), list):
                raise AppServerProtocolError("model/list response is malformed")
            models.extend(item for item in result["data"] if isinstance(item, dict))
            cursor = result.get("nextCursor")
            if not cursor:
                return models

    def _model_available(self, model: str) -> bool:
        return any(item.get("id") == model or item.get("model") == model for item in self.models)

    async def _request(self, method: str, params: dict[str, Any] | None) -> Any:
        self._require_started(
            allow_initializing=method in {"initialize", "account/read", "model/list"}
        )
        if self._fatal_error is not None:
            raise self._fatal_error
        self._request_id += 1
        request_id = self._request_id
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending[request_id] = (method, future)
        await self._send({"id": request_id, "method": method, "params": params})
        try:
            return await asyncio.wait_for(
                asyncio.shield(future),
                timeout=self.request_timeout_seconds,
            )
        except asyncio.TimeoutError as exc:
            self._pending.pop(request_id, None)
            raise AppServerProtocolError(f"app-server request timed out: {method}") from exc

    async def _notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        message = {"method": method}
        if params is not None:
            message["params"] = params
        await self._send(message)

    async def _send(self, message: dict[str, Any]) -> None:
        if self.process is None or self.process.stdin is None:
            raise AppServerProcessDied("app-server stdin is unavailable")
        data = (json.dumps(message, ensure_ascii=True, separators=(",", ":")) + "\n").encode()
        async with self._write_lock:
            try:
                self.process.stdin.write(data)
                await self.process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError) as exc:
                error = AppServerProcessDied("app-server stdin closed")
                self._set_fatal(error)
                raise error from exc

    async def _reader_loop(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        message_context = "waiting_for_message"
        try:
            while True:
                line = await self.process.stdout.readline()
                if not line:
                    return
                try:
                    message = json.loads(line)
                except json.JSONDecodeError as exc:
                    self._set_fatal(AppServerProtocolError("app-server emitted malformed JSON"))
                    return
                if not isinstance(message, dict):
                    self._set_fatal(AppServerProtocolError("app-server message is not an object"))
                    return
                if "id" in message and ("result" in message or "error" in message):
                    message_context = f"response:{message.get('id')}"
                    self._handle_response(message)
                elif isinstance(message.get("method"), str) and "id" not in message:
                    message_context = f"notification:{message['method']}"
                    self._handle_notification(message)
                elif isinstance(message.get("method"), str) and "id" in message:
                    message_context = f"server_request:{message['method']}"
                    await self._handle_server_request(message)
                else:
                    self._set_fatal(AppServerProtocolError("app-server emitted an unknown message shape"))
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            detail = str(exc).strip().replace("\n", " ")[:300]
            suffix = f": {detail}" if detail else ""
            self._set_fatal(
                AppServerProtocolError(
                    f"app-server reader failed while handling {message_context}: {type(exc).__name__}{suffix}"
                )
            )

    def _handle_response(self, message: dict[str, Any]) -> None:
        request_id = message.get("id")
        pending = self._pending.pop(request_id, None)
        if pending is None:
            self._set_fatal(AppServerProtocolError("app-server returned an unknown request id"))
            return
        method, future = pending
        if "error" in message:
            error = message.get("error") or {}
            future.set_exception(AppServerRPCError(method, error.get("code")))
        else:
            future.set_result(message.get("result"))

    def _handle_notification(self, message: dict[str, Any]) -> None:
        method = message["method"]
        params = message.get("params") or {}
        if method not in {
            "item/completed",
            "turn/completed",
            "thread/tokenUsage/updated",
        }:
            return
        thread_id = params.get("threadId")
        if method == "turn/completed":
            turn = params.get("turn")
            turn_id = turn.get("id") if isinstance(turn, dict) else None
        else:
            turn_id = params.get("turnId")
        if not isinstance(thread_id, str) or not isinstance(turn_id, str):
            self._set_fatal(AppServerProtocolError(f"{method} omitted threadId or turnId"))
            return
        state = self._turn_state(thread_id, turn_id)
        if method == "item/completed":
            item = params.get("item")
            if isinstance(item, dict) and item.get("type") == "agentMessage":
                state.agent_messages.append(item)
            return
        if method == "thread/tokenUsage/updated":
            token_usage = params.get("tokenUsage") or {}
            try:
                state.last_usage = TokenUsage.from_protocol(token_usage.get("last"))
                state.total_usage = TokenUsage.from_protocol(token_usage.get("total"))
            except AppServerProtocolError as exc:
                if not state.completed.done():
                    state.completed.set_exception(exc)
                return
            state.usage_arrived.set()
            return
        if not state.completed.done():
            state.completed.set_result(params)

    async def _handle_server_request(self, message: dict[str, Any]) -> None:
        request_id = message.get("id")
        await self._send(
            {
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": "Client tools and approvals are disabled for this worker.",
                },
            }
        )
        self._set_fatal(AppServerProtocolError("app-server requested a disabled client action"))

    async def _wait_loop(self) -> None:
        assert self.process is not None
        return_code = await self.process.wait()
        if not self._closing:
            self._set_fatal(AppServerProcessDied(f"app-server exited unexpectedly with code {return_code}"))

    async def _stderr_loop(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        while True:
            chunk = await self.process.stderr.read(8192)
            if not chunk:
                return
            self._stderr_hash.update(chunk)
            self._stderr_bytes += len(chunk)

    def _turn_state(self, thread_id: str, turn_id: str) -> _TurnState:
        key = (thread_id, turn_id)
        state = self._turn_states.get(key)
        if state is None:
            state = _TurnState(asyncio.get_running_loop())
            self._turn_states[key] = state
        return state

    def _set_fatal(self, error: AppServerError) -> None:
        if self._fatal_error is not None:
            return
        self._fatal_error = error
        for _method, future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        for state in self._turn_states.values():
            if not state.completed.done():
                state.completed.set_exception(error)
        if self.process is not None and self.process.returncode is None and not self._closing:
            self.process.terminate()

    def _require_started(self, *, allow_initializing: bool = False) -> None:
        if self.process is None:
            raise AppServerProcessDied("app-server client is not started")
        if self._fatal_error is not None:
            raise self._fatal_error
        if not allow_initializing and self.account_summary is None:
            raise AppServerProtocolError("app-server client is not initialized")

    @staticmethod
    def _validate_thread_id(thread_id: str) -> str:
        if not isinstance(thread_id, str) or not thread_id or thread_id != thread_id.strip():
            raise ValueError("thread id must be a non-empty string without surrounding whitespace")
        return thread_id

    @staticmethod
    def _assert_new_sidecar(path: Path) -> None:
        if path.exists():
            try:
                state = json.loads(path.read_text(encoding="utf-8")).get("state")
            except (OSError, json.JSONDecodeError):
                state = "unreadable"
            raise AppServerRecoveryRequired(
                f"turn sidecar already exists in state {state}; explicit recovery is required: {path}"
            )

    @staticmethod
    def _write_sidecar(path: Path, payload: dict[str, Any]) -> None:
        write_text_atomic(
            path,
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        )
