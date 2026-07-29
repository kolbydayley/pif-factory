from __future__ import annotations

"""Reserve-aware capacity gating for bounded managed app-server phases."""

import asyncio
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional

from .app_server_capacity import AppServerCapacityError, parse_rate_limit_snapshot
from .codex_app_server import CodexAppServerClient
from .util import now_iso, write_text_atomic


RESERVE_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
RESERVE_CAPACITY_GATE_VERSION = "pif_app_server_reserve_capacity_gate_v1"
RESERVE_CAPACITY_CHECKPOINT_VERSION = (
    "pif_app_server_reserve_capacity_checkpoint_v1"
)
MINIMUM_REMAINING_RESERVE_PERCENT = 20


class ReserveCapacityError(AppServerCapacityError):
    """A bounded phase cannot safely start or continue."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_embedded_audit_record(record: Any) -> None:
    if not isinstance(record, Mapping):
        raise ReserveCapacityError("reserve capacity policy has no audit record")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    expected_sha = record.get("sha256")
    expected_size = record.get("size_bytes")
    if (
        not path.is_file()
        or not isinstance(expected_sha, str)
        or len(expected_sha) != 64
        or isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or expected_size < 0
        or _sha256_file(path) != expected_sha
        or path.stat().st_size != expected_size
    ):
        raise ReserveCapacityError("reserve capacity audit record drifted")
    try:
        audit = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReserveCapacityError("reserve capacity audit is invalid") from exc
    if (
        not isinstance(audit, dict)
        or audit.get("schema_version")
        != "pif_app_server_capacity_policy_audit_v20"
        or audit.get("production_mutation_performed") is not False
    ):
        raise ReserveCapacityError("reserve capacity audit contract drifted")


def load_reserve_capacity_policy(path: Path) -> Dict[str, Any]:
    target = path.expanduser().resolve()
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReserveCapacityError("reserve capacity policy is missing or invalid") from exc
    if not isinstance(value, dict):
        raise ReserveCapacityError("reserve capacity policy is not an object")
    turn_names = value.get("ordered_turn_names")
    integers = {
        "minimum_remaining_reserve_percent": value.get(
            "minimum_remaining_reserve_percent"
        ),
        "quota_points_per_million_tokens": value.get(
            "quota_points_per_million_tokens"
        ),
        "maximum_total_tokens_per_turn": value.get(
            "maximum_total_tokens_per_turn"
        ),
        "phase_total_token_bound": value.get("phase_total_token_bound"),
        "projected_phase_quota_points": value.get(
            "projected_phase_quota_points"
        ),
    }
    if (
        value.get("schema_version") != RESERVE_CAPACITY_POLICY_VERSION
        or value.get("managed_chatgpt_auth_only") is not True
        or value.get("official_persistent_codex_app_server_only") is not True
        or value.get("retry_count_per_turn") != 0
        or value.get("production_mutation_allowed") is not False
        or value.get("rate_limit_reached_type_must_be_null") is not True
        or value.get("unknown_usage_hard_stop") is not True
        or not isinstance(turn_names, list)
        or not turn_names
        or len(turn_names) != len(set(turn_names))
        or any(not isinstance(item, str) or not item for item in turn_names)
        or any(
            isinstance(item, bool) or not isinstance(item, int) or item <= 0
            for item in integers.values()
        )
        or integers["minimum_remaining_reserve_percent"]
        < MINIMUM_REMAINING_RESERVE_PERCENT
        or integers["phase_total_token_bound"]
        != len(turn_names) * integers["maximum_total_tokens_per_turn"]
        or integers["projected_phase_quota_points"]
        != math.ceil(
            integers["phase_total_token_bound"]
            * integers["quota_points_per_million_tokens"]
            / 1_000_000
        )
    ):
        raise ReserveCapacityError("reserve capacity policy contract drifted")
    output_root = Path(str(value.get("semantic_output_root") or "")).expanduser()
    if not output_root.is_absolute():
        raise ReserveCapacityError("reserve capacity output root is not absolute")
    _verify_embedded_audit_record(value.get("audit"))
    return value


def evaluate_reserve_capacity(
    snapshot: Mapping[str, Any],
    *,
    policy: Mapping[str, Any],
    remaining_turn_count: int,
) -> Dict[str, Any]:
    """Project a bounded phase against remaining quota, not used quota."""

    if remaining_turn_count < 1 or remaining_turn_count > len(
        policy["ordered_turn_names"]
    ):
        raise ReserveCapacityError("remaining turn count is outside the frozen phase")
    used = snapshot.get("primary_used_percent")
    reached = snapshot.get("rate_limit_reached_type")
    if isinstance(used, bool) or not isinstance(used, int) or not 0 <= used <= 100:
        raise ReserveCapacityError("live primary used percentage is malformed")
    if reached is not None and not isinstance(reached, str):
        raise ReserveCapacityError("live reached type is malformed")
    reserve = int(policy["minimum_remaining_reserve_percent"])
    maximum_turn_tokens = int(policy["maximum_total_tokens_per_turn"])
    quota_rate = int(policy["quota_points_per_million_tokens"])
    projected_tokens = remaining_turn_count * maximum_turn_tokens
    projected_points = math.ceil(projected_tokens * quota_rate / 1_000_000)
    remaining_percent = 100 - used
    usable_above_reserve = max(0, remaining_percent - reserve)
    cleared = bool(reached is None and projected_points <= usable_above_reserve)
    return {
        "schema_version": RESERVE_CAPACITY_GATE_VERSION,
        "primary_used_percent": used,
        "primary_remaining_percent": remaining_percent,
        "minimum_remaining_reserve_percent": reserve,
        "usable_percent_above_reserve": usable_above_reserve,
        "remaining_turn_count": remaining_turn_count,
        "maximum_total_tokens_per_turn": maximum_turn_tokens,
        "projected_remaining_tokens": projected_tokens,
        "quota_points_per_million_tokens": quota_rate,
        "projected_remaining_quota_points": projected_points,
        "projected_terminal_remaining_percent": remaining_percent
        - projected_points,
        "rate_limit_reached_type": reached,
        "cleared_for_semantic_turn": cleared,
        "thread_started": False,
        "turn_started": False,
        "sidecar_started": False,
    }


class ReserveCapacityGatedCodexAppServerClient:
    """Persistent client enforcing one immutable, reserve-bounded phase."""

    def __init__(
        self,
        *,
        policy_path: Path,
        inner_factory: Callable[[], Any] = CodexAppServerClient,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        del sleep  # The v20 policy stops rather than polling inside a semantic phase.
        self.policy_path = policy_path.expanduser().resolve()
        self.policy = load_reserve_capacity_policy(self.policy_path)
        self.policy_sha256 = _sha256_file(self.policy_path)
        self.inner_factory = inner_factory
        self.inner_context: Optional[Any] = None
        self.inner: Optional[Any] = None
        self._semantic_turn_lock = asyncio.Lock()
        self._sticky_failure: Optional[str] = None
        self.capacity_checks: list[Dict[str, Any]] = []

    async def __aenter__(self) -> "ReserveCapacityGatedCodexAppServerClient":
        if self.inner is not None:
            raise ReserveCapacityError("reserve capacity client cannot be entered twice")
        self.inner_context = self.inner_factory()
        self.inner = await self.inner_context.__aenter__()
        account = getattr(self.inner, "account_summary", None)
        if (
            not isinstance(account, dict)
            or account.get("type") != "chatgpt"
            or account.get("plan_type") != "pro"
        ):
            await self.inner_context.__aexit__(None, None, None)
            self.inner = None
            self.inner_context = None
            raise ReserveCapacityError(
                "reserve capacity gate requires managed ChatGPT Pro auth"
            )
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        context = self.inner_context
        self.inner = None
        self.inner_context = None
        if context is not None:
            await context.__aexit__(exc_type, exc, traceback)

    def __getattr__(self, name: str) -> Any:
        inner = self.__dict__.get("inner")
        if inner is None:
            raise AttributeError(name)
        return getattr(inner, name)

    def _turn_index(self, checkpoint_path: Any) -> tuple[int, Path]:
        if checkpoint_path is None:
            raise ReserveCapacityError("reserve policy requires a capacity checkpoint")
        checkpoint = Path(checkpoint_path).expanduser().resolve()
        output_root = Path(self.policy["semantic_output_root"]).expanduser().resolve()
        try:
            relative = checkpoint.relative_to(output_root)
        except ValueError as exc:
            raise ReserveCapacityError(
                "capacity checkpoint escapes the frozen semantic root"
            ) from exc
        if len(relative.parts) != 3 or relative.parts[0] != "turns" or relative.name != "capacity.json":
            raise ReserveCapacityError("capacity checkpoint layout drifted")
        directory_to_index = {
            name.replace("_", "-"): index
            for index, name in enumerate(self.policy["ordered_turn_names"])
        }
        directory = relative.parts[1]
        if directory not in directory_to_index:
            raise ReserveCapacityError("semantic turn is outside the frozen phase")
        return directory_to_index[directory], checkpoint

    def _verify_prior_turns(self, turn_index: int) -> None:
        root = Path(self.policy["semantic_output_root"]).expanduser().resolve()
        maximum = int(self.policy["maximum_total_tokens_per_turn"])
        for name in self.policy["ordered_turn_names"][:turn_index]:
            turn_root = root / "turns" / name.replace("_", "-")
            sidecar_path = turn_root / "sidecar.json"
            capacity_path = turn_root / "capacity.json"
            output_path = turn_root / "output.private.json"
            try:
                sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
                capacity = json.loads(capacity_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ReserveCapacityError(
                    "a prior frozen turn is not terminal and measured"
                ) from exc
            usage = sidecar.get("usage") if isinstance(sidecar, dict) else None
            total = usage.get("total_tokens") if isinstance(usage, dict) else None
            if (
                sidecar.get("state") != "completed"
                or sidecar.get("status") != "completed"
                or sidecar.get("usage_complete") is not True
                or sidecar.get("usage_status") != "measured"
                or isinstance(total, bool)
                or not isinstance(total, int)
                or total < 0
                or total > maximum
                or capacity.get("schema_version")
                != RESERVE_CAPACITY_CHECKPOINT_VERSION
                or capacity.get("policy_sha256") != self.policy_sha256
                or not output_path.is_file()
            ):
                raise ReserveCapacityError("a prior frozen turn contract drifted")

    async def _probe(self, *, remaining_turn_count: int) -> Dict[str, Any]:
        if self._sticky_failure is not None:
            raise ReserveCapacityError(
                "semantic phase previously stopped: %s" % self._sticky_failure
            )
        if self.inner is None:
            raise ReserveCapacityError("reserve capacity client is not started")
        account = getattr(self.inner, "account_summary", None)
        if (
            not isinstance(account, dict)
            or account.get("type") != "chatgpt"
            or account.get("plan_type") != "pro"
        ):
            self._sticky_failure = "managed_auth_drift"
            raise ReserveCapacityError("managed ChatGPT auth drifted")
        try:
            response = await self.inner._request(  # noqa: SLF001 - official no-turn method
                "account/rateLimits/read", {}
            )
            snapshot = parse_rate_limit_snapshot(
                response, maximum_primary_used_percent=100
            )
            evaluation = evaluate_reserve_capacity(
                snapshot,
                policy=self.policy,
                remaining_turn_count=remaining_turn_count,
            )
        except BaseException:
            self._sticky_failure = "capacity_probe_exception_or_cancellation"
            raise
        record = {
            **evaluation,
            "managed_chatgpt_auth_verified": True,
            "plan_type": account.get("plan_type"),
            "primary_resets_at": snapshot.get("primary_resets_at"),
        }
        self.capacity_checks.append(record)
        if not evaluation["cleared_for_semantic_turn"]:
            self._sticky_failure = (
                "provider_rate_limit_reached"
                if evaluation["rate_limit_reached_type"] is not None
                else "minimum_reserve_or_projected_phase_bound_lost"
            )
            raise ReserveCapacityError(self._sticky_failure)
        return record

    async def wait_for_semantic_capacity(self) -> Dict[str, Any]:
        """Perform one no-thread/no-turn probe for the entire remaining phase."""

        return await self._probe(
            remaining_turn_count=len(self.policy["ordered_turn_names"])
        )

    def _write_checkpoint(
        self,
        *,
        path: Path,
        snapshot: Mapping[str, Any],
        turn_index: int,
    ) -> None:
        if path.exists():
            raise ReserveCapacityError(
                "capacity checkpoint exists; semantic retry is prohibited"
            )
        payload = {
            "schema_version": RESERVE_CAPACITY_CHECKPOINT_VERSION,
            "checked_at": now_iso(),
            "policy_path": str(self.policy_path),
            "policy_sha256": self.policy_sha256,
            "phase_id": self.policy["phase_id"],
            "turn_name": self.policy["ordered_turn_names"][turn_index],
            "turn_ordinal": turn_index,
            "managed_chatgpt_auth_verified": True,
            "plan_type": snapshot.get("plan_type"),
            "primary_used_percent": snapshot["primary_used_percent"],
            "primary_remaining_percent": snapshot["primary_remaining_percent"],
            "primary_resets_at": snapshot.get("primary_resets_at"),
            "rate_limit_reached_type": snapshot["rate_limit_reached_type"],
            "minimum_remaining_reserve_percent": snapshot[
                "minimum_remaining_reserve_percent"
            ],
            "usable_percent_above_reserve": snapshot[
                "usable_percent_above_reserve"
            ],
            "remaining_turn_count": snapshot["remaining_turn_count"],
            "projected_remaining_tokens": snapshot[
                "projected_remaining_tokens"
            ],
            "quota_points_per_million_tokens": snapshot[
                "quota_points_per_million_tokens"
            ],
            "projected_remaining_quota_points": snapshot[
                "projected_remaining_quota_points"
            ],
            "projected_terminal_remaining_percent": snapshot[
                "projected_terminal_remaining_percent"
            ],
            "cleared_for_semantic_turn": True,
            "thread_started": False,
            "turn_started": False,
            "sidecar_started": False,
            "retry_checkpoint_reuse_allowed": False,
            "privacy": "capacity_status_policy_hash_and_counts_no_prompt_output_email_credentials_or_thread_ids",
        }
        write_text_atomic(
            path,
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        )

    def _validate_result_usage(self, result: Any) -> None:
        usage = getattr(result, "usage", None)
        total = getattr(usage, "total_tokens", None)
        if (
            usage is None
            or isinstance(total, bool)
            or not isinstance(total, int)
            or total < 0
        ):
            self._sticky_failure = "unknown_usage"
            raise ReserveCapacityError("semantic turn returned unknown usage")
        if total > int(self.policy["maximum_total_tokens_per_turn"]):
            self._sticky_failure = "per_turn_token_bound_exceeded"
            raise ReserveCapacityError("semantic turn exceeded its frozen token bound")
        if getattr(result, "status_ok", False) is not True:
            self._sticky_failure = "semantic_turn_failed"
            raise ReserveCapacityError("semantic turn did not complete successfully")

    async def _run(self, method: str, *args: Any, **kwargs: Any) -> Any:
        async with self._semantic_turn_lock:
            checkpoint_value = kwargs.pop("capacity_checkpoint_path", None)
            turn_index, checkpoint = self._turn_index(checkpoint_value)
            if self._sticky_failure is not None:
                raise ReserveCapacityError(
                    "semantic phase previously stopped: %s" % self._sticky_failure
                )
            if checkpoint.exists():
                self._sticky_failure = "existing_checkpoint_retry_prohibited"
                raise ReserveCapacityError(
                    "capacity checkpoint exists; semantic retry is prohibited"
                )
            try:
                self._verify_prior_turns(turn_index)
            except BaseException:
                self._sticky_failure = "prior_turn_sequence_invalid"
                raise
            remaining_turn_count = len(self.policy["ordered_turn_names"]) - turn_index
            snapshot = await self._probe(remaining_turn_count=remaining_turn_count)
            self._write_checkpoint(
                path=checkpoint, snapshot=snapshot, turn_index=turn_index
            )
            if self.inner is None:  # pragma: no cover - guarded above
                raise ReserveCapacityError("reserve capacity client stopped before turn")
            try:
                result = await getattr(self.inner, method)(*args, **kwargs)
            except BaseException:
                self._sticky_failure = "semantic_turn_exception_or_cancellation"
                raise
            self._validate_result_usage(result)
            return result

    async def run_ephemeral_structured_turn(self, *args: Any, **kwargs: Any) -> Any:
        return await self._run("run_ephemeral_structured_turn", *args, **kwargs)

    async def run_structured_turn(self, *args: Any, **kwargs: Any) -> Any:
        return await self._run("run_structured_turn", *args, **kwargs)
