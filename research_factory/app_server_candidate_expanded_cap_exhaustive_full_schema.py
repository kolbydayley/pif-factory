from __future__ import annotations

"""Run the checksum-bound epoch-4 expanded-cap full-schema canary.

This module is additive. It reuses the frozen v249 source, semantics, and
explicit-applicability projection, raises only the structural event ceiling,
and adds one topic-general exhaustive enumeration instruction. A no-model
thread preflight freezes the effective Codex instruction sources before the
runtime lock is issued.
"""

import argparse
import asyncio
import copy
import fcntl
import hashlib
import json
import math
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterator, Mapping, Sequence

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.9 compatibility
    import tomli as tomllib  # type: ignore[no-redef]

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5_selection_v232_window_ledger as v232
from . import app_server_judge_v5_selection_v233_blind_unit_sweep as v233
from . import app_server_judge_v5_selection_v239_frontier_long_horizon as v239
from . import app_server_judge_v5_selection_v248_source_span_enrichment as v248
from . import app_server_judge_v5_selection_v249_explicit_applicability as v249
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_candidate_expanded_cap_exhaustive_full_schema_v4"
LOCK_VERSION = "pif_candidate_expanded_cap_exhaustive_full_schema_runtime_lock_v4"
PREFLIGHT_VERSION = "pif_candidate_expanded_cap_no_model_preflight_v1"
LAUNCH_VERSION = "pif_candidate_expanded_cap_launch_receipt_v2"
CAPACITY_VERSION = "pif_expanded_cap_capacity_checkpoint_v2"
INSTRUCTION_VERIFICATION_VERSION = (
    "pif_expanded_cap_semantic_instruction_verification_v2"
)
SIDECAR_LINEAGE_VERSION = "pif_expanded_cap_turn_lineage_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
DIRECTIVE_VERSION = "pif_evaluation_expanded_cap_exhaustive_full_schema_directive_v4"
PLAN_VERSION = "pif_evaluation_semantic_plan_v1"
THREAD_ID = "019f4cf1-c46e-7db3-acd2-bf03c4459a10"
PLAN_EPOCH = 4
STEP_ID = "expanded_cap_exhaustive_full_schema_canary_v4"
MODEL = "gpt-5.6-sol"
EFFORT = "high"
TURN_NAME = "expanded-cap-exhaustive-full-schema"
MAX_EVENTS_PER_SEGMENT = 48
PRIOR_MAX_EVENTS_PER_SEGMENT = 32
MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING = 72_891
PREFLIGHT_CAPACITY_TOKEN_ENVELOPE = 72_891
TIMEOUT_SECONDS = 1200.0
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
BASELINE_END_TO_END_TOKENS = 10_065_426
PRODUCTION_AMORTIZED_CONTEXT_TOKENS = 600_538
PRODUCTION_SCALE = 30
PRODUCTION_RATIO_MAX = 0.28
USAGE_FIELDS = v233.USAGE_FIELDS

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = (
    PROJECT_ROOT / "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
PLAN_PATH = (PROJECT_ROOT / "automation/pif-evaluation-semantic-plan-v4.json").resolve()
DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation/pif-evaluation-expanded-cap-exhaustive-full-schema-v4.json"
).resolve()
EXPECTED_PLAN_SHA256 = "98cbe3eee5a69986d683a4d69dda4e89374b7ce8595e1698e8fcdfe18b90c7b0"
EXPECTED_DIRECTIVE_SHA256 = "96ee0d6e18268cc7a7773b5a072bc9d7100d2dbe05e40a01060ff5569425b2fd"
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-selection-v249-expanded-cap-exhaustive-full-schema-v4"
).resolve()
V249_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v249-explicit-applicability"
).resolve()
V249_TURN_ROOT = (
    V249_ROOT
    / "turns/v249-explicit-applicability-d7c914bc1cee2c432b36"
).resolve()
PINNED_CODEX = v249.PINNED_CODEX_0_144_1.resolve()
CODEX_CONFIG_PATH = (Path.home() / ".codex/config.toml").resolve()
TURN_START_SCHEMA_PATH = (
    PROJECT_ROOT
    / "research_factory/protocol/codex_app_server_0_144_1/v2/TurnStartParams.json"
).resolve()
PROCESS_LOCK_NAME = ".expanded-cap-exhaustive-full-schema.lock"

EXHAUSTIVE_INSTRUCTIONS = """Review every source unit before finalizing the segment.

Enumerate every independent grounded eligible proposition. Do not collapse adjacent recommendations, practical counterclaims, consequences, dependency effects, security effects, or capacity tradeoffs merely because they occur in the same exchange or share evidence. Keep genuinely distinct propositions as distinct events and keep one proposition per event. Do not aim for a target count and do not infer claims beyond the source units. Preserve source order and select the smallest complete exact evidence-unit range that supports every material field of each event."""
OLD_CAP_INSTRUCTION = "enforce the per-segment 32-event cap"
NEW_CAP_INSTRUCTION = "enforce the per-segment 48-event safety cap"

_RECORD_CACHE: dict[tuple[str, int, int, int, int], dict[str, Any]] = {}


class ExpandedCapCanaryError(RuntimeError):
    """The immutable epoch-4 contract or artifact integrity failed."""


class OperationalWaitingError(ExpandedCapCanaryError):
    """A genuine app-server, auth, capacity, transport, or I/O condition waits."""


class StructuralRejectionError(ExpandedCapCanaryError):
    """A completed output or measured cost failed the structural contract."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExpandedCapCanaryError(f"cannot read {label}") from exc


def _sha256_file(path: Path) -> str:
    resolved = path.expanduser().resolve(strict=True)
    stat = resolved.stat()
    key = (
        str(resolved),
        int(stat.st_dev),
        int(stat.st_ino),
        int(stat.st_size),
        int(stat.st_mtime_ns),
    )
    cached = _RECORD_CACHE.get(key)
    if cached is not None:
        return str(cached["sha256"])
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    value = digest.hexdigest()
    _RECORD_CACHE[key] = {
        "path": str(resolved),
        "sha256": value,
        "size_bytes": stat.st_size,
    }
    return value


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    stat = resolved.stat()
    key = (
        str(resolved),
        int(stat.st_dev),
        int(stat.st_ino),
        int(stat.st_size),
        int(stat.st_mtime_ns),
    )
    cached = _RECORD_CACHE.get(key)
    if cached is None:
        _sha256_file(resolved)
        cached = _RECORD_CACHE[key]
    return dict(cached)


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        actual = _record(Path(str(record["path"])))
        expected = {key: record.get(key) for key in actual}
        return actual == expected and set(record) - set(actual) <= {"role"}
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _record_matches(record: Any, path: Path) -> bool:
    return isinstance(record, Mapping) and dict(record) == _record(path)


def _is_iso_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value or value != value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise ExpandedCapCanaryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_immutable_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise ExpandedCapCanaryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_immutable_bytes(path: Path, value: bytes) -> None:
    if path.exists():
        if path.read_bytes() != value:
            raise ExpandedCapCanaryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        prior = _load_json(path, f"existing {path.name}")
        value[key] = prior.get(key)
    _write_immutable_json(path, value)


@contextmanager
def _advisory_process_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    path = root / PROCESS_LOCK_NAME
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OperationalWaitingError("another epoch-4 process owns the lock") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _configured_mcp_server_names(path: Path = CODEX_CONFIG_PATH) -> tuple[str, ...]:
    try:
        config = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ExpandedCapCanaryError("cannot read Codex MCP server names") from exc
    servers = config.get("mcp_servers", {})
    if not isinstance(servers, Mapping):
        raise ExpandedCapCanaryError("Codex MCP server table is malformed")
    names = tuple(sorted(str(name) for name in servers))
    if any(not name or any(character.isspace() for character in name) for name in names):
        raise ExpandedCapCanaryError("Codex MCP server name is malformed")
    return names


def _config_overlay(directive: Mapping[str, Any]) -> dict[str, Any]:
    execution = directive.get("execution_contract")
    if not isinstance(execution, Mapping):
        raise ExpandedCapCanaryError("epoch-4 execution contract is missing")
    overlay = copy.deepcopy(execution.get("context_control_overlay"))
    if not isinstance(overlay, dict):
        raise ExpandedCapCanaryError("epoch-4 context overlay is missing")
    configured_names = _configured_mcp_server_names()
    disabled = overlay.get("mcp_servers")
    if not isinstance(disabled, Mapping) or tuple(sorted(disabled)) != configured_names:
        raise ExpandedCapCanaryError("context overlay MCP server names drifted")
    if any(value != {"enabled": False} for value in disabled.values()):
        raise ExpandedCapCanaryError("context overlay copied MCP configuration details")
    return overlay


def _summary_none_supported() -> bool:
    schema = _load_json(TURN_START_SCHEMA_PATH, "pinned TurnStartParams schema")
    summary = (schema.get("definitions") or {}).get("ReasoningSummary")
    choices = summary.get("oneOf") if isinstance(summary, Mapping) else None
    return bool(
        isinstance(choices, list)
        and any(
            isinstance(choice, Mapping) and "none" in (choice.get("enum") or [])
            for choice in choices
        )
    )


def load_contract() -> dict[str, Any]:
    if _sha256_file(PLAN_PATH) != EXPECTED_PLAN_SHA256:
        raise ExpandedCapCanaryError("epoch-4 semantic plan checksum drifted")
    if _sha256_file(DIRECTIVE_PATH) != EXPECTED_DIRECTIVE_SHA256:
        raise ExpandedCapCanaryError("epoch-4 directive checksum drifted")
    plan = _load_json(PLAN_PATH, "epoch-4 semantic plan")
    directive = _load_json(DIRECTIVE_PATH, "epoch-4 directive")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    execution = directive.get("execution_contract")
    architecture = directive.get("architecture_contract")
    acceptance = directive.get("structural_acceptance_contract")
    terminal = directive.get("terminal_contract")
    cost = execution.get("production_cost_projection") if isinstance(execution, Mapping) else None
    capacity_contract = (
        execution.get("fresh_pre_turn_capacity_probe")
        if isinstance(execution, Mapping)
        else None
    )
    if (
        not isinstance(step, Mapping)
        or set(plan) != {"schema_version", "thread_id", "plan_epoch", "state", "step"}
        or set(step)
        != {
            "step_id",
            "state",
            "max_model_calls",
            "max_total_tokens",
            "expected_receipt_path",
            "accepted_receipt_states",
            "directive_path",
            "directive_sha256",
        }
        or plan.get("schema_version") != PLAN_VERSION
        or plan.get("thread_id") != THREAD_ID
        or plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or step.get("step_id") != STEP_ID
        or step.get("state") != "executable"
        or step.get("max_model_calls") != 1
        or step.get("max_total_tokens") != MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING
        or Path(str(step.get("directive_path") or "")).resolve() != DIRECTIVE_PATH
        or step.get("directive_sha256") != EXPECTED_DIRECTIVE_SHA256
        or step.get("accepted_receipt_states") != ["passed", "rejected", "waiting"]
        or Path(str(step.get("expected_receipt_path") or "")).resolve()
        != DEFAULT_OUTPUT_ROOT / "plan-step-receipt.json"
        or directive.get("schema_version") != DIRECTIVE_VERSION
        or directive.get("thread_id") != THREAD_ID
        or directive.get("plan_epoch") != PLAN_EPOCH
        or directive.get("step_id") != STEP_ID
        or not isinstance(execution, Mapping)
        or not isinstance(architecture, Mapping)
        or not isinstance(acceptance, Mapping)
        or not isinstance(terminal, Mapping)
        or execution.get("extraction_model_call_cap") != 1
        or execution.get("semantic_judge_call_cap") != 0
        or execution.get("semantic_retry_cap") != 0
        or execution.get("model") != MODEL
        or execution.get("reasoning_effort") != EFFORT
        or execution.get("app_server_arguments")
        != ["app-server", "--stdio", "--strict-config"]
        or execution.get("app_server_process_count") != 1
        or execution.get("thread_count") != 1
        or execution.get("thread_ephemeral") is not True
        or execution.get("turn_count") != 1
        or execution.get("measured_total_token_acceptance_ceiling")
        != MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING
        or execution.get("preflight_capacity_token_envelope")
        != PREFLIGHT_CAPACITY_TOKEN_ENVELOPE
        or execution.get("transport_native_maximum_token_field_available") is not False
        or execution.get("token_ceiling_is_post_completion_acceptance_not_transport_limit")
        is not True
        or execution.get("v249_full_applicability_schema_and_projection_required") is not True
        or execution.get("managed_chatgpt_auth_required") is not True
        or execution.get("zero_silent_retry") is not True
        or execution.get("no_model_overlay_preflight_required_before_runtime_lock") is not True
        or execution.get("turn_summary_none_if_supported_by_pinned_protocol") is not True
        or architecture.get("dense_per_segment_safety_cap") != MAX_EVENTS_PER_SEGMENT
        or architecture.get("prior_dense_per_segment_safety_cap")
        != PRIOR_MAX_EVENTS_PER_SEGMENT
        or architecture.get("deterministic_semantic_pruning_allowed") is not False
        or architecture.get("deterministic_support_filtering_allowed") is not False
        or architecture.get("deterministic_deduplication_allowed") is not False
        or architecture.get("deterministic_relabeling_allowed") is not False
        or acceptance.get("exact_identity_duplicate_count_reported_diagnostic_only")
        is not True
        or acceptance.get("event_count_is_not_a_semantic_acceptance_proxy") is not True
        or acceptance.get("measured_extraction_total_tokens_max")
        != MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING
        or acceptance.get("production_amortized_total_token_ratio_max")
        != PRODUCTION_RATIO_MAX
        or acceptance.get("production_mutated") is not False
        or acceptance.get("holdout_authorized") is not False
        or terminal.get("genuine_app_server_auth_capacity_transport_or_io_failure_state")
        != "waiting"
        or terminal.get("completed_output_structural_or_cost_failure_state")
        != "rejected"
        or terminal.get("integrity_or_programming_defect")
        != "raise_without_masking_as_waiting"
        or not isinstance(capacity_contract, Mapping)
        or capacity_contract.get("minimum_remaining_reserve_percent_after_conservative_projection")
        != MIN_REMAINING_RESERVE_PERCENT
        or capacity_contract.get("quota_points_per_million_tokens")
        != QUOTA_POINTS_PER_MILLION_TOKENS
        or capacity_contract.get("projected_phase_quota_points")
        != math.ceil(
            PREFLIGHT_CAPACITY_TOKEN_ENVELOPE
            * QUOTA_POINTS_PER_MILLION_TOKENS
            / 1_000_000
        )
        or not isinstance(cost, Mapping)
        or cost.get("baseline_end_to_end_tokens") != BASELINE_END_TO_END_TOKENS
        or cost.get("production_amortized_context_tokens")
        != PRODUCTION_AMORTIZED_CONTEXT_TOKENS
        or cost.get("production_scale") != PRODUCTION_SCALE
        or cost.get("extraction_total_token_acceptance_ceiling")
        != MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING
        or cost.get("required_ratio_max") != PRODUCTION_RATIO_MAX
    ):
        raise ExpandedCapCanaryError("epoch-4 semantic plan or directive drifted")
    frozen_rows = directive.get("frozen_inputs")
    if not isinstance(frozen_rows, list) or not frozen_rows:
        raise ExpandedCapCanaryError("epoch-4 frozen inputs are missing")
    roles = {str(row.get("role")) for row in frozen_rows if isinstance(row, Mapping)}
    required_roles = {
        "epoch_3_receipt",
        "epoch_3_runtime_lock",
        "epoch_2_receipt",
        "epoch_2_score",
        "epoch_2_runtime_lock",
        "v249_runtime_lock",
        "v249_terminal",
        "v249_source_packet",
        "v249_base_instructions",
        "v249_prompt",
        "v249_output_schema",
        "v249_projection_schema",
        "instruction_source",
    }
    if roles != required_roles or any(
        not isinstance(row, Mapping) or not _verify_record(row) for row in frozen_rows
    ):
        raise ExpandedCapCanaryError("epoch-4 frozen input set drifted")
    predecessor = directive.get("predecessor_receipt")
    if (
        not isinstance(predecessor, Mapping)
        or predecessor.get("state") != "rejected"
        or predecessor.get("turn_1_total_tokens") != 25_063
        or predecessor.get("turn_1_measured_acceptance_ceiling") != 24_000
        or predecessor.get("turn_2_started") is not False
    ):
        raise ExpandedCapCanaryError("epoch-3 predecessor evidence drifted")
    quality = directive.get("quality_basis")
    if (
        not isinstance(quality, Mapping)
        or quality.get("candidate_strict_full_field_macro_f1") != 0.963768
        or quality.get("quality_threshold") != 0.97
        or quality.get("dense_candidate_supported_units") != 32
        or quality.get("dense_shared_reference_units") != 37
        or quality.get("one_additional_supported_dense_unit_projects_macro_f1")
        != 0.971429
    ):
        raise ExpandedCapCanaryError("epoch-2 quality basis drifted")
    overlay = _config_overlay(directive)
    if not _summary_none_supported():
        raise ExpandedCapCanaryError("pinned TurnStartParams does not support summary none")
    return {
        "plan": plan,
        "directive": directive,
        "overlay": overlay,
        "frozen_records": [dict(row) for row in frozen_rows],
        "receipt_path": DEFAULT_OUTPUT_ROOT / "plan-step-receipt.json",
    }


class Epoch4CodexAppServerClient(codex_app_server.CodexAppServerClient):
    """Inject the checksum-bound context overlay without changing shared transport."""

    def __init__(self, *, config_overlay: Mapping[str, Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._epoch4_config_overlay = copy.deepcopy(dict(config_overlay))
        self._epoch4_instruction_sources: dict[str, tuple[str, ...]] = {}
        self._epoch4_turn_lineage: dict[str, Any] | None = None
        self._epoch4_instruction_verification: dict[str, Any] | None = None
        self._epoch4_instruction_verification_path: Path | None = None

    def bind_turn_lineage(
        self,
        *,
        runtime_lock: Mapping[str, Any],
        launch_receipt: Mapping[str, Any],
        capacity_checkpoint: Mapping[str, Any],
    ) -> None:
        lineage = {
            "lineage_schema_version": SIDECAR_LINEAGE_VERSION,
            "runtime_lock": dict(runtime_lock),
            "launch_receipt": dict(launch_receipt),
            "capacity_checkpoint": dict(capacity_checkpoint),
        }
        if self._epoch4_turn_lineage is not None and self._epoch4_turn_lineage != lineage:
            raise ExpandedCapCanaryError("epoch-4 client turn lineage was rebound")
        self._epoch4_turn_lineage = lineage

    def bind_instruction_verification(
        self, *, verification: Mapping[str, Any], path: Path
    ) -> None:
        value = copy.deepcopy(dict(verification))
        resolved = path.expanduser().resolve()
        if (
            self._epoch4_instruction_verification is not None
            and (
                self._epoch4_instruction_verification != value
                or self._epoch4_instruction_verification_path != resolved
            )
        ):
            raise ExpandedCapCanaryError(
                "epoch-4 instruction verification was rebound"
            )
        self._epoch4_instruction_verification = value
        self._epoch4_instruction_verification_path = resolved

    def _write_sidecar(self, path: Path, payload: dict[str, Any]) -> None:
        enriched = dict(payload)
        if self._epoch4_turn_lineage is not None:
            enriched.update(copy.deepcopy(self._epoch4_turn_lineage))
            enriched["semantic_thread_id"] = enriched.get("thread_id")
            enriched["semantic_turn_id"] = enriched.get("turn_id")
        if (
            isinstance(enriched.get("turn_id"), str)
            and enriched.get("turn_id")
            and self._epoch4_instruction_verification is not None
            and self._epoch4_instruction_verification_path is not None
        ):
            verification = copy.deepcopy(self._epoch4_instruction_verification)
            verification["semantic_turn_started"] = True
            verification["semantic_turn_id"] = enriched["turn_id"]
            _write_stable_time(
                self._epoch4_instruction_verification_path,
                verification,
                "verified_at",
            )
            enriched["instruction_verification"] = _record(
                self._epoch4_instruction_verification_path
            )
        codex_app_server.CodexAppServerClient._write_sidecar(path, enriched)

    async def _request(self, method: str, params: dict[str, Any]) -> Any:
        request_params = copy.deepcopy(params)
        if method == "thread/start":
            if "config" in request_params:
                raise ExpandedCapCanaryError("thread/start config overlay was supplied twice")
            request_params["config"] = copy.deepcopy(self._epoch4_config_overlay)
            request_params["personality"] = "none"
            request_params["environments"] = []
            request_params["dynamicTools"] = []
        elif method == "turn/start":
            request_params["summary"] = "none"
        result = await super()._request(method, request_params)
        if method == "thread/start":
            thread = result.get("thread") if isinstance(result, Mapping) else None
            thread_id = thread.get("id") if isinstance(thread, Mapping) else None
            sources = result.get("instructionSources") if isinstance(result, Mapping) else None
            if (
                not isinstance(thread_id, str)
                or not isinstance(sources, list)
                or any(not isinstance(item, str) for item in sources)
            ):
                raise codex_app_server.AppServerProtocolError(
                    "thread/start did not expose a valid instruction-source list"
                )
            self._epoch4_instruction_sources[thread_id] = tuple(sources)
        return result

    def instruction_sources_for(self, thread_id: str) -> tuple[str, ...]:
        try:
            return self._epoch4_instruction_sources[thread_id]
        except KeyError as exc:
            raise ExpandedCapCanaryError(
                "effective instruction sources were not captured"
            ) from exc


def _client_factory(overlay: Mapping[str, Any]) -> Epoch4CodexAppServerClient:
    return Epoch4CodexAppServerClient(
        config_overlay=overlay,
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"],
    )


def _source_paths() -> dict[str, Path]:
    return {
        "input": V249_TURN_ROOT / "input.private.json",
        "prompt": V249_TURN_ROOT / "prompt.private.md",
        "base": V249_TURN_ROOT / "base-instructions.private.md",
        "schema": V249_TURN_ROOT / "schema.json",
        "direct_schema": V249_TURN_ROOT / "projection-schema.json",
    }


def _expand_event_cap(schema: Mapping[str, Any]) -> dict[str, Any]:
    expanded = copy.deepcopy(dict(schema))
    try:
        segment = expanded["properties"]["segments"]["items"]
        events = segment["properties"]["events"]
        receipt_count = segment["properties"]["unit_receipts"]["items"]["properties"][
            "eligible_event_count"
        ]
        unresolved_count = segment["properties"]["unit_receipts"]["items"][
            "properties"
        ]["unresolved_count"]
    except (KeyError, TypeError) as exc:
        raise ExpandedCapCanaryError("frozen v249 schema shape drifted") from exc
    if (
        events.get("maxItems") != PRIOR_MAX_EVENTS_PER_SEGMENT
        or receipt_count.get("maximum") != PRIOR_MAX_EVENTS_PER_SEGMENT
        or unresolved_count.get("maximum") != PRIOR_MAX_EVENTS_PER_SEGMENT
    ):
        raise ExpandedCapCanaryError("frozen v249 event-cap fields drifted")
    events["maxItems"] = MAX_EVENTS_PER_SEGMENT
    receipt_count["maximum"] = MAX_EVENTS_PER_SEGMENT
    if unresolved_count.get("maximum") != PRIOR_MAX_EVENTS_PER_SEGMENT:
        raise ExpandedCapCanaryError("non-event source-unit constraint changed")
    return expanded


def prepare_turn(contract: Mapping[str, Any]) -> dict[str, Any]:
    paths = _source_paths()
    source = _load_json(paths["input"], "frozen v249 source packet")
    prompt = paths["prompt"].read_text(encoding="utf-8")
    source_base = paths["base"].read_text(encoding="utf-8")
    source_schema = _load_json(paths["schema"], "frozen v249 output schema")
    source_direct_schema = _load_json(
        paths["direct_schema"], "frozen v249 projection schema"
    )
    segment_ids = [str(row.get("segment_id")) for row in source.get("segments") or []]
    if segment_ids != [v233.DENSE_SEGMENT_ID, v233.NO_SIGNAL_SEGMENT_ID]:
        raise ExpandedCapCanaryError("frozen v249 segment order or membership drifted")
    if source_base.count(OLD_CAP_INSTRUCTION) != 1:
        raise ExpandedCapCanaryError("frozen v249 base cap instruction drifted")
    expanded_base = source_base.replace(OLD_CAP_INSTRUCTION, NEW_CAP_INSTRUCTION)
    if OLD_CAP_INSTRUCTION in expanded_base or expanded_base.count(NEW_CAP_INSTRUCTION) != 1:
        raise ExpandedCapCanaryError("epoch-4 base retained a stale 32-event cap")
    base = (
        expanded_base.rstrip()
        + "\n\n# Expanded-cap exhaustive enumeration contract\n"
        + EXHAUSTIVE_INSTRUCTIONS
        + "\n"
    )
    schema = _expand_event_cap(source_schema)
    direct_schema = _expand_event_cap(source_direct_schema)
    exhaustive_lower = EXHAUSTIVE_INSTRUCTIONS.lower()
    target_count_prohibition = "do not aim for a target count"
    if exhaustive_lower.count(target_count_prohibition) != 1:
        raise ExpandedCapCanaryError("target-count prohibition drifted")
    target_count_guidance = exhaustive_lower.replace(target_count_prohibition, "")
    if (
        "reference" in exhaustive_lower
        or "prior candidate" in exhaustive_lower
        or "target count" in target_count_guidance
        or "41" in EXHAUSTIVE_INSTRUCTIONS
        or "37" in EXHAUSTIVE_INSTRUCTIONS
        or "33" in EXHAUSTIVE_INSTRUCTIONS
    ):
        raise ExpandedCapCanaryError("exhaustive instruction exposes evaluation guidance")
    return {
        "turn_name": TURN_NAME,
        "episode_id": str(source.get("episode_id")),
        "segment_ids": segment_ids,
        "private_input": source,
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "direct_schema": direct_schema,
        "prompt_bytes": len(prompt.encode("utf-8")),
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
        "direct_schema_bytes": len(_canonical_json(direct_schema).encode("utf-8")),
        "config_overlay": copy.deepcopy(contract["overlay"]),
    }


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "direct_schema": turn_root / "projection-schema.json",
        "capacity": turn_root / "capacity.json",
        "instruction_verification": turn_root / "instruction-source-verification.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized-output.private.json",
        "provenance": turn_root / "evidence-provenance.private.json",
        "diagnostics": turn_root / "diagnostics.private.json",
        "applicability": turn_root / "applicability-receipt.json",
    }


def _request_records(root: Path) -> list[dict[str, Any]]:
    paths = _turn_paths(root)
    return [
        _record(paths[name])
        for name in ("input", "prompt", "base", "schema", "direct_schema")
    ]


def _meaningful_entries(root: Path) -> list[Path]:
    return [path for path in root.iterdir() if path.name != PROCESS_LOCK_NAME]


def _prepare_unlocked(root: Path) -> dict[str, Any]:
    if (root / "runtime-lock.json").is_file():
        verify_runtime_lock(root / "runtime-lock.json", acquire_lock=False)
        return _load_frozen(root)
    allowed = {
        "attempt-spec.json",
        "config-overlay.json",
        "mcp-server-name-receipt.json",
        "no-model-preflight.json",
        "turns",
    }
    if root.exists() and any(path.name not in allowed for path in _meaningful_entries(root)):
        raise ExpandedCapCanaryError("unfrozen epoch-4 root contains unknown artifacts")
    contract = load_contract()
    turn = prepare_turn(contract)
    paths = _turn_paths(root)
    source_paths = _source_paths()
    _write_immutable_bytes(paths["input"], source_paths["input"].read_bytes())
    _write_immutable_bytes(paths["prompt"], source_paths["prompt"].read_bytes())
    _write_immutable_text(paths["base"], turn["base"])
    _write_immutable_json(paths["schema"], turn["schema"])
    _write_immutable_json(paths["direct_schema"], turn["direct_schema"])
    overlay_path = root / "config-overlay.json"
    _write_immutable_json(overlay_path, turn["config_overlay"])
    mcp_names = list(_configured_mcp_server_names())
    mcp_receipt_path = root / "mcp-server-name-receipt.json"
    _write_immutable_json(
        mcp_receipt_path,
        {
            "schema_version": SCHEMA_VERSION,
            "configured_server_names": mcp_names,
            "configured_server_names_sha256": sha256_text(_canonical_json(mcp_names)),
            "disabled_server_names": sorted(turn["config_overlay"]["mcp_servers"]),
            "config_values_or_credentials_copied": False,
        },
    )
    spec_path = root / "attempt-spec.json"
    spec = {
        "schema_version": SCHEMA_VERSION,
        "created_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "turn_name": TURN_NAME,
        "episode_id": turn["episode_id"],
        "segment_ids": turn["segment_ids"],
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "expanded_event_cap": MAX_EVENTS_PER_SEGMENT,
        "measured_total_token_acceptance_ceiling": MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING,
        "transport_native_token_limit_configured": False,
        "prompt_bytes": turn["prompt_bytes"],
        "base_instructions_bytes": turn["base_bytes"],
        "schema_bytes": turn["schema_bytes"],
        "projection_schema_bytes": turn["direct_schema_bytes"],
        "config_overlay_sha256": _sha256_file(overlay_path),
        "reference_visible_to_model": False,
        "target_count_visible_to_model": False,
        "prior_output_visible_to_model": False,
        "deterministic_semantic_pruning_allowed": False,
        "deterministic_support_filtering_allowed": False,
        "deterministic_deduplication_allowed": False,
        "deterministic_relabeling_allowed": False,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
    }
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "contract": contract,
        "turn": {**turn, "paths": paths},
        "spec": _load_json(spec_path, "epoch-4 attempt spec"),
        "spec_path": spec_path,
        "overlay_path": overlay_path,
        "mcp_receipt_path": mcp_receipt_path,
        "preflight_path": root / "no-model-preflight.json",
        "runtime_lock": root / "runtime-lock.json",
    }


def prepare_run(output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _advisory_process_lock(root):
        return _prepare_unlocked(root)


def _instruction_source_records(paths: Sequence[str]) -> list[dict[str, Any]]:
    records = []
    for item in paths:
        path = Path(item).expanduser().resolve()
        if not path.is_absolute() or not path.is_file():
            raise OperationalWaitingError("effective instruction source is unavailable")
        records.append(_record(path))
    return records


def _instruction_sources_for(client: Any, thread_id: str) -> tuple[str, ...]:
    method = getattr(client, "instruction_sources_for", None)
    if not callable(method):
        raise ExpandedCapCanaryError("client does not expose effective instruction sources")
    sources = method(thread_id)
    if not isinstance(sources, tuple) or any(not isinstance(item, str) for item in sources):
        raise ExpandedCapCanaryError("effective instruction sources are malformed")
    return sources


def _verify_preflight_payload(payload: Mapping[str, Any], frozen: Mapping[str, Any]) -> None:
    sources = payload.get("effective_instruction_source_paths")
    records = payload.get("effective_instruction_source_records")
    expected_sources_json = _canonical_json(sources)
    if (
        payload.get("schema_version") != PREFLIGHT_VERSION
        or payload.get("state") != "passed"
        or payload.get("managed_chatgpt_auth_verified") is not True
        or payload.get("plan_type") != "pro"
        or payload.get("strict_config_overlay_accepted") is not True
        or payload.get("thread_started") is not True
        or payload.get("turn_started") is not False
        or payload.get("semantic_model_call_count") != 0
        or payload.get("usage_status") != "not_applicable"
        or payload.get("turn_summary_none_protocol_supported") is not True
        or payload.get("config_overlay") != _record(frozen["overlay_path"])
        or not isinstance(sources, list)
        or any(not isinstance(item, str) for item in sources)
        or not isinstance(records, list)
        or len(records) != len(sources)
        or [row.get("path") for row in records if isinstance(row, Mapping)]
        != [str(Path(item).expanduser().resolve()) for item in sources]
        or any(not isinstance(row, Mapping) or not _verify_record(row) for row in records)
        or payload.get("effective_instruction_sources_sha256")
        != sha256_text(expected_sources_json)
        or payload.get("effective_instruction_sources_count") != len(sources)
        or payload.get("production_mutated") is not False
    ):
        raise ExpandedCapCanaryError("epoch-4 no-model preflight drifted")


async def _run_no_model_preflight_unlocked(
    root: Path,
    *,
    client_factory: Callable[[Mapping[str, Any]], Any],
) -> dict[str, Any]:
    frozen = _prepare_unlocked(root)
    path = frozen["preflight_path"]
    if path.is_file():
        payload = _load_json(path, "epoch-4 no-model preflight")
        _verify_preflight_payload(payload, frozen)
        return payload
    if frozen["runtime_lock"].exists():
        raise ExpandedCapCanaryError("runtime lock exists without its bound preflight")
    try:
        async with client_factory(frozen["turn"]["config_overlay"]) as client:
            account = getattr(client, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                raise OperationalWaitingError("managed ChatGPT Pro auth is unavailable")
            thread = await client.start_thread(
                model=MODEL,
                base_instructions=frozen["turn"]["base"],
                cwd=PROJECT_ROOT,
                ephemeral=True,
            )
            sources = _instruction_sources_for(client, thread.thread_id)
            source_records = _instruction_source_records(sources)
            sources_json = _canonical_json(list(sources))
            if (
                thread.model != MODEL
                or thread.ephemeral is not True
                or thread.instruction_sources_count != len(sources)
                or thread.instruction_sources_sha256 != sha256_text(sources_json)
            ):
                raise OperationalWaitingError("no-model thread contract drifted")
    except (
        codex_app_server.AppServerError,
        asyncio.TimeoutError,
        OSError,
    ) as exc:
        raise OperationalWaitingError("no-model strict-config preflight failed") from exc
    payload = {
        "schema_version": PREFLIGHT_VERSION,
        "created_at": now_iso(),
        "state": "passed",
        "managed_chatgpt_auth_verified": True,
        "plan_type": "pro",
        "strict_config_overlay_accepted": True,
        "thread_started": True,
        "turn_started": False,
        "semantic_model_call_count": 0,
        "usage_status": "not_applicable",
        "config_overlay": _record(frozen["overlay_path"]),
        "effective_instruction_source_paths": list(sources),
        "effective_instruction_source_records": source_records,
        "effective_instruction_sources_sha256": sha256_text(sources_json),
        "effective_instruction_sources_count": len(sources),
        "turn_summary_none_protocol_supported": _summary_none_supported(),
        "production_mutated": False,
        "privacy": "instruction_source_paths_and_hashes_no_instruction_text_or_credentials",
    }
    _write_stable_time(path, payload, "created_at")
    persisted = _load_json(path, "persisted no-model preflight")
    _verify_preflight_payload(persisted, frozen)
    return persisted


async def run_no_model_preflight(
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    *,
    client_factory: Callable[[Mapping[str, Any]], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _advisory_process_lock(root):
        return await _run_no_model_preflight_unlocked(
            root, client_factory=client_factory
        )


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                Path(__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(v232.__file__).resolve(),
                Path(v233.__file__).resolve(),
                Path(v239.__file__).resolve(),
                Path(v248.__file__).resolve(),
                Path(v249.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
                TURN_START_SCHEMA_PATH,
            },
            key=str,
        )
    )


def _freeze_unlocked(root: Path) -> dict[str, Any]:
    frozen = _prepare_unlocked(root)
    lock_path = frozen["runtime_lock"]
    if lock_path.is_file():
        verify_runtime_lock(lock_path, acquire_lock=False)
        return _load_frozen(root)
    preflight_path = frozen["preflight_path"]
    if not preflight_path.is_file():
        raise ExpandedCapCanaryError(
            "no-model strict-config preflight must pass before runtime freeze"
        )
    preflight = _load_json(preflight_path, "epoch-4 no-model preflight")
    _verify_preflight_payload(preflight, frozen)
    spec_path = frozen["spec_path"]
    lock = {
        "schema_version": LOCK_VERSION,
        "created_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "expanded_event_cap": MAX_EVENTS_PER_SEGMENT,
        "measured_total_token_acceptance_ceiling": MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING,
        "preflight_capacity_token_envelope": PREFLIGHT_CAPACITY_TOKEN_ENVELOPE,
        "transport_native_token_limit_configured": False,
        "app_server_arguments": ["app-server", "--stdio", "--strict-config"],
        "plan": _record(PLAN_PATH),
        "directive": _record(DIRECTIVE_PATH),
        "frozen_inputs": frozen["contract"]["frozen_records"],
        "runtime_files": [_record(path) for path in _runtime_files()],
        "pinned_codex_cli": _record(PINNED_CODEX),
        "attempt_spec": _record(spec_path),
        "config_overlay": _record(frozen["overlay_path"]),
        "mcp_server_name_receipt": _record(frozen["mcp_receipt_path"]),
        "no_model_preflight": _record(preflight_path),
        "effective_instruction_source_paths": preflight[
            "effective_instruction_source_paths"
        ],
        "effective_instruction_source_records": preflight[
            "effective_instruction_source_records"
        ],
        "effective_instruction_sources_sha256": preflight[
            "effective_instruction_sources_sha256"
        ],
        "frozen_request": _request_records(root),
        "semantic_regex_or_keyword_filtering": False,
        "deterministic_semantic_pruning_allowed": False,
        "deterministic_support_filtering_allowed": False,
        "deterministic_deduplication_allowed": False,
        "deterministic_relabeling_allowed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "created_at")
    verify_runtime_lock(lock_path, acquire_lock=False)
    return _load_frozen(root)


def freeze_run(output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _advisory_process_lock(root):
        return _freeze_unlocked(root)


def verify_runtime_lock(path: Path, *, acquire_lock: bool = True) -> dict[str, Any]:
    def verify() -> dict[str, Any]:
        lock = _load_json(path, "epoch-4 runtime lock")
        root = path.parent.resolve()
        expected_runtime_paths = {str(item) for item in _runtime_files()}
        actual_runtime_paths = {
            str(Path(str(row.get("path"))).resolve())
            for row in lock.get("runtime_files") or []
            if isinstance(row, Mapping)
        }
        expected_request_paths = {row["path"] for row in _request_records(root)}
        actual_request_paths = {
            str(row.get("path"))
            for row in lock.get("frozen_request") or []
            if isinstance(row, Mapping)
        }
        contract = load_contract()
        frozen = _prepare_unlocked(root) if not path.is_file() else {
            "overlay_path": root / "config-overlay.json",
            "mcp_receipt_path": root / "mcp-server-name-receipt.json",
            "preflight_path": root / "no-model-preflight.json",
        }
        preflight = _load_json(
            frozen["preflight_path"], "epoch-4 no-model preflight"
        )
        preflight_context = {
            "overlay_path": frozen["overlay_path"],
        }
        _verify_preflight_payload(preflight, preflight_context)
        mcp_receipt = _load_json(
            frozen["mcp_receipt_path"], "epoch-4 MCP server-name receipt"
        )
        configured_names = list(_configured_mcp_server_names())
        records = [
            lock.get("plan"),
            lock.get("directive"),
            lock.get("pinned_codex_cli"),
            lock.get("attempt_spec"),
            lock.get("config_overlay"),
            lock.get("mcp_server_name_receipt"),
            lock.get("no_model_preflight"),
            *(lock.get("frozen_inputs") or []),
            *(lock.get("runtime_files") or []),
            *(lock.get("effective_instruction_source_records") or []),
            *(lock.get("frozen_request") or []),
        ]
        if (
            lock.get("schema_version") != LOCK_VERSION
            or lock.get("thread_id") != THREAD_ID
            or lock.get("plan_epoch") != PLAN_EPOCH
            or lock.get("step_id") != STEP_ID
            or lock.get("model") != MODEL
            or lock.get("effort") != EFFORT
            or lock.get("declared_turn_count") != 1
            or lock.get("retry_count") != 0
            or lock.get("expanded_event_cap") != MAX_EVENTS_PER_SEGMENT
            or lock.get("measured_total_token_acceptance_ceiling")
            != MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING
            or lock.get("preflight_capacity_token_envelope")
            != PREFLIGHT_CAPACITY_TOKEN_ENVELOPE
            or lock.get("transport_native_token_limit_configured") is not False
            or lock.get("app_server_arguments")
            != ["app-server", "--stdio", "--strict-config"]
            or lock.get("plan") != _record(PLAN_PATH)
            or lock.get("directive") != _record(DIRECTIVE_PATH)
            or lock.get("pinned_codex_cli") != _record(PINNED_CODEX)
            or actual_runtime_paths != expected_runtime_paths
            or actual_request_paths != expected_request_paths
            or lock.get("config_overlay") != _record(root / "config-overlay.json")
            or lock.get("mcp_server_name_receipt")
            != _record(root / "mcp-server-name-receipt.json")
            or lock.get("no_model_preflight")
            != _record(root / "no-model-preflight.json")
            or lock.get("effective_instruction_source_paths")
            != preflight.get("effective_instruction_source_paths")
            or lock.get("effective_instruction_source_records")
            != preflight.get("effective_instruction_source_records")
            or lock.get("effective_instruction_sources_sha256")
            != preflight.get("effective_instruction_sources_sha256")
            or lock.get("frozen_inputs") != contract["frozen_records"]
            or mcp_receipt.get("configured_server_names") != configured_names
            or mcp_receipt.get("disabled_server_names") != configured_names
            or mcp_receipt.get("configured_server_names_sha256")
            != sha256_text(_canonical_json(configured_names))
            or lock.get("semantic_regex_or_keyword_filtering") is not False
            or lock.get("deterministic_semantic_pruning_allowed") is not False
            or lock.get("deterministic_support_filtering_allowed") is not False
            or lock.get("deterministic_deduplication_allowed") is not False
            or lock.get("deterministic_relabeling_allowed") is not False
            or lock.get("development_winner_frozen") is not False
            or lock.get("holdout_authorized") is not False
            or lock.get("production_mutation_allowed") is not False
            or any(not isinstance(record, Mapping) or not _verify_record(record) for record in records)
        ):
            raise ExpandedCapCanaryError("epoch-4 runtime lock drifted")
        return lock

    if acquire_lock:
        with _advisory_process_lock(path.parent.resolve()):
            return verify()
    return verify()


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    paths = _turn_paths(root)
    contract = load_contract()
    private_input = _load_json(paths["input"], "epoch-4 input")
    return {
        "root": root,
        "contract": contract,
        "spec": _load_json(spec_path, "epoch-4 attempt spec"),
        "spec_path": spec_path,
        "overlay_path": root / "config-overlay.json",
        "mcp_receipt_path": root / "mcp-server-name-receipt.json",
        "preflight_path": root / "no-model-preflight.json",
        "runtime_lock": root / "runtime-lock.json",
        "turn": {
            "turn_name": TURN_NAME,
            "episode_id": private_input["episode_id"],
            "segment_ids": [
                str(row["segment_id"]) for row in private_input["segments"]
            ],
            "private_input": private_input,
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "epoch-4 schema"),
            "direct_schema": _load_json(
                paths["direct_schema"], "epoch-4 projection schema"
            ),
            "config_overlay": _load_json(
                root / "config-overlay.json", "epoch-4 config overlay"
            ),
            "paths": paths,
        },
    }


def _project_direct_output_duplicate_tolerant(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], int]:
    try:
        labels_module._validate_schema(turn["direct_schema"], output, path="$")  # noqa: SLF001
    except (labels_module.ValidationError, ValueError, TypeError) as exc:
        raise StructuralRejectionError("projected output schema failed") from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise StructuralRejectionError("episode id drifted")
    rows = list(output.get("segments") or [])
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise StructuralRejectionError("segment order or coverage drifted")
    source_by_id = {
        str(row["segment_id"]): row for row in turn["private_input"]["segments"]
    }
    normalized_rows: list[dict[str, Any]] = []
    provenance_rows: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    seen_identities: set[str] = set()
    total_duplicate_count = 0
    raw_event_count = 0
    for row in rows:
        segment_id = str(row["segment_id"])
        source = source_by_id[segment_id]
        units = list(source["units"])
        expected_unit_ids = [str(unit["unit_id"]) for unit in units]
        receipts = list(row.get("unit_receipts") or [])
        if [receipt.get("unit_id") for receipt in receipts] != expected_unit_ids:
            raise StructuralRejectionError("unit receipt order or coverage drifted")
        coverage = row.get("coverage_audit") or {}
        if (
            coverage.get("all_source_units_reviewed") is not True
            or coverage.get("unresolved_count") != 0
        ):
            raise StructuralRejectionError("source-unit coverage audit failed")
        receipt_counts: dict[str, int] = {}
        for receipt in receipts:
            eligible = receipt.get("eligible_event_count")
            if (
                isinstance(eligible, bool)
                or not isinstance(eligible, int)
                or eligible < 0
                or eligible > MAX_EVENTS_PER_SEGMENT
                or receipt.get("unresolved_count") != 0
            ):
                raise StructuralRejectionError("unit receipt count is invalid")
            receipt_counts[str(receipt["unit_id"])] = eligible
        events = list(row.get("events") or [])
        raw_event_count += len(events)
        if sum(receipt_counts.values()) != len(events):
            raise StructuralRejectionError("unit receipt event total drifted")
        if (row.get("status") == "coded") != bool(events):
            raise StructuralRejectionError("coded status does not match events")
        if not events and row.get("status") != "no_signal":
            raise StructuralRejectionError("empty extraction must be no_signal")
        if len(events) > MAX_EVENTS_PER_SEGMENT:
            raise StructuralRejectionError("expanded event safety cap exceeded")
        unit_index = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
        start_counts = {unit_id: 0 for unit_id in expected_unit_ids}
        prior_start_index = -1
        projected_events: list[dict[str, Any]] = []
        text = str(source["segment_text"])
        boundaries = list(source["boundaries"])
        segment_duplicate_count = 0
        for event_index, raw_event in enumerate(events):
            event = dict(raw_event)
            start_id = str(event.pop("evidence_start_unit_id"))
            end_id = str(event.pop("evidence_end_unit_id"))
            if start_id not in unit_index or end_id not in unit_index:
                raise StructuralRejectionError("evidence unit belongs to another segment")
            start_index = unit_index[start_id]
            end_index = unit_index[end_id]
            if start_index > end_index:
                raise StructuralRejectionError("evidence unit range is reversed")
            if start_index < prior_start_index:
                raise StructuralRejectionError("event evidence order drifted")
            prior_start_index = start_index
            start_char = int(units[start_index]["start_char"])
            end_char = int(units[end_index]["end_char"])
            evidence = text[start_char:end_char]
            if not evidence:
                raise StructuralRejectionError("projected evidence is empty")
            if not any(
                int(boundary["extract_start"]) <= start_char
                and end_char <= int(boundary["extract_end"])
                for boundary in boundaries
            ):
                raise StructuralRejectionError(
                    "projected evidence is outside every fixed evidence window"
                )
            metric_values = [
                str(event.get(field) or "")
                for field in (
                    "metric_value",
                    "metric_unit",
                    "metric_comparator",
                    "metric_raw_text",
                )
            ]
            if any(value and value not in evidence for value in metric_values):
                raise StructuralRejectionError("metric literal is not in evidence")
            if any(metric_values) == (
                event.get("metric_direction") == "not_applicable"
            ):
                raise StructuralRejectionError("metric direction applicability drifted")
            identity = _canonical_json(
                {field: event.get(field) for field in v233.IDENTITY_FIELDS}
            )
            if identity in seen_identities:
                segment_duplicate_count += 1
                total_duplicate_count += 1
            else:
                seen_identities.add(identity)
            window_id = v232._owner_window(start_char, boundaries)  # noqa: SLF001
            event["window_id"] = window_id
            event["evidence"] = evidence
            projected_events.append(event)
            start_counts[start_id] += 1
            provenance_rows.append(
                {
                    "segment_id": segment_id,
                    "event_index": event_index,
                    "evidence_start_unit_id": start_id,
                    "evidence_end_unit_id": end_id,
                    "start_char": start_char,
                    "end_char": end_char,
                    "window_id": window_id,
                    "evidence_sha256": sha256_text(evidence),
                }
            )
        if start_counts != receipt_counts:
            raise StructuralRejectionError("unit receipt ownership drifted")
        normalized_rows.append(
            {
                "segment_id": segment_id,
                "status": row["status"],
                "segment_source_context": row["segment_source_context"],
                "no_signal_reason": row["no_signal_reason"],
                "events": projected_events,
            }
        )
        diagnostics.append(
            {
                "segment_id": segment_id,
                "density_stratum": source["density_stratum"],
                "source_unit_count": len(units),
                "reviewed_source_unit_count": len(receipts),
                "event_count": len(projected_events),
                "unresolved_count": 0,
                "event_cap": MAX_EVENTS_PER_SEGMENT,
                "event_cap_violation_count": 0,
                "metric_grounding_error_event_count": 0,
                "exact_identity_duplicate_count": segment_duplicate_count,
                "exact_identity_duplicate_detection": "diagnostic_only_nonblocking",
            }
        )
    projected_event_count = sum(
        len(row["events"]) for row in normalized_rows
    )
    if projected_event_count != raw_event_count:
        raise StructuralRejectionError("deterministic projection dropped an emitted event")
    return (
        {"episode_id": turn["episode_id"], "segments": normalized_rows},
        {
            "schema_version": SCHEMA_VERSION,
            "episode_id": turn["episode_id"],
            "events": provenance_rows,
        },
        diagnostics,
        total_duplicate_count,
    )


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    try:
        labels_module._validate_schema(turn["schema"], output, path="$")  # noqa: SLF001
    except (labels_module.ValidationError, ValueError, TypeError) as exc:
        raise StructuralRejectionError(
            "explicit-applicability output schema failed"
        ) from exc
    direct = copy.deepcopy(dict(output))
    state_counts: dict[str, dict[str, int]] = {}
    raw_event_count = 0
    for segment in direct.get("segments") or []:
        projected_events = []
        for event in segment.get("events") or []:
            raw_event_count += 1
            for field in (
                *v249.OPTIONAL_TEXT_FIELDS,
                "claim_type",
                "stance",
                "speaker",
                "actor",
                "reported_actor",
                "metric",
                *v249.ENTITY_FIELDS,
            ):
                state = str(event[field]["applicability"])
                bucket = state_counts.setdefault(field, {})
                bucket[state] = bucket.get(state, 0) + 1
            try:
                projected_events.append(v249._project_event(event))  # noqa: SLF001
            except (v249.V249OutputContractError, KeyError, TypeError, ValueError) as exc:
                raise StructuralRejectionError(
                    "explicit applicability projection failed"
                ) from exc
        segment["events"] = projected_events
    normalized, provenance, diagnostics, duplicate_count = (
        _project_direct_output_duplicate_tolerant(direct, turn)
    )
    normalized_event_count = sum(
        len(row.get("events") or []) for row in normalized["segments"]
    )
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "state_counts": state_counts,
        "all_optional_semantics_selected_by_llm": True,
        "deterministic_projection_only": True,
        "emitted_event_count": raw_event_count,
        "normalized_event_count": normalized_event_count,
        "all_emitted_events_preserved": raw_event_count == normalized_event_count,
        "exact_identity_duplicate_count": duplicate_count,
        "exact_identity_duplicate_detection": "diagnostic_only_nonblocking",
        "semantic_pruning_performed": False,
        "support_filtering_performed": False,
        "deduplication_performed": False,
        "relabeling_performed": False,
    }
    if receipt["all_emitted_events_preserved"] is not True:
        raise StructuralRejectionError("projection did not preserve every emitted event")
    return normalized, provenance, diagnostics, receipt


_LAUNCH_KEYS = {
    "schema_version",
    "launched_at",
    "thread_id",
    "plan_epoch",
    "step_id",
    "model",
    "effort",
    "declared_turn_count",
    "retry_count",
    "measured_total_token_acceptance_ceiling",
    "transport_native_token_limit_configured",
    "config_overlay",
    "no_model_preflight",
    "runtime_lock",
    "development_winner_frozen",
    "holdout_authorized",
    "production_mutation_allowed",
}

_CAPACITY_KEYS = {
    "schema_version",
    "checked_at",
    "managed_chatgpt_auth_verified",
    "plan_type",
    "primary_used_percent",
    "primary_remaining_percent",
    "minimum_remaining_reserve_percent",
    "preflight_capacity_token_envelope",
    "projected_phase_quota_points",
    "projected_terminal_remaining_percent",
    "rate_limit_reached_type",
    "cleared_for_semantic_turn",
    "thread_started_before_probe",
    "semantic_turn_started_before_probe",
    "sidecar_started_before_probe",
    "transport_native_token_limit_configured",
    "runtime_lock",
    "launch_receipt",
    "production_mutated",
}

_INSTRUCTION_VERIFICATION_KEYS = {
    "schema_version",
    "verified_at",
    "semantic_thread_started",
    "semantic_turn_started",
    "semantic_thread_id",
    "semantic_turn_id",
    "managed_chatgpt_auth_verified",
    "model",
    "effort",
    "config_overlay",
    "runtime_lock",
    "launch_receipt",
    "capacity_checkpoint",
    "effective_instruction_source_paths",
    "effective_instruction_source_records",
    "effective_instruction_sources_sha256",
    "effective_instruction_sources_count",
    "matches_no_model_preflight",
    "turn_summary",
    "dynamic_tools",
    "environments",
    "production_mutated",
}


def _launch_receipt_payload(frozen: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": LAUNCH_VERSION,
        "launched_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "measured_total_token_acceptance_ceiling": MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING,
        "transport_native_token_limit_configured": False,
        "config_overlay": _record(frozen["overlay_path"]),
        "no_model_preflight": _record(frozen["preflight_path"]),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }


def _validate_launch_receipt(
    path: Path, frozen: Mapping[str, Any]
) -> dict[str, Any]:
    launch = _load_json(path, "epoch-4 launch receipt")
    if (
        not isinstance(launch, Mapping)
        or set(launch) != _LAUNCH_KEYS
        or launch.get("schema_version") != LAUNCH_VERSION
        or not _is_iso_timestamp(launch.get("launched_at"))
        or launch.get("thread_id") != THREAD_ID
        or launch.get("plan_epoch") != PLAN_EPOCH
        or launch.get("step_id") != STEP_ID
        or launch.get("model") != MODEL
        or launch.get("effort") != EFFORT
        or launch.get("declared_turn_count") != 1
        or launch.get("retry_count") != 0
        or launch.get("measured_total_token_acceptance_ceiling")
        != MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING
        or launch.get("transport_native_token_limit_configured") is not False
        or not _record_matches(launch.get("config_overlay"), frozen["overlay_path"])
        or not _record_matches(launch.get("no_model_preflight"), frozen["preflight_path"])
        or not _record_matches(launch.get("runtime_lock"), frozen["runtime_lock"])
        or launch.get("development_winner_frozen") is not False
        or launch.get("holdout_authorized") is not False
        or launch.get("production_mutation_allowed") is not False
    ):
        raise ExpandedCapCanaryError("epoch-4 launch receipt integrity failed")
    return dict(launch)


def _validate_capacity_checkpoint(
    path: Path,
    *,
    runtime_lock_record: Mapping[str, Any],
    launch_receipt_record: Mapping[str, Any],
    require_clear: bool,
) -> dict[str, Any]:
    checkpoint = _load_json(path, "epoch-4 capacity checkpoint")
    used = checkpoint.get("primary_used_percent") if isinstance(checkpoint, Mapping) else None
    remaining = 100 - used if isinstance(used, int) and not isinstance(used, bool) else None
    projected_points = math.ceil(
        PREFLIGHT_CAPACITY_TOKEN_ENVELOPE
        * QUOTA_POINTS_PER_MILLION_TOKENS
        / 1_000_000
    )
    reached = checkpoint.get("rate_limit_reached_type") if isinstance(checkpoint, Mapping) else None
    expected_clear = bool(
        remaining is not None
        and reached is None
        and remaining - projected_points >= MIN_REMAINING_RESERVE_PERCENT
    )
    if (
        not isinstance(checkpoint, Mapping)
        or set(checkpoint) != _CAPACITY_KEYS
        or checkpoint.get("schema_version") != CAPACITY_VERSION
        or not _is_iso_timestamp(checkpoint.get("checked_at"))
        or checkpoint.get("managed_chatgpt_auth_verified") is not True
        or checkpoint.get("plan_type") != "pro"
        or isinstance(used, bool)
        or not isinstance(used, int)
        or not 0 <= used <= 100
        or checkpoint.get("primary_remaining_percent") != remaining
        or checkpoint.get("minimum_remaining_reserve_percent")
        != MIN_REMAINING_RESERVE_PERCENT
        or checkpoint.get("preflight_capacity_token_envelope")
        != PREFLIGHT_CAPACITY_TOKEN_ENVELOPE
        or checkpoint.get("projected_phase_quota_points") != projected_points
        or checkpoint.get("projected_terminal_remaining_percent")
        != remaining - projected_points
        or (reached is not None and not isinstance(reached, str))
        or checkpoint.get("cleared_for_semantic_turn") is not expected_clear
        or checkpoint.get("thread_started_before_probe") is not False
        or checkpoint.get("semantic_turn_started_before_probe") is not False
        or checkpoint.get("sidecar_started_before_probe") is not False
        or checkpoint.get("transport_native_token_limit_configured") is not False
        or dict(checkpoint.get("runtime_lock") or {}) != dict(runtime_lock_record)
        or dict(checkpoint.get("launch_receipt") or {}) != dict(launch_receipt_record)
        or not _verify_record(runtime_lock_record)
        or not _verify_record(launch_receipt_record)
        or checkpoint.get("production_mutated") is not False
        or (require_clear and expected_clear is not True)
    ):
        raise ExpandedCapCanaryError("epoch-4 capacity checkpoint integrity failed")
    return dict(checkpoint)


async def _probe_capacity(
    client: Any,
    *,
    checkpoint_path: Path,
    runtime_lock_record: Mapping[str, Any],
    launch_receipt_record: Mapping[str, Any],
) -> dict[str, Any]:
    if checkpoint_path.exists():
        raise ExpandedCapCanaryError("capacity checkpoint cannot be reused")
    account = getattr(client, "account_summary", None)
    if (
        not isinstance(account, Mapping)
        or account.get("type") != "chatgpt"
        or account.get("plan_type") != "pro"
    ):
        raise OperationalWaitingError("managed ChatGPT Pro auth is unavailable")
    response = await client._request("account/rateLimits/read", {})  # noqa: SLF001
    snapshot = capacity.parse_rate_limit_snapshot(
        response, maximum_primary_used_percent=100
    )
    used = snapshot.get("primary_used_percent")
    reached = snapshot.get("rate_limit_reached_type")
    if isinstance(used, bool) or not isinstance(used, int) or not 0 <= used <= 100:
        raise OperationalWaitingError("capacity percentage is malformed")
    projected_points = math.ceil(
        PREFLIGHT_CAPACITY_TOKEN_ENVELOPE
        * QUOTA_POINTS_PER_MILLION_TOKENS
        / 1_000_000
    )
    remaining = 100 - used
    cleared = bool(
        reached is None
        and remaining - projected_points >= MIN_REMAINING_RESERVE_PERCENT
    )
    checkpoint = {
        "schema_version": CAPACITY_VERSION,
        "checked_at": now_iso(),
        "managed_chatgpt_auth_verified": True,
        "plan_type": "pro",
        "primary_used_percent": used,
        "primary_remaining_percent": remaining,
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "preflight_capacity_token_envelope": PREFLIGHT_CAPACITY_TOKEN_ENVELOPE,
        "projected_phase_quota_points": projected_points,
        "projected_terminal_remaining_percent": remaining - projected_points,
        "rate_limit_reached_type": reached,
        "cleared_for_semantic_turn": cleared,
        "thread_started_before_probe": False,
        "semantic_turn_started_before_probe": False,
        "sidecar_started_before_probe": False,
        "transport_native_token_limit_configured": False,
        "runtime_lock": dict(runtime_lock_record),
        "launch_receipt": dict(launch_receipt_record),
        "production_mutated": False,
    }
    _write_immutable_json(checkpoint_path, checkpoint)
    _validate_capacity_checkpoint(
        checkpoint_path,
        runtime_lock_record=runtime_lock_record,
        launch_receipt_record=launch_receipt_record,
        require_clear=False,
    )
    if not cleared:
        raise OperationalWaitingError("minimum reserve or provider capacity is unavailable")
    return checkpoint


def _semantic_instruction_verification(
    *,
    client: Any,
    thread: Any,
    frozen: Mapping[str, Any],
) -> dict[str, Any]:
    preflight = _load_json(frozen["preflight_path"], "epoch-4 no-model preflight")
    launch_path = frozen["root"] / "launch-receipt.json"
    capacity_path = frozen["turn"]["paths"]["capacity"]
    launch = _validate_launch_receipt(launch_path, frozen)
    capacity_checkpoint = _validate_capacity_checkpoint(
        capacity_path,
        runtime_lock_record=_record(frozen["runtime_lock"]),
        launch_receipt_record=_record(launch_path),
        require_clear=True,
    )
    sources = _instruction_sources_for(client, thread.thread_id)
    records = _instruction_source_records(sources)
    sources_json = _canonical_json(list(sources))
    verification = {
        "schema_version": INSTRUCTION_VERIFICATION_VERSION,
        "verified_at": now_iso(),
        "semantic_thread_started": True,
        "semantic_turn_started": False,
        "semantic_thread_id": thread.thread_id,
        "semantic_turn_id": None,
        "managed_chatgpt_auth_verified": True,
        "model": MODEL,
        "effort": EFFORT,
        "config_overlay": _record(frozen["overlay_path"]),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "launch_receipt": _record(launch_path),
        "capacity_checkpoint": _record(capacity_path),
        "effective_instruction_source_paths": list(sources),
        "effective_instruction_source_records": records,
        "effective_instruction_sources_sha256": sha256_text(sources_json),
        "effective_instruction_sources_count": len(sources),
        "matches_no_model_preflight": bool(
            list(sources) == preflight.get("effective_instruction_source_paths")
            and records == preflight.get("effective_instruction_source_records")
            and sha256_text(sources_json)
            == preflight.get("effective_instruction_sources_sha256")
            and len(sources)
            == preflight.get("effective_instruction_sources_count")
        ),
        "turn_summary": "none",
        "dynamic_tools": [],
        "environments": [],
        "production_mutated": False,
    }
    if (
        verification["matches_no_model_preflight"] is not True
        or thread.instruction_sources_sha256
        != verification["effective_instruction_sources_sha256"]
        or thread.instruction_sources_count
        != verification["effective_instruction_sources_count"]
    ):
        raise OperationalWaitingError("semantic instruction-source contract drifted")
    if launch.get("model") != MODEL or capacity_checkpoint.get(
        "cleared_for_semantic_turn"
    ) is not True:
        raise ExpandedCapCanaryError("semantic launch lineage drifted")
    return verification


def _validate_instruction_verification(
    path: Path,
    *,
    frozen: Mapping[str, Any],
    launch_path: Path,
    capacity_path: Path,
) -> dict[str, Any]:
    verification = _load_json(path, "semantic instruction-source verification")
    preflight = _load_json(frozen["preflight_path"], "epoch-4 no-model preflight")
    sources = verification.get("effective_instruction_source_paths") if isinstance(
        verification, Mapping
    ) else None
    records = verification.get("effective_instruction_source_records") if isinstance(
        verification, Mapping
    ) else None
    sources_json = _canonical_json(sources)
    recomputed_match = bool(
        isinstance(sources, list)
        and all(isinstance(item, str) for item in sources)
        and isinstance(records, list)
        and len(records) == len(sources)
        and [row.get("path") for row in records if isinstance(row, Mapping)]
        == [str(Path(item).expanduser().resolve()) for item in sources]
        and all(isinstance(row, Mapping) and _verify_record(row) for row in records)
        and sources == preflight.get("effective_instruction_source_paths")
        and records == preflight.get("effective_instruction_source_records")
        and sha256_text(sources_json)
        == preflight.get("effective_instruction_sources_sha256")
        and len(sources) == preflight.get("effective_instruction_sources_count")
    )
    semantic_thread_id = verification.get("semantic_thread_id") if isinstance(
        verification, Mapping
    ) else None
    semantic_turn_id = verification.get("semantic_turn_id") if isinstance(
        verification, Mapping
    ) else None
    if (
        not isinstance(verification, Mapping)
        or set(verification) != _INSTRUCTION_VERIFICATION_KEYS
        or verification.get("schema_version") != INSTRUCTION_VERIFICATION_VERSION
        or not _is_iso_timestamp(verification.get("verified_at"))
        or verification.get("semantic_thread_started") is not True
        or verification.get("semantic_turn_started") is not True
        or not isinstance(semantic_thread_id, str)
        or not semantic_thread_id
        or semantic_thread_id != semantic_thread_id.strip()
        or not isinstance(semantic_turn_id, str)
        or not semantic_turn_id
        or semantic_turn_id != semantic_turn_id.strip()
        or verification.get("managed_chatgpt_auth_verified") is not True
        or verification.get("model") != MODEL
        or verification.get("effort") != EFFORT
        or not _record_matches(verification.get("config_overlay"), frozen["overlay_path"])
        or not _record_matches(verification.get("runtime_lock"), frozen["runtime_lock"])
        or not _record_matches(verification.get("launch_receipt"), launch_path)
        or not _record_matches(verification.get("capacity_checkpoint"), capacity_path)
        or verification.get("effective_instruction_sources_sha256")
        != sha256_text(sources_json)
        or verification.get("effective_instruction_sources_count")
        != (len(sources) if isinstance(sources, list) else None)
        or verification.get("matches_no_model_preflight") is not True
        or recomputed_match is not True
        or verification.get("turn_summary") != "none"
        or verification.get("dynamic_tools") != []
        or verification.get("environments") != []
        or verification.get("production_mutated") is not False
    ):
        raise ExpandedCapCanaryError(
            "epoch-4 semantic instruction verification integrity failed"
        )
    return dict(verification)


def _bind_completed_semantic_lineage(
    *,
    frozen: Mapping[str, Any],
    verification: Mapping[str, Any],
    result: Any,
) -> dict[str, Any]:
    paths = frozen["turn"]["paths"]
    launch_path = frozen["root"] / "launch-receipt.json"
    capacity_path = paths["capacity"]
    semantic_thread_id = getattr(result, "thread_id", None)
    semantic_turn_id = getattr(result, "turn_id", None)
    if (
        not isinstance(semantic_thread_id, str)
        or not semantic_thread_id
        or not isinstance(semantic_turn_id, str)
        or not semantic_turn_id
        or verification.get("semantic_thread_id") != semantic_thread_id
    ):
        raise ExpandedCapCanaryError("completed semantic turn lineage is malformed")
    completed_verification = dict(verification)
    completed_verification["semantic_turn_started"] = True
    completed_verification["semantic_turn_id"] = semantic_turn_id
    _write_stable_time(
        paths["instruction_verification"], completed_verification, "verified_at"
    )
    persisted_verification = _validate_instruction_verification(
        paths["instruction_verification"],
        frozen=frozen,
        launch_path=launch_path,
        capacity_path=capacity_path,
    )
    sidecar = _load_json(paths["sidecar"], "completed epoch-4 turn sidecar")
    if (
        not isinstance(sidecar, Mapping)
        or sidecar.get("thread_id") != semantic_thread_id
        or sidecar.get("turn_id") != semantic_turn_id
    ):
        raise ExpandedCapCanaryError("transport sidecar semantic ids drifted")
    bound_sidecar = dict(sidecar)
    lineage = {
        "lineage_schema_version": SIDECAR_LINEAGE_VERSION,
        "runtime_lock": _record(frozen["runtime_lock"]),
        "launch_receipt": _record(launch_path),
        "capacity_checkpoint": _record(capacity_path),
        "instruction_verification": _record(paths["instruction_verification"]),
        "semantic_thread_id": semantic_thread_id,
        "semantic_turn_id": semantic_turn_id,
    }
    for key, value in lineage.items():
        if key in bound_sidecar and bound_sidecar[key] != value:
            raise ExpandedCapCanaryError("transport sidecar lineage already drifted")
        bound_sidecar[key] = value
    util_module.write_text_atomic(
        paths["sidecar"],
        json.dumps(bound_sidecar, ensure_ascii=True, sort_keys=True, indent=2) + "\n",
    )
    return persisted_verification


def _usage_from_sidecar(
    path: Path,
    *,
    output_path: Path,
    frozen: Mapping[str, Any],
    enforce_token_ceiling: bool = True,
) -> tuple[dict[str, int], dict[str, Any]]:
    launch_path = frozen["root"] / "launch-receipt.json"
    capacity_path = frozen["turn"]["paths"]["capacity"]
    instruction_path = frozen["turn"]["paths"]["instruction_verification"]
    _validate_launch_receipt(launch_path, frozen)
    _validate_capacity_checkpoint(
        capacity_path,
        runtime_lock_record=_record(frozen["runtime_lock"]),
        launch_receipt_record=_record(launch_path),
        require_clear=True,
    )
    instruction_verification = _validate_instruction_verification(
        instruction_path,
        frozen=frozen,
        launch_path=launch_path,
        capacity_path=capacity_path,
    )
    sidecar = _load_json(path, "epoch-4 turn sidecar")
    usage = sidecar.get("usage")
    if not isinstance(usage, Mapping):
        raise OperationalWaitingError("turn usage is absent")
    values: dict[str, int] = {}
    for field in USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise OperationalWaitingError("turn usage is incomplete")
        values[field] = value
    preflight = _load_json(frozen["preflight_path"], "epoch-4 no-model preflight")
    output_text = output_path.read_text(encoding="utf-8")
    output_message = output_text[:-1] if output_text.endswith("\n") else output_text
    wall = sidecar.get("wall_elapsed_seconds")
    schema_json = _canonical_json(frozen["turn"]["schema"])
    semantic_thread_id = instruction_verification["semantic_thread_id"]
    semantic_turn_id = instruction_verification["semantic_turn_id"]
    if (
        sidecar.get("schema_version")
        != codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
        or not _is_iso_timestamp(sidecar.get("started_at"))
        or not _is_iso_timestamp(sidecar.get("finished_at"))
        or sidecar.get("client_version")
        != codex_app_server.APP_SERVER_CLIENT_VERSION
        or sidecar.get("cli_version")
        != codex_app_server.PINNED_CODEX_CLI_VERSION
        or sidecar.get("protocol_schema_sha256")
        != _sha256_file(codex_app_server.PROTOCOL_SCHEMA_PATH)
        or sidecar.get("transport") != "stdio"
        or not isinstance(sidecar.get("app_server_user_agent"), str)
        or not sidecar.get("app_server_user_agent")
        or isinstance(sidecar.get("max_message_bytes"), bool)
        or not isinstance(sidecar.get("max_message_bytes"), int)
        or sidecar.get("max_message_bytes") < 64 * 1024
        or sidecar.get("synthetic_debug_errors") is not False
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("thread_mode") != "new_thread"
        or sidecar.get("batch_size") != 2
        or sidecar.get("error_class") is not None
        or sidecar.get("recovery_reran_model") is not False
        or sidecar.get("thread_id") != semantic_thread_id
        or sidecar.get("turn_id") != semantic_turn_id
        or sidecar.get("semantic_thread_id") != semantic_thread_id
        or sidecar.get("semantic_turn_id") != semantic_turn_id
        or sidecar.get("lineage_schema_version") != SIDECAR_LINEAGE_VERSION
        or not _record_matches(sidecar.get("runtime_lock"), frozen["runtime_lock"])
        or not _record_matches(sidecar.get("launch_receipt"), launch_path)
        or not _record_matches(sidecar.get("capacity_checkpoint"), capacity_path)
        or not _record_matches(sidecar.get("instruction_verification"), instruction_path)
        or sidecar.get("prompt_sha256") != sha256_text(frozen["turn"]["prompt"])
        or sidecar.get("prompt_bytes")
        != len(frozen["turn"]["prompt"].encode("utf-8"))
        or sidecar.get("base_instructions_sha256")
        != sha256_text(frozen["turn"]["base"])
        or sidecar.get("base_instructions_bytes")
        != len(frozen["turn"]["base"].encode("utf-8"))
        or sidecar.get("output_schema_sha256") != sha256_text(schema_json)
        or sidecar.get("output_schema_bytes")
        != len(schema_json.encode("utf-8"))
        or sidecar.get("instruction_sources_sha256")
        != preflight.get("effective_instruction_sources_sha256")
        or sidecar.get("instruction_sources_count")
        != preflight.get("effective_instruction_sources_count")
        or sidecar.get("output_sha256") != sha256_text(output_message)
        or Path(str(sidecar.get("output_path") or "")).resolve() != output_path.resolve()
        or sidecar.get("thread_total_usage") != usage
        or not isinstance(sidecar.get("stderr_sha256"), str)
        or len(sidecar.get("stderr_sha256")) != 64
        or isinstance(sidecar.get("stderr_bytes"), bool)
        or not isinstance(sidecar.get("stderr_bytes"), int)
        or sidecar.get("stderr_bytes") < 0
        or isinstance(wall, bool)
        or not isinstance(wall, (int, float))
        or wall < 0
        or values["cached_input_tokens"] > values["input_tokens"]
        or values["reasoning_output_tokens"] > values["output_tokens"]
        or values["total_tokens"]
        != values["input_tokens"] + values["output_tokens"]
    ):
        raise OperationalWaitingError("measured managed-auth sidecar contract failed")
    if (
        enforce_token_ceiling
        and values["total_tokens"] > MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING
    ):
        raise StructuralRejectionError(
            "completed turn exceeded its measured token acceptance ceiling"
        )
    return values, instruction_verification


def _terminal_sidecar_accounting(
    path: Path,
    *,
    frozen: Mapping[str, Any],
    receipt_state: str,
) -> dict[str, Any]:
    if receipt_state not in {"waiting", "structured_output_rejected"}:
        raise ExpandedCapCanaryError("invalid noncompleted sidecar receipt state")
    launch_path = frozen["root"] / "launch-receipt.json"
    capacity_path = frozen["turn"]["paths"]["capacity"]
    instruction_path = frozen["turn"]["paths"]["instruction_verification"]
    _validate_launch_receipt(launch_path, frozen)
    _validate_capacity_checkpoint(
        capacity_path,
        runtime_lock_record=_record(frozen["runtime_lock"]),
        launch_receipt_record=_record(launch_path),
        require_clear=True,
    )
    instruction = _validate_instruction_verification(
        instruction_path,
        frozen=frozen,
        launch_path=launch_path,
        capacity_path=capacity_path,
    )
    sidecar = _load_json(path, "epoch-4 terminal turn sidecar")
    schema_json = _canonical_json(frozen["turn"]["schema"])
    semantic_thread_id = instruction["semantic_thread_id"]
    semantic_turn_id = instruction["semantic_turn_id"]
    waiting_state = (
        sidecar.get("state") in {"interrupted", "failed", "cancelled"}
        and sidecar.get("status") != "completed"
        and sidecar.get("error_class") is not None
    )
    completed_operational_waiting_state = (
        sidecar.get("state") == "failed"
        and sidecar.get("status") == "completed"
        and sidecar.get("error_class")
        in {"token_usage_missing", "agent_message_missing"}
    )
    structured_output_state = (
        sidecar.get("state") == "failed"
        and sidecar.get("status") == "completed"
        and sidecar.get("error_class") == "structured_output_invalid"
    )
    if (
        not isinstance(sidecar, Mapping)
        or sidecar.get("schema_version")
        != codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
        or not _is_iso_timestamp(sidecar.get("started_at"))
        or not _is_iso_timestamp(sidecar.get("finished_at"))
        or sidecar.get("client_version")
        != codex_app_server.APP_SERVER_CLIENT_VERSION
        or sidecar.get("cli_version")
        != codex_app_server.PINNED_CODEX_CLI_VERSION
        or sidecar.get("protocol_schema_sha256")
        != _sha256_file(codex_app_server.PROTOCOL_SCHEMA_PATH)
        or sidecar.get("transport") != "stdio"
        or sidecar.get("synthetic_debug_errors") is not False
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("plan_type") != "pro"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("thread_mode") != "new_thread"
        or sidecar.get("batch_size") != 2
        or sidecar.get("recovery_reran_model") is not False
        or sidecar.get("thread_id") != semantic_thread_id
        or sidecar.get("turn_id") != semantic_turn_id
        or sidecar.get("semantic_thread_id") != semantic_thread_id
        or sidecar.get("semantic_turn_id") != semantic_turn_id
        or sidecar.get("lineage_schema_version") != SIDECAR_LINEAGE_VERSION
        or not _record_matches(sidecar.get("runtime_lock"), frozen["runtime_lock"])
        or not _record_matches(sidecar.get("launch_receipt"), launch_path)
        or not _record_matches(sidecar.get("capacity_checkpoint"), capacity_path)
        or not _record_matches(sidecar.get("instruction_verification"), instruction_path)
        or sidecar.get("prompt_sha256") != sha256_text(frozen["turn"]["prompt"])
        or sidecar.get("prompt_bytes")
        != len(frozen["turn"]["prompt"].encode("utf-8"))
        or sidecar.get("base_instructions_sha256")
        != sha256_text(frozen["turn"]["base"])
        or sidecar.get("base_instructions_bytes")
        != len(frozen["turn"]["base"].encode("utf-8"))
        or sidecar.get("output_schema_sha256") != sha256_text(schema_json)
        or sidecar.get("output_schema_bytes") != len(schema_json.encode("utf-8"))
        or sidecar.get("instruction_sources_sha256")
        != instruction.get("effective_instruction_sources_sha256")
        or sidecar.get("instruction_sources_count")
        != instruction.get("effective_instruction_sources_count")
        or Path(str(sidecar.get("output_path") or "")).resolve()
        != frozen["turn"]["paths"]["output"].resolve()
        or (
            receipt_state == "waiting"
            and not (waiting_state or completed_operational_waiting_state)
        )
        or (
            receipt_state == "structured_output_rejected"
            and not structured_output_state
        )
    ):
        raise ExpandedCapCanaryError(
            "epoch-4 noncompleted sidecar lineage integrity failed"
        )
    usage = sidecar.get("usage")
    if sidecar.get("usage_status") == "measured":
        if sidecar.get("usage_complete") is not True or not isinstance(usage, Mapping):
            raise ExpandedCapCanaryError("measured terminal usage is malformed")
        values: dict[str, int] = {}
        for field in USAGE_FIELDS:
            value = usage.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ExpandedCapCanaryError("measured terminal usage is malformed")
            values[field] = value
        if (
            sidecar.get("thread_total_usage") != usage
            or values["cached_input_tokens"] > values["input_tokens"]
            or values["reasoning_output_tokens"] > values["output_tokens"]
            or values["total_tokens"]
            != values["input_tokens"] + values["output_tokens"]
        ):
            raise ExpandedCapCanaryError("measured terminal usage is inconsistent")
        return {
            "semantic_model_call_count": 1,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": values,
        }
    if (
        sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage_complete") is not False
        or usage is not None
    ):
        raise ExpandedCapCanaryError("unknown terminal usage is malformed")
    return {
        "semantic_model_call_count": 1,
        "usage_status": "unknown",
        "accounting_complete": False,
        "usage": None,
    }


def _partial_recovery_accounting(
    root: Path, frozen: Mapping[str, Any]
) -> dict[str, Any]:
    paths = frozen["turn"]["paths"]
    launch_path = root / "launch-receipt.json"
    _validate_launch_receipt(launch_path, frozen)
    if not paths["capacity"].is_file():
        raise ExpandedCapCanaryError(
            "partial semantic artifacts have no capacity checkpoint"
        )
    _validate_capacity_checkpoint(
        paths["capacity"],
        runtime_lock_record=_record(frozen["runtime_lock"]),
        launch_receipt_record=_record(launch_path),
        require_clear=True,
    )
    instruction = None
    if paths["instruction_verification"].exists():
        if not paths["instruction_verification"].is_file():
            raise ExpandedCapCanaryError(
                "partial instruction verification is not a file"
            )
        instruction = _validate_instruction_verification(
            paths["instruction_verification"],
            frozen=frozen,
            launch_path=launch_path,
            capacity_path=paths["capacity"],
        )
    sidecar = None
    if paths["sidecar"].exists():
        if not paths["sidecar"].is_file():
            raise ExpandedCapCanaryError("partial sidecar is not a file")
        sidecar = _load_json(paths["sidecar"], "partial epoch-4 sidecar")
        schema_json = _canonical_json(frozen["turn"]["schema"])
        semantic_thread_id = sidecar.get("semantic_thread_id") if isinstance(
            sidecar, Mapping
        ) else None
        semantic_turn_id = sidecar.get("semantic_turn_id") if isinstance(
            sidecar, Mapping
        ) else None
        turn_started = bool(
            isinstance(sidecar.get("turn_id"), str)
            and sidecar.get("turn_id")
            and isinstance(semantic_turn_id, str)
            and semantic_turn_id
        )
        pre_turn = bool(
            sidecar.get("turn_id") is None
            and semantic_turn_id is None
            and instruction is None
            and sidecar.get("state") in {"started", "failed", "cancelled"}
        )
        if (
            not isinstance(sidecar, Mapping)
            or sidecar.get("schema_version")
            != codex_app_server.TURN_SIDECAR_SCHEMA_VERSION
            or sidecar.get("client_version")
            != codex_app_server.APP_SERVER_CLIENT_VERSION
            or sidecar.get("cli_version")
            != codex_app_server.PINNED_CODEX_CLI_VERSION
            or sidecar.get("protocol_schema_sha256")
            != _sha256_file(codex_app_server.PROTOCOL_SCHEMA_PATH)
            or sidecar.get("transport") != "stdio"
            or sidecar.get("synthetic_debug_errors") is not False
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("plan_type") != "pro"
            or sidecar.get("model") != MODEL
            or sidecar.get("effort") != EFFORT
            or sidecar.get("thread_mode") != "new_thread"
            or sidecar.get("batch_size") != 2
            or not isinstance(sidecar.get("thread_id"), str)
            or not sidecar.get("thread_id")
            or semantic_thread_id != sidecar.get("thread_id")
            or not (turn_started or pre_turn)
            or (turn_started and semantic_turn_id != sidecar.get("turn_id"))
            or (
                pre_turn
                and sidecar.get("state") == "failed"
                and (
                    sidecar.get("status") != "client_error"
                    or not isinstance(sidecar.get("error_class"), str)
                    or not sidecar.get("error_class")
                )
            )
            or sidecar.get("lineage_schema_version") != SIDECAR_LINEAGE_VERSION
            or not _record_matches(sidecar.get("runtime_lock"), frozen["runtime_lock"])
            or not _record_matches(sidecar.get("launch_receipt"), launch_path)
            or not _record_matches(
                sidecar.get("capacity_checkpoint"), paths["capacity"]
            )
            or sidecar.get("prompt_sha256")
            != sha256_text(frozen["turn"]["prompt"])
            or sidecar.get("prompt_bytes")
            != len(frozen["turn"]["prompt"].encode("utf-8"))
            or sidecar.get("base_instructions_sha256")
            != sha256_text(frozen["turn"]["base"])
            or sidecar.get("base_instructions_bytes")
            != len(frozen["turn"]["base"].encode("utf-8"))
            or sidecar.get("output_schema_sha256") != sha256_text(schema_json)
            or sidecar.get("output_schema_bytes") != len(schema_json.encode("utf-8"))
            or Path(str(sidecar.get("output_path") or "")).resolve()
            != paths["output"].resolve()
        ):
            raise ExpandedCapCanaryError(
                "partial epoch-4 sidecar lineage integrity failed"
            )
        if instruction is not None:
            if (
                sidecar.get("thread_id") != instruction["semantic_thread_id"]
                or sidecar.get("turn_id") != instruction["semantic_turn_id"]
                or not _record_matches(
                    sidecar.get("instruction_verification"),
                    paths["instruction_verification"],
                )
                or sidecar.get("instruction_sources_sha256")
                != instruction["effective_instruction_sources_sha256"]
                or sidecar.get("instruction_sources_count")
                != instruction["effective_instruction_sources_count"]
            ):
                raise ExpandedCapCanaryError(
                    "partial sidecar and instruction lineage disagree"
                )
        else:
            preflight = _load_json(
                frozen["preflight_path"], "epoch-4 no-model preflight"
            )
            if (
                "instruction_verification" in sidecar
                or sidecar.get("instruction_sources_sha256")
                != preflight.get("effective_instruction_sources_sha256")
                or sidecar.get("instruction_sources_count")
                != preflight.get("effective_instruction_sources_count")
            ):
                raise ExpandedCapCanaryError(
                    "partial sidecar references absent instruction verification"
                )
    if paths["output"].exists():
        if not paths["output"].is_file() or sidecar is None:
            raise ExpandedCapCanaryError("partial output has no lineage-bound sidecar")
        output_text = paths["output"].read_text(encoding="utf-8")
        output_message = output_text[:-1] if output_text.endswith("\n") else output_text
        output_sha256 = sidecar.get("output_sha256")
        nonterminal_output_commit = (
            sidecar.get("state") in {"started", "in_progress"}
            and output_sha256 is None
        )
        if (
            not nonterminal_output_commit
            and output_sha256 != sha256_text(output_message)
        ):
            raise ExpandedCapCanaryError("partial output hash does not match sidecar")
    if sidecar is None:
        return {
            "semantic_model_call_count": 1,
            "usage_status": "unknown",
            "accounting_complete": False,
            "usage": None,
        }
    usage = sidecar.get("usage")
    if (
        sidecar.get("usage_status") == "measured"
        and sidecar.get("usage_complete") is True
        and isinstance(usage, Mapping)
    ):
        values = {}
        for field in USAGE_FIELDS:
            value = usage.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ExpandedCapCanaryError("partial measured usage is malformed")
            values[field] = value
        if (
            sidecar.get("thread_total_usage") != usage
            or values["cached_input_tokens"] > values["input_tokens"]
            or values["reasoning_output_tokens"] > values["output_tokens"]
            or values["total_tokens"]
            != values["input_tokens"] + values["output_tokens"]
        ):
            raise ExpandedCapCanaryError("partial measured usage is inconsistent")
        return {
            "semantic_model_call_count": 1,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": values,
        }
    if (
        sidecar.get("usage_status") != "unknown"
        or sidecar.get("usage_complete") is not False
        or usage is not None
    ):
        nonterminal_usage_not_yet_persisted = (
            sidecar.get("state") in {"started", "in_progress"}
            and all(
                field not in sidecar
                for field in (
                    "usage",
                    "thread_total_usage",
                    "usage_status",
                    "usage_complete",
                )
            )
        )
        if not nonterminal_usage_not_yet_persisted:
            raise ExpandedCapCanaryError("partial unknown usage is malformed")
    return {
        "semantic_model_call_count": 1,
        "usage_status": "unknown",
        "accounting_complete": False,
        "usage": None,
    }


def _production_cost(total_tokens: int) -> tuple[int, float]:
    production_total = PRODUCTION_AMORTIZED_CONTEXT_TOKENS + (
        total_tokens * PRODUCTION_SCALE
    )
    return production_total, production_total / BASELINE_END_TO_END_TOKENS


def _structural_gate(
    *,
    usage: Mapping[str, int],
    normalized: Mapping[str, Any],
    diagnostics: Sequence[Mapping[str, Any]],
    applicability: Mapping[str, Any],
    instruction_verification: Mapping[str, Any],
) -> dict[str, Any]:
    rows = list(normalized.get("segments") or [])
    by_id = {str(row.get("segment_id")): row for row in diagnostics}
    event_count = sum(len(row.get("events") or []) for row in rows)
    production_total, ratio = _production_cost(int(usage["total_tokens"]))
    checks = {
        "both_declared_segments_returned_once": [
            row.get("segment_id") for row in rows
        ]
        == [v233.DENSE_SEGMENT_ID, v233.NO_SIGNAL_SEGMENT_ID],
        "complete_v249_projection_valid": True,
        "all_source_units_reviewed": set(by_id)
        == {v233.DENSE_SEGMENT_ID, v233.NO_SIGNAL_SEGMENT_ID}
        and all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_source_unit_count_0": all(
            int(row["unresolved_count"]) == 0 for row in diagnostics
        ),
        "exact_evidence_rate_1": True,
        "metric_grounding_error_event_count_0": all(
            int(row["metric_grounding_error_event_count"]) == 0
            for row in diagnostics
        ),
        "event_cap_violation_count_0": all(
            int(row["event_cap_violation_count"]) == 0
            and int(row["event_count"]) <= MAX_EVENTS_PER_SEGMENT
            for row in diagnostics
        ),
        "exact_identity_duplicate_count_reported_diagnostic_only": isinstance(
            applicability.get("exact_identity_duplicate_count"), int
        ),
        "all_emitted_events_preserved": applicability.get(
            "all_emitted_events_preserved"
        )
        is True
        and applicability.get("emitted_event_count") == event_count,
        "instruction_source_path_and_content_contract_passed": instruction_verification.get(
            "matches_no_model_preflight"
        )
        is True,
        "measured_extraction_total_tokens_lte_72891": int(usage["total_tokens"])
        <= MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING,
        "production_amortized_total_token_ratio_lte_0_28": ratio
        <= PRODUCTION_RATIO_MAX,
        "accounting_complete": True,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "failed_checks": [name for name, passed in checks.items() if not passed],
        "usage": dict(usage),
        "emitted_event_count": event_count,
        "event_count_is_semantic_acceptance_proxy": False,
        "exact_identity_duplicate_count": applicability.get(
            "exact_identity_duplicate_count"
        ),
        "exact_identity_duplicate_count_is_acceptance_gate": False,
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "full_event_ab_ba_evaluation_required": True,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _artifact_records(root: Path) -> list[dict[str, Any]]:
    turn = _turn_paths(root)
    paths = [
        root / "attempt-spec.json",
        root / "config-overlay.json",
        root / "mcp-server-name-receipt.json",
        root / "no-model-preflight.json",
        root / "runtime-lock.json",
        root / "launch-receipt.json",
        root / "structural-gate.json",
        turn["input"],
        turn["prompt"],
        turn["base"],
        turn["schema"],
        turn["direct_schema"],
        turn["capacity"],
        turn["instruction_verification"],
        turn["sidecar"],
        turn["output"],
        turn["normalized"],
        turn["provenance"],
        turn["diagnostics"],
        turn["applicability"],
    ]
    return [_record(path) for path in paths if path.is_file()]


def _best_effort_accounting(
    root: Path, frozen: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    sidecar_path = _turn_paths(root)["sidecar"]
    if not sidecar_path.is_file():
        return {
            "semantic_model_call_count": 0,
            "usage_status": "not_started",
            "accounting_complete": True,
            "usage": None,
        }
    try:
        sidecar = _load_json(sidecar_path, "best-effort epoch-4 sidecar")
    except ExpandedCapCanaryError:
        return {
            "semantic_model_call_count": 1,
            "usage_status": "unknown",
            "accounting_complete": False,
            "usage": None,
        }
    if frozen is not None and _turn_paths(root)["output"].is_file():
        try:
            values, _verification = _usage_from_sidecar(
                sidecar_path,
                output_path=_turn_paths(root)["output"],
                frozen=frozen,
                enforce_token_ceiling=False,
            )
        except ExpandedCapCanaryError:
            values = {}
        if len(values) == len(USAGE_FIELDS):
            return {
                "semantic_model_call_count": 1,
                "usage_status": "complete",
                "accounting_complete": True,
                "usage": values,
            }
    usage = sidecar.get("usage")
    if (
        sidecar.get("usage_status") == "measured"
        and sidecar.get("usage_complete") is True
        and isinstance(usage, Mapping)
        and frozen is None
    ):
        try:
            values = {field: int(usage[field]) for field in USAGE_FIELDS}
        except (KeyError, TypeError, ValueError):
            values = {}
        if len(values) == len(USAGE_FIELDS) and all(value >= 0 for value in values.values()):
            return {
                "semantic_model_call_count": 1,
                "usage_status": "complete",
                "accounting_complete": True,
                "usage": values,
            }
    return {
        "semantic_model_call_count": 1,
        "usage_status": "unknown",
        "accounting_complete": False,
        "usage": None,
    }


def _receipt(
    *,
    root: Path,
    state: str,
    terminal_reason: str,
    accounting: Mapping[str, Any],
    error: BaseException | None = None,
) -> dict[str, Any]:
    if state not in {"passed", "rejected", "waiting"}:
        raise ExpandedCapCanaryError("invalid epoch-4 receipt state")
    records = _artifact_records(root)
    gate_path = root / "structural-gate.json"
    gate = _load_json(gate_path, "epoch-4 structural gate") if gate_path.is_file() else None
    return {
        "schema_version": RECEIPT_VERSION,
        "created_at": now_iso(),
        "thread_id": THREAD_ID,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": state,
        "terminal_reason": terminal_reason,
        "error_class": type(error).__name__ if error is not None else None,
        "error_message_sha256": (
            sha256_text(str(error)) if error is not None else None
        ),
        "semantic_model_call_count": accounting.get("semantic_model_call_count", 0),
        "semantic_retry_count": 0,
        "usage_status": accounting.get("usage_status", "unknown"),
        "accounting_complete": accounting.get("accounting_complete", False),
        "usage": accounting.get("usage"),
        "measured_total_token_acceptance_ceiling": MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING,
        "transport_native_token_limit_configured": False,
        "production_amortized_total_token_ratio": (
            gate.get("production_amortized_total_token_ratio")
            if isinstance(gate, Mapping)
            else None
        ),
        "structural_gate_passed": (
            gate.get("passed") if isinstance(gate, Mapping) else False
        ),
        "full_event_ab_ba_evaluation_authorized": state == "passed",
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "records": records,
        "records_sha256": sha256_text(_canonical_json(records)),
    }


def verify_receipt(
    root: Path = DEFAULT_OUTPUT_ROOT,
    *,
    acquire_lock: bool = True,
    allow_single_mirror: bool = False,
) -> dict[str, Any]:
    def verify() -> dict[str, Any]:
        verify_runtime_lock(root / "runtime-lock.json", acquire_lock=False)
        frozen = _load_frozen(root)
        receipt_path = root / "plan-step-receipt.json"
        terminal_path = root / "terminal.json"
        if allow_single_mirror and receipt_path.is_file() != terminal_path.is_file():
            existing_path = receipt_path if receipt_path.is_file() else terminal_path
            existing = _load_json(existing_path, "single epoch-4 terminal mirror")
            receipt = existing
            terminal = existing
        else:
            receipt = _load_json(receipt_path, "epoch-4 receipt")
            terminal = _load_json(terminal_path, "epoch-4 terminal")
        records = receipt.get("records")
        record_paths = {
            str(row.get("path"))
            for row in records or []
            if isinstance(row, Mapping)
        }
        expected_records = _artifact_records(root)
        turn = _turn_paths(root)
        required_static = {
            str((root / "attempt-spec.json").resolve()),
            str((root / "config-overlay.json").resolve()),
            str((root / "mcp-server-name-receipt.json").resolve()),
            str((root / "no-model-preflight.json").resolve()),
            str((root / "runtime-lock.json").resolve()),
            str(turn["input"].resolve()),
            str(turn["prompt"].resolve()),
            str(turn["base"].resolve()),
            str(turn["schema"].resolve()),
            str(turn["direct_schema"].resolve()),
        }
        call_count = receipt.get("semantic_model_call_count")
        if (
            receipt != terminal
            or receipt.get("schema_version") != RECEIPT_VERSION
            or receipt.get("thread_id") != THREAD_ID
            or receipt.get("plan_epoch") != PLAN_EPOCH
            or receipt.get("step_id") != STEP_ID
            or receipt.get("state") not in {"passed", "rejected", "waiting"}
            or call_count not in {0, 1}
            or receipt.get("semantic_retry_count") != 0
            or receipt.get("measured_total_token_acceptance_ceiling")
            != MEASURED_TOTAL_TOKEN_ACCEPTANCE_CEILING
            or receipt.get("transport_native_token_limit_configured") is not False
            or receipt.get("development_winner_frozen") is not False
            or receipt.get("holdout_authorized") is not False
            or receipt.get("production_mutated") is not False
            or not isinstance(records, list)
            or records != expected_records
            or not required_static.issubset(record_paths)
            or any(not isinstance(row, Mapping) or not _verify_record(row) for row in records)
            or receipt.get("records_sha256") != sha256_text(_canonical_json(records))
        ):
            raise ExpandedCapCanaryError("epoch-4 receipt integrity failed")
        if call_count == 1:
            required_lineage = {
                str((root / "launch-receipt.json").resolve()),
                str(turn["capacity"].resolve()),
            }
            if not required_lineage.issubset(record_paths):
                raise ExpandedCapCanaryError("epoch-4 turn artifacts are incomplete")
            _validate_launch_receipt(root / "launch-receipt.json", frozen)
            _validate_capacity_checkpoint(
                turn["capacity"],
                runtime_lock_record=_record(frozen["runtime_lock"]),
                launch_receipt_record=_record(root / "launch-receipt.json"),
                require_clear=True,
            )
            terminal_reason = receipt.get("terminal_reason")
            completed_output_state = bool(
                receipt.get("state") == "passed"
                or terminal_reason
                == "epoch4_expanded_cap_exhaustive_full_schema_structural_or_cost_rejected"
            )
            if completed_output_state:
                required_completed = {
                    str(turn["instruction_verification"].resolve()),
                    str(turn["sidecar"].resolve()),
                    str(turn["output"].resolve()),
                }
                if not required_completed.issubset(record_paths):
                    raise ExpandedCapCanaryError(
                        "completed epoch-4 turn artifacts are incomplete"
                    )
                validated_usage, _instruction_verification = _usage_from_sidecar(
                    turn["sidecar"],
                    output_path=turn["output"],
                    frozen=frozen,
                    enforce_token_ceiling=False,
                )
                validated_accounting = {
                    "semantic_model_call_count": 1,
                    "usage_status": "complete",
                    "accounting_complete": True,
                    "usage": validated_usage,
                }
            elif (
                receipt.get("state") == "rejected"
                and terminal_reason
                == "epoch4_expanded_cap_exhaustive_full_schema_completed_output_rejected"
            ):
                required_terminal = {
                    str(turn["instruction_verification"].resolve()),
                    str(turn["sidecar"].resolve()),
                }
                if (
                    not required_terminal.issubset(record_paths)
                    or turn["output"].exists()
                ):
                    raise ExpandedCapCanaryError(
                        "structured-output rejection artifacts are invalid"
                    )
                validated_accounting = _terminal_sidecar_accounting(
                    turn["sidecar"],
                    frozen=frozen,
                    receipt_state="structured_output_rejected",
                )
            elif receipt.get("state") == "waiting":
                if terminal_reason == (
                    "epoch4_partial_semantic_artifacts_require_no_retry_recovery"
                ):
                    validated_accounting = _partial_recovery_accounting(root, frozen)
                else:
                    required_terminal = {
                        str(turn["instruction_verification"].resolve()),
                        str(turn["sidecar"].resolve()),
                    }
                    if (
                        not required_terminal.issubset(record_paths)
                        or turn["output"].exists()
                    ):
                        raise ExpandedCapCanaryError(
                            "waiting terminal turn artifacts are invalid"
                        )
                    validated_accounting = _terminal_sidecar_accounting(
                        turn["sidecar"], frozen=frozen, receipt_state="waiting"
                    )
            else:
                raise ExpandedCapCanaryError(
                    "call-count one receipt has an invalid terminal class"
                )
            if (
                receipt.get("usage_status")
                != validated_accounting["usage_status"]
                or receipt.get("accounting_complete")
                != validated_accounting["accounting_complete"]
                or receipt.get("usage") != validated_accounting["usage"]
            ):
                raise ExpandedCapCanaryError(
                    "epoch-4 receipt accounting lineage failed"
                )
        else:
            launch_path = root / "launch-receipt.json"
            if launch_path.exists():
                if not launch_path.is_file():
                    raise ExpandedCapCanaryError("launch receipt is not a file")
                _validate_launch_receipt(launch_path, frozen)
            if turn["capacity"].exists():
                if not launch_path.is_file() or not turn["capacity"].is_file():
                    raise ExpandedCapCanaryError(
                        "capacity checkpoint has no valid launch"
                    )
                _validate_capacity_checkpoint(
                    turn["capacity"],
                    runtime_lock_record=_record(frozen["runtime_lock"]),
                    launch_receipt_record=_record(launch_path),
                    require_clear=False,
                )
            if any(
                path.exists()
                for path in (
                    turn["instruction_verification"],
                    turn["sidecar"],
                    turn["output"],
                )
            ):
                raise ExpandedCapCanaryError(
                    "zero-call receipt contains semantic turn artifacts"
                )
        if receipt.get("state") == "passed":
            gate = _load_json(root / "structural-gate.json", "epoch-4 structural gate")
            if (
                receipt.get("accounting_complete") is not True
                or receipt.get("usage_status") != "complete"
                or receipt.get("structural_gate_passed") is not True
                or receipt.get("full_event_ab_ba_evaluation_authorized") is not True
                or gate.get("passed") is not True
                or gate.get("development_winner_frozen") is not False
                or gate.get("holdout_authorized") is not False
                or gate.get("production_mutated") is not False
            ):
                raise ExpandedCapCanaryError("epoch-4 passed receipt is invalid")
        elif receipt.get("full_event_ab_ba_evaluation_authorized") is not False:
            raise ExpandedCapCanaryError("nonpassing epoch-4 receipt authorized evaluation")
        return receipt

    if acquire_lock:
        with _advisory_process_lock(root.resolve()):
            return verify()
    return verify()


def _finalize_completed_turn(root: Path, frozen: Mapping[str, Any]) -> dict[str, Any]:
    paths = frozen["turn"]["paths"]
    usage, instruction_verification = _usage_from_sidecar(
        paths["sidecar"], output_path=paths["output"], frozen=frozen
    )
    output = _load_json(paths["output"], "epoch-4 structured output")
    normalized, provenance, diagnostics, applicability = project_output(
        output, frozen["turn"]
    )
    _write_immutable_json(paths["normalized"], normalized)
    _write_immutable_json(paths["provenance"], provenance)
    _write_immutable_json(paths["diagnostics"], {"segments": diagnostics})
    _write_immutable_json(paths["applicability"], applicability)
    gate = _structural_gate(
        usage=usage,
        normalized=normalized,
        diagnostics=diagnostics,
        applicability=applicability,
        instruction_verification=instruction_verification,
    )
    _write_immutable_json(root / "structural-gate.json", gate)
    if gate["passed"] is not True:
        raise StructuralRejectionError("epoch-4 structural or cost gate failed")
    accounting = {
        "semantic_model_call_count": 1,
        "usage_status": "complete",
        "accounting_complete": True,
        "usage": usage,
    }
    return _receipt(
        root=root,
        state="passed",
        terminal_reason="epoch4_expanded_cap_exhaustive_full_schema_structural_gate_passed",
        accounting=accounting,
    )


async def _run_unlocked(
    root: Path,
    *,
    client_factory: Callable[[Mapping[str, Any]], Any],
    capacity_probe: Callable[..., Awaitable[dict[str, Any]]],
    timeout_seconds: float,
) -> dict[str, Any]:
    receipt_path = root / "plan-step-receipt.json"
    terminal_path = root / "terminal.json"
    if receipt_path.exists() or terminal_path.exists():
        if receipt_path.is_file() != terminal_path.is_file():
            existing = verify_receipt(
                root,
                acquire_lock=False,
                allow_single_mirror=True,
            )
            missing_path = terminal_path if receipt_path.is_file() else receipt_path
            _write_immutable_json(missing_path, existing)
        elif not receipt_path.is_file() or not terminal_path.is_file():
            raise ExpandedCapCanaryError("partial epoch-4 terminal integrity failure")
        return verify_receipt(root, acquire_lock=False)
    frozen = _freeze_unlocked(root)
    verify_runtime_lock(frozen["runtime_lock"], acquire_lock=False)
    launch_path = root / "launch-receipt.json"
    paths = frozen["turn"]["paths"]
    if launch_path.exists():
        _validate_launch_receipt(launch_path, frozen)
        if paths["capacity"].exists():
            if not paths["capacity"].is_file():
                raise ExpandedCapCanaryError("capacity checkpoint is not a file")
            _validate_capacity_checkpoint(
                paths["capacity"],
                runtime_lock_record=_record(frozen["runtime_lock"]),
                launch_receipt_record=_record(launch_path),
                require_clear=False,
            )
        semantic_artifacts = (
            paths["sidecar"],
            paths["output"],
            paths["instruction_verification"],
        )
        if any(path.exists() for path in semantic_artifacts):
            if all(path.is_file() for path in semantic_artifacts):
                try:
                    receipt = _finalize_completed_turn(root, frozen)
                except StructuralRejectionError as exc:
                    receipt = _receipt(
                        root=root,
                        state="rejected",
                        terminal_reason=(
                            "epoch4_expanded_cap_exhaustive_full_schema_"
                            "structural_or_cost_rejected"
                        ),
                        accounting=_best_effort_accounting(root, frozen),
                        error=exc,
                    )
                except OperationalWaitingError as exc:
                    receipt = _receipt(
                        root=root,
                        state="waiting",
                        terminal_reason=(
                            "epoch4_partial_semantic_artifacts_require_no_retry_recovery"
                        ),
                        accounting=_partial_recovery_accounting(root, frozen),
                        error=exc,
                    )
            elif (
                paths["sidecar"].is_file()
                and paths["instruction_verification"].is_file()
                and not paths["output"].exists()
            ):
                sidecar = _load_json(
                    paths["sidecar"], "existing terminal epoch-4 sidecar"
                )
                structured_output_rejected = bool(
                    sidecar.get("state") == "failed"
                    and sidecar.get("status") == "completed"
                    and sidecar.get("error_class") == "structured_output_invalid"
                )
                terminal_kind = (
                    "structured_output_rejected"
                    if structured_output_rejected
                    else "waiting"
                )
                receipt = _receipt(
                    root=root,
                    state="rejected" if structured_output_rejected else "waiting",
                    terminal_reason=(
                        "epoch4_expanded_cap_exhaustive_full_schema_"
                        "completed_output_rejected"
                        if structured_output_rejected
                        else "epoch4_existing_terminal_turn_requires_no_retry_recovery"
                    ),
                    accounting=_terminal_sidecar_accounting(
                        paths["sidecar"],
                        frozen=frozen,
                        receipt_state=terminal_kind,
                    ),
                )
            else:
                receipt = _receipt(
                    root=root,
                    state="waiting",
                    terminal_reason=(
                        "epoch4_partial_semantic_artifacts_require_no_retry_recovery"
                    ),
                    accounting=_partial_recovery_accounting(root, frozen),
                )
        else:
            receipt = _receipt(
                root=root,
                state="waiting",
                terminal_reason="epoch4_existing_launch_requires_no_retry_recovery",
                accounting=_best_effort_accounting(root, frozen),
            )
        _write_immutable_json(receipt_path, receipt)
        _write_immutable_json(terminal_path, receipt)
        return verify_receipt(root, acquire_lock=False)
    _write_immutable_json(launch_path, _launch_receipt_payload(frozen))
    _validate_launch_receipt(launch_path, frozen)
    try:
        async with client_factory(frozen["turn"]["config_overlay"]) as client:
            await capacity_probe(
                client,
                checkpoint_path=paths["capacity"],
                runtime_lock_record=_record(frozen["runtime_lock"]),
                launch_receipt_record=_record(launch_path),
            )
            _validate_capacity_checkpoint(
                paths["capacity"],
                runtime_lock_record=_record(frozen["runtime_lock"]),
                launch_receipt_record=_record(launch_path),
                require_clear=True,
            )
            bind_turn_lineage = getattr(client, "bind_turn_lineage", None)
            if not callable(bind_turn_lineage):
                raise ExpandedCapCanaryError(
                    "app-server client does not expose sidecar lineage binding"
                )
            bind_turn_lineage(
                runtime_lock=_record(frozen["runtime_lock"]),
                launch_receipt=_record(launch_path),
                capacity_checkpoint=_record(paths["capacity"]),
            )
            account = getattr(client, "account_summary", None)
            if (
                not isinstance(account, Mapping)
                or account.get("type") != "chatgpt"
                or account.get("plan_type") != "pro"
            ):
                raise OperationalWaitingError("managed ChatGPT Pro auth is unavailable")
            thread = await client.start_thread(
                model=MODEL,
                base_instructions=frozen["turn"]["base"],
                cwd=PROJECT_ROOT,
                ephemeral=True,
            )
            if thread.model != MODEL or thread.ephemeral is not True:
                raise OperationalWaitingError("app-server thread contract drifted")
            instruction_verification = _semantic_instruction_verification(
                client=client, thread=thread, frozen=frozen
            )
            bind_instruction_verification = getattr(
                client, "bind_instruction_verification", None
            )
            if not callable(bind_instruction_verification):
                raise ExpandedCapCanaryError(
                    "app-server client does not expose instruction lineage binding"
                )
            bind_instruction_verification(
                verification=instruction_verification,
                path=paths["instruction_verification"],
            )
            result = await client.run_structured_turn(
                thread=thread,
                effort=EFFORT,
                prompt=frozen["turn"]["prompt"],
                output_schema=frozen["turn"]["schema"],
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
            )
            if result.status_ok is not True or not isinstance(result.output, Mapping):
                raise OperationalWaitingError("epoch-4 semantic turn did not complete")
            if getattr(result, "thread_id", None) != thread.thread_id:
                raise OperationalWaitingError("semantic turn escaped the pinned thread")
            if not isinstance(getattr(result, "turn_id", None), str) or not result.turn_id:
                raise OperationalWaitingError("semantic turn id is unavailable")
            _bind_completed_semantic_lineage(
                frozen=frozen,
                verification=instruction_verification,
                result=result,
            )
        receipt = _finalize_completed_turn(root, frozen)
    except StructuralRejectionError as exc:
        receipt = _receipt(
            root=root,
            state="rejected",
            terminal_reason=(
                "epoch4_expanded_cap_exhaustive_full_schema_structural_or_cost_rejected"
            ),
            accounting=_best_effort_accounting(root, frozen),
            error=exc,
        )
    except codex_app_server.AppServerStructuredOutputError as exc:
        structured_accounting = (
            _terminal_sidecar_accounting(
                paths["sidecar"],
                frozen=frozen,
                receipt_state="structured_output_rejected",
            )
            if paths["sidecar"].is_file()
            and paths["instruction_verification"].is_file()
            else _best_effort_accounting(root, frozen)
        )
        receipt = _receipt(
            root=root,
            state="rejected",
            terminal_reason=(
                "epoch4_expanded_cap_exhaustive_full_schema_completed_output_rejected"
            ),
            accounting=structured_accounting,
            error=exc,
        )
    except (
        OperationalWaitingError,
        capacity.AppServerCapacityError,
        codex_app_server.AppServerAuthError,
        codex_app_server.AppServerProcessDied,
        codex_app_server.AppServerRPCError,
        codex_app_server.AppServerProtocolError,
        codex_app_server.AppServerRecoveryRequired,
        codex_app_server.AppServerTurnTimeout,
        asyncio.TimeoutError,
        OSError,
    ) as exc:
        partial_waiting = bool(
            paths["sidecar"].is_file()
            and not paths["instruction_verification"].is_file()
        )
        if partial_waiting:
            waiting_accounting = _partial_recovery_accounting(root, frozen)
            terminal_reason = (
                "epoch4_partial_semantic_artifacts_require_no_retry_recovery"
            )
        elif (
            paths["sidecar"].is_file()
            and paths["instruction_verification"].is_file()
        ):
            waiting_accounting = _terminal_sidecar_accounting(
                paths["sidecar"], frozen=frozen, receipt_state="waiting"
            )
            terminal_reason = (
                "epoch4_expanded_cap_exhaustive_full_schema_operational_waiting"
            )
        else:
            waiting_accounting = _best_effort_accounting(root, frozen)
            terminal_reason = (
                "epoch4_expanded_cap_exhaustive_full_schema_operational_waiting"
            )
        receipt = _receipt(
            root=root,
            state="waiting",
            terminal_reason=terminal_reason,
            accounting=waiting_accounting,
            error=exc,
        )
    _write_immutable_json(receipt_path, receipt)
    _write_immutable_json(terminal_path, receipt)
    return verify_receipt(root, acquire_lock=False)


async def run(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Mapping[str, Any]], Any] = _client_factory,
    capacity_probe: Callable[..., Awaitable[dict[str, Any]]] = _probe_capacity,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _advisory_process_lock(root):
        return await _run_unlocked(
            root,
            client_factory=client_factory,
            capacity_probe=capacity_probe,
            timeout_seconds=timeout_seconds,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the epoch-4 expanded-cap exhaustive full-schema canary"
    )
    parser.add_argument(
        "action",
        choices=("prepare", "preflight", "freeze", "run", "verify-runtime", "verify"),
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.action == "prepare":
        prepared = prepare_run(args.output_dir)
        result = {
            "state": "prepared",
            "preflight_path": str(prepared["preflight_path"]),
            "semantic_model_call_count": 0,
        }
    elif args.action == "preflight":
        preflight = asyncio.run(run_no_model_preflight(args.output_dir))
        result = {
            "state": preflight["state"],
            "semantic_model_call_count": 0,
            "effective_instruction_sources_count": preflight[
                "effective_instruction_sources_count"
            ],
        }
    elif args.action == "freeze":
        frozen = freeze_run(args.output_dir)
        result = {
            "state": "frozen",
            "runtime_lock": str(frozen["runtime_lock"]),
            "semantic_model_call_count": 0,
        }
    elif args.action == "verify-runtime":
        verify_runtime_lock(args.output_dir / "runtime-lock.json")
        result = {"state": "runtime_lock_verified", "semantic_model_call_count": 0}
    elif args.action == "verify":
        receipt = verify_receipt(args.output_dir)
        result = {"state": receipt["state"], "verified": True}
    else:
        receipt = asyncio.run(
            run(
                output_dir=args.output_dir,
                timeout_seconds=args.timeout_seconds,
            )
        )
        result = {
            "state": receipt["state"],
            "terminal_reason": receipt["terminal_reason"],
            "semantic_model_call_count": receipt["semantic_model_call_count"],
        }
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
