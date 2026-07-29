from __future__ import annotations

"""Run one losslessly vectorized Sol support-only selector canary."""

import argparse
import asyncio
import copy
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_selection_v216_spark_support_canary as v216
from . import app_server_judge_v5_selection_v217_spark_support_score as v217
from .app_server_capacity_reserve import load_reserve_capacity_policy
from .app_server_judge_v5_calibration_v25_diagnostic import (
    PINNED_CODEX_0_144_1,
    QUOTA_POINTS_PER_MILLION_TOKENS,
)
from .app_server_judge_v5_calibration_v26_diagnostic import (
    JudgeV5CalibrationV26DiagnosticAttemptFailed,
    _aggregate_usage,
    _client_factory,
    _freeze_turn_request,
    _get_or_run_turn,
    _load_json,
    _write_immutable,
)
from .app_server_judge_v5_calibration_v86_fresh_enhanced_diagnostic import (
    _record,
    _verify_record,
)
from .app_server_judge_v5_diagnostic import (
    USAGE_FIELDS,
    _attempt_records,
    _validate_usage,
)
from .labels import ValidationError, _validate_schema
from .util import now_iso


V218_MODEL_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v218_model_audit_v1"
V218_PROJECTION_VERSION = "pif_app_server_judge_v5_4_selection_v218_projection_v1"
V218_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V218_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V218_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v218_spec_v1"
V218_RUNTIME_LOCK_VERSION = (
    "pif_app_server_judge_v5_4_selection_v218_runtime_lock_v1"
)
V218_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v218_launch_v1"
V218_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v218_failure_v1"
V218_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v218_gate_v1"
V218_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v218_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v218_compact_support_canary"
TURN_NAME = "selection_sol_compact_support_only_canary_00"
DEFAULT_OUTPUT_ROOT = (
    v217.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v218-compact-support-canary"
).resolve()

MODEL = "gpt-5.6-sol"
EFFORT = "low"
TIMEOUT_SECONDS = v216.TIMEOUT_SECONDS
PROMOTION_TOTAL_TOKEN_GATE = v216.PROMOTION_TOTAL_TOKEN_GATE
RUNTIME_TOTAL_TOKEN_BOUND = 50_000
MAX_PROMPT_BYTES = 45_000
MAX_INSTRUCTIONS_BYTES = 4_096
MAX_SCHEMA_BYTES = 4_096


class JudgeV5SelectionV218Error(RuntimeError):
    """The v218 compact support canary cannot preserve its contract."""


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _attempt_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _v217_paths() -> dict[str, Path]:
    root = v217.DEFAULT_OUTPUT_ROOT
    return {
        "spec": root / "attempt-spec.json",
        "runtime_lock": root / "runtime-lock.json",
        "audit": root / "score-recovery-audit.json",
        "gate": root / "spark-support-score-gate.json",
        "score": root / "spark-support-score.private.json",
        "output": root / "support-positive-selector-output.private.json",
        "terminal": root / "terminal.json",
    }


def _validate_v217_checkpoint() -> dict[str, Any]:
    root = v217.DEFAULT_OUTPUT_ROOT
    paths = _v217_paths()
    expected = {path.resolve() for path in paths.values()}
    actual = {path.resolve() for path in root.rglob("*") if path.is_file()}
    if actual != expected:
        raise JudgeV5SelectionV218Error("v217 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV218Error("v217 immutable artifact drifted")
    v217.verify_runtime_lock(paths["runtime_lock"])
    terminal = _load_json(paths["terminal"], "v217 terminal")
    gate = _load_json(paths["gate"], "v217 gate")
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason")
        != "v217_semantic_quality_passed_selector_cost_not_passed"
        or terminal.get("semantic_quality_gate_passed") is not True
        or terminal.get("selector_cost_gate_passed") is not False
        or terminal.get("new_semantic_model_calls") != 0
        or terminal.get("cumulative_known_usage_lower_bound", {}).get(
            "total_tokens"
        )
        != 9_053_190
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or gate.get("failed_checks")
        != ["actual_selector_total_tokens_lte_35000"]
        or gate.get("candidate_mean_f1") != 0.803978
        or gate.get("maximum_dense_case_regret_to_oracle") != 0.124286
    ):
        raise JudgeV5SelectionV218Error("v217 terminal contract drifted")
    v216_checkpoint = v217._validate_v216_checkpoint()
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "gate": gate,
        "v216": v216_checkpoint,
        "scoring_predecessor": v216_checkpoint["request"][
            "scoring_predecessor"
        ],
    }


def selector_instructions() -> str:
    return """
You are a side-free source-grounded support verifier. The packet contains complete
source text and opaque event rows from hidden extraction passes. You do not know
system identity, reference labels, scores, or density.

Packet format: f[i] names field i. If t[i] is an array, an integer in event column i
indexes that table; otherwise the value is literal. Null means the field was omitted.
c[k] is [annotated_source,event_rows]. Event row n is associated with marker number
n in that case. {+1,3} opens exact evidence for rows 1 and 3 and {-1,3} closes it;
removing markers recovers the complete source exactly. Markers prove boundaries only.

Evaluate every nonnull field: claim, actor, attribution, causal mechanism, certainty,
event type, metric, negation or counterclaim, reported actor, speaker, stance, target,
and temporal horizon. Role and event-type labels must characterize the source but need
not appear literally. Supported paraphrase and coreference pass. Exact wording alone
does not make an unsupported inference pass.

Return s only when every listed proposition is source-supported, u for a direct
material conflict or ungrounded assertion, and a only when the source cannot resolve
support. Judge rows independently. Do not compare, merge, or deduplicate events. Do
not rewrite, add, or infer events. Preserve case and row order and return one verdict
per row in v. Output only the structured object, without rationale or confidence.
""".strip()


def _json_size(value: Any) -> int:
    return len(
        json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    )


def _same_value(left: Any, right: Any) -> bool:
    return type(left) is type(right) and left == right


def _exact_index(values: Sequence[Any], target: Any) -> int:
    for index, value in enumerate(values):
        if _same_value(value, target):
            return index
    raise JudgeV5SelectionV218Error("value-table projection drifted")


def _compact_markers(value: str, event_count: int) -> str:
    raw = v216.v211.strip_selector_markers(value)
    if "{+" in raw or "{-" in raw:
        raise JudgeV5SelectionV218Error("source collides with compact markers")
    parts: list[str] = []
    cursor = 0
    while cursor < len(value):
        if value.startswith("<<+", cursor) or value.startswith("<<-", cursor):
            sign = value[cursor + 2]
            end = value.find(">>", cursor)
            if end < 0:
                raise JudgeV5SelectionV218Error("source marker is unterminated")
            payload = value[cursor + 3 : end]
            ids = payload.split(",") if payload else []
            if not ids or any(
                len(event_id) != 4
                or event_id[0] != "e"
                or not event_id[1:].isdigit()
                or int(event_id[1:]) < 1
                or int(event_id[1:]) > event_count
                for event_id in ids
            ):
                raise JudgeV5SelectionV218Error("source marker IDs drifted")
            numbers = ",".join(str(int(event_id[1:])) for event_id in ids)
            parts.append("{" + sign + numbers + "}")
            cursor = end + 2
            continue
        parts.append(value[cursor])
        cursor += 1
    compact = "".join(parts)
    if _expand_markers(compact, event_count) != value:
        raise JudgeV5SelectionV218Error("compact marker projection is not reversible")
    return compact


def _expand_markers(value: str, event_count: int) -> str:
    parts: list[str] = []
    cursor = 0
    while cursor < len(value):
        if value.startswith("{+", cursor) or value.startswith("{-", cursor):
            sign = value[cursor + 1]
            end = value.find("}", cursor)
            if end < 0:
                raise JudgeV5SelectionV218Error("compact marker is unterminated")
            payload = value[cursor + 2 : end]
            numbers = payload.split(",") if payload else []
            if not numbers or any(
                not number.isdigit()
                or int(number) < 1
                or int(number) > event_count
                for number in numbers
            ):
                raise JudgeV5SelectionV218Error("compact marker IDs drifted")
            ids = ",".join(f"e{int(number):03d}" for number in numbers)
            parts.append(f"<<{sign}{ids}>>")
            cursor = end + 1
            continue
        parts.append(value[cursor])
        cursor += 1
    return "".join(parts)


def _vector_packet(
    original: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    legend = {
        str(alias): str(name)
        for alias, name in (original.get("event_field_legend") or {}).items()
    }
    aliases = list(legend)
    cases = list(original.get("cases") or [])
    events = [
        event
        for case in cases
        for event in (case.get("candidate_events") or [])
    ]
    if len(cases) != 4 or len(events) != 79:
        raise JudgeV5SelectionV218Error("vector source coverage drifted")
    if any(
        value is None
        for event in events
        for value in (event.get("values") or {}).values()
    ):
        raise JudgeV5SelectionV218Error("present null field cannot be vectorized")

    tables_by_alias: dict[str, list[Any]] = {}
    for alias in aliases:
        values = [
            event["values"][alias]
            for event in events
            if alias in event.get("values", {})
        ]
        unique: list[Any] = []
        for value in values:
            if not any(_same_value(value, known) for known in unique):
                unique.append(copy.deepcopy(value))
        inline_size = 4 + sum(_json_size(value) for value in values)
        coded_size = _json_size(unique) + sum(
            len(str(_exact_index(unique, value))) for value in values
        )
        if coded_size < inline_size:
            tables_by_alias[alias] = unique

    packet_cases = []
    identity_cases = []
    for case in cases:
        case_events = list(case.get("candidate_events") or [])
        vectors = []
        for event in case_events:
            vector = []
            values = event.get("values") or {}
            for alias in aliases:
                if alias not in values:
                    vector.append(None)
                elif alias in tables_by_alias:
                    vector.append(
                        _exact_index(tables_by_alias[alias], values[alias])
                    )
                else:
                    vector.append(copy.deepcopy(values[alias]))
            vectors.append(vector)
        packet_cases.append(
            [
                _compact_markers(
                    str(case["annotated_source"]),
                    len(case_events),
                ),
                vectors,
            ]
        )
        identity_cases.append(
            {
                "case_id": str(case["case_id"]),
                "event_ids": [str(event["event_id"]) for event in case_events],
            }
        )

    tables = [tables_by_alias.get(alias) for alias in aliases]
    packet = {
        "f": [legend[alias] for alias in aliases],
        "t": tables,
        "c": packet_cases,
    }
    identity = {
        "field_aliases": aliases,
        "cases": identity_cases,
    }

    reconstructed_cases = []
    for case_index, packet_case in enumerate(packet_cases):
        source, vectors = packet_case
        source_case = cases[case_index]
        reconstructed_events = []
        for event_index, vector in enumerate(vectors):
            values = {}
            for field_index, encoded in enumerate(vector):
                if encoded is None:
                    continue
                alias = aliases[field_index]
                table = tables[field_index]
                values[alias] = (
                    copy.deepcopy(table[int(encoded)])
                    if table is not None
                    else copy.deepcopy(encoded)
                )
            reconstructed_events.append(
                {
                    "event_id": identity_cases[case_index]["event_ids"][event_index],
                    "values": values,
                }
            )
        reconstructed_cases.append(
            {
                "case_id": identity_cases[case_index]["case_id"],
                "annotated_source": _expand_markers(
                    str(source),
                    len(vectors),
                ),
                "candidate_events": reconstructed_events,
            }
        )
        if reconstructed_cases[-1] != source_case:
            raise JudgeV5SelectionV218Error("vector projection is not reversible")

    audit = {
        "schema_version": V218_PROJECTION_VERSION,
        "field_count": len(aliases),
        "table_coded_field_count": len(tables_by_alias),
        "table_coded_fields": [
            legend[alias] for alias in aliases if alias in tables_by_alias
        ],
        "case_count": len(packet_cases),
        "candidate_event_count": len(events),
        "case_event_counts": [len(case[1]) for case in packet_cases],
        "losslessly_reversible": reconstructed_cases == cases,
        "complete_source_preserved": all(
            v216.v211.strip_selector_markers(
                _expand_markers(str(new[0]), len(new[1]))
            )
            == v216.v211.strip_selector_markers(str(old["annotated_source"]))
            for old, new in zip(cases, packet_cases, strict=True)
        ),
        "case_order_preserved": True,
        "event_order_and_coverage_preserved": True,
        "identity_projection_is_structural_only": True,
        "semantic_rewrite_performed": False,
        "semantic_filtering_performed": False,
        "semantic_deduplication_performed": False,
        "packet_bytes": _json_size(packet),
    }
    if not all(
        audit[key]
        for key in (
            "losslessly_reversible",
            "complete_source_preserved",
            "case_order_preserved",
            "event_order_and_coverage_preserved",
        )
    ):
        raise JudgeV5SelectionV218Error("vector packet audit drifted")
    return packet, audit, identity


def _selector_prompt(packet: Mapping[str, Any]) -> str:
    return "# Compact support packet\n" + json.dumps(
        packet,
        ensure_ascii=True,
        separators=(",", ":"),
    )


def _support_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["v"],
        "properties": {
            "v": {
                "type": "array",
                "minItems": 4,
                "maxItems": 4,
                "items": {
                    "type": "array",
                    "maxItems": v216.v211.MAX_EVENTS_PER_CASE,
                    "items": {
                        "type": "string",
                        "enum": ["s", "u", "a"],
                    },
                },
            }
        },
    }


def validate_support_output(
    output: Any,
    packet: Mapping[str, Any],
    schema: Mapping[str, Any],
) -> list[str]:
    if not isinstance(output, dict):
        return ["output_not_object"]
    try:
        _validate_schema(dict(schema), output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        return [f"schema:{type(exc).__name__}"]
    expected_counts = [len(case[1]) for case in packet["c"]]
    actual = output.get("v") or []
    if len(actual) != len(expected_counts):
        return ["case_order_or_coverage"]
    if [len(row) for row in actual] != expected_counts:
        return ["event_order_or_coverage"]
    return []


def _project_support_output(
    output: Mapping[str, Any],
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    verdict_map = {"s": "keep", "u": "unsupported", "a": "abstain"}
    cases = []
    for verdicts, identity_case in zip(
        output["v"],
        identity["cases"],
        strict=True,
    ):
        decisions = []
        for verdict_code, event_id in zip(
            verdicts,
            identity_case["event_ids"],
            strict=True,
        ):
            verdict = verdict_map[str(verdict_code)]
            decisions.append(
                {
                    "event_id": str(event_id),
                    "verdict": verdict,
                    "canonical_event_id": (
                        str(event_id) if verdict == "keep" else ""
                    ),
                }
            )
        cases.append(
            {
                "case_id": str(identity_case["case_id"]),
                "decisions": decisions,
            }
        )
    return {"cases": cases}


def _build_request(checkpoint: Mapping[str, Any]) -> dict[str, Any]:
    original = checkpoint["scoring_predecessor"]["input"]
    packet, projection, identity = _vector_packet(original)
    instructions = selector_instructions()
    prompt = _selector_prompt(packet)
    schema = _support_schema()
    prompt_bytes = len(prompt.encode("utf-8"))
    instruction_bytes = len(instructions.encode("utf-8"))
    schema_transport = json.dumps(
        schema,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    schema_bytes = len(schema_transport.encode("utf-8"))
    if (
        prompt_bytes > MAX_PROMPT_BYTES
        or instruction_bytes > MAX_INSTRUCTIONS_BYTES
        or schema_bytes > MAX_SCHEMA_BYTES
        or v216.llm_judge_module.validate_app_server_output_schema_subset(schema)
    ):
        raise JudgeV5SelectionV218Error("v218 request exceeds frozen bounds")
    return {
        "packet": packet,
        "projection": projection,
        "identity": identity,
        "instructions": instructions,
        "prompt": prompt,
        "schema": schema,
        "prompt_bytes": prompt_bytes,
        "instruction_bytes": instruction_bytes,
        "schema_bytes": schema_bytes,
        "scoring_predecessor": checkpoint["scoring_predecessor"],
    }


def _build_capacity_policy(
    root: Path,
    checkpoint: Mapping[str, Any],
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        RUNTIME_TOTAL_TOKEN_BOUND
        * QUOTA_POINTS_PER_MILLION_TOKENS
        / 1_000_000
    )
    terminal = checkpoint["terminal"]
    audit = {
        "schema_version": V218_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v217_terminal": checkpoint["records"]["terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": terminal[
                "cumulative_known_usage_lower_bound"
            ]["total_tokens"],
            "predecessor_unknown_usage_turn_count": terminal[
                "cumulative_unknown_usage_turn_count"
            ],
            "predecessor_unknown_usage_upper_bound": terminal[
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": RUNTIME_TOTAL_TOKEN_BOUND,
            "phase_total_token_bound": RUNTIME_TOTAL_TOKEN_BOUND,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V218_CAPACITY_POLICY_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [TURN_NAME],
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": RUNTIME_TOTAL_TOKEN_BOUND,
        "phase_total_token_bound": RUNTIME_TOTAL_TOKEN_BOUND,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _expected_runtime_paths() -> tuple[Path, ...]:
    return tuple(
        sorted(
            set(v216._expected_runtime_paths())
            | {
                Path(v217.__file__).resolve(),
                Path(__file__).resolve(),
            },
            key=str,
        )
    )


def _freeze_runtime_lock(
    *,
    root: Path,
    checkpoint: Mapping[str, Any],
    model_audit_path: Path,
    projection_path: Path,
    identity_path: Path,
    instructions_path: Path,
    spec_path: Path,
    capacity: Mapping[str, Path],
    paths: Mapping[str, Path],
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V218_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "pinned_protocol_schema": _record(
            v216.codex_app_server_module.PROTOCOL_SCHEMA_PATH
        ),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v217_receipt": list(checkpoint["records"].values()),
        "model_selection_audit": _record(model_audit_path),
        "projection_audit": _record(projection_path),
        "identity_projection": _record(identity_path),
        "attempt_spec": _record(spec_path),
        "capacity_audit": _record(capacity["audit"]),
        "capacity_policy": _record(capacity["policy"]),
        "frozen_request": [
            _record(paths["input"]),
            _record(paths["prompt"]),
            _record(paths["schema"]),
            _record(instructions_path),
        ],
        "managed_chatgpt_auth_only": True,
        "production_mutation_allowed": False,
    }
    _write_stable_time(path, lock, "created_at")
    verify_runtime_lock(path)
    return path


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v218 runtime lock")
    checkpoint = _validate_v217_checkpoint()
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    if (
        lock.get("schema_version") != V218_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("managed_chatgpt_auth_only") is not True
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or lock.get("v217_receipt") != list(checkpoint["records"].values())
    ):
        raise JudgeV5SelectionV218Error("v218 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        lock.get("pinned_protocol_schema"),
        lock.get("model_selection_audit"),
        lock.get("projection_audit"),
        lock.get("identity_projection"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_request") or []),
        *(lock.get("runtime_files") or []),
        *(lock.get("v217_receipt") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV218Error("v218 runtime lock record drifted")
    policy = load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    if policy.get("audit") != lock.get("capacity_audit"):
        raise JudgeV5SelectionV218Error("v218 policy/audit cross-link drifted")
    return lock


def _load_frozen_v218(root: Path) -> dict[str, Any]:
    paths = _attempt_paths(root)
    if any(
        path.exists()
        for path in (
            root / "launch-receipt.json",
            paths["capacity"],
            paths["sidecar"],
            paths["output"],
        )
    ):
        raise JudgeV5SelectionV218Error(
            "v218 started attempt cannot be silently resumed"
        )
    required = {
        "model_audit": root / "model-selection-audit.json",
        "projection": root / "vector-projection-audit.private.json",
        "identity": root / "identity-projection.private.json",
        "instructions": root / "selector-instructions.private.md",
        "spec": root / "attempt-spec.json",
        "capacity_policy": root / "capacity-policy.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "runtime_lock": root / "runtime-lock.json",
        "input": paths["input"],
        "prompt": paths["prompt"],
        "schema": paths["schema"],
    }
    if not all(path.is_file() for path in required.values()):
        raise JudgeV5SelectionV218Error("v218 frozen root is incomplete")
    verify_runtime_lock(required["runtime_lock"])
    checkpoint = _validate_v217_checkpoint()
    request = _build_request(checkpoint)
    if (
        _load_json(required["input"], "v218 input") != request["packet"]
        or required["prompt"].read_text(encoding="utf-8") != request["prompt"]
        or _load_json(required["schema"], "v218 schema") != request["schema"]
        or _load_json(required["identity"], "v218 identity")
        != request["identity"]
        or required["instructions"].read_text(encoding="utf-8")
        != request["instructions"]
    ):
        raise JudgeV5SelectionV218Error("v218 frozen request drifted")
    return {
        "root": root,
        "checkpoint": checkpoint,
        "request": request,
        "paths": paths,
        "instructions": required["instructions"],
        "capacity_policy": required["capacity_policy"],
        "capacity_audit": required["capacity_audit"],
        "spec_path": required["spec"],
        "runtime_lock": required["runtime_lock"],
    }


def freeze_v218(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {
            "root": root,
            "terminal": _load_json(root / "terminal.json", "v218 terminal"),
        }
    if (root / "runtime-lock.json").exists():
        return _load_frozen_v218(root)
    if any(root.iterdir()):
        raise JudgeV5SelectionV218Error("v218 root is partial before runtime lock")

    checkpoint = _validate_v217_checkpoint()
    request = _build_request(checkpoint)
    model_audit = {
        "schema_version": V218_MODEL_AUDIT_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "spark_quality_result": checkpoint["records"]["gate"],
        "spark_measured_total_tokens": 41_499,
        "spark_measured_output_tokens": 8_044,
        "sol_prior_measured_total_tokens": 39_476,
        "sol_prior_measured_output_tokens": 2_901,
        "selected_model": MODEL,
        "selection_reason": (
            "measured_lower_output_reasoning_usage_with_lossless_prompt_compaction"
        ),
        "semantic_support_contract_changed": False,
        "new_semantic_model_calls": 0,
        "production_mutated": False,
    }
    model_audit_path = root / "model-selection-audit.json"
    _write_stable_time(model_audit_path, model_audit, "created_at")
    projection_path = root / "vector-projection-audit.private.json"
    identity_path = root / "identity-projection.private.json"
    _write_immutable(projection_path, request["projection"])
    _write_immutable(identity_path, request["identity"])
    instructions_path = root / "selector-instructions.private.md"
    if instructions_path.exists():
        if instructions_path.read_text(encoding="utf-8") != request["instructions"]:
            raise JudgeV5SelectionV218Error("v218 instructions drifted")
    else:
        instructions_path.write_text(request["instructions"], encoding="utf-8")
    paths = _freeze_turn_request(
        root=root,
        turn_name=TURN_NAME,
        input_value=request["packet"],
        prompt=request["prompt"],
        schema=request["schema"],
    )
    capacity = _build_capacity_policy(root, checkpoint)
    spec = {
        "schema_version": V218_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "lossless_vector_coded_sol_low_support_only_existing_event_order",
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "maximum_total_tokens_per_turn": RUNTIME_TOTAL_TOKEN_BOUND,
        "promotion_total_token_gate": PROMOTION_TOTAL_TOKEN_GATE,
        "maximum_prompt_bytes": MAX_PROMPT_BYTES,
        "maximum_instructions_bytes": MAX_INSTRUCTIONS_BYTES,
        "maximum_schema_bytes": MAX_SCHEMA_BYTES,
        "actual_prompt_bytes": request["prompt_bytes"],
        "actual_instructions_bytes": request["instruction_bytes"],
        "actual_schema_bytes": request["schema_bytes"],
        "v216_prompt_bytes": 53_483,
        "prompt_byte_reduction_fraction": round(
            1 - request["prompt_bytes"] / 53_483,
            6,
        ),
        "candidate_event_count": 79,
        "semantic_support_owner": MODEL,
        "semantic_deduplication_allowed": False,
        "deterministic_identity_projection_scope": "case_and_event_order_only",
        "deterministic_deduplication_scope": "exact_identity_and_ownership_only",
        "semantic_rewrite_allowed": False,
        "semantic_filtering_by_deterministic_code": False,
        "extraction_model_calls_authorized": 0,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "v216_semantic_turn_replayed": False,
        "v217_receipt": checkpoint["records"],
        "model_selection_audit": _record(model_audit_path),
        "projection_audit": _record(projection_path),
        "identity_projection": _record(identity_path),
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "frozen_request": {
            "input": _record(paths["input"]),
            "prompt": _record(paths["prompt"]),
            "schema": _record(paths["schema"]),
            "instructions": _record(instructions_path),
        },
        "privacy": (
            "private_source_prompts_outputs_sanitized_counts_hashes_metrics_only"
        ),
    }
    spec_path = root / "attempt-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    lock_path = _freeze_runtime_lock(
        root=root,
        checkpoint=checkpoint,
        model_audit_path=model_audit_path,
        projection_path=projection_path,
        identity_path=identity_path,
        instructions_path=instructions_path,
        spec_path=spec_path,
        capacity=capacity,
        paths=paths,
    )
    return {
        "root": root,
        "checkpoint": checkpoint,
        "request": request,
        "paths": paths,
        "instructions": instructions_path,
        "capacity_policy": capacity["policy"],
        "capacity_audit": capacity["audit"],
        "spec_path": spec_path,
        "runtime_lock": lock_path,
    }


def _freeze_launch_receipt(frozen: Mapping[str, Any]) -> Path:
    path = frozen["root"] / "launch-receipt.json"
    if path.exists():
        raise JudgeV5SelectionV218Error("v218 launch receipt already exists")
    receipt = {
        "schema_version": V218_LAUNCH_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "state": "semantic_attempt_not_started",
        "declared_turn_count": 1,
        "turn_plan": [TURN_NAME],
        "retry_count_per_turn": 0,
        "managed_chatgpt_auth_only": True,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "capacity_policy": _record(frozen["capacity_policy"]),
        "capacity_checkpoint_exists_before_launch": frozen["paths"][
            "capacity"
        ].exists(),
        "sidecar_exists_before_launch": frozen["paths"]["sidecar"].exists(),
        "output_exists_before_launch": frozen["paths"]["output"].exists(),
        "production_mutated": False,
    }
    if any(
        receipt[key]
        for key in (
            "capacity_checkpoint_exists_before_launch",
            "sidecar_exists_before_launch",
            "output_exists_before_launch",
        )
    ):
        raise JudgeV5SelectionV218Error("v218 semantic artifacts predate launch")
    _write_stable_time(path, receipt, "created_at")
    return path


def _write_failure(
    root: Path,
    checkpoint: Mapping[str, Any],
    error_class: str,
) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    known = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        sidecar_record = attempt.get("sidecar")
        if not isinstance(sidecar_record, Mapping):
            unknown += 1
            continue
        try:
            usage = _validate_usage(
                _load_json(Path(sidecar_record["path"]), "v218 failed sidecar")
            )
        except Exception:
            unknown += 1
            continue
        known = _sum_usage(known, usage)
    complete = unknown == 0
    terminal = checkpoint["terminal"]
    cumulative = _sum_usage(
        terminal["cumulative_known_usage_lower_bound"],
        known,
    )
    failure = {
        "schema_version": V218_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": TURN_NAME,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": known if complete else None,
        "known_usage_lower_bound": known,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    result = {
        "schema_version": V218_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "terminal_classification": "inactive_incomplete_recovery_required",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "semantic_retry_allowed": False,
        "semantic_retry_count": 0,
        "full_development_router_authorized": False,
        "full_development_selector_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
        "cumulative_unknown_usage_turn_count": terminal[
            "cumulative_unknown_usage_turn_count"
        ]
        + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": terminal[
            "cumulative_conservative_unknown_usage_upper_bound"
        ]
        + unknown * RUNTIME_TOTAL_TOKEN_BOUND,
    }
    _write_immutable(root / "terminal.json", result)
    return result


async def run_v218(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v218 terminal")
    frozen = freeze_v218(output_dir=root, timeout_seconds=timeout_seconds)
    verify_runtime_lock(frozen["runtime_lock"])
    launch_path = _freeze_launch_receipt(frozen)
    checkpoint = frozen["checkpoint"]
    request = frozen["request"]
    try:
        async with (client_factory or _client_factory)(
            frozen["capacity_policy"]
        ) as client:
            output, sidecar, _adopted = await _get_or_run_turn(
                client=client,
                turn_name=TURN_NAME,
                paths=frozen["paths"],
                prompt=request["prompt"],
                schema=request["schema"],
                base_instructions=request["instructions"],
                model=MODEL,
                effort=EFFORT,
                timeout_seconds=timeout_seconds,
                batch_size=4,
                policy_path=frozen["capacity_policy"],
                output_validator=lambda value: validate_support_output(
                    value,
                    request["packet"],
                    request["schema"],
                ),
            )
        accounting = _aggregate_usage([sidecar])
        projected_output = _project_support_output(output, request["identity"])
        projection_errors = v216.v212.validate_selector_output(
            projected_output,
            request["scoring_predecessor"],
        )
        if projection_errors:
            raise JudgeV5SelectionV218Error(
                "support projection invalid: " + ";".join(projection_errors)
            )
        projected_path = root / "support-positive-selector-output.private.json"
        _write_immutable(projected_path, projected_output)
        gate, private_score = v216.v212._score_selector(
            output=projected_output,
            usage=accounting["usage"],
            predecessor=request["scoring_predecessor"],
        )
        verdict_counts = Counter(
            str(verdict)
            for case in output["v"]
            for verdict in case
        )
        gate = {
            **gate,
            "schema_version": V218_GATE_VERSION,
            "model": MODEL,
            "effort": EFFORT,
            "support_verdict_counts": dict(sorted(verdict_counts.items())),
            "semantic_deduplication_performed": False,
            "vector_projection_lossless": True,
            "prompt_bytes": request["prompt_bytes"],
            "instruction_bytes": request["instruction_bytes"],
            "schema_bytes": request["schema_bytes"],
        }
        private_score = {**private_score, "gate": gate}
        gate_path = root / "compact-support-canary-gate.json"
        score_path = root / "compact-support-canary-score.private.json"
        _write_immutable(gate_path, gate)
        _write_immutable(score_path, private_score)
        terminal = checkpoint["terminal"]
        cumulative = _sum_usage(
            terminal["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        passed = bool(gate["passed"])
        result = {
            "schema_version": V218_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v218_compact_support_canary_passed_full_router_authorized"
                if passed
                else "v218_compact_support_canary_quality_or_cost_gate_not_passed"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "v216_semantic_turn_replayed": False,
            "extraction_model_calls_started": 0,
            "launch_receipt": _record(launch_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "support_positive_output": _record(projected_path),
            "gate": _record(gate_path),
            "private_score": _record(score_path),
            "full_development_router_authorized": passed,
            "full_development_selector_authorized": False,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_allowed": False,
            "semantic_retry_count": 0,
            "viable_systems": (
                ["sol_low_compact_support_only_existing_event_selector"]
                if passed
                else []
            ),
            "unresolved_selection_decision": (
                "full_development_quality_and_cost_generalization"
                if passed
                else "compact_selector_did_not_clear_the_frozen_canary"
            ),
            "more_development_cases_can_change_selection": passed,
            "shortest_path_to_holdout_verdict": (
                "full_coverage_router_then_budgeted_selector_then_development_freeze"
                if passed
                else "new_evidence_based_strategy_required_holdout_closed"
            ),
            "cumulative_known_usage_lower_bound": cumulative,
            "cumulative_unknown_usage_turn_count": terminal[
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_conservative_unknown_usage_upper_bound": terminal[
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "required_next_artifact_path": (
                str(
                    root.parent
                    / "development-selection-v5_4-v219-full-coverage-router"
                    / "terminal.json"
                )
                if passed
                else None
            ),
            **accounting,
        }
        _write_immutable(terminal_path, result)
        return result
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, checkpoint, exc.error_class)
    except Exception as exc:
        return _write_failure(root, checkpoint, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the v218 compact Sol support-only selector canary"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    parser.add_argument("--freeze-only", action="store_true")
    args = parser.parse_args(argv)
    if args.freeze_only:
        frozen = freeze_v218(
            output_dir=Path(args.output_dir),
            timeout_seconds=args.timeout_seconds,
        )
        result = {
            "state": "frozen_before_model_call",
            "runtime_lock": str(frozen["runtime_lock"]),
            "holdout_authorized": False,
            "production_mutated": False,
        }
    else:
        terminal = asyncio.run(
            run_v218(
                output_dir=Path(args.output_dir),
                timeout_seconds=args.timeout_seconds,
            )
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "full_development_router_authorized": terminal.get(
                "full_development_router_authorized", False
            ),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
            "usage_status": terminal.get("usage_status"),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
