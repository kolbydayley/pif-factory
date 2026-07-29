from __future__ import annotations

"""Run one dictionary-coded Spark support-only selector canary."""

import argparse
import asyncio
import copy
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity_module
from . import app_server_capacity_reserve as reserve_module
from . import app_server_dev_selection as selection_module
from . import app_server_judge_v5_calibration_v26_diagnostic as v26
from . import app_server_judge_v5_selection_v186_convergence_checkpoint as v186
from . import app_server_judge_v5_selection_v188_composite_postprocess as v188
from . import app_server_judge_v5_selection_v192_residual_repair_design as v192
from . import app_server_judge_v5_selection_v201_residual_repair_score as v201
from . import app_server_judge_v5_selection_v207_adaptive_router_design as v207
from . import app_server_judge_v5_selection_v208_adaptive_router_canary as v208
from . import app_server_judge_v5_selection_v209_adaptive_router_schema_recovery as v209
from . import app_server_judge_v5_selection_v210_adaptive_router_nonacceptance as v210
from . import app_server_judge_v5_selection_v211_event_selector_design as v211
from . import app_server_judge_v5_selection_v212_event_selector_canary as v212
from . import (
    app_server_judge_v5_selection_v213_event_selector_presemantic_recovery as v213,
)
from . import (
    app_server_judge_v5_selection_v214_truth_conditional_selector as v214,
)
from . import app_server_judge_v5_selection_v215_id_normalization_score as v215
from . import app_server_llm_judge as llm_judge_module
from . import codex_app_server as codex_app_server_module
from . import labels as labels_module
from . import util as util_module
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


V216_MODEL_AUDIT_VERSION = "pif_app_server_judge_v5_4_selection_v216_model_audit_v1"
V216_PROJECTION_VERSION = "pif_app_server_judge_v5_4_selection_v216_projection_v1"
V216_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V216_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V216_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v216_spec_v1"
V216_RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_4_selection_v216_runtime_lock_v1"
V216_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v216_launch_v1"
V216_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v216_failure_v1"
V216_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v216_gate_v1"
V216_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v216_terminal_v1"
PHASE_ID = "judge_v5_4_selection_v216_spark_support_canary"
TURN_NAME = "selection_spark_support_only_canary_00"
DEFAULT_OUTPUT_ROOT = (
    v215.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v216-model-packaging-diagnostic"
).resolve()

MODEL = "gpt-5.3-codex-spark"
EFFORT = "low"
TIMEOUT_SECONDS = v211.TIMEOUT_SECONDS
PROMOTION_TOTAL_TOKEN_GATE = v211.CANARY_SELECTOR_TOTAL_TOKEN_GATE
RUNTIME_TOTAL_TOKEN_BOUND = 38_000
MAX_PROMPT_BYTES = 55_000
MAX_INSTRUCTIONS_BYTES = 4_096
MAX_SCHEMA_BYTES = 8_192

DICTIONARY_FIELD_NAMES = (
    "actor_name",
    "actor_type",
    "certainty",
    "event_type",
    "metric_direction",
    "reported_actor_type",
    "speaker_name",
    "speaker_role",
    "stance",
    "temporal_horizon",
)

SPARK_ROOT = (
    v215.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v36-pointwise-probe-spark"
).resolve()
MINI_ROOT = (
    v215.DEFAULT_OUTPUT_ROOT.parent
    / "judge-calibration-v5_4-v35-pointwise-probe-mini"
).resolve()


class JudgeV5SelectionV216Error(RuntimeError):
    """The v216 Spark support canary cannot preserve its frozen contract."""


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


def _v215_paths() -> dict[str, Path]:
    root = v215.DEFAULT_OUTPUT_ROOT
    return {
        "spec": root / "attempt-spec.json",
        "audit": root / "id-normalization-audit.json",
        "gate": root / "normalized-selector-gate.json",
        "output": root / "normalized-selector-output.private.json",
        "score": root / "normalized-selector-score.private.json",
        "runtime_lock": root / "runtime-lock.json",
        "terminal": root / "terminal.json",
    }


def _validate_v215_checkpoint() -> dict[str, Any]:
    root = v215.DEFAULT_OUTPUT_ROOT
    paths = _v215_paths()
    expected = {path.resolve() for path in paths.values()}
    actual = {path.resolve() for path in root.rglob("*") if path.is_file()}
    if actual != expected:
        raise JudgeV5SelectionV216Error("v215 immutable file set drifted")
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV216Error("v215 immutable artifact drifted")
    v215.verify_runtime_lock(paths["runtime_lock"])
    terminal = _load_json(paths["terminal"], "v215 terminal")
    gate = _load_json(paths["gate"], "v215 gate")
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason")
        != "v215_normalized_selector_quality_or_cost_gate_not_passed"
        or terminal.get("new_semantic_model_calls") != 0
        or terminal.get("usage", {}).get("total_tokens") != 0
        or terminal.get("cumulative_known_usage_lower_bound", {}).get("total_tokens")
        != 9_011_691
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or gate.get("candidate_mean_f1") != 0.785931
        or gate.get("maximum_dense_case_regret_to_oracle") != 0.250602
        or set(gate.get("failed_checks") or [])
        != {
            "actual_selector_total_tokens_lte_35000",
            "maximum_dense_case_regret_lte_0_15",
        }
    ):
        raise JudgeV5SelectionV216Error("v215 terminal contract drifted")
    v214_checkpoint = v215._validate_v214_checkpoint()
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "gate": gate,
        "v214": v214_checkpoint,
        "predecessor": v214_checkpoint["request"]["scoring_predecessor"],
    }


def _model_evidence_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns"
    names = [
        "pointwise-case-context-repair-shard-00",
        "pointwise-case-context-repair-shard-01",
        "pointwise-case-context-repair-shard-02",
    ]
    sidecars = {
        f"sidecar_{index}": turn_root / name / "sidecar.json"
        for index, name in enumerate(names)
    }
    return {
        **sidecars,
        "score": root / "pointwise-probe-score.json",
        "terminal": root / "terminal.json",
    }


def _validate_model_evidence() -> dict[str, Any]:
    configurations = {
        "spark": (SPARK_ROOT, MODEL, 100_319),
        "mini": (MINI_ROOT, "gpt-5.4-mini", 123_333),
    }
    result = {}
    for name, (root, model, expected_total) in configurations.items():
        paths = _model_evidence_paths(root)
        if any(not path.is_file() for path in paths.values()):
            raise JudgeV5SelectionV216Error(f"{name} model evidence is missing")
        values = {
            key: _load_json(path, f"{name} {key}") for key, path in paths.items()
        }
        sidecars = [values[f"sidecar_{index}"] for index in range(3)]
        terminal = values["terminal"]
        score = values["score"]
        if (
            any(row.get("model") != model for row in sidecars)
            or any(row.get("status") != "completed" for row in sidecars)
            or any(row.get("auth_type") != "chatgpt" for row in sidecars)
            or any(row.get("usage_complete") is not True for row in sidecars)
            or terminal.get("usage_status") != "complete"
            or terminal.get("usage", {}).get("total_tokens") != expected_total
            or score.get("metrics", {}).get("structured_field_accuracy") != 0.8
            or score.get("metrics", {}).get("support_sensitivity") != 0.942857
            or score.get("metrics", {}).get("support_specificity") != 0.8
        ):
            raise JudgeV5SelectionV216Error(f"{name} model evidence drifted")
        result[name] = {
            "records": {key: _record(path) for key, path in paths.items()},
            "model": model,
            "turn_count": 3,
            "total_tokens": expected_total,
            "input_tokens": sum(int(row["usage"]["input_tokens"]) for row in sidecars),
            "output_tokens": sum(
                int(row["usage"]["output_tokens"]) for row in sidecars
            ),
            "reasoning_output_tokens": sum(
                int(row["usage"]["reasoning_output_tokens"]) for row in sidecars
            ),
            "metrics": score["metrics"],
        }
    if result["spark"]["total_tokens"] >= result["mini"]["total_tokens"]:
        raise JudgeV5SelectionV216Error("Spark efficiency evidence drifted")
    return result


def selector_instructions() -> str:
    return """
You are a side-free source-grounded support verifier. You receive complete source
text and opaque candidate events from hidden extraction passes. You do not know
system identity, reference labels, scores, or density. The source is unchanged
except for exact-evidence markers. A marker proves only its literal boundary;
evaluate each event using that evidence and the complete source context.

The packet contains every truth-conditional selection field. For aliases listed in
value_tables, an integer value indexes the same alias's array; decode it before
judging. Evaluate claim, actor, attribution, causal mechanism, certainty, event
type, metric, negation or counterclaim, reported actor, speaker, stance, target,
and temporal horizon. Role and event-type values are classification labels: require
them to characterize the source accurately, not to appear literally. Supported
paraphrase and coreference pass. Exact wording alone does not make an unsupported
inference pass.

Return s when every listed material proposition is source-supported, u for a direct
material conflict or ungrounded assertion, and a only when the source cannot resolve
support. Judge each event independently. Do not compare, merge, or deduplicate
events; events sharing evidence or topic may express distinct truth conditions. Do
not rewrite, add, or infer an event. Preserve case and event order, classify every
event exactly once, and output only the required structured object. Do not emit
rationale or confidence.
""".strip()


def _dictionary_packet(
    original: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    legend = {
        str(alias): str(name)
        for alias, name in (original.get("event_field_legend") or {}).items()
    }
    name_to_alias = {name: alias for alias, name in legend.items()}
    if not set(DICTIONARY_FIELD_NAMES).issubset(name_to_alias):
        raise JudgeV5SelectionV216Error("dictionary field coverage drifted")
    dictionary_aliases = {name_to_alias[name] for name in DICTIONARY_FIELD_NAMES}
    value_tables: dict[str, list[Any]] = {alias: [] for alias in dictionary_aliases}
    for case in original.get("cases") or []:
        for event in case.get("candidate_events") or []:
            for alias, value in (event.get("values") or {}).items():
                if alias in value_tables and value not in value_tables[alias]:
                    value_tables[alias].append(copy.deepcopy(value))
    cases = []
    for case in original.get("cases") or []:
        events = []
        for event in case.get("candidate_events") or []:
            values = {}
            for alias, value in (event.get("values") or {}).items():
                if alias in value_tables:
                    values[alias] = value_tables[alias].index(value)
                else:
                    values[alias] = copy.deepcopy(value)
            events.append({"event_id": str(event["event_id"]), "values": values})
        cases.append(
            {
                "case_id": str(case["case_id"]),
                "annotated_source": str(case["annotated_source"]),
                "candidate_events": events,
            }
        )
    packet = {
        "evidence_marker_contract": str(original["evidence_marker_contract"]),
        "field_scope_contract": str(original["field_scope_contract"]),
        "event_field_legend": legend,
        "value_table_contract": (
            "integer values for a tabled alias index its same-alias array"
        ),
        "value_tables": {
            alias: value_tables[alias] for alias in sorted(value_tables)
        },
        "cases": cases,
    }
    decoded = copy.deepcopy(packet)
    decoded.pop("value_table_contract")
    decoded.pop("value_tables")
    for case in decoded["cases"]:
        for event in case["candidate_events"]:
            for alias, value in list(event["values"].items()):
                if alias in value_tables:
                    event["values"][alias] = value_tables[alias][int(value)]
    expected = copy.deepcopy(dict(original))
    if decoded != expected:
        raise JudgeV5SelectionV216Error("dictionary projection is not reversible")
    audit = {
        "schema_version": V216_PROJECTION_VERSION,
        "dictionary_field_names": list(DICTIONARY_FIELD_NAMES),
        "dictionary_aliases": sorted(dictionary_aliases),
        "case_count": len(cases),
        "candidate_event_count": sum(len(row["candidate_events"]) for row in cases),
        "losslessly_reversible": True,
        "complete_source_preserved": all(
            new["annotated_source"] == old["annotated_source"]
            for new, old in zip(cases, original.get("cases") or [], strict=True)
        ),
        "case_order_preserved": [row["case_id"] for row in cases]
        == [str(row["case_id"]) for row in original.get("cases") or []],
        "event_order_and_coverage_preserved": all(
            [str(event["event_id"]) for event in new["candidate_events"]]
            == [str(event["event_id"]) for event in old.get("candidate_events") or []]
            for new, old in zip(cases, original.get("cases") or [], strict=True)
        ),
        "semantic_rewrite_performed": False,
        "semantic_filtering_performed": False,
        "semantic_deduplication_performed": False,
    }
    if (
        audit["case_count"] != 4
        or audit["candidate_event_count"] != 79
        or not all(
            audit[key]
            for key in (
                "losslessly_reversible",
                "complete_source_preserved",
                "case_order_preserved",
                "event_order_and_coverage_preserved",
            )
        )
    ):
        raise JudgeV5SelectionV216Error("dictionary packet audit drifted")
    return packet, audit


def _selector_prompt(packet: Mapping[str, Any]) -> str:
    return "# Dictionary-coded support-only event packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    )


def _support_schema(packet: Mapping[str, Any]) -> dict[str, Any]:
    case_ids = [str(row["case_id"]) for row in packet["cases"]]
    event_ids = sorted(
        {
            str(event["event_id"])
            for case in packet["cases"]
            for event in case["candidate_events"]
        }
    )
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["cases"],
        "properties": {
            "cases": {
                "type": "array",
                "minItems": len(case_ids),
                "maxItems": len(case_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["case_id", "decisions"],
                    "properties": {
                        "case_id": {"type": "string", "enum": case_ids},
                        "decisions": {
                            "type": "array",
                            "maxItems": v211.MAX_EVENTS_PER_CASE,
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["event_id", "verdict"],
                                "properties": {
                                    "event_id": {
                                        "type": "string",
                                        "enum": event_ids,
                                    },
                                    "verdict": {
                                        "type": "string",
                                        "enum": ["s", "u", "a"],
                                    },
                                },
                            },
                        },
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
    expected_cases = [str(row["case_id"]) for row in packet["cases"]]
    if [row.get("case_id") for row in output.get("cases") or []] != expected_cases:
        return ["case_order_or_coverage"]
    input_by_case = {str(row["case_id"]): row for row in packet["cases"]}
    for case in output["cases"]:
        expected_events = [
            str(row["event_id"])
            for row in input_by_case[str(case["case_id"])]["candidate_events"]
        ]
        if [row.get("event_id") for row in case["decisions"]] != expected_events:
            return ["event_order_or_coverage"]
    return []


def _project_support_output(output: Mapping[str, Any]) -> dict[str, Any]:
    projected = {"cases": []}
    verdict_map = {"s": "keep", "u": "unsupported", "a": "abstain"}
    for case in output["cases"]:
        decisions = []
        for decision in case["decisions"]:
            event_id = str(decision["event_id"])
            verdict = verdict_map[str(decision["verdict"])]
            decisions.append(
                {
                    "event_id": event_id,
                    "verdict": verdict,
                    "canonical_event_id": event_id if verdict == "keep" else "",
                }
            )
        projected["cases"].append(
            {"case_id": str(case["case_id"]), "decisions": decisions}
        )
    return projected


def _build_request(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    packet, projection = _dictionary_packet(predecessor["input"])
    instructions = selector_instructions()
    prompt = _selector_prompt(packet)
    schema = _support_schema(packet)
    prompt_bytes = len(prompt.encode("utf-8"))
    instruction_bytes = len(instructions.encode("utf-8"))
    schema_transport = json.dumps(
        schema, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    schema_bytes = len(schema_transport.encode("utf-8"))
    if (
        prompt_bytes > MAX_PROMPT_BYTES
        or instruction_bytes > MAX_INSTRUCTIONS_BYTES
        or schema_bytes > MAX_SCHEMA_BYTES
        or llm_judge_module.validate_app_server_output_schema_subset(schema)
    ):
        raise JudgeV5SelectionV216Error("v216 request exceeds frozen bounds")
    return {
        "packet": packet,
        "projection": projection,
        "instructions": instructions,
        "prompt": prompt,
        "schema": schema,
        "prompt_bytes": prompt_bytes,
        "instruction_bytes": instruction_bytes,
        "schema_bytes": schema_bytes,
        "scoring_predecessor": predecessor,
    }


def _build_capacity_policy(
    root: Path,
    checkpoint: Mapping[str, Any],
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        RUNTIME_TOTAL_TOKEN_BOUND * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": V216_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v215_terminal": checkpoint["records"]["terminal"],
        "measured_basis": {
            "predecessor_known_total_tokens_lower_bound": checkpoint["terminal"][
                "cumulative_known_usage_lower_bound"
            ]["total_tokens"],
            "predecessor_unknown_usage_turn_count": checkpoint["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "predecessor_unknown_usage_upper_bound": checkpoint["terminal"][
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
        "schema_version": V216_CAPACITY_POLICY_VERSION,
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
            set(v214._expected_runtime_paths())
            | {Path(v215.__file__).resolve(), Path(__file__).resolve()},
            key=str,
        )
    )


def _freeze_runtime_lock(
    *,
    root: Path,
    checkpoint: Mapping[str, Any],
    model_evidence: Mapping[str, Any],
    model_audit_path: Path,
    projection_path: Path,
    instructions_path: Path,
    spec_path: Path,
    capacity: Mapping[str, Path],
    paths: Mapping[str, Path],
) -> Path:
    path = root / "runtime-lock.json"
    evidence_records = [
        record
        for model in ("spark", "mini")
        for record in model_evidence[model]["records"].values()
    ]
    lock = {
        "schema_version": V216_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "pinned_protocol_schema": _record(codex_app_server_module.PROTOCOL_SCHEMA_PATH),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v215_receipt": list(checkpoint["records"].values()),
        "model_evidence": evidence_records,
        "model_selection_audit": _record(model_audit_path),
        "projection_audit": _record(projection_path),
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
    lock = _load_json(path, "v216 runtime lock")
    checkpoint = _validate_v215_checkpoint()
    model_evidence = _validate_model_evidence()
    expected_evidence = [
        record
        for model in ("spark", "mini")
        for record in model_evidence[model]["records"].values()
    ]
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    if (
        lock.get("schema_version") != V216_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("managed_chatgpt_auth_only") is not True
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or lock.get("v215_receipt") != list(checkpoint["records"].values())
        or lock.get("model_evidence") != expected_evidence
    ):
        raise JudgeV5SelectionV216Error("v216 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        lock.get("pinned_protocol_schema"),
        lock.get("model_selection_audit"),
        lock.get("projection_audit"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_request") or []),
        *(lock.get("runtime_files") or []),
        *(lock.get("v215_receipt") or []),
        *(lock.get("model_evidence") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV216Error("v216 runtime lock record drifted")
    policy = load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    if policy.get("audit") != lock.get("capacity_audit"):
        raise JudgeV5SelectionV216Error("v216 policy/audit cross-link drifted")
    return lock


def _load_frozen_v216(root: Path) -> dict[str, Any]:
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
        raise JudgeV5SelectionV216Error(
            "v216 started attempt cannot be silently resumed"
        )
    required = {
        "model_audit": root / "model-selection-audit.json",
        "projection": root / "dictionary-projection-audit.private.json",
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
        raise JudgeV5SelectionV216Error("v216 frozen root is incomplete")
    verify_runtime_lock(required["runtime_lock"])
    checkpoint = _validate_v215_checkpoint()
    request = _build_request(checkpoint["predecessor"])
    if (
        _load_json(required["input"], "v216 input") != request["packet"]
        or required["prompt"].read_text(encoding="utf-8") != request["prompt"]
        or _load_json(required["schema"], "v216 schema") != request["schema"]
        or required["instructions"].read_text(encoding="utf-8")
        != request["instructions"]
    ):
        raise JudgeV5SelectionV216Error("v216 frozen request drifted")
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


def freeze_v216(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {
            "root": root,
            "terminal": _load_json(root / "terminal.json", "v216 terminal"),
        }
    if (root / "runtime-lock.json").exists():
        return _load_frozen_v216(root)
    if any(root.iterdir()):
        raise JudgeV5SelectionV216Error("v216 root is partial before runtime lock")

    checkpoint = _validate_v215_checkpoint()
    model_evidence = _validate_model_evidence()
    request = _build_request(checkpoint["predecessor"])
    model_audit = {
        "schema_version": V216_MODEL_AUDIT_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "comparison_scope": "matched_prior_pointwise_probe_three_turn_totals",
        "spark": {
            key: value
            for key, value in model_evidence["spark"].items()
            if key != "records"
        },
        "mini": {
            key: value
            for key, value in model_evidence["mini"].items()
            if key != "records"
        },
        "selected_model": MODEL,
        "selection_reason": (
            "matched_quality_metrics_and_lower_measured_input_output_total_tokens"
        ),
        "new_semantic_model_calls": 0,
        "production_mutated": False,
    }
    model_audit_path = root / "model-selection-audit.json"
    _write_stable_time(model_audit_path, model_audit, "created_at")
    projection_path = root / "dictionary-projection-audit.private.json"
    _write_immutable(projection_path, request["projection"])
    instructions_path = root / "selector-instructions.private.md"
    if instructions_path.exists():
        if instructions_path.read_text(encoding="utf-8") != request["instructions"]:
            raise JudgeV5SelectionV216Error("v216 instructions drifted")
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
        "schema_version": V216_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "dictionary_coded_spark_low_support_only_existing_event_ids",
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
        "candidate_event_count": 79,
        "semantic_support_owner": MODEL,
        "semantic_deduplication_allowed": False,
        "deterministic_deduplication_scope": "exact_identity_and_ownership_only",
        "semantic_rewrite_allowed": False,
        "semantic_filtering_by_deterministic_code": False,
        "extraction_model_calls_authorized": 0,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "v214_semantic_turn_replayed": False,
        "v215_receipt": checkpoint["records"],
        "model_selection_audit": _record(model_audit_path),
        "projection_audit": _record(projection_path),
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
        model_evidence=model_evidence,
        model_audit_path=model_audit_path,
        projection_path=projection_path,
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
        raise JudgeV5SelectionV216Error("v216 launch receipt already exists")
    receipt = {
        "schema_version": V216_LAUNCH_VERSION,
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
        raise JudgeV5SelectionV216Error("v216 semantic artifacts predate launch")
    _write_stable_time(path, receipt, "created_at")
    return path


def _write_failure(
    root: Path, checkpoint: Mapping[str, Any], error_class: str
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
                _load_json(Path(sidecar_record["path"]), "v216 failed sidecar")
            )
        except Exception:
            unknown += 1
            continue
        known = _sum_usage(known, usage)
    complete = unknown == 0
    cumulative = _sum_usage(
        checkpoint["terminal"]["cumulative_known_usage_lower_bound"], known
    )
    failure = {
        "schema_version": V216_FAILURE_VERSION,
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
    terminal = {
        "schema_version": V216_TERMINAL_VERSION,
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
        "cumulative_unknown_usage_turn_count": checkpoint["terminal"][
            "cumulative_unknown_usage_turn_count"
        ]
        + unknown,
        "cumulative_conservative_unknown_usage_upper_bound": checkpoint["terminal"][
            "cumulative_conservative_unknown_usage_upper_bound"
        ]
        + unknown * RUNTIME_TOTAL_TOKEN_BOUND,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v216(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v216 terminal")
    frozen = freeze_v216(output_dir=root, timeout_seconds=timeout_seconds)
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
                    value, request["packet"], request["schema"]
                ),
            )
        accounting = _aggregate_usage([sidecar])
        projected_output = _project_support_output(output)
        projection_errors = v212.validate_selector_output(
            projected_output, request["scoring_predecessor"]
        )
        if projection_errors:
            raise JudgeV5SelectionV216Error(
                "support projection invalid: " + ";".join(projection_errors)
            )
        projected_path = root / "support-positive-selector-output.private.json"
        _write_immutable(projected_path, projected_output)
        gate, private_score = v212._score_selector(
            output=projected_output,
            usage=accounting["usage"],
            predecessor=request["scoring_predecessor"],
        )
        verdict_counts = Counter(
            str(decision["verdict"])
            for case in output["cases"]
            for decision in case["decisions"]
        )
        gate = {
            **gate,
            "schema_version": V216_GATE_VERSION,
            "model": MODEL,
            "effort": EFFORT,
            "support_verdict_counts": dict(sorted(verdict_counts.items())),
            "semantic_deduplication_performed": False,
            "dictionary_projection_lossless": True,
            "prompt_bytes": request["prompt_bytes"],
            "instruction_bytes": request["instruction_bytes"],
            "schema_bytes": request["schema_bytes"],
        }
        private_score = {**private_score, "gate": gate}
        gate_path = root / "spark-support-canary-gate.json"
        score_path = root / "spark-support-canary-score.private.json"
        _write_immutable(gate_path, gate)
        _write_immutable(score_path, private_score)
        cumulative = _sum_usage(
            checkpoint["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        passed = bool(gate["passed"])
        terminal = {
            "schema_version": V216_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v216_spark_support_canary_passed_full_router_authorized"
                if passed
                else "v216_spark_support_canary_quality_or_cost_gate_not_passed"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "v214_semantic_turn_replayed": False,
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
                ["spark_low_support_only_existing_event_selector"] if passed else []
            ),
            "unresolved_selection_decision": (
                "full_development_quality_and_cost_generalization"
                if passed
                else "no_selector_candidate_cleared_the_frozen_canary"
            ),
            "more_development_cases_can_change_selection": passed,
            "shortest_path_to_holdout_verdict": (
                "full_coverage_router_then_budgeted_selector_then_development_freeze"
                if passed
                else "new_semantic_strategy_required_holdout_closed"
            ),
            "cumulative_known_usage_lower_bound": cumulative,
            "cumulative_unknown_usage_turn_count": checkpoint["terminal"][
                "cumulative_unknown_usage_turn_count"
            ],
            "cumulative_conservative_unknown_usage_upper_bound": checkpoint["terminal"][
                "cumulative_conservative_unknown_usage_upper_bound"
            ],
            "required_next_artifact_path": (
                str(
                    root.parent
                    / "development-selection-v5_4-v217-full-coverage-router"
                    / "terminal.json"
                )
                if passed
                else None
            ),
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(root, checkpoint, exc.error_class)
    except Exception as exc:
        return _write_failure(root, checkpoint, type(exc).__name__)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the v216 Spark support-only selector canary"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    parser.add_argument("--freeze-only", action="store_true")
    args = parser.parse_args(argv)
    if args.freeze_only:
        frozen = freeze_v216(
            output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds
        )
        result = {
            "state": "frozen_before_model_call",
            "runtime_lock": str(frozen["runtime_lock"]),
            "holdout_authorized": False,
            "production_mutated": False,
        }
    else:
        terminal = asyncio.run(
            run_v216(
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
