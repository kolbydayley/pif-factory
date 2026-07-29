from __future__ import annotations

"""Run a compact truth-conditional repair of the v213 event selector."""

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
from .util import now_iso


V214_DEFECT_VERSION = "pif_app_server_judge_v5_4_selection_v214_defect_v1"
V214_PROJECTION_VERSION = "pif_app_server_judge_v5_4_selection_v214_projection_v1"
V214_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V214_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V214_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_v214_spec_v1"
V214_RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_4_selection_v214_runtime_lock_v1"
V214_LAUNCH_VERSION = "pif_app_server_judge_v5_4_selection_v214_launch_v1"
V214_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_v214_failure_v1"
V214_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_v214_terminal_v1"
V214_GATE_VERSION = "pif_app_server_judge_v5_4_selection_v214_gate_v1"
PHASE_ID = "judge_v5_4_selection_v214_truth_conditional_selector"
TURN_NAME = "selection_truth_conditional_event_selector_00"
DEFAULT_OUTPUT_ROOT = (
    v213.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v214-truth-conditional-selector"
).resolve()

MODEL = v211.MODEL
EFFORT = v211.EFFORT
TIMEOUT_SECONDS = v211.TIMEOUT_SECONDS
PROMOTION_TOTAL_TOKEN_GATE = v211.CANARY_SELECTOR_TOTAL_TOKEN_GATE
RUNTIME_TOTAL_TOKEN_BOUND = 40_000
MAX_PROMPT_BYTES = 64_000
MAX_INSTRUCTIONS_BYTES = 4_096
MAX_SCHEMA_BYTES = 8_192

TRUTH_CONDITIONAL_FIELDS = (
    "actor_name",
    "actor_type",
    "causal_mechanism",
    "certainty",
    "claim_text",
    "counterclaim",
    "event_type",
    "metric_comparator",
    "metric_direction",
    "metric_raw_text",
    "metric_unit",
    "metric_value",
    "reported_actor_name",
    "reported_actor_type",
    "speaker_name",
    "speaker_role",
    "stance",
    "target_concept",
    "temporal_horizon",
)
OMITTED_NONSELECTION_FIELDS = (
    "claim_type",
    "confidence",
    "event_subtype",
    "model_names",
    "organizations",
    "people",
    "product_names",
    "signal_reason",
    "source_context_kind",
    "window_id",
)


class JudgeV5SelectionV214Error(RuntimeError):
    """The v214 truth-conditional selector cannot preserve its contract."""


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


def _v213_paths() -> dict[str, Path]:
    root = v213.DEFAULT_OUTPUT_ROOT
    turn = root / "turns" / v213.TURN_NAME.replace("_", "-")
    return {
        "attempt_spec": root / "attempt-spec.json",
        "capacity_audit": root / "capacity-policy-audit.json",
        "capacity_policy": root / "capacity-policy.json",
        "gate": root / "event-selector-canary-gate.json",
        "score": root / "event-selector-canary-score.private.json",
        "launch": root / "launch-receipt.json",
        "runtime_lock": root / "runtime-lock.json",
        "terminal": root / "terminal.json",
        "capacity": turn / "capacity.json",
        "input": turn / "input.private.json",
        "output": turn / "output.private.json",
        "prompt": turn / "prompt.private.md",
        "schema": turn / "schema.json",
        "sidecar": turn / "sidecar.json",
        "incident": root / "v212-presemantic-incident.json",
    }


def _validate_v213_checkpoint() -> dict[str, Any]:
    root = v213.DEFAULT_OUTPUT_ROOT
    paths = _v213_paths()
    expected = {path.resolve() for path in paths.values()}
    actual = {path.resolve() for path in root.rglob("*") if path.is_file()}
    if actual != expected:
        raise JudgeV5SelectionV214Error("v213 immutable file set drifted")
    values = {
        name: _load_json(path, f"v213 {name}")
        for name, path in paths.items()
        if name != "prompt"
    }
    v213.verify_runtime_lock(paths["runtime_lock"])
    if any(not _verify_record(_record(path)) for path in paths.values()):
        raise JudgeV5SelectionV214Error("v213 immutable artifact drifted")

    terminal = values["terminal"]
    gate = values["gate"]
    sidecar = values["sidecar"]
    capacity = values["capacity"]
    output = values["output"]
    usage = terminal.get("usage")
    decisions = [
        decision
        for case in output.get("cases") or []
        for decision in case.get("decisions") or []
    ]
    verdicts = Counter(str(row.get("verdict")) for row in decisions)
    expected_failures = {
        "actual_selector_total_tokens_lte_35000",
        "dense_cases_improved_over_base_min_3",
        "maximum_dense_case_regret_lte_0_15",
        "mean_f1_regret_lte_0_05",
    }
    if (
        terminal.get("state") != "inactive"
        or terminal.get("terminal_reason")
        != "v213_event_selector_canary_quality_or_cost_gate_not_passed"
        or terminal.get("overall_evaluation_complete") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or not isinstance(usage, Mapping)
        or int(usage.get("total_tokens", -1)) != 40_773
        or gate.get("passed") is not False
        or set(gate.get("failed_checks") or []) != expected_failures
        or float(gate.get("candidate_mean_f1", -1)) != 0.5
        or int(gate.get("maximum_selected_event_count", -1)) != 0
        or int(gate.get("normalized_nonexact_evidence_events", -1)) != 0
        or len(decisions) != 79
        or verdicts != Counter({"unsupported": 79})
        or sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("usage") != usage
        or capacity.get("cleared_for_semantic_turn") is not True
        or capacity.get("managed_chatgpt_auth_verified") is not True
        or capacity.get("rate_limit_reached_type") is not None
    ):
        raise JudgeV5SelectionV214Error("v213 terminal contract drifted")

    predecessor = v212._validate_v211_authorization()
    if (
        values["input"] != predecessor["input"]
        or values["schema"] != predecessor["schema"]
        or paths["prompt"].read_text(encoding="utf-8") != predecessor["prompt"]
    ):
        raise JudgeV5SelectionV214Error("v213 frozen selector request drifted")
    return {
        "root": root,
        "paths": paths,
        "records": {name: _record(path) for name, path in paths.items()},
        "terminal": terminal,
        "gate": gate,
        "sidecar": sidecar,
        "capacity": capacity,
        "output": output,
        "predecessor": predecessor,
    }


def selector_instructions() -> str:
    return """
You are a side-free source-grounded event selector. You receive complete source text
and opaque candidate events from hidden extraction passes. You do not know system
identity, reference labels, scores, or density. The source is unchanged except for
exact-evidence markers. A marker proves only the literal evidence boundary; evaluate
each event using that evidence and its full source context.

The packet is an explicit truth-conditional projection. Evaluate every listed field
and no omitted bookkeeping field. The material proposition comprises claim, actor,
attribution, causal mechanism, certainty, event type, metric, negation or
counterclaim, reported actor, speaker, stance, target, and temporal horizon. Role and
event-type values are classification labels: require them to accurately characterize
the source, not to appear literally in it. Supported paraphrase and coreference pass.
Exact wording alone does not make an unsupported inference pass.

Use verdict keep only when every listed material proposition is source-supported and
the event is not truth-conditionally equivalent to an earlier kept event in that case.
Use duplicate only when it is supported but expresses the same event as an earlier
kept event; point canonical_event_id to that earlier kept ID. Keep multi-lens events
separate when any listed material proposition differs. Use unsupported for a direct
material conflict or ungrounded assertion. Use abstain only when the source cannot
resolve support or equivalence. Omitted metadata is retained unchanged after
selection and is never a reason to reject an event. Do not rewrite, add, merge, or
infer an event. Preserve case and event order, classify every event exactly once, keep
at most 32 events per case, and output only the required structured object. Do not
emit rationale or confidence.
""".strip()


def _compact_packet(
    original: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    original_legend = {
        str(alias): str(name)
        for alias, name in (original.get("event_field_legend") or {}).items()
    }
    truth_fields = set(TRUTH_CONDITIONAL_FIELDS)
    omitted_fields = set(OMITTED_NONSELECTION_FIELDS)
    if set(original_legend.values()) != truth_fields | omitted_fields:
        raise JudgeV5SelectionV214Error("selector field partition is incomplete")
    compact_legend = {
        alias: name for alias, name in original_legend.items() if name in truth_fields
    }
    cases = []
    audit_cases = []
    for case in original.get("cases") or []:
        projected_events = []
        for event in case.get("candidate_events") or []:
            projected_events.append(
                {
                    "event_id": str(event["event_id"]),
                    "values": {
                        alias: copy.deepcopy(value)
                        for alias, value in (event.get("values") or {}).items()
                        if alias in compact_legend
                    },
                }
            )
        cases.append(
            {
                "case_id": str(case["case_id"]),
                "annotated_source": str(case["annotated_source"]),
                "candidate_events": projected_events,
            }
        )
        audit_cases.append(
            {
                "case_id": str(case["case_id"]),
                "event_count": len(projected_events),
            }
        )
    packet = {
        "evidence_marker_contract": str(original["evidence_marker_contract"]),
        "field_scope_contract": (
            "listed fields are the complete truth-conditional selection surface; "
            "omitted bookkeeping metadata is retained unchanged and is not a "
            "rejection criterion"
        ),
        "event_field_legend": compact_legend,
        "cases": cases,
    }
    audit = {
        "schema_version": V214_PROJECTION_VERSION,
        "truth_conditional_fields": list(TRUTH_CONDITIONAL_FIELDS),
        "omitted_nonselection_fields": list(OMITTED_NONSELECTION_FIELDS),
        "case_count": len(cases),
        "candidate_event_count": sum(row["event_count"] for row in audit_cases),
        "cases": audit_cases,
        "complete_source_preserved": all(
            v211.strip_selector_markers(str(new["annotated_source"]))
            == v211.strip_selector_markers(str(old["annotated_source"]))
            for old, new in zip(original.get("cases") or [], cases, strict=True)
        ),
        "case_order_preserved": [row["case_id"] for row in cases]
        == [str(row["case_id"]) for row in original.get("cases") or []],
        "event_order_and_coverage_preserved": all(
            [str(event["event_id"]) for event in old.get("candidate_events") or []]
            == [str(event["event_id"]) for event in new["candidate_events"]]
            for old, new in zip(original.get("cases") or [], cases, strict=True)
        ),
        "semantic_rewrite_performed": False,
        "semantic_filtering_performed": False,
    }
    if (
        audit["case_count"] != 4
        or audit["candidate_event_count"] != 79
        or audit["complete_source_preserved"] is not True
        or audit["case_order_preserved"] is not True
        or audit["event_order_and_coverage_preserved"] is not True
    ):
        raise JudgeV5SelectionV214Error("compact selector projection drifted")
    return packet, audit


def _selector_prompt(packet: Mapping[str, Any]) -> str:
    return "# Truth-conditional event selection packet\n" + json.dumps(
        packet, ensure_ascii=True, separators=(",", ":")
    )


def _selector_schema(packet: Mapping[str, Any]) -> dict[str, Any]:
    return v211._selector_schema(packet)


def _build_request(predecessor: Mapping[str, Any]) -> dict[str, Any]:
    packet, projection = _compact_packet(predecessor["input"])
    instructions = selector_instructions()
    prompt = _selector_prompt(packet)
    schema = _selector_schema(packet)
    prompt_bytes = len(prompt.encode("utf-8"))
    instruction_bytes = len(instructions.encode("utf-8"))
    schema_bytes = len(
        (
            json.dumps(schema, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
    )
    if (
        prompt_bytes > MAX_PROMPT_BYTES
        or instruction_bytes > MAX_INSTRUCTIONS_BYTES
        or schema_bytes > MAX_SCHEMA_BYTES
        or llm_judge_module.validate_app_server_output_schema_subset(schema)
    ):
        raise JudgeV5SelectionV214Error(
            "compact selector request exceeds frozen bounds"
        )
    scoring_predecessor = {
        **predecessor,
        "input": packet,
        "prompt": prompt,
        "instructions": instructions,
        "schema": schema,
    }
    return {
        "packet": packet,
        "projection": projection,
        "instructions": instructions,
        "prompt": prompt,
        "schema": schema,
        "prompt_bytes": prompt_bytes,
        "instruction_bytes": instruction_bytes,
        "schema_bytes": schema_bytes,
        "scoring_predecessor": scoring_predecessor,
    }


def _build_capacity_policy(
    root: Path, checkpoint: Mapping[str, Any]
) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        RUNTIME_TOTAL_TOKEN_BOUND * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
    audit = {
        "schema_version": V214_CAPACITY_AUDIT_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v213_terminal": checkpoint["records"]["terminal"],
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
        "schema_version": V214_CAPACITY_POLICY_VERSION,
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
            {
                Path(__file__).resolve(),
                Path(v213.__file__).resolve(),
                Path(v212.__file__).resolve(),
                Path(v211.__file__).resolve(),
                Path(v210.__file__).resolve(),
                Path(v209.__file__).resolve(),
                Path(v208.__file__).resolve(),
                Path(v207.__file__).resolve(),
                Path(v201.__file__).resolve(),
                Path(v192.__file__).resolve(),
                Path(v188.__file__).resolve(),
                Path(v186.__file__).resolve(),
                Path(v26.__file__).resolve(),
                Path(reserve_module.__file__).resolve(),
                Path(capacity_module.__file__).resolve(),
                Path(codex_app_server_module.__file__).resolve(),
                Path(selection_module.__file__).resolve(),
                Path(llm_judge_module.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
            },
            key=str,
        )
    )


def _freeze_runtime_lock(
    *,
    root: Path,
    checkpoint: Mapping[str, Any],
    defect_path: Path,
    projection_path: Path,
    spec_path: Path,
    capacity: Mapping[str, Path],
    paths: Mapping[str, Path],
    instructions_path: Path,
) -> Path:
    path = root / "runtime-lock.json"
    lock = {
        "schema_version": V214_RUNTIME_LOCK_VERSION,
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "pinned_protocol_schema": _record(codex_app_server_module.PROTOCOL_SCHEMA_PATH),
        "runtime_files": [_record(item) for item in _expected_runtime_paths()],
        "v213_attempt": list(checkpoint["records"].values()),
        "v211_semantic_sources": list(
            checkpoint["predecessor"]["records"].values()
        ),
        "materiality_defect_audit": _record(defect_path),
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
    lock = _load_json(path, "v214 runtime lock")
    checkpoint = _validate_v213_checkpoint()
    expected_paths = {str(item) for item in _expected_runtime_paths()}
    actual_paths = {
        str(Path(str(record.get("path") or "")).expanduser().resolve())
        for record in lock.get("runtime_files") or []
        if isinstance(record, Mapping)
    }
    if (
        lock.get("schema_version") != V214_RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("managed_chatgpt_auth_only") is not True
        or lock.get("production_mutation_allowed") is not False
        or actual_paths != expected_paths
        or lock.get("v213_attempt") != list(checkpoint["records"].values())
        or lock.get("v211_semantic_sources")
        != list(checkpoint["predecessor"]["records"].values())
    ):
        raise JudgeV5SelectionV214Error("v214 runtime lock drifted")
    records = [
        lock.get("pinned_codex_cli"),
        lock.get("pinned_protocol_schema"),
        lock.get("materiality_defect_audit"),
        lock.get("projection_audit"),
        lock.get("attempt_spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("frozen_request") or []),
        *(lock.get("runtime_files") or []),
        *(lock.get("v213_attempt") or []),
        *(lock.get("v211_semantic_sources") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise JudgeV5SelectionV214Error("v214 runtime lock record drifted")
    policy = load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    if policy.get("audit") != lock.get("capacity_audit"):
        raise JudgeV5SelectionV214Error("v214 policy/audit cross-link drifted")
    return lock


def _load_frozen_v214(root: Path) -> dict[str, Any]:
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
        raise JudgeV5SelectionV214Error(
            "v214 started attempt cannot be silently resumed"
        )
    required = {
        "defect": root / "materiality-defect-audit.json",
        "projection": root / "selector-projection-audit.private.json",
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
        raise JudgeV5SelectionV214Error("v214 frozen root is incomplete")
    verify_runtime_lock(required["runtime_lock"])
    checkpoint = _validate_v213_checkpoint()
    request = _build_request(checkpoint["predecessor"])
    if (
        _load_json(required["input"], "v214 input") != request["packet"]
        or required["prompt"].read_text(encoding="utf-8") != request["prompt"]
        or _load_json(required["schema"], "v214 schema") != request["schema"]
        or required["instructions"].read_text(encoding="utf-8")
        != request["instructions"]
    ):
        raise JudgeV5SelectionV214Error("v214 frozen request drifted")
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


def freeze_v214(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {
            "root": root,
            "terminal": _load_json(root / "terminal.json", "v214 terminal"),
        }
    if (root / "runtime-lock.json").exists():
        return _load_frozen_v214(root)
    if any(root.iterdir()):
        raise JudgeV5SelectionV214Error("v214 root is partial before runtime lock")

    checkpoint = _validate_v213_checkpoint()
    request = _build_request(checkpoint["predecessor"])
    defect = {
        "schema_version": V214_DEFECT_VERSION,
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "classification": "measured_selector_materiality_contract_defect",
        "evidence_scope": "contract_and_degenerate_output_not_individual_event_truth",
        "candidate_event_count": 79,
        "verdict_counts": {"unsupported": 79},
        "selected_event_count": 0,
        "normalized_nonexact_evidence_events": 0,
        "candidate_mean_f1": checkpoint["gate"]["candidate_mean_f1"],
        "measured_total_tokens": checkpoint["terminal"]["usage"]["total_tokens"],
        "promotion_total_token_gate": PROMOTION_TOTAL_TOKEN_GATE,
        "old_serialized_field_count": len(
            checkpoint["predecessor"]["input"]["event_field_legend"]
        ),
        "new_truth_conditional_field_count": len(TRUTH_CONDITIONAL_FIELDS),
        "omitted_nonselection_field_count": len(OMITTED_NONSELECTION_FIELDS),
        "root_cause_class": (
            "undefined_material_field_scope_with_nonselection_metadata_"
            "in_support_packet"
        ),
        "semantic_retry_of_v213": False,
        "production_mutated": False,
    }
    defect_path = root / "materiality-defect-audit.json"
    _write_stable_time(defect_path, defect, "created_at")
    projection_path = root / "selector-projection-audit.private.json"
    _write_immutable(projection_path, request["projection"])
    instructions_path = root / "selector-instructions.private.md"
    if instructions_path.exists():
        if instructions_path.read_text(encoding="utf-8") != request["instructions"]:
            raise JudgeV5SelectionV214Error("v214 instructions drifted")
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
        "schema_version": V214_SPEC_VERSION,
        "state": "frozen_before_model_call",
        "created_at": now_iso(),
        "phase_id": PHASE_ID,
        "strategy": "truth_conditional_existing_event_id_selector",
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
        "semantic_rewrite_allowed": False,
        "semantic_filtering_by_deterministic_code": False,
        "extraction_model_calls_authorized": 0,
        "extraction_replay_allowed": False,
        "batch_5_replay_allowed": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "v213_semantic_turn_replayed": False,
        "v213_attempt": checkpoint["records"],
        "materiality_defect_audit": _record(defect_path),
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
        defect_path=defect_path,
        projection_path=projection_path,
        spec_path=spec_path,
        capacity=capacity,
        paths=paths,
        instructions_path=instructions_path,
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
        raise JudgeV5SelectionV214Error("v214 launch receipt already exists")
    receipt = {
        "schema_version": V214_LAUNCH_VERSION,
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
        raise JudgeV5SelectionV214Error("v214 semantic artifacts predate launch")
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
                _load_json(Path(sidecar_record["path"]), "v214 failed sidecar")
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
        "schema_version": V214_FAILURE_VERSION,
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
        "schema_version": V214_TERMINAL_VERSION,
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


async def run_v214(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v214 terminal")
    frozen = freeze_v214(output_dir=root, timeout_seconds=timeout_seconds)
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
                output_validator=lambda value: v212.validate_selector_output(
                    value, request["scoring_predecessor"]
                ),
            )
        accounting = _aggregate_usage([sidecar])
        gate, private_score = v212._score_selector(
            output=output,
            usage=accounting["usage"],
            predecessor=request["scoring_predecessor"],
        )
        gate = {
            **gate,
            "schema_version": V214_GATE_VERSION,
            "materiality_contract_corrected": True,
            "prompt_bytes": request["prompt_bytes"],
            "instruction_bytes": request["instruction_bytes"],
            "schema_bytes": request["schema_bytes"],
        }
        private_score = {**private_score, "gate": gate}
        gate_path = root / "truth-conditional-selector-canary-gate.json"
        score_path = root / "truth-conditional-selector-canary-score.private.json"
        _write_immutable(gate_path, gate)
        _write_immutable(score_path, private_score)
        cumulative = _sum_usage(
            checkpoint["terminal"]["cumulative_known_usage_lower_bound"],
            accounting["usage"],
        )
        passed = bool(gate["passed"])
        terminal = {
            "schema_version": V214_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v214_truth_conditional_selector_canary_passed_full_router_authorized"
                if passed
                else "v214_truth_conditional_selector_quality_or_cost_gate_not_passed"
            ),
            "terminal_classification": "inactive_incomplete_recovery_required",
            "overall_evaluation_complete": False,
            "v213_semantic_turn_replayed": False,
            "extraction_model_calls_started": 0,
            "launch_receipt": _record(launch_path),
            "runtime_lock": _record(frozen["runtime_lock"]),
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
                ["truth_conditional_existing_event_id_selector"] if passed else []
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
                    / "development-selection-v5_4-v215-full-coverage-router"
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
        description="Run the v214 truth-conditional event selector canary"
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    parser.add_argument("--freeze-only", action="store_true")
    args = parser.parse_args(argv)
    if args.freeze_only:
        frozen = freeze_v214(
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
            run_v214(
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
