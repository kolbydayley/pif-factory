from __future__ import annotations

"""Scale-check the frozen v174 alignment judge on the largest selection case."""

import argparse
import asyncio
import json
import math
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_judge_v5_calibration_v130_retained_alignment_owner as v130
from . import app_server_judge_v5_calibration_v155_fresh_full_development as v155
from . import app_server_judge_v5_calibration_v157_exact_span_canary_recovery as v157
from . import app_server_judge_v5_selection_v175_support as v175
from . import app_server_judge_v5_selection_v176_support_evidence_ids as v176
from .app_server_judge_v5 import (
    build_neutral_alignment_input,
    load_fixture_truth_audit,
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
)
from .app_server_judge_v5_calibration_v25_diagnostic import (
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
from .util import now_iso, sha256_text


V177_SPEC_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v177_spec_v1"
V177_TERMINAL_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v177_terminal_v1"
V177_FAILURE_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v177_failure_v1"
V177_COMPACT_VERSION = "pif_app_server_judge_v5_4_lossless_alignment_packet_v1"
V177_AUDIT_VERSION = "pif_app_server_judge_v5_4_lossless_alignment_audit_v1"
V177_SCORE_VERSION = "pif_app_server_judge_v5_4_selection_alignment_v177_score_v1"
V177_CAPACITY_AUDIT_VERSION = "pif_app_server_capacity_policy_audit_v20"
V177_CAPACITY_POLICY_VERSION = "pif_app_server_capacity_policy_v20"
V177_PHASE_ID = "judge_v5_4_selection_v177_alignment_scale_diagnostic"

MODEL = "gpt-5.5"
EFFORT = "high"
TURN_NAMES = ("selection_alignment_scale_base", "selection_alignment_scale_canary")
MAXIMUM_TOTAL_TOKENS_PER_TURN = 100_000
MAXIMUM_PROMPT_BYTES = 90_000
MAXIMUM_SCHEMA_BYTES = 20_000
TIMEOUT_SECONDS = v176.TIMEOUT_SECONDS
DEFAULT_OUTPUT_ROOT = (
    v176.DEFAULT_OUTPUT_ROOT.parent
    / "development-selection-v5_4-v177-alignment-scale-diagnostic"
).resolve()

SUPPORT_RECEIPT_FIELDS = (
    "proposition_verdict",
    "proposition_evidence_spans",
    "structured_field_verdict",
    "field_issue_fields",
    "field_evidence_spans",
)


class JudgeV5SelectionV177Error(RuntimeError):
    """The immutable v177 alignment diagnostic cannot be preserved."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _sum_usage(left: Mapping[str, int], right: Mapping[str, int]) -> dict[str, int]:
    return {field: int(left[field]) + int(right[field]) for field in USAGE_FIELDS}


def _validate_v176_success() -> dict[str, Any]:
    root = v176.DEFAULT_OUTPUT_ROOT
    paths = {
        "terminal": root / "terminal.json",
        "spec": root / "selection-support-evidence-ids-spec.json",
        "policy": root / "capacity-policy.json",
        "audit": root / "capacity-policy-audit.json",
        "support_output": root / "support-output-expanded.private.json",
        "support_receipts": root / "support-receipts.private.json",
    }
    values = {name: _load_json(path, f"v176 {name}") for name, path in paths.items()}
    terminal, spec = values["terminal"], values["spec"]
    expected_usage = {
        "input_tokens": 727130,
        "cached_input_tokens": 0,
        "output_tokens": 177960,
        "reasoning_output_tokens": 51019,
        "total_tokens": 905090,
    }
    if (
        terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "v176_selection_support_completed_alignment_authorized"
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("usage") != expected_usage
        or terminal.get("turn_count") != 28
        or terminal.get("support_receipts_frozen") is not True
        or terminal.get("alignment_authorized") is not True
        or terminal.get("selection_winner_frozen") is not False
        or terminal.get("holdout_authorized") is not False
        or terminal.get("production_mutated") is not False
        or terminal.get("support_status_counts")
        != {"supported": 1914, "unsupported": 46, "abstain": 0}
        or spec.get("turn_plan")
        != [f"selection_support_evidence_ids_{index:02d}" for index in range(28)]
        or spec.get("retry_count_per_turn") != 0
        or spec.get("semantic_similarity_used") is not False
        or spec.get("semantic_regex_or_keyword_rules_used") is not False
    ):
        raise JudgeV5SelectionV177Error("v176 success contract drifted")
    for path in paths.values():
        if not path.is_file():
            raise JudgeV5SelectionV177Error("v176 artifact disappeared")
    for record in spec.get("runtime_files") or []:
        if not _verify_record(record):
            raise JudgeV5SelectionV177Error("v176 runtime record drifted")
    for request in spec["frozen_inputs"]["turns"]:
        for key in ("input", "prompt", "schema"):
            if not _verify_record(request[key]):
                raise JudgeV5SelectionV177Error("v176 request record drifted")

    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    if len(attempts) != 28:
        raise JudgeV5SelectionV177Error("v176 attempt coverage drifted")
    sidecars = []
    for attempt in attempts:
        for key in ("capacity", "sidecar", "output"):
            record = attempt.get(key)
            if not isinstance(record, Mapping) or not _verify_record(record):
                raise JudgeV5SelectionV177Error(f"v176 {key} record drifted")
        sidecar = _load_json(Path(attempt["sidecar"]["path"]), "v176 sidecar")
        _validate_usage(sidecar)
        sidecars.append(sidecar)
    if _aggregate_usage(sidecars)["usage"] != expected_usage:
        raise JudgeV5SelectionV177Error("v176 measured usage aggregate drifted")

    predecessor = v176._validate_v175_failure()
    output = values["support_output"]
    full_value = v175._support_value(predecessor["sources"]["pointwise"]["units"])
    if v175.v143.validate_support_output(output, full_value):
        raise JudgeV5SelectionV177Error("v176 expanded support output drifted")
    if v155._support_receipts(output) != values["support_receipts"]:
        raise JudgeV5SelectionV177Error("v176 support receipts drifted")
    if (
        terminal.get("support_output") != _record(paths["support_output"])
        or terminal.get("support_receipts") != _record(paths["support_receipts"])
        or terminal.get("predecessor_cumulative_usage") != predecessor["cumulative_usage"]
        or terminal.get("cumulative_evaluation_usage")
        != _sum_usage(predecessor["cumulative_usage"], expected_usage)
        or not _verify_record(terminal["v174_protocol"])
    ):
        raise JudgeV5SelectionV177Error("v176 terminal lineage drifted")
    return {
        "root": root,
        "paths": paths,
        "values": values,
        "records": {name: _record(path) for name, path in paths.items()},
        "attempts": attempts,
        "pool": predecessor["sources"]["pool"],
        "pool_record": predecessor["sources"]["pool_record"],
        "receipts": values["support_receipts"],
        "cumulative_usage": terminal["cumulative_evaluation_usage"],
    }


def build_supported_alignment_input(
    pool: Mapping[str, Any],
    receipts: Mapping[str, Any],
    *,
    case_id: str,
    permutation: str,
) -> dict[str, Any]:
    value = build_neutral_alignment_input(
        pool, receipts, case_ids=[case_id], permutation=permutation
    )
    case = value["cases"][0]
    case["witnesses"] = [
        witness
        for witness in case["witnesses"]
        if witness["support_receipt"]["proposition_verdict"] == "supported"
    ]
    if not case["witnesses"]:
        raise JudgeV5SelectionV177Error("alignment diagnostic selected an empty case")
    value["supported_witnesses_only"] = True
    value["structured_field_receipts_withheld"] = True
    return value


def _collect_strings(value: Any, strings: set[str]) -> None:
    if isinstance(value, str):
        strings.add(value)
    elif isinstance(value, list):
        for item in value:
            _collect_strings(item, strings)
    elif isinstance(value, Mapping):
        for item in value.values():
            _collect_strings(item, strings)


def _encode_value(value: Any, indexes: Mapping[str, int]) -> Any:
    if isinstance(value, str):
        return {"s": indexes[value]}
    if isinstance(value, list):
        return [_encode_value(item, indexes) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _encode_value(item, indexes) for key, item in value.items()}
    return value


def _decode_value(value: Any, strings: Sequence[str]) -> Any:
    if isinstance(value, Mapping) and set(value) == {"s"}:
        index = value["s"]
        if not isinstance(index, int) or not 0 <= index < len(strings):
            raise JudgeV5SelectionV177Error("compact string reference is invalid")
        return strings[index]
    if isinstance(value, list):
        return [_decode_value(item, strings) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _decode_value(item, strings) for key, item in value.items()}
    return value


def build_lossless_compact_case(case: Mapping[str, Any]) -> dict[str, Any]:
    witnesses = case.get("witnesses")
    if not isinstance(witnesses, list) or not witnesses:
        raise JudgeV5SelectionV177Error("compact alignment case has no witnesses")
    event_fields = sorted(
        {str(key) for witness in witnesses for key in witness["event"].keys()}
    )
    strings: set[str] = set()
    for witness in witnesses:
        _collect_strings(witness["event"], strings)
        _collect_strings(witness["support_receipt"], strings)
    string_table = sorted(strings)
    indexes = {value: index for index, value in enumerate(string_table)}

    definitions_by_json: dict[str, Mapping[str, Any]] = {}
    for witness in witnesses:
        event = witness["event"]
        definitions_by_json.setdefault(_canonical_json(event), event)
    definition_keys = sorted(definitions_by_json)
    definition_ids = {
        canonical: f"event_def_{index:03d}"
        for index, canonical in enumerate(definition_keys)
    }
    event_definitions = []
    for canonical in definition_keys:
        event = definitions_by_json[canonical]
        event_definitions.append(
            {
                "event_definition_id": definition_ids[canonical],
                "nonempty_field_values": [
                    [event_fields.index(field), _encode_value(event[field], indexes)]
                    for field in event_fields
                    if field in event
                ],
            }
        )
    compact_witnesses = []
    for witness in witnesses:
        receipt = witness["support_receipt"]
        if set(receipt) != set(SUPPORT_RECEIPT_FIELDS):
            raise JudgeV5SelectionV177Error("support receipt shape drifted")
        compact_witnesses.append(
            [
                witness["witness_id"],
                definition_ids[_canonical_json(witness["event"])],
                [_encode_value(receipt[field], indexes) for field in SUPPORT_RECEIPT_FIELDS],
            ]
        )
    compact = {
        "schema_version": V177_COMPACT_VERSION,
        "case_id": case["case_id"],
        "source_excerpt": case["source_excerpt"],
        "event_field_order": event_fields,
        "support_receipt_field_order": list(SUPPORT_RECEIPT_FIELDS),
        "string_table": string_table,
        "event_definitions": event_definitions,
        "witnesses": compact_witnesses,
        "serialization_is_lossless": True,
        "semantic_fields_pruned": False,
    }
    if decode_lossless_compact_case(compact) != dict(case):
        raise JudgeV5SelectionV177Error("compact alignment serialization is not lossless")
    return compact


def decode_lossless_compact_case(compact: Mapping[str, Any]) -> dict[str, Any]:
    if compact.get("schema_version") != V177_COMPACT_VERSION:
        raise JudgeV5SelectionV177Error("compact alignment version drifted")
    strings = compact["string_table"]
    fields = compact["event_field_order"]
    receipt_fields = compact["support_receipt_field_order"]
    definitions = {}
    for row in compact["event_definitions"]:
        event = {}
        for field_index, encoded in row["nonempty_field_values"]:
            event[fields[field_index]] = _decode_value(encoded, strings)
        definitions[row["event_definition_id"]] = event
    witnesses = []
    for witness_id, definition_id, receipt_values in compact["witnesses"]:
        witnesses.append(
            {
                "witness_id": witness_id,
                "event": deepcopy(definitions[definition_id]),
                "support_receipt": {
                    field: _decode_value(value, strings)
                    for field, value in zip(
                        receipt_fields, receipt_values, strict=True
                    )
                },
            }
        )
    return {
        "case_id": compact["case_id"],
        "source_excerpt": compact["source_excerpt"],
        "witnesses": witnesses,
    }


def compact_alignment_prompt(value: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    case = value["cases"][0]
    compact = build_lossless_compact_case(case)
    audit = load_fixture_truth_audit()
    rubric = [
        item
        for field in value["checklist_field_order"]
        for item in audit["mismatch_checklist"]
        if item["field"] == field
    ]
    packet = {
        "rubric": rubric,
        "mismatch_precedence": audit["mismatch_precedence"],
        "lossless_compact_case": compact,
    }
    prompt = (
        "All presented witnesses are support-positive. Align only these visible witnesses, "
        "freeze one-to-one assignment, then complete the full 15-row checklist. Do not infer "
        "omitted witnesses. Return the case exactly once. equivalence_groups must be a disjoint "
        "partition of every visible witness ID. Every witness must appear exactly once in an "
        "alignment pair or unpaired_witness_ids. Every pair checklist must contain each supplied "
        "field exactly once. Equivalent requires every row same. Partial is reserved for exactly "
        "event_boundary plus evidence merge/split differences. Any unresolved material row "
        "requires abstain. source_evidence_spans must be exact source_excerpt substrings; use [] "
        "for witness-only conflicts. The input is losslessly columnar: event fields are indexed "
        "by event_field_order; event_definition_id expands through event_definitions; each {s:N} "
        "references string_table[N]; support receipt values "
        "follow support_receipt_field_order. "
        "No event field or support-receipt value was pruned.\n\n# Neutral pooled case\n"
        + _canonical_json(packet)
        + "\n"
    )
    return prompt, compact


def _request_bytes(prompt: str, schema: Mapping[str, Any]) -> tuple[int, int]:
    return len(prompt.encode("utf-8")), len(_canonical_json(schema).encode("utf-8"))


def _build_capacity_policy(root: Path, predecessor: Mapping[str, Any]) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAXIMUM_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": V177_CAPACITY_AUDIT_VERSION,
        "phase_id": V177_PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "v176_terminal": predecessor["records"]["terminal"],
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": 20,
            "maximum_live_used_percent_for_launch": 80 - projected,
            "v176_last_observed_primary_used_percent": 41,
            "scale_diagnostic_only": True,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": V177_CAPACITY_POLICY_VERSION,
        "phase_id": V177_PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": list(TURN_NAMES),
        "minimum_remaining_reserve_percent": 20,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    return {"audit": audit_path, "policy": policy_path}


def freeze_v177(
    *, output_dir: Path = DEFAULT_OUTPUT_ROOT, timeout_seconds: float = TIMEOUT_SECONDS
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v177 terminal")}
    predecessor = _validate_v176_success()
    candidates = []
    for case in predecessor["pool"]["cases"]:
        case_id = str(case["case_id"])
        value = build_supported_alignment_input(
            predecessor["pool"], predecessor["receipts"], case_id=case_id, permutation="base"
        ) if any(
            row["case_id"] == case_id and row["proposition_verdict"] == "supported"
            for row in predecessor["receipts"]["units"]
        ) else None
        if value is None:
            continue
        prompt, compact = compact_alignment_prompt(value)
        schema = neutral_alignment_output_schema(value)
        prompt_bytes, schema_bytes = _request_bytes(prompt, schema)
        candidates.append(
            {
                "case_id": case_id,
                "value": value,
                "prompt": prompt,
                "compact": compact,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
            }
        )
    if len(candidates) != 28:
        raise JudgeV5SelectionV177Error("v177 nonempty alignment case count drifted")
    selected = max(candidates, key=lambda row: (row["prompt_bytes"], row["case_id"]))
    canary_value = build_supported_alignment_input(
        predecessor["pool"],
        predecessor["receipts"],
        case_id=selected["case_id"],
        permutation="balanced_canary",
    )
    canary_prompt, canary_compact = compact_alignment_prompt(canary_value)
    canary_schema = neutral_alignment_output_schema(canary_value)
    canary_prompt_bytes, canary_schema_bytes = _request_bytes(canary_prompt, canary_schema)
    turns = []
    for turn_name, value, prompt, compact, schema, prompt_bytes, schema_bytes in (
        (
            TURN_NAMES[0], selected["value"], selected["prompt"], selected["compact"],
            selected["schema"], selected["prompt_bytes"], selected["schema_bytes"],
        ),
        (
            TURN_NAMES[1], canary_value, canary_prompt, canary_compact,
            canary_schema, canary_prompt_bytes, canary_schema_bytes,
        ),
    ):
        if prompt_bytes > MAXIMUM_PROMPT_BYTES or schema_bytes > MAXIMUM_SCHEMA_BYTES:
            raise JudgeV5SelectionV177Error("v177 request exceeds frozen byte cap")
        paths = _freeze_turn_request(
            root=root, turn_name=turn_name, input_value=value, prompt=prompt, schema=schema
        )
        audit_path = root / "turns" / turn_name.replace("_", "-") / "lossless-serialization-audit.json"
        _write_immutable(
            audit_path,
            {
                "schema_version": V177_AUDIT_VERSION,
                "case_id": selected["case_id"],
                "input_case_sha256": sha256_text(_canonical_json(value["cases"][0])),
                "decoded_case_sha256": sha256_text(
                    _canonical_json(decode_lossless_compact_case(compact))
                ),
                "lossless_round_trip": decode_lossless_compact_case(compact) == value["cases"][0],
                "semantic_fields_pruned": False,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
                "witness_count": len(value["cases"][0]["witnesses"]),
                "unique_event_definition_count": len(compact["event_definitions"]),
            },
        )
        turns.append(
            {
                "turn_name": turn_name,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "compact": compact,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
                "paths": paths,
                "serialization_audit": audit_path,
            }
        )
    base_ids = [row["witness_id"] for row in turns[0]["value"]["cases"][0]["witnesses"]]
    canary_ids = [row["witness_id"] for row in turns[1]["value"]["cases"][0]["witnesses"]]
    if base_ids != list(reversed(canary_ids)):
        raise JudgeV5SelectionV177Error("v177 canary witness permutation drifted")

    capacity = _build_capacity_policy(root, predecessor)
    spec = {
        "schema_version": V177_SPEC_VERSION,
        "state": "frozen_before_model_calls",
        "created_at": now_iso(),
        "phase_id": V177_PHASE_ID,
        "model": MODEL,
        "reasoning_effort": EFFORT,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "strategy": "largest_case_lossless_string_table_base_plus_balanced_canary",
        "diagnostic_case_count": 1,
        "diagnostic_witness_count": len(base_ids),
        "turn_plan": list(TURN_NAMES),
        "retry_count_per_turn": 0,
        "promotion_rules": {
            "both_turns_structurally_valid": True,
            "base_canary_projection_exact": True,
            "minimum_alignment_pair_count": 1,
            "abstained_pair_count": 0,
            "maximum_total_tokens_per_turn": MAXIMUM_TOTAL_TOKENS_PER_TURN,
        },
        "lossless_compact_serialization": True,
        "semantic_fields_pruned": False,
        "semantic_similarity_used": False,
        "semantic_regex_or_keyword_rules_used": False,
        "support_receipts_frozen": True,
        "full_alignment_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutation_allowed": False,
        "capacity_policy": _record(capacity["policy"]),
        "capacity_audit": _record(capacity["audit"]),
        "predecessor": predecessor["records"],
        "v174_protocol": predecessor["values"]["terminal"]["v174_protocol"],
        "runtime_files": [
            _record(Path(__file__)),
            _record(Path(v176.__file__)),
            _record(Path(v157.__file__)),
            _record(Path(v130.__file__)),
            *predecessor["values"]["spec"]["runtime_files"],
        ],
        "frozen_instructions": {
            "alignment_base_sha256": sha256_text(v130.alignment_instructions_v130()),
        },
        "frozen_inputs": {
            "pool": predecessor["pool_record"],
            "support_receipts": predecessor["records"]["support_receipts"],
            "turns": [
                {
                    "turn_name": turn["turn_name"],
                    "input": _record(turn["paths"]["input"]),
                    "prompt": _record(turn["paths"]["prompt"]),
                    "schema": _record(turn["paths"]["schema"]),
                    "serialization_audit": _record(turn["serialization_audit"]),
                    "prompt_bytes": turn["prompt_bytes"],
                    "schema_bytes": turn["schema_bytes"],
                }
                for turn in turns
            ],
        },
        "privacy": "private_source_event_prompt_output_mapping_sanitized_counts_hashes_only",
    }
    spec_path = root / "selection-alignment-scale-diagnostic-spec.json"
    _write_stable_time(spec_path, spec, "created_at")
    return {
        "root": root,
        "spec": spec,
        "spec_path": spec_path,
        "capacity_policy": capacity["policy"],
        "turns": turns,
        "predecessor": predecessor,
        "selected_case_id": selected["case_id"],
    }


def score_v177(base: Mapping[str, Any], canary: Mapping[str, Any]) -> dict[str, Any]:
    base_projection = v130._project_alignment(base["cases"][0])
    canary_projection = v130._project_alignment(canary["cases"][0])
    pairs = base["cases"][0]["alignment_pairs"]
    abstained = sum(
        pair["relation"] == "abstain"
        or any(row["decision"] == "abstain" for row in pair["checklist"])
        for pair in pairs
    )
    checks = {
        "base_canary_projection_exact": base_projection == canary_projection,
        "minimum_alignment_pair_count": len(pairs) >= 1,
        "abstained_pair_count": abstained == 0,
    }
    return {
        "schema_version": V177_SCORE_VERSION,
        "passed": all(checks.values()),
        "checks": checks,
        "alignment_pair_count": len(pairs),
        "abstained_pair_count": abstained,
        "base_canary_projection_exact": base_projection == canary_projection,
    }


def _write_failure(
    root: Path, predecessor: Mapping[str, Any], turn_name: Optional[str], error_class: str
) -> dict[str, Any]:
    attempts = [
        row
        for row in _attempt_records(root)
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    for attempt in attempts:
        record = attempt.get("sidecar")
        if not isinstance(record, Mapping):
            unknown += 1
            continue
        try:
            measured = _validate_usage(_load_json(Path(record["path"]), "v177 sidecar"))
        except Exception:
            unknown += 1
            continue
        for field in USAGE_FIELDS:
            usage[field] += measured[field]
    complete = unknown == 0
    cumulative = _sum_usage(predecessor["cumulative_usage"], usage)
    failure = {
        "schema_version": V177_FAILURE_VERSION,
        "terminal_at": now_iso(),
        "classification": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": turn_name,
        "error_class": error_class,
        "retry_allowed_in_this_version": False,
        "accounting_complete": complete,
        "usage_status": "complete" if complete else "unknown",
        "usage": usage if complete else None,
        "known_usage_lower_bound": usage,
        "unknown_usage_turn_count": unknown,
        "attempts": attempts,
        "predecessor_cumulative_usage": predecessor["cumulative_usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    failure_path = root / "failure.json"
    _write_immutable(failure_path, failure)
    terminal = {
        "schema_version": V177_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "overall_evaluation_complete": False,
        "failure": _record(failure_path),
        "support_receipts_frozen": True,
        "full_alignment_authorized": False,
        "selection_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "semantic_retry_count": 0,
        "accounting_complete": complete,
        "usage_status": failure["usage_status"],
        "usage": failure["usage"],
        "cumulative_known_usage_lower_bound": cumulative,
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


async def run_v177(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "v177 terminal")
    frozen = freeze_v177(output_dir=root, timeout_seconds=timeout_seconds)
    current_turn: Optional[str] = None
    projected_outputs, sidecars = [], []
    try:
        async with (client_factory or _client_factory)(frozen["capacity_policy"]) as client:
            for turn in frozen["turns"]:
                current_turn = turn["turn_name"]
                output, sidecar, _ = await _get_or_run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=turn["paths"],
                    prompt=turn["prompt"],
                    schema=turn["schema"],
                    base_instructions=v130.alignment_instructions_v130(),
                    model=MODEL,
                    effort=EFFORT,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(turn["value"]["cases"][0]["witnesses"]),
                    policy_path=frozen["capacity_policy"],
                    output_validator=lambda candidate, item=turn["value"]: (
                        v157.validate_structurally_projectable_output(candidate, item)
                    ),
                )
                projected, projection_audit = v157.project_exact_spans_and_relation(
                    output, turn["value"]
                )
                turn_root = root / "turns" / current_turn.replace("_", "-")
                _write_immutable(turn_root / "alignment-projected.private.json", projected)
                _write_immutable(turn_root / "structural-projection-audit.json", projection_audit)
                projected_outputs.append(projected)
                sidecars.append(sidecar)
        normalized = [
            normalize_neutral_alignment_output(output, turn["value"])
            for output, turn in zip(projected_outputs, frozen["turns"], strict=True)
        ]
        score = score_v177(normalized[0], normalized[1])
        score_path = root / "selection-alignment-scale-diagnostic-score.json"
        _write_immutable(score_path, score)
        accounting = _aggregate_usage(sidecars)
        cumulative = _sum_usage(
            frozen["predecessor"]["cumulative_usage"], accounting["usage"]
        )
        passed = bool(score["passed"])
        terminal = {
            "schema_version": V177_TERMINAL_VERSION,
            "state": "completed" if passed else "inactive",
            "terminal_at": now_iso(),
            "terminal_reason": (
                "v177_alignment_scale_diagnostic_passed_full_alignment_authorized"
                if passed
                else "inactive_incomplete_recovery_required"
            ),
            "development_terminal_reason": (
                "v177_alignment_scale_diagnostic_passed"
                if passed
                else "v177_alignment_scale_diagnostic_quality_gate_not_passed"
            ),
            "overall_evaluation_complete": False,
            "support_receipts_frozen": True,
            "full_alignment_authorized": passed,
            "selection_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "semantic_retry_count": 0,
            "diagnostic_case_count": 1,
            "diagnostic_witness_count": frozen["spec"]["diagnostic_witness_count"],
            "score": _record(score_path),
            "checks": score["checks"],
            "alignment_pair_count": score["alignment_pair_count"],
            "predecessor_cumulative_usage": frozen["predecessor"]["cumulative_usage"],
            "cumulative_evaluation_usage": cumulative,
            **accounting,
        }
        _write_immutable(terminal_path, terminal)
        return terminal
    except JudgeV5CalibrationV26DiagnosticAttemptFailed as exc:
        return _write_failure(
            root, frozen["predecessor"], exc.turn_name, exc.error_class
        )
    except Exception as exc:
        return _write_failure(
            root, frozen["predecessor"], current_turn, type(exc).__name__
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v177 alignment scale diagnostic")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v177(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "full_alignment_authorized": terminal.get("full_alignment_authorized", False),
                "usage_status": terminal.get("usage_status"),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal["state"] != "failed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
