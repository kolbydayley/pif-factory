from __future__ import annotations

"""Reference-bound v5 semantic judge and five-arm development selection."""

import argparse
import asyncio
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_dev_selection as dev_selection
from .app_server_capacity import CAPACITY_CHECKPOINT_VERSION
from .app_server_capacity import CapacityGatedCodexAppServerClient
from .app_server_dev_selection import (
    DEV_FULL_JUDGE_VERSION,
    _read_only_connection,
    run_app_server_dev_selection,
)
from .app_server_judge_v5 import (
    ADJUDICATION_INPUT_VERSION,
    PROTOCOL_VERSION,
    adjudication_alignment_input,
    build_disagreement_adjudication_prompt,
    build_neutral_alignment_input,
    build_neutral_alignment_prompt,
    build_pointwise_support_input,
    build_pointwise_support_prompt,
    freeze_support_receipts,
    neutral_alignment_base_instructions,
    neutral_alignment_output_schema,
    normalize_neutral_alignment_output,
    pointwise_support_base_instructions,
    pointwise_support_output_schema,
    validate_neutral_alignment_output,
    validate_pointwise_support_output,
)
from .app_server_judge_v5_diagnostic import (
    JudgeV5DiagnosticAttemptFailed,
    _aggregate_usage,
    _attempt_records,
    _record,
    _sha256_file,
    _turn_paths,
    _write_immutable_json,
    _write_immutable_text,
)
from .app_server_judge_v5_fresh_calibration import (
    FRESH_CALIBRATION_SPEC_VERSION,
    FRESH_CALIBRATION_TERMINAL_VERSION,
)
from .app_server_judge_v5_calibration import (
    CALIBRATION_GATES,
    CALIBRATION_SCORE_VERSION,
)
from .app_server_judge_v5_calibration_runner import (
    CALIBRATION_RUN_VERSION,
    CALIBRATION_TERMINAL_VERSION,
)
from .app_server_judge_v5_continuation import CONTINUATION_TERMINAL_VERSION
from .app_server_llm_judge import JUDGE_CONSENSUS_VERSION
from .app_server_v5_reuse import verify_v5_reuse_contract
from .paths import db_path
from .util import now_iso, sha256_text


SELECTION_JUDGE_VERSION = "pif_app_server_v5_selection_judge_v1"
SELECTION_JUDGE_TERMINAL_VERSION = "pif_app_server_v5_selection_judge_terminal_v1"
SELECTION_ATTEMPT_SPEC_VERSION = "pif_app_server_v5_selection_attempt_spec_v1"
SELECTION_ATTEMPT_TERMINAL_VERSION = "pif_app_server_v5_selection_attempt_terminal_v1"
MAX_PROMPT_BYTES = 640 * 1024
MAX_OUTPUT_SCHEMA_BYTES = 64 * 1024
MAX_ALIGNMENT_CASES_PER_SHARD = 4
CANARY_STRIDE = 4
DEFAULT_PIPELINE_ROOT = Path(
    "work/app-server-development-v2/unattended-pipeline-v5"
).resolve()
DEFAULT_FRESH_CALIBRATION_ROOT = (
    DEFAULT_PIPELINE_ROOT / "judge-calibration-v5_4-reference-v2-v1"
)
DEFAULT_CONTINUATION_TERMINAL = Path(
    "work/app-server-development-v2/unattended-control-v17/terminal.json"
).resolve()
DEFAULT_REUSE_CONTRACT = DEFAULT_PIPELINE_ROOT / "reuse-contract-v4.json"
DEFAULT_OUTPUT_ROOT = DEFAULT_PIPELINE_ROOT / "development-selection-v5-reference-v2"
CALIBRATION_CHECK_KEYS = {
    "minimum_cases",
    "support_sensitivity",
    "support_specificity",
    "structured_field_accuracy",
    "alignment_f1",
    "equivalent_sensitivity",
    "equivalent_specificity",
    "field_diagnostic_f1",
    "relation_accuracy",
    "equivalence_partition_exact_case_rate",
    "unpaired_exact_case_rate",
    "abstention_rate",
    "order_bias",
    "canary_case_count",
}


class V5SelectionError(RuntimeError):
    """The fresh judge or five-arm selection contract is unsafe."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load(path: Path, purpose: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V5SelectionError(f"{purpose} is missing or invalid") from exc
    if not isinstance(value, dict):
        raise V5SelectionError(f"{purpose} is not an object")
    return value


def _verify_record(record: Any, *, expected_path: Optional[Path] = None) -> Path:
    if not isinstance(record, Mapping):
        raise V5SelectionError("selection record is missing")
    path = Path(str(record.get("path") or "")).expanduser().resolve()
    if expected_path is not None and path != expected_path.expanduser().resolve():
        raise V5SelectionError("selection record path drifted")
    if (
        not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise V5SelectionError("selection record content drifted")
    return path


def _verify_fresh_calibration_attempts(
    attempts: Sequence[Mapping[str, Any]], terminal: Mapping[str, Any]
) -> dict[str, Any]:
    required_names = {
        *(f"pointwise_support_shard_{index:02d}" for index in range(11)),
        *(f"neutral_alignment_shard_{index:02d}" for index in range(11)),
        "neutral_alignment_canary",
    }
    observed_names = [str(attempt.get("turn_name")) for attempt in attempts]
    if len(observed_names) != len(set(observed_names)):
        raise V5SelectionError("fresh calibration attempt names are duplicated")
    observed_set = set(observed_names)
    if len(attempts) == 24:
        required_names.add("disagreement_adjudication")
    if observed_set != required_names:
        raise V5SelectionError("fresh calibration attempt coverage drifted")
    sidecars = []
    for attempt in attempts:
        capacity_path = _verify_record(attempt.get("capacity"))
        _verify_record(attempt.get("output"))
        sidecar_path = _verify_record(attempt.get("sidecar"))
        capacity = _load(capacity_path, "fresh calibration capacity checkpoint")
        used = capacity.get("primary_used_percent")
        if (
            capacity.get("schema_version") != CAPACITY_CHECKPOINT_VERSION
            or capacity.get("managed_chatgpt_auth_verified") is not True
            or capacity.get("plan_type") != "pro"
            or capacity.get("cleared_for_semantic_turn") is not True
            or capacity.get("maximum_primary_used_percent") != 20
            or not isinstance(used, (int, float))
            or isinstance(used, bool)
            or used > 20
            or capacity.get("thread_started") is not False
            or capacity.get("turn_started") is not False
            or capacity.get("sidecar_started") is not False
            or capacity.get("retry_checkpoint_reuse_allowed") is not False
        ):
            raise V5SelectionError("fresh calibration capacity contract drifted")
        sidecar = _load(sidecar_path, "fresh calibration usage sidecar")
        turn_root = sidecar_path.parent
        input_path = turn_root / "input.private.json"
        prompt_path = turn_root / "prompt.private.md"
        schema_path = turn_root / "schema.json"
        for request_path in (input_path, prompt_path, schema_path):
            if not request_path.is_file():
                raise V5SelectionError("fresh calibration request artifact is missing")
        prompt_bytes = prompt_path.stat().st_size
        schema_value = _load(schema_path, "fresh calibration output schema")
        canonical_schema = _canonical_json(schema_value)
        schema_bytes = len(canonical_schema.encode("utf-8"))
        output_path = Path(str(attempt["output"]["path"])).expanduser().resolve()
        output_text = output_path.read_text(encoding="utf-8")
        acceptable_output_hashes = {sha256_text(output_text)}
        if output_text.endswith("\n"):
            acceptable_output_hashes.add(sha256_text(output_text[:-1]))
        if (
            sidecar.get("state") != "completed"
            or sidecar.get("status") != "completed"
            or sidecar.get("usage_status") != "measured"
            or sidecar.get("usage_complete") is not True
            or sidecar.get("auth_type") != "chatgpt"
            or sidecar.get("transport") != "stdio"
            or sidecar.get("model") != "gpt-5.6-sol"
            or sidecar.get("effort") != "high"
            or sidecar.get("thread_mode") != "new_thread"
            or sidecar.get("error_class") is not None
            or sidecar.get("recovery_reran_model") is not False
            or sidecar.get("prompt_sha256") != _sha256_file(prompt_path)
            or sidecar.get("prompt_bytes") != prompt_bytes
            or sidecar.get("output_schema_sha256") != sha256_text(canonical_schema)
            or sidecar.get("output_schema_bytes") != schema_bytes
            or sidecar.get("output_sha256") not in acceptable_output_hashes
            or Path(str(sidecar.get("output_path") or "")).expanduser().resolve()
            != output_path
            or prompt_bytes > MAX_PROMPT_BYTES
            or schema_bytes > MAX_OUTPUT_SCHEMA_BYTES
        ):
            raise V5SelectionError("fresh calibration sidecar contract drifted")
        sidecars.append(sidecar)
    accounting = _aggregate_usage(sidecars)
    if (
        accounting["usage"] != terminal.get("usage")
        or accounting["turn_count"] != terminal.get("turn_count")
        or accounting["wall_elapsed_seconds_sum"]
        != terminal.get("wall_elapsed_seconds_sum")
    ):
        raise V5SelectionError("fresh calibration recomputed accounting drifted")
    return accounting


def verify_fresh_calibration_for_selection(
    *,
    calibration_root: Path = DEFAULT_FRESH_CALIBRATION_ROOT,
    continuation_terminal_path: Path = DEFAULT_CONTINUATION_TERMINAL,
) -> dict[str, Any]:
    root = calibration_root.expanduser().resolve()
    terminal_path = root / "terminal.json"
    terminal = _load(terminal_path, "fresh calibration terminal")
    if (
        terminal.get("schema_version") != FRESH_CALIBRATION_TERMINAL_VERSION
        or terminal.get("state") != "completed"
        or terminal.get("terminal_reason")
        != "fresh_reference_calibration_passed_selection_authorized"
        or terminal.get("reference_binding_verified") is not True
        or terminal.get("scorer_truth_binding_verified") is not True
        or terminal.get("accounting_contract_verified") is not True
        or terminal.get("calibration_passed") is not True
        or terminal.get("selection_authorized") is not True
        or terminal.get("semantic_retry_count") != 0
        or terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("turn_count") not in {23, 24}
        or terminal.get("production_mutated") is not False
    ):
        raise V5SelectionError("fresh calibration does not authorize selection")
    attempts = terminal.get("attempts")
    if (
        not isinstance(attempts, list)
        or len(attempts) != terminal["turn_count"]
        or any(
            not isinstance(attempt, Mapping)
            or attempt.get("state") != "completed"
            or attempt.get("status") != "completed"
            or attempt.get("usage_status") != "measured"
            or attempt.get("error_class") is not None
            for attempt in attempts
        )
    ):
        raise V5SelectionError("fresh calibration attempt telemetry drifted")
    accounting = _verify_fresh_calibration_attempts(attempts, terminal)
    inner_terminal_path = _verify_record(
        terminal.get("inner_terminal"), expected_path=root / "fresh-attempt/terminal.json"
    )
    _verify_record(
        terminal.get("inner_truth"),
        expected_path=root / "fresh-attempt/calibration-truth.private.json",
    )
    score_path = _verify_record(terminal.get("inner_score"))
    score = _load(score_path, "fresh calibration score")
    checks = score.get("checks")
    if (
        score.get("schema_version") != CALIBRATION_SCORE_VERSION
        or score.get("passed") is not True
        or score.get("gates") != CALIBRATION_GATES
        or not isinstance(checks, Mapping)
        or set(checks) != CALIBRATION_CHECK_KEYS
        or not all(value is True for value in checks.values())
        or score.get("proposition_and_structured_field_scores_separate") is not True
    ):
        raise V5SelectionError("fresh calibration score does not pass every frozen gate")
    inner = _load(inner_terminal_path, "inner fresh calibration terminal")
    fresh_spec = _load(root / "fresh-calibration-spec.json", "fresh calibration spec")
    inner_spec = _load(
        root / "fresh-attempt/calibration-spec.json", "inner calibration spec"
    )
    if (
        inner.get("schema_version") != CALIBRATION_TERMINAL_VERSION
        or inner.get("state") != "completed"
        or inner.get("calibration_passed") is not True
        or inner.get("selection_authorized") is not True
        or inner.get("usage") != terminal.get("usage")
        or inner.get("attempts") != attempts
        or inner.get("turn_count") != accounting["turn_count"]
        or inner.get("wall_elapsed_seconds_sum")
        != accounting["wall_elapsed_seconds_sum"]
    ):
        raise V5SelectionError("inner and outer calibration terminals disagree")
    if (
        fresh_spec.get("schema_version") != FRESH_CALIBRATION_SPEC_VERSION
        or fresh_spec.get("protocol_version") != PROTOCOL_VERSION
        or fresh_spec.get("model") != "gpt-5.6-sol"
        or fresh_spec.get("reasoning_effort") != "high"
        or fresh_spec.get("timeout_seconds") != 1200.0
        or fresh_spec.get("case_count") != 66
        or fresh_spec.get("witness_count") != 182
        or fresh_spec.get("cases_per_shard") != 6
        or fresh_spec.get("retry_count_per_turn") != 0
        or fresh_spec.get("all_semantic_turns_fresh") is not True
        or fresh_spec.get("prior_calibration_output_reuse_allowed") is not False
        or fresh_spec.get("reference_truth_exposed_to_model") is not False
        or fresh_spec.get("production_mutation_allowed") is not False
        or inner_spec.get("schema_version") != CALIBRATION_RUN_VERSION
        or inner_spec.get("execution_purpose") != "calibration"
        or inner_spec.get("protocol_version") != PROTOCOL_VERSION
        or inner_spec.get("model") != "gpt-5.6-sol"
        or inner_spec.get("reasoning_effort") != "high"
        or inner_spec.get("timeout_seconds") != 1200.0
        or inner_spec.get("case_count") != 66
        or inner_spec.get("witness_count") != 182
        or inner_spec.get("cases_per_shard") != 6
        or inner_spec.get("retry_count_per_turn") != 0
        or inner_spec.get("all_turns_fresh") is not True
        or inner_spec.get("calibration_v1_output_reuse_allowed") is not False
        or inner_spec.get("provisional_truth_exposed_to_model") is not False
        or inner_spec.get("calibration_gates") != CALIBRATION_GATES
        or inner_spec.get("production_mutation_allowed") is not False
    ):
        raise V5SelectionError("fresh calibration prompt or schema contract drifted")
    continuation_path = continuation_terminal_path.expanduser().resolve()
    continuation = _load(continuation_path, "reference-to-calibration continuation")
    if (
        continuation.get("schema_version") != CONTINUATION_TERMINAL_VERSION
        or continuation.get("state") != "completed"
        or continuation.get("terminal_reason")
        != "fresh_calibration_passed_selection_ready"
        or continuation.get("calibration_passed") is not True
        or continuation.get("selection_authorized") is not True
        or continuation.get("production_mutated") is not False
        or _verify_record(
            continuation.get("calibration_terminal"), expected_path=terminal_path
        )
        != terminal_path
    ):
        raise V5SelectionError("continuation terminal does not authorize selection")
    return {
        "terminal": terminal,
        "score": score,
        "records": {
            "outer_terminal": _record(terminal_path),
            "inner_terminal": _record(inner_terminal_path),
            "inner_truth": terminal["inner_truth"],
            "score": terminal["inner_score"],
            "continuation_terminal": _record(continuation_path),
            "fresh_spec": _record(root / "fresh-calibration-spec.json"),
            "inner_spec": _record(root / "fresh-attempt/calibration-spec.json"),
        },
    }


def _request_bytes(prompt: str, schema: Mapping[str, Any]) -> tuple[int, int]:
    return len(prompt.encode("utf-8")), len(_canonical_json(schema).encode("utf-8"))


def _freeze_large_request(
    *,
    root: Path,
    turn_name: str,
    input_value: Mapping[str, Any],
    prompt: str,
    schema: Mapping[str, Any],
) -> dict[str, Path]:
    prompt_bytes, schema_bytes = _request_bytes(prompt, schema)
    if prompt_bytes > MAX_PROMPT_BYTES or schema_bytes > MAX_OUTPUT_SCHEMA_BYTES:
        raise V5SelectionError("selection judge request exceeds frozen byte caps")
    paths = _turn_paths(root, turn_name)
    _write_immutable_json(paths["input"], input_value)
    _write_immutable_text(paths["prompt"], prompt)
    _write_immutable_json(paths["schema"], schema)
    return paths


def plan_pointwise_selection_shards(pool: Mapping[str, Any]) -> list[dict[str, Any]]:
    full = build_pointwise_support_input(pool)
    rows = []
    for index, case in enumerate(pool.get("cases") or []):
        case_id = str(case["case_id"])
        units = [deepcopy(unit) for unit in full["units"] if unit["case_id"] == case_id]
        if not units:
            continue
        value = {**deepcopy(full), "units": units}
        prompt = build_pointwise_support_prompt(value)
        schema = pointwise_support_output_schema(value)
        prompt_bytes, schema_bytes = _request_bytes(prompt, schema)
        if prompt_bytes > MAX_PROMPT_BYTES or schema_bytes > MAX_OUTPUT_SCHEMA_BYTES:
            raise V5SelectionError("one pointwise case exceeds frozen byte caps")
        rows.append(
            {
                "index": index,
                "case_ids": [case_id],
                "input": value,
                "prompt": prompt,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
                "witness_count": len(units),
            }
        )
    expected_witnesses = {unit["witness_id"] for unit in full["units"]}
    observed_witnesses = {
        unit["witness_id"] for shard in rows for unit in shard["input"]["units"]
    }
    if observed_witnesses != expected_witnesses:
        raise V5SelectionError("pointwise selection shards do not partition witnesses")
    return rows


def plan_alignment_selection_shards(
    *,
    pool: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
    case_ids: Sequence[str],
    permutation: str,
) -> list[dict[str, Any]]:
    requested = list(case_ids)
    if len(requested) != len(set(requested)):
        raise V5SelectionError("alignment selection case IDs are duplicated")
    planned = []
    current: list[str] = []

    def render(ids: Sequence[str]) -> tuple[dict[str, Any], str, dict[str, Any], int, int]:
        value = build_neutral_alignment_input(
            pool, support_receipts, case_ids=ids, permutation=permutation
        )
        prompt = build_neutral_alignment_prompt(value)
        schema = neutral_alignment_output_schema(value)
        prompt_bytes, schema_bytes = _request_bytes(prompt, schema)
        return value, prompt, schema, prompt_bytes, schema_bytes

    for case_id in requested:
        candidate = current + [case_id]
        value, prompt, schema, prompt_bytes, schema_bytes = render(candidate)
        if current and (
            len(candidate) > MAX_ALIGNMENT_CASES_PER_SHARD
            or prompt_bytes > MAX_PROMPT_BYTES
            or schema_bytes > MAX_OUTPUT_SCHEMA_BYTES
        ):
            prior = render(current)
            planned.append((list(current), *prior))
            current = [case_id]
            value, prompt, schema, prompt_bytes, schema_bytes = render(current)
        else:
            current = candidate
        if prompt_bytes > MAX_PROMPT_BYTES or schema_bytes > MAX_OUTPUT_SCHEMA_BYTES:
            raise V5SelectionError("one alignment case exceeds frozen byte caps")
    if current:
        planned.append((list(current), *render(current)))
    rows = []
    for index, (ids, value, prompt, schema, prompt_bytes, schema_bytes) in enumerate(planned):
        rows.append(
            {
                "index": index,
                "case_ids": ids,
                "input": value,
                "prompt": prompt,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
            }
        )
    observed = [case_id for row in rows for case_id in row["case_ids"]]
    if observed != requested:
        raise V5SelectionError("alignment selection shards do not cover requested cases")
    return rows


def _merge_outputs(outputs: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    values = [deepcopy(row) for output in outputs for row in output.get(key) or []]
    identity = [
        (str(row.get("case_id")), str(row.get("witness_id") or "")) for row in values
    ]
    if len(identity) != len(set(identity)):
        raise V5SelectionError("selection judge shard outputs overlap")
    return {key: values}


def _selection_attempt_records(
    root: Path, *, invoked_turn_name: Optional[str] = None
) -> list[dict[str, Any]]:
    planned = _attempt_records(root)
    attempts = [
        row
        for row in planned
        if any(row.get(key) is not None for key in ("capacity", "sidecar", "output"))
    ]
    attempted_names = {str(row["turn_name"]) for row in attempts}
    if invoked_turn_name is not None and invoked_turn_name not in attempted_names:
        attempts.append(
            {
                "turn_name": invoked_turn_name,
                "capacity": None,
                "sidecar": None,
                "output": None,
                "state": "unknown",
                "status": "unknown",
                "error_class": "turn_invoked_without_terminal_checkpoint",
                "usage_status": "unknown",
            }
        )
    return sorted(attempts, key=lambda row: str(row["turn_name"]))


def _selection_unstarted_request_names(
    root: Path, attempts: Sequence[Mapping[str, Any]]
) -> list[str]:
    attempted = {str(row["turn_name"]) for row in attempts}
    planned = {str(row["turn_name"]) for row in _attempt_records(root)}
    return sorted(planned - attempted)


def _known_usage_from_attempts(
    attempts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    sidecars = []
    for attempt in attempts:
        record = attempt.get("sidecar")
        if attempt.get("usage_status") == "measured" and isinstance(record, Mapping):
            sidecars.append(_load(Path(str(record["path"])), "measured sidecar"))
    known = _aggregate_usage(sidecars)
    return {
        "known_measured_usage": known["usage"],
        "known_measured_turn_count": known["turn_count"],
        "known_measured_wall_elapsed_seconds_sum": known[
            "wall_elapsed_seconds_sum"
        ],
    }


def _merge_alignment_inputs(
    *, pool: Mapping[str, Any], receipts: Mapping[str, Any], case_ids: Sequence[str], permutation: str
) -> dict[str, Any]:
    return build_neutral_alignment_input(
        pool, receipts, case_ids=case_ids, permutation=permutation
    )


def validate_selection_alignment_output(
    output: Any, alignment_input: Mapping[str, Any]
) -> list[str]:
    errors = validate_neutral_alignment_output(output, alignment_input)
    if errors:
        return errors
    consistency_errors = []
    for case_index, case in enumerate(output["cases"]):
        group_by_witness = {}
        for group_index, group in enumerate(case["equivalence_groups"]):
            for witness_id in group["witness_ids"]:
                group_by_witness[witness_id] = group_index
        for pair_index, pair in enumerate(case["alignment_pairs"]):
            same_group = (
                group_by_witness[pair["witness_id_1"]]
                == group_by_witness[pair["witness_id_2"]]
            )
            equivalent = pair["relation"] == "equivalent"
            if same_group != equivalent:
                consistency_errors.append(
                    f"case_{case_index}_pair_{pair_index}_relation_partition_conflict"
                )
    return consistency_errors


def find_selection_alignment_disagreements(
    *,
    base_output: Mapping[str, Any],
    base_input: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    canary_input: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
) -> dict[str, Any]:
    base = normalize_neutral_alignment_output(base_output, base_input)
    canary = normalize_neutral_alignment_output(canary_output, canary_input)
    base_by_case = {row["case_id"]: row for row in base["cases"]}
    canary_by_case = {row["case_id"]: row for row in canary["cases"]}
    if not set(canary_by_case) <= set(base_by_case):
        raise V5SelectionError("alignment canary contains an unknown case")
    support = {
        str(row["witness_id"]): row for row in support_receipts.get("units") or []
    }
    expected_witnesses = {
        witness_id
        for case in base_by_case.values()
        for group in case["equivalence_groups"]
        for witness_id in group
    }
    if not expected_witnesses <= set(support):
        raise V5SelectionError("support receipts do not cover aligned witnesses")
    reasons_by_case: dict[str, set[str]] = {}
    for case_id, canary_case in canary_by_case.items():
        if base_by_case[case_id] != canary_case:
            reasons_by_case.setdefault(case_id, set()).add(
                "permutation_output_changed"
            )
    for case_id, case in base_by_case.items():
        for group in case["equivalence_groups"]:
            proposition_verdicts = {
                support[witness_id]["proposition_verdict"]
                for witness_id in group
            }
            structured_verdicts = {
                support[witness_id]["structured_field_verdict"]
                for witness_id in group
            }
            if len(proposition_verdicts - {"abstain"}) > 1:
                reasons_by_case.setdefault(case_id, set()).add(
                    "support_alignment_proposition_conflict"
                )
            if len(structured_verdicts - {"abstain"}) > 1:
                reasons_by_case.setdefault(case_id, set()).add(
                    "support_alignment_structured_field_conflict"
                )
    rows = [
        {"case_id": case_id, "reasons": sorted(reasons)}
        for case_id, reasons in sorted(reasons_by_case.items())
    ]
    return {
        "schema_version": "pif_app_server_v5_selection_disagreements_v1",
        "disagreement_case_count": len(rows),
        "disagreements": rows,
        "adjudication_required": bool(rows),
        "adjudication_call_cap": 1,
        "majority_voting_used": False,
    }


def build_selection_adjudication_packet(
    *,
    base_output: Mapping[str, Any],
    base_input: Mapping[str, Any],
    canary_output: Mapping[str, Any],
    canary_input: Mapping[str, Any],
    disagreements: Mapping[str, Any],
) -> dict[str, Any]:
    base = {
        row["case_id"]: row
        for row in normalize_neutral_alignment_output(base_output, base_input)[
            "cases"
        ]
    }
    canary = {
        row["case_id"]: row
        for row in normalize_neutral_alignment_output(canary_output, canary_input)[
            "cases"
        ]
    }
    base_cases = {row["case_id"]: row for row in base_input["cases"]}
    rows = []
    for disagreement in disagreements.get("disagreements") or []:
        case_id = str(disagreement["case_id"])
        if case_id not in base or case_id not in base_cases:
            raise V5SelectionError("adjudication disagreement is outside base alignment")
        row = {
            **deepcopy(base_cases[case_id]),
            "observed_disagreement_reasons": list(disagreement["reasons"]),
            "anonymous_candidate_1": base[case_id],
        }
        if case_id in canary:
            row["anonymous_candidate_2"] = canary[case_id]
        rows.append(row)
    return {
        "schema_version": ADJUDICATION_INPUT_VERSION,
        "cases": rows,
        "adjudication_required": bool(rows),
        "call_cap": 1,
        "candidate_order_has_no_vote_meaning": True,
    }


def _reconcile_selection_alignment(
    *,
    base_output: Mapping[str, Any],
    base_input: Mapping[str, Any],
    disagreements: Mapping[str, Any],
    adjudication_output: Optional[Mapping[str, Any]],
    adjudication_input: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    base = normalize_neutral_alignment_output(base_output, base_input)
    disagreement_ids = {
        str(item["case_id"]) for item in disagreements.get("disagreements") or []
    }
    adjudicated = {}
    if adjudication_output is not None:
        if adjudication_input is None:
            raise V5SelectionError("adjudication output has no frozen input")
        errors = validate_selection_alignment_output(
            adjudication_output, adjudication_input
        )
        if errors:
            raise V5SelectionError("selection adjudication output is invalid")
        normalized = normalize_neutral_alignment_output(
            adjudication_output, adjudication_input
        )
        adjudicated = {row["case_id"]: row for row in normalized["cases"]}
        if set(adjudicated) != disagreement_ids:
            raise V5SelectionError("selection adjudication coverage drifted")
    cases = []
    for row in base["cases"]:
        case_id = row["case_id"]
        if case_id not in disagreement_ids:
            cases.append({"case_id": case_id, "status": "accepted_base", **row})
        elif case_id in adjudicated:
            cases.append(
                {"case_id": case_id, "status": "adjudicated", **adjudicated[case_id]}
            )
        else:
            cases.append(
                {
                    "case_id": case_id,
                    "status": "abstain",
                    "equivalence_groups": [],
                    "alignment_pairs": [],
                    "unpaired_witness_ids": [],
                }
            )
    return {
        "schema_version": "pif_app_server_v5_selection_reconciled_alignment_v1",
        "cases": cases,
        "observable_disagreement_case_count": len(disagreement_ids),
        "adjudication_call_count": int(bool(adjudicated)),
        "adjudication_call_cap": 1,
        "majority_voting_used": False,
        "unresolved_cases_abstained": sorted(
            row["case_id"] for row in cases if row["status"] == "abstain"
        ),
    }


def project_v5_selection_consensus(
    *,
    pool: Mapping[str, Any],
    support_receipts: Mapping[str, Any],
    reconciled_alignment: Mapping[str, Any],
) -> dict[str, Any]:
    support_by_case: dict[str, list[dict[str, Any]]] = {}
    for row in support_receipts.get("units") or []:
        support_by_case.setdefault(str(row["case_id"]), []).append(
            {"witness_id": row["witness_id"], "verdict": row["proposition_verdict"]}
        )
    witnesses_by_case = {
        str(case["case_id"]): sorted(
            str(witness["witness_id"])
            for side in ("a", "b")
            for witness in case.get(f"event_set_{side}") or []
        )
        for case in pool.get("cases") or []
    }
    alignment_by_case = {
        str(row["case_id"]): row for row in reconciled_alignment.get("cases") or []
    }
    if set(alignment_by_case) != set(witnesses_by_case):
        raise V5SelectionError("reconciled selection alignment coverage drifted")
    cases = []
    for case in pool["cases"]:
        case_id = str(case["case_id"])
        aligned = alignment_by_case[case_id]
        witness_ids = witnesses_by_case[case_id]
        supports = sorted(
            support_by_case.get(case_id, []), key=lambda row: row["witness_id"]
        )
        if [row["witness_id"] for row in supports] != witness_ids:
            raise V5SelectionError("selection support receipts do not cover every witness")
        abstained = aligned.get("status") == "abstain"
        cases.append(
            {
                "case_id": case_id,
                "status": "abstain" if abstained else "completed",
                "support_results": supports,
                "alignment_results": [
                    {
                        "left_witness_id": pair["witness_ids"][0],
                        "right_witness_id": pair["witness_ids"][1],
                        "relation": pair["relation"],
                        "mismatch_fields": pair["mismatch_fields"],
                    }
                    for pair in aligned.get("alignment_pairs") or []
                ],
                "alignment_abstained_witness_ids": witness_ids if abstained else [],
                "equivalence_groups": (
                    [] if abstained else aligned.get("equivalence_groups") or []
                ),
                "partition_abstained_witness_ids": witness_ids if abstained else [],
            }
        )
    abstained_cases = sum(row["status"] == "abstain" for row in cases)
    support_abstentions = sum(
        item["verdict"] == "abstain" for row in cases for item in row["support_results"]
    )
    alignment_labels = [
        item for row in cases for item in row["alignment_results"]
    ]

    def ratio(numerator: int, denominator: int) -> float:
        return round(numerator / denominator, 6) if denominator else 0.0

    return {
        "schema_version": JUDGE_CONSENSUS_VERSION,
        "pool_sha256": sha256_text(_canonical_json(pool)),
        "cases": cases,
        "abstentions": {
            "support": {
                "numerator": support_abstentions,
                "denominator": sum(len(row["support_results"]) for row in cases),
                "rate": ratio(
                    support_abstentions,
                    sum(len(row["support_results"]) for row in cases),
                ),
            },
            "alignment_labels": {
                "numerator": sum(
                    item["relation"] == "abstain" for item in alignment_labels
                ),
                "denominator": len(alignment_labels),
                "rate": ratio(
                    sum(item["relation"] == "abstain" for item in alignment_labels),
                    len(alignment_labels),
                ),
            },
            "alignment_topology": {
                "numerator": abstained_cases,
                "denominator": len(cases),
                "rate": ratio(abstained_cases, len(cases)),
            },
            "equivalence_partition": {
                "numerator": abstained_cases,
                "denominator": len(cases),
                "rate": ratio(abstained_cases, len(cases)),
            },
            "cases": {
                "numerator": abstained_cases,
                "denominator": len(cases),
                "rate": ratio(abstained_cases, len(cases)),
            },
        },
        "selection_admissible": True,
        "abstention_gate_applied": False,
        "source_protocol_version": PROTOCOL_VERSION,
    }


async def run_v5_selection_judge(
    *,
    pool: Mapping[str, Any],
    output_dir: Path,
    model: str,
    reasoning_effort: str,
    timeout_seconds: float,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
    **_unused: Any,
) -> dict[str, Any]:
    if model != "gpt-5.6-sol" or reasoning_effort != "high" or timeout_seconds != 1200.0:
        raise V5SelectionError("selection judge model, effort, and timeout are frozen")
    root = output_dir.expanduser().resolve()
    report_path = root / "report.json"
    if report_path.exists():
        return _load(report_path, "v5 selection judge report")
    root.mkdir(parents=True, exist_ok=True)
    full_pointwise_input = build_pointwise_support_input(pool)
    pointwise_shards = plan_pointwise_selection_shards(pool)
    case_ids = [str(case["case_id"]) for case in pool["cases"]]
    canary_ids = case_ids[::CANARY_STRIDE]
    if len(case_ids) != 32 or len(canary_ids) != 8:
        raise V5SelectionError("selection judge case or canary coverage drifted")
    requests = []
    for index, shard in enumerate(pointwise_shards):
        turn_name = f"selection_pointwise_case_{index:02d}"
        paths = _freeze_large_request(
            root=root,
            turn_name=turn_name,
            input_value=shard["input"],
            prompt=shard["prompt"],
            schema=shard["schema"],
        )
        requests.append({**shard, "turn_name": turn_name, "paths": paths})
    spec_path = root / "selection-judge-spec.json"
    prior_spec = (
        _load(spec_path, "existing selection judge spec")
        if spec_path.is_file()
        else None
    )
    spec = {
        "schema_version": SELECTION_JUDGE_VERSION,
        "state": "frozen_before_selection_judge_calls",
        "created_at": prior_spec.get("created_at") if prior_spec else now_iso(),
        "protocol_version": PROTOCOL_VERSION,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "timeout_seconds": timeout_seconds,
        "transport": "official_persistent_codex_app_server_stdio_managed_chatgpt_auth",
        "case_count": 32,
        "witness_count": len(full_pointwise_input["units"]),
        "pointwise_turn_count": len(requests),
        "pointwise_cases_per_turn": 1,
        "alignment_cases_per_turn_maximum": MAX_ALIGNMENT_CASES_PER_SHARD,
        "canary_case_ids": canary_ids,
        "canary_case_count": 8,
        "retry_count_per_turn": 0,
        "adjudication_call_cap": 1,
        "prompt_byte_cap": MAX_PROMPT_BYTES,
        "output_schema_byte_cap": MAX_OUTPUT_SCHEMA_BYTES,
        "support_is_side_free": True,
        "alignment_is_origin_neutral": True,
        "majority_voting_allowed": False,
        "reference_truth_exposed_to_model": False,
        "production_mutation_allowed": False,
        "pointwise_requests": [
            {
                "turn_name": request["turn_name"],
                "case_ids": request["case_ids"],
                "witness_count": request["witness_count"],
                "prompt_bytes": request["prompt_bytes"],
                "schema_bytes": request["schema_bytes"],
                "input": _record(request["paths"]["input"]),
                "prompt": _record(request["paths"]["prompt"]),
                "schema": _record(request["paths"]["schema"]),
            }
            for request in requests
        ],
    }
    _write_immutable_json(spec_path, spec)
    sidecars = []
    adopted: dict[str, bool] = {}
    current_turn = None
    try:
        async with client_factory() as client:
            pointwise_outputs = []
            for request in requests:
                current_turn = request["turn_name"]
                output, sidecar, was_adopted = await _run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=request["paths"],
                    prompt=request["prompt"],
                    schema=request["schema"],
                    base_instructions=pointwise_support_base_instructions(),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=request["witness_count"],
                    validator=lambda value, item=request: validate_pointwise_support_output(
                        value, item["input"]
                    ),
                )
                pointwise_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            pointwise_output = _merge_outputs(pointwise_outputs, "units")
            if validate_pointwise_support_output(pointwise_output, full_pointwise_input):
                raise V5SelectionError("merged pointwise selection output is invalid")
            support_receipts = freeze_support_receipts(
                pointwise_output, full_pointwise_input
            )
            support_path = root / "support-receipts.private.json"
            _write_immutable_json(support_path, support_receipts)
            base_shards = plan_alignment_selection_shards(
                pool=pool,
                support_receipts=support_receipts,
                case_ids=case_ids,
                permutation="base",
            )
            base_outputs = []
            for index, shard in enumerate(base_shards):
                current_turn = f"selection_alignment_base_{index:02d}"
                paths = _freeze_large_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=shard["input"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                )
                output, sidecar, was_adopted = await _run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=neutral_alignment_base_instructions(),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["case_ids"]),
                    validator=lambda value, item=shard: validate_selection_alignment_output(
                        value, item["input"]
                    ),
                )
                base_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            base_output = _merge_outputs(base_outputs, "cases")
            base_input = _merge_alignment_inputs(
                pool=pool, receipts=support_receipts, case_ids=case_ids, permutation="base"
            )
            canary_shards = plan_alignment_selection_shards(
                pool=pool,
                support_receipts=support_receipts,
                case_ids=canary_ids,
                permutation="balanced_canary",
            )
            canary_outputs = []
            for index, shard in enumerate(canary_shards):
                current_turn = f"selection_alignment_canary_{index:02d}"
                paths = _freeze_large_request(
                    root=root,
                    turn_name=current_turn,
                    input_value=shard["input"],
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                )
                output, sidecar, was_adopted = await _run_turn(
                    client=client,
                    turn_name=current_turn,
                    paths=paths,
                    prompt=shard["prompt"],
                    schema=shard["schema"],
                    base_instructions=neutral_alignment_base_instructions(),
                    model=model,
                    reasoning_effort=reasoning_effort,
                    timeout_seconds=timeout_seconds,
                    batch_size=len(shard["case_ids"]),
                    validator=lambda value, item=shard: validate_selection_alignment_output(
                        value, item["input"]
                    ),
                )
                canary_outputs.append(output)
                sidecars.append(sidecar)
                adopted[current_turn] = was_adopted
            canary_output = _merge_outputs(canary_outputs, "cases")
            canary_input = _merge_alignment_inputs(
                pool=pool,
                receipts=support_receipts,
                case_ids=canary_ids,
                permutation="balanced_canary",
            )
            disagreements = find_selection_alignment_disagreements(
                base_output=base_output,
                base_input=base_input,
                canary_output=canary_output,
                canary_input=canary_input,
                support_receipts=support_receipts,
            )
            disagreement_path = root / "observable-disagreements.private.json"
            _write_immutable_json(disagreement_path, disagreements)
            adjudication_output = None
            adjudication_input = None
            adjudication_omitted_reason = None
            if disagreements["adjudication_required"]:
                packet = build_selection_adjudication_packet(
                    base_input=base_input,
                    base_output=base_output,
                    canary_output=canary_output,
                    canary_input=canary_input,
                    disagreements=disagreements,
                )
                adjudication_input = adjudication_alignment_input(
                    base_input=base_input, adjudication_input=packet
                )
                prompt = build_disagreement_adjudication_prompt(
                    adjudication_input=packet,
                    adjudication_alignment=adjudication_input,
                )
                schema = neutral_alignment_output_schema(adjudication_input)
                prompt_bytes, schema_bytes = _request_bytes(prompt, schema)
                if prompt_bytes <= MAX_PROMPT_BYTES and schema_bytes <= MAX_OUTPUT_SCHEMA_BYTES:
                    current_turn = "selection_disagreement_adjudication"
                    paths = _freeze_large_request(
                        root=root,
                        turn_name=current_turn,
                        input_value={
                            "adjudication_packet": packet,
                            "alignment_input": adjudication_input,
                        },
                        prompt=prompt,
                        schema=schema,
                    )
                    adjudication_output, sidecar, was_adopted = await _run_turn(
                        client=client,
                        turn_name=current_turn,
                        paths=paths,
                        prompt=prompt,
                        schema=schema,
                        base_instructions=neutral_alignment_base_instructions(),
                        model=model,
                        reasoning_effort=reasoning_effort,
                        timeout_seconds=timeout_seconds,
                        batch_size=len(adjudication_input["cases"]),
                        validator=lambda value: validate_selection_alignment_output(
                            value, adjudication_input
                        ),
                    )
                    sidecars.append(sidecar)
                    adopted[current_turn] = was_adopted
                else:
                    adjudication_omitted_reason = "disagreement_packet_exceeded_frozen_byte_cap"
        reconciled = _reconcile_selection_alignment(
            base_output=base_output,
            base_input=base_input,
            disagreements=disagreements,
            adjudication_output=adjudication_output,
            adjudication_input=adjudication_input,
        )
        reconciled_path = root / "reconciled-alignment.private.json"
        _write_immutable_json(reconciled_path, reconciled)
        consensus = project_v5_selection_consensus(
            pool=pool,
            support_receipts=support_receipts,
            reconciled_alignment=reconciled,
        )
        consensus_path = root / "consensus.private.json"
        _write_immutable_json(consensus_path, consensus)
        accounting = _aggregate_usage(sidecars)
        attempts = _selection_attempt_records(root)
        if len(attempts) != len(sidecars):
            raise V5SelectionError("selection judge attempt accounting drifted")
        report = {
            "schema_version": DEV_FULL_JUDGE_VERSION,
            "v5_selection_judge_schema_version": SELECTION_JUDGE_TERMINAL_VERSION,
            "state": "completed",
            "terminal_reason": "selection_judge_completed",
            "model": model,
            "reasoning_effort": reasoning_effort,
            "case_count": 32,
            "witness_count": len(full_pointwise_input["units"]),
            "pointwise_shard_count": len(pointwise_shards),
            "base_alignment_shard_count": len(base_shards),
            "canary_alignment_shard_count": len(canary_shards),
            "adjudication_omitted_reason": adjudication_omitted_reason,
            "semantic_retry_count": 0,
            "accounting_complete": accounting["accounting_complete"],
            "usage_status": accounting["usage_status"],
            "usage": accounting["usage"],
            "turn_count": accounting["turn_count"],
            "wall_elapsed_seconds_sum": accounting["wall_elapsed_seconds_sum"],
            "abstentions": consensus["abstentions"],
            "consensus_path": str(consensus_path),
            "consensus_sha256": _sha256_file(consensus_path),
            "support_receipts": _record(support_path),
            "reconciled_alignment": _record(reconciled_path),
            "observable_disagreements": _record(disagreement_path),
            "attempts": attempts,
            "unstarted_request_names": _selection_unstarted_request_names(
                root, attempts
            ),
            "completed_checkpoint_adoptions": adopted,
            "selection_admissible": True,
            "production_mutation_performed": False,
            "privacy": "hashes_counts_usage_and_private_artifact_paths_no_source_or_event_text",
        }
        _write_immutable_json(report_path, report)
        return report
    except JudgeV5DiagnosticAttemptFailed as exc:
        error_class = exc.error_class
        current_turn = exc.turn_name
    except Exception as exc:
        error_class = type(exc).__name__
    attempts = _selection_attempt_records(root, invoked_turn_name=current_turn)
    known_usage = _known_usage_from_attempts(attempts)
    usage_measured = all(
        attempt.get("usage_status") == "measured" for attempt in attempts
    )
    if usage_measured:
        measured_sidecars = [
            _load(Path(str(attempt["sidecar"]["path"])), "measured sidecar")
            for attempt in attempts
        ]
        accounting = _aggregate_usage(measured_sidecars)
    else:
        accounting = {
            "accounting_complete": False,
            "usage_status": "unknown",
            "usage": None,
            "turn_count": len(attempts),
            "wall_elapsed_seconds_sum": None,
            **known_usage,
        }
    report = {
        "schema_version": DEV_FULL_JUDGE_VERSION,
        "v5_selection_judge_schema_version": SELECTION_JUDGE_TERMINAL_VERSION,
        "state": "failed",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "failed_turn_name": current_turn,
        "error_class": error_class,
        "attempts": attempts,
        "unstarted_request_names": _selection_unstarted_request_names(root, attempts),
        "completed_checkpoint_adoptions": adopted,
        "semantic_retry_count": 0,
        "selection_admissible": False,
        "production_mutation_performed": False,
        **accounting,
    }
    _write_immutable_json(report_path, report)
    raise V5SelectionError("selection judge attempt failed")


async def _run_turn(
    *,
    client: Any,
    turn_name: str,
    paths: Mapping[str, Path],
    prompt: str,
    schema: Mapping[str, Any],
    base_instructions: str,
    model: str,
    reasoning_effort: str,
    timeout_seconds: float,
    batch_size: int,
    validator: Callable[[Any], Sequence[str]],
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    from .app_server_judge_v5_diagnostic import _get_or_run_turn

    return await _get_or_run_turn(
        client=client,
        turn_name=turn_name,
        paths=paths,
        prompt=prompt,
        schema=schema,
        base_instructions=base_instructions,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds,
        batch_size=batch_size,
        output_validator=validator,
    )


async def run_v5_five_arm_selection(
    *,
    repo_root: Path,
    reuse_contract_path: Path = DEFAULT_REUSE_CONTRACT,
    fresh_calibration_root: Path = DEFAULT_FRESH_CALIBRATION_ROOT,
    continuation_terminal_path: Path = DEFAULT_CONTINUATION_TERMINAL,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
) -> dict[str, Any]:
    repo = repo_root.expanduser().resolve()
    if Path.cwd().resolve() != repo:
        raise V5SelectionError("selection must run from its hash-bound repository root")
    root = output_dir.expanduser().resolve()
    outer_terminal_path = root / "v5-selection-terminal.json"
    if outer_terminal_path.exists():
        return _load(outer_terminal_path, "v5 selection terminal")
    fresh = verify_fresh_calibration_for_selection(
        calibration_root=fresh_calibration_root,
        continuation_terminal_path=continuation_terminal_path,
    )
    contract = verify_v5_reuse_contract(reuse_contract_path.expanduser().resolve())
    inputs = contract["selection_inputs"]
    manifest_path = Path(inputs["development_manifest"]["path"])
    context_path = Path(inputs["context_usage_recovery"]["path"])
    run_spec_path = Path(inputs["extraction_run_spec"]["path"])
    witness_root = Path(inputs["preassembled_assembly_report"]["path"]).parent
    itt_path = Path(contract["interrupted_batch_5_same_thread"]["path"])
    arm_paths = [Path(item["report"]["path"]) for item in contract["clean_arms"]]
    root.mkdir(parents=True, exist_ok=True)
    attempt_spec_path = root / "v5-selection-attempt-spec.json"
    prior_attempt_spec = (
        _load(attempt_spec_path, "existing v5 selection attempt spec")
        if attempt_spec_path.is_file()
        else None
    )
    attempt_spec = {
        "schema_version": SELECTION_ATTEMPT_SPEC_VERSION,
        "state": "frozen_before_selection_model_calls",
        "created_at": (
            prior_attempt_spec.get("created_at")
            if prior_attempt_spec
            else now_iso()
        ),
        "protocol_version": PROTOCOL_VERSION,
        "selection_adapter_source": _record(Path(__file__).resolve()),
        "judge_protocol_source": _record(
            Path(__file__).with_name("app_server_judge_v5.py").resolve()
        ),
        "deterministic_selection_source": _record(
            Path(__file__).with_name("app_server_dev_selection.py").resolve()
        ),
        "fresh_calibration_records": fresh["records"],
        "reuse_contract": _record(reuse_contract_path),
        "manifest": _record(manifest_path),
        "preassembled_witness_pool": inputs["preassembled_witness_pool"],
        "clean_arm_reports": [item["report"] for item in contract["clean_arms"]],
        "interrupted_arm_intent_to_treat": contract[
            "interrupted_batch_5_same_thread"
        ],
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "timeout_seconds": 1200.0,
        "retry_count_per_turn": 0,
        "extraction_model_calls_allowed": False,
        "old_judge_outputs_reused": False,
        "one_shared_augmented_reference": True,
        "production_mutation_allowed": False,
    }
    _write_immutable_json(attempt_spec_path, attempt_spec)

    async def calibration_adoption(**kwargs: Any) -> dict[str, Any]:
        calibration_dir = Path(kwargs["output_dir"]).expanduser().resolve()
        calibration_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "schema_version": "pif_app_server_v5_calibration_adoption_v1",
            "state": "completed",
            "calibrated": True,
            "fail_closed_reason": None,
            "fresh_model_calls_performed": 0,
            "source_fresh_calibration": fresh["records"]["outer_terminal"],
            "source_calibration_score": fresh["records"]["score"],
            "usage": fresh["terminal"]["usage"],
            "usage_status": "complete",
            "accounting_complete": True,
            "selection_authorized": True,
            "production_mutation_performed": False,
        }
        _write_immutable_json(calibration_dir / "report.json", report)
        return report

    original_full_judge = dev_selection.run_full_judge_shards
    dev_selection.run_full_judge_shards = run_v5_selection_judge
    try:
        conn = _read_only_connection(db_path())
        try:
            result = await run_app_server_dev_selection(
                conn,
                manifest_path=manifest_path,
                arm_report_paths=arm_paths,
                context_usage_recovery_path=context_path,
                output_dir=root / "selection",
                selection_output_path=root / "selection/selection-result.json",
                run_spec_path=run_spec_path,
                interrupted_arm_provenance_path=itt_path,
                judge_model="gpt-5.6-sol",
                judge_reasoning_effort="high",
                judge_timeout_seconds=1200.0,
                preassembled_witness_root=witness_root,
                selection_context={
                    "schema_version": "pif_app_server_v5_reference_bound_selection_v1",
                    "fresh_calibration_terminal_sha256": fresh["records"][
                        "outer_terminal"
                    ]["sha256"],
                    "protocol_version": PROTOCOL_VERSION,
                    "extraction_model_calls_allowed": False,
                    "old_judge_outputs_reused": False,
                    "batch_5_same_thread_retry_allowed": False,
                },
                client_factory=client_factory,
                calibration_runner=calibration_adoption,
            )
        finally:
            conn.close()
    finally:
        dev_selection.run_full_judge_shards = original_full_judge
    selection_path = root / "selection/selection-result.json"
    frozen_winner = result.get("selection_status") == "frozen_winner"
    terminal = {
        "schema_version": SELECTION_ATTEMPT_TERMINAL_VERSION,
        "state": "completed" if frozen_winner else "stopped",
        "terminal_at": now_iso(),
        "terminal_reason": (
            "five_arm_selection_passed_winner_frozen"
            if frozen_winner
            else result.get("terminal_classification")
            or "development_quality_or_cost_gate_not_passed"
        ),
        "attempt_spec": _record(attempt_spec_path),
        "selection_result": _record(selection_path),
        "winner_frozen": frozen_winner,
        "holdout_preparation_authorized": bool(
            frozen_winner and result.get("holdout_preparation_authorized") is True
        ),
        "holdout_model_calls_authorized": False,
        "production_mutated": False,
    }
    if (root / "selection/full-judge/report.json").is_file():
        terminal["selection_judge_report"] = _record(
            root / "selection/full-judge/report.json"
        )
    _write_immutable_json(outer_terminal_path, terminal)
    return terminal


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run frozen v5 judge over the five-arm development matrix"
    )
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--reuse-contract", default=str(DEFAULT_REUSE_CONTRACT))
    parser.add_argument(
        "--fresh-calibration-root", default=str(DEFAULT_FRESH_CALIBRATION_ROOT)
    )
    parser.add_argument(
        "--continuation-terminal", default=str(DEFAULT_CONTINUATION_TERMINAL)
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args(argv)
    terminal = asyncio.run(
        run_v5_five_arm_selection(
            repo_root=Path(args.repo_root),
            reuse_contract_path=Path(args.reuse_contract),
            fresh_calibration_root=Path(args.fresh_calibration_root),
            continuation_terminal_path=Path(args.continuation_terminal),
            output_dir=Path(args.output_dir),
        )
    )
    print(
        json.dumps(
            {
                "state": terminal["state"],
                "terminal_reason": terminal["terminal_reason"],
                "winner_frozen": terminal.get("winner_frozen", False),
            },
            sort_keys=True,
        )
    )
    return 0 if terminal.get("winner_frozen") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
