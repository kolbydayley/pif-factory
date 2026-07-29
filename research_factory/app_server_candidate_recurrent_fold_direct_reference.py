from __future__ import annotations

"""Direct full-output semantic evaluation for the ordered recurrent-fold canary.

The evaluator performs no extraction and consumes no pointwise semantic result.
It checksum-binds the structural canary terminal and its normalized output,
rebuilds a complete baseline/canary witness container from the raw outputs, and
submits every event to the mature full-event AB/BA judge.  Deterministic work is
limited to artifact integrity, exact event/evidence lineage, opaque blinding,
accounting, locking, and symmetric scoring.
"""

import argparse
import asyncio
import fcntl
import hashlib
import json
from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from . import app_server_candidate_shared_reference_repair as shared_repair
from . import app_server_candidate_ordered_recurrent_full_schema_fold_canary as canary
from . import app_server_candidate_true_full_output_repair as prior
from . import app_server_capacity
from . import app_server_llm_judge as judge
from . import codex_app_server
from .app_server_capacity import CapacityGatedCodexAppServerClient
from .util import now_iso


SCHEMA_VERSION = "pif_candidate_recurrent_fold_direct_reference_v1"
LOCK_VERSION = "pif_candidate_recurrent_fold_direct_reference_lock_v1"
RECEIPT_VERSION = "pif_semantic_plan_step_receipt_v1"
PLAN_EPOCH = 4
STEP_ID = "ordered_recurrent_full_schema_fold_direct_reference_v1"
MODEL = "gpt-5.5"
EFFORT = "high"
MODEL_CALL_CAP = 3
ADJUDICATION_CALL_CAP = 1
TOTAL_TOKEN_CAP = 400_000
QUALITY_THRESHOLD = 0.97
TOKEN_RATIO_TARGET = 0.28
SYSTEM_BASELINE = "baseline_reference_seed"
SYSTEM_CANDIDATE = "candidate_ordered_recurrent_fold"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = (PROJECT_ROOT / "automation" / "pif-evaluation-semantic-plan-v1.json").resolve()
DEFAULT_DIRECTIVE_PATH = (
    PROJECT_ROOT
    / "automation"
    / "pif-evaluation-ordered-recurrent-fold-direct-reference-v1.json"
).resolve()
DEFAULT_CANARY_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "development-selection-v249-reject-ordered-recurrent-full-schema-fold-canary-v1"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "work"
    / "app-server-development-v2"
    / "unattended-pipeline-v5"
    / "development-selection-v249-ordered-recurrent-fold-direct-reference-v1"
).resolve()
PINNED_CODEX = prior.PINNED_CODEX
USAGE_FIELDS = prior.USAGE_FIELDS
PROCESS_LOCK_NAME = ".ordered-recurrent-fold-direct-reference.lock"

_ROLE_ALIASES = {
    "baseline": {
        "baseline_complete_output",
        "frozen_baseline_output",
        "complete_baseline_output",
    },
    "source": {
        "blind_source_packet",
        "frozen_source_packet",
        "source_artifact",
    },
    "terminal": {
        "recurrent_canary_terminal",
        "ordered_recurrent_fold_terminal",
        "candidate_terminal",
        "structural_canary_terminal",
    },
    "normalized": {
        "recurrent_canary_normalized_output",
        "ordered_recurrent_fold_normalized_output",
        "candidate_normalized_output",
        "normalized_output",
    },
}
_TERMINAL_RECORD_KEYS = (
    "normalized_output",
    "evidence_provenance",
    "runtime_lock",
)

_CANARY_COMBINED_BASENAMES = {
    "normalized_output": "combined-normalized-output.private.json",
    "evidence_provenance": "combined-evidence-provenance.private.json",
    "runtime_lock": "runtime-lock.json",
    "fold_receipt": "fold-receipt.json",
    "structural_gate": "structural-gate.json",
}


class RecurrentFoldDirectReferenceError(RuntimeError):
    """A frozen evaluator contract or direct full-output artifact is invalid."""


class OperationalWaitingError(RecurrentFoldDirectReferenceError):
    """An operational, capacity, or measured-budget condition must wait."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecurrentFoldDirectReferenceError(f"cannot read {label}") from exc


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _is_capacity_record(record: Mapping[str, Any]) -> bool:
    name = Path(str(record.get("path") or "")).name
    return name.startswith("capacity") or name.endswith(".capacity.json")


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        return not _is_capacity_record(record) and _record(Path(str(record["path"]))) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _write_immutable_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise RecurrentFoldDirectReferenceError(f"immutable {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_immutable_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise RecurrentFoldDirectReferenceError(f"immutable {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _record_digest(records: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json(list(records)).encode("utf-8")).hexdigest()


@contextmanager
def _advisory_process_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    with (root / PROCESS_LOCK_NAME).open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OperationalWaitingError("another direct-reference evaluator owns the process lock") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _validated_frozen_record(row: Mapping[str, Any], label: str) -> dict[str, Any]:
    path = Path(str(row.get("path") or "")).expanduser().resolve()
    if not path.is_file() or row.get("sha256") != _sha256_file(path):
        raise RecurrentFoldDirectReferenceError(f"{label} checksum drifted")
    record = _record(path)
    if _is_capacity_record(record):
        raise RecurrentFoldDirectReferenceError("capacity JSON cannot be a frozen semantic artifact")
    return record


def _terminal_records(terminal: Mapping[str, Any]) -> list[dict[str, Any]]:
    values = terminal.get("records")
    if isinstance(values, list):
        if any(not isinstance(row, Mapping) for row in values):
            raise RecurrentFoldDirectReferenceError("canary terminal record list is malformed")
        records = [
            _validated_frozen_record(row, "canary terminal record")
            for row in values
        ]
        declared_digest = terminal.get("records_sha256")
        if declared_digest is not None and declared_digest != hashlib.sha256(
            _canonical_json(values).encode("utf-8")
        ).hexdigest():
            raise RecurrentFoldDirectReferenceError("canary terminal record-list digest drifted")
        return records
    if isinstance(values, Mapping):
        return [
            _validated_frozen_record(row, "canary terminal record")
            for row in values.values()
            if isinstance(row, Mapping) and row.get("path") and row.get("sha256")
        ]
    raise RecurrentFoldDirectReferenceError("canary terminal records are missing")


def _record_by_path_suffix(
    records: Sequence[Mapping[str, Any]], suffix: str, *, required: bool = True
) -> dict[str, Any] | None:
    normalized_suffix = suffix.replace("\\", "/")
    matches = [
        dict(record)
        for record in records
        if str(record.get("path") or "").replace("\\", "/").endswith(normalized_suffix)
    ]
    if len(matches) > 1:
        raise RecurrentFoldDirectReferenceError(f"canary record suffix is ambiguous: {suffix}")
    if not matches:
        if required:
            raise RecurrentFoldDirectReferenceError(f"canary record is missing: {suffix}")
        return None
    return matches[0]


def _terminal_record(
    terminal: Mapping[str, Any], key: str, records: Sequence[Mapping[str, Any]] | None = None
) -> dict[str, Any] | None:
    candidates: list[Any] = [terminal.get(key)]
    named_records = terminal.get("records")
    if isinstance(named_records, Mapping):
        candidates.append(named_records.get(key))
    artifacts = terminal.get("artifacts")
    if isinstance(artifacts, Mapping):
        candidates.append(artifacts.get(key))
    for value in candidates:
        if isinstance(value, Mapping) and value.get("path") and value.get("sha256"):
            return _validated_frozen_record(value, f"canary terminal {key}")
    basename = _CANARY_COMBINED_BASENAMES.get(key)
    if basename and records is not None:
        return _record_by_path_suffix(records, f"/{basename}", required=False)
    return None


def _find_role(rows: Sequence[Mapping[str, Any]], family: str) -> dict[str, Any] | None:
    matches = [row for row in rows if str(row.get("role") or "") in _ROLE_ALIASES[family]]
    if len(matches) > 1:
        raise RecurrentFoldDirectReferenceError(f"duplicate {family} frozen-input role")
    return dict(matches[0]) if matches else None


def load_contract(
    *,
    plan_path: Path | None = None,
    directive_path: Path | None = None,
) -> dict[str, Any]:
    """Load the not-yet-stable epoch-4 contract through semantic role names.

    The plan may point at any versioned directive filename.  Candidate artifacts
    are preferentially resolved from the checksum-bound canary terminal so the
    evaluator does not depend on the canary's internal per-turn layout.
    """

    resolved_plan = (plan_path or PLAN_PATH).expanduser().resolve()
    plan = _load_json(resolved_plan, "epoch-4 semantic plan")
    step = plan.get("step") if isinstance(plan, Mapping) else None
    if not isinstance(step, Mapping):
        raise RecurrentFoldDirectReferenceError("epoch-4 plan step is missing")
    resolved_directive = (
        directive_path.expanduser().resolve()
        if directive_path is not None
        else Path(str(step.get("directive_path") or DEFAULT_DIRECTIVE_PATH)).expanduser().resolve()
    )
    directive = _load_json(resolved_directive, "epoch-4 direct-reference directive")
    execution = directive.get("execution_contract")
    acceptance = directive.get("acceptance_contract")
    if (
        plan.get("plan_epoch") != PLAN_EPOCH
        or plan.get("state") != "executable"
        or step.get("state") != "executable"
        or step.get("step_id") != directive.get("step_id")
        or step.get("step_id") != STEP_ID
        or Path(str(step.get("directive_path"))).expanduser().resolve() != resolved_directive
        or step.get("directive_sha256") != _sha256_file(resolved_directive)
        or directive.get("plan_epoch") != PLAN_EPOCH
        or not isinstance(execution, Mapping)
        or not isinstance(acceptance, Mapping)
        or execution.get("extraction_model_call_cap") != 0
        or execution.get("semantic_judge_call_cap") != MODEL_CALL_CAP
        or execution.get("semantic_retry_cap") != 0
        or execution.get("adjudication_call_cap") != ADJUDICATION_CALL_CAP
        or execution.get("judge_model") != MODEL
        or execution.get("judge_reasoning_effort") != EFFORT
        or execution.get("judge_transport")
        != "official_codex_app_server_stdio_managed_chatgpt_auth"
        or execution.get("variants") != ["ab", "ba"]
        or execution.get("all_events_direct_to_full_event_judge") is not True
        or execution.get("pointwise_support_results_allowed") is not False
        or execution.get("support_filtering_allowed") is not False
        or execution.get("alignment_filtering_allowed") is not False
        or execution.get("deterministic_semantic_pruning_allowed") is not False
        or acceptance.get("candidate_strict_full_field_macro_f1_min") != QUALITY_THRESHOLD
        or acceptance.get("candidate_must_be_noninferior_to_baseline") is not True
        or acceptance.get("production_amortized_total_token_ratio_max") != TOKEN_RATIO_TARGET
        or acceptance.get("quality_failure_is_rejected") is not True
        or acceptance.get("operational_failure_is_waiting") is not True
    ):
        raise RecurrentFoldDirectReferenceError("epoch-4 direct-reference contract drifted")
    if step.get("max_model_calls") != MODEL_CALL_CAP:
        raise RecurrentFoldDirectReferenceError("epoch-4 model-call cap drifted")
    total_cap = step.get("max_total_tokens", TOTAL_TOKEN_CAP)
    if total_cap != TOTAL_TOKEN_CAP:
        raise RecurrentFoldDirectReferenceError("epoch-4 semantic token cap drifted")

    raw_rows = directive.get("frozen_inputs")
    if not isinstance(raw_rows, list) or not raw_rows or any(not isinstance(row, Mapping) for row in raw_rows):
        raise RecurrentFoldDirectReferenceError("epoch-4 frozen inputs are missing")
    rows = [dict(row) for row in raw_rows]
    baseline_row = _find_role(rows, "baseline")
    source_row = _find_role(rows, "source")
    terminal_row = _find_role(rows, "terminal")
    normalized_row = _find_role(rows, "normalized")
    if baseline_row is None or source_row is None or terminal_row is None:
        raise RecurrentFoldDirectReferenceError("baseline, source, and canary terminal must be frozen")
    baseline_record = _validated_frozen_record(baseline_row, "baseline complete output")
    source_record = _validated_frozen_record(source_row, "blind source packet")
    terminal_record = _validated_frozen_record(terminal_row, "recurrent canary terminal")
    terminal = _load_json(Path(terminal_record["path"]), "recurrent canary terminal")
    if terminal.get("state") not in {"passed", "structurally_passed"}:
        raise RecurrentFoldDirectReferenceError("recurrent canary has not structurally passed")
    listed_records = _terminal_records(terminal)
    terminal_artifacts = {
        key: _terminal_record(terminal, key, listed_records)
        for key in (*_TERMINAL_RECORD_KEYS, "fold_receipt", "structural_gate")
    }
    declared_normalized = terminal_artifacts["normalized_output"]
    frozen_normalized = (
        _validated_frozen_record(normalized_row, "recurrent normalized output")
        if normalized_row is not None
        else None
    )
    normalized_record = declared_normalized or frozen_normalized
    if normalized_record is None:
        raise RecurrentFoldDirectReferenceError("canary terminal does not bind a normalized output")
    if declared_normalized is not None and frozen_normalized is not None and declared_normalized != frozen_normalized:
        raise RecurrentFoldDirectReferenceError("terminal and directive normalized-output records differ")
    for key in (
        "evidence_provenance",
        "runtime_lock",
        "fold_receipt",
        "structural_gate",
    ):
        if terminal_artifacts[key] is None:
            raise RecurrentFoldDirectReferenceError(f"canary terminal does not bind {key}")

    canary_receipt_rows = [
        row
        for row in rows
        if Path(str(row.get("path") or "")).name == "plan-step-receipt.json"
    ]
    if len(canary_receipt_rows) != 1:
        raise RecurrentFoldDirectReferenceError("checksum-bound canary plan-step receipt is missing")
    canary_receipt_record = _validated_frozen_record(
        canary_receipt_rows[0], "recurrent canary plan-step receipt"
    )
    canary_receipt = _load_json(
        Path(canary_receipt_record["path"]), "recurrent canary plan-step receipt"
    )
    if canary_receipt != terminal:
        raise RecurrentFoldDirectReferenceError("canary terminal and plan-step receipt differ")
    canary_usage = terminal.get("usage")
    if (
        terminal.get("usage_status") != "complete"
        or terminal.get("accounting_complete") is not True
        or terminal.get("semantic_model_call_count") != 2
        or not isinstance(canary_usage, Mapping)
        or any(
            isinstance(canary_usage.get(field), bool)
            or not isinstance(canary_usage.get(field), int)
            or canary_usage.get(field) < 0
            for field in USAGE_FIELDS
        )
        or canary_usage.get("total_tokens")
        != canary_usage.get("input_tokens") + canary_usage.get("output_tokens")
    ):
        raise RecurrentFoldDirectReferenceError("canary inline extraction accounting is incomplete")

    frozen_records = [_validated_frozen_record(row, "epoch-4 frozen input") for row in rows]
    all_records: list[dict[str, Any]] = []
    for record in [*frozen_records, *listed_records]:
        if record not in all_records:
            all_records.append(record)
    cost_projection = execution.get("production_cost_projection")
    if not isinstance(cost_projection, Mapping):
        cost_projection = directive.get("production_cost_projection")
    if not isinstance(cost_projection, Mapping):
        raise RecurrentFoldDirectReferenceError("frozen production-cost projection is missing")
    baseline_tokens = cost_projection.get("baseline_end_to_end_tokens")
    context_tokens = cost_projection.get("production_amortized_context_tokens")
    production_scale = cost_projection.get("production_scale")
    if (
        isinstance(baseline_tokens, bool)
        or not isinstance(baseline_tokens, int)
        or baseline_tokens <= 0
        or isinstance(context_tokens, bool)
        or not isinstance(context_tokens, int)
        or context_tokens < 0
        or isinstance(production_scale, bool)
        or not isinstance(production_scale, int)
        or production_scale <= 0
    ):
        raise RecurrentFoldDirectReferenceError("frozen production-cost formula is invalid")
    extraction_total = int(canary_usage["total_tokens"])
    production_total = context_tokens + extraction_total * production_scale
    ratio = round(production_total / baseline_tokens, 6)
    structural_gate = _load_json(
        Path(str(terminal_artifacts["structural_gate"]["path"])),
        "canary structural gate",
    )
    if (
        not isinstance(structural_gate, Mapping)
        or structural_gate.get("passed") is not True
        or structural_gate.get("combined_extraction_total_tokens") != extraction_total
        or structural_gate.get("production_amortized_total_tokens") != production_total
        or structural_gate.get("production_amortized_total_token_ratio") != ratio
    ):
        raise RecurrentFoldDirectReferenceError(
            "canary accounting does not reproduce the frozen structural cost gate"
        )
    declared_ratio = directive.get("production_amortized_total_token_ratio")
    if declared_ratio is not None and declared_ratio != ratio:
        raise RecurrentFoldDirectReferenceError(
            "directive production ratio differs from recomputed canary accounting"
        )
    adjudication_max = execution.get("adjudication_max_total_tokens")
    if (
        isinstance(adjudication_max, bool)
        or not isinstance(adjudication_max, int)
        or adjudication_max <= 0
        or adjudication_max > TOTAL_TOKEN_CAP
    ):
        raise RecurrentFoldDirectReferenceError(
            "a conservative per-adjudication total-token maximum must be frozen"
        )
    receipt_path = Path(str(step.get("expected_receipt_path") or directive.get("expected_receipt_path"))).resolve()
    if receipt_path != Path(str(directive.get("expected_receipt_path"))).resolve():
        raise RecurrentFoldDirectReferenceError("expected receipt path drifted")
    return {
        "plan": plan,
        "directive": directive,
        "plan_record": _record(resolved_plan),
        "directive_record": _record(resolved_directive),
        "receipt_path": receipt_path,
        "frozen_records": all_records,
        "baseline_record": baseline_record,
        "source_record": source_record,
        "terminal_record": terminal_record,
        "canary_receipt_record": canary_receipt_record,
        "normalized_record": normalized_record,
        "canary_records": listed_records,
        "canary_accounting": {
            "usage_status": terminal["usage_status"],
            "accounting_complete": terminal["accounting_complete"],
            "semantic_model_call_count": terminal["semantic_model_call_count"],
            "usage": dict(canary_usage),
        },
        "terminal_artifacts": terminal_artifacts,
        "production_token_ratio": float(ratio),
        "production_cost_projection": {
            "baseline_end_to_end_tokens": baseline_tokens,
            "production_amortized_context_tokens": context_tokens,
            "production_scale": production_scale,
            "combined_extraction_total_tokens": extraction_total,
            "production_amortized_total_tokens": production_total,
        },
        "adjudication_max_total_tokens": adjudication_max,
    }


def _event_sha256(event: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(event).encode("utf-8")).hexdigest()


def _source_cases(source: Mapping[str, Any]) -> list[dict[str, Any]]:
    segments = source.get("segments")
    if not isinstance(segments, list) or not segments:
        raise RecurrentFoldDirectReferenceError("blind source packet has no ordered segments")
    result = []
    seen = set()
    for index, row in enumerate(segments):
        if not isinstance(row, Mapping):
            raise RecurrentFoldDirectReferenceError("blind source segment is malformed")
        segment_id = str(row.get("segment_id") or "")
        excerpt = row.get("segment_text", row.get("source_excerpt"))
        if not segment_id or segment_id in seen or not isinstance(excerpt, str) or not excerpt:
            raise RecurrentFoldDirectReferenceError("blind source segment identity/text drifted")
        seen.add(segment_id)
        result.append(
            {
                "segment_id": segment_id,
                "source_excerpt": excerpt,
                "density_stratum": row.get("density_stratum"),
                "source_index": index,
            }
        )
    return result


def _normalized_events(output: Mapping[str, Any], *, baseline: bool) -> list[tuple[str, dict[str, Any]]]:
    result: list[tuple[str, dict[str, Any]]] = []
    if baseline and isinstance(output.get("references"), list):
        for row in output["references"]:
            if not isinstance(row, Mapping):
                raise RecurrentFoldDirectReferenceError("baseline reference row is malformed")
            events = (row.get("golden_output") or {}).get("discourse_events")
            if not isinstance(events, list):
                raise RecurrentFoldDirectReferenceError("baseline golden events are missing")
            segment_id = str(row.get("segment_id") or "")
            for event in events:
                if not isinstance(event, Mapping):
                    raise RecurrentFoldDirectReferenceError("baseline event is malformed")
                result.append((segment_id, deepcopy(dict(event))))
        return result
    segments = output.get("segments")
    if not isinstance(segments, list):
        raise RecurrentFoldDirectReferenceError("normalized output segments are missing")
    for row in segments:
        if not isinstance(row, Mapping):
            raise RecurrentFoldDirectReferenceError("normalized segment is malformed")
        segment_id = str(row.get("segment_id") or "")
        events = row.get("events", row.get("discourse_events"))
        if not isinstance(events, list):
            raise RecurrentFoldDirectReferenceError("normalized segment events are missing")
        for event in events:
            if not isinstance(event, Mapping):
                raise RecurrentFoldDirectReferenceError("normalized event is malformed")
            result.append((segment_id, deepcopy(dict(event))))
    return result


def _event_multiset(rows: Sequence[tuple[str, Mapping[str, Any]]]) -> Counter[tuple[str, str]]:
    return Counter((segment_id, _event_sha256(event)) for segment_id, event in rows)


def _pretty_json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _require_replayed_bytes(path: Path, value: Any, label: str) -> None:
    try:
        observed = path.read_bytes()
    except OSError as exc:
        raise RecurrentFoldDirectReferenceError(f"cannot read stored {label}") from exc
    if observed != _pretty_json_bytes(value):
        raise RecurrentFoldDirectReferenceError(f"replayed {label} bytes differ from storage")


def _validate_diagnostics_wrapper(path: Path, label: str) -> list[dict[str, Any]]:
    value = _load_json(path, label)
    segments = value.get("segments") if isinstance(value, Mapping) else None
    if (
        not isinstance(value, Mapping)
        or set(value) != {"segments"}
        or not isinstance(segments, list)
        or any(not isinstance(row, Mapping) for row in segments)
    ):
        raise RecurrentFoldDirectReferenceError(
            f"{label} must use the canary diagnostics object wrapper"
        )
    return [dict(row) for row in segments]


def _replay_canary_lineage(
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Independently replay raw projection and fold from terminal-bound artifacts."""

    records = contract.get("canary_records")
    if not isinstance(records, list):
        raise RecurrentFoldDirectReferenceError("terminal-bound canary record list is absent")
    projected: list[
        tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]
    ] = []
    raw_rows: list[tuple[str, int, dict[str, Any]]] = []
    projected_rows: list[tuple[str, int, dict[str, Any]]] = []
    for turn_name in canary.TURN_NAMES:
        prefix = f"/turns/{turn_name}/"
        paths = {
            key: Path(str(_record_by_path_suffix(records, prefix + basename)["path"]))
            for key, basename in {
                "input": "input.private.json",
                "schema": "schema.json",
                "direct_schema": "projection-schema.json",
                "output": "output.private.json",
                "normalized": "normalized-output.private.json",
                "provenance": "evidence-provenance.private.json",
                "diagnostics": "diagnostics.private.json",
                "applicability": "applicability-receipt.json",
            }.items()
        }
        private_input = _load_json(paths["input"], f"{turn_name} frozen input")
        raw_output = _load_json(paths["output"], f"{turn_name} raw output")
        segments = private_input.get("segments") if isinstance(private_input, Mapping) else None
        if not isinstance(segments, list) or len(segments) != 1:
            raise RecurrentFoldDirectReferenceError("canary turn input is not one isolated segment")
        segment_id = str(segments[0].get("segment_id") or "")
        replay_turn = {
            "episode_id": private_input.get("episode_id"),
            "segment_ids": [segment_id],
            "segment_id": segment_id,
            "private_input": private_input,
            "schema": _load_json(paths["schema"], f"{turn_name} schema"),
            "direct_schema": _load_json(
                paths["direct_schema"], f"{turn_name} projection schema"
            ),
        }
        try:
            replayed = canary.project_turn_output(raw_output, replay_turn)
        except canary.StructuralRejectionError as exc:
            raise RecurrentFoldDirectReferenceError(
                f"raw canary turn no longer passes the frozen v249 projection: {turn_name}"
            ) from exc
        normalized, provenance, diagnostics, applicability = replayed
        _require_replayed_bytes(paths["normalized"], normalized, f"{turn_name} normalized output")
        _require_replayed_bytes(paths["provenance"], provenance, f"{turn_name} evidence provenance")
        stored_diagnostics = _validate_diagnostics_wrapper(
            paths["diagnostics"], f"{turn_name} diagnostics"
        )
        if stored_diagnostics != diagnostics:
            raise RecurrentFoldDirectReferenceError(
                f"replayed {turn_name} diagnostics differ from storage"
            )
        _require_replayed_bytes(
            paths["diagnostics"],
            {"segments": diagnostics},
            f"{turn_name} diagnostics",
        )
        _require_replayed_bytes(
            paths["applicability"], applicability, f"{turn_name} applicability receipt"
        )
        raw_segments = raw_output.get("segments") if isinstance(raw_output, Mapping) else None
        normalized_segments = normalized.get("segments")
        if (
            not isinstance(raw_segments, list)
            or len(raw_segments) != 1
            or not isinstance(normalized_segments, list)
            or len(normalized_segments) != 1
            or raw_segments[0].get("segment_id") != segment_id
            or normalized_segments[0].get("segment_id") != segment_id
        ):
            raise RecurrentFoldDirectReferenceError("replayed turn segment lineage drifted")
        raw_events = raw_segments[0].get("events")
        normalized_events = normalized_segments[0].get("events")
        if not isinstance(raw_events, list) or not isinstance(normalized_events, list):
            raise RecurrentFoldDirectReferenceError("replayed turn event arrays are missing")
        if len(raw_events) != len(normalized_events):
            raise RecurrentFoldDirectReferenceError("raw-to-projected event count is not one-to-one")
        for index, (raw_event, projected_event) in enumerate(zip(raw_events, normalized_events)):
            if not isinstance(raw_event, Mapping) or not isinstance(projected_event, Mapping):
                raise RecurrentFoldDirectReferenceError("raw/projected event lineage is malformed")
            raw_rows.append((segment_id, index, deepcopy(dict(raw_event))))
            projected_rows.append((segment_id, index, deepcopy(dict(projected_event))))
        projected.append(
            (
                dict(normalized),
                dict(provenance),
                [dict(row) for row in diagnostics],
                dict(applicability),
            )
        )
    try:
        folded, provenance, diagnostics, fold_receipt = canary.fold_validated_outputs(
            projected[0], projected[1]
        )
    except canary.StructuralRejectionError as exc:
        raise RecurrentFoldDirectReferenceError("replayed canary fold is structurally invalid") from exc
    combined_paths = {
        "normalized": Path(str(contract["normalized_record"]["path"])),
        "provenance": Path(str(contract["terminal_artifacts"]["evidence_provenance"]["path"])),
        "diagnostics": Path(
            str(
                _record_by_path_suffix(
                    records, "/combined-diagnostics.private.json"
                )["path"]
            )
        ),
        "fold": Path(str(contract["terminal_artifacts"]["fold_receipt"]["path"])),
    }
    _require_replayed_bytes(combined_paths["normalized"], folded, "combined folded output")
    _require_replayed_bytes(combined_paths["provenance"], provenance, "combined provenance")
    stored_combined_diagnostics = _validate_diagnostics_wrapper(
        combined_paths["diagnostics"], "combined diagnostics"
    )
    if stored_combined_diagnostics != diagnostics:
        raise RecurrentFoldDirectReferenceError(
            "replayed combined diagnostics differ from storage"
        )
    _require_replayed_bytes(
        combined_paths["diagnostics"],
        {"segments": diagnostics},
        "combined diagnostics",
    )
    _require_replayed_bytes(combined_paths["fold"], fold_receipt, "fold receipt")

    folded_by_id = {
        str(row.get("segment_id")): row.get("events")
        for row in folded.get("segments") or []
        if isinstance(row, Mapping)
    }
    lineage = []
    raw_multiset: Counter[tuple[str, str]] = Counter()
    bound_raw_multiset: Counter[tuple[str, str]] = Counter()
    projected_multiset: Counter[tuple[str, str]] = Counter()
    folded_multiset: Counter[tuple[str, str]] = Counter()
    for (raw_segment, index, raw_event), (
        projected_segment,
        projected_index,
        projected_event,
    ) in zip(raw_rows, projected_rows):
        folded_events = folded_by_id.get(raw_segment)
        if (
            raw_segment != projected_segment
            or index != projected_index
            or not isinstance(folded_events, list)
            or index >= len(folded_events)
            or folded_events[index] != projected_event
        ):
            raise RecurrentFoldDirectReferenceError("projected-to-folded event lineage drifted")
        raw_hash = _event_sha256(raw_event)
        projected_hash = _event_sha256(projected_event)
        folded_hash = _event_sha256(folded_events[index])
        raw_multiset[(raw_segment, raw_hash)] += 1
        bound_raw_multiset[(raw_segment, raw_hash)] += 1
        projected_multiset[(raw_segment, projected_hash)] += 1
        folded_multiset[(raw_segment, folded_hash)] += 1
        lineage.append(
            {
                "segment_id": raw_segment,
                "event_index": index,
                "raw_event_sha256": raw_hash,
                "projected_event_sha256": projected_hash,
                "folded_event_sha256": folded_hash,
            }
        )
    expected_folded = _normalized_events(folded, baseline=False)
    if (
        raw_multiset != bound_raw_multiset
        or projected_multiset != _event_multiset(expected_folded)
        or folded_multiset != _event_multiset(expected_folded)
        or len(lineage) != len(expected_folded)
    ):
        raise RecurrentFoldDirectReferenceError("raw/projected/folded event multiset drifted")
    return folded, lineage


def build_complete_container(contract: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build the all-event pool and prove exact multiset lineage on both sides."""

    source = _load_json(Path(str(contract["source_record"]["path"])), "blind source packet")
    baseline = _load_json(Path(str(contract["baseline_record"]["path"])), "frozen baseline output")
    candidate, candidate_lineage = _replay_canary_lineage(contract)
    cases = _source_cases(source)
    baseline_rows = _normalized_events(baseline, baseline=True)
    candidate_rows = _normalized_events(candidate, baseline=False)
    source_ids = {row["segment_id"] for row in cases}
    if any(not segment_id or segment_id not in source_ids for segment_id, _ in baseline_rows + candidate_rows):
        raise RecurrentFoldDirectReferenceError("raw output contains an event outside the frozen source cases")
    baseline_by_segment: dict[str, list[dict[str, Any]]] = {key: [] for key in source_ids}
    candidate_by_segment: dict[str, list[dict[str, Any]]] = {key: [] for key in source_ids}
    for segment_id, event in baseline_rows:
        baseline_by_segment[segment_id].append(event)
    for segment_id, event in candidate_rows:
        candidate_by_segment[segment_id].append(event)
    lineage_by_key = {
        (str(row["segment_id"]), int(row["event_index"])): row
        for row in candidate_lineage
    }
    raw_cases = []
    for case in cases:
        segment_id = case["segment_id"]
        raw_cases.append(
            {
                "case_key": segment_id,
                "source_excerpt": case["source_excerpt"],
                "event_set_a": [
                    {
                        "event": event,
                        "provenance": {
                            "system_id": SYSTEM_BASELINE,
                            "segment_id": segment_id,
                            "raw_event_sha256": _event_sha256(event),
                            "raw_event_index": index,
                        },
                    }
                    for index, event in enumerate(baseline_by_segment[segment_id])
                ],
                "event_set_b": [
                    {
                        "event": event,
                        "provenance": {
                            "system_id": SYSTEM_CANDIDATE,
                            "segment_id": segment_id,
                            **lineage_by_key[(segment_id, index)],
                            "raw_event_index": index,
                        },
                    }
                    for index, event in enumerate(candidate_by_segment[segment_id])
                ],
                "provenance": {
                    "density_stratum": case["density_stratum"],
                    "source_index": case["source_index"],
                    "segment_id": segment_id,
                },
            }
        )
    try:
        pool, mapping = judge.make_shared_witness_pool(raw_cases, seed=STEP_ID)
    except (TypeError, ValueError) as exc:
        raise RecurrentFoldDirectReferenceError("complete all-event pool is invalid") from exc
    if judge.validate_shared_witness_pool(pool):
        raise RecurrentFoldDirectReferenceError("complete all-event pool failed validation")

    witness_multisets = {SYSTEM_BASELINE: Counter(), SYSTEM_CANDIDATE: Counter()}
    for case in mapping["cases"]:
        for witness in case["witnesses"]:
            provenance = witness["provenance"]
            system_id = str(provenance.get("system_id"))
            event = provenance.get("original_event")
            if system_id not in witness_multisets or not isinstance(event, Mapping):
                raise RecurrentFoldDirectReferenceError("private all-event witness lineage drifted")
            witness_multisets[system_id][
                (str(provenance.get("segment_id")), _event_sha256(event))
            ] += 1
    if witness_multisets[SYSTEM_BASELINE] != _event_multiset(baseline_rows):
        raise RecurrentFoldDirectReferenceError("baseline raw/witness event-hash multiset drifted")
    if witness_multisets[SYSTEM_CANDIDATE] != _event_multiset(candidate_rows):
        raise RecurrentFoldDirectReferenceError("candidate raw/witness event-hash multiset drifted")
    candidate_raw_witnesses = Counter(
        (
            str(witness["provenance"].get("segment_id")),
            str(witness["provenance"].get("raw_event_sha256")),
        )
        for case in mapping["cases"]
        for witness in case["witnesses"]
        if witness["provenance"].get("system_id") == SYSTEM_CANDIDATE
    )
    expected_raw = Counter(
        (str(row["segment_id"]), str(row["raw_event_sha256"]))
        for row in candidate_lineage
    )
    if candidate_raw_witnesses != expected_raw:
        raise RecurrentFoldDirectReferenceError("raw canary events are not one-to-one with witnesses")
    variants = judge.build_judge_variants(pool)
    ab_cases = variants["ab"]["cases"]
    ba_cases = variants["ba"]["cases"]
    if [row["case_id"] for row in ab_cases] != [row["case_id"] for row in ba_cases]:
        raise RecurrentFoldDirectReferenceError("AB/BA case order drifted")
    for ab, ba in zip(ab_cases, ba_cases):
        if ab["event_set_a"] != ba["event_set_b"] or ab["event_set_b"] != ba["event_set_a"]:
            raise RecurrentFoldDirectReferenceError("AB/BA opaque IDs or full event payloads drifted")
    return pool, mapping


build_shared_pool = build_complete_container


def _runtime_files() -> tuple[Path, ...]:
    paths = (
        Path(__file__).resolve(),
        Path(shared_repair.__file__).resolve(),
        Path(prior.__file__).resolve(),
        Path(judge.__file__).resolve(),
        Path(app_server_capacity.__file__).resolve(),
        Path(codex_app_server.__file__).resolve(),
        PINNED_CODEX,
        *canary._runtime_files(),  # noqa: SLF001
    )
    return tuple(dict.fromkeys(path.expanduser().resolve() for path in paths))


def _meaningful_root_entries(root: Path) -> list[Path]:
    return [path for path in root.iterdir() if path.name != PROCESS_LOCK_NAME]


def _freeze_unlocked(root: Path) -> dict[str, Any]:
    lock_path = root / "runtime-lock.json"
    if lock_path.is_file():
        verify_runtime_lock(lock_path, acquire_lock=False)
        return {"root": root, "runtime_lock": lock_path}
    if root.exists() and _meaningful_root_entries(root):
        raise RecurrentFoldDirectReferenceError("unfrozen direct-reference output root is not empty")
    contract = load_contract()
    if contract["receipt_path"] != root / "plan-step-receipt.json":
        raise RecurrentFoldDirectReferenceError("output root differs from the epoch-4 receipt contract")
    pool, mapping = build_complete_container(contract)
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "private-witness-mapping.private.json"
    _write_immutable_json(pool_path, pool)
    _write_immutable_json(mapping_path, mapping)
    variants = judge.build_judge_variants(pool)
    instructions = judge.judge_base_instructions()
    request_records = []
    for name in ("ab", "ba"):
        prompt_path = root / "requests" / f"prompt-{name}.private.md"
        schema_path = root / "requests" / f"schema-{name}.json"
        _write_immutable_text(prompt_path, judge.build_judge_prompt(variants[name]))
        _write_immutable_json(schema_path, judge.semantic_judge_output_schema(variants[name]))
        request_records.extend((_record(prompt_path), _record(schema_path)))
    instructions_path = root / "requests" / "base-instructions.private.md"
    _write_immutable_text(instructions_path, instructions)
    request_records.append(_record(instructions_path))
    spec_path = root / "attempt-spec.json"
    witness_count = sum(
        len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"]
    )
    _write_immutable_json(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "plan_epoch": PLAN_EPOCH,
            "step_id": STEP_ID,
            "model": MODEL,
            "effort": EFFORT,
            "variant_order": ["ab", "ba"],
            "all_event_witness_count": witness_count,
            "all_events_direct_to_full_event_judge": True,
            "pointwise_support_results_consumed": False,
            "support_filtering_applied": False,
            "alignment_filtering_applied": False,
            "semantic_regex_or_keyword_filtering_applied": False,
            "deterministic_semantic_pruning_applied": False,
            "reference_system_ids": None,
            "semantic_model_call_cap": MODEL_CALL_CAP,
            "adjudication_call_cap": ADJUDICATION_CALL_CAP,
            "adjudication_max_total_tokens": contract[
                "adjudication_max_total_tokens"
            ],
            "adjudication_ceiling_enforcement": (
                "conservative_measured_preflight_and_post_turn_verification_"
                "app_server_has_no_native_mid_turn_total_token_stop"
            ),
            "semantic_total_token_cap": TOTAL_TOKEN_CAP,
            "production_amortized_total_token_ratio": contract["production_token_ratio"],
            "managed_chatgpt_auth_only": True,
            "official_pinned_app_server_only": True,
            "winner_frozen": False,
            "holdout": False,
            "production": False,
        },
    )
    runtime_records = [_record(path) for path in _runtime_files()]
    source_records = [
        contract["plan_record"],
        contract["directive_record"],
        *contract["frozen_records"],
    ]
    records = [
        *runtime_records,
        *source_records,
        _record(pool_path),
        _record(mapping_path),
        *request_records,
        _record(spec_path),
    ]
    if any(_is_capacity_record(row) for row in records):
        raise RecurrentFoldDirectReferenceError("capacity checkpoint leaked into immutable hashes")
    lock = {
        "schema_version": LOCK_VERSION,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "runtime_files": runtime_records,
        "source_records": source_records,
        "pool": _record(pool_path),
        "mapping": _record(mapping_path),
        "request_records": request_records,
        "attempt_spec": _record(spec_path),
        "direct_record_digest": _record_digest(records),
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "adjudication_call_cap": ADJUDICATION_CALL_CAP,
        "adjudication_max_total_tokens": contract[
            "adjudication_max_total_tokens"
        ],
        "semantic_total_token_cap": TOTAL_TOKEN_CAP,
        "semantic_retry_count": 0,
        "extraction_model_call_cap": 0,
        "capacity_checkpoint_records_excluded": True,
        "winner_frozen": False,
        "holdout": False,
        "production": False,
    }
    _write_immutable_json(lock_path, lock)
    verify_runtime_lock(lock_path, acquire_lock=False)
    return {"root": root, "runtime_lock": lock_path}


def freeze_run(output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _advisory_process_lock(root):
        return _freeze_unlocked(root)


def verify_runtime_lock(path: Path, *, acquire_lock: bool = True) -> dict[str, Any]:
    lock_path = path.expanduser().resolve()

    def verify() -> dict[str, Any]:
        lock = _load_json(lock_path, "direct-reference runtime lock")
        records = [
            *(lock.get("runtime_files") or []),
            *(lock.get("source_records") or []),
            lock.get("pool") or {},
            lock.get("mapping") or {},
            *(lock.get("request_records") or []),
            lock.get("attempt_spec") or {},
        ]
        if (
            lock.get("schema_version") != LOCK_VERSION
            or lock.get("plan_epoch") != PLAN_EPOCH
            or lock.get("step_id") != STEP_ID
            or lock.get("semantic_model_call_cap") != MODEL_CALL_CAP
            or lock.get("adjudication_call_cap") != ADJUDICATION_CALL_CAP
            or not isinstance(lock.get("adjudication_max_total_tokens"), int)
            or lock.get("adjudication_max_total_tokens") <= 0
            or lock.get("semantic_total_token_cap") != TOTAL_TOKEN_CAP
            or lock.get("semantic_retry_count") != 0
            or lock.get("extraction_model_call_cap") != 0
            or lock.get("capacity_checkpoint_records_excluded") is not True
            or lock.get("winner_frozen") is not False
            or lock.get("holdout") is not False
            or lock.get("production") is not False
            or lock.get("direct_record_digest") != _record_digest(records)
            or any(_is_capacity_record(row) or not _verify_record(row) for row in records)
        ):
            raise RecurrentFoldDirectReferenceError("direct-reference runtime lock or artifact drifted")
        load_contract()
        return lock

    if acquire_lock:
        with _advisory_process_lock(lock_path.parent):
            return verify()
    return verify()


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory() -> CapacityGatedCodexAppServerClient:
    return CapacityGatedCodexAppServerClient(inner_factory=_inner_factory)


def _aggregate_usage(sidecar_paths: Sequence[Path]) -> dict[str, Any]:
    if any(_is_capacity_record({"path": str(path)}) for path in sidecar_paths):
        raise OperationalWaitingError("capacity JSON cannot enter clean judge accounting")
    try:
        return shared_repair._aggregate_usage(sidecar_paths)  # noqa: SLF001
    except shared_repair.SharedReferenceRepairError as exc:
        raise RecurrentFoldDirectReferenceError(str(exc)) from exc


def _fresh_attempt_accounting(sidecar_paths: Sequence[Path]) -> dict[str, Any]:
    try:
        return _aggregate_usage(sidecar_paths)
    except RecurrentFoldDirectReferenceError as exc:
        raise OperationalWaitingError("fresh semantic usage accounting is incomplete") from exc


def _preflight_adjudication(accounting: Mapping[str, Any], *, token_max: int) -> int:
    usage = accounting.get("usage") if isinstance(accounting, Mapping) else None
    calls = accounting.get("measured_model_call_count")
    used = usage.get("total_tokens") if isinstance(usage, Mapping) else None
    if (
        accounting.get("accounting_complete") is not True
        or calls != 2
        or isinstance(used, bool)
        or not isinstance(used, int)
        or used < 0
        or isinstance(token_max, bool)
        or not isinstance(token_max, int)
        or token_max <= 0
        or token_max > TOTAL_TOKEN_CAP
    ):
        raise OperationalWaitingError("adjudication preflight lacks measured AB/BA accounting")
    remaining = TOTAL_TOKEN_CAP - used
    if used + token_max > TOTAL_TOKEN_CAP or calls + 1 > MODEL_CALL_CAP:
        raise OperationalWaitingError(
            "measured AB/BA total plus frozen adjudication maximum exceeds the total cap"
        )
    return remaining


def _enforce_adjudication_usage(
    accounting: Mapping[str, Any], *, measured_ab_ba_total: int, token_max: int
) -> int:
    usage = accounting.get("usage") if isinstance(accounting, Mapping) else None
    third_total = usage.get("total_tokens") if isinstance(usage, Mapping) else None
    if (
        accounting.get("accounting_complete") is not True
        or accounting.get("measured_model_call_count") != 1
        or isinstance(third_total, bool)
        or not isinstance(third_total, int)
        or third_total < 0
        or third_total > token_max
        or measured_ab_ba_total + third_total > TOTAL_TOKEN_CAP
    ):
        raise OperationalWaitingError(
            "origin-neutral adjudication exceeded its frozen per-turn or final total-token cap"
        )
    return third_total


def _zero_accounting() -> dict[str, Any]:
    return {
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_model_call_count": 0,
        "usage": {field: 0 for field in USAGE_FIELDS},
        "turns": [],
    }


def _normalized_consensus_metadata(consensus: Mapping[str, Any]) -> dict[str, Any]:
    return prior._normalized_consensus_metadata(consensus)  # noqa: SLF001


def _witness_systems(mapping: Mapping[str, Any]) -> dict[str, str]:
    result = {}
    for case in mapping.get("cases") or []:
        for witness in case.get("witnesses") or []:
            provenance = witness.get("provenance") or {}
            system_id = provenance.get("system_id")
            witness_id = str(witness.get("witness_id"))
            if system_id not in {SYSTEM_BASELINE, SYSTEM_CANDIDATE} or witness_id in result:
                raise RecurrentFoldDirectReferenceError("private witness system lineage drifted")
            result[witness_id] = str(system_id)
    return result


def _macro_scores(
    *, pool: Mapping[str, Any], mapping: Mapping[str, Any], consensus: Mapping[str, Any]
) -> dict[str, Any]:
    systems = _witness_systems(mapping)
    provenance = {
        str(row["case_id"]): row.get("case_provenance") or {}
        for row in mapping.get("cases") or []
    }
    pool_cases = {str(row["case_id"]): row for row in pool.get("cases") or []}
    totals = {SYSTEM_BASELINE: [], SYSTEM_CANDIDATE: []}
    case_scores = []
    for row in consensus.get("cases") or []:
        case_id = str(row.get("case_id"))
        if case_id not in pool_cases or row.get("partition_abstained_witness_ids"):
            raise RecurrentFoldDirectReferenceError("cannot score an abstained or unknown partition")
        events = {
            witness["witness_id"]: witness["event"]
            for side in ("a", "b")
            for witness in pool_cases[case_id][f"event_set_{side}"]
        }
        support = {
            str(item["witness_id"]): str(item["verdict"])
            for item in row.get("support_results") or []
        }
        groups = [frozenset(group) for group in row.get("equivalence_groups") or []]
        grouped = set().union(*groups) if groups else set()
        if grouped != set(events) or set(support) != set(events):
            raise RecurrentFoldDirectReferenceError("shared-reference partition omits a witness")
        group_by_witness = {
            witness_id: index for index, group in enumerate(groups) for witness_id in group
        }
        reference_units = {
            group_by_witness[witness_id]
            for witness_id, verdict in support.items()
            if verdict == "supported" and events[witness_id].get("submitted_evidence_exact") is True
        }
        rows = {}
        for system_id in (SYSTEM_BASELINE, SYSTEM_CANDIDATE):
            submitted_ids = {key for key in events if systems.get(key) == system_id}
            submitted_units = {group_by_witness[key] for key in submitted_ids}
            supported_units = {
                group_by_witness[key]
                for key in submitted_ids
                if support[key] == "supported"
                and events[key].get("submitted_evidence_exact") is True
            }
            precision = (
                len(supported_units) / len(submitted_units)
                if submitted_units
                else (1.0 if not reference_units else 0.0)
            )
            recall = (
                len(supported_units & reference_units) / len(reference_units)
                if reference_units
                else 1.0
            )
            f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
            totals[system_id].append(f1)
            rows[system_id] = {
                "submitted_unit_count": len(submitted_units),
                "supported_unit_count": len(supported_units),
                "shared_reference_unit_count": len(reference_units),
                "strict_precision": round(precision, 6),
                "strict_recall": round(recall, 6),
                "strict_f1": round(f1, 6),
            }
        case_scores.append(
            {
                "case_id": case_id,
                "density_stratum": provenance.get(case_id, {}).get("density_stratum"),
                "systems": rows,
            }
        )
    if len(case_scores) != len(pool_cases) or not case_scores:
        raise RecurrentFoldDirectReferenceError("macro score does not cover every source case")
    return {
        "systems": {
            system_id: {
                "strict_full_field_macro_f1": round(sum(values) / len(values), 6)
            }
            for system_id, values in totals.items()
        },
        "cases": case_scores,
    }


def score_shared_reference(
    *,
    pool: Mapping[str, Any],
    mapping: Mapping[str, Any],
    consensus: Mapping[str, Any],
    production_token_ratio: float,
    accounting: Mapping[str, Any],
    adjudication_call_count: int,
) -> dict[str, Any]:
    shared = judge.score_named_systems_against_shared_reference(
        pool=pool,
        private_mapping=mapping,
        consensus=consensus,
        reference_system_ids=None,
    )
    if shared.get("reference_system_ids") != [SYSTEM_BASELINE, SYSTEM_CANDIDATE]:
        raise RecurrentFoldDirectReferenceError("shared union did not symmetrically include both systems")
    macro = _macro_scores(pool=pool, mapping=mapping, consensus=consensus)
    baseline_macro = macro["systems"][SYSTEM_BASELINE]["strict_full_field_macro_f1"]
    candidate_macro = macro["systems"][SYSTEM_CANDIDATE]["strict_full_field_macro_f1"]
    witness_count = sum(
        len(case["event_set_a"]) + len(case["event_set_b"]) for case in pool["cases"]
    )
    exact_count = sum(
        witness["event"].get("submitted_evidence_exact") is True
        for case in pool["cases"]
        for side in ("a", "b")
        for witness in case[f"event_set_{side}"]
    )
    usage = accounting.get("usage") or {}
    calls = accounting.get("measured_model_call_count")
    total_tokens = usage.get("total_tokens")
    if (
        accounting.get("accounting_complete") is not True
        or isinstance(calls, bool)
        or not isinstance(calls, int)
        or calls > MODEL_CALL_CAP
        or isinstance(total_tokens, bool)
        or not isinstance(total_tokens, int)
        or total_tokens > TOTAL_TOKEN_CAP
        or adjudication_call_count > ADJUDICATION_CALL_CAP
    ):
        raise OperationalWaitingError("judge accounting or semantic call cap is inadmissible")
    checks = {
        "candidate_strict_full_field_macro_f1_gte_0_97": candidate_macro >= QUALITY_THRESHOLD,
        "candidate_noninferior_to_baseline": candidate_macro >= baseline_macro,
        "production_amortized_total_token_ratio_lte_0_28": production_token_ratio <= TOKEN_RATIO_TARGET,
        "exact_evidence_rate_1": witness_count > 0 and exact_count == witness_count,
    }
    failed = sorted(name for name, passed in checks.items() if passed is not True)
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "systems": shared["systems"],
        "macro": macro,
        "reference_system_ids": shared["reference_system_ids"],
        "shared_reference_unit_count": shared["reference_unit_count"],
        "candidate_strict_full_field_macro_f1": candidate_macro,
        "baseline_strict_full_field_macro_f1": baseline_macro,
        "candidate_noninferiority_delta": round(candidate_macro - baseline_macro, 6),
        "production_amortized_total_token_ratio": round(production_token_ratio, 6),
        "exact_evidence_rate": round(exact_count / witness_count, 6),
        "semantic_model_call_count": calls,
        "semantic_total_tokens": total_tokens,
        "adjudication_call_count": adjudication_call_count,
        "winner_frozen": False,
        "holdout": False,
        "production": False,
    }


def _observable_disagreement_case_ids(
    pool: Mapping[str, Any], outputs: Mapping[str, Mapping[str, Any]]
) -> list[str]:
    try:
        return shared_repair._observable_disagreement_case_ids(pool, outputs)  # noqa: SLF001
    except shared_repair.SharedReferenceRepairError as exc:
        raise RecurrentFoldDirectReferenceError(str(exc)) from exc


async def _run_adjudication(
    *,
    root: Path,
    pool: Mapping[str, Any],
    case_ids: Sequence[str],
    client_factory: Callable[[], Any],
    timeout_seconds: float,
    measured_ab_ba_total: int,
    token_max: int,
) -> tuple[dict[str, Any], Path]:
    phase = root / "adjudication"
    if (phase / "sidecar.json").exists() or (phase / "output.private.json").exists():
        raise OperationalWaitingError("the sole adjudication already has an immutable attempt")
    try:
        variant = shared_repair._build_adjudication_variant(pool, case_ids)  # noqa: SLF001
    except shared_repair.SharedReferenceRepairError as exc:
        raise RecurrentFoldDirectReferenceError(str(exc)) from exc
    instructions = judge.judge_base_instructions()
    prompt = (
        "This is the sole origin-neutral adjudication. Independently judge complete-event "
        "source support and the full exact-evidence semantic-equivalence partition. Do not "
        "vote between or reference prior outputs. This turn is evaluated against a frozen "
        f"measured total-token ceiling of {token_max}. The app-server transport has no native "
        "mid-turn total-token stop, so any measured overrun makes this attempt waiting.\n\n"
        + judge.build_judge_prompt(variant)
    )
    schema = judge.semantic_judge_output_schema(variant)
    _write_immutable_json(phase / "variant.private.json", variant)
    _write_immutable_text(phase / "base-instructions.private.md", instructions)
    _write_immutable_text(phase / "prompt.private.md", prompt)
    _write_immutable_json(phase / "schema.json", schema)
    sidecar = phase / "sidecar.json"
    output_path = phase / "output.private.json"
    async with client_factory() as client:
        result = await client.run_ephemeral_structured_turn(
            model=MODEL,
            effort=EFFORT,
            base_instructions=instructions,
            prompt=prompt,
            output_schema=schema,
            cwd=PROJECT_ROOT,
            sidecar_path=sidecar,
            output_path=output_path,
            capacity_checkpoint_path=phase / "capacity.json",
            batch_size=len(case_ids),
            thread_mode="new_thread",
            timeout_seconds=timeout_seconds,
        )
    if not result.status_ok or not isinstance(result.output, Mapping):
        raise OperationalWaitingError("origin-neutral adjudication did not complete")
    output = _load_json(output_path, "origin-neutral adjudication output")
    if judge.validate_judge_output(output, variant):
        raise OperationalWaitingError("origin-neutral adjudication output is invalid")
    third_accounting = _fresh_attempt_accounting([sidecar])
    _enforce_adjudication_usage(
        third_accounting,
        measured_ab_ba_total=measured_ab_ba_total,
        token_max=token_max,
    )
    return output, sidecar


def _merge_adjudication(
    consensus: Mapping[str, Any], output: Mapping[str, Any], case_ids: Sequence[str]
) -> dict[str, Any]:
    try:
        merged = shared_repair._merge_adjudication(  # noqa: SLF001
            consensus=consensus,
            adjudication_output=output,
            adjudicated_case_ids=case_ids,
        )
    except shared_repair.SharedReferenceRepairError as exc:
        raise RecurrentFoldDirectReferenceError(str(exc)) from exc
    return _normalized_consensus_metadata(merged)


def _reconstruct_bound_consensus(
    *,
    pool: Mapping[str, Any],
    records: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], int]:
    outputs: dict[str, Mapping[str, Any]] = {}
    for name in ("ab", "ba"):
        record = records.get(f"judge_output_{name}")
        if not isinstance(record, Mapping):
            raise RecurrentFoldDirectReferenceError(
                "quality receipt must bind both AB/BA judge outputs"
            )
        output = _load_json(Path(str(record["path"])), f"receipt-bound {name} output")
        if not isinstance(output, Mapping):
            raise RecurrentFoldDirectReferenceError("receipt-bound judge output is malformed")
        outputs[name] = output
    try:
        base_consensus = judge.combine_judge_consensus(pool, outputs)
    except (TypeError, ValueError, KeyError) as exc:
        raise RecurrentFoldDirectReferenceError(
            "receipt-bound AB/BA outputs do not reconstruct a valid consensus"
        ) from exc
    disagreements = _observable_disagreement_case_ids(pool, outputs)
    adjudication_record = records.get("adjudication_output")
    adjudication_sidecar = records.get("adjudication_sidecar")
    if disagreements:
        if not isinstance(adjudication_record, Mapping) or not isinstance(
            adjudication_sidecar, Mapping
        ):
            raise RecurrentFoldDirectReferenceError(
                "observable AB/BA disagreement lacks a bound adjudication output/sidecar"
            )
        adjudication = _load_json(
            Path(str(adjudication_record["path"])), "receipt-bound adjudication output"
        )
        try:
            variant = shared_repair._build_adjudication_variant(  # noqa: SLF001
                pool, disagreements
            )
        except shared_repair.SharedReferenceRepairError as exc:
            raise RecurrentFoldDirectReferenceError(str(exc)) from exc
        if not isinstance(adjudication, Mapping) or judge.validate_judge_output(
            adjudication, variant
        ):
            raise RecurrentFoldDirectReferenceError(
                "receipt-bound adjudication output is invalid"
            )
        final_consensus = _merge_adjudication(
            base_consensus, adjudication, disagreements
        )
        return final_consensus, base_consensus, 1
    if adjudication_record is not None or adjudication_sidecar is not None:
        raise RecurrentFoldDirectReferenceError(
            "adjudication artifacts exist without observable AB/BA disagreement"
        )
    return _normalized_consensus_metadata(base_consensus), base_consensus, 0


def _receipt(
    *,
    root: Path,
    state: str,
    terminal_reason: str,
    accounting: Mapping[str, Any],
    score: Mapping[str, Any] | None,
    error: Exception | None = None,
) -> dict[str, Any]:
    if state not in {"passed", "rejected", "waiting"}:
        raise RecurrentFoldDirectReferenceError("invalid direct-reference receipt state")

    def optional(path: Path) -> dict[str, Any] | None:
        return _record(path) if path.is_file() and not path.name.startswith("capacity") else None

    records = {
        "runtime_lock": _record(root / "runtime-lock.json"),
        "judge_report": optional(root / "judge" / "report.json"),
        "judge_output_ab": optional(root / "judge" / "output-ab.private.json"),
        "judge_output_ba": optional(root / "judge" / "output-ba.private.json"),
        "judge_sidecar_ab": optional(root / "judge" / "sidecars" / "ab.json"),
        "judge_sidecar_ba": optional(root / "judge" / "sidecars" / "ba.json"),
        "adjudication_output": optional(root / "adjudication" / "output.private.json"),
        "adjudication_sidecar": optional(root / "adjudication" / "sidecar.json"),
        "consensus": optional(root / "consensus.private.json"),
        "score": optional(root / "shared-reference-score.json"),
    }
    payload = {
        "schema_version": RECEIPT_VERSION,
        "plan_epoch": PLAN_EPOCH,
        "step_id": STEP_ID,
        "state": state,
        "terminal_at": now_iso(),
        "terminal_reason": terminal_reason,
        "semantic_model_call_cap": MODEL_CALL_CAP,
        "semantic_model_call_count": accounting.get("measured_model_call_count", 0),
        "semantic_total_token_cap": TOTAL_TOKEN_CAP,
        "semantic_retry_count": 0,
        "extraction_model_call_count": 0,
        "usage_status": accounting.get("usage_status", "unknown"),
        "accounting_complete": accounting.get("accounting_complete", False),
        "usage": accounting.get("usage"),
        "development_quality_passed": bool(score and score.get("passed")),
        "winner_frozen": False,
        "development_winner_frozen": False,
        "holdout": False,
        "holdout_authorized": False,
        "production": False,
        "production_mutated": False,
        "records": records,
        "next_action": (
            "return_passed_epoch_4_step_without_freezing_a_winner"
            if state == "passed"
            else "retain_recurrent_fold_as_quality_rejected"
            if state == "rejected"
            else "wait_for_versioned_operational_recovery"
        ),
        "privacy": "sanitized metrics counts hashes and failure class only",
    }
    if score is not None:
        payload.update(
            {
                "failed_checks": score.get("failed_checks"),
                "candidate_strict_full_field_macro_f1": score.get(
                    "candidate_strict_full_field_macro_f1"
                ),
                "baseline_strict_full_field_macro_f1": score.get(
                    "baseline_strict_full_field_macro_f1"
                ),
                "candidate_noninferiority_delta": score.get("candidate_noninferiority_delta"),
                "production_amortized_total_token_ratio": score.get(
                    "production_amortized_total_token_ratio"
                ),
                "exact_evidence_rate": score.get("exact_evidence_rate"),
            }
        )
    if error is not None:
        message = str(error).encode("utf-8")
        payload.update(
            {
                "error_class": type(error).__name__,
                "error_message_sha256": hashlib.sha256(message).hexdigest(),
                "error_message_size_bytes": len(message),
            }
        )
    return payload


def verify_receipt(root: Path = DEFAULT_OUTPUT_ROOT, *, acquire_lock: bool = True) -> dict[str, Any]:
    root = root.expanduser().resolve()

    def verify() -> dict[str, Any]:
        runtime_lock = verify_runtime_lock(root / "runtime-lock.json", acquire_lock=False)
        receipt = _load_json(root / "plan-step-receipt.json", "direct-reference receipt")
        terminal = _load_json(root / "terminal.json", "direct-reference terminal")
        if receipt != terminal:
            raise RecurrentFoldDirectReferenceError("terminal and plan-step receipt differ")
        if (
            receipt.get("schema_version") != RECEIPT_VERSION
            or receipt.get("plan_epoch") != PLAN_EPOCH
            or receipt.get("step_id") != STEP_ID
            or receipt.get("state") not in {"passed", "rejected", "waiting"}
            or receipt.get("winner_frozen") is not False
            or receipt.get("development_winner_frozen") is not False
            or receipt.get("holdout") is not False
            or receipt.get("holdout_authorized") is not False
            or receipt.get("production") is not False
            or receipt.get("production_mutated") is not False
        ):
            raise RecurrentFoldDirectReferenceError("direct-reference receipt drifted")
        records = receipt.get("records")
        if not isinstance(records, Mapping):
            raise RecurrentFoldDirectReferenceError("receipt records are missing")
        present = [row for row in records.values() if row is not None]
        if not present or any(not isinstance(row, Mapping) or not _verify_record(row) for row in present):
            raise RecurrentFoldDirectReferenceError("receipt artifact integrity failed")
        if receipt["state"] in {"passed", "rejected"} and any(
            records.get(key) is None for key in ("score", "consensus", "judge_report")
        ):
            raise RecurrentFoldDirectReferenceError("quality receipt lacks scored evidence")
        if receipt["state"] in {"passed", "rejected"}:
            score = _load_json(
                Path(str(records["score"]["path"])), "receipt-bound shared-reference score"
            )
            consensus = _load_json(
                Path(str(records["consensus"]["path"])), "receipt-bound final consensus"
            )
            report = _load_json(
                Path(str(records["judge_report"]["path"])), "receipt-bound judge report"
            )
            if (
                not isinstance(score, Mapping)
                or not isinstance(consensus, Mapping)
                or not isinstance(report, Mapping)
                or isinstance(score.get("production_amortized_total_token_ratio"), bool)
                or not isinstance(
                    score.get("production_amortized_total_token_ratio"), (int, float)
                )
            ):
                raise RecurrentFoldDirectReferenceError(
                    "receipt-bound score/consensus/report shape is invalid"
                )
            ab_record = records.get("judge_sidecar_ab")
            ba_record = records.get("judge_sidecar_ba")
            if not isinstance(ab_record, Mapping) or not isinstance(ba_record, Mapping):
                raise RecurrentFoldDirectReferenceError("quality receipt lacks both AB/BA sidecars")
            pool = _load_json(root / "shared-witness-pool.private.json", "receipt pool")
            mapping = _load_json(
                root / "private-witness-mapping.private.json", "receipt mapping"
            )
            reconstructed_consensus, reconstructed_base, adjudication_calls = (
                _reconstruct_bound_consensus(pool=pool, records=records)
            )
            if consensus != reconstructed_consensus:
                raise RecurrentFoldDirectReferenceError(
                    "final consensus differs from bound AB/BA/adjudication reconstruction"
                )
            sidecar_paths = [Path(str(ab_record["path"])), Path(str(ba_record["path"]))]
            adjudication_record = records.get("adjudication_sidecar")
            if adjudication_record is not None:
                if not isinstance(adjudication_record, Mapping):
                    raise RecurrentFoldDirectReferenceError("adjudication sidecar record is malformed")
                sidecar_paths.append(Path(str(adjudication_record["path"])))
                adjudication_calls = 1
            accounting = _aggregate_usage(sidecar_paths)
            ab_ba_accounting = _aggregate_usage(sidecar_paths[:2])
            if adjudication_calls:
                _preflight_adjudication(
                    ab_ba_accounting,
                    token_max=runtime_lock["adjudication_max_total_tokens"],
                )
                _enforce_adjudication_usage(
                    _aggregate_usage(sidecar_paths[2:]),
                    measured_ab_ba_total=ab_ba_accounting["usage"]["total_tokens"],
                    token_max=runtime_lock["adjudication_max_total_tokens"],
                )
            judge_consensus_path = root / "judge" / "consensus.private.json"
            judge_consensus = _load_json(judge_consensus_path, "receipt-bound judge consensus")
            if (
                report.get("schema_version") != judge.JUDGE_RUN_VERSION
                or report.get("state") != "completed"
                or report.get("model") != MODEL
                or report.get("reasoning_effort") != EFFORT
                or report.get("variant_count") != 2
                or report.get("accounting_complete") is not True
                or report.get("usage_status") != "complete"
                or report.get("usage") != ab_ba_accounting["usage"]
                or report.get("consensus_sha256") != _sha256_file(judge_consensus_path)
                or report.get("selection_admissible") is not True
                or consensus.get("schema_version") != judge.JUDGE_CONSENSUS_VERSION
                or judge_consensus.get("schema_version") != judge.JUDGE_CONSENSUS_VERSION
                or judge_consensus != reconstructed_base
            ):
                raise RecurrentFoldDirectReferenceError("judge report/consensus integrity drifted")
            if (
                receipt.get("usage_status") != "complete"
                or receipt.get("accounting_complete") is not True
                or receipt.get("semantic_model_call_count")
                != accounting["measured_model_call_count"]
                or receipt.get("usage") != accounting["usage"]
                or score.get("semantic_model_call_count")
                != accounting["measured_model_call_count"]
                or score.get("semantic_total_tokens") != accounting["usage"]["total_tokens"]
                or score.get("adjudication_call_count") != adjudication_calls
            ):
                raise RecurrentFoldDirectReferenceError("quality receipt accounting drifted")
            try:
                expected_ratio = load_contract()["production_token_ratio"]
                recomputed = score_shared_reference(
                    pool=pool,
                    mapping=mapping,
                    consensus=consensus,
                    production_token_ratio=expected_ratio,
                    accounting=accounting,
                    adjudication_call_count=adjudication_calls,
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise RecurrentFoldDirectReferenceError(
                    "receipt-bound score inputs are internally inconsistent"
                ) from exc
            if recomputed != score:
                raise RecurrentFoldDirectReferenceError("receipt-bound score does not recompute exactly")
            metric_fields = (
                "failed_checks",
                "candidate_strict_full_field_macro_f1",
                "baseline_strict_full_field_macro_f1",
                "candidate_noninferiority_delta",
                "production_amortized_total_token_ratio",
                "exact_evidence_rate",
            )
            if any(receipt.get(field) != score.get(field) for field in metric_fields):
                raise RecurrentFoldDirectReferenceError("receipt quality metrics differ from score")
            allowed_quality_checks = {
                "candidate_strict_full_field_macro_f1_gte_0_97",
                "candidate_noninferior_to_baseline",
                "production_amortized_total_token_ratio_lte_0_28",
                "exact_evidence_rate_1",
            }
            failed_checks = score.get("failed_checks")
            if (
                not isinstance(failed_checks, list)
                or any(check not in allowed_quality_checks for check in failed_checks)
                or (receipt["state"] == "passed") != (score.get("passed") is True)
                or (receipt["state"] == "rejected") != (score.get("passed") is False)
                or (receipt["state"] == "passed" and failed_checks)
                or (receipt["state"] == "rejected" and not failed_checks)
                or receipt.get("development_quality_passed")
                != (score.get("passed") is True)
            ):
                raise RecurrentFoldDirectReferenceError(
                    "receipt state is not an exact passed/rejected quality projection"
                )
        elif records.get("score") is not None:
            raise RecurrentFoldDirectReferenceError("waiting receipt cannot carry a quality score")
        return receipt

    if acquire_lock:
        with _advisory_process_lock(root):
            return verify()
    return verify()


def _best_effort_accounting(root: Path) -> dict[str, Any]:
    sidecars = sorted(root.glob("judge/sidecars/*.json")) + sorted(
        root.glob("adjudication/sidecar.json")
    )
    if not sidecars:
        return _zero_accounting()
    try:
        return _aggregate_usage(sidecars)
    except (OperationalWaitingError, RecurrentFoldDirectReferenceError):
        return {
            "usage_status": "unknown",
            "accounting_complete": False,
            "measured_model_call_count": len(sidecars),
            "usage": None,
            "turns": [],
        }


async def _run_unlocked(
    *,
    root: Path,
    timeout_seconds: float,
    client_factory: Callable[[], Any],
    judge_runner: Callable[..., Any],
) -> dict[str, Any]:
    receipt_path = root / "plan-step-receipt.json"
    if receipt_path.is_file():
        return verify_receipt(root, acquire_lock=False)
    _freeze_unlocked(root)
    pool = _load_json(root / "shared-witness-pool.private.json", "complete all-event pool")
    mapping = _load_json(root / "private-witness-mapping.private.json", "private mapping")
    try:
        await judge_runner(
            pool_path=root / "shared-witness-pool.private.json",
            output_dir=root / "judge",
            model=MODEL,
            reasoning_effort=EFFORT,
            timeout_seconds=timeout_seconds,
            client_factory=client_factory,
        )
        sidecars = [root / "judge" / "sidecars" / f"{name}.json" for name in ("ab", "ba")]
        accounting = _fresh_attempt_accounting(sidecars)
        if accounting["measured_model_call_count"] != 2:
            raise OperationalWaitingError("AB/BA measured call count drifted")
        outputs = {
            name: _load_json(root / "judge" / f"output-{name}.private.json", f"{name} output")
            for name in ("ab", "ba")
        }
        consensus = _load_json(root / "judge" / "consensus.private.json", "AB/BA consensus")
        disagreements = _observable_disagreement_case_ids(pool, outputs)
        adjudication_calls = 0
        if disagreements:
            contract = load_contract()
            _preflight_adjudication(
                accounting, token_max=contract["adjudication_max_total_tokens"]
            )
            adjudicated, sidecar = await _run_adjudication(
                root=root,
                pool=pool,
                case_ids=disagreements,
                client_factory=client_factory,
                timeout_seconds=timeout_seconds,
                measured_ab_ba_total=accounting["usage"]["total_tokens"],
                token_max=contract["adjudication_max_total_tokens"],
            )
            sidecars.append(sidecar)
            accounting = _fresh_attempt_accounting(sidecars)
            if accounting["usage"]["total_tokens"] > TOTAL_TOKEN_CAP:
                raise OperationalWaitingError("final semantic usage exceeded the total-token cap")
            adjudication_calls = 1
            consensus = _merge_adjudication(consensus, adjudicated, disagreements)
        else:
            consensus = _normalized_consensus_metadata(consensus)
        _write_immutable_json(root / "consensus.private.json", consensus)
        contract = load_contract()
        score = score_shared_reference(
            pool=pool,
            mapping=mapping,
            consensus=consensus,
            production_token_ratio=contract["production_token_ratio"],
            accounting=accounting,
            adjudication_call_count=adjudication_calls,
        )
        _write_immutable_json(root / "shared-reference-score.json", score)
        state = "passed" if score["passed"] else "rejected"
        reason = (
            "recurrent_fold_direct_reference_quality_passed"
            if score["passed"]
            else "recurrent_fold_direct_reference_quality_rejected"
        )
        receipt = _receipt(
            root=root,
            state=state,
            terminal_reason=reason,
            accounting=accounting,
            score=score,
        )
    except (
        OperationalWaitingError,
        app_server_capacity.AppServerCapacityError,
        codex_app_server.AppServerError,
        judge.JudgeAttemptFailed,
        asyncio.TimeoutError,
    ) as exc:
        receipt = _receipt(
            root=root,
            state="waiting",
            terminal_reason="recurrent_fold_direct_reference_operational_or_cap_waiting",
            accounting=_best_effort_accounting(root),
            score=None,
            error=exc,
        )
    _write_immutable_json(receipt_path, receipt)
    _write_immutable_json(root / "terminal.json", receipt)
    return verify_receipt(root, acquire_lock=False)


async def run(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = _client_factory,
    judge_runner: Callable[..., Any] = judge.run_app_server_semantic_judge,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    with _advisory_process_lock(root):
        return await _run_unlocked(
            root=root,
            timeout_seconds=timeout_seconds,
            client_factory=client_factory,
            judge_runner=judge_runner,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "run", "verify"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.action == "prepare":
        frozen = freeze_run(args.output_dir)
        result = {"state": "prepared", "runtime_lock": str(frozen["runtime_lock"])}
    elif args.action == "verify":
        receipt = verify_receipt(args.output_dir)
        result = {"state": receipt["state"], "verified": True}
    else:
        receipt = asyncio.run(run(output_dir=args.output_dir, timeout_seconds=args.timeout_seconds))
        result = {
            "state": receipt["state"],
            "terminal_reason": receipt["terminal_reason"],
            "semantic_model_call_count": receipt["semantic_model_call_count"],
            "accounting_complete": receipt["accounting_complete"],
            "winner_frozen": receipt["winner_frozen"],
            "holdout": receipt["holdout"],
            "production": receipt["production"],
        }
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
