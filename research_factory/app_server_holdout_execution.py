from __future__ import annotations

"""Checkpointed, private execution of a frozen app-server holdout.

The three phases are intentionally filesystem-only.  They read the production
corpus database, but never insert or update production rows and never promote a
candidate.  Every model attempt is preceded by an immutable intent artifact so
an interrupted process cannot silently retry it.
"""

import argparse
import asyncio
import copy
import hashlib
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Optional, Sequence, Union

from . import app_server_expanded_cap_episode_batch as expanded_cap
from . import app_server_evaluation as evaluation
from .app_server_evaluation import APP_SERVER_DEVELOPMENT_MANIFEST_V2
from .app_server_capacity import CapacityGatedCodexAppServerClient
from .app_server_checkpoint import (
    build_holdout_leaf_binding,
    validate_completed_managed_sidecar,
    validate_holdout_leaf_binding,
    validate_managed_sidecar_execution_lineage,
    verify_instruction_contract,
    verify_label_pack_contract,
)
from .app_server_holdout_client import (
    LazyClientSession,
    resolve_holdout_client_factory,
    verified_holdout_execution_lineage,
)
from .app_server_holdout import (
    HOLDOUT_COVENANT_VERSION,
    load_frozen_winner,
    verify_frozen_holdout,
    verify_holdout_model_call_authorization,
)
from .codex_app_server import APP_SERVER_CLIENT_VERSION
from .labels import (
    load_label_pack,
    render_prompt,
    repair_label_output_for_submission,
    validate_label_output,
)
from .paths import db_path, root as factory_root
from .util import stable_id, write_text_atomic
from .worker import (
    EPISODE_CONTEXT_SCHEMA_VERSION,
    render_episode_context_prompt,
    validate_episode_context_output,
)


HOLDOUT_EXECUTION_PLAN_VERSION = "pif_app_server_holdout_execution_plan_v2"
HOLDOUT_CONTEXT_PHASE_VERSION = "pif_app_server_holdout_context_phase_v2"
HOLDOUT_CANDIDATE_PHASE_VERSION = "pif_app_server_holdout_candidate_phase_v2"
HOLDOUT_BASELINE_PHASE_VERSION = "pif_app_server_holdout_baseline_phase_v2"
HOLDOUT_PRIVATE_MANIFEST_VERSION = APP_SERVER_DEVELOPMENT_MANIFEST_V2
HOLDOUT_STRATIFIED_SELECTION_VERSION = "pif_app_server_holdout_stratified_selection_v1"

CONTEXT_MODEL = "gpt-5.5"
CONTEXT_EFFORT = "high"
BASELINE_MODEL = "gpt-5.5"
BASELINE_EFFORT = "high"
LABEL_PACK = "ai_discourse_v3_1"

_USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
_TERMINAL_SIDECAR_STATES = {"completed", "failed", "interrupted", "cancelled"}


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Union[str, Path], *, purpose: str) -> tuple[Path, dict[str, Any]]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ValueError(f"{purpose} is missing: {resolved}")
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{purpose} is not valid UTF-8 JSON: {resolved}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{purpose} must contain a JSON object: {resolved}")
    return resolved, value


def _write_immutable(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = path.open("xb")
    except FileExistsError as exc:
        raise ValueError(f"immutable execution artifact already exists: {path}") from exc
    with descriptor:
        descriptor.write(content)
        descriptor.flush()


def _ensure_immutable(path: Path, content: bytes) -> None:
    if path.exists():
        if not path.is_file() or path.read_bytes() != content:
            raise ValueError(f"immutable execution artifact drift: {path}")
        return
    _write_immutable(path, content)


def _resolve_artifact(path_value: str) -> Path:
    candidate = Path(path_value).expanduser()
    return candidate.resolve() if candidate.is_absolute() else (factory_root() / candidate).resolve()


_resolve_holdout_client_factory = resolve_holdout_client_factory


def _verified_text_file(path_value: str, expected_hash: str, *, purpose: str) -> tuple[Path, str]:
    path = _resolve_artifact(path_value)
    if not path.is_file():
        raise ValueError(f"{purpose} is missing: {path}")
    observed = _sha256_file(path)
    if observed != expected_hash:
        raise ValueError(f"{purpose} DB/file hash drift: {path}")
    try:
        return path, path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{purpose} is not UTF-8: {path}") from exc


def _load_execution_inputs(
    conn: sqlite3.Connection,
    *,
    covenant_path: Union[str, Path],
    frozen_winner_path: Union[str, Path],
    stratified_selection_path: Optional[Union[str, Path]],
    acceptance_mode: bool,
) -> dict[str, Any]:
    covenant_file, covenant = verify_holdout_model_call_authorization(covenant_path)
    verification = verify_frozen_holdout(conn, covenant_path=covenant_file)
    winner = load_frozen_winner(frozen_winner_path)
    if winner["artifact_sha256"] != covenant.get("winner_artifact_sha256"):
        raise ValueError("frozen winner artifact does not match holdout covenant")
    if winner["payload"]["winner"] != covenant.get("winner"):
        raise ValueError("frozen winner configuration does not match holdout covenant")
    if winner["payload"]["gates"] != covenant.get("winner_gates"):
        raise ValueError("frozen winner gates do not match holdout covenant")
    winner_record = winner["payload"].get("winner") or {}
    config = winner_record.get("frozen_configuration")
    if not isinstance(config, dict):
        raise ValueError("frozen winner lacks the exact expanded-cap configuration")
    config_sha256 = hashlib.sha256(
        json.dumps(
            config, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    if (
        config_sha256 != winner_record.get("frozen_configuration_sha256")
        or config.get("winner_system_id") != winner_record.get("winner_system_id")
        or config.get("batch_size") != winner_record.get("batch_size")
        or config.get("thread_mode") != winner_record.get("thread_mode")
        or config.get("model") != winner_record.get("model")
        or config.get("effort") != winner_record.get("reasoning_effort")
        or winner_record.get("concurrency") != 1
        or config.get("retry_count") != 0
        or config.get("max_events_per_segment") != expanded_cap.MAX_EVENTS_PER_SEGMENT
        or config.get("transport_client_version") != APP_SERVER_CLIENT_VERSION
        or config.get("semantic_postprocessing") is not False
        or config.get("production_mutation_allowed") is not False
    ):
        raise ValueError("frozen winner extraction configuration is unsafe or inconsistent")

    reservoirs = []
    for name in ("paired_quality_reservoir", "terminal_position_no_signal_candidates"):
        record = (covenant.get("artifacts") or {}).get(name)
        if not isinstance(record, dict) or not record.get("filename"):
            raise ValueError(f"holdout covenant is missing reservoir: {name}")
        reservoir_path = covenant_file.parent / str(record["filename"])
        _, payload = _read_json(reservoir_path, purpose=f"{name} reservoir")
        for segment in payload.get("segments") or []:
            if not isinstance(segment, dict):
                raise ValueError(f"{name} contains a malformed segment")
            reservoirs.append({**segment, "reservoir": name})
    segment_ids = [str(row.get("segment_id") or "") for row in reservoirs]
    if not segment_ids or any(not item for item in segment_ids):
        raise ValueError("holdout reservoirs contain no executable segments")
    if len(segment_ids) != len(set(segment_ids)):
        raise ValueError("holdout reservoirs overlap or contain duplicate segments")
    if stratified_selection_path is None:
        if acceptance_mode:
            raise ValueError("acceptance execution requires a frozen stratified selection artifact")
        raise ValueError("holdout execution requires a frozen stratified selection artifact")
    selection = verify_stratified_selection(
        stratified_selection_path,
        covenant_sha256=verification["covenant_sha256"],
        reservoir_segments=reservoirs,
        require_acceptance_shape=acceptance_mode,
    )
    return {
        "covenant_file": covenant_file,
        "covenant": covenant,
        "covenant_sha256": verification["covenant_sha256"],
        "winner": winner,
        "reservoir_segments": reservoirs,
        "segments": selection["segments"],
        "selection_file": selection["artifact_path"],
        "selection_sha256": selection["artifact_sha256"],
        "selection": selection["payload"],
        "winner_config": config,
        "winner_record": winner_record,
    }


def verify_stratified_selection(
    path: Union[str, Path],
    *,
    covenant_sha256: str,
    reservoir_segments: Sequence[dict[str, Any]],
    require_acceptance_shape: bool = True,
) -> dict[str, Any]:
    """Verify a frozen, pre-execution LLM-reference selection.

    This function validates declared strata and exact provenance only.  It does
    not infer semantic density with code and cannot top up a stratum.
    """

    artifact, payload = _read_json(path, purpose="frozen stratified selection")
    if payload.get("schema_version") != HOLDOUT_STRATIFIED_SELECTION_VERSION:
        raise ValueError("unsupported frozen stratified selection schema")
    if payload.get("selection_status") != "frozen_stratified_selection":
        raise ValueError("stratified selection is not frozen")
    if payload.get("selection_frozen") is not True:
        raise ValueError("stratified selection_frozen must be true")
    if payload.get("covenant_sha256") != covenant_sha256:
        raise ValueError("stratified selection does not match the holdout covenant")
    if payload.get("selection_basis") != "llm_reference_only_pre_candidate_pre_baseline":
        raise ValueError("stratified selection must be based only on frozen LLM reference output")
    if payload.get("candidate_outputs_observed") is not False:
        raise ValueError("stratified selection observed candidate outputs")
    if payload.get("baseline_outputs_observed") is not False:
        raise ValueError("stratified selection observed baseline outputs")
    if payload.get("adaptive_top_up") is not False:
        raise ValueError("adaptive stratified selection top-up is prohibited")
    reference_hash = payload.get("reference_artifact_sha256")
    if not (
        isinstance(reference_hash, str)
        and len(reference_hash) == 64
        and all(character in "0123456789abcdef" for character in reference_hash)
    ):
        raise ValueError("stratified selection reference_artifact_sha256 is invalid")

    universe = {str(row["segment_id"]): row for row in reservoir_segments}
    selected_rows = payload.get("segments")
    if not isinstance(selected_rows, list) or not selected_rows:
        raise ValueError("stratified selection contains no segments")
    enriched = []
    seen: set[str] = set()
    counts: Counter[tuple[str, str]] = Counter()
    for expected_order, selected in enumerate(selected_rows):
        if not isinstance(selected, dict):
            raise ValueError("stratified selection segment is malformed")
        segment_id = str(selected.get("segment_id") or "")
        if not segment_id or segment_id in seen:
            raise ValueError("stratified selection segment IDs must be unique")
        seen.add(segment_id)
        if selected.get("selection_order") != expected_order:
            raise ValueError("stratified selection order must be contiguous and frozen")
        frozen = universe.get(segment_id)
        if frozen is None:
            raise ValueError(f"stratified selection is not a covenant-reservoir subset: {segment_id}")
        for field in ("episode_id", "transcript_id", "text_sha256"):
            if str(selected.get(field)) != str(frozen.get(field)):
                raise ValueError(f"stratified selection {field} drift: {segment_id}")
        evaluation_set = selected.get("evaluation_set")
        stratum = selected.get("stratum")
        allowed = (
            evaluation_set == "paired_quality"
            and stratum in {"no_signal", "low", "medium", "dense"}
        ) or (evaluation_set == "clean_no_signal_power" and stratum == "no_signal")
        if not allowed:
            raise ValueError(f"invalid stratified selection lane or stratum: {segment_id}")
        expected_reservoir = (
            "paired_quality_reservoir"
            if evaluation_set == "paired_quality"
            else "terminal_position_no_signal_candidates"
        )
        if frozen.get("reservoir") != expected_reservoir:
            raise ValueError(f"stratified selection lane is not from its frozen reservoir: {segment_id}")
        if (
            evaluation_set == "clean_no_signal_power"
            and selected.get("reference_clean_no_signal") is not True
        ):
            raise ValueError(f"clean no-signal case lacks frozen LLM-reference confirmation: {segment_id}")
        case_hash = selected.get("reference_case_sha256")
        if not (
            isinstance(case_hash, str)
            and len(case_hash) == 64
            and all(character in "0123456789abcdef" for character in case_hash)
        ):
            raise ValueError(f"stratified selection reference case hash is invalid: {segment_id}")
        event_count = selected.get("reference_event_count")
        if isinstance(event_count, bool) or not isinstance(event_count, int) or event_count < 0:
            raise ValueError(f"stratified selection reference event count is invalid: {segment_id}")
        count_matches_stratum = (
            (stratum == "no_signal" and event_count == 0)
            or (stratum == "low" and 1 <= event_count <= 4)
            or (stratum == "medium" and 5 <= event_count <= 15)
            or (stratum == "dense" and event_count >= 16)
        )
        if not count_matches_stratum:
            raise ValueError(f"stratified selection event count contradicts stratum: {segment_id}")
        counts[(str(evaluation_set), str(stratum))] += 1
        enriched.append(
            {
                **frozen,
                "evaluation_set": evaluation_set,
                "reference_stratum": stratum,
                "selection_order": expected_order,
                "reference_case_sha256": case_hash,
                "reference_event_count": event_count,
            }
        )
    expected_counts = {
        ("paired_quality", "no_signal"): 15,
        ("paired_quality", "low"): 15,
        ("paired_quality", "medium"): 15,
        ("paired_quality", "dense"): 15,
        ("clean_no_signal_power", "no_signal"): 60,
    }
    if require_acceptance_shape and dict(counts) != expected_counts:
        rendered = {f"{lane}:{stratum}": count for (lane, stratum), count in sorted(counts.items())}
        raise ValueError(
            "acceptance stratified selection must contain exactly 15 each paired stratum and "
            f"60 separate clean no-signal cases; observed {rendered}"
        )
    declared_counts = payload.get("stratum_counts")
    observed_counts = {
        f"{lane}:{stratum}": count for (lane, stratum), count in sorted(counts.items())
    }
    if declared_counts != observed_counts:
        raise ValueError("stratified selection declared counts do not match its frozen segment list")
    return {
        "ok": True,
        "artifact_path": str(artifact),
        "artifact_sha256": _sha256_file(artifact),
        "segments": enriched,
        "counts": observed_counts,
        "payload": payload,
    }


def _ensure_execution_plan(
    root: Path,
    *,
    inputs: dict[str, Any],
    event_cap: Optional[int],
) -> dict[str, Any]:
    config = inputs["winner_config"]
    frozen_event_cap = int(config["max_events_per_segment"])
    if event_cap is not None and event_cap != frozen_event_cap:
        raise ValueError("holdout event cap must equal the frozen winner event cap")
    event_cap = frozen_event_cap
    reference_max = max(int(row["reference_event_count"]) for row in inputs["segments"])
    if reference_max > event_cap:
        raise ValueError(
            f"frozen holdout reference count {reference_max} exceeds winner event cap {event_cap}"
        )
    instruction_contract = verify_instruction_contract()
    execution_lineage = verified_holdout_execution_lineage(instruction_contract)
    plan = {
        "schema_version": HOLDOUT_EXECUTION_PLAN_VERSION,
        "status": "frozen_no_production_promotion",
        "covenant_sha256": inputs["covenant_sha256"],
        "winner_artifact_sha256": inputs["winner"]["artifact_sha256"],
        "stratified_selection_sha256": inputs["selection_sha256"],
        "winner": inputs["winner"]["payload"]["winner"],
        "winner_config": config,
        "segment_ids": [str(row["segment_id"]) for row in inputs["segments"]],
        "segment_text_sha256s": [str(row["text_sha256"]) for row in inputs["segments"]],
        "event_cap": event_cap,
        "observed_reference_event_max": reference_max,
        "context": {"model": CONTEXT_MODEL, "reasoning_effort": CONTEXT_EFFORT},
        "baseline": {
            "label_pack": LABEL_PACK,
            "model": BASELINE_MODEL,
            "reasoning_effort": BASELINE_EFFORT,
        },
        "retry_policy": "zero_model_retries_intent_to_treat",
        "instruction_contract": instruction_contract,
        "execution_lineage": execution_lineage,
        "label_pack_contract": verify_label_pack_contract(LABEL_PACK),
        "database_policy": "read_only_no_production_mutation",
        "promotion_policy": "never_promote_from_execution_module",
    }
    _ensure_immutable(root / "execution-plan.json", _canonical_bytes(plan))
    return plan


def episode_context_output_schema(episode_id: str) -> dict[str, Any]:
    """Structured schema aligned with ``validate_episode_context_output``."""

    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "schema_version",
            "episode_id",
            "context_summary",
            "speaker_map",
            "section_map",
            "entity_seed",
            "concept_seed",
            "extraction_guidance",
            "episode_context",
            "excluded_source_context",
            "quality_flags",
            "overall_confidence",
            "needs_review",
            "review_reason",
        ],
        "properties": {
            "schema_version": {"type": "string", "const": EPISODE_CONTEXT_SCHEMA_VERSION},
            "episode_id": {"type": "string", "const": episode_id},
            "context_summary": {"type": "string"},
            "speaker_map": {"type": "array"},
            "section_map": {"type": "array"},
            "entity_seed": {"type": "object"},
            "concept_seed": {"type": "array"},
            "extraction_guidance": {"type": "string"},
            "episode_context": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "context_summary",
                    "speaker_map",
                    "section_map",
                    "entity_seed",
                    "concept_seed",
                ],
                "properties": {
                    "context_summary": {"type": "string"},
                    "speaker_map": {"type": "array"},
                    "section_map": {"type": "array"},
                    "entity_seed": {"type": "object"},
                    "concept_seed": {"type": "array"},
                },
            },
            "excluded_source_context": {
                "type": "array",
                "items": {"type": "string"},
            },
            "quality_flags": {"type": "array"},
            "overall_confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "needs_review": {"type": "boolean"},
            "review_reason": {"type": ["string", "null"]},
        },
    }


def _episode_records(segments: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for segment in segments:
        by_episode[str(segment["episode_id"])].append(segment)
    records = []
    for episode_id, rows in sorted(by_episode.items()):
        identity_fields = (
            "source_id",
            "source_name",
            "transcript_id",
            "raw_text_sha256",
            "preparation_id",
            "prepared_text_sha256",
        )
        for field in identity_fields:
            if len({str(row.get(field)) for row in rows}) != 1:
                raise ValueError(f"holdout episode has conflicting {field}: {episode_id}")
        records.append(
            {
                "episode_id": episode_id,
                **{field: rows[0].get(field) for field in identity_fields},
                "selected_segments": sorted(rows, key=lambda row: (int(row["segment_index"]), row["segment_id"])),
            }
        )
    return records


def _build_full_episode_context_input(conn: sqlite3.Connection, episode: dict[str, Any]) -> dict[str, Any]:
    transcript = conn.execute(
        """
        SELECT t.id, t.episode_id, t.raw_text_path, t.raw_text_sha256,
               e.title, e.published_at, e.source_id, s.name AS source_name,
               tp.id AS preparation_id, tp.status AS preparation_status,
               tp.cleaned_text_path, tp.cleaned_text_sha256,
               tp.artifact_type, tp.substantive_word_count, tp.boilerplate_ratio,
               tp.speaker_turn_count, tp.quality_score
        FROM transcripts t
        JOIN episodes e ON e.id = t.episode_id
        JOIN sources s ON s.id = e.source_id
        JOIN transcript_preparations tp ON tp.transcript_id = t.id
        WHERE t.id = ? AND t.episode_id = ? AND t.status = 'ready' AND tp.status = 'prepared'
        """,
        (episode["transcript_id"], episode["episode_id"]),
    ).fetchone()
    if not transcript:
        raise ValueError(f"frozen canonical transcript is no longer ready: {episode['episode_id']}")
    if str(transcript["source_id"]) != str(episode["source_id"]):
        raise ValueError(f"frozen canonical transcript source drift: {episode['episode_id']}")
    if str(transcript["preparation_id"]) != str(episode["preparation_id"]):
        raise ValueError(f"frozen canonical transcript preparation drift: {episode['episode_id']}")
    _, raw_text = _verified_text_file(
        transcript["raw_text_path"],
        str(episode["raw_text_sha256"]),
        purpose=f"frozen raw transcript {episode['episode_id']}",
    )
    if transcript["raw_text_sha256"] != episode["raw_text_sha256"]:
        raise ValueError(f"frozen raw transcript DB hash drift: {episode['episode_id']}")
    _, prepared_text = _verified_text_file(
        transcript["cleaned_text_path"],
        str(episode["prepared_text_sha256"]),
        purpose=f"frozen prepared transcript {episode['episode_id']}",
    )
    if transcript["cleaned_text_sha256"] != episode["prepared_text_sha256"]:
        raise ValueError(f"frozen prepared transcript DB hash drift: {episode['episode_id']}")
    del raw_text  # The raw artifact is hash-verified but never placed in the prompt.

    rows = conn.execute(
        """
        SELECT id, segment_index, start_char, end_char, word_count, text_path, text_sha256
        FROM segments
        WHERE transcript_id = ? AND episode_id = ?
        ORDER BY segment_index, id
        """,
        (episode["transcript_id"], episode["episode_id"]),
    ).fetchall()
    if not rows:
        raise ValueError(f"frozen canonical transcript has no segments: {episode['episode_id']}")
    parts = []
    total_words = 0
    for row in rows:
        _, text = _verified_text_file(
            row["text_path"], str(row["text_sha256"]), purpose=f"segment {row['id']}"
        )
        start = int(row["start_char"])
        end = int(row["end_char"])
        if not (0 <= start <= end <= len(prepared_text)) or prepared_text[start:end] != text:
            raise ValueError(f"segment/prepared-transcript offset drift: {row['id']}")
        total_words += int(row["word_count"] or len(text.split()))
        parts.append(
            "\n".join(
                (
                    f"===== SEGMENT {row['segment_index']} | {row['id']} | "
                    f"chars={start}-{end} | words={row['word_count']} =====",
                    text,
                )
            )
        )
    metadata = {
        "episode_id": episode["episode_id"],
        "transcript_id": episode["transcript_id"],
        "source_id": episode["source_id"],
        "source_name": transcript["source_name"],
        "episode_title": transcript["title"],
        "episode_published_at": transcript["published_at"],
        "transcript_preparation_id": transcript["preparation_id"],
        "transcript_artifact_type": transcript["artifact_type"],
        "transcript_preparation_status": transcript["preparation_status"],
        "transcript_substantive_word_count": transcript["substantive_word_count"],
        "transcript_boilerplate_ratio": transcript["boilerplate_ratio"],
        "transcript_speaker_turn_count": transcript["speaker_turn_count"],
        "transcript_quality_score": transcript["quality_score"],
        "segment_count": len(rows),
        "total_segment_words": total_words,
        "privacy_boundary": "private_analysis_only_do_not_output_full_transcript",
    }
    return {
        "episode": metadata,
        "full_segmented_episode_text": "\n\n".join(parts),
    }


def _sidecar_usage(path: Path) -> tuple[Optional[dict[str, int]], bool, Optional[str]]:
    if not path.is_file():
        return None, False, "sidecar_missing"
    try:
        sidecar = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, False, "sidecar_invalid"
    if not isinstance(sidecar, dict) or sidecar.get("state") not in _TERMINAL_SIDECAR_STATES:
        return None, False, "sidecar_nonterminal"
    usage = sidecar.get("usage")
    if sidecar.get("usage_complete") is not True or not isinstance(usage, dict):
        return None, False, "usage_unknown"
    normalized = {}
    for field in _USAGE_FIELDS:
        value = usage.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None, False, f"usage_invalid_{field}"
        normalized[field] = value
    if normalized["cached_input_tokens"] > normalized["input_tokens"]:
        return None, False, "usage_cached_exceeds_input"
    if normalized["reasoning_output_tokens"] > normalized["output_tokens"]:
        return None, False, "usage_reasoning_exceeds_output"
    if normalized["total_tokens"] != normalized["input_tokens"] + normalized["output_tokens"]:
        return None, False, "usage_total_mismatch"
    return normalized, True, None


def _aggregate_usage(outcomes: Sequence[dict[str, Any]]) -> dict[str, int]:
    total: Counter[str] = Counter()
    for outcome in outcomes:
        usage = outcome.get("usage")
        if isinstance(usage, dict):
            total.update({field: int(usage.get(field) or 0) for field in _USAGE_FIELDS})
    return {field: int(total[field]) for field in _USAGE_FIELDS}


def _valid_usage(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != set(_USAGE_FIELDS):
        return False
    if any(
        isinstance(value[field], bool)
        or not isinstance(value[field], int)
        or value[field] < 0
        for field in _USAGE_FIELDS
    ):
        return False
    return bool(
        value["cached_input_tokens"] <= value["input_tokens"]
        and value["reasoning_output_tokens"] <= value["output_tokens"]
        and value["total_tokens"]
        == value["input_tokens"] + value["output_tokens"]
    )


def _inside(path: Path, root: Path) -> bool:
    try:
        path.expanduser().resolve().relative_to(root.expanduser().resolve())
    except ValueError:
        return False
    return True


def _verified_record_payload(
    record: Any, *, root: Path, purpose: str
) -> tuple[Path, dict[str, Any]]:
    if not isinstance(record, Mapping):
        raise ValueError(f"{purpose} record is missing")
    path_value = record.get("path")
    if not isinstance(path_value, str) or not path_value:
        raise ValueError(f"{purpose} path is missing")
    path = Path(path_value).expanduser().resolve()
    if (
        not _inside(path, root)
        or not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise ValueError(f"{purpose} record drifted")
    _, payload = _read_json(path, purpose=purpose)
    return path, payload


def _checkpoint(path: Path, payload: dict[str, Any]) -> None:
    write_text_atomic(path, json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


def _validate_terminal_phase_lineage(
    report: Mapping[str, Any],
    *,
    phase_root: Path,
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any],
) -> None:
    if (
        report.get("instruction_contract") != instruction_contract
        or report.get("execution_lineage") != execution_lineage
    ):
        raise ValueError("terminal holdout phase execution lineage drift")
    root = Path(phase_root).expanduser().resolve()
    expected_by_phase = {
        "A_episode_contexts": {
            "schema_version": HOLDOUT_CONTEXT_PHASE_VERSION,
            "root_name": "phase-a-contexts",
            "model": CONTEXT_MODEL,
            "effort": CONTEXT_EFFORT,
            "count_field": "requested_episodes",
            "identity_field": "episode_id",
            "leaf_dir": "episodes",
        },
        "B_frozen_winner_candidate": {
            "schema_version": HOLDOUT_CANDIDATE_PHASE_VERSION,
            "root_name": "phase-b-candidate",
            "model": expanded_cap.MODEL,
            "effort": expanded_cap.EFFORT,
            "count_field": "requested_calls",
            "identity_field": "batch_id",
            "leaf_dir": None,
        },
        "C_full_v31_baseline": {
            "schema_version": HOLDOUT_BASELINE_PHASE_VERSION,
            "root_name": "phase-c-baseline",
            "model": BASELINE_MODEL,
            "effort": BASELINE_EFFORT,
            "count_field": "requested_segments",
            "identity_field": "segment_id",
            "leaf_dir": "segments",
        },
    }
    expected = expected_by_phase.get(str(report.get("phase") or ""))
    if (
        expected is None
        or not root.is_dir()
        or root.name != expected["root_name"]
        or report.get("schema_version") != expected["schema_version"]
    ):
        raise ValueError("terminal holdout phase schema or root identity drift")
    leaf_bindings = report.get("leaf_bindings")
    if not isinstance(leaf_bindings, list):
        raise ValueError("terminal holdout phase leaf bindings are missing")
    seen_sidecars: set[str] = set()
    seen_turns: set[str] = set()
    for binding in leaf_bindings:
        validated = validate_holdout_leaf_binding(
            binding,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
            expected_model=expected["model"],
            expected_effort=expected["effort"],
        )
        if any(
            not _inside(Path(str(record.get("path") or "")), root)
            for record in validated["artifacts"].values()
        ):
            raise ValueError("terminal holdout phase leaf artifact is outside current root")
        sidecar_path = validated["artifacts"]["sidecar"]["path"]
        turn_id = validated["turn_id"]
        if sidecar_path in seen_sidecars or turn_id in seen_turns:
            raise ValueError("terminal holdout phase leaf identity is duplicated")
        seen_sidecars.add(sidecar_path)
        seen_turns.add(turn_id)
    count_value = report.get(expected["count_field"])
    if isinstance(count_value, bool) or not isinstance(count_value, int) or count_value < 0:
        raise ValueError("terminal holdout phase request count is malformed")
    expected_count = count_value
    if report.get("ok") is True:
        if len(leaf_bindings) != expected_count:
            raise ValueError("terminal holdout phase leaf binding count mismatch")
    outcomes = report.get("outcomes") or []
    if not isinstance(outcomes, list):
        raise ValueError("terminal holdout phase outcomes are malformed")
    if len(outcomes) != expected_count:
        raise ValueError("terminal holdout phase does not enumerate every leaf outcome")
    identities = [
        str(outcome.get(expected["identity_field"]) or "")
        for outcome in outcomes
        if isinstance(outcome, Mapping)
    ]
    if (
        len(identities) != len(outcomes)
        or any(not identity for identity in identities)
        or len(identities) != len(set(identities))
    ):
        raise ValueError("terminal holdout phase leaf outcome identities are malformed")
    binding_by_sidecar = {
        str(binding["artifacts"]["sidecar"]["path"]): binding
        for binding in leaf_bindings
    }
    validated_outcome_sidecars: set[str] = set()
    for outcome in outcomes:
        if not isinstance(outcome, Mapping):
            raise ValueError("terminal holdout phase outcome is malformed")
        identity = str(outcome[expected["identity_field"]])
        if report.get("phase") == "B_frozen_winner_candidate":
            expected_sidecar = expanded_cap._batch_paths(  # noqa: SLF001 - frozen adapter layout
                root / "candidate-arm", identity
            )["sidecar"].resolve()
        else:
            expected_sidecar = (
                root / str(expected["leaf_dir"]) / identity / "sidecar.json"
            ).resolve()
        if not _inside(expected_sidecar, root):
            raise ValueError("terminal holdout phase request identity escapes current root")
        sidecar_value = outcome.get("sidecar_path")
        if sidecar_value is None:
            if outcome.get("status") == "validated":
                raise ValueError("validated holdout outcome has no sidecar lineage")
            continue
        sidecar_path = Path(str(sidecar_value)).expanduser().resolve()
        if sidecar_path != expected_sidecar or not sidecar_path.is_file():
            raise ValueError("terminal holdout sidecar request identity drift")
        _, sidecar = _read_json(sidecar_path, purpose="terminal holdout sidecar")
        validate_managed_sidecar_execution_lineage(
            sidecar=sidecar,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        if outcome.get("status") == "validated":
            binding = binding_by_sidecar.get(str(expected_sidecar))
            if binding is None:
                raise ValueError("validated holdout outcome lacks its exact leaf binding")
            if (
                report.get("phase") == "B_frozen_winner_candidate"
                and outcome.get("leaf_identity_sha256")
                != binding.get("leaf_identity_sha256")
            ):
                raise ValueError("terminal candidate outcome leaf identity drift")
            validated_outcome_sidecars.add(str(expected_sidecar))
    if validated_outcome_sidecars != seen_sidecars:
        raise ValueError("terminal holdout phase bindings differ from validated outcomes")

    if report.get("phase") == "B_frozen_winner_candidate" and report.get("ok") is True:
        candidate_dir = root / "candidate-arm"
        expected_mapping_path = (candidate_dir / "private-mapping.json").resolve()
        mapping_value = report.get("candidate_mapping_path")
        if (
            not isinstance(mapping_value, str)
            or Path(mapping_value).expanduser().resolve() != expected_mapping_path
            or not expected_mapping_path.is_file()
            or report.get("candidate_mapping_sha256")
            != _sha256_file(expected_mapping_path)
        ):
            raise ValueError("terminal candidate mapping identity drift")
        _, mapping = _read_json(
            expected_mapping_path, purpose="terminal candidate private mapping"
        )
        batches = mapping.get("batches")
        mapped_batch_ids = [
            str(batch.get("batch_id") or "")
            for batch in batches or []
            if isinstance(batch, Mapping)
        ]
        if (
            mapping.get("schema_version") != expanded_cap.PRIVATE_MAPPING_VERSION
            or mapping.get("winner_system_id") != expanded_cap.WINNER_SYSTEM_ID
            or not isinstance(batches, list)
            or len(mapped_batch_ids) != len(batches)
            or mapped_batch_ids != identities
        ):
            raise ValueError("terminal candidate request mapping drift")
        for batch in batches:
            batch_id = str(batch["batch_id"])
            expected_sidecar = expanded_cap._batch_paths(  # noqa: SLF001
                candidate_dir, batch_id
            )["sidecar"].resolve()
            if (
                not _inside(expected_sidecar, candidate_dir)
                or Path(str(batch.get("sidecar_path") or "")).expanduser().resolve()
                != expected_sidecar
            ):
                raise ValueError("terminal candidate mapped sidecar request identity drift")


async def _wait_for_capacity_before_attempt(client: Any) -> None:
    wait = getattr(client, "wait_for_semantic_capacity", None)
    if callable(wait):
        await wait()


def _context_base_instructions() -> str:
    return (
        "You are the frozen GPT-5.5 high-reasoning full-episode context reader for the private "
        "ai_discourse_v3_1 holdout. Follow the supplied prompt and structured schema exactly. "
        "Do not use tools, network access, or local files. Return only the compact context JSON."
    )


def _context_manifest(
    conn: sqlite3.Connection,
    *,
    phase_root: Path,
    inputs: dict[str, Any],
    contexts: dict[str, dict[str, Any]],
    event_cap: int,
) -> dict[str, Any]:
    episodes = []
    for episode in _episode_records(inputs["segments"]):
        episode_id = episode["episode_id"]
        context = contexts[episode_id]
        selected = []
        for row in episode["selected_segments"]:
            selected.append(
                {
                    "segment_id": row["segment_id"],
                    "segment_index": int(row["segment_index"]),
                    "text_sha256": row["text_sha256"],
                    "golden_output_sha256": row["reference_case_sha256"],
                    "golden_event_count": int(row["reference_event_count"]),
                    "density_stratum": row["reference_stratum"],
                    "evaluation_set": row["evaluation_set"],
                    "holdout_reservoir": row["reservoir"],
                }
            )
        episode_row = conn.execute(
            "SELECT title FROM episodes WHERE id = ?", (episode_id,)
        ).fetchone()
        episodes.append(
            {
                "episode_id": episode_id,
                "source_id": episode["source_id"],
                "source_name": episode["source_name"],
                "episode_title": episode_row["title"] if episode_row else "",
                "canonical_transcript": {
                    "selection_policy": "frozen_holdout_covenant",
                    "transcript_id": episode["transcript_id"],
                    "raw_text_sha256": episode["raw_text_sha256"],
                    "preparation_id": episode["preparation_id"],
                    "prepared_text_sha256": episode["prepared_text_sha256"],
                },
                "episode_context": {
                    "run_id": context["context_id"],
                    "transcript_id": episode["transcript_id"],
                    "artifact_path": context["artifact_path"],
                    "artifact_sha256": context["artifact_sha256"],
                },
                "segments": selected,
                "density_counts": dict(
                    sorted(Counter(row["density_stratum"] for row in selected).items())
                ),
                "observed_golden_event_max": max(row["golden_event_count"] for row in selected),
            }
        )
    all_segments = [segment for episode in episodes for segment in episode["segments"]]
    return {
        "schema_version": HOLDOUT_PRIVATE_MANIFEST_VERSION,
        "evaluation_role": "untouched_private_holdout_never_production",
        "seed": inputs["covenant"].get("seed"),
        "selection_policy": "frozen_covenant_exact_segment_ids_and_text_sha256s_no_semantic_selection",
        "label_pack": LABEL_PACK,
        "golden_model": BASELINE_MODEL,
        "event_cap": event_cap,
        "dense_event_min": 16,
        "segments_per_episode": None,
        "minimum_no_signal_per_episode": None,
        "maximum_no_signal_per_episode": None,
        "minimum_dense_per_episode": None,
        "episode_count": len(episodes),
        "source_count": len({episode["source_id"] for episode in episodes}),
        "segment_count": len(all_segments),
        "unique_text_sha256_count": len({segment["text_sha256"] for segment in all_segments}),
        "density_counts": dict(
            sorted(Counter(segment["density_stratum"] for segment in all_segments).items())
        ),
        "observed_golden_event_max": max(segment["golden_event_count"] for segment in all_segments),
        "episodes": episodes,
        "covenant_sha256": inputs["covenant_sha256"],
        "context_phase_root": str(phase_root),
        "privacy": "private_analysis_only_contains_local_paths_ids_and_hashes_no_transcript_text",
    }


async def run_holdout_context_phase(
    conn: sqlite3.Connection,
    *,
    covenant_path: Union[str, Path],
    frozen_winner_path: Union[str, Path],
    stratified_selection_path: Optional[Union[str, Path]],
    execution_dir: Union[str, Path],
    acceptance_mode: bool = True,
    event_cap: Optional[int] = None,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
) -> dict[str, Any]:
    inputs = _load_execution_inputs(
        conn,
        covenant_path=covenant_path,
        frozen_winner_path=frozen_winner_path,
        stratified_selection_path=stratified_selection_path,
        acceptance_mode=acceptance_mode,
    )
    root = Path(execution_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    execution_plan = _ensure_execution_plan(root, inputs=inputs, event_cap=event_cap)
    instruction_contract = execution_plan["instruction_contract"]
    execution_lineage = execution_plan["execution_lineage"]
    effective_client_factory = _resolve_holdout_client_factory(
        client_factory, instruction_contract
    )
    phase_root = root / "phase-a-contexts"
    phase_root.mkdir(parents=True, exist_ok=True)
    report_path = phase_root / "report.json"
    if report_path.is_file():
        _, report = _read_json(report_path, purpose="context phase report")
        _validate_terminal_phase_lineage(
            report,
            phase_root=phase_root,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        return report

    episodes = _episode_records(inputs["segments"])
    plan = {
        "schema_version": HOLDOUT_CONTEXT_PHASE_VERSION,
        "phase": "A_episode_contexts",
        "model": CONTEXT_MODEL,
        "reasoning_effort": CONTEXT_EFFORT,
        "persistent_client_count": 1,
        "retry_count": 0,
        "episode_ids": [episode["episode_id"] for episode in episodes],
        "covenant_sha256": inputs["covenant_sha256"],
        "stratified_selection_sha256": inputs["selection_sha256"],
        "instruction_contract": instruction_contract,
        "execution_lineage": execution_lineage,
        "production_database_mutation": False,
    }
    _ensure_immutable(phase_root / "plan.json", _canonical_bytes(plan))
    before_changes = conn.total_changes
    prepared: dict[str, dict[str, Any]] = {}
    for episode in episodes:
        context_input = _build_full_episode_context_input(conn, episode)
        episode_id = episode["episode_id"]
        episode_dir = phase_root / "episodes" / episode_id
        prompt = render_episode_context_prompt(LABEL_PACK, context_input) + (
            "\n\n# Frozen source-context exclusion authority\n"
            "Also return excluded_source_context as a JSON array of concise semantic source-context "
            "classes that later extraction must exclude (for example ads, show setup, or page chrome). "
            "Derive this list from the complete episode; do not use keyword or regex rules."
        )
        schema = episode_context_output_schema(episode_id)
        prompt_bytes = prompt.encode("utf-8")
        schema_bytes = _canonical_bytes(schema)
        _ensure_immutable(episode_dir / "prompt.md", prompt_bytes)
        _ensure_immutable(episode_dir / "schema.json", schema_bytes)
        prepared[episode_id] = {
            "episode": episode,
            "dir": episode_dir,
            "prompt": prompt,
            "schema": schema,
            "prompt_sha256": _sha256_bytes(prompt_bytes),
            "schema_sha256": _sha256_bytes(schema_bytes),
        }

    checkpoint_path = phase_root / "checkpoint.json"
    if checkpoint_path.is_file():
        _, checkpoint = _read_json(checkpoint_path, purpose="context checkpoint")
    else:
        checkpoint = {"schema_version": HOLDOUT_CONTEXT_PHASE_VERSION, "outcomes": {}}
    outcomes_by_id = checkpoint.get("outcomes")
    if not isinstance(outcomes_by_id, dict):
        raise ValueError("context checkpoint outcomes are malformed")

    base_instructions = _context_base_instructions()
    base_instructions_path = phase_root / "base-instructions.md"
    _ensure_immutable(base_instructions_path, base_instructions.encode("utf-8"))
    async with LazyClientSession(effective_client_factory) as client:
        for episode_id in [episode["episode_id"] for episode in episodes]:
            item = prepared[episode_id]
            episode_dir = item["dir"]
            attempt_path = episode_dir / "attempt.json"
            outcome_path = episode_dir / "outcome.json"
            sidecar_path = episode_dir / "sidecar.json"
            raw_output_path = episode_dir / "raw-output.json"
            context_path = episode_dir / "context.json"
            if episode_id in outcomes_by_id:
                outcome = outcomes_by_id[episode_id]
                if outcome.get("status") == "validated":
                    _sidecar, usage = validate_completed_managed_sidecar(
                        sidecar_path=sidecar_path,
                        raw_output_path=raw_output_path,
                        model=CONTEXT_MODEL,
                        effort=CONTEXT_EFFORT,
                        thread_mode="new_thread",
                        batch_size=1,
                        prompt=item["prompt"],
                        output_schema=item["schema"],
                        base_instructions=base_instructions,
                        instruction_contract=instruction_contract,
                        execution_lineage=execution_lineage,
                    )
                    if usage != outcome.get("usage"):
                        raise ValueError("context checkpoint usage drift")
                    if (
                        _sha256_file(sidecar_path) != outcome.get("sidecar_sha256")
                        or _sha256_file(raw_output_path) != outcome.get("raw_output_sha256")
                    ):
                        raise ValueError("context checkpoint artifact hash drift")
                continue
            if attempt_path.exists():
                if outcome_path.is_file():
                    _, outcome = _read_json(outcome_path, purpose="context episode outcome")
                else:
                    outcome = {
                        "episode_id": episode_id,
                        "status": "ambiguous_interrupted_no_retry",
                        "status_ok": False,
                        "usage": None,
                        "usage_complete": False,
                        "failure_class": "prior_attempt_has_no_terminal_outcome",
                    }
                    _write_immutable(outcome_path, _canonical_bytes(outcome))
                if outcome.get("status") == "validated":
                    _sidecar, usage = validate_completed_managed_sidecar(
                        sidecar_path=sidecar_path,
                        raw_output_path=raw_output_path,
                        model=CONTEXT_MODEL,
                        effort=CONTEXT_EFFORT,
                        thread_mode="new_thread",
                        batch_size=1,
                        prompt=item["prompt"],
                        output_schema=item["schema"],
                        base_instructions=base_instructions,
                        instruction_contract=instruction_contract,
                        execution_lineage=execution_lineage,
                    )
                    if usage != outcome.get("usage"):
                        raise ValueError("context outcome usage differs from managed sidecar")
                    if (
                        _sha256_file(sidecar_path) != outcome.get("sidecar_sha256")
                        or _sha256_file(raw_output_path) != outcome.get("raw_output_sha256")
                    ):
                        raise ValueError("context checkpoint artifact hash drift")
                outcomes_by_id[episode_id] = outcome
                _checkpoint(checkpoint_path, checkpoint)
                continue
            if not attempt_path.exists():
                unexpected = [
                    path
                    for path in (outcome_path, sidecar_path, raw_output_path, context_path)
                    if path.exists()
                ]
                if unexpected:
                    raise ValueError(
                        "context artifacts exist without an immutable attempt: "
                        + ", ".join(str(path) for path in unexpected)
                    )
                await _wait_for_capacity_before_attempt(client)
                attempt = {
                    "episode_id": episode_id,
                    "model": CONTEXT_MODEL,
                    "reasoning_effort": CONTEXT_EFFORT,
                    "prompt_sha256": item["prompt_sha256"],
                    "schema_sha256": item["schema_sha256"],
                    "retry_ordinal": 0,
                }
                _write_immutable(attempt_path, _canonical_bytes(attempt))
            try:
                result = await client.run_ephemeral_structured_turn(
                    model=CONTEXT_MODEL,
                    effort=CONTEXT_EFFORT,
                    base_instructions=base_instructions,
                    prompt=item["prompt"],
                    output_schema=item["schema"],
                    cwd=Path.cwd(),
                    sidecar_path=sidecar_path,
                    output_path=raw_output_path,
                    batch_size=1,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                )
                if not result.status_ok or not isinstance(result.output, dict):
                    raise ValueError(result.error_class or f"turn_{result.status}")
                if not raw_output_path.exists():
                    _write_immutable(raw_output_path, _canonical_bytes(result.output))
                artifact = validate_episode_context_output(
                    result.output, expected_episode_id=episode_id
                )
                excluded = result.output.get("excluded_source_context")
                if not isinstance(excluded, list) or any(
                    not isinstance(item, str) for item in excluded
                ):
                    raise ValueError(
                        "episode context excluded_source_context must be a string array"
                    )
                artifact["excluded_source_context"] = copy.deepcopy(excluded)
                _write_immutable(context_path, _canonical_bytes(artifact))
                _sidecar, usage = validate_completed_managed_sidecar(
                    sidecar_path=sidecar_path,
                    raw_output_path=raw_output_path,
                    model=CONTEXT_MODEL,
                    effort=CONTEXT_EFFORT,
                    thread_mode="new_thread",
                    batch_size=1,
                    prompt=item["prompt"],
                    output_schema=item["schema"],
                    base_instructions=base_instructions,
                    instruction_contract=instruction_contract,
                    execution_lineage=execution_lineage,
                )
                usage_complete = True
                usage_error = None
                outcome = {
                    "episode_id": episode_id,
                    "context_id": stable_id(
                        inputs["covenant_sha256"], episode_id, prefix="holdctx_"
                    ),
                    "status": "validated" if usage_complete else "validated_usage_unknown",
                    "status_ok": usage_complete,
                    "failure_class": usage_error,
                    "prompt_sha256": item["prompt_sha256"],
                    "schema_sha256": item["schema_sha256"],
                    "raw_output_sha256": _sha256_file(raw_output_path),
                    "artifact_path": str(context_path),
                    "artifact_sha256": _sha256_file(context_path),
                    "sidecar_path": str(sidecar_path),
                    "sidecar_sha256": _sha256_file(sidecar_path),
                    "usage": usage,
                    "usage_complete": usage_complete,
                }
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # Model attempts are terminal; do not retry.
                usage, usage_complete, usage_error = _sidecar_usage(sidecar_path)
                outcome = {
                    "episode_id": episode_id,
                    "status": "failed_no_retry",
                    "status_ok": False,
                    "failure_class": type(exc).__name__,
                    "usage_error": usage_error,
                    "usage": usage,
                    "usage_complete": usage_complete,
                    "sidecar_path": str(sidecar_path) if sidecar_path.exists() else None,
                    "sidecar_sha256": _sha256_file(sidecar_path) if sidecar_path.exists() else None,
                }
            _write_immutable(outcome_path, _canonical_bytes(outcome))
            outcomes_by_id[episode_id] = outcome
            _checkpoint(checkpoint_path, checkpoint)

    outcomes = [outcomes_by_id[episode["episode_id"]] for episode in episodes]
    all_valid = all(outcome.get("status") == "validated" for outcome in outcomes)
    accounting_complete = all(outcome.get("usage_complete") is True for outcome in outcomes)
    contexts = {
        str(outcome["episode_id"]): outcome
        for outcome in outcomes
        if outcome.get("status") == "validated"
    }
    manifest_path = phase_root / "holdout-manifest.json"
    manifest_sha256 = None
    if all_valid and accounting_complete:
        manifest = _context_manifest(
            conn,
            phase_root=phase_root,
            inputs=inputs,
            contexts=contexts,
            event_cap=int(execution_plan["event_cap"]),
        )
        _ensure_immutable(manifest_path, _canonical_bytes(manifest))
        manifest_sha256 = _sha256_file(manifest_path)
    if conn.total_changes != before_changes:
        raise RuntimeError("context phase mutated the production database")
    leaf_bindings = [
        build_holdout_leaf_binding(
            sidecar_path=prepared[str(outcome["episode_id"])]["dir"] / "sidecar.json",
            prompt_path=prepared[str(outcome["episode_id"])]["dir"] / "prompt.md",
            output_schema_path=prepared[str(outcome["episode_id"])]["dir"] / "schema.json",
            base_instructions_path=base_instructions_path,
            raw_output_path=prepared[str(outcome["episode_id"])]["dir"] / "raw-output.json",
            model=CONTEXT_MODEL,
            effort=CONTEXT_EFFORT,
            thread_mode="new_thread",
            batch_size=1,
            prompt=prepared[str(outcome["episode_id"])]["prompt"],
            output_schema=prepared[str(outcome["episode_id"])]["schema"],
            base_instructions=base_instructions,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        for outcome in outcomes
        if outcome.get("status") == "validated"
    ]
    report = {
        "schema_version": HOLDOUT_CONTEXT_PHASE_VERSION,
        "phase": "A_episode_contexts",
        "ok": all_valid and accounting_complete,
        "execution_complete": len(outcomes) == len(episodes),
        "requested_episodes": len(episodes),
        "validated_episodes": sum(outcome.get("status") == "validated" for outcome in outcomes),
        "failed_episodes": sum(outcome.get("status") != "validated" for outcome in outcomes),
        "retry_count": 0,
        "persistent_client_count": 1,
        "accounting_complete": accounting_complete,
        "usage": _aggregate_usage(outcomes) if accounting_complete else None,
        "measured_partial_usage": _aggregate_usage(outcomes),
        "manifest_path": str(manifest_path) if manifest_path.exists() else None,
        "manifest_sha256": manifest_sha256,
        "outcomes": outcomes,
        "leaf_bindings": leaf_bindings,
        "instruction_contract": instruction_contract,
        "execution_lineage": execution_lineage,
        "production_database_mutation": False,
        "production_promotion": False,
    }
    _validate_terminal_phase_lineage(
        report,
        phase_root=phase_root,
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    _write_immutable(report_path, _canonical_bytes(report))
    return report


def _prepare_holdout_candidate_episodes(
    conn: sqlite3.Connection,
    *,
    inputs: Mapping[str, Any],
    manifest_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Reconstruct the exact final-adapter requests without a model call."""

    _, manifest = _read_json(manifest_path, purpose="holdout candidate manifest")
    config = inputs["winner_config"]
    expected_episode_rows = _episode_records(inputs["segments"])
    expected_episode_ids = [str(row["episode_id"]) for row in expected_episode_rows]
    expected_segment_ids = [
        str(segment["segment_id"])
        for episode in expected_episode_rows
        for segment in episode["selected_segments"]
    ]
    manifest_episodes = manifest.get("episodes")
    if (
        manifest.get("schema_version") != HOLDOUT_PRIVATE_MANIFEST_VERSION
        or manifest.get("evaluation_role")
        != "untouched_private_holdout_never_production"
        or manifest.get("covenant_sha256") != inputs["covenant_sha256"]
        or manifest.get("event_cap") != config["max_events_per_segment"]
        or manifest.get("segment_count") != len(expected_segment_ids)
        or manifest.get("episode_count") != len(expected_episode_ids)
        or not isinstance(manifest_episodes, list)
        or [str(row.get("episode_id") or "") for row in manifest_episodes]
        != expected_episode_ids
    ):
        raise ValueError("holdout candidate manifest does not bind the frozen selection")
    observed_segment_ids: list[str] = []
    selected_by_id = {
        str(row["segment_id"]): row for row in inputs["segments"]
    }
    for episode, expected_episode in zip(manifest_episodes, expected_episode_rows):
        if not isinstance(episode, Mapping):
            raise ValueError("holdout candidate manifest episode is malformed")
        segments = episode.get("segments")
        if not isinstance(segments, list):
            raise ValueError("holdout candidate manifest segments are malformed")
        episode_segment_ids = [
            str(row.get("segment_id") or "")
            for row in segments
            if isinstance(row, Mapping)
        ]
        expected_ids = [
            str(row["segment_id"])
            for row in expected_episode["selected_segments"]
        ]
        if len(episode_segment_ids) != len(segments) or episode_segment_ids != expected_ids:
            raise ValueError("holdout candidate manifest segment order drifted")
        context_record = episode.get("episode_context")
        if not isinstance(context_record, Mapping):
            raise ValueError("holdout candidate episode context record is missing")
        context_path = _resolve_artifact(str(context_record.get("artifact_path") or ""))
        if (
            not _inside(context_path, manifest_path.parent)
            or not context_path.is_file()
            or _sha256_file(context_path) != context_record.get("artifact_sha256")
        ):
            raise ValueError("holdout candidate episode context artifact drifted")
        for segment in segments:
            segment_id = str(segment["segment_id"])
            selected = selected_by_id[segment_id]
            if (
                str(episode.get("episode_id")) != str(selected["episode_id"])
                or str(segment.get("text_sha256")) != str(selected["text_sha256"])
                or segment.get("density_stratum") != selected["reference_stratum"]
                or segment.get("evaluation_set") != selected["evaluation_set"]
            ):
                raise ValueError("holdout candidate manifest provenance drifted")
        observed_segment_ids.extend(episode_segment_ids)
    if observed_segment_ids != expected_segment_ids:
        raise ValueError("holdout candidate manifest does not exactly partition selection")

    episodes = evaluation._load_prepared_episodes(  # noqa: SLF001
        conn,
        manifest=manifest,
        window_count=int(inputs["winner_record"]["window_count"]),
        context_chars=int(inputs["winner_record"]["context_chars"]),
    )
    requests: list[dict[str, Any]] = []
    for episode in episodes:
        requests.extend(
            expanded_cap.prepare_episode_batches(
                episode,
                batch_size=int(config["batch_size"]),
                thread_mode=str(config["thread_mode"]),
            )
        )
    request_segment_ids = [
        str(segment_id)
        for request in requests
        for segment_id in request["segment_ids"]
    ]
    if request_segment_ids != expected_segment_ids:
        raise ValueError("final candidate request partition drifted from frozen selection")
    return episodes, requests, expected_segment_ids


def _validate_candidate_runner_report(
    report: Any,
    *,
    candidate_dir: Path,
    inputs: Mapping[str, Any],
    requests: Sequence[Mapping[str, Any]],
    segment_ids: Sequence[str],
    fixture_mode: bool,
    capacity_sha256: str,
) -> dict[str, Any]:
    if not isinstance(report, Mapping):
        raise ValueError("final candidate runner report is missing")
    report = dict(report)
    config = inputs["winner_config"]
    expected_calls = len(requests)
    fixed_valid = bool(
        report.get("schema_version") == expanded_cap.RUN_REPORT_VERSION
        and report.get("state") == "passed"
        and report.get("winner_system_id") == expanded_cap.WINNER_SYSTEM_ID
        and report.get("batch_size") == config["batch_size"]
        and report.get("thread_mode") == config["thread_mode"]
        and report.get("model") == config["model"]
        and report.get("effort") == config["effort"]
        and report.get("max_events_per_segment")
        == expanded_cap.MAX_EVENTS_PER_SEGMENT
        and report.get("semantic_postprocessing") is False
        and report.get("requested_calls") == expected_calls
        and report.get("attempted_calls") == expected_calls
        and report.get("validated_calls") == expected_calls
        and report.get("terminal_sidecars") == expected_calls
        and report.get("attempt_contract_failures") == 0
        and report.get("sidecar_contract_failures") == 0
        and report.get("usage_status") == "complete"
        and report.get("accounting_complete") is True
        and report.get("usage_measured_attempts") == expected_calls
        and report.get("usage_unknown_attempts") == 0
        and report.get("ambiguous_outcome_attempts") == 0
        and report.get("retry_count") == 0
        and report.get("ambiguous_retry_count") == 0
        and report.get("all_emitted_events_preserved") is True
        and report.get("capacity_binding_valid") is True
        and report.get("fixture_mode") is fixture_mode
        and report.get("production_mutated") is False
        and _valid_usage(report.get("usage"))
    )
    if not fixed_valid:
        raise ValueError("final candidate runner report is incomplete or unsafe")
    _config_path, recorded_config = _verified_record_payload(
        report.get("frozen_configuration"),
        root=candidate_dir,
        purpose="candidate frozen configuration",
    )
    if recorded_config != config:
        raise ValueError("candidate runner configuration differs from frozen winner")
    _binding_path, binding = _verified_record_payload(
        report.get("capacity_binding"),
        root=candidate_dir,
        purpose="candidate capacity binding",
    )
    _run_path, run_configuration = _verified_record_payload(
        report.get("run_configuration"),
        root=candidate_dir,
        purpose="candidate run configuration",
    )
    if (
        binding.get("schema_version") != expanded_cap.CAPACITY_BINDING_VERSION
        or binding.get("state") != "verified_before_app_server_start"
        or binding.get("expected_canonical_sha256") != capacity_sha256
        or binding.get("observed_canonical_sha256") != capacity_sha256
        or binding.get("fixture") is not fixture_mode
        or run_configuration.get("schema_version")
        != expanded_cap.RUN_CONFIGURATION_VERSION
        or run_configuration.get("state") != "capacity_bound_configuration"
        or run_configuration.get("capacity_admission_canonical_sha256")
        != capacity_sha256
        or run_configuration.get("fixture_mode") is not fixture_mode
        or run_configuration.get("concurrency") != 1
    ):
        raise ValueError("candidate capacity or run-configuration binding drifted")
    mapping_path = candidate_dir / "private-mapping.json"
    _, mapping = _read_json(mapping_path, purpose="candidate private mapping")
    batches = mapping.get("batches")
    mapped_ids = [
        str(segment_id)
        for batch in batches or []
        if isinstance(batch, Mapping)
        for segment_id in batch.get("segment_ids") or []
    ]
    if (
        mapping.get("schema_version") != expanded_cap.PRIVATE_MAPPING_VERSION
        or mapping.get("winner_system_id") != expanded_cap.WINNER_SYSTEM_ID
        or not isinstance(batches, list)
        or len(batches) != expected_calls
        or mapped_ids != list(segment_ids)
    ):
        raise ValueError("candidate private mapping does not partition frozen selection")
    return report


async def run_holdout_candidate_phase(
    conn: sqlite3.Connection,
    *,
    covenant_path: Union[str, Path],
    frozen_winner_path: Union[str, Path],
    stratified_selection_path: Optional[Union[str, Path]],
    execution_dir: Union[str, Path],
    acceptance_mode: bool = True,
    event_cap: Optional[int] = None,
    capacity_admission: Optional[Union[Mapping[str, Any], Path]] = None,
    capacity_admission_sha256: Optional[str] = None,
    fixture_mode: bool = False,
    timeout_seconds: float = 1200.0,
    core_runner: Callable[..., Awaitable[dict[str, Any]]] = expanded_cap.run_episode_batch_arm,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
) -> dict[str, Any]:
    inputs = _load_execution_inputs(
        conn,
        covenant_path=covenant_path,
        frozen_winner_path=frozen_winner_path,
        stratified_selection_path=stratified_selection_path,
        acceptance_mode=acceptance_mode,
    )
    root = Path(execution_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    execution_plan = _ensure_execution_plan(root, inputs=inputs, event_cap=event_cap)
    instruction_contract = execution_plan["instruction_contract"]
    execution_lineage = execution_plan["execution_lineage"]
    effective_client_factory = _resolve_holdout_client_factory(
        client_factory, instruction_contract
    )
    if acceptance_mode and fixture_mode:
        raise ValueError("acceptance holdout execution prohibits fixture capacity admission")
    context_report_path = root / "phase-a-contexts" / "report.json"
    _, context_report = _read_json(context_report_path, purpose="context phase report")
    _validate_terminal_phase_lineage(
        context_report,
        phase_root=root / "phase-a-contexts",
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    if context_report.get("ok") is not True or not context_report.get("manifest_path"):
        raise ValueError("candidate phase requires a successful frozen context phase")
    manifest_path = Path(context_report["manifest_path"]).resolve()
    if _sha256_file(manifest_path) != context_report.get("manifest_sha256"):
        raise ValueError("holdout manifest drift before candidate phase")

    episodes, requests, segment_ids = _prepare_holdout_candidate_episodes(
        conn,
        inputs=inputs,
        manifest_path=manifest_path,
    )

    phase_root = root / "phase-b-candidate"
    phase_root.mkdir(parents=True, exist_ok=True)
    report_path = phase_root / "report.json"
    if report_path.is_file():
        _, report = _read_json(report_path, purpose="candidate phase report")
        _validate_terminal_phase_lineage(
            report,
            phase_root=phase_root,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        return report
    winner = inputs["winner"]["payload"]["winner"]
    config = inputs["winner_config"]
    intent_path = phase_root / "attempt.json"
    candidate_dir = phase_root / "candidate-arm"
    before_changes = conn.total_changes
    if intent_path.exists():
        _, intent = _read_json(intent_path, purpose="candidate immutable attempt")
        recorded_capacity_sha256 = intent.get("capacity_admission_sha256")
        if (
            intent.get("phase") != "B_frozen_winner_candidate"
            or intent.get("covenant_sha256") != inputs["covenant_sha256"]
            or intent.get("stratified_selection_sha256") != inputs["selection_sha256"]
            or intent.get("manifest_sha256") != _sha256_file(manifest_path)
            or intent.get("winner_artifact_sha256")
            != inputs["winner"]["artifact_sha256"]
            or intent.get("frozen_configuration_sha256")
            != inputs["winner_record"]["frozen_configuration_sha256"]
            or intent.get("event_cap") != execution_plan["event_cap"]
            or intent.get("segment_count") != len(segment_ids)
            or intent.get("batch_count") != len(requests)
            or intent.get("fixture_mode") is not fixture_mode
            or not isinstance(recorded_capacity_sha256, str)
            or len(recorded_capacity_sha256) != 64
            or any(character not in "0123456789abcdef" for character in recorded_capacity_sha256)
            or intent.get("retry_ordinal") != 0
        ):
            raise ValueError("candidate immutable attempt binding drifted")
        runner_report_path = candidate_dir / "report.json"
        if runner_report_path.is_file():
            _, runner_report = _read_json(runner_report_path, purpose="candidate arm report")
            try:
                runner_report = _validate_candidate_runner_report(
                    runner_report,
                    candidate_dir=candidate_dir,
                    inputs=inputs,
                    requests=requests,
                    segment_ids=segment_ids,
                    fixture_mode=fixture_mode,
                    capacity_sha256=recorded_capacity_sha256,
                )
            except Exception as exc:
                runner_report = None
                failure_class = type(exc).__name__
                attempt_status = "failed_validation_no_retry"
            else:
                failure_class = None
                attempt_status = "completed"
        else:
            runner_report = None
            failure_class = "prior_attempt_has_no_terminal_report"
            attempt_status = "ambiguous_interrupted_no_retry"
    else:
        if capacity_admission is None or capacity_admission_sha256 is None:
            raise ValueError("candidate phase requires a fresh exact capacity admission receipt")
        validated_capacity = expanded_cap.validate_capacity_admission(
            capacity_admission,
            expected_sha256=capacity_admission_sha256,
            batch_size=int(config["batch_size"]),
            thread_mode=str(config["thread_mode"]),
            concurrency=1,
            episode_ids=[str(episode["episode_id"]) for episode in episodes],
            segment_count=len(segment_ids),
            batch_count=len(requests),
            fixture_mode=fixture_mode,
        )
        intent = {
            "phase": "B_frozen_winner_candidate",
            "covenant_sha256": inputs["covenant_sha256"],
            "stratified_selection_sha256": inputs["selection_sha256"],
            "manifest_sha256": _sha256_file(manifest_path),
            "winner_artifact_sha256": inputs["winner"]["artifact_sha256"],
            "frozen_configuration_sha256": inputs["winner_record"][
                "frozen_configuration_sha256"
            ],
            "capacity_admission_sha256": validated_capacity["canonical_sha256"],
            "event_cap": execution_plan["event_cap"],
            "segment_count": len(segment_ids),
            "batch_count": len(requests),
            "fixture_mode": fixture_mode,
            "retry_ordinal": 0,
        }
        _write_immutable(intent_path, _canonical_bytes(intent))
        try:
            runner_report = await core_runner(
                episodes,
                output_dir=candidate_dir,
                batch_size=int(config["batch_size"]),
                thread_mode=str(config["thread_mode"]),
                capacity_admission=validated_capacity["receipt"],
                capacity_admission_sha256=validated_capacity["canonical_sha256"],
                frozen_configuration=config,
                concurrency=1,
                fixture_mode=fixture_mode,
                timeout_seconds=timeout_seconds,
                client_factory=effective_client_factory,
            )
            runner_report_path = candidate_dir / "report.json"
            _, disk_report = _read_json(runner_report_path, purpose="candidate arm report")
            if dict(runner_report) != disk_report:
                raise ValueError("candidate runner return differs from immutable report")
            runner_report = _validate_candidate_runner_report(
                disk_report,
                candidate_dir=candidate_dir,
                inputs=inputs,
                requests=requests,
                segment_ids=segment_ids,
                fixture_mode=fixture_mode,
                capacity_sha256=validated_capacity["canonical_sha256"],
            )
            failure_class = None
            attempt_status = "completed"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            runner_report = None
            failure_class = type(exc).__name__
            attempt_status = "failed_no_retry"
    accounting_complete = bool(
        isinstance(runner_report, dict)
        and runner_report.get("accounting_complete") is True
        and _valid_usage(runner_report.get("usage"))
    )
    if conn.total_changes != before_changes:
        raise RuntimeError("candidate phase mutated the production database")
    leaf_bindings = []
    leaf_outcomes = []
    binding_by_sidecar: dict[str, dict[str, Any]] = {}
    if isinstance(runner_report, dict) and attempt_status == "completed":
        for request in requests:
            paths = expanded_cap._batch_paths(  # noqa: SLF001 - frozen adapter layout
                candidate_dir, str(request["batch_id"])
            )
            binding = build_holdout_leaf_binding(
                sidecar_path=paths["sidecar"],
                prompt_path=paths["prompt"],
                output_schema_path=paths["schema"],
                base_instructions_path=paths["base"],
                raw_output_path=paths["output"],
                model=expanded_cap.MODEL,
                effort=expanded_cap.EFFORT,
                thread_mode=str(request["thread_mode"]),
                batch_size=int(request["effective_batch_size"]),
                prompt=str(request["prompt"]),
                output_schema=request["schema"],
                base_instructions=str(request["base_instructions"]),
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
            leaf_bindings.append(binding)
            binding_by_sidecar[str(paths["sidecar"].resolve())] = binding
    for request in requests:
        paths = expanded_cap._batch_paths(  # noqa: SLF001 - frozen adapter layout
            candidate_dir, str(request["batch_id"])
        )
        sidecar_path = paths["sidecar"].resolve()
        leaf_outcomes.append(
            {
                "batch_id": str(request["batch_id"]),
                "episode_id": str(request["episode_id"]),
                "status": (
                    "validated"
                    if str(sidecar_path) in binding_by_sidecar
                    else "failed_no_retry"
                    if attempt_status == "failed_no_retry"
                    else "not_completed"
                ),
                "sidecar_path": str(sidecar_path) if sidecar_path.is_file() else None,
                "leaf_identity_sha256": (
                    binding_by_sidecar[str(sidecar_path)]["leaf_identity_sha256"]
                    if str(sidecar_path) in binding_by_sidecar
                    else None
                ),
            }
        )
    report = {
        "schema_version": HOLDOUT_CANDIDATE_PHASE_VERSION,
        "phase": "B_frozen_winner_candidate",
        "evaluation_role": "untouched_private_holdout_never_production",
        "ok": attempt_status == "completed" and accounting_complete,
        "execution_complete": True,
        "attempt_status": attempt_status,
        "failure_class": failure_class,
        "retry_count": 0,
        "automatic_retry_prohibited": True,
        "holdout_authorized": True,
        "winner": winner,
        "frozen_configuration_sha256": inputs["winner_record"][
            "frozen_configuration_sha256"
        ],
        "capacity_admission_sha256": (
            intent.get("capacity_admission_sha256")
            if isinstance(intent, Mapping)
            else None
        ),
        "fixture_mode": fixture_mode,
        "requested_segments": len(segment_ids),
        "requested_calls": len(requests),
        "manifest_path": str(manifest_path),
        "manifest_sha256": _sha256_file(manifest_path),
        "candidate_arm_report_path": (
            str(candidate_dir / "report.json")
            if (candidate_dir / "report.json").is_file()
            else None
        ),
        "candidate_arm_report_sha256": (
            _sha256_file(candidate_dir / "report.json")
            if (candidate_dir / "report.json").is_file()
            else None
        ),
        "candidate_mapping_path": (
            str(candidate_dir / "private-mapping.json")
            if (candidate_dir / "private-mapping.json").is_file()
            else None
        ),
        "candidate_mapping_sha256": (
            _sha256_file(candidate_dir / "private-mapping.json")
            if (candidate_dir / "private-mapping.json").is_file()
            else None
        ),
        "accounting_complete": accounting_complete,
        "usage": runner_report.get("usage") if isinstance(runner_report, dict) else None,
        "runner_report": runner_report,
        "outcomes": leaf_outcomes,
        "leaf_bindings": leaf_bindings,
        "instruction_contract": instruction_contract,
        "execution_lineage": execution_lineage,
        "production_database_mutation": False,
        "production_promotion": False,
        "production_changed": False,
    }
    _validate_terminal_phase_lineage(
        report,
        phase_root=phase_root,
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    _write_immutable(report_path, _canonical_bytes(report))
    return report


def _segment_baseline_input(
    conn: sqlite3.Connection,
    *,
    frozen: dict[str, Any],
    context_artifact: dict[str, Any],
    context_id: str,
) -> tuple[str, dict[str, Any]]:
    row = conn.execute(
        """
        SELECT sg.*, e.title AS episode_title, e.published_at AS episode_published_at,
               s.name AS source_name, tp.id AS transcript_preparation_id,
               tp.artifact_type AS transcript_artifact_type,
               tp.status AS transcript_preparation_status,
               tp.substantive_word_count AS transcript_substantive_word_count,
               tp.boilerplate_ratio AS transcript_boilerplate_ratio,
               tp.speaker_turn_count AS transcript_speaker_turn_count,
               tp.quality_score AS transcript_quality_score
        FROM segments sg
        JOIN episodes e ON e.id = sg.episode_id
        JOIN sources s ON s.id = sg.source_id
        JOIN transcript_preparations tp ON tp.transcript_id = sg.transcript_id
        WHERE sg.id = ?
        """,
        (frozen["segment_id"],),
    ).fetchone()
    if not row:
        raise ValueError(f"frozen baseline segment missing: {frozen['segment_id']}")
    if str(row["episode_id"]) != str(frozen["episode_id"]):
        raise ValueError(f"frozen baseline episode drift: {frozen['segment_id']}")
    if str(row["transcript_id"]) != str(frozen["transcript_id"]):
        raise ValueError(f"frozen baseline transcript drift: {frozen['segment_id']}")
    _, segment_text = _verified_text_file(
        row["text_path"], str(frozen["text_sha256"]), purpose=f"segment {row['id']}"
    )
    neighbors = conn.execute(
        """
        SELECT id, segment_index, start_char, end_char, text_path, text_sha256
        FROM segments
        WHERE transcript_id = ? AND segment_index BETWEEN ? AND ?
        ORDER BY segment_index, id
        """,
        (row["transcript_id"], int(row["segment_index"]) - 1, int(row["segment_index"]) + 1),
    ).fetchall()
    adjacent = []
    for neighbor in neighbors:
        _, neighbor_text = _verified_text_file(
            neighbor["text_path"],
            str(neighbor["text_sha256"]),
            purpose=f"adjacent segment {neighbor['id']}",
        )
        role = "current"
        if int(neighbor["segment_index"]) < int(row["segment_index"]):
            role = "previous"
            neighbor_text = neighbor_text[-3500:]
        elif int(neighbor["segment_index"]) > int(row["segment_index"]):
            role = "next"
            neighbor_text = neighbor_text[:3500]
        elif len(neighbor_text) > 3500:
            neighbor_text = neighbor_text[:1750] + "\n[...current segment middle omitted for context budget...]\n" + neighbor_text[-1750:]
        adjacent.append(
            {
                "role": role,
                "segment_id": neighbor["id"],
                "segment_index": neighbor["segment_index"],
                "char_range": [neighbor["start_char"], neighbor["end_char"]],
                "text_excerpt": neighbor_text,
            }
        )
    context = {
        "segment_id": row["id"],
        "episode_id": row["episode_id"],
        "source_id": row["source_id"],
        "source_name": row["source_name"],
        "episode_title": row["episode_title"],
        "episode_published_at": row["episode_published_at"],
        "segment_index": row["segment_index"],
        "start_char": row["start_char"],
        "end_char": row["end_char"],
        "transcript_preparation_id": row["transcript_preparation_id"],
        "transcript_artifact_type": row["transcript_artifact_type"],
        "transcript_preparation_status": row["transcript_preparation_status"],
        "transcript_substantive_word_count": row["transcript_substantive_word_count"],
        "transcript_boilerplate_ratio": row["transcript_boilerplate_ratio"],
        "transcript_speaker_turn_count": row["transcript_speaker_turn_count"],
        "transcript_quality_score": row["transcript_quality_score"],
        "privacy_boundary": "private_analysis_only_do_not_output_full_transcript",
        "episode_context_artifact": context_artifact,
        "episode_context_run_id": context_id,
        "adjacent_segment_context": {
            "purpose": "Resolve speaker continuity and overlap duplicates. Do not use adjacent excerpts as evidence for current-segment events.",
            "current_segment_id": row["id"],
            "segments": adjacent,
        },
        "episode_context_contract": (
            "This compact artifact came from a frozen GPT-5.5 full-episode read. Use it for "
            "speaker/entity/concept context and adjacent context for continuity, but emit evidence "
            "only from the current Segment Text section."
        ),
    }
    return segment_text, context


def _baseline_base_instructions() -> str:
    return (
        "You are the frozen full ai_discourse_v3_1 GPT-5.5 high-reasoning baseline extractor. "
        "The supplied prompt contains the complete codebook, schema, context, and segment. "
        "Do not use tools, network access, or local files. Return only the structured JSON."
    )


async def run_holdout_baseline_phase(
    conn: sqlite3.Connection,
    *,
    covenant_path: Union[str, Path],
    frozen_winner_path: Union[str, Path],
    stratified_selection_path: Optional[Union[str, Path]],
    execution_dir: Union[str, Path],
    acceptance_mode: bool = True,
    event_cap: Optional[int] = None,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
) -> dict[str, Any]:
    inputs = _load_execution_inputs(
        conn,
        covenant_path=covenant_path,
        frozen_winner_path=frozen_winner_path,
        stratified_selection_path=stratified_selection_path,
        acceptance_mode=acceptance_mode,
    )
    root = Path(execution_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    execution_plan = _ensure_execution_plan(root, inputs=inputs, event_cap=event_cap)
    instruction_contract = execution_plan["instruction_contract"]
    execution_lineage = execution_plan["execution_lineage"]
    effective_client_factory = _resolve_holdout_client_factory(
        client_factory, instruction_contract
    )
    _, context_report = _read_json(
        root / "phase-a-contexts" / "report.json", purpose="context phase report"
    )
    _validate_terminal_phase_lineage(
        context_report,
        phase_root=root / "phase-a-contexts",
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    if context_report.get("ok") is not True:
        raise ValueError("baseline phase requires a successful frozen context phase")
    _, candidate_report = _read_json(
        root / "phase-b-candidate" / "report.json", purpose="candidate phase report"
    )
    _validate_terminal_phase_lineage(
        candidate_report,
        phase_root=root / "phase-b-candidate",
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    if candidate_report.get("execution_complete") is not True:
        raise ValueError("baseline phase requires a terminal candidate intent-to-treat phase")

    phase_root = root / "phase-c-baseline"
    phase_root.mkdir(parents=True, exist_ok=True)
    report_path = phase_root / "report.json"
    if report_path.is_file():
        _, report = _read_json(report_path, purpose="baseline phase report")
        _validate_terminal_phase_lineage(
            report,
            phase_root=phase_root,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        return report
    plan = {
        "schema_version": HOLDOUT_BASELINE_PHASE_VERSION,
        "phase": "C_full_v31_baseline",
        "label_pack": LABEL_PACK,
        "model": BASELINE_MODEL,
        "reasoning_effort": BASELINE_EFFORT,
        "segment_ids": [row["segment_id"] for row in inputs["segments"]],
        "persistent_client_count": 1,
        "retry_count": 0,
        "raw_and_repaired_outputs_separate": True,
        "instruction_contract": instruction_contract,
        "execution_lineage": execution_lineage,
    }
    _ensure_immutable(phase_root / "plan.json", _canonical_bytes(plan))
    context_outcomes = {
        str(row["episode_id"]): row for row in context_report.get("outcomes") or []
    }
    context_by_episode = {}
    for episode_id, outcome in context_outcomes.items():
        path = Path(outcome["artifact_path"]).resolve()
        if _sha256_file(path) != outcome["artifact_sha256"]:
            raise ValueError(f"frozen episode context drift: {episode_id}")
        context_by_episode[episode_id] = json.loads(path.read_text(encoding="utf-8"))

    pack = load_label_pack(LABEL_PACK)
    prepared = {}
    for frozen in inputs["segments"]:
        segment_id = str(frozen["segment_id"])
        segment_dir = phase_root / "segments" / segment_id
        segment_text, variable_context = _segment_baseline_input(
            conn,
            frozen=frozen,
            context_artifact=context_by_episode[str(frozen["episode_id"])],
            context_id=str(context_outcomes[str(frozen["episode_id"])]["context_id"]),
        )
        prompt = render_prompt(LABEL_PACK, {"text": segment_text}, variable_context)
        schema = copy.deepcopy(pack.schema)
        schema["properties"]["segment_id"] = {"type": "string", "const": segment_id}
        schema["properties"]["episode_id"] = {
            "type": "string",
            "const": str(frozen["episode_id"]),
        }
        prompt_bytes = prompt.encode("utf-8")
        schema_bytes = _canonical_bytes(schema)
        _ensure_immutable(segment_dir / "prompt.md", prompt_bytes)
        _ensure_immutable(segment_dir / "schema.json", schema_bytes)
        prepared[segment_id] = {
            "frozen": frozen,
            "segment_text": segment_text,
            "prompt": prompt,
            "schema": schema,
            "dir": segment_dir,
            "prompt_sha256": _sha256_bytes(prompt_bytes),
            "schema_sha256": _sha256_bytes(schema_bytes),
        }

    checkpoint_path = phase_root / "checkpoint.json"
    if checkpoint_path.is_file():
        _, checkpoint = _read_json(checkpoint_path, purpose="baseline checkpoint")
    else:
        checkpoint = {"schema_version": HOLDOUT_BASELINE_PHASE_VERSION, "outcomes": {}}
    outcomes_by_id = checkpoint.get("outcomes")
    if not isinstance(outcomes_by_id, dict):
        raise ValueError("baseline checkpoint outcomes are malformed")
    before_changes = conn.total_changes
    base_instructions = _baseline_base_instructions()
    base_instructions_path = phase_root / "base-instructions.md"
    _ensure_immutable(base_instructions_path, base_instructions.encode("utf-8"))
    async with LazyClientSession(effective_client_factory) as client:
        for frozen in inputs["segments"]:
            segment_id = str(frozen["segment_id"])
            item = prepared[segment_id]
            segment_dir = item["dir"]
            attempt_path = segment_dir / "attempt.json"
            outcome_path = segment_dir / "outcome.json"
            sidecar_path = segment_dir / "sidecar.json"
            raw_path = segment_dir / "raw-output.json"
            repaired_path = segment_dir / "repaired-output.json"
            if segment_id in outcomes_by_id:
                outcome = outcomes_by_id[segment_id]
                if outcome.get("status") == "validated":
                    _sidecar, usage = validate_completed_managed_sidecar(
                        sidecar_path=sidecar_path,
                        raw_output_path=raw_path,
                        model=BASELINE_MODEL,
                        effort=BASELINE_EFFORT,
                        thread_mode="new_thread",
                        batch_size=1,
                        prompt=item["prompt"],
                        output_schema=item["schema"],
                        base_instructions=base_instructions,
                        instruction_contract=instruction_contract,
                        execution_lineage=execution_lineage,
                    )
                    if usage != outcome.get("usage"):
                        raise ValueError("baseline checkpoint usage drift")
                    if (
                        _sha256_file(sidecar_path) != outcome.get("sidecar_sha256")
                        or _sha256_file(raw_path) != outcome.get("raw_output_sha256")
                    ):
                        raise ValueError("baseline checkpoint artifact hash drift")
                continue
            if attempt_path.exists():
                if outcome_path.is_file():
                    _, outcome = _read_json(outcome_path, purpose="baseline segment outcome")
                else:
                    outcome = {
                        "segment_id": segment_id,
                        "episode_id": frozen["episode_id"],
                        "status": "ambiguous_interrupted_no_retry",
                        "status_ok": False,
                        "failure_class": "prior_attempt_has_no_terminal_outcome",
                        "usage": None,
                        "usage_complete": False,
                    }
                    _write_immutable(outcome_path, _canonical_bytes(outcome))
                if outcome.get("status") == "validated":
                    _sidecar, usage = validate_completed_managed_sidecar(
                        sidecar_path=sidecar_path,
                        raw_output_path=raw_path,
                        model=BASELINE_MODEL,
                        effort=BASELINE_EFFORT,
                        thread_mode="new_thread",
                        batch_size=1,
                        prompt=item["prompt"],
                        output_schema=item["schema"],
                        base_instructions=base_instructions,
                        instruction_contract=instruction_contract,
                        execution_lineage=execution_lineage,
                    )
                    if usage != outcome.get("usage"):
                        raise ValueError("baseline outcome usage differs from managed sidecar")
                    if (
                        _sha256_file(sidecar_path) != outcome.get("sidecar_sha256")
                        or _sha256_file(raw_path) != outcome.get("raw_output_sha256")
                    ):
                        raise ValueError("baseline checkpoint artifact hash drift")
                outcomes_by_id[segment_id] = outcome
                _checkpoint(checkpoint_path, checkpoint)
                continue
            if not attempt_path.exists():
                unexpected = [
                    path
                    for path in (outcome_path, sidecar_path, raw_path, repaired_path)
                    if path.exists()
                ]
                if unexpected:
                    raise ValueError(
                        "baseline artifacts exist without an immutable attempt: "
                        + ", ".join(str(path) for path in unexpected)
                    )
                await _wait_for_capacity_before_attempt(client)
                attempt = {
                    "segment_id": segment_id,
                    "episode_id": frozen["episode_id"],
                    "model": BASELINE_MODEL,
                    "reasoning_effort": BASELINE_EFFORT,
                    "prompt_sha256": item["prompt_sha256"],
                    "schema_sha256": item["schema_sha256"],
                    "retry_ordinal": 0,
                }
                _write_immutable(attempt_path, _canonical_bytes(attempt))
            try:
                result = await client.run_ephemeral_structured_turn(
                    model=BASELINE_MODEL,
                    effort=BASELINE_EFFORT,
                    base_instructions=base_instructions,
                    prompt=item["prompt"],
                    output_schema=item["schema"],
                    cwd=Path.cwd(),
                    sidecar_path=sidecar_path,
                    output_path=raw_path,
                    batch_size=1,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                )
                if not result.status_ok or not isinstance(result.output, dict):
                    raise ValueError(result.error_class or f"turn_{result.status}")
                if not raw_path.exists():
                    _write_immutable(raw_path, _canonical_bytes(result.output))
                raw_output = copy.deepcopy(result.output)
                raw_validation_error = None
                try:
                    validate_label_output(
                        LABEL_PACK, raw_output, segment_text=item["segment_text"]
                    )
                except Exception as exc:
                    raw_validation_error = type(exc).__name__
                repaired = copy.deepcopy(result.output)
                repair_count = repair_label_output_for_submission(
                    LABEL_PACK, repaired, segment_text=item["segment_text"]
                )
                _write_immutable(repaired_path, _canonical_bytes(repaired))
                validate_label_output(
                    LABEL_PACK, repaired, segment_text=item["segment_text"]
                )
                if repaired.get("segment_id") != segment_id:
                    raise ValueError("baseline segment_id does not match frozen segment")
                if repaired.get("episode_id") != frozen["episode_id"]:
                    raise ValueError("baseline episode_id does not match frozen episode")
                _sidecar, usage = validate_completed_managed_sidecar(
                    sidecar_path=sidecar_path,
                    raw_output_path=raw_path,
                    model=BASELINE_MODEL,
                    effort=BASELINE_EFFORT,
                    thread_mode="new_thread",
                    batch_size=1,
                    prompt=item["prompt"],
                    output_schema=item["schema"],
                    base_instructions=base_instructions,
                    instruction_contract=instruction_contract,
                    execution_lineage=execution_lineage,
                )
                usage_complete = True
                usage_error = None
                outcome = {
                    "segment_id": segment_id,
                    "episode_id": frozen["episode_id"],
                    "status": "validated" if usage_complete else "validated_usage_unknown",
                    "status_ok": usage_complete,
                    "failure_class": usage_error,
                    "raw_validation_error": raw_validation_error,
                    "repair_count": repair_count,
                    "raw_output_path": str(raw_path),
                    "raw_output_sha256": _sha256_file(raw_path),
                    "repaired_output_path": str(repaired_path),
                    "repaired_output_sha256": _sha256_file(repaired_path),
                    "sidecar_path": str(sidecar_path),
                    "sidecar_sha256": _sha256_file(sidecar_path),
                    "usage": usage,
                    "usage_complete": usage_complete,
                }
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                usage, usage_complete, usage_error = _sidecar_usage(sidecar_path)
                outcome = {
                    "segment_id": segment_id,
                    "episode_id": frozen["episode_id"],
                    "status": "failed_no_retry",
                    "status_ok": False,
                    "failure_class": type(exc).__name__,
                    "usage_error": usage_error,
                    "usage": usage,
                    "usage_complete": usage_complete,
                    "raw_output_path": str(raw_path) if raw_path.exists() else None,
                    "raw_output_sha256": _sha256_file(raw_path) if raw_path.exists() else None,
                    "repaired_output_path": str(repaired_path) if repaired_path.exists() else None,
                    "repaired_output_sha256": (
                        _sha256_file(repaired_path) if repaired_path.exists() else None
                    ),
                    "sidecar_path": str(sidecar_path) if sidecar_path.exists() else None,
                    "sidecar_sha256": _sha256_file(sidecar_path) if sidecar_path.exists() else None,
                }
            _write_immutable(outcome_path, _canonical_bytes(outcome))
            outcomes_by_id[segment_id] = outcome
            _checkpoint(checkpoint_path, checkpoint)

    outcomes = [outcomes_by_id[str(row["segment_id"])] for row in inputs["segments"]]
    accounting_complete = all(outcome.get("usage_complete") is True for outcome in outcomes)
    all_valid = all(outcome.get("status") == "validated" for outcome in outcomes)
    if conn.total_changes != before_changes:
        raise RuntimeError("baseline phase mutated the production database")
    leaf_bindings = [
        build_holdout_leaf_binding(
            sidecar_path=prepared[str(outcome["segment_id"])]["dir"] / "sidecar.json",
            prompt_path=prepared[str(outcome["segment_id"])]["dir"] / "prompt.md",
            output_schema_path=prepared[str(outcome["segment_id"])]["dir"] / "schema.json",
            base_instructions_path=base_instructions_path,
            raw_output_path=prepared[str(outcome["segment_id"])]["dir"] / "raw-output.json",
            model=BASELINE_MODEL,
            effort=BASELINE_EFFORT,
            thread_mode="new_thread",
            batch_size=1,
            prompt=prepared[str(outcome["segment_id"])]["prompt"],
            output_schema=prepared[str(outcome["segment_id"])]["schema"],
            base_instructions=base_instructions,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        for outcome in outcomes
        if outcome.get("status") == "validated"
    ]
    report = {
        "schema_version": HOLDOUT_BASELINE_PHASE_VERSION,
        "phase": "C_full_v31_baseline",
        "ok": all_valid and accounting_complete,
        "execution_complete": len(outcomes) == len(inputs["segments"]),
        "requested_segments": len(inputs["segments"]),
        "validated_segments": sum(outcome.get("status") == "validated" for outcome in outcomes),
        "failed_segments_intent_to_treat": sum(
            outcome.get("status") != "validated" for outcome in outcomes
        ),
        "retry_count": 0,
        "persistent_client_count": 1,
        "accounting_complete": accounting_complete,
        "usage": _aggregate_usage(outcomes) if accounting_complete else None,
        "measured_partial_usage": _aggregate_usage(outcomes),
        "outcomes": outcomes,
        "leaf_bindings": leaf_bindings,
        "instruction_contract": instruction_contract,
        "execution_lineage": execution_lineage,
        "production_database_mutation": False,
        "production_promotion": False,
    }
    _validate_terminal_phase_lineage(
        report,
        phase_root=phase_root,
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    _write_immutable(report_path, _canonical_bytes(report))
    return report


async def run_frozen_holdout_execution(
    conn: sqlite3.Connection,
    *,
    covenant_path: Union[str, Path],
    frozen_winner_path: Union[str, Path],
    stratified_selection_path: Optional[Union[str, Path]],
    execution_dir: Union[str, Path],
    acceptance_mode: bool = True,
    event_cap: Optional[int] = None,
    capacity_admission: Optional[Union[Mapping[str, Any], Path]] = None,
    capacity_admission_sha256: Optional[str] = None,
    fixture_mode: bool = False,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
    core_runner: Callable[..., Awaitable[dict[str, Any]]] = expanded_cap.run_episode_batch_arm,
) -> dict[str, Any]:
    contexts = await run_holdout_context_phase(
        conn,
        covenant_path=covenant_path,
        frozen_winner_path=frozen_winner_path,
        stratified_selection_path=stratified_selection_path,
        execution_dir=execution_dir,
        acceptance_mode=acceptance_mode,
        event_cap=event_cap,
        timeout_seconds=timeout_seconds,
        client_factory=client_factory,
    )
    if not contexts.get("ok"):
        return {"ok": False, "blocked_phase": "A", "contexts": contexts}
    candidate = await run_holdout_candidate_phase(
        conn,
        covenant_path=covenant_path,
        frozen_winner_path=frozen_winner_path,
        stratified_selection_path=stratified_selection_path,
        execution_dir=execution_dir,
        acceptance_mode=acceptance_mode,
        event_cap=event_cap,
        capacity_admission=capacity_admission,
        capacity_admission_sha256=capacity_admission_sha256,
        fixture_mode=fixture_mode,
        timeout_seconds=timeout_seconds,
        core_runner=core_runner,
        client_factory=client_factory,
    )
    baseline = await run_holdout_baseline_phase(
        conn,
        covenant_path=covenant_path,
        frozen_winner_path=frozen_winner_path,
        stratified_selection_path=stratified_selection_path,
        execution_dir=execution_dir,
        acceptance_mode=acceptance_mode,
        event_cap=event_cap,
        timeout_seconds=timeout_seconds,
        client_factory=client_factory,
    )
    return {
        "ok": bool(contexts.get("ok") and candidate.get("ok") and baseline.get("ok")),
        "contexts": contexts,
        "candidate": candidate,
        "baseline": baseline,
        "production_promotion": False,
    }


def _read_only_connection(path: Union[str, Path]) -> sqlite3.Connection:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ValueError(f"database is missing: {resolved}")
    conn = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run checkpointed frozen holdout phases via managed Codex app-server only."
    )
    parser.add_argument("--database", default=str(db_path()))
    parser.add_argument("--covenant", required=True)
    parser.add_argument("--winner", required=True)
    parser.add_argument("--stratified-selection", required=True)
    parser.add_argument("--execution-dir", required=True)
    parser.add_argument("--phase", choices=("contexts", "candidate", "baseline", "all"), default="all")
    parser.add_argument("--event-cap", type=int)
    parser.add_argument(
        "--capacity-admission",
        help="Exact coordinator capacity-admission receipt required by candidate/all phases.",
    )
    parser.add_argument("--capacity-admission-sha256")
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    parser.add_argument(
        "--development-shape",
        action="store_true",
        help="Allow a non-120-case frozen selection for fixture/development execution only.",
    )
    return parser


async def _run_cli(args: argparse.Namespace, conn: sqlite3.Connection) -> dict[str, Any]:
    kwargs = {
        "covenant_path": args.covenant,
        "frozen_winner_path": args.winner,
        "stratified_selection_path": args.stratified_selection,
        "execution_dir": args.execution_dir,
        "acceptance_mode": not args.development_shape,
        "event_cap": args.event_cap,
        "timeout_seconds": args.timeout_seconds,
    }
    if args.phase == "contexts":
        return await run_holdout_context_phase(conn, **kwargs)
    if args.phase == "candidate":
        return await run_holdout_candidate_phase(
            conn,
            **kwargs,
            capacity_admission=(
                Path(args.capacity_admission) if args.capacity_admission else None
            ),
            capacity_admission_sha256=args.capacity_admission_sha256,
            fixture_mode=args.development_shape,
        )
    if args.phase == "baseline":
        return await run_holdout_baseline_phase(conn, **kwargs)
    return await run_frozen_holdout_execution(
        conn,
        **kwargs,
        capacity_admission=(
            Path(args.capacity_admission) if args.capacity_admission else None
        ),
        capacity_admission_sha256=args.capacity_admission_sha256,
        fixture_mode=args.development_shape,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = _read_only_connection(args.database)
        result = asyncio.run(_run_cli(args, conn))
    except Exception as exc:
        print(
            json.dumps({"ok": False, "error_class": type(exc).__name__}, sort_keys=True),
            file=sys.stderr,
        )
        return 2
    finally:
        if conn is not None:
            conn.close()
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0 if result.get("ok") else 3


if __name__ == "__main__":
    raise SystemExit(main())
