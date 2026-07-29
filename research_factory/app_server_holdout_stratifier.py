from __future__ import annotations

"""LLM-only pre-execution reference stratification for frozen holdout reservoirs."""

import argparse
import asyncio
import hashlib
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable, Optional, Sequence, Union

from .app_server_holdout import (
    HOLDOUT_COVENANT_VERSION,
    verify_frozen_holdout,
    verify_holdout_model_call_authorization,
)
from .app_server_holdout_execution import HOLDOUT_STRATIFIED_SELECTION_VERSION
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
from .labels import load_label_pack
from .paths import db_path, root as factory_root
from .util import sha256_text, stable_id, write_text_atomic


STRATIFIER_PLAN_VERSION = "pif_app_server_holdout_stratifier_plan_v1"
ATOMIC_REFERENCE_OUTPUT_VERSION = "pif_atomic_reference_events_v1"
ATOMIC_REFERENCE_BATCH_OUTPUT_VERSION = "pif_atomic_reference_event_batch_v1"
SUPPORT_OUTPUT_VERSION = "pif_atomic_reference_support_v1"
REFERENCE_CASE_VERSION = "pif_atomic_reference_case_v1"
REFERENCE_INDEX_VERSION = "pif_atomic_reference_index_v1"
STRATIFIER_REPORT_VERSION = "pif_app_server_holdout_stratifier_report_v1"

REFERENCE_MODEL = "gpt-5.5"
REFERENCE_EFFORT = "high"
SUPPORT_MODEL = "gpt-5.6-sol"
SUPPORT_EFFORT = "high"
LABEL_PACK = "ai_discourse_v3_1"
DEFAULT_EVENT_CAP = 32
DEFAULT_PACKET_SIZE = 8
DEFAULT_EXTRACTION_BATCH_SIZE = 8
DEFAULT_SUPPORT_MAX_WITNESSES = 96
DEFAULT_SUPPORT_MAX_PACKET_BYTES = 240000

EVENT_TYPES = (
    "term_usage",
    "frame_usage",
    "stance_position",
    "forecast",
    "causal_mechanism",
    "capability_claim",
    "product_signal",
    "market_signal",
    "risk_signal",
    "counterclaim",
    "uncertainty",
    "adoption_signal",
    "actor_mention",
    "entity_reference",
)
SUPPORT_VERDICTS = ("supported", "unsupported", "abstain")
COVERAGE_VERDICTS = (
    "complete_event_coverage",
    "clean_no_signal",
    "events_missing",
    "abstain",
)
_USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
_TERMINAL_STATES = {"completed", "failed", "interrupted", "cancelled"}


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


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


def _inside(path: Path, root: Path) -> bool:
    try:
        path.expanduser().resolve().relative_to(root.expanduser().resolve())
    except ValueError:
        return False
    return True


def _write_immutable(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        handle = path.open("xb")
    except FileExistsError as exc:
        raise ValueError(f"immutable stratifier artifact already exists: {path}") from exc
    with handle:
        handle.write(content)
        handle.flush()


def _ensure_immutable(path: Path, content: bytes) -> None:
    if path.exists():
        if not path.is_file() or path.read_bytes() != content:
            raise ValueError(f"immutable stratifier artifact drift: {path}")
        return
    _write_immutable(path, content)


def _checkpoint(path: Path, payload: dict[str, Any]) -> None:
    write_text_atomic(path, json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


async def _wait_for_capacity_before_attempt(client: Any) -> None:
    wait = getattr(client, "wait_for_semantic_capacity", None)
    if callable(wait):
        await wait()


def _resolve_artifact(path_value: str) -> Path:
    candidate = Path(path_value).expanduser()
    return candidate.resolve() if candidate.is_absolute() else (factory_root() / candidate).resolve()


def _verified_segment_text(conn: sqlite3.Connection, frozen: dict[str, Any]) -> str:
    row = conn.execute(
        "SELECT id, episode_id, transcript_id, text_path, text_sha256 FROM segments WHERE id = ?",
        (frozen["segment_id"],),
    ).fetchone()
    if not row:
        raise ValueError(f"frozen reservoir segment is missing: {frozen['segment_id']}")
    for field in ("episode_id", "transcript_id", "text_sha256"):
        if str(row[field]) != str(frozen[field]):
            raise ValueError(f"frozen reservoir {field} drift: {frozen['segment_id']}")
    path = _resolve_artifact(str(row["text_path"]))
    if not path.is_file():
        raise ValueError(f"frozen reservoir segment text is missing: {frozen['segment_id']}")
    data = path.read_bytes()
    observed = hashlib.sha256(data).hexdigest()
    if observed != frozen["text_sha256"]:
        raise ValueError(f"frozen reservoir segment DB/file hash drift: {frozen['segment_id']}")
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"frozen reservoir segment is not UTF-8: {frozen['segment_id']}") from exc


def _load_reservoirs(
    conn: sqlite3.Connection,
    *,
    covenant_path: Union[str, Path],
) -> dict[str, Any]:
    covenant_file, covenant = verify_holdout_model_call_authorization(covenant_path)
    verified = verify_frozen_holdout(conn, covenant_path=covenant_file)
    records = []
    for artifact_name, reservoir in (
        ("paired_quality_reservoir", "paired_quality_reservoir"),
        (
            "terminal_position_no_signal_candidates",
            "terminal_position_no_signal_candidates",
        ),
    ):
        artifact = (covenant.get("artifacts") or {}).get(artifact_name)
        if not isinstance(artifact, dict) or not artifact.get("filename"):
            raise ValueError(f"holdout covenant is missing {artifact_name}")
        _, payload = _read_json(
            covenant_file.parent / str(artifact["filename"]), purpose=artifact_name
        )
        for row in payload.get("segments") or []:
            if not isinstance(row, dict):
                raise ValueError(f"{artifact_name} contains a malformed segment")
            records.append({**row, "reservoir": reservoir})
    segment_ids = [str(row.get("segment_id") or "") for row in records]
    if not segment_ids or len(segment_ids) != len(set(segment_ids)):
        raise ValueError("frozen holdout reservoirs are empty or overlap")
    for row in records:
        row["segment_text"] = _verified_segment_text(conn, row)
    return {
        "covenant": covenant,
        "covenant_path": covenant_file,
        "covenant_sha256": verified["covenant_sha256"],
        "segments": records,
    }


def atomic_reference_output_schema(
    *, segment_id: str, episode_id: str, event_cap: int
) -> dict[str, Any]:
    event = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "event_type",
            "atomic_claim",
            "speaker",
            "actor",
            "target",
            "stance",
            "certainty",
            "temporal_horizon",
            "evidence",
            "evidence_start",
            "evidence_end",
        ],
        "properties": {
            "event_type": {"type": "string", "enum": list(EVENT_TYPES)},
            "atomic_claim": {"type": "string", "minLength": 1, "maxLength": 1200},
            "speaker": {"type": ["string", "null"]},
            "actor": {"type": ["string", "null"]},
            "target": {"type": ["string", "null"]},
            "stance": {
                "type": "string",
                "enum": ["supportive", "skeptical", "warning", "neutral", "uncertain", "mixed"],
            },
            "certainty": {
                "type": "string",
                "enum": ["high", "medium", "low", "uncertain"],
            },
            "temporal_horizon": {
                "type": "string",
                "enum": ["past", "present", "near_future", "long_future", "timeless", "unclear"],
            },
            "evidence": {"type": "string", "minLength": 1},
            "evidence_start": {"type": "integer", "minimum": 0},
            "evidence_end": {"type": "integer", "minimum": 1},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "schema_version",
            "segment_id",
            "episode_id",
            "extraction_status",
            "coverage_truncated",
            "events",
            "no_signal_reason",
        ],
        "properties": {
            "schema_version": {"type": "string", "const": ATOMIC_REFERENCE_OUTPUT_VERSION},
            "segment_id": {"type": "string", "const": segment_id},
            "episode_id": {"type": "string", "const": episode_id},
            "extraction_status": {"type": "string", "enum": ["coded", "no_signal", "abstain"]},
            "coverage_truncated": {"type": "boolean"},
            "events": {"type": "array", "maxItems": event_cap, "items": event},
            "no_signal_reason": {"type": ["string", "null"]},
        },
    }


def atomic_reference_batch_output_schema(
    rows: Sequence[dict[str, Any]], *, event_cap: int
) -> dict[str, Any]:
    segment_schema = atomic_reference_output_schema(
        segment_id=str(rows[0]["segment_id"]),
        episode_id=str(rows[0]["episode_id"]),
        event_cap=event_cap,
    )
    segment_schema["properties"]["segment_id"] = {
        "type": "string",
        "enum": [str(row["segment_id"]) for row in rows],
    }
    segment_schema["properties"]["episode_id"] = {
        "type": "string",
        "enum": sorted({str(row["episode_id"]) for row in rows}),
    }
    # The deterministic validator below binds each segment ID back to its exact
    # frozen episode and enforces a complete, duplicate-free partition.
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "segments"],
        "properties": {
            "schema_version": {
                "type": "string",
                "const": ATOMIC_REFERENCE_BATCH_OUTPUT_VERSION,
            },
            "segments": {
                "type": "array",
                "minItems": len(rows),
                "maxItems": len(rows),
                "items": segment_schema,
            },
        },
    }
def _reference_base_instructions(event_cap: int) -> str:
    pack = load_label_pack(LABEL_PACK)
    return "\n\n".join(
        (
            "You are the frozen LLM-only atomic reference extractor for a private holdout.",
            "Read meaning directly. Do not use keyword, regex, token-overlap, embedding, phrase-match, or deterministic semantic gates.",
            "Extract each distinct grounded discourse event as one atomic event. Split genuinely distinct events even when they share evidence; do not split one event into field fragments. Preserve negation, attribution, uncertainty, speaker, actor, target, stance, time, and boundaries.",
            "Every event evidence value must be one exact contiguous source substring and offsets must exactly identify it. Return no_signal only when no in-scope event remains. Set coverage_truncated true rather than silently omitting events if more than %d atomic events are present." % event_cap,
            "# ai_discourse_v3_1 codebook",
            pack.codebook.strip() or pack.prompt.strip(),
        )
    )


def _reference_batch_prompt(rows: Sequence[dict[str, Any]]) -> str:
    packets = [
        {
            "segment_id": row["segment_id"],
            "episode_id": row["episode_id"],
            "source_text_sha256": row["text_sha256"],
            "source_text": row["segment_text"],
        }
        for row in rows
    ]
    return (
        "Independently extract the complete atomic reference event set for every exact frozen "
        "source packet. Preserve the supplied packet order and return exactly one structured "
        "segment result per segment_id. Never carry evidence or meaning between packets. Return "
        "only the structured JSON.\n\n# Frozen source packets\n"
        + _compact_json({"segments": packets})
        + "\n"
    )


def _plan_extraction_batches(
    ordered: Sequence[dict[str, Any]], *, batch_size: int
) -> list[list[dict[str, Any]]]:
    if not 5 <= batch_size <= 8:
        raise ValueError("extraction batch size must be between 5 and 8")
    by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    episode_order = []
    for row in ordered:
        episode_id = str(row["episode_id"])
        if episode_id not in by_episode:
            episode_order.append(episode_id)
        by_episode[episode_id].append(row)
    episode_chunks = []
    for episode_id in episode_order:
        rows = by_episode[episode_id]
        for offset in range(0, len(rows), batch_size):
            episode_chunks.append(rows[offset : offset + batch_size])
    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for chunk in episode_chunks:
        if current and len(current) + len(chunk) > batch_size:
            batches.append(current)
            current = []
        current.extend(chunk)
        if len(current) == batch_size:
            batches.append(current)
            current = []
    if current:
        batches.append(current)
    return batches


def _plan_support_packets(
    extraction_rows: Sequence[dict[str, Any]],
    *,
    packet_size: int,
    max_witnesses: int,
    max_packet_bytes: int,
) -> list[list[dict[str, Any]]]:
    if packet_size < 1 or max_witnesses < 1 or max_packet_bytes < 1:
        raise ValueError("support packet bounds must be positive")
    packets: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_witnesses = 0
    current_bytes = 0
    for row in extraction_rows:
        witnesses = len(row["events"])
        case_bytes = len(
            _compact_json(
                {
                    "source_text": row["segment_text"],
                    "source_text_sha256": row["text_sha256"],
                    "events": row["events"],
                }
            ).encode("utf-8")
        )
        if witnesses > max_witnesses or case_bytes > max_packet_bytes:
            raise ValueError(f"one support case exceeds frozen packet bounds: {row['segment_id']}")
        exceeds = current and (
            len(current) >= packet_size
            or current_witnesses + witnesses > max_witnesses
            or current_bytes + case_bytes > max_packet_bytes
        )
        if exceeds:
            packets.append(current)
            current = []
            current_witnesses = 0
            current_bytes = 0
        current.append(row)
        current_witnesses += witnesses
        current_bytes += case_bytes
    if current:
        packets.append(current)
    return packets


def validate_atomic_reference_output(
    output: Any,
    *,
    frozen: dict[str, Any],
    event_cap: int,
) -> list[dict[str, Any]]:
    if not isinstance(output, dict):
        raise ValueError("atomic reference output is not an object")
    if output.get("schema_version") != ATOMIC_REFERENCE_OUTPUT_VERSION:
        raise ValueError("atomic reference output schema mismatch")
    if output.get("segment_id") != frozen["segment_id"]:
        raise ValueError("atomic reference segment_id mismatch")
    if output.get("episode_id") != frozen["episode_id"]:
        raise ValueError("atomic reference episode_id mismatch")
    events = output.get("events")
    if not isinstance(events, list) or len(events) > event_cap:
        raise ValueError("atomic reference event list is invalid")
    if output.get("coverage_truncated") is not False:
        raise ValueError("atomic reference coverage is truncated or unknown")
    status = output.get("extraction_status")
    if status == "coded" and not events:
        raise ValueError("coded atomic reference has no events")
    if status == "no_signal" and events:
        raise ValueError("no-signal atomic reference contains events")
    if status not in {"coded", "no_signal"}:
        raise ValueError("atomic reference abstained")
    source = frozen["segment_text"]
    canonical_events = set()
    normalized = []
    required = {
        "event_type",
        "atomic_claim",
        "speaker",
        "actor",
        "target",
        "stance",
        "certainty",
        "temporal_horizon",
        "evidence",
        "evidence_start",
        "evidence_end",
    }
    for event in events:
        if not isinstance(event, dict) or set(event) != required:
            raise ValueError("atomic reference event shape is invalid")
        evidence = event.get("evidence")
        start = event.get("evidence_start")
        end = event.get("evidence_end")
        if (
            not isinstance(evidence, str)
            or not evidence
            or isinstance(start, bool)
            or not isinstance(start, int)
            or isinstance(end, bool)
            or not isinstance(end, int)
            or not (0 <= start < end <= len(source))
            or source[start:end] != evidence
        ):
            raise ValueError("atomic reference event evidence or offsets are not exact")
        canonical = _compact_json(event)
        if canonical in canonical_events:
            raise ValueError("atomic reference contains an exact duplicate event")
        canonical_events.add(canonical)
        normalized.append(event)
    return normalized


def validate_atomic_reference_batch_output(
    output: Any,
    *,
    rows: Sequence[dict[str, Any]],
    event_cap: int,
) -> dict[str, list[dict[str, Any]]]:
    if (
        not isinstance(output, dict)
        or output.get("schema_version") != ATOMIC_REFERENCE_BATCH_OUTPUT_VERSION
        or not isinstance(output.get("segments"), list)
    ):
        raise ValueError("atomic reference batch output root is invalid")
    expected = {str(row["segment_id"]): row for row in rows}
    if len(output["segments"]) != len(expected):
        raise ValueError("atomic reference batch segment count is invalid")
    normalized = {}
    for item in output["segments"]:
        if not isinstance(item, dict):
            raise ValueError("atomic reference batch contains a malformed segment")
        segment_id = str(item.get("segment_id") or "")
        if segment_id not in expected or segment_id in normalized:
            raise ValueError("atomic reference batch segment partition is invalid")
        normalized[segment_id] = validate_atomic_reference_output(
            item, frozen=expected[segment_id], event_cap=event_cap
        )
    if set(normalized) != set(expected):
        raise ValueError("atomic reference batch omitted a frozen segment")
    return normalized


def _sidecar_usage(path: Path) -> tuple[Optional[dict[str, int]], bool, Optional[str]]:
    if not path.is_file():
        return None, False, "sidecar_missing"
    try:
        sidecar = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None, False, "sidecar_invalid"
    if not isinstance(sidecar, dict) or sidecar.get("state") not in _TERMINAL_STATES:
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


def _split_witnesses(
    rows: Sequence[dict[str, Any]], *, seed: str
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    cases = []
    witness_mapping = {}
    for row in rows:
        case_id = stable_id(seed, "case", row["segment_id"], prefix="rcase_")
        left = []
        right = []
        for index, event in enumerate(row["events"]):
            witness_id = stable_id(seed, "event", row["segment_id"], str(index), prefix="rwit_")
            witness = {"witness_id": witness_id, "event": event}
            (left if index % 2 == 0 else right).append(witness)
            witness_mapping[witness_id] = {
                "segment_id": row["segment_id"],
                "event_index": index,
                "event": event,
            }
        cases.append(
            {
                "case_id": case_id,
                "segment_id": row["segment_id"],
                "source_text": row["segment_text"],
                "source_text_sha256": row["text_sha256"],
                "event_set_a": left,
                "event_set_b": right,
            }
        )
    return cases, witness_mapping


def _support_variant(cases: Sequence[dict[str, Any]], orientation: str) -> dict[str, Any]:
    if orientation not in {"ab", "ba"}:
        raise ValueError("support orientation must be ab or ba")
    reverse = orientation == "ba"
    rendered = []
    for case in cases:
        rendered.append(
            {
                "case_id": case["case_id"],
                "source_text": case["source_text"],
                "source_text_sha256": case["source_text_sha256"],
                "event_set_left": case["event_set_b"] if reverse else case["event_set_a"],
                "event_set_right": case["event_set_a"] if reverse else case["event_set_b"],
            }
        )
    return {"orientation": orientation, "cases": rendered}


def support_output_schema(variant: dict[str, Any]) -> dict[str, Any]:
    case_ids = [case["case_id"] for case in variant["cases"]]
    witness_ids = [
        witness["witness_id"]
        for case in variant["cases"]
        for side in ("event_set_left", "event_set_right")
        for witness in case[side]
    ]
    support = {
        "type": "object",
        "additionalProperties": False,
        "required": ["witness_id", "verdict", "evidence_spans", "rationale"],
        "properties": {
            "witness_id": (
                {"type": "string", "enum": witness_ids}
                if witness_ids
                else {"type": "string"}
            ),
            "verdict": {"type": "string", "enum": list(SUPPORT_VERDICTS)},
            "evidence_spans": {"type": "array", "items": {"type": "string"}},
            "rationale": {"type": "string"},
        },
    }
    coverage = {
        "type": "object",
        "additionalProperties": False,
        "required": ["verdict", "missing_event_evidence_spans", "rationale"],
        "properties": {
            "verdict": {"type": "string", "enum": list(COVERAGE_VERDICTS)},
            "missing_event_evidence_spans": {
                "type": "array",
                "items": {"type": "string"},
            },
            "rationale": {"type": "string"},
        },
    }
    case_result = {
        "type": "object",
        "additionalProperties": False,
        "required": ["case_id", "support_results", "coverage"],
        "properties": {
            "case_id": {"type": "string", "enum": case_ids},
            "support_results": {"type": "array", "items": support},
            "coverage": coverage,
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "cases"],
        "properties": {
            "schema_version": {"type": "string", "const": SUPPORT_OUTPUT_VERSION},
            "cases": {
                "type": "array",
                "minItems": len(case_ids),
                "maxItems": len(case_ids),
                "items": case_result,
            },
        },
    }


def _support_base_instructions() -> str:
    return (
        "You are an independent blinded LLM support and coverage verifier for atomic reference "
        "events. Judge meaning directly from each exact source. Do not use keyword, regex, token "
        "overlap, embeddings, phrase matching, or side preference. For each anonymous event, use "
        "supported only when the complete atomic meaning is source-grounded, unsupported when it "
        "is not, and abstain when unresolved. Return exact source spans for supported events. "
        "Then judge coverage: clean_no_signal only when the source contains no in-scope atomic "
        "event; complete_event_coverage when the submitted set covers every in-scope atomic event; "
        "events_missing with exact missing spans when events were omitted; otherwise abstain."
    )


def _support_prompt(variant: dict[str, Any]) -> str:
    return (
        "Complete support verification first for every witness, then independently decide source "
        "coverage for every case. Include every case and witness exactly once. The left/right order "
        "is arbitrary and conveys no provenance. Return only structured JSON.\n\n"
        "# Blinded reference packet\n"
        + _compact_json({"cases": variant["cases"]})
        + "\n"
    )


def validate_support_output(output: Any, variant: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(output, dict) or output.get("schema_version") != SUPPORT_OUTPUT_VERSION:
        raise ValueError("support output root is invalid")
    expected = {case["case_id"]: case for case in variant["cases"]}
    rows = output.get("cases")
    if not isinstance(rows, list) or len(rows) != len(expected):
        raise ValueError("support output case count is invalid")
    normalized = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"case_id", "support_results", "coverage"}:
            raise ValueError("support output case shape is invalid")
        case_id = row["case_id"]
        if case_id not in expected or case_id in normalized:
            raise ValueError("support output case IDs are invalid")
        case = expected[case_id]
        source = case["source_text"]
        witness_ids = {
            witness["witness_id"]
            for side in ("event_set_left", "event_set_right")
            for witness in case[side]
        }
        results = row["support_results"]
        if not isinstance(results, list):
            raise ValueError("support results are not an array")
        by_witness = {}
        for result in results:
            if not isinstance(result, dict) or set(result) != {
                "witness_id",
                "verdict",
                "evidence_spans",
                "rationale",
            }:
                raise ValueError("support result shape is invalid")
            witness_id = result["witness_id"]
            if witness_id not in witness_ids or witness_id in by_witness:
                raise ValueError("support witness partition is invalid")
            if result["verdict"] not in SUPPORT_VERDICTS:
                raise ValueError("support verdict is invalid")
            spans = result["evidence_spans"]
            if not isinstance(spans, list) or any(
                not isinstance(span, str) or not span or span not in source for span in spans
            ):
                raise ValueError("support evidence span is not exact")
            if result["verdict"] == "supported" and not spans:
                raise ValueError("supported witness has no exact evidence span")
            by_witness[witness_id] = result
        if set(by_witness) != witness_ids:
            raise ValueError("support witnesses do not exactly partition the case")
        coverage = row["coverage"]
        if not isinstance(coverage, dict) or set(coverage) != {
            "verdict",
            "missing_event_evidence_spans",
            "rationale",
        }:
            raise ValueError("coverage result shape is invalid")
        if coverage["verdict"] not in COVERAGE_VERDICTS:
            raise ValueError("coverage verdict is invalid")
        missing = coverage["missing_event_evidence_spans"]
        if not isinstance(missing, list) or any(
            not isinstance(span, str) or not span or span not in source for span in missing
        ):
            raise ValueError("coverage missing-event span is not exact")
        if coverage["verdict"] == "events_missing" and not missing:
            raise ValueError("events-missing coverage has no exact evidence")
        normalized[case_id] = {"support": by_witness, "coverage": coverage}
    return normalized


def _consensus_reference_cases(
    extraction_rows: Sequence[dict[str, Any]],
    *,
    packet_outputs: Sequence[dict[str, Any]],
    seed: str,
) -> list[dict[str, Any]]:
    support_by_case: dict[str, dict[str, dict[str, Any]]] = {}
    mapping_by_witness: dict[str, dict[str, Any]] = {}
    for packet in packet_outputs:
        mapping_by_witness.update(packet["witness_mapping"])
        ab = packet["normalized_ab"]
        ba = packet["normalized_ba"]
        if set(ab) != set(ba):
            raise ValueError("AB/BA support case partitions differ")
        for case_id in ab:
            if case_id in support_by_case:
                raise ValueError("support case appears in multiple packets")
            support_by_case[case_id] = {"ab": ab[case_id], "ba": ba[case_id]}
    cases = []
    for row in extraction_rows:
        case_id = stable_id(seed, "case", row["segment_id"], prefix="rcase_")
        support = support_by_case.get(case_id)
        if support is None:
            raise ValueError(f"reference case has no AB/BA support packet: {row['segment_id']}")
        coverage_ab = support["ab"]["coverage"]["verdict"]
        coverage_ba = support["ba"]["coverage"]["verdict"]
        raw_count = len(row["events"])
        clean_no_signal = (
            raw_count == 0
            and coverage_ab == "clean_no_signal"
            and coverage_ba == "clean_no_signal"
        )
        coverage_complete = clean_no_signal or (
            raw_count > 0
            and coverage_ab == "complete_event_coverage"
            and coverage_ba == "complete_event_coverage"
        )
        supported_events = []
        support_consensus = []
        support_abstained = False
        for witness_id, mapped in sorted(
            mapping_by_witness.items(), key=lambda item: (item[1]["segment_id"], item[1]["event_index"])
        ):
            if mapped["segment_id"] != row["segment_id"]:
                continue
            left = support["ab"]["support"][witness_id]
            right = support["ba"]["support"][witness_id]
            verdict = left["verdict"] if left["verdict"] == right["verdict"] else "abstain"
            spans = (
                sorted(set(left["evidence_spans"]) | set(right["evidence_spans"]))
                if verdict == "supported"
                else []
            )
            support_consensus.append(
                {"witness_id": witness_id, "verdict": verdict, "evidence_spans": spans}
            )
            support_abstained = support_abstained or verdict == "abstain"
            if verdict == "supported":
                supported_events.append(mapped["event"])
        event_count = len(supported_events) if coverage_complete else None
        cases.append(
            {
                "schema_version": REFERENCE_CASE_VERSION,
                "case_id": case_id,
                "segment_id": row["segment_id"],
                "episode_id": row["episode_id"],
                "transcript_id": row["transcript_id"],
                "text_sha256": row["text_sha256"],
                "reservoir": row["reservoir"],
                "raw_event_count": raw_count,
                "reference_event_count": event_count,
                "coverage_complete": coverage_complete,
                "clean_no_signal": clean_no_signal,
                "coverage_ab": coverage_ab,
                "coverage_ba": coverage_ba,
                "support_consensus": support_consensus,
                "support_abstained": support_abstained,
                "supported_events": supported_events,
                "selection_eligible": (
                    coverage_complete and event_count is not None and not support_abstained
                ),
            }
        )
    return cases


def _stratum(event_count: int) -> str:
    if event_count == 0:
        return "no_signal"
    if 1 <= event_count <= 4:
        return "low"
    if 5 <= event_count <= 15:
        return "medium"
    return "dense"


def _aggregate_usage(outcomes: Sequence[dict[str, Any]]) -> dict[str, int]:
    total: Counter[str] = Counter()
    for outcome in outcomes:
        usage = outcome.get("usage")
        if isinstance(usage, dict):
            total.update({field: int(usage.get(field) or 0) for field in _USAGE_FIELDS})
    return {field: int(total[field]) for field in _USAGE_FIELDS}


def _execution_outputs_absent(execution_dir: Path) -> dict[str, Any]:
    checked = [
        execution_dir / "phase-b-candidate",
        execution_dir / "phase-c-baseline",
    ]
    observed = [str(path) for path in checked if path.exists()]
    if observed:
        raise ValueError("candidate or baseline outputs already exist; stratification must precede them")
    return {
        "execution_dir": str(execution_dir),
        "checked_absent_paths": [str(path) for path in checked],
        "candidate_outputs_observed": False,
        "baseline_outputs_observed": False,
    }


def _validate_terminal_stratifier_lineage(
    root: Path,
    *,
    report: dict[str, Any],
    frozen_plan: dict[str, Any],
    instruction_contract: dict[str, Any],
    execution_lineage: dict[str, Any],
) -> None:
    root = Path(root).expanduser().resolve()
    if (
        not root.is_dir()
        or frozen_plan.get("schema_version") != STRATIFIER_PLAN_VERSION
        or report.get("schema_version") != STRATIFIER_REPORT_VERSION
        or frozen_plan.get("state") != "frozen_before_reference_model_calls"
        or frozen_plan.get("model") != REFERENCE_MODEL
        or frozen_plan.get("reasoning_effort") != REFERENCE_EFFORT
        or frozen_plan.get("support_model") != SUPPORT_MODEL
        or frozen_plan.get("support_reasoning_effort") != SUPPORT_EFFORT
        or report.get("model") != REFERENCE_MODEL
        or report.get("reasoning_effort") != REFERENCE_EFFORT
        or report.get("support_model") != SUPPORT_MODEL
        or report.get("support_reasoning_effort") != SUPPORT_EFFORT
        or frozen_plan.get("instruction_contract") != instruction_contract
        or report.get("instruction_contract") != instruction_contract
        or frozen_plan.get("execution_lineage") != execution_lineage
        or report.get("execution_lineage") != execution_lineage
    ):
        raise ValueError("terminal stratifier execution lineage drift")
    extraction_count = report.get("requested_extraction_calls")
    support_count = report.get("requested_support_calls")
    validated_extraction_count = report.get("validated_extraction_calls")
    validated_support_count = report.get("validated_support_calls")
    planned_batches = frozen_plan.get("extraction_batch_segment_ids")
    if (
        isinstance(extraction_count, bool)
        or not isinstance(extraction_count, int)
        or extraction_count < 0
        or isinstance(support_count, bool)
        or not isinstance(support_count, int)
        or support_count < 0
        or support_count % 2
        or isinstance(validated_extraction_count, bool)
        or not isinstance(validated_extraction_count, int)
        or not 0 <= validated_extraction_count <= extraction_count
        or isinstance(validated_support_count, bool)
        or not isinstance(validated_support_count, int)
        or not 0 <= validated_support_count <= support_count
        or not isinstance(planned_batches, list)
        or len(planned_batches) != extraction_count
    ):
        raise ValueError("terminal stratifier request identity drift")
    expected_extraction_sidecars = {
        str((root / "extractions" / f"batch-{index:04d}" / "sidecar.json").resolve())
        for index in range(extraction_count)
    }
    expected_support_sidecars = {
        str(
            (
                root
                / "support"
                / f"packet-{packet_index:04d}"
                / orientation
                / "sidecar.json"
            ).resolve()
        )
        for packet_index in range(support_count // 2)
        for orientation in ("ab", "ba")
    }
    expected_sidecars = expected_extraction_sidecars | expected_support_sidecars
    leaf_bindings = report.get("leaf_bindings")
    if not isinstance(leaf_bindings, list):
        raise ValueError("terminal stratifier leaf bindings are missing")
    seen_sidecars: set[str] = set()
    seen_turns: set[str] = set()
    for binding in leaf_bindings:
        validated = validate_holdout_leaf_binding(
            binding,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        if any(
            not _inside(Path(str(record.get("path") or "")), root)
            for record in validated["artifacts"].values()
        ):
            raise ValueError("terminal stratifier leaf artifact is outside current root")
        sidecar_path = validated["artifacts"]["sidecar"]["path"]
        if sidecar_path in expected_extraction_sidecars:
            expected_model, expected_effort = REFERENCE_MODEL, REFERENCE_EFFORT
        elif sidecar_path in expected_support_sidecars:
            expected_model, expected_effort = SUPPORT_MODEL, SUPPORT_EFFORT
        else:
            raise ValueError("terminal stratifier leaf path is outside a frozen phase")
        if (
            validated["model"] != expected_model
            or validated["effort"] != expected_effort
            or sidecar_path in seen_sidecars
            or validated["turn_id"] in seen_turns
        ):
            raise ValueError("terminal stratifier leaf identity drift")
        seen_sidecars.add(sidecar_path)
        seen_turns.add(validated["turn_id"])
    sidecars = sorted(root.glob("extractions/*/sidecar.json")) + sorted(
        root.glob("support/packet-*/*/sidecar.json")
    )
    current_sidecars = {str(path.resolve()) for path in sidecars}
    if not current_sidecars.issubset(expected_sidecars):
        raise ValueError("terminal stratifier contains a foreign sidecar request")
    if not seen_sidecars.issubset(current_sidecars):
        raise ValueError("terminal stratifier binding sidecars differ from current root")
    validated_binding_count = validated_extraction_count + validated_support_count
    if len(leaf_bindings) != validated_binding_count:
        raise ValueError("terminal stratifier leaf binding evidence is incomplete")
    if len(current_sidecars) == validated_binding_count and seen_sidecars != current_sidecars:
        raise ValueError(
            "terminal stratifier binding sidecars differ from exact current-root sidecars"
        )
    for sidecar_path in sidecars:
        _, sidecar = _read_json(sidecar_path, purpose="terminal stratifier sidecar")
        validate_managed_sidecar_execution_lineage(
            sidecar=sidecar,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
    if report.get("ok") is True:
        expected = extraction_count + support_count
        if len(sidecars) != expected:
            raise ValueError("terminal stratifier sidecar lineage evidence is incomplete")
        if len(leaf_bindings) != expected:
            raise ValueError("terminal stratifier leaf binding evidence is incomplete")
        if current_sidecars != expected_sidecars or seen_sidecars != current_sidecars:
            raise ValueError(
                "terminal stratifier binding sidecars differ from exact current-root sidecars"
            )


async def run_holdout_reference_stratifier(
    conn: sqlite3.Connection,
    *,
    covenant_path: Union[str, Path],
    output_dir: Union[str, Path],
    execution_dir: Union[str, Path],
    event_cap: int = DEFAULT_EVENT_CAP,
    packet_size: int = DEFAULT_PACKET_SIZE,
    extraction_batch_size: int = DEFAULT_EXTRACTION_BATCH_SIZE,
    support_max_witnesses: int = DEFAULT_SUPPORT_MAX_WITNESSES,
    support_max_packet_bytes: int = DEFAULT_SUPPORT_MAX_PACKET_BYTES,
    paired_per_stratum: int = 15,
    clean_no_signal_count: int = 60,
    seed: str = "app-server-holdout-reference-stratification-v1",
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
) -> dict[str, Any]:
    if event_cap < 16 or packet_size < 1:
        raise ValueError("invalid stratifier event cap or packet size")
    if paired_per_stratum < 1 or clean_no_signal_count < 1:
        raise ValueError("stratifier target counts must be positive")
    inputs = _load_reservoirs(conn, covenant_path=covenant_path)
    root = Path(output_dir).expanduser().resolve()
    execution_root = Path(execution_dir).expanduser().resolve()
    instruction_contract = verify_instruction_contract()
    execution_lineage = verified_holdout_execution_lineage(instruction_contract)
    effective_client_factory = resolve_holdout_client_factory(
        client_factory, instruction_contract
    )
    label_pack_contract = verify_label_pack_contract(LABEL_PACK)
    terminal_report_path = root / "report.json"
    if terminal_report_path.is_file():
        _, report = _read_json(terminal_report_path, purpose="stratifier report")
        _, frozen_plan = _read_json(root / "plan.json", purpose="stratifier plan")
        _validate_terminal_stratifier_lineage(
            root,
            report=report,
            frozen_plan=frozen_plan,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        if (
            frozen_plan.get("instruction_contract") != instruction_contract
            or report.get("instruction_contract") != instruction_contract
            or frozen_plan.get("label_pack_contract") != label_pack_contract
            or report.get("label_pack_contract") != label_pack_contract
        ):
            raise ValueError("terminal stratifier instruction contract drift")
        return report
    root.mkdir(parents=True, exist_ok=True)
    absence = _execution_outputs_absent(execution_root)
    ordered = sorted(
        inputs["segments"],
        key=lambda row: (
            sha256_text(
                "|".join(
                    (
                        seed,
                        str(row["reservoir"]),
                        str(row["segment_id"]),
                        str(row["text_sha256"]),
                    )
                )
            ),
            str(row["segment_id"]),
        ),
    )
    for index, row in enumerate(ordered):
        row["precommitted_rank"] = index
        row["selection_hash"] = sha256_text(
            f"{seed}|select|{row['reservoir']}|{row['segment_id']}|{row['text_sha256']}"
        )
    extraction_batches = _plan_extraction_batches(
        ordered, batch_size=extraction_batch_size
    )
    plan = {
        "schema_version": STRATIFIER_PLAN_VERSION,
        "state": "frozen_before_reference_model_calls",
        "covenant_sha256": inputs["covenant_sha256"],
        "model": REFERENCE_MODEL,
        "reasoning_effort": REFERENCE_EFFORT,
        "support_model": SUPPORT_MODEL,
        "support_reasoning_effort": SUPPORT_EFFORT,
        "event_cap": event_cap,
        "packet_size": packet_size,
        "extraction_batch_size": extraction_batch_size,
        "support_max_witnesses": support_max_witnesses,
        "support_max_packet_bytes": support_max_packet_bytes,
        "paired_per_stratum": paired_per_stratum,
        "clean_no_signal_count": clean_no_signal_count,
        "seed_sha256": sha256_text(seed),
        "segment_ids_in_precommitted_order": [row["segment_id"] for row in ordered],
        "extraction_batch_segment_ids": [
            [row["segment_id"] for row in batch] for batch in extraction_batches
        ],
        "retry_count": 0,
        "semantic_method": "llm_atomic_extraction_then_blinded_ab_ba_support_and_coverage",
        "deterministic_semantics_prohibited": True,
        "adaptive_top_up": False,
        "instruction_contract": instruction_contract,
        "execution_lineage": execution_lineage,
        "label_pack_contract": label_pack_contract,
        **absence,
    }
    _ensure_immutable(root / "plan.json", _canonical_bytes(plan))
    ranking = {
        "schema_version": STRATIFIER_PLAN_VERSION,
        "rows": [
            {
                "segment_id": row["segment_id"],
                "episode_id": row["episode_id"],
                "transcript_id": row["transcript_id"],
                "text_sha256": row["text_sha256"],
                "reservoir": row["reservoir"],
                "precommitted_rank": row["precommitted_rank"],
                "selection_hash": row["selection_hash"],
            }
            for row in ordered
        ],
    }
    _ensure_immutable(root / "precommitted-ranking.json", _canonical_bytes(ranking))
    before_changes = conn.total_changes

    extraction_checkpoint_path = root / "extraction-checkpoint.json"
    if extraction_checkpoint_path.is_file():
        _, extraction_checkpoint = _read_json(
            extraction_checkpoint_path, purpose="extraction checkpoint"
        )
    else:
        extraction_checkpoint = {"schema_version": STRATIFIER_PLAN_VERSION, "outcomes": {}}
    extraction_batch_outcomes = extraction_checkpoint.get("outcomes")
    if not isinstance(extraction_batch_outcomes, dict):
        raise ValueError("extraction checkpoint is malformed")
    base_instructions = _reference_base_instructions(event_cap)
    extraction_base_path = root / "extraction-base-instructions.private.md"
    _ensure_immutable(extraction_base_path, base_instructions.encode("utf-8"))
    support_base_instructions = _support_base_instructions()
    support_base_path = root / "support-base-instructions.private.md"
    _ensure_immutable(
        support_base_path, support_base_instructions.encode("utf-8")
    )
    prepared_batches = {}
    for batch_index, batch_rows in enumerate(extraction_batches):
        batch_id = f"batch-{batch_index:04d}"
        item_dir = root / "extractions" / batch_id
        prompt = _reference_batch_prompt(batch_rows)
        schema = atomic_reference_batch_output_schema(
            batch_rows, event_cap=event_cap
        )
        _ensure_immutable(item_dir / "prompt.private.md", prompt.encode("utf-8"))
        _ensure_immutable(item_dir / "schema.json", _canonical_bytes(schema))
        prepared_batches[batch_id] = {
            "rows": batch_rows,
            "dir": item_dir,
            "prompt": prompt,
            "schema": schema,
        }

    support_packet_outputs = []
    support_outcomes = []
    support_packets: list[list[dict[str, Any]]] = []
    prepared_support: dict[tuple[int, str], dict[str, Any]] = {}
    async with LazyClientSession(effective_client_factory) as client:
        for batch_index, batch_rows in enumerate(extraction_batches):
            batch_id = f"batch-{batch_index:04d}"
            item = prepared_batches[batch_id]
            item_dir = item["dir"]
            attempt_path = item_dir / "attempt.json"
            outcome_path = item_dir / "outcome.json"
            raw_path = item_dir / "raw-output.private.json"
            sidecar_path = item_dir / "sidecar.json"
            if batch_id in extraction_batch_outcomes:
                outcome = extraction_batch_outcomes[batch_id]
                if outcome.get("status") == "validated":
                    _sidecar, usage = validate_completed_managed_sidecar(
                        sidecar_path=sidecar_path,
                        raw_output_path=raw_path,
                        model=REFERENCE_MODEL,
                        effort=REFERENCE_EFFORT,
                        thread_mode="new_thread",
                        batch_size=len(batch_rows),
                        prompt=item["prompt"],
                        output_schema=item["schema"],
                        base_instructions=base_instructions,
                        instruction_contract=instruction_contract,
                        execution_lineage=execution_lineage,
                    )
                    if usage != outcome.get("usage"):
                        raise ValueError("extraction checkpoint usage drift")
                    if (
                        _sha256_file(raw_path) != outcome.get("raw_output_sha256")
                        or _sha256_file(sidecar_path) != outcome.get("sidecar_sha256")
                    ):
                        raise ValueError("extraction checkpoint artifact hash drift")
                continue
            clean_pre_turn_resume = bool(
                attempt_path.exists()
                and not any(path.exists() for path in (outcome_path, raw_path, sidecar_path))
            )
            if attempt_path.exists() and not clean_pre_turn_resume:
                if outcome_path.is_file():
                    _, outcome = _read_json(outcome_path, purpose="extraction outcome")
                else:
                    outcome = {
                        "batch_id": batch_id,
                        "segment_ids": [row["segment_id"] for row in batch_rows],
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
                        model=REFERENCE_MODEL,
                        effort=REFERENCE_EFFORT,
                        thread_mode="new_thread",
                        batch_size=len(batch_rows),
                        prompt=item["prompt"],
                        output_schema=item["schema"],
                        base_instructions=base_instructions,
                        instruction_contract=instruction_contract,
                        execution_lineage=execution_lineage,
                    )
                    if usage != outcome.get("usage"):
                        raise ValueError("extraction outcome usage differs from managed sidecar")
                    if (
                        _sha256_file(raw_path) != outcome.get("raw_output_sha256")
                        or _sha256_file(sidecar_path) != outcome.get("sidecar_sha256")
                    ):
                        raise ValueError("extraction checkpoint artifact hash drift")
                extraction_batch_outcomes[batch_id] = outcome
                _checkpoint(extraction_checkpoint_path, extraction_checkpoint)
                continue
            if not attempt_path.exists():
                unexpected = [
                    path
                    for path in (outcome_path, raw_path, sidecar_path)
                    if path.exists()
                ]
                if unexpected:
                    raise ValueError("reference artifacts exist without an immutable attempt")
                await _wait_for_capacity_before_attempt(client)
                attempt = {
                    "batch_id": batch_id,
                    "segment_ids": [row["segment_id"] for row in batch_rows],
                    "model": REFERENCE_MODEL,
                    "reasoning_effort": REFERENCE_EFFORT,
                    "prompt_sha256": sha256_text(item["prompt"]),
                    "schema_sha256": _sha256_bytes(_canonical_bytes(item["schema"])),
                    "retry_ordinal": 0,
                }
                _write_immutable(attempt_path, _canonical_bytes(attempt))
            else:
                await _wait_for_capacity_before_attempt(client)
            try:
                result = await client.run_ephemeral_structured_turn(
                    model=REFERENCE_MODEL,
                    effort=REFERENCE_EFFORT,
                    base_instructions=base_instructions,
                    prompt=item["prompt"],
                    output_schema=item["schema"],
                    cwd=Path.cwd(),
                    sidecar_path=sidecar_path,
                    output_path=raw_path,
                    batch_size=len(batch_rows),
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                )
                if not result.status_ok or not isinstance(result.output, dict):
                    raise ValueError(result.error_class or f"turn_{result.status}")
                if not raw_path.exists():
                    _write_immutable(raw_path, _canonical_bytes(result.output))
                events_by_segment = validate_atomic_reference_batch_output(
                    result.output, rows=batch_rows, event_cap=event_cap
                )
                per_segment_outputs = {}
                for row in batch_rows:
                    segment_id = row["segment_id"]
                    normalized_path = item_dir / "segments" / f"{segment_id}.private.json"
                    normalized = {
                        "schema_version": ATOMIC_REFERENCE_OUTPUT_VERSION,
                        "segment_id": segment_id,
                        "episode_id": row["episode_id"],
                        "transcript_id": row["transcript_id"],
                        "text_sha256": row["text_sha256"],
                        "reservoir": row["reservoir"],
                        "events": events_by_segment[segment_id],
                    }
                    _write_immutable(normalized_path, _canonical_bytes(normalized))
                    per_segment_outputs[segment_id] = {
                        "events_path": str(normalized_path),
                        "events_sha256": _sha256_file(normalized_path),
                    }
                _sidecar, usage = validate_completed_managed_sidecar(
                    sidecar_path=sidecar_path,
                    raw_output_path=raw_path,
                    model=REFERENCE_MODEL,
                    effort=REFERENCE_EFFORT,
                    thread_mode="new_thread",
                    batch_size=len(batch_rows),
                    prompt=item["prompt"],
                    output_schema=item["schema"],
                    base_instructions=base_instructions,
                    instruction_contract=instruction_contract,
                    execution_lineage=execution_lineage,
                )
                usage_complete = True
                usage_error = None
                outcome = {
                    "batch_id": batch_id,
                    "segment_ids": [row["segment_id"] for row in batch_rows],
                    "status": "validated" if usage_complete else "validated_usage_unknown",
                    "status_ok": usage_complete,
                    "failure_class": usage_error,
                    "per_segment_outputs": per_segment_outputs,
                    "raw_output_sha256": _sha256_file(raw_path),
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
                    "batch_id": batch_id,
                    "segment_ids": [row["segment_id"] for row in batch_rows],
                    "status": "failed_no_retry",
                    "status_ok": False,
                    "failure_class": type(exc).__name__,
                    "usage_error": usage_error,
                    "usage": usage,
                    "usage_complete": usage_complete,
                    "sidecar_path": str(sidecar_path) if sidecar_path.exists() else None,
                }
            _write_immutable(outcome_path, _canonical_bytes(outcome))
            extraction_batch_outcomes[batch_id] = outcome
            _checkpoint(extraction_checkpoint_path, extraction_checkpoint)

        extraction_rows = []
        rows_by_id = {row["segment_id"]: row for row in ordered}
        for batch_index, batch_rows in enumerate(extraction_batches):
            batch_id = f"batch-{batch_index:04d}"
            outcome = extraction_batch_outcomes[batch_id]
            if outcome.get("status") != "validated":
                continue
            per_segment = outcome.get("per_segment_outputs") or {}
            for segment_id in outcome["segment_ids"]:
                record = per_segment.get(segment_id)
                if not isinstance(record, dict):
                    raise ValueError("validated extraction batch lacks per-segment output")
                _, normalized = _read_json(
                    record["events_path"], purpose="atomic reference events"
                )
                if _sha256_file(Path(record["events_path"])) != record["events_sha256"]:
                    raise ValueError("atomic reference per-segment artifact drift")
                extraction_rows.append(
                    {**rows_by_id[segment_id], "events": normalized["events"]}
                )
        extraction_rows.sort(key=lambda row: int(row["precommitted_rank"]))
        extraction_complete = len(extraction_rows) == len(ordered) and all(
            extraction_batch_outcomes[f"batch-{index:04d}"].get("usage_complete") is True
            for index in range(len(extraction_batches))
        )
        if extraction_complete:
            support_packets = _plan_support_packets(
                extraction_rows,
                packet_size=packet_size,
                max_witnesses=support_max_witnesses,
                max_packet_bytes=support_max_packet_bytes,
            )
            support_plan = {
                "schema_version": STRATIFIER_PLAN_VERSION,
                "packet_size_ceiling": packet_size,
                "witness_ceiling": support_max_witnesses,
                "packet_bytes_ceiling": support_max_packet_bytes,
                "packets": [
                    {
                        "packet_index": index,
                        "segment_ids": [row["segment_id"] for row in packet_rows],
                        "witness_count": sum(len(row["events"]) for row in packet_rows),
                        "estimated_packet_bytes": sum(
                            len(
                                _compact_json(
                                    {
                                        "source_text": row["segment_text"],
                                        "source_text_sha256": row["text_sha256"],
                                        "events": row["events"],
                                    }
                                ).encode("utf-8")
                            )
                            for row in packet_rows
                        ),
                    }
                    for index, packet_rows in enumerate(support_packets)
                ],
            }
            _ensure_immutable(
                root / "support-packet-plan.json", _canonical_bytes(support_plan)
            )
            for packet_index, packet_rows in enumerate(support_packets):
                packet_ids = [row["segment_id"] for row in packet_rows]
                cases, witness_mapping = _split_witnesses(packet_rows, seed=seed)
                orientations = {}
                packet_outcomes = []
                for orientation in ("ab", "ba"):
                    variant = _support_variant(cases, orientation)
                    packet_dir = root / "support" / f"packet-{packet_index:04d}" / orientation
                    prompt = _support_prompt(variant)
                    schema = support_output_schema(variant)
                    _ensure_immutable(packet_dir / "prompt.private.md", prompt.encode("utf-8"))
                    _ensure_immutable(packet_dir / "schema.json", _canonical_bytes(schema))
                    prepared_support[(packet_index, orientation)] = {
                        "dir": packet_dir,
                        "prompt": prompt,
                        "schema": schema,
                        "batch_size": len(packet_rows),
                    }
                    attempt_path = packet_dir / "attempt.json"
                    outcome_path = packet_dir / "outcome.json"
                    raw_path = packet_dir / "raw-output.private.json"
                    sidecar_path = packet_dir / "sidecar.json"
                    clean_pre_turn_resume = bool(
                        attempt_path.exists()
                        and not any(
                            path.exists() for path in (outcome_path, raw_path, sidecar_path)
                        )
                    )
                    if attempt_path.exists() and not clean_pre_turn_resume:
                        if not outcome_path.is_file():
                            outcome = {
                                "packet_index": packet_index,
                                "orientation": orientation,
                                "status": "ambiguous_interrupted_no_retry",
                                "status_ok": False,
                                "failure_class": "prior_attempt_has_no_terminal_outcome",
                                "usage": None,
                                "usage_complete": False,
                            }
                            _write_immutable(outcome_path, _canonical_bytes(outcome))
                        _, outcome = _read_json(outcome_path, purpose="support outcome")
                        if outcome.get("status") == "validated":
                            _, output = _read_json(raw_path, purpose="support raw output")
                            orientations[orientation] = validate_support_output(output, variant)
                            _sidecar, usage = validate_completed_managed_sidecar(
                                sidecar_path=sidecar_path,
                                raw_output_path=raw_path,
                                model=SUPPORT_MODEL,
                                effort=SUPPORT_EFFORT,
                                thread_mode="new_thread",
                                batch_size=len(packet_rows),
                                prompt=prompt,
                                output_schema=schema,
                                base_instructions=support_base_instructions,
                                instruction_contract=instruction_contract,
                                execution_lineage=execution_lineage,
                            )
                            if usage != outcome.get("usage"):
                                raise ValueError(
                                    "support outcome usage differs from managed sidecar"
                                )
                            if (
                                _sha256_file(raw_path) != outcome.get("raw_output_sha256")
                                or _sha256_file(sidecar_path)
                                != outcome.get("sidecar_sha256")
                            ):
                                raise ValueError("support checkpoint artifact hash drift")
                        packet_outcomes.append(outcome)
                        continue
                    if not attempt_path.exists():
                        unexpected = [
                            path
                            for path in (outcome_path, raw_path, sidecar_path)
                            if path.exists()
                        ]
                        if unexpected:
                            raise ValueError(
                                "support artifacts exist without an immutable attempt"
                            )
                        await _wait_for_capacity_before_attempt(client)
                        attempt = {
                            "packet_index": packet_index,
                            "orientation": orientation,
                            "segment_ids": packet_ids,
                            "model": SUPPORT_MODEL,
                            "reasoning_effort": SUPPORT_EFFORT,
                            "prompt_sha256": sha256_text(prompt),
                            "schema_sha256": _sha256_bytes(_canonical_bytes(schema)),
                            "retry_ordinal": 0,
                        }
                        _write_immutable(attempt_path, _canonical_bytes(attempt))
                    else:
                        await _wait_for_capacity_before_attempt(client)
                    try:
                        result = await client.run_ephemeral_structured_turn(
                            model=SUPPORT_MODEL,
                            effort=SUPPORT_EFFORT,
                            base_instructions=support_base_instructions,
                            prompt=prompt,
                            output_schema=schema,
                            cwd=Path.cwd(),
                            sidecar_path=sidecar_path,
                            output_path=raw_path,
                            batch_size=len(packet_rows),
                            thread_mode="new_thread",
                            timeout_seconds=timeout_seconds,
                        )
                        if not result.status_ok or not isinstance(result.output, dict):
                            raise ValueError(result.error_class or f"turn_{result.status}")
                        if not raw_path.exists():
                            _write_immutable(raw_path, _canonical_bytes(result.output))
                        orientations[orientation] = validate_support_output(result.output, variant)
                        _sidecar, usage = validate_completed_managed_sidecar(
                            sidecar_path=sidecar_path,
                            raw_output_path=raw_path,
                            model=SUPPORT_MODEL,
                            effort=SUPPORT_EFFORT,
                            thread_mode="new_thread",
                            batch_size=len(packet_rows),
                            prompt=prompt,
                            output_schema=schema,
                            base_instructions=support_base_instructions,
                            instruction_contract=instruction_contract,
                            execution_lineage=execution_lineage,
                        )
                        usage_complete = True
                        usage_error = None
                        outcome = {
                            "packet_index": packet_index,
                            "orientation": orientation,
                            "status": "validated" if usage_complete else "validated_usage_unknown",
                            "status_ok": usage_complete,
                            "failure_class": usage_error,
                            "raw_output_path": str(raw_path),
                            "raw_output_sha256": _sha256_file(raw_path),
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
                            "packet_index": packet_index,
                            "orientation": orientation,
                            "status": "failed_no_retry",
                            "status_ok": False,
                            "failure_class": type(exc).__name__,
                            "usage_error": usage_error,
                            "usage": usage,
                            "usage_complete": usage_complete,
                        }
                    _write_immutable(outcome_path, _canonical_bytes(outcome))
                    packet_outcomes.append(outcome)
                support_outcomes.extend(packet_outcomes)
                if set(orientations) == {"ab", "ba"}:
                    support_packet_outputs.append(
                        {
                            "packet_index": packet_index,
                            "witness_mapping": witness_mapping,
                            "normalized_ab": orientations["ab"],
                            "normalized_ba": orientations["ba"],
                        }
                    )

    extraction_outcome_list = [
        extraction_batch_outcomes[f"batch-{index:04d}"]
        for index in range(len(extraction_batches))
    ]
    extraction_accounting = all(
        outcome.get("usage_complete") is True for outcome in extraction_outcome_list
    )
    support_expected_calls = len(support_packets) * 2 if extraction_complete else 0
    support_accounting = (
        len(support_outcomes) == support_expected_calls
        and all(outcome.get("usage_complete") is True for outcome in support_outcomes)
    )
    all_calls_valid = extraction_complete and support_accounting and all(
        outcome.get("status") == "validated" for outcome in support_outcomes
    )
    selected_rows = []
    reference_index_path = root / "reference-index.private.json"
    underpowered = {}
    if all_calls_valid:
        reference_cases = _consensus_reference_cases(
            extraction_rows, packet_outputs=support_packet_outputs, seed=seed
        )
        case_records = []
        for case in reference_cases:
            case_path = root / "reference-cases" / f"{case['segment_id']}.private.json"
            _ensure_immutable(case_path, _canonical_bytes(case))
            case_records.append(
                {
                    **case,
                    "reference_case_path": str(case_path),
                    "reference_case_sha256": _sha256_file(case_path),
                }
            )
        reference_index = {
            "schema_version": REFERENCE_INDEX_VERSION,
            "covenant_sha256": inputs["covenant_sha256"],
            "model": REFERENCE_MODEL,
            "reasoning_effort": REFERENCE_EFFORT,
            "support_model": SUPPORT_MODEL,
            "support_reasoning_effort": SUPPORT_EFFORT,
            "case_count": len(case_records),
            "cases": [
                {
                    key: row[key]
                    for key in (
                        "segment_id",
                        "episode_id",
                        "transcript_id",
                        "text_sha256",
                        "reservoir",
                        "raw_event_count",
                        "reference_event_count",
                        "coverage_complete",
                        "clean_no_signal",
                        "support_abstained",
                        "selection_eligible",
                        "reference_case_path",
                        "reference_case_sha256",
                    )
                }
                for row in case_records
            ],
        }
        _ensure_immutable(reference_index_path, _canonical_bytes(reference_index))
        eligible = [row for row in case_records if row["selection_eligible"]]
        paired_by_stratum: dict[str, list[dict[str, Any]]] = defaultdict(list)
        clean_candidates = []
        ranking_by_id = {row["segment_id"]: row for row in ordered}
        for row in eligible:
            frozen = ranking_by_id[row["segment_id"]]
            count = int(row["reference_event_count"])
            if row["reservoir"] == "paired_quality_reservoir":
                paired_by_stratum[_stratum(count)].append({**row, **{"selection_hash": frozen["selection_hash"]}})
            elif (
                row["reservoir"] == "terminal_position_no_signal_candidates"
                and row["clean_no_signal"]
                and count == 0
            ):
                clean_candidates.append({**row, **{"selection_hash": frozen["selection_hash"]}})
        for values in paired_by_stratum.values():
            values.sort(key=lambda row: (row["selection_hash"], row["segment_id"]))
        clean_candidates.sort(key=lambda row: (row["selection_hash"], row["segment_id"]))
        for stratum in ("no_signal", "low", "medium", "dense"):
            available = len(paired_by_stratum.get(stratum) or [])
            if available < paired_per_stratum:
                underpowered[f"paired_quality:{stratum}"] = {
                    "required": paired_per_stratum,
                    "available": available,
                }
        if len(clean_candidates) < clean_no_signal_count:
            underpowered["clean_no_signal_power:no_signal"] = {
                "required": clean_no_signal_count,
                "available": len(clean_candidates),
            }
        if not underpowered:
            selection_order = 0
            for stratum in ("no_signal", "low", "medium", "dense"):
                for row in paired_by_stratum[stratum][:paired_per_stratum]:
                    selected_rows.append(
                        {
                            "selection_order": selection_order,
                            "segment_id": row["segment_id"],
                            "episode_id": row["episode_id"],
                            "transcript_id": row["transcript_id"],
                            "text_sha256": row["text_sha256"],
                            "evaluation_set": "paired_quality",
                            "stratum": stratum,
                            "reference_event_count": row["reference_event_count"],
                            "reference_case_sha256": row["reference_case_sha256"],
                            "reference_clean_no_signal": bool(row["clean_no_signal"]),
                        }
                    )
                    selection_order += 1
            for row in clean_candidates[:clean_no_signal_count]:
                selected_rows.append(
                    {
                        "selection_order": selection_order,
                        "segment_id": row["segment_id"],
                        "episode_id": row["episode_id"],
                        "transcript_id": row["transcript_id"],
                        "text_sha256": row["text_sha256"],
                        "evaluation_set": "clean_no_signal_power",
                        "stratum": "no_signal",
                        "reference_event_count": 0,
                        "reference_case_sha256": row["reference_case_sha256"],
                        "reference_clean_no_signal": True,
                    }
                )
                selection_order += 1

    all_outcomes = [*extraction_outcome_list, *support_outcomes]
    accounting_complete = extraction_accounting and support_accounting
    usage = _aggregate_usage(all_outcomes)
    selection_path = root / "stratified-selection.json"
    selection_sha256 = None
    if all_calls_valid and not underpowered and selected_rows and accounting_complete:
        counts = Counter(
            f"{row['evaluation_set']}:{row['stratum']}" for row in selected_rows
        )
        selection = {
            "schema_version": HOLDOUT_STRATIFIED_SELECTION_VERSION,
            "selection_status": "frozen_stratified_selection",
            "selection_frozen": True,
            "covenant_sha256": inputs["covenant_sha256"],
            "selection_basis": "llm_reference_only_pre_candidate_pre_baseline",
            "candidate_outputs_observed": False,
            "baseline_outputs_observed": False,
            "adaptive_top_up": False,
            "reference_artifact_sha256": _sha256_file(reference_index_path),
            "reference_model": REFERENCE_MODEL,
            "reference_reasoning_effort": REFERENCE_EFFORT,
            "support_model": SUPPORT_MODEL,
            "support_reasoning_effort": SUPPORT_EFFORT,
            "accounting_complete": True,
            "usage": usage,
            "stratum_thresholds": {
                "no_signal": "0",
                "low": "1-4",
                "medium": "5-15",
                "dense": f"16-{event_cap}",
            },
            "stratum_counts": dict(sorted(counts.items())),
            "segments": selected_rows,
        }
        _ensure_immutable(selection_path, _canonical_bytes(selection))
        selection_sha256 = _sha256_file(selection_path)
    if conn.total_changes != before_changes:
        raise RuntimeError("holdout reference stratifier mutated the production database")
    ok = bool(selection_sha256 and accounting_complete and all_calls_valid and not underpowered)
    leaf_bindings = []
    for outcome in extraction_outcome_list:
        if outcome.get("status") != "validated":
            continue
        item = prepared_batches[str(outcome["batch_id"])]
        leaf_bindings.append(
            build_holdout_leaf_binding(
                sidecar_path=item["dir"] / "sidecar.json",
                prompt_path=item["dir"] / "prompt.private.md",
                output_schema_path=item["dir"] / "schema.json",
                base_instructions_path=extraction_base_path,
                raw_output_path=item["dir"] / "raw-output.private.json",
                model=REFERENCE_MODEL,
                effort=REFERENCE_EFFORT,
                thread_mode="new_thread",
                batch_size=len(item["rows"]),
                prompt=item["prompt"],
                output_schema=item["schema"],
                base_instructions=base_instructions,
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
        )
    for outcome in support_outcomes:
        if outcome.get("status") != "validated":
            continue
        key = (int(outcome["packet_index"]), str(outcome["orientation"]))
        item = prepared_support[key]
        leaf_bindings.append(
            build_holdout_leaf_binding(
                sidecar_path=item["dir"] / "sidecar.json",
                prompt_path=item["dir"] / "prompt.private.md",
                output_schema_path=item["dir"] / "schema.json",
                base_instructions_path=support_base_path,
                raw_output_path=item["dir"] / "raw-output.private.json",
                model=SUPPORT_MODEL,
                effort=SUPPORT_EFFORT,
                thread_mode="new_thread",
                batch_size=int(item["batch_size"]),
                prompt=item["prompt"],
                output_schema=item["schema"],
                base_instructions=support_base_instructions,
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
        )
    report = {
        "schema_version": STRATIFIER_REPORT_VERSION,
        "ok": ok,
        "status": (
            "frozen_selection_ready"
            if ok
            else "underpowered_reference_strata"
            if underpowered
            else "reference_calls_failed_or_accounting_incomplete"
        ),
        "covenant_sha256": inputs["covenant_sha256"],
        "model": REFERENCE_MODEL,
        "reasoning_effort": REFERENCE_EFFORT,
        "support_model": SUPPORT_MODEL,
        "support_reasoning_effort": SUPPORT_EFFORT,
        "persistent_client_sessions_this_run": 1,
        "retry_count": 0,
        "requested_extractions": len(ordered),
        "requested_extraction_calls": len(extraction_batches),
        "extraction_batch_sizes": [len(batch) for batch in extraction_batches],
        "validated_extractions": sum(
            len(outcome.get("segment_ids") or [])
            for outcome in extraction_outcome_list
            if outcome.get("status") == "validated"
        ),
        "validated_extraction_calls": sum(
            outcome.get("status") == "validated" for outcome in extraction_outcome_list
        ),
        "requested_support_calls": support_expected_calls,
        "validated_support_calls": sum(
            outcome.get("status") == "validated" for outcome in support_outcomes
        ),
        "accounting_complete": accounting_complete,
        "usage": usage if accounting_complete else None,
        "measured_partial_usage": usage,
        "underpowered": underpowered,
        "selection_path": str(selection_path) if selection_path.exists() else None,
        "selection_sha256": selection_sha256,
        "reference_index_path": str(reference_index_path) if reference_index_path.exists() else None,
        "reference_index_sha256": (
            _sha256_file(reference_index_path) if reference_index_path.exists() else None
        ),
        "candidate_outputs_observed": False,
        "baseline_outputs_observed": False,
        "adaptive_top_up": False,
        "instruction_contract": instruction_contract,
        "execution_lineage": execution_lineage,
        "leaf_bindings": leaf_bindings,
        "label_pack_contract": label_pack_contract,
        "production_database_mutation": False,
        "production_promotion": False,
    }
    _validate_terminal_stratifier_lineage(
        root,
        report=report,
        frozen_plan=plan,
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    _write_immutable(terminal_report_path, _canonical_bytes(report))
    return report


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
        description="Build a frozen LLM-only reference-stratified holdout selection."
    )
    parser.add_argument("--database", default=str(db_path()))
    parser.add_argument("--covenant", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--execution-dir", required=True)
    parser.add_argument("--event-cap", type=int, default=DEFAULT_EVENT_CAP)
    parser.add_argument("--packet-size", type=int, default=DEFAULT_PACKET_SIZE)
    parser.add_argument(
        "--extraction-batch-size", type=int, default=DEFAULT_EXTRACTION_BATCH_SIZE
    )
    parser.add_argument(
        "--support-max-witnesses", type=int, default=DEFAULT_SUPPORT_MAX_WITNESSES
    )
    parser.add_argument(
        "--support-max-packet-bytes", type=int, default=DEFAULT_SUPPORT_MAX_PACKET_BYTES
    )
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    parser.add_argument("--seed", default="app-server-holdout-reference-stratification-v1")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = _read_only_connection(args.database)
        result = asyncio.run(
            run_holdout_reference_stratifier(
                conn,
                covenant_path=args.covenant,
                output_dir=args.output_dir,
                execution_dir=args.execution_dir,
                event_cap=args.event_cap,
                packet_size=args.packet_size,
                extraction_batch_size=args.extraction_batch_size,
                support_max_witnesses=args.support_max_witnesses,
                support_max_packet_bytes=args.support_max_packet_bytes,
                seed=args.seed,
                timeout_seconds=args.timeout_seconds,
            )
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "error_class": type(exc).__name__}, sort_keys=True), file=sys.stderr)
        return 2
    finally:
        if conn is not None:
            conn.close()
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0 if result.get("ok") else 3


if __name__ == "__main__":
    raise SystemExit(main())
