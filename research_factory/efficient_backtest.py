from __future__ import annotations

import json
import math
import os
import re
import signal
import subprocess
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

from .labels import ValidationError, load_label_pack, validate_label_output
from .paths import corpus_dir, exports_dir
from .util import now_iso, sha256_text, write_text_atomic


COMPACT_SCHEMA_VERSION = "ai_discourse_v3_1_compact_episode_batch_v1"
SPARSE_COMPACT_SCHEMA_VERSION = "ai_discourse_v3_1_sparse_episode_batch_v1"
OFFSET_SPARSE_COMPACT_SCHEMA_VERSION = "ai_discourse_v3_1_offset_sparse_episode_batch_v1"
EVIDENCE_SPARSE_COMPACT_SCHEMA_VERSION = "ai_discourse_v3_1_evidence_sparse_episode_batch_v1"
FLAT_LEDGER_SCHEMA_VERSION = "ai_discourse_v3_1_flat_ledger_v1"
FULL_SCHEMA_BATCH_VERSION = "ai_discourse_v3_1_full_episode_batch_v2"
WINDOWED_EVENT_CORE_SCHEMA_VERSION = "ai_discourse_v3_1_windowed_event_core_v3"
WINDOWED_ENRICHMENT_SCHEMA_VERSION = "ai_discourse_v3_1_windowed_metadata_enrichment_v2"
WINDOWED_SEMANTIC_JUDGE_SCHEMA_VERSION = "ai_discourse_v3_1_windowed_semantic_judge_v1"
WINDOWED_HOLDOUT_MANIFEST_VERSION = "ai_discourse_v3_1_windowed_source_holdout_v1"
DEFAULT_WINDOWED_GUIDELINES_PATH = (
    Path(__file__).resolve().parent / "prompt_guidelines" / "windowed_event_core_v1.json"
)
SPARSE_CHUNK_MAX_SEGMENTS = 10
DEFAULT_CANDIDATE_REPRESENTATION = "sparse_compact"
CANDIDATE_REPRESENTATIONS = {"sparse_compact", "compact", "offset_sparse_compact", "evidence_sparse_compact", "flat_ledger", "flat_ledger_full", "full_schema"}
TEXT_CHECK_SAMPLE_LIMIT = 300
MAX_VALIDATION_SEGMENT_TEXT_BYTES = 200_000
DEFAULT_SMOKE_TIMEOUT_SECONDS = 900
DEFAULT_QUALITY_GATE = {
    "max_total_token_ratio": 0.25,
    "max_runtime_ratio": 0.25,
    "min_status_accuracy": 0.95,
    "min_event_precision": 0.9,
    "min_event_recall": 0.9,
}

WINDOWED_EVENT_TYPES = [
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
]
WINDOWED_CLAIM_TYPES = [
    "descriptive",
    "prediction",
    "causal",
    "comparative",
    "product_market",
    "terminology",
    "uncertainty",
    "counterclaim",
    "not_applicable",
]
WINDOWED_ENRICHMENT_KEY_MAP = {
    "i": "id",
    "terms": "surface_terms",
    "frames": "frames",
    "ex": "exclusion_flags",
    "q": "quality_flags",
}


def _collect_manifest_holdout_ids(value: Any) -> tuple[set[str], set[str]]:
    segment_ids = set()
    episode_ids = set()

    def visit(item: Any, key: str | None = None) -> None:
        if isinstance(item, dict):
            for child_key, child_value in item.items():
                visit(child_value, child_key)
            return
        if isinstance(item, list):
            for child in item:
                visit(child, key)
            return
        if not isinstance(item, str):
            return
        if key in {"segment_id", "segment_ids"}:
            segment_ids.add(item)
        elif key in {"episode_id", "episode_ids"}:
            episode_ids.add(item)

    visit(value)
    return segment_ids, episode_ids


def export_windowed_source_holdout_manifest(
    conn,
    *,
    output_path: str | Path,
    exclude_roots: list[str | Path] | None = None,
    source_limit: int = 0,
    min_events: int = 14,
    max_events: int = 25,
    seed: str = "windowed-final-holdout-v1",
) -> dict[str, Any]:
    if source_limit < 0:
        raise ValueError("source limit must be non-negative")
    if min_events < 0 or max_events < min_events:
        raise ValueError("event bounds are invalid")
    excluded_segments = set()
    excluded_episodes = set()
    scanned_manifests = 0
    for root in exclude_roots or []:
        root_path = Path(root).expanduser().resolve()
        paths = [root_path] if root_path.is_file() else sorted(root_path.rglob("*manifest*.json"))
        for path in paths:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            segment_ids, episode_ids = _collect_manifest_holdout_ids(payload)
            excluded_segments.update(segment_ids)
            excluded_episodes.update(episode_ids)
            scanned_manifests += 1
    rows = conn.execute(
        """
        SELECT l.segment_id,
               sg.episode_id,
               sg.source_id,
               so.name AS source_name,
               json_array_length(json_extract(l.output_json, '$.discourse_events')) AS event_count
        FROM labels l
        JOIN segments sg ON sg.id = l.segment_id
        JOIN sources so ON so.id = sg.source_id
        WHERE l.label_pack = 'ai_discourse_v3_1'
          AND l.model = 'gpt-5.5'
          AND l.status IN ('ready', 'completed')
          AND json_array_length(json_extract(l.output_json, '$.discourse_events')) BETWEEN ? AND ?
        """,
        (min_events, max_events),
    ).fetchall()
    by_source: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        if row["segment_id"] in excluded_segments or row["episode_id"] in excluded_episodes:
            continue
        by_source[row["source_id"]].append(row)
    selected = []
    for source_id, candidates in sorted(by_source.items()):
        candidates.sort(key=lambda row: sha256_text(f"{seed}|{source_id}|{row['segment_id']}"))
        selected.append(candidates[0])
    selected.sort(key=lambda row: sha256_text(f"{seed}|source|{row['source_id']}"))
    if source_limit:
        selected = selected[:source_limit]
    chunks = [
        {
            "chunk_id": row["segment_id"],
            "episode_id": row["episode_id"],
            "expected_coded_segments": 1,
            "expected_discourse_events": row["event_count"],
            "label_count": 1,
            "segment_count": 1,
            "segment_ids": [row["segment_id"]],
            "source_name": row["source_name"],
        }
        for row in selected
    ]
    output_file = Path(output_path).expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(
        output_file,
        json.dumps(
            {
                "schema_version": WINDOWED_HOLDOUT_MANIFEST_VERSION,
                "privacy": "private_analysis_only",
                "chunks": chunks,
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    event_counts = [int(row["event_count"]) for row in selected]
    return {
        "ok": bool(selected),
        "path": str(output_file),
        "privacy": "sanitized_no_prompt_or_transcript_text",
        "scanned_manifests": scanned_manifests,
        "excluded_segment_ids": len(excluded_segments),
        "excluded_episode_ids": len(excluded_episodes),
        "selected_segments": len(selected),
        "selected_sources": len({row["source_id"] for row in selected}),
        "golden_events": sum(event_counts),
        "min_events": min(event_counts) if event_counts else None,
        "max_events": max(event_counts) if event_counts else None,
        "seed": seed,
    }


EVENT_KEY_MAP = {
    "t": "event_type",
    "sub": "event_subtype",
    "a": "actor",
    "sp": "speaker_context",
    "ra": "reported_actor",
    "src": "source_context",
    "tar": "target",
    "terms": "surface_terms",
    "frames": "frames",
    "models": "model_names",
    "products": "product_names",
    "orgs": "organizations",
    "people": "people",
    "stance": "stance",
    "claim": "claim_text",
    "ct": "claim_type",
    "cert": "certainty",
    "h": "temporal_horizon",
    "mech": "causal_mechanism",
    "counter": "counterclaim",
    "m": "metric",
    "why": "signal_reason",
    "exclude": "exclusion_flags",
    "q": "quality_flags",
    "ev": "evidence",
    "s": "evidence_start",
    "e": "evidence_end",
    "cf": "confidence",
    "notes": "audit_notes",
}

LABEL_KEY_MAP = {
    "id": "segment_id",
    "st": "extraction_status",
    "sq": "segment_quality",
    "sc": "segment_source_context",
    "evs": "discourse_events",
    "cc": "concept_candidates",
    "rc": "rejected_candidates",
    "ns": "no_signal_reason",
    "cf": "overall_confidence",
    "nr": "needs_review",
    "rr": "review_reason",
}

SEGMENT_QUALITY_KEY_MAP = {
    "at": "artifact_type",
    "br": "boilerplate_risk",
    "wc": "substantive_word_count",
    "tp": "transcript_preparation_id",
}
SEGMENT_QUALITY_DEFAULTS = {"at": "dialogue_transcript", "br": "low", "tp": None}

SOURCE_CONTEXT_KEY_MAP = {
    "k": "kind",
    "cf": "confidence",
    "r": "rationale",
}
SOURCE_CONTEXT_DEFAULTS = {"k": "substantive_dialogue", "cf": 0.9, "r": ""}

ACTOR_KEY_MAP = {
    "n": "name",
    "t": "actor_type",
    "af": "affiliation",
    "r": "role",
}
ACTOR_DEFAULTS = {"n": "unknown", "t": "unknown", "af": None, "r": None}

SPEAKER_KEY_MAP = {
    "n": "name",
    "r": "role",
    "af": "affiliation",
    "cf": "confidence",
}
SPEAKER_DEFAULTS = {"n": "unknown", "r": "unknown", "af": None, "cf": 0.5}

REPORTED_ACTOR_KEY_MAP = {
    "n": "name",
    "t": "actor_type",
    "af": "affiliation",
    "cf": "confidence",
}
REPORTED_ACTOR_DEFAULTS = {"n": "none", "t": "none", "af": None, "cf": 0}

TARGET_KEY_MAP = {
    "raw": "raw_target",
    "cand": "candidate_concept",
    "canon": "canonical_concept",
    "cf": "concept_confidence",
}
TARGET_DEFAULTS = {"raw": "", "cand": "", "canon": None, "cf": 0.0}

METRIC_KEY_MAP = {
    "v": "value",
    "u": "unit",
    "cmp": "comparator",
    "dir": "direction",
    "raw": "raw_text",
}
METRIC_DEFAULTS = {"v": None, "u": None, "cmp": None, "dir": "not_applicable", "raw": None}

CONCEPT_CANDIDATE_KEY_MAP = {
    "c": "candidate",
    "terms": "surface_terms",
    "why": "rationale",
    "score": "usefulness_score",
    "ev": "evidence",
    "s": "evidence_start",
    "e": "evidence_end",
    "cf": "confidence",
}
CONCEPT_CANDIDATE_DEFAULTS = {"terms": []}

REJECTED_CANDIDATE_KEY_MAP = {
    "txt": "text",
    "why": "reason",
    "k": "source_context_kind",
}

SPARSE_LABEL_DEFAULTS = {
    "st": "coded",
    "evs": [],
    "cc": [],
    "rc": [],
    "ns": None,
    "nr": False,
    "rr": None,
}

SPARSE_EVENT_DEFAULTS = {
    "sub": "",
    "terms": [],
    "frames": [],
    "models": [],
    "products": [],
    "orgs": [],
    "people": [],
    "stance": "not_applicable",
    "ct": "descriptive",
    "cert": "medium",
    "h": "unspecified",
    "mech": "",
    "counter": "",
    "exclude": [],
    "q": [],
    "notes": "",
}


def run_efficiency_backtest(
    conn,
    *,
    label_pack: str,
    model: str,
    episode_limit: int | None = None,
    per_source_limit: int | None = None,
    seed: str = "efficiency-backtest-v1",
    output: str | Path | None = None,
    candidate_output_dir: str | Path | None = None,
    export_candidate_prompts: str | Path | None = None,
    candidate_chunk_size: int = SPARSE_CHUNK_MAX_SEGMENTS,
    candidate_representation: str = DEFAULT_CANDIDATE_REPRESENTATION,
) -> dict[str, Any]:
    if label_pack != "ai_discourse_v3_1":
        raise ValueError("efficiency-backtest currently supports ai_discourse_v3_1")
    candidate_representation = _normalize_candidate_representation(candidate_representation)
    episode_ids = _select_episode_ids(
        conn,
        label_pack=label_pack,
        model=model,
        episode_limit=episode_limit,
        per_source_limit=per_source_limit,
        seed=seed,
    )
    if not episode_ids:
        raise ValueError("No golden episodes found for requested label pack/model")
    labels = _golden_labels_for_episodes(conn, episode_ids=episode_ids, label_pack=label_pack, model=model)
    if not labels:
        raise ValueError("Selected episodes have no ready golden labels")

    labels_by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in labels:
        row["output"] = json.loads(row["output_json"])
        labels_by_episode[row["episode_id"]].append(row)
    for rows in labels_by_episode.values():
        rows.sort(key=lambda item: (int(item["segment_index"] or 0), item["segment_id"]))

    runtime_stats = _runtime_stats(conn, labels=labels, labels_by_episode=labels_by_episode, label_pack=label_pack, model=model)
    historical_cost = _historical_artifact_cost(labels)
    current_cost = _current_segment_path_cost(conn, labels_by_episode=labels_by_episode, label_pack=label_pack, model=model)
    batch_full_cost = _episode_batch_cost(conn, labels_by_episode=labels_by_episode, label_pack=label_pack, compact=False)
    batch_compact_cost, compact_quality = _episode_batch_compact_cost(
        conn,
        labels_by_episode=labels_by_episode,
        label_pack=label_pack,
    )
    batch_sparse_cost, sparse_quality = _episode_batch_sparse_compact_cost(
        conn,
        labels_by_episode=labels_by_episode,
        label_pack=label_pack,
    )
    chunk_sparse_cost = _episode_chunk_cost(
        conn,
        labels_by_episode=labels_by_episode,
        label_pack=label_pack,
        chunk_size=candidate_chunk_size,
        representation="sparse_compact",
    )
    chunk_candidate_cost = _episode_chunk_cost(
        conn,
        labels_by_episode=labels_by_episode,
        label_pack=label_pack,
        chunk_size=candidate_chunk_size,
        representation=candidate_representation,
    )
    _attach_runtime_estimates(
        current_cost=current_cost,
        candidate_costs=[historical_cost, batch_full_cost, batch_compact_cost, batch_sparse_cost, chunk_sparse_cost, chunk_candidate_cost],
        runtime_stats=runtime_stats,
    )
    golden_quality = _golden_quality_proxy(conn, labels)
    offset_sparse_quality = None
    if candidate_representation == "offset_sparse_compact":
        offset_sparse_quality = _representation_quality(
            conn,
            labels_by_episode=labels_by_episode,
            label_pack=label_pack,
            representation=OFFSET_SPARSE_COMPACT_SCHEMA_VERSION,
            encode=offset_sparse_compact_label,
            decode=expand_sparse_compact_label,
            hydrate_from_reference=True,
        )
    evidence_sparse_quality = None
    if candidate_representation == "evidence_sparse_compact":
        evidence_sparse_quality = _representation_quality(
            conn,
            labels_by_episode=labels_by_episode,
            label_pack=label_pack,
            representation=EVIDENCE_SPARSE_COMPACT_SCHEMA_VERSION,
            encode=evidence_sparse_compact_label,
            decode=expand_sparse_compact_label,
            hydrate_offsets_from_reference=True,
        )
    flat_ledger_quality = None
    if candidate_representation in {"flat_ledger", "flat_ledger_full"}:
        flat_ledger_quality = _representation_quality(
            conn,
            labels_by_episode=labels_by_episode,
            label_pack=label_pack,
            representation=FLAT_LEDGER_SCHEMA_VERSION,
            encode=flat_ledger_label,
            decode=expand_flat_ledger_label,
            hydrate_offsets_from_reference=True,
        )
    candidate_representation_quality = {
        "offset_sparse_compact": offset_sparse_quality,
        "evidence_sparse_compact": evidence_sparse_quality,
        "flat_ledger": flat_ledger_quality,
        "flat_ledger_full": flat_ledger_quality,
        "sparse_compact": sparse_quality,
        "compact": compact_quality,
        "full_schema": compact_quality,
    }[candidate_representation]
    candidate_comparison = None
    if candidate_output_dir:
        candidate_comparison = compare_candidate_episode_outputs(
            conn,
            labels_by_episode=labels_by_episode,
            candidate_output_dir=Path(candidate_output_dir).expanduser(),
            label_pack=label_pack,
        )
    prompt_export = None
    if export_candidate_prompts:
        prompt_export = export_sparse_candidate_prompts(
            conn,
            labels_by_episode=labels_by_episode,
            label_pack=label_pack,
            output_dir=Path(export_candidate_prompts).expanduser(),
            chunk_size=candidate_chunk_size,
            representation=candidate_representation,
        )

    summary = {
        "episode_count": len(labels_by_episode),
        "label_count": len(labels),
        "source_count": len({row["source_name"] for row in labels}),
        "discourse_event_count": sum(len(row["output"].get("discourse_events") or []) for row in labels),
        "candidate": chunk_candidate_cost["name"],
        "candidate_representation": candidate_representation,
        "estimated_current_vs_candidate_total_token_ratio": _ratio(
            chunk_candidate_cost["estimated_total_tokens"],
            current_cost["estimated_total_tokens"],
        ),
        "estimated_current_vs_candidate_input_token_ratio": _ratio(
            chunk_candidate_cost["estimated_input_tokens"],
            current_cost["estimated_input_tokens"],
        ),
        "estimated_current_vs_candidate_request_ratio": _ratio(
            chunk_candidate_cost["request_count"],
            current_cost["request_count"],
        ),
        "estimated_current_vs_candidate_runtime_ratio": _ratio(
            chunk_candidate_cost.get("estimated_runtime_seconds"),
            current_cost.get("estimated_runtime_seconds"),
        ),
        "estimated_current_vs_full_episode_sparse_total_token_ratio": _ratio(
            batch_sparse_cost["estimated_total_tokens"],
            current_cost["estimated_total_tokens"],
        ),
        "estimated_current_vs_full_episode_sparse_input_token_ratio": _ratio(
            batch_sparse_cost["estimated_input_tokens"],
            current_cost["estimated_input_tokens"],
        ),
        "estimated_current_vs_full_episode_sparse_request_ratio": _ratio(
            batch_sparse_cost["request_count"],
            current_cost["request_count"],
        ),
        "estimated_current_vs_full_episode_sparse_runtime_ratio": _ratio(
            batch_sparse_cost.get("estimated_runtime_seconds"),
            current_cost.get("estimated_runtime_seconds"),
        ),
        "estimated_current_vs_dense_compact_total_token_ratio": _ratio(
            batch_compact_cost["estimated_total_tokens"],
            current_cost["estimated_total_tokens"],
        ),
        "estimated_current_vs_dense_compact_input_token_ratio": _ratio(
            batch_compact_cost["estimated_input_tokens"],
            current_cost["estimated_input_tokens"],
        ),
        "estimated_current_vs_dense_compact_request_ratio": _ratio(
            batch_compact_cost["request_count"],
            current_cost["request_count"],
        ),
        "estimated_current_vs_dense_compact_runtime_ratio": _ratio(
            batch_compact_cost.get("estimated_runtime_seconds"),
            current_cost.get("estimated_runtime_seconds"),
        ),
        "sparse_compact_roundtrip_exact_labels": sparse_quality["exact_label_roundtrip_rate"],
        "dense_compact_roundtrip_exact_labels": compact_quality["exact_label_roundtrip_rate"],
        "candidate_output_compared": candidate_comparison is not None,
    }
    quality_gate = _quality_gate(
        current_cost=current_cost,
        candidate_cost=chunk_candidate_cost,
        compact_quality=candidate_representation_quality,
        candidate_comparison=candidate_comparison,
    )
    summary["candidate_quality_gate_passed"] = quality_gate["passed"]
    summary["candidate_quality_gate_status"] = quality_gate["status"]
    payload = {
        "ok": True,
        "generated_at": now_iso(),
        "privacy": "sanitized_no_transcript_text",
        "label_pack": label_pack,
        "model": model,
        "seed": seed,
        "selection": {
            "episode_limit": episode_limit,
            "per_source_limit": per_source_limit,
            "episode_count": len(labels_by_episode),
            "label_count": len(labels),
            "source_count": len({row["source_name"] for row in labels}),
            "sources": _source_counts(labels),
        },
        "summary": summary,
        "runtime_stats": runtime_stats,
        "cost_estimates": {
            "historical_linked_artifacts": historical_cost,
            "current_segment_context_path": current_cost,
            "episode_batch_full_schema_v1": batch_full_cost,
            "episode_batch_compact_v1": batch_compact_cost,
            "episode_batch_sparse_compact_v1": batch_sparse_cost,
            "episode_chunk_sparse_compact_v1": chunk_sparse_cost,
            chunk_candidate_cost["name"]: chunk_candidate_cost,
        },
        "quality_backtest": {
            "golden_self_check": golden_quality,
            "compact_representation": compact_quality,
            "dense_compact_representation": compact_quality,
            "sparse_compact_representation": sparse_quality,
            "offset_sparse_compact_representation": offset_sparse_quality,
            "evidence_sparse_compact_representation": evidence_sparse_quality,
            "flat_ledger_representation": flat_ledger_quality,
            "candidate_output_comparison": candidate_comparison,
            "candidate_quality_gate": quality_gate,
        },
        "candidate_prompt_export": prompt_export,
        "interpretation": _interpretation(
            current_cost=current_cost,
            compact_cost=chunk_candidate_cost,
            compact_quality=candidate_representation_quality,
        ),
    }
    path = Path(output).expanduser().resolve() if output else exports_dir() / "efficiency-backtest.json"
    write_text_atomic(path, json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    payload["path"] = str(path)
    return payload


def export_distillation_dataset(
    conn,
    *,
    output_dir: str | Path,
    label_pack: str = "ai_discourse_v3_1",
    model: str = "gpt-5.5",
    seed: str = "distillation-source-split-v1",
    compact_context: bool = True,
    task: str = "full_label",
) -> dict[str, Any]:
    if task not in {"full_label", "event_detection"}:
        raise ValueError("distillation task must be full_label or event_detection")
    output_root = Path(output_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    episode_ids = _select_episode_ids(conn, label_pack=label_pack, model=model, episode_limit=None, per_source_limit=None, seed=seed)
    rows = _golden_labels_for_episodes(conn, episode_ids=episode_ids, label_pack=label_pack, model=model)
    sources = sorted({str(row["source_name"]) for row in rows}, key=lambda name: sha256_text(f"{seed}:{name}"))
    test_count = max(1, round(len(sources) * 0.2))
    validation_count = max(1, round(len(sources) * 0.2))
    source_split = {}
    for index, source in enumerate(sources):
        if index < test_count:
            source_split[source] = "test"
        elif index < test_count + validation_count:
            source_split[source] = "validation"
        else:
            source_split[source] = "train"
    paths = {split: output_root / f"{split}.private.jsonl" for split in ["train", "validation", "test"]}
    handles = {split: path.open("w", encoding="utf-8") for split, path in paths.items()}
    counts = Counter()
    event_counts = Counter()
    read_failures = 0
    try:
        for row in rows:
            split = source_split[str(row["source_name"])]
            segment_text, read_error = _safe_segment_text(conn, row["segment_id"])
            if read_error:
                read_failures += 1
                continue
            context_packet = _attach_existing_context(
                conn,
                {"episode": {"episode_id": row["episode_id"], "source_name": row["source_name"]}, "segments": [{"segment_id": row["segment_id"], "text": segment_text}]},
                segment_ids=[row["segment_id"]],
                label_pack=label_pack,
            )
            if compact_context:
                context_packet = _compact_distillation_input(context_packet)
            label = json.loads(row["output_json"])
            target = evidence_sparse_compact_label(label)
            target_representation = EVIDENCE_SPARSE_COMPACT_SCHEMA_VERSION
            if task == "event_detection":
                target = event_detection_target(target)
                target_representation = "ai_discourse_v3_1_event_detection_v1"
            example = {
                "schema_version": "pif_distillation_example_v1",
                "input": context_packet,
                "target_representation": target_representation,
                "target": target,
            }
            handles[split].write(json.dumps(example, ensure_ascii=True, separators=(",", ":")) + "\n")
            counts[split] += 1
            event_counts[split] += len(label.get("discourse_events") or [])
    finally:
        for handle in handles.values():
            handle.close()
    manifest = {
        "ok": True,
        "generated_at": now_iso(),
        "privacy": "private_jsonl_contains_transcript_text_manifest_contains_counts_only",
        "seed": seed,
        "label_pack": label_pack,
        "teacher_model": model,
        "task": task,
        "input_context": "compact_model_generated_semantic_context" if compact_context else "full_existing_context",
        "split_unit": "podcast_source",
        "source_count": len(sources),
        "episode_count": len(episode_ids),
        "label_count": sum(counts.values()),
        "segment_text_read_failures": read_failures,
        "splits": {
            split: {
                "source_count": sum(1 for value in source_split.values() if value == split),
                "example_count": counts[split],
                "event_count": event_counts[split],
                "private_jsonl_path": str(paths[split]),
                "bytes": paths[split].stat().st_size,
            }
            for split in ["train", "validation", "test"]
        },
    }
    manifest_path = output_root / "manifest.json"
    write_text_atomic(manifest_path, json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def event_detection_target(label: dict[str, Any]) -> dict[str, Any]:
    events = []
    for event in label.get("evs") or []:
        actor = event.get("a") or {}
        speaker = event.get("sp") or {}
        source = event.get("src") or {}
        candidate = {
            "t": event.get("t"),
            "sub": event.get("sub"),
            "actor": actor.get("n"),
            "speaker": speaker.get("n"),
            "source_kind": source.get("k"),
            "claim": event.get("claim"),
            "why": event.get("why"),
            "ev": event.get("ev"),
            "cf": event.get("cf"),
        }
        events.append({key: value for key, value in candidate.items() if value not in (None, "", [], {})})
    return {"id": label["id"], "st": label.get("st"), "evs": events}


def _compact_distillation_input(packet: dict[str, Any]) -> dict[str, Any]:
    """Retain transcript text plus concise, model-produced semantic context."""
    compact = {
        "episode": packet.get("episode") or {},
        "segments": packet.get("segments") or [],
    }
    artifact = packet.get("episode_context_artifact")
    if not isinstance(artifact, dict):
        return compact
    speaker_map = []
    for speaker in artifact.get("speaker_map") or []:
        if not isinstance(speaker, dict):
            continue
        speaker_map.append(
            {
                key: speaker[key]
                for key in ("raw_speaker", "likely_identity", "role", "aliases_or_variants", "confidence")
                if speaker.get(key) not in (None, "", [], {})
            }
        )
    semantic_context = {
        key: artifact[key]
        for key in ("context_summary", "entity_seed", "concept_seed", "extraction_guidance")
        if artifact.get(key) not in (None, "", [], {})
    }
    if speaker_map:
        semantic_context["speaker_map"] = speaker_map
    if semantic_context:
        compact["model_generated_context"] = semantic_context
        compact["context_contract"] = "Context supports identity and continuity; extract evidence only from current segment text."
    return compact


def run_chunk_size_sweep(
    conn,
    *,
    label_pack: str,
    model: str,
    chunk_sizes: list[int],
    episode_limit: int | None = None,
    per_source_limit: int | None = None,
    seed: str = "efficiency-backtest-v1",
    report_dir: str | Path | None = None,
    output: str | Path | None = None,
    candidate_representation: str = DEFAULT_CANDIDATE_REPRESENTATION,
) -> dict[str, Any]:
    if not chunk_sizes:
        raise ValueError("At least one chunk size is required")
    candidate_representation = _normalize_candidate_representation(candidate_representation)
    normalized_sizes = []
    for size in chunk_sizes:
        if size < 1:
            raise ValueError("Chunk sizes must be at least 1")
        if size not in normalized_sizes:
            normalized_sizes.append(size)
    report_root = Path(report_dir).expanduser().resolve() if report_dir else exports_dir() / "efficiency-chunk-sweep"
    report_root.mkdir(parents=True, exist_ok=True)
    rows = []
    started_transaction = False
    if not conn.in_transaction:
        conn.execute("BEGIN")
        started_transaction = True
    try:
        for chunk_size in normalized_sizes:
            report_path = report_root / f"efficiency-backtest-chunk{chunk_size}-v31.json"
            report = run_efficiency_backtest(
                conn,
                label_pack=label_pack,
                model=model,
                episode_limit=episode_limit,
                per_source_limit=per_source_limit,
                seed=seed,
                output=report_path,
                candidate_chunk_size=chunk_size,
                candidate_representation=candidate_representation,
            )
            summary = report["summary"]
            cost = report["cost_estimates"][summary["candidate"]]
            gate = report["quality_backtest"]["candidate_quality_gate"]
            rows.append(
                {
                    "chunk_size": chunk_size,
                    "candidate": summary["candidate"],
                    "candidate_representation": candidate_representation,
                    "report_path": report["path"],
                    "chunk_count": cost["request_count"],
                    "episode_count": summary["episode_count"],
                    "label_count": summary["label_count"],
                    "source_count": summary["source_count"],
                    "discourse_event_count": summary["discourse_event_count"],
                    "total_token_ratio": summary["estimated_current_vs_candidate_total_token_ratio"],
                    "input_token_ratio": summary["estimated_current_vs_candidate_input_token_ratio"],
                    "request_ratio": summary["estimated_current_vs_candidate_request_ratio"],
                    "runtime_ratio": summary["estimated_current_vs_candidate_runtime_ratio"],
                    "gate_status": gate["status"],
                    "gate_passed": gate["passed"],
                    "compact_roundtrip_exact": summary["sparse_compact_roundtrip_exact_labels"],
                }
            )
    finally:
        if started_transaction:
            conn.rollback()
    eligible = [
        row
        for row in rows
        if row["total_token_ratio"] is not None
        and row["total_token_ratio"] <= DEFAULT_QUALITY_GATE["max_total_token_ratio"]
        and row["runtime_ratio"] is not None
        and row["runtime_ratio"] <= DEFAULT_QUALITY_GATE["max_runtime_ratio"]
        and row["compact_roundtrip_exact"] == 1.0
    ]
    recommended = None
    if eligible:
        # Prefer the smallest passing chunk to limit output-window risk, then lower token ratio.
        recommended = sorted(eligible, key=lambda row: (row["chunk_size"], row["total_token_ratio"]))[0]
    payload = {
        "ok": True,
        "generated_at": now_iso(),
        "privacy": "sanitized_no_transcript_text",
        "label_pack": label_pack,
        "model": model,
        "candidate_representation": candidate_representation,
        "seed": seed,
        "selection": {
            "episode_limit": episode_limit,
            "per_source_limit": per_source_limit,
            "consistent_sqlite_read_snapshot": started_transaction,
        },
        "thresholds": {
            "max_total_token_ratio": DEFAULT_QUALITY_GATE["max_total_token_ratio"],
            "max_runtime_ratio": DEFAULT_QUALITY_GATE["max_runtime_ratio"],
        },
        "report_dir": str(report_root),
        "chunk_sizes": rows,
        "recommended_chunk_size": recommended["chunk_size"] if recommended else None,
        "recommendation": recommended,
        "note": "Recommendation chooses the smallest chunk size that clears quarter token/runtime estimates and exact compact roundtrip. The sweep uses one SQLite read snapshot when possible. Live candidate outputs still decide quality.",
    }
    if output:
        path = Path(output).expanduser().resolve()
        write_text_atomic(path, json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
        payload["path"] = str(path)
    return payload


def compact_label(label: dict[str, Any]) -> dict[str, Any]:
    return {
        "v": COMPACT_SCHEMA_VERSION,
        "id": label["segment_id"],
        "st": label["extraction_status"],
        "sq": label["segment_quality"],
        "sc": label["segment_source_context"],
        "evs": [_compact_event(event) for event in label.get("discourse_events") or []],
        "cc": label.get("concept_candidates") or [],
        "rc": label.get("rejected_candidates") or [],
        "ns": label.get("no_signal_reason"),
        "cf": label.get("overall_confidence"),
        "nr": label.get("needs_review"),
        "rr": label.get("review_reason"),
    }


def expand_compact_label(compact: dict[str, Any], *, episode_id: str) -> dict[str, Any]:
    if compact.get("v") != COMPACT_SCHEMA_VERSION:
        raise ValueError(f"compact label version must be {COMPACT_SCHEMA_VERSION}")
    return {
        "schema_version": "ai_discourse_v3_1",
        "segment_id": compact["id"],
        "episode_id": episode_id,
        "extraction_status": compact["st"],
        "segment_quality": compact["sq"],
        "segment_source_context": compact["sc"],
        "discourse_events": [_expand_event(event) for event in compact.get("evs") or []],
        "concept_candidates": compact.get("cc") or [],
        "rejected_candidates": compact.get("rc") or [],
        "no_signal_reason": compact.get("ns"),
        "overall_confidence": compact.get("cf"),
        "needs_review": compact.get("nr"),
        "review_reason": compact.get("rr"),
    }


def sparse_compact_label(label: dict[str, Any]) -> dict[str, Any]:
    sparse = {
        "id": label["segment_id"],
        "st": label["extraction_status"],
        "sq": _sparse_segment_quality(label["segment_quality"]),
        "sc": _sparse_source_context(label["segment_source_context"]),
        "evs": [_sparse_event(event) for event in label.get("discourse_events") or []],
        "cc": [_sparse_concept_candidate(item) for item in label.get("concept_candidates") or []],
        "rc": [_sparse_rejected_candidate(item) for item in label.get("rejected_candidates") or []],
        "ns": label.get("no_signal_reason"),
        "cf": label.get("overall_confidence"),
        "nr": label.get("needs_review"),
        "rr": label.get("review_reason"),
    }
    return _omit_defaults(sparse, SPARSE_LABEL_DEFAULTS)


def offset_sparse_compact_label(label: dict[str, Any]) -> dict[str, Any]:
    sparse = sparse_compact_label(label)
    for event in sparse.get("evs") or []:
        if isinstance(event, dict):
            event.pop("ev", None)
    for candidate in sparse.get("cc") or []:
        if isinstance(candidate, dict):
            candidate.pop("ev", None)
    return sparse


def evidence_sparse_compact_label(label: dict[str, Any]) -> dict[str, Any]:
    sparse = sparse_compact_label(label)
    for event in sparse.get("evs") or []:
        if isinstance(event, dict):
            event.pop("s", None)
            event.pop("e", None)
    for candidate in sparse.get("cc") or []:
        if isinstance(candidate, dict):
            candidate.pop("s", None)
            candidate.pop("e", None)
    return sparse


def flat_ledger_label(label: dict[str, Any]) -> list[Any]:
    return [
        label["segment_id"],
        label["extraction_status"],
        _values(label["segment_quality"], SEGMENT_QUALITY_KEY_MAP),
        _values(label["segment_source_context"], SOURCE_CONTEXT_KEY_MAP),
        [_flat_event(event) for event in label.get("discourse_events") or []],
        [_values(item, CONCEPT_CANDIDATE_KEY_MAP, omit={"s", "e"}) for item in label.get("concept_candidates") or []],
        [_values(item, REJECTED_CANDIDATE_KEY_MAP) for item in label.get("rejected_candidates") or []],
        label.get("no_signal_reason"),
        label.get("overall_confidence"),
        label.get("needs_review"),
        label.get("review_reason"),
    ]


def expand_flat_ledger_label(value: list[Any], *, episode_id: str) -> dict[str, Any]:
    if len(value) != 11:
        raise ValueError("flat ledger label must contain exactly 11 fields")
    return {
        "schema_version": "ai_discourse_v3_1",
        "segment_id": value[0],
        "episode_id": episode_id,
        "extraction_status": value[1],
        "segment_quality": _from_values(value[2], SEGMENT_QUALITY_KEY_MAP),
        "segment_source_context": _from_values(value[3], SOURCE_CONTEXT_KEY_MAP),
        "discourse_events": [_expand_flat_event(event) for event in value[4]],
        "concept_candidates": [_from_values(item, CONCEPT_CANDIDATE_KEY_MAP, omitted={"s", "e"}) for item in value[5]],
        "rejected_candidates": [_from_values(item, REJECTED_CANDIDATE_KEY_MAP) for item in value[6]],
        "no_signal_reason": value[7],
        "overall_confidence": value[8],
        "needs_review": value[9],
        "review_reason": value[10],
    }


def expand_sparse_compact_label(compact: dict[str, Any], *, episode_id: str) -> dict[str, Any]:
    version = compact.get("v")
    if version not in (None, SPARSE_COMPACT_SCHEMA_VERSION):
        raise ValueError(f"sparse compact label version must be {SPARSE_COMPACT_SCHEMA_VERSION}")
    return {
        "schema_version": "ai_discourse_v3_1",
        "segment_id": compact["id"],
        "episode_id": episode_id,
        "extraction_status": compact.get("st", SPARSE_LABEL_DEFAULTS["st"]),
        "segment_quality": _expand_sparse_segment_quality(compact["sq"]),
        "segment_source_context": _expand_sparse_source_context(compact["sc"]),
        "discourse_events": [_expand_sparse_event(event) for event in compact.get("evs", SPARSE_LABEL_DEFAULTS["evs"])],
        "concept_candidates": [
            _expand_sparse_concept_candidate(item)
            for item in compact.get("cc", SPARSE_LABEL_DEFAULTS["cc"])
        ],
        "rejected_candidates": [
            _expand_sparse_rejected_candidate(item)
            for item in compact.get("rc", SPARSE_LABEL_DEFAULTS["rc"])
        ],
        "no_signal_reason": compact.get("ns", SPARSE_LABEL_DEFAULTS["ns"]),
        "overall_confidence": compact.get("cf"),
        "needs_review": compact.get("nr", SPARSE_LABEL_DEFAULTS["nr"]),
        "review_reason": compact.get("rr", SPARSE_LABEL_DEFAULTS["rr"]),
    }


def expand_offset_sparse_compact_label(compact: dict[str, Any], *, episode_id: str, segment_text: str) -> dict[str, Any]:
    version = compact.get("v")
    if version not in (None, OFFSET_SPARSE_COMPACT_SCHEMA_VERSION):
        raise ValueError(f"offset sparse compact label version must be {OFFSET_SPARSE_COMPACT_SCHEMA_VERSION}")
    expanded = expand_sparse_compact_label({k: v for k, v in compact.items() if k != "v"}, episode_id=episode_id)
    return _hydrate_label_evidence_from_offsets(expanded, segment_text=segment_text)


def _hydrate_label_evidence_from_offsets(label: dict[str, Any], *, segment_text: str) -> dict[str, Any]:
    hydrated = json.loads(json.dumps(label, ensure_ascii=True))
    for item in list(hydrated.get("discourse_events") or []) + list(hydrated.get("concept_candidates") or []):
        if not isinstance(item, dict):
            continue
        start = item.get("evidence_start")
        end = item.get("evidence_end")
        if isinstance(start, int) and isinstance(end, int) and 0 <= start < end <= len(segment_text):
            item["evidence"] = segment_text[start:end]
    return hydrated


def _hydrate_label_evidence_from_reference(label: dict[str, Any], reference: dict[str, Any]) -> dict[str, Any]:
    hydrated = json.loads(json.dumps(label, ensure_ascii=True))
    for output_key in ["discourse_events", "concept_candidates"]:
        output_items = hydrated.get(output_key) or []
        reference_items = reference.get(output_key) or []
        for index, item in enumerate(output_items):
            if not isinstance(item, dict) or index >= len(reference_items) or not isinstance(reference_items[index], dict):
                continue
            item["evidence"] = reference_items[index].get("evidence")
    return hydrated


def _hydrate_label_offsets_from_evidence(label: dict[str, Any], *, segment_text: str) -> dict[str, Any]:
    hydrated = json.loads(json.dumps(label, ensure_ascii=True))
    for item in hydrated.get("discourse_events") or []:
        if not isinstance(item, dict):
            continue
        evidence = item.get("evidence")
        if not isinstance(evidence, str) or evidence not in segment_text:
            for alternate_key in ["audit_notes", "signal_reason"]:
                alternate = item.get(alternate_key)
                if isinstance(alternate, str) and alternate and alternate in segment_text:
                    item[alternate_key], item["evidence"] = evidence, alternate
                    break
    for item in list(hydrated.get("discourse_events") or []) + list(hydrated.get("concept_candidates") or []):
        if not isinstance(item, dict):
            continue
        evidence = item.get("evidence")
        if not isinstance(evidence, str) or not evidence:
            continue
        start = segment_text.find(evidence)
        if start >= 0:
            item["evidence_start"] = start
            item["evidence_end"] = start + len(evidence)
    return hydrated


def _hydrate_flat_ledger_evidence(label: dict[str, Any], *, segment_text: str) -> dict[str, Any]:
    hydrated = _hydrate_label_offsets_from_evidence(label, segment_text=segment_text)
    hydrated["discourse_events"] = [
        event
        for event in hydrated.get("discourse_events") or []
        if isinstance(event.get("evidence_start"), int) and isinstance(event.get("evidence_end"), int)
    ]
    return hydrated


def _prune_invalid_flat_events(label: dict[str, Any], *, label_pack: str, segment_text: str) -> dict[str, Any]:
    pruned = json.loads(json.dumps(label, ensure_ascii=True))
    while True:
        try:
            validate_label_output(label_pack, pruned, segment_text=segment_text)
            return pruned
        except Exception as exc:
            match = re.search(r"\$\.discourse_events\[(\d+)\]", str(exc))
            if not match:
                return pruned
            index = int(match.group(1))
            events = pruned.get("discourse_events") or []
            if index >= len(events):
                return pruned
            del events[index]


def _hydrate_label_offsets_from_reference(label: dict[str, Any], reference: dict[str, Any]) -> dict[str, Any]:
    hydrated = json.loads(json.dumps(label, ensure_ascii=True))
    for output_key in ["discourse_events", "concept_candidates"]:
        output_items = hydrated.get(output_key) or []
        reference_items = reference.get(output_key) or []
        for index, item in enumerate(output_items):
            if not isinstance(item, dict) or index >= len(reference_items) or not isinstance(reference_items[index], dict):
                continue
            item["evidence_start"] = reference_items[index].get("evidence_start")
            item["evidence_end"] = reference_items[index].get("evidence_end")
    return hydrated


def _normalize_candidate_representation(value: str) -> str:
    normalized = str(value or DEFAULT_CANDIDATE_REPRESENTATION).strip().lower().replace("-", "_")
    if normalized not in CANDIDATE_REPRESENTATIONS:
        raise ValueError(f"candidate representation must be one of {sorted(CANDIDATE_REPRESENTATIONS)}")
    return normalized


def _candidate_schema_version(representation: str) -> str:
    representation = _normalize_candidate_representation(representation)
    if representation == "sparse_compact":
        return SPARSE_COMPACT_SCHEMA_VERSION
    if representation == "offset_sparse_compact":
        return OFFSET_SPARSE_COMPACT_SCHEMA_VERSION
    if representation == "evidence_sparse_compact":
        return EVIDENCE_SPARSE_COMPACT_SCHEMA_VERSION
    if representation in {"flat_ledger", "flat_ledger_full"}:
        return FLAT_LEDGER_SCHEMA_VERSION
    if representation == "full_schema":
        return FULL_SCHEMA_BATCH_VERSION
    return COMPACT_SCHEMA_VERSION


def _candidate_cost_name(representation: str) -> str:
    representation = _normalize_candidate_representation(representation)
    if representation == "sparse_compact":
        return "episode_chunk_sparse_compact_v1"
    if representation == "offset_sparse_compact":
        return "episode_chunk_offset_sparse_compact_v1"
    if representation == "evidence_sparse_compact":
        return "episode_chunk_evidence_sparse_compact_v1"
    if representation in {"flat_ledger", "flat_ledger_full"}:
        return f"episode_chunk_{representation}_v1"
    if representation == "full_schema":
        return "episode_chunk_full_schema_v2"
    return "episode_chunk_compact_v1"


def _candidate_encode_label(label: dict[str, Any], *, representation: str) -> Any:
    representation = _normalize_candidate_representation(representation)
    if representation == "sparse_compact":
        return sparse_compact_label(label)
    if representation == "offset_sparse_compact":
        return offset_sparse_compact_label(label)
    if representation == "evidence_sparse_compact":
        return evidence_sparse_compact_label(label)
    if representation in {"flat_ledger", "flat_ledger_full"}:
        return flat_ledger_label(label)
    if representation == "full_schema":
        return label
    return compact_label(label)


def compare_candidate_episode_outputs(
    conn,
    *,
    labels_by_episode: dict[str, list[dict[str, Any]]],
    candidate_output_dir: Path,
    label_pack: str,
) -> dict[str, Any]:
    candidate_by_segment = _load_candidate_outputs(candidate_output_dir)
    golden_by_segment = {
        row["segment_id"]: row
        for rows in labels_by_episode.values()
        for row in rows
    }
    compared = []
    for segment_id, row in golden_by_segment.items():
        golden = row["output"]
        base_item = {
            "segment_id": segment_id,
            "episode_id": row.get("episode_id"),
            "source_name": row.get("source_name"),
            "golden_status": golden.get("extraction_status"),
            "golden_events": len(golden.get("discourse_events") or []),
        }
        candidate = candidate_by_segment.get(segment_id)
        if not candidate:
            compared.append({**base_item, "missing": True})
            continue
        segment_text, read_error = _safe_segment_text(conn, segment_id)
        if read_error:
            compared.append(
                {
                    **base_item,
                    "missing": False,
                    "validation_ok": False,
                    "validation_error": read_error,
                    **_label_similarity(golden, candidate),
                }
            )
            continue
        if candidate.get("_offset_sparse_needs_hydration"):
            candidate = _hydrate_label_evidence_from_offsets(candidate, segment_text=segment_text)
            candidate.pop("_offset_sparse_needs_hydration", None)
        if candidate.get("_evidence_sparse_needs_hydration"):
            candidate = _hydrate_label_offsets_from_evidence(candidate, segment_text=segment_text)
            candidate.pop("_evidence_sparse_needs_hydration", None)
            candidate = _prune_invalid_flat_events(candidate, label_pack=label_pack, segment_text=segment_text)
        if candidate.get("_flat_ledger_needs_hydration"):
            candidate = _hydrate_flat_ledger_evidence(candidate, segment_text=segment_text)
            candidate.pop("_flat_ledger_needs_hydration", None)
            candidate = _prune_invalid_flat_events(candidate, label_pack=label_pack, segment_text=segment_text)
        try:
            validate_label_output(label_pack, candidate, segment_text=segment_text)
            validation_ok = True
            validation_error = None
        except Exception as exc:
            validation_ok = False
            validation_error = _sanitize_error(str(exc))
        compared.append(
            {
                **base_item,
                "missing": False,
                "validation_ok": validation_ok,
                "validation_error": validation_error,
                **_label_similarity(golden, candidate),
            }
        )
    return _aggregate_candidate_comparison(compared)


def export_sparse_candidate_prompts(
    conn,
    *,
    labels_by_episode: dict[str, list[dict[str, Any]]],
    label_pack: str,
    output_dir: Path,
    chunk_size: int = SPARSE_CHUNK_MAX_SEGMENTS,
    representation: str = DEFAULT_CANDIDATE_REPRESENTATION,
) -> dict[str, Any]:
    if chunk_size < 1:
        raise ValueError("candidate chunk size must be at least 1")
    representation = _normalize_candidate_representation(representation)
    prompt_dir = output_dir / "prompts"
    candidate_output_dir = output_dir / "outputs"
    prompt_dir.mkdir(parents=True, exist_ok=True)
    candidate_output_dir.mkdir(parents=True, exist_ok=True)
    schema_path = output_dir / f"{_candidate_schema_version(representation)}_schema.json"
    if representation == "full_schema":
        write_text_atomic(schema_path, json.dumps(full_candidate_output_schema(label_pack), ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    elif representation in {"flat_ledger", "flat_ledger_full"}:
        write_text_atomic(
            schema_path,
            json.dumps(flat_ledger_output_schema(), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        )
    elif representation in {"sparse_compact", "offset_sparse_compact", "evidence_sparse_compact"}:
        write_text_atomic(
            schema_path,
            json.dumps(
                _strict_response_schema(
                    sparse_candidate_output_schema(
                        schema_version=_candidate_schema_version(representation),
                        require_evidence_text=representation != "offset_sparse_compact",
                        require_offsets=representation != "evidence_sparse_compact",
                    )
                ),
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
    else:
        write_text_atomic(
            schema_path,
            json.dumps(
                {
                    "schema_version": _candidate_schema_version(representation),
                    "note": "Dense compact outputs use the compact output contract and are expanded by local comparison code; no strict JSON schema is maintained for this variant.",
                },
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
    episodes = []
    chunks = []
    total_read_failures = 0
    for episode_id, rows in sorted(labels_by_episode.items()):
        row_chunks = _chunk_episode_rows(rows, chunk_size=chunk_size)
        episode_read_failures = 0
        episode_golden_summary = _golden_chunk_summary(rows)
        for chunk_index, chunk_rows in enumerate(row_chunks, start=1):
            safe_name = f"{_safe_filename(episode_id)}-c{chunk_index:02d}"
            prompt_path = prompt_dir / f"{safe_name}.md"
            output_path = candidate_output_dir / f"{safe_name}.json"
            golden_summary = _golden_chunk_summary(chunk_rows)
            prompt, read_failures = render_episode_batch_prompt_with_read_stats(
                conn,
                episode_id=episode_id,
                segment_ids=[row["segment_id"] for row in chunk_rows],
                label_pack=label_pack,
                compact=True,
                sparse=representation in {"sparse_compact", "offset_sparse_compact", "evidence_sparse_compact"},
                candidate_representation=representation,
            )
            total_read_failures += read_failures
            episode_read_failures += read_failures
            write_text_atomic(prompt_path, prompt)
            if not output_path.exists():
                write_text_atomic(output_path, "")
            chunks.append(
                {
                    "chunk_id": safe_name,
                    "episode_id": episode_id,
                    "source_name": rows[0]["source_name"],
                    "chunk_index": chunk_index,
                    "chunk_count": len(row_chunks),
                    "label_count": len(chunk_rows),
                    "prompt_path": str(prompt_path.resolve()),
                    "output_path": str(output_path.resolve()),
                    "segment_count": len(chunk_rows),
                    "segment_ids": [row["segment_id"] for row in chunk_rows],
                    "segment_text_read_failures": read_failures,
                    **golden_summary,
                }
            )
        episodes.append(
            {
                "episode_id": episode_id,
                "source_name": rows[0]["source_name"],
                "label_count": len(rows),
                "chunk_count": len(row_chunks),
                "segment_count": len(rows),
                "segment_text_read_failures": episode_read_failures,
                **episode_golden_summary,
            }
        )
    manifest = {
        "schema_version": _candidate_schema_version(representation),
        "candidate_representation": representation,
        "privacy": "local_private_prompts_may_contain_transcript_text_manifest_is_sanitized",
        "label_pack": label_pack,
        "chunk_size": chunk_size,
        "chunk_count": len(chunks),
        "episode_count": len(episodes),
        "label_count": sum(item["label_count"] for item in episodes),
        "expected_coded_segments": sum(item["expected_coded_segments"] for item in episodes),
        "expected_discourse_events": sum(item["expected_discourse_events"] for item in episodes),
        "candidate_output_dir": str(candidate_output_dir.resolve()),
        "validator_schema_path": str(schema_path.resolve()),
        "segment_text_read_failures": total_read_failures,
        "instructions": "Process each chunk prompt_path with GPT-5.5/Codex app, write strict JSON only to output_path, then run efficiency-backtest with --candidate-output-dir. validator_schema_path is a reference/local validator schema; Codex structured-output mode may require a denser schema.",
        "episodes": episodes,
        "chunks": chunks,
    }
    manifest_path = output_dir / "manifest.json"
    write_text_atomic(manifest_path, json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    return {
        "manifest_path": str(manifest_path.resolve()),
        "candidate_output_dir": str(candidate_output_dir.resolve()),
        "validator_schema_path": str(schema_path.resolve()),
        "candidate_representation": representation,
        "episode_count": len(episodes),
        "chunk_count": len(chunks),
        "chunk_size": chunk_size,
        "label_count": manifest["label_count"],
        "expected_coded_segments": manifest["expected_coded_segments"],
        "expected_discourse_events": manifest["expected_discourse_events"],
        "segment_text_read_failures": total_read_failures,
        "privacy": manifest["privacy"],
    }


def run_candidate_prompt_smoke(
    *,
    manifest_path: str | Path,
    model: str = "gpt-5.5",
    limit: int = 1,
    rerun: bool = False,
    max_active_gpt55: int = 0,
    timeout_seconds: int = DEFAULT_SMOKE_TIMEOUT_SECONDS,
    ignore_user_config: bool = False,
    dry_run: bool = False,
    min_expected_events: int | None = None,
    max_expected_events: int | None = None,
    wait_for_clear_seconds: int = 0,
    wait_poll_seconds: int = 30,
    reasoning_effort: str = "low",
    concurrency: int = 1,
) -> dict[str, Any]:
    if limit < 0:
        raise ValueError("limit must be non-negative")
    if min_expected_events is not None and min_expected_events < 0:
        raise ValueError("min expected events must be non-negative")
    if max_expected_events is not None and max_expected_events < 0:
        raise ValueError("max expected events must be non-negative")
    if wait_for_clear_seconds < 0:
        raise ValueError("wait for clear seconds must be non-negative")
    if wait_poll_seconds < 1:
        raise ValueError("wait poll seconds must be at least 1")
    if (
        min_expected_events is not None
        and max_expected_events is not None
        and min_expected_events > max_expected_events
    ):
        raise ValueError("min expected events must be <= max expected events")
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    schema_path = Path(manifest["validator_schema_path"]).expanduser().resolve()
    active_gpt55 = _active_codex_gpt55_processes()
    waited_seconds = 0
    active_gpt55_observations = [active_gpt55]
    if not dry_run and active_gpt55 > max_active_gpt55 and wait_for_clear_seconds:
        deadline = time.monotonic() + wait_for_clear_seconds
        while active_gpt55 > max_active_gpt55 and time.monotonic() < deadline:
            sleep_seconds = min(wait_poll_seconds, max(1, int(deadline - time.monotonic())))
            time.sleep(sleep_seconds)
            waited_seconds += sleep_seconds
            active_gpt55 = _active_codex_gpt55_processes()
            active_gpt55_observations.append(active_gpt55)
    if not dry_run and active_gpt55 > max_active_gpt55:
        return {
            "ok": False,
            "blocked": True,
            "reason": "codex_gpt55_lane_busy",
            "active_gpt55_processes": active_gpt55,
            "active_gpt55_process_observations": active_gpt55_observations,
            "max_active_gpt55": max_active_gpt55,
            "waited_seconds": waited_seconds,
            "wait_for_clear_seconds": wait_for_clear_seconds,
            "wait_poll_seconds": wait_poll_seconds,
            "manifest_path": str(manifest_file),
            "privacy": "sanitized_no_prompt_or_transcript_text",
        }
    chunks = manifest.get("chunks") or manifest.get("episodes") or []
    selected = []
    if limit > 0:
        ranked_chunks = _rank_candidate_smoke_chunks(
            chunks,
            min_expected_events=min_expected_events,
            max_expected_events=max_expected_events,
        )
        for item in ranked_chunks:
            prompt_path = Path(item["prompt_path"]).expanduser()
            output_path = Path(item["output_path"]).expanduser()
            if not rerun and output_path.exists() and output_path.stat().st_size > 0:
                continue
            selected.append((item, prompt_path, output_path))
            if len(selected) >= limit:
                break
    results = []
    if dry_run:
        for item, prompt_path, output_path in selected:
            results.append(
                {
                    "chunk_id": item.get("chunk_id"),
                    "episode_id": item.get("episode_id"),
                    "source_name": item.get("source_name"),
                    "segment_count": item.get("segment_count"),
                    "label_count": item.get("label_count"),
                    "expected_coded_segments": item.get("expected_coded_segments"),
                    "expected_discourse_events": item.get("expected_discourse_events"),
                    "prompt_path": str(prompt_path.resolve()),
                    "output_path": str(output_path.resolve()),
                    "output_bytes": output_path.stat().st_size if output_path.exists() else 0,
                    "dry_run": True,
                }
            )
        return {
            "ok": True,
            "blocked": False,
            "dry_run": True,
            "manifest_path": str(manifest_file),
            "model": model,
            "requested_limit": limit,
            "min_expected_events": min_expected_events,
            "max_expected_events": max_expected_events,
            "selected": len(selected),
            "active_gpt55_processes_at_start": active_gpt55,
            "active_gpt55_process_observations": active_gpt55_observations,
            "waited_seconds": waited_seconds,
            "privacy": "sanitized_no_prompt_or_transcript_text",
            "results": results,
        }
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    def run_one(selection):
        item, prompt_path, output_path = selection
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("", encoding="utf-8")
        log_path = output_path.parent.parent / f"smoke-{output_path.stem}.log"
        command = [
            "codex",
            "exec",
            "-m",
            model,
            "-c",
            f'model_reasoning_effort="{reasoning_effort}"',
            "-C",
            str(scratch_dir),
            "--skip-git-repo-check",
            "--ignore-rules",
            "--ephemeral",
            "--sandbox",
            "read-only",
            "--output-schema",
            str(schema_path),
            "--output-last-message",
            str(output_path),
            "--json",
            "-",
        ]
        if ignore_user_config:
            command.insert(6, "--ignore-user-config")
        exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
            command,
            prompt_path=prompt_path,
            log_path=log_path,
            timeout_seconds=timeout_seconds,
        )
        output_bytes = output_path.stat().st_size if output_path.exists() else 0
        usage = _codex_usage_from_jsonl(log_path)
        return {
                "chunk_id": item.get("chunk_id"),
                "episode_id": item.get("episode_id"),
                "source_name": item.get("source_name"),
                "segment_count": item.get("segment_count"),
                "label_count": item.get("label_count"),
                "expected_coded_segments": item.get("expected_coded_segments"),
                "expected_discourse_events": item.get("expected_discourse_events"),
                "prompt_path": str(prompt_path.resolve()),
                "output_path": str(output_path.resolve()),
                "log_path": str(log_path.resolve()),
                "exit_code": exit_code,
                "timed_out": timed_out,
                "elapsed_seconds": elapsed_seconds,
                "timeout_seconds": timeout_seconds,
                "output_bytes": output_bytes,
                "usage": usage,
                "json_ok": _json_file_ok(output_path) if output_bytes else False,
            }

    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    if concurrency == 1:
        results = [run_one(selection) for selection in selected]
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(run_one, selection): selection for selection in selected}
            results = [future.result() for future in as_completed(futures)]
        results.sort(key=lambda item: item.get("chunk_id") or "")
    return {
        "ok": all(item["exit_code"] == 0 and item["json_ok"] for item in results) if results else True,
        "blocked": False,
        "manifest_path": str(manifest_file),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "concurrency": concurrency,
        "requested_limit": limit,
        "min_expected_events": min_expected_events,
        "max_expected_events": max_expected_events,
        "selected": len(selected),
        "active_gpt55_processes_at_start": active_gpt55,
        "active_gpt55_process_observations": active_gpt55_observations,
        "waited_seconds": waited_seconds,
        "privacy": "sanitized_no_prompt_or_transcript_text",
        "results": results,
    }


def run_candidate_event_verifier_smoke(
    conn,
    *,
    manifest_path: str | Path,
    limit: int = 1,
    model: str = "gpt-5.5",
    reasoning_effort: str = "low",
    timeout_seconds: int = 600,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    root_dir = manifest_file.parent
    verifier_prompt_dir = root_dir / "verifier_prompts"
    verifier_output_dir = root_dir / "verifier_outputs"
    verified_candidate_dir = root_dir / "verified_outputs"
    for path in [verifier_prompt_dir, verifier_output_dir, verified_candidate_dir]:
        path.mkdir(parents=True, exist_ok=True)
    schema_path = root_dir / "event_verifier_schema.json"
    write_text_atomic(
        schema_path,
        json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": False,
                "required": ["keep"],
                "properties": {"keep": {"type": "array", "items": {"type": "integer", "minimum": 0}}},
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        ) + "\n",
    )
    selected = []
    for chunk in manifest.get("chunks") or []:
        candidate_path = Path(chunk["output_path"]).expanduser()
        if candidate_path.exists() and candidate_path.stat().st_size > 0:
            selected.append((chunk, candidate_path))
        if len(selected) >= limit:
            break
    results = []
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    for chunk, candidate_path in selected:
        payload = json.loads(candidate_path.read_text(encoding="utf-8"))
        segment_outputs = payload.get("segment_outputs") or []
        if len(segment_outputs) != 1:
            continue
        segment_id = str(segment_outputs[0]["segment_id"])
        segment_text, read_error = _safe_segment_text(conn, segment_id)
        if read_error:
            continue
        packet = _attach_existing_context(
            conn,
            {"episode": {"episode_id": chunk["episode_id"]}, "segments": [{"segment_id": segment_id, "text": segment_text}]},
            segment_ids=[segment_id],
            label_pack="ai_discourse_v3_1",
        )
        events = (segment_outputs[0].get("label") or {}).get("evs") or []
        prompt = "\n\n".join(
            [
                "You are a low-reasoning precision verifier for private ai_discourse_v3_1 extraction.",
                "Read the complete current segment and context, then inspect every numbered candidate event. This is a conservative precision pass: remove an event only when you are at least 95% certain it is a duplicate relabeling, low-value identity mention, setup, ad, incidental example, unsupported inference, or materially mistyped. If uncertain, keep it. Distinct supported propositions may share evidence; do not remove a supported causal mechanism or stance merely because another signal shares its evidence. Return JSON only with the zero-based candidate indices to keep.",
                "# Packet",
                json.dumps(packet, ensure_ascii=True, separators=(",", ":")),
                "# Candidate Events",
                json.dumps([{"i": index, "event": event} for index, event in enumerate(events)], ensure_ascii=True, separators=(",", ":")),
            ]
        )
        prompt_path = verifier_prompt_dir / f"{chunk['chunk_id']}.md"
        verifier_output_path = verifier_output_dir / f"{chunk['chunk_id']}.json"
        verified_path = verified_candidate_dir / candidate_path.name
        write_text_atomic(prompt_path, prompt)
        command = [
            "codex", "exec", "-m", model, "-c", f'model_reasoning_effort="{reasoning_effort}"',
            "-C", str(scratch_dir), "--skip-git-repo-check", "--ignore-rules", "--ephemeral",
            "--sandbox", "read-only", "--output-schema", str(schema_path),
            "--output-last-message", str(verifier_output_path), "-",
        ]
        exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
            command,
            prompt_path=prompt_path,
            log_path=root_dir / f"verifier-{chunk['chunk_id']}.log",
            timeout_seconds=timeout_seconds,
        )
        keep = []
        if exit_code == 0 and _json_file_ok(verifier_output_path):
            decision = json.loads(verifier_output_path.read_text(encoding="utf-8"))
            keep = sorted({index for index in decision.get("keep") or [] if isinstance(index, int) and 0 <= index < len(events)})
            revised = json.loads(json.dumps(payload, ensure_ascii=True))
            revised["segment_outputs"][0]["label"]["evs"] = [events[index] for index in keep]
            write_text_atomic(verified_path, json.dumps(revised, ensure_ascii=True, separators=(",", ":")) + "\n")
        results.append(
            {
                "chunk_id": chunk["chunk_id"],
                "source_name": chunk["source_name"],
                "candidate_events": len(events),
                "kept_events": len(keep),
                "exit_code": exit_code,
                "timed_out": timed_out,
                "elapsed_seconds": elapsed_seconds,
                "json_ok": _json_file_ok(verifier_output_path) if verifier_output_path.exists() else False,
                "verified_output_path": str(verified_path.resolve()),
            }
        )
    return {
        "ok": bool(results) and all(item["exit_code"] == 0 and item["json_ok"] for item in results),
        "manifest_path": str(manifest_file),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "selected": len(results),
        "verified_candidate_dir": str(verified_candidate_dir.resolve()),
        "privacy": "sanitized_no_prompt_or_transcript_text",
        "results": results,
    }


def run_proposition_extractor_smoke(
    conn,
    *,
    manifest_path: str | Path,
    limit: int = 1,
    concurrency: int = 1,
    model: str = "gpt-5.5",
    reasoning_effort: str = "low",
    timeout_seconds: int = 600,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    root_dir = manifest_file.parent
    prompt_dir = root_dir / "proposition_prompts"
    output_dir = root_dir / "proposition_outputs"
    for path in [prompt_dir, output_dir]:
        path.mkdir(parents=True, exist_ok=True)
    schema_path = root_dir / "proposition_extractor_schema.json"
    write_text_atomic(
        schema_path,
        json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": False,
                "required": ["p"],
                "properties": {
                    "p": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["e", "c"],
                            "properties": {"e": {"type": "string"}, "c": {"type": "string"}},
                        },
                    }
                },
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        ) + "\n",
    )
    selected = []
    for chunk in _rank_candidate_smoke_chunks(manifest.get("chunks") or [], min_expected_events=10, max_expected_events=40):
        output_path = output_dir / f"{chunk['chunk_id']}.json"
        if output_path.exists() and output_path.stat().st_size > 0:
            continue
        selected.append((chunk, output_path))
        if len(selected) >= limit:
            break
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    prepared = []
    for chunk, output_path in selected:
        segment_id = _single_segment_id_for_chunk(chunk)
        segment_text, read_error = _safe_segment_text(conn, segment_id)
        if read_error:
            continue
        packet = _attach_existing_context(
            conn,
            {"episode": {"episode_id": chunk["episode_id"]}, "segments": [{"segment_id": segment_id, "text": segment_text}]},
            segment_ids=[segment_id],
            label_pack="ai_discourse_v3_1",
        )
        prompt = "\n\n".join(
            [
                "You are a low-reasoning proposition extractor for private podcast research.",
                "Read every word of the current segment. Emit one record for every distinct substantive proposition, including explicit causal explanations, forecasts, stances, uncertainty, terminology, capabilities, product facts, market facts, risks, adoption claims, counterclaims, and graph-useful actor or entity relationships. Do not classify event types yet. Do not use keyword rules. Exclude ads, setup, page chrome, incidental examples, and unsupported inference. Multiple distinct propositions may share evidence. For e, copy one exact contiguous current-segment quote byte-for-byte. For c, write a concise faithful proposition. Privately scan once for candidates and once for omissions, then return JSON only.",
                "Use episode context only for identity and continuity; evidence must come from the current segment.",
                json.dumps(packet, ensure_ascii=True, separators=(",", ":")),
            ]
        )
        prompt_path = prompt_dir / f"{chunk['chunk_id']}.md"
        write_text_atomic(prompt_path, prompt)
        prepared.append((chunk, output_path, prompt_path))

    def run_one(selection):
        chunk, output_path, prompt_path = selection
        command = [
            "codex", "exec", "-m", model, "-c", f'model_reasoning_effort="{reasoning_effort}"',
            "-C", str(scratch_dir), "--skip-git-repo-check", "--ignore-rules", "--ephemeral",
            "--sandbox", "read-only", "--output-schema", str(schema_path),
            "--output-last-message", str(output_path), "-",
        ]
        exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
            command,
            prompt_path=prompt_path,
            log_path=root_dir / f"proposition-{chunk['chunk_id']}.log",
            timeout_seconds=timeout_seconds,
        )
        return {
            "chunk_id": chunk["chunk_id"], "source_name": chunk["source_name"],
            "expected_discourse_events": chunk.get("expected_discourse_events"),
            "exit_code": exit_code, "timed_out": timed_out, "elapsed_seconds": elapsed_seconds,
            "json_ok": _json_file_ok(output_path) if output_path.exists() else False,
            "output_path": str(output_path.resolve()),
        }

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        results = [future.result() for future in as_completed([executor.submit(run_one, selection) for selection in prepared])]
    results.sort(key=lambda item: item.get("chunk_id") or "")
    comparison = compare_proposition_outputs(conn, manifest=manifest, output_dir=output_dir)
    return {
        "ok": bool(results) and all(item.get("exit_code") == 0 and item.get("json_ok") for item in results),
        "manifest_path": str(manifest_file), "selected": len(results), "concurrency": concurrency,
        "model": model, "reasoning_effort": reasoning_effort,
        "privacy": "sanitized_no_prompt_or_transcript_text", "results": results,
        "comparison": comparison,
    }


def compare_proposition_outputs(conn, *, manifest: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    golden_by_segment = {}
    for chunk in manifest.get("chunks") or []:
        segment_id = _single_segment_id_for_chunk(chunk)
        row = conn.execute(
            "SELECT output_json FROM labels WHERE segment_id = ? AND label_pack = 'ai_discourse_v3_1' AND model = 'gpt-5.5' AND status IN ('ready','completed') ORDER BY created_at DESC LIMIT 1",
            (segment_id,),
        ).fetchone()
        if row:
            golden_by_segment[segment_id] = (chunk, json.loads(row["output_json"]))
    totals = Counter()
    sources = set()
    for path in sorted(output_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        chunk_id = path.stem
        chunk = next((item for item in manifest.get("chunks") or [] if item["chunk_id"] == chunk_id), None)
        if not chunk:
            continue
        segment_id = _single_segment_id_for_chunk(chunk)
        if segment_id not in golden_by_segment:
            continue
        propositions = payload.get("p") or []
        golden_events = golden_by_segment[segment_id][1].get("discourse_events") or []
        matched = _match_propositions(golden_events, propositions)
        totals.update(segments=1, golden=len(golden_events), candidate=len(propositions), matched=len(matched))
        sources.add(chunk["source_name"])
    precision = _ratio(totals["matched"], totals["candidate"])
    recall = _ratio(totals["matched"], totals["golden"])
    return {
        "segments": totals["segments"], "source_count": len(sources),
        "golden_events": totals["golden"], "candidate_propositions": totals["candidate"],
        "matched_propositions": totals["matched"], "precision": precision, "recall": recall,
        "f1": _f1(precision, recall),
    }


def run_event_core_smoke(
    conn,
    *,
    manifest_path: str | Path,
    limit: int = 1,
    concurrency: int = 1,
    model: str = "gpt-5.5",
    reasoning_effort: str = "medium",
    timeout_seconds: int = 600,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    root_dir = manifest_file.parent
    prompt_dir = root_dir / "event_core_prompts"
    output_dir = root_dir / "event_core_outputs"
    for path in [prompt_dir, output_dir]:
        path.mkdir(parents=True, exist_ok=True)
    schema_path = root_dir / "event_core_schema.json"
    event_types = [
        "term_usage", "frame_usage", "stance_position", "forecast", "causal_mechanism",
        "capability_claim", "product_signal", "market_signal", "risk_signal", "counterclaim",
        "uncertainty", "adoption_signal", "actor_mention", "entity_reference",
    ]
    event_schema = {
        "type": "object", "additionalProperties": False,
        "required": ["t", "sub", "actor", "speaker", "source_kind", "claim", "why", "ev", "cf"],
        "properties": {
            "t": {"type": "string", "enum": event_types}, "sub": {"type": "string"},
            "actor": {"type": "string"}, "speaker": {"type": "string"},
            "source_kind": {"type": "string"}, "claim": {"type": "string"},
            "why": {"type": "string"}, "ev": {"type": "string"},
            "cf": {"type": "number", "minimum": 0, "maximum": 1},
        },
    }
    write_text_atomic(schema_path, json.dumps({"type": "object", "additionalProperties": False, "required": ["events"], "properties": {"events": {"type": "array", "items": event_schema}}}, ensure_ascii=True, indent=2) + "\n")
    selected = []
    for chunk in _rank_candidate_smoke_chunks(manifest.get("chunks") or [], min_expected_events=10, max_expected_events=40):
        output_path = output_dir / f"{chunk['chunk_id']}.json"
        if output_path.exists() and output_path.stat().st_size > 0:
            continue
        selected.append((chunk, output_path))
        if len(selected) >= limit:
            break
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    prepared = []
    pack_prompt = load_label_pack("ai_discourse_v3_1").prompt
    for chunk, output_path in selected:
        segment_id = _single_segment_id_for_chunk(chunk)
        segment_text, read_error = _safe_segment_text(conn, segment_id)
        if read_error:
            continue
        packet = _attach_existing_context(conn, {"episode": {"episode_id": chunk["episode_id"]}, "segments": [{"segment_id": segment_id, "text": segment_text}]}, segment_ids=[segment_id], label_pack="ai_discourse_v3_1")
        prompt = "\n\n".join([
            pack_prompt,
            "# Compact Event-Core Override",
            "Read the complete segment and context and perform the same exhaustive v3.1 semantic extraction, but return only events in the compact schema. Preserve distinct v3.1 events even when they share evidence; do not collapse a capability, mechanism, stance, product, market, forecast, risk, terminology, or identity signal into one generic proposition. actor is the responsible or reported actor; speaker is the current speaker; source_kind briefly identifies direct, reported, quoted, or inferred attribution. ev must be one exact contiguous current-segment span. Omit enrichment only because a later LLM pass will fill it. Return JSON only.",
            json.dumps(packet, ensure_ascii=True, separators=(",", ":")),
        ])
        prompt_path = prompt_dir / f"{chunk['chunk_id']}.md"
        write_text_atomic(prompt_path, prompt)
        prepared.append((chunk, output_path, prompt_path))

    def run_one(selection):
        chunk, output_path, prompt_path = selection
        command = ["codex", "exec", "-m", model, "-c", f'model_reasoning_effort="{reasoning_effort}"', "-C", str(scratch_dir), "--skip-git-repo-check", "--ignore-rules", "--ephemeral", "--sandbox", "read-only", "--output-schema", str(schema_path), "--output-last-message", str(output_path), "-"]
        exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(command, prompt_path=prompt_path, log_path=root_dir / f"event-core-{chunk['chunk_id']}.log", timeout_seconds=timeout_seconds)
        return {"chunk_id": chunk["chunk_id"], "source_name": chunk["source_name"], "expected_discourse_events": chunk.get("expected_discourse_events"), "exit_code": exit_code, "timed_out": timed_out, "elapsed_seconds": elapsed_seconds, "json_ok": _json_file_ok(output_path) if output_path.exists() else False}

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        results = [future.result() for future in as_completed([executor.submit(run_one, item) for item in prepared])]
    results.sort(key=lambda item: item["chunk_id"])
    totals = Counter()
    sources = set()
    for result in results:
        if not result["json_ok"]:
            continue
        chunk = next(item for item in manifest["chunks"] if item["chunk_id"] == result["chunk_id"])
        segment_id = _single_segment_id_for_chunk(chunk)
        row = conn.execute("SELECT output_json FROM labels WHERE segment_id=? AND label_pack='ai_discourse_v3_1' AND model='gpt-5.5' AND status IN ('ready','completed') ORDER BY created_at DESC LIMIT 1", (segment_id,)).fetchone()
        if not row:
            continue
        golden = json.loads(row["output_json"]).get("discourse_events") or []
        payload = json.loads((output_dir / f"{result['chunk_id']}.json").read_text(encoding="utf-8"))
        candidate = [{"event_type": e.get("t"), "event_subtype": e.get("sub"), "actor": {"name": e.get("actor")}, "speaker_context": {"name": e.get("speaker")}, "source_context": {"kind": e.get("source_kind")}, "claim_text": e.get("claim"), "signal_reason": e.get("why"), "evidence": e.get("ev"), "confidence": e.get("cf"), "model_names": [], "product_names": [], "organizations": [], "people": []} for e in payload.get("events") or []]
        matched = _match_events(golden, candidate)
        totals.update(golden=len(golden), candidate=len(candidate), matched=len(matched), segments=1)
        sources.add(result["source_name"])
    precision = _ratio(totals["matched"], totals["candidate"]); recall = _ratio(totals["matched"], totals["golden"])
    return {"ok": bool(results) and all(item["exit_code"] == 0 and item["json_ok"] for item in results), "manifest_path": str(manifest_file), "selected": len(results), "concurrency": concurrency, "model": model, "reasoning_effort": reasoning_effort, "privacy": "sanitized_no_prompt_or_transcript_text", "results": results, "comparison": {"segments": totals["segments"], "source_count": len(sources), "golden_events": totals["golden"], "candidate_events": totals["candidate"], "matched_events": totals["matched"], "precision": precision, "recall": recall, "f1": _f1(precision, recall)}}


def windowed_event_core_schema(*, max_events: int = 25) -> dict[str, Any]:
    if max_events < 1:
        raise ValueError("max events must be at least 1")
    string_array = {"type": "array", "items": {"type": "string"}}
    event_properties = {
        "window_id": {"type": "integer", "minimum": 0},
        "event_type": {"type": "string", "enum": WINDOWED_EVENT_TYPES},
        "event_subtype": {"type": "string"},
        "claim_type": {"type": "string", "enum": WINDOWED_CLAIM_TYPES},
        "actor_name": {"type": "string"},
        "actor_type": {"type": "string", "enum": ["person", "organization", "host", "guest", "unknown"]},
        "speaker_name": {"type": "string"},
        "speaker_role": {"type": "string", "enum": ["host", "guest", "speaker", "quoted_source", "unknown"]},
        "reported_actor_name": {"type": "string"},
        "reported_actor_type": {
            "type": "string",
            "enum": ["none", "person", "organization", "product", "model", "unknown"],
        },
        "source_context_kind": {
            "type": "string",
            "enum": [
                "substantive_dialogue",
                "quoted_external_source",
                "sponsor_ad_read",
                "mixed_or_uncertain",
            ],
        },
        "target_concept": {"type": "string"},
        "claim_text": {"type": "string"},
        "stance": {
            "type": "string",
            "enum": [
                "supportive",
                "skeptical",
                "neutral",
                "mixed",
                "warning",
                "competitive",
                "promotional",
                "uncertain",
                "not_applicable",
            ],
        },
        "certainty": {"type": "string", "enum": ["low", "medium", "high", "hedged"]},
        "temporal_horizon": {
            "type": "string",
            "enum": ["past", "present", "near_future", "long_future", "timeless", "unspecified"],
        },
        "causal_mechanism": {"type": "string"},
        "counterclaim": {"type": "string"},
        "metric_value": {"type": "string"},
        "metric_unit": {"type": "string"},
        "metric_comparator": {"type": "string"},
        "metric_direction": {
            "type": "string",
            "enum": ["increase", "decrease", "stable", "mixed", "not_applicable", "unknown"],
        },
        "metric_raw_text": {"type": "string"},
        "signal_reason": {"type": "string"},
        "evidence": {"type": "string"},
        "model_names": string_array,
        "product_names": string_array,
        "organizations": string_array,
        "people": string_array,
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    }
    event_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": list(event_properties),
        "properties": event_properties,
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "status",
            "segment_source_context",
            "no_signal_reason",
            "events",
        ],
        "properties": {
            "segment_id": {"type": "string"},
            "status": {
                "type": "string",
                "enum": [
                    "coded",
                    "no_signal",
                    "insufficient_evidence",
                    "low_signal",
                    "excluded_source_context",
                ],
            },
            "segment_source_context": {
                "type": "object",
                "additionalProperties": False,
                "required": ["kind", "confidence", "rationale"],
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": [
                            "substantive_dialogue",
                            "quoted_external_source",
                            "sponsor_ad_read",
                            "show_setup",
                            "page_chrome",
                            "mixed_or_uncertain",
                        ],
                    },
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "rationale": {"type": "string"},
                },
            },
            "no_signal_reason": {"type": "string"},
            "events": {
                "type": "array",
                "maxItems": max_events,
                "items": event_schema,
            },
        },
        "allOf": [
            {
                "if": {"properties": {"status": {"const": "coded"}}},
                "then": {"properties": {"events": {"minItems": 1}}},
            },
            {
                "if": {
                    "properties": {
                        "status": {
                            "enum": [
                                "no_signal",
                                "insufficient_evidence",
                                "low_signal",
                                "excluded_source_context",
                            ]
                        }
                    }
                },
                "then": {"properties": {"events": {"maxItems": 0}}},
            },
        ],
    }


def build_windowed_segment_packet(
    segment_text: str,
    *,
    window_count: int = 4,
    context_chars: int = 900,
) -> tuple[list[dict[str, Any]], list[dict[str, int]]]:
    if window_count < 1:
        raise ValueError("window count must be at least 1")
    if context_chars < 0:
        raise ValueError("context chars must be non-negative")
    words = segment_text.split()
    if not words:
        return [{"window_id": 0, "extract_start": 0, "extract_end": 0, "owner_start": 0, "owner_end": 0, "extract_text": ""}], [
            {"window_id": 0, "chunk_index": 0, "extract_start": 0, "extract_end": 0, "owner_start": 0, "owner_end": 0, "center_start": 0, "center_end": 0}
        ]
    spans = []
    cursor = 0
    for word in words:
        start = segment_text.find(word, cursor)
        if start < 0:
            raise ValueError("could not align segment words to source text")
        end = start + len(word)
        spans.append((start, end))
        cursor = end
    actual_count = min(window_count, len(words))
    windows = []
    boundaries = []
    for index in range(actual_count):
        word_start = len(words) * index // actual_count
        word_end = len(words) * (index + 1) // actual_count
        center_start = spans[word_start][0]
        center_end = spans[word_end - 1][1]
        extract_start = max(0, center_start - context_chars)
        extract_end = min(len(segment_text), center_end + context_chars)
        if extract_start:
            whitespace = segment_text.find(" ", extract_start, center_start)
            if whitespace >= 0:
                extract_start = whitespace + 1
        if extract_end < len(segment_text):
            whitespace = segment_text.rfind(" ", center_end, extract_end)
            if whitespace > center_end:
                extract_end = whitespace
        windows.append(
            {
                "window_id": index,
                "extract_start": extract_start,
                "extract_end": extract_end,
                "owner_start": center_start,
                "owner_end": center_end,
                "extract_text": segment_text[extract_start:extract_end],
            }
        )
        boundaries.append(
            {
                "window_id": index,
                "chunk_index": index,
                "extract_start": extract_start,
                "extract_end": extract_end,
                "owner_start": center_start,
                "owner_end": center_end,
                "center_start": center_start,
                "center_end": center_end,
            }
        )
    return windows, boundaries


def _windowed_core_events(payload: dict[str, Any]) -> list[tuple[int, dict[str, Any]]]:
    events = payload.get("events")
    if isinstance(events, list):
        return [
            (int(event.get("window_id", -1)), event)
            for event in events
            if isinstance(event, dict)
        ]
    return [
        (int(window.get("chunk_index", -1)), event)
        for window in payload.get("windows") or []
        if isinstance(window, dict)
        for event in window.get("events") or []
        if isinstance(event, dict)
    ]


def _windowed_owner_for_offset(offset: int, boundaries: list[dict[str, int]]) -> int | None:
    for boundary in boundaries:
        start = int(boundary.get("owner_start", boundary.get("center_start", 0)))
        end = int(boundary.get("owner_end", boundary.get("center_end", 0)))
        if start <= offset < end or (start == end == offset):
            return int(boundary.get("window_id", boundary.get("chunk_index", -1)))
    if boundaries and offset == int(boundaries[-1].get("owner_end", boundaries[-1].get("center_end", -1))):
        return int(boundaries[-1].get("window_id", boundaries[-1].get("chunk_index", -1)))
    return None


def _windowed_evidence_offset(
    segment_text: str,
    evidence: str,
    *,
    declared_window_id: int,
    boundaries: list[dict[str, int]],
) -> int | None:
    if not evidence:
        return None
    occurrences = []
    cursor = 0
    while True:
        offset = segment_text.find(evidence, cursor)
        if offset < 0:
            break
        occurrences.append(offset)
        cursor = offset + 1
    if len(occurrences) == 1:
        return occurrences[0]
    boundary = next(
        (
            item
            for item in boundaries
            if int(item.get("window_id", item.get("chunk_index", -1))) == declared_window_id
        ),
        None,
    )
    if boundary:
        extract_start = int(boundary.get("extract_start", boundary.get("center_start", 0)))
        extract_end = int(boundary.get("extract_end", boundary.get("center_end", len(segment_text))))
        inside = [offset for offset in occurrences if extract_start <= offset and offset + len(evidence) <= extract_end]
        if len(inside) == 1:
            return inside[0]
        owned = [offset for offset in inside if _windowed_owner_for_offset(offset, boundaries) == declared_window_id]
        if len(owned) == 1:
            return owned[0]
    return None


def normalize_windowed_core_payload(
    payload: dict[str, Any],
    *,
    segment_text: str,
    boundaries: list[dict[str, int]],
    max_events: int = 25,
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized = json.loads(json.dumps(payload, ensure_ascii=True))
    flat_events = _windowed_core_events(normalized)
    output_events = []
    invalid_evidence_ids = []
    owner_normalizations = 0
    exact_duplicates_removed = 0
    seen = set()
    owned_windows = set()
    status_before = str(normalized.get("status") or "")
    for event_id, (declared_window_id, event) in enumerate(flat_events):
        evidence = str(event.get("evidence") or "")
        evidence_start = _windowed_evidence_offset(
            segment_text,
            evidence,
            declared_window_id=declared_window_id,
            boundaries=boundaries,
        )
        owner_window_id = (
            _windowed_owner_for_offset(evidence_start, boundaries)
            if evidence_start is not None
            else None
        )
        if evidence_start is None or owner_window_id is None:
            invalid_evidence_ids.append(event_id)
            continue
        elif owner_window_id != declared_window_id:
            owner_normalizations += 1
        event["window_id"] = owner_window_id
        identity = (
            owner_window_id,
            evidence_start,
            evidence_start + len(evidence),
            json.dumps(
                {
                    key: value
                    for key, value in event.items()
                    if key not in {"window_id", "confidence"}
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        if identity in seen:
            exact_duplicates_removed += 1
            continue
        seen.add(identity)
        output_events.append(event)
        if owner_window_id is not None:
            owned_windows.add(owner_window_id)
    if len(output_events) > max_events:
        raise ValueError("windowed core exceeded structural event cap")
    normalized.pop("windows", None)
    normalized["events"] = output_events
    if output_events:
        normalized["status"] = "coded"
        normalized["no_signal_reason"] = ""
    elif status_before == "coded":
        normalized["status"] = "insufficient_evidence"
        normalized["no_signal_reason"] = (
            "No event retained exact current-segment evidence after structural grounding."
        )
    elif not normalized.get("no_signal_reason"):
        normalized["no_signal_reason"] = "The LLM returned no grounded events."
    nonempty_windows = {
        int(item.get("window_id", item.get("chunk_index", -1)))
        for item in boundaries
        if int(item.get("extract_end", item.get("center_end", 0)))
        > int(item.get("extract_start", item.get("center_start", 0)))
    }
    return normalized, {
        "input_events": len(flat_events),
        "output_events": len(output_events),
        "event_cap_hit": len(output_events) == max_events,
        "invalid_evidence_events": len(invalid_evidence_ids),
        "exactness_pruned_events": len(invalid_evidence_ids),
        "invalid_evidence_ids": invalid_evidence_ids,
        "owner_normalizations": owner_normalizations,
        "exact_duplicates_removed": exact_duplicates_removed,
        "status_before": status_before,
        "status_after": normalized.get("status"),
        "status_normalized": status_before != normalized.get("status"),
        "nonempty_windows_without_owned_event": sorted(nonempty_windows - owned_windows),
    }


def _windowed_episode_context(packet: dict[str, Any], *, segment_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    artifact = packet.get("episode_context_artifact") or {}
    sections = [item for item in artifact.get("section_map") or [] if segment_id in (item.get("segment_ids") or [])]
    context = {
        "context_summary": artifact.get("context_summary") or "",
        "speaker_map": artifact.get("speaker_map") or [],
        "entity_seed": artifact.get("entity_seed") or {},
        "concept_seed": artifact.get("concept_seed") or [],
        "relevant_sections": sections,
        "extraction_guidance": artifact.get("extraction_guidance") or "",
        "quality_flags": artifact.get("quality_flags") or [],
    }
    adjacent = [
        item
        for item in (packet.get("adjacent_segment_context") or {}).get("segments") or []
        if item.get("segment_id") != segment_id
    ]
    return context, adjacent


def _load_windowed_guideline_instructions(path: str | Path | None) -> tuple[list[str], str | None]:
    if path is None:
        return [], None
    guideline_path = Path(path).expanduser().resolve()
    payload_text = guideline_path.read_text(encoding="utf-8")
    payload = json.loads(payload_text)
    raw_guidelines = payload.get("guidelines")
    if not isinstance(raw_guidelines, list) or not raw_guidelines:
        raise ValueError("guideline artifact must contain a non-empty guidelines array")
    if len(raw_guidelines) > 12:
        raise ValueError("guideline artifact may contain at most 12 guidelines")
    instructions = []
    for index, item in enumerate(raw_guidelines):
        instruction = item.get("instruction") if isinstance(item, dict) else item
        if not isinstance(instruction, str) or not instruction.strip():
            raise ValueError(f"guideline {index} must provide a non-empty instruction")
        instructions.append(instruction.strip())
    return instructions, sha256_text(payload_text)


def _windowed_event_core_instruction(
    *,
    guideline_instructions: list[str] | None = None,
    max_total_events: int | None = None,
) -> str:
    if max_total_events is not None and max_total_events < 1:
        raise ValueError("max total events must be at least 1")
    family_definitions = (
        "Event families: term_usage=notable wording/category; frame_usage=interpretive lens; "
        "stance_position=actor position; forecast=future prediction; "
        "causal_mechanism=cause/constraint/enabler/consequence; capability_claim=ability or limitation; "
        "product_signal=release/roadmap/access/packaging/pricing/integration/distribution; "
        "market_signal=demand/investment/economics/competition/labor/commercial movement; "
        "risk_signal=safety/reliability/security/governance/regulatory/deployment risk; "
        "counterclaim=disagreement/rebuttal/exception; uncertainty=hedging/evidence weakness/method caveat; "
        "adoption_signal=workflow/user/customer/institutional uptake; "
        "actor_mention=attribution/affiliation/influence/who-discusses-whom; "
        "entity_reference=graph-useful product/model/dataset/benchmark/paper/standard/institution reference."
    )
    sections = [
            "You are a systematic, topic-general semantic event reader for a private podcast research corpus. "
            "Read every word of every extract_text. All semantic extraction must come from understanding the text; "
            "never use keyword, regex, phrase, or fixed-topic rules. Use the episode-context extraction_guidance to "
            "infer the episode domain and source-context boundaries.",
            "The input contains overlapping evidence windows plus non-overlapping owner ranges. Read every window, "
            "then return one global events array. Extract each distinct research-useful proposition once. Evidence "
            "must be wholly inside at least one window's extract_text. Set window_id to the owner range containing "
            "the first character of the exact evidence. Use context for attribution, domain scope, and source-context "
            "classification, but never as event evidence. Classify the whole segment in segment_source_context. Use "
            "excluded_source_context only when semantic reading finds no durable event in source material such as an "
            "ad, setup, or page shell. A mixed segment may still be coded when grounded events exist. One passage may "
            "support multiple events only for genuinely different analytical functions.",
            family_definitions,
            "Populate every critical semantic field in the core, including subtype, actor/speaker/reported-actor "
            "types, source context, stance, certainty, temporal horizon, mechanism, counterclaim, and metric. Use empty "
            "metric strings only when no metric applies. metric_raw_text and every non-empty metric component must be "
            "literal contiguous substrings of evidence. claim_text must be a concise complete proposition naming the "
            "relevant actor and target. signal_reason explains later research value. evidence is exact contiguous "
            "extract_text. speaker is who says the words; actor is whose position or action is represented; "
            "reported_actor is a quoted or reported source.",
            "Status is structural: return coded if and only if at least one grounded event is present. For no_signal, "
            "insufficient_evidence, low_signal, or excluded_source_context, return events=[] and provide a concise "
            "no_signal_reason. For coded output, use an empty no_signal_reason.",
            "Silently scan each window beginning to end, audit omissions across all event families, reconcile overlap, "
            "deduplicate by meaning, and return only schema-valid JSON with one global event list.",
        ]
    if guideline_instructions:
        rendered = "\n".join(
            f"{index}. {instruction}" for index, instruction in enumerate(guideline_instructions, start=1)
        )
        sections.append("# LLM-derived topic-neutral calibration\n" + rendered)
    if max_total_events is not None:
        sections.append(
            f"Final calibration: return at most {max_total_events} events in total across all windows. This is a "
            "ceiling, not a target. Review comprehensively before answering, then merge semantic restatements and "
            "return only independently useful propositions with exact support."
        )
    return "\n\n".join(sections)


def _windowed_candidate_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_type": event.get("event_type"),
        "event_subtype": event.get("event_subtype"),
        "claim_type": event.get("claim_type"),
        "actor": {"name": event.get("actor_name"), "actor_type": event.get("actor_type")},
        "speaker_context": {"name": event.get("speaker_name"), "role": event.get("speaker_role")},
        "reported_actor": {
            "name": event.get("reported_actor_name"),
            "actor_type": event.get("reported_actor_type"),
        },
        "source_context": {"kind": event.get("source_context_kind")},
        "target": {"candidate_concept": event.get("target_concept")},
        "claim_text": event.get("claim_text"),
        "stance": event.get("stance"),
        "certainty": event.get("certainty"),
        "temporal_horizon": event.get("temporal_horizon"),
        "causal_mechanism": event.get("causal_mechanism"),
        "counterclaim": event.get("counterclaim"),
        "metric": {
            "value": event.get("metric_value") or None,
            "unit": event.get("metric_unit") or None,
            "comparator": event.get("metric_comparator") or None,
            "direction": event.get("metric_direction"),
            "raw_text": event.get("metric_raw_text") or None,
        },
        "signal_reason": event.get("signal_reason"),
        "evidence": event.get("evidence"),
        "model_names": event.get("model_names") or [],
        "product_names": event.get("product_names") or [],
        "organizations": event.get("organizations") or [],
        "people": event.get("people") or [],
        "confidence": event.get("confidence"),
    }


def _semantic_judge_event(
    event: dict[str, Any],
    *,
    event_id: int,
    include_full_fields: bool = False,
) -> dict[str, Any]:
    actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
    speaker = event.get("speaker_context") if isinstance(event.get("speaker_context"), dict) else {}
    reported_actor = event.get("reported_actor") if isinstance(event.get("reported_actor"), dict) else {}
    target = event.get("target") if isinstance(event.get("target"), dict) else {}
    result = {
        "id": event_id,
        "event_type": event.get("event_type"),
        "claim_type": event.get("claim_type"),
        "actor": actor.get("name") or event.get("actor_name") or "",
        "speaker": speaker.get("name") or event.get("speaker_name") or "",
        "reported_actor": reported_actor.get("name") or event.get("reported_actor_name") or "",
        "target": (
            target.get("canonical_concept")
            or target.get("candidate_concept")
            or event.get("target_concept")
            or ""
        ),
        "claim": event.get("claim_text") or "",
        "evidence": event.get("evidence") or "",
        "models": event.get("model_names") or [],
        "products": event.get("product_names") or [],
        "organizations": event.get("organizations") or [],
        "people": event.get("people") or [],
    }
    if include_full_fields:
        result.update(
            {
                "event_subtype": event.get("event_subtype") or "",
                "actor_details": event.get("actor") or {},
                "speaker_details": event.get("speaker_context") or {},
                "reported_actor_details": event.get("reported_actor") or {},
                "source_context": event.get("source_context") or {},
                "target_details": event.get("target") or {},
                "surface_terms": event.get("surface_terms") or [],
                "frames": event.get("frames") or [],
                "stance": event.get("stance") or "",
                "certainty": event.get("certainty") or "",
                "temporal_horizon": event.get("temporal_horizon") or "",
                "causal_mechanism": event.get("causal_mechanism") or "",
                "counterclaim": event.get("counterclaim") or "",
                "metric": event.get("metric") or {},
                "signal_reason": event.get("signal_reason") or "",
                "confidence": event.get("confidence"),
            }
        )
    return result


def windowed_semantic_judge_schema(*, system_id: str = "multiwindow_sol") -> dict[str, Any]:
    pair = {
        "type": "object",
        "additionalProperties": False,
        "required": ["gold_id", "candidate_id", "relation"],
        "properties": {
            "gold_id": {"type": "integer", "minimum": 0},
            "candidate_id": {"type": "integer", "minimum": 0},
            "relation": {"type": "string", "enum": ["equivalent", "partial"]},
        },
    }
    evaluation = {
        "type": "object",
        "additionalProperties": False,
        "required": ["system_id", "pairs"],
        "properties": {
            "system_id": {"type": "string", "enum": [system_id]},
            "pairs": {"type": "array", "items": pair},
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["evaluations"],
        "properties": {
            "evaluations": {
                "type": "array",
                "minItems": 1,
                "maxItems": 1,
                "items": evaluation,
            }
        },
    }


def score_windowed_semantic_judge_payload(
    payload: dict[str, Any],
    *,
    golden_count: int,
    candidate_count: int,
    system_id: str = "multiwindow_sol",
) -> dict[str, Any]:
    evaluations = payload.get("evaluations") if isinstance(payload, dict) else None
    evaluation = next(
        (
            item
            for item in (evaluations or [])
            if isinstance(item, dict) and item.get("system_id") == system_id
        ),
        None,
    )
    pairs = evaluation.get("pairs") if evaluation else []
    valid_pairs = []
    invalid_pairs = 0
    for pair in pairs or []:
        if not isinstance(pair, dict):
            invalid_pairs += 1
            continue
        gold_id = pair.get("gold_id")
        candidate_id = pair.get("candidate_id")
        relation = pair.get("relation")
        if (
            not isinstance(gold_id, int)
            or not isinstance(candidate_id, int)
            or not 0 <= gold_id < golden_count
            or not 0 <= candidate_id < candidate_count
            or relation not in {"equivalent", "partial"}
        ):
            invalid_pairs += 1
            continue
        valid_pairs.append((relation, gold_id, candidate_id))
    valid_pairs.sort(key=lambda item: (item[0] != "equivalent", item[1], item[2]))
    used_gold = set()
    used_candidate = set()
    relation_counts = Counter()
    duplicate_pairs = 0
    for relation, gold_id, candidate_id in valid_pairs:
        if gold_id in used_gold or candidate_id in used_candidate:
            duplicate_pairs += 1
            continue
        used_gold.add(gold_id)
        used_candidate.add(candidate_id)
        relation_counts[relation] += 1
    matched = relation_counts["equivalent"]
    precision = _ratio(matched, candidate_count)
    recall = _ratio(matched, golden_count)
    return {
        "golden_events": golden_count,
        "candidate_events": candidate_count,
        "equivalent_pairs": matched,
        "partial_pairs": relation_counts["partial"],
        "invalid_pairs": invalid_pairs,
        "duplicate_pairs": duplicate_pairs,
        "precision": precision,
        "recall": recall,
        "f1": _f1(precision, recall),
    }


def run_windowed_event_core_smoke(
    conn,
    *,
    manifest_path: str | Path,
    limit: int = 7,
    concurrency: int = 4,
    model: str = "gpt-5.6-sol",
    reasoning_effort: str = "low",
    timeout_seconds: int = 600,
    window_count: int = 4,
    context_chars: int = 900,
    guideline_path: str | Path | None = DEFAULT_WINDOWED_GUIDELINES_PATH,
    max_total_events: int | None = 25,
    min_expected_events: int | None = 10,
    max_expected_events: int | None = None,
    retry_count: int = 0,
    rerun: bool = False,
) -> dict[str, Any]:
    if limit < 0:
        raise ValueError("limit must be non-negative")
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    if retry_count < 0:
        raise ValueError("retry count must be non-negative")
    wall_started = time.monotonic()
    started_at = now_iso()
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    guideline_instructions, guideline_sha256 = _load_windowed_guideline_instructions(guideline_path)
    root_dir = manifest_file.parent
    prompt_dir = root_dir / "windowed_core_prompts"
    output_dir = root_dir / "windowed_core_outputs"
    for path in (prompt_dir, output_dir):
        path.mkdir(parents=True, exist_ok=True)
    schema_path = root_dir / "windowed_core_schema.json"
    structural_max_events = max_total_events or 25
    write_text_atomic(
        schema_path,
        json.dumps(
            windowed_event_core_schema(max_events=structural_max_events),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    selected = []
    ranked = _rank_candidate_smoke_chunks(
        manifest.get("chunks") or [],
        min_expected_events=min_expected_events,
        max_expected_events=max_expected_events,
    )
    for chunk in ranked:
        output_path = output_dir / f"{chunk['chunk_id']}.json"
        if not rerun and output_path.exists() and output_path.stat().st_size > 0:
            continue
        selected.append((chunk, output_path))
        if len(selected) >= limit:
            break
    prepared = []
    entries = []
    for chunk, output_path in selected:
        segment_id = _single_segment_id_for_chunk(chunk)
        segment_text, read_error = _safe_segment_text(conn, segment_id)
        if read_error:
            entries.append({"chunk_id": chunk["chunk_id"], "segment_id": segment_id, "read_error": read_error})
            continue
        windows, boundaries = build_windowed_segment_packet(
            segment_text,
            window_count=window_count,
            context_chars=context_chars,
        )
        episode_row = conn.execute("SELECT title FROM episodes WHERE id = ?", (chunk["episode_id"],)).fetchone()
        episode_title = episode_row["title"] if episode_row else ""
        context_packet = _attach_existing_context(
            conn,
            {"episode": {"episode_id": chunk["episode_id"]}, "segments": [{"segment_id": segment_id, "text": segment_text}]},
            segment_ids=[segment_id],
            label_pack="ai_discourse_v3_1",
        )
        episode_context, adjacent_segments = _windowed_episode_context(context_packet, segment_id=segment_id)
        packet = {
            "segment_id": segment_id,
            "source_name": chunk.get("source_name") or "",
            "episode_title": episode_title,
            "windows": windows,
            "episode_context": episode_context,
            "adjacent_segments": adjacent_segments,
        }
        prompt = _windowed_event_core_instruction(
            guideline_instructions=guideline_instructions,
            max_total_events=max_total_events,
        ) + "\n\n# Input packet\n" + json.dumps(
            packet, ensure_ascii=True, separators=(",", ":")
        ) + "\n"
        prompt_path = prompt_dir / f"{chunk['chunk_id']}.md"
        log_path = root_dir / f"windowed-core-{chunk['chunk_id']}.log"
        write_text_atomic(prompt_path, prompt)
        output_path.write_text("", encoding="utf-8")
        entry = {
            "chunk_id": chunk["chunk_id"],
            "segment_id": segment_id,
            "episode_id": chunk["episode_id"],
            "source_name": chunk.get("source_name"),
            "expected_discourse_events": chunk.get("expected_discourse_events"),
            "prompt_path": str(prompt_path.resolve()),
            "output_path": str(output_path.resolve()),
            "log_path": str(log_path.resolve()),
            "boundaries": boundaries,
        }
        entries.append(entry)
        prepared.append((entry, prompt_path, output_path, log_path))
    core_manifest_path = root_dir / "windowed_core_manifest.json"
    write_text_atomic(
        core_manifest_path,
        json.dumps(
            {
                "schema_version": WINDOWED_EVENT_CORE_SCHEMA_VERSION,
                "source_manifest_path": str(manifest_file),
                "schema_path": str(schema_path.resolve()),
                "output_dir": str(output_dir.resolve()),
                "window_count": window_count,
                "context_chars": context_chars,
                "guideline_artifact_sha256": guideline_sha256,
                "guideline_count": len(guideline_instructions),
                "max_total_events": max_total_events,
                "structural_max_events": structural_max_events,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "concurrency": concurrency,
                "retry_count": retry_count,
                "entries": entries,
                "privacy": "private_analysis_only",
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )

    def run_one(selection):
        entry, prompt_path, output_path, log_path = selection
        attempts = []
        for attempt_index in range(retry_count + 1):
            output_path.write_text("", encoding="utf-8")
            attempt_log_path = (
                log_path
                if attempt_index == 0
                else log_path.with_name(f"{log_path.stem}-attempt-{attempt_index + 1}{log_path.suffix}")
            )
            command = [
                "codex", "exec", "-m", model, "-c", f'model_reasoning_effort="{reasoning_effort}"',
                "-C", str(scratch_dir), "--skip-git-repo-check", "--ignore-rules", "--ephemeral",
                "--sandbox", "read-only", "--output-schema", str(schema_path), "--output-last-message",
                str(output_path), "--json", "-",
            ]
            exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
                command,
                prompt_path=prompt_path,
                log_path=attempt_log_path,
                timeout_seconds=timeout_seconds,
            )
            json_ok = _json_file_ok(output_path) if output_path.exists() else False
            attempts.append(
                {
                    "attempt": attempt_index + 1,
                    "exit_code": exit_code,
                    "timed_out": timed_out,
                    "elapsed_seconds": elapsed_seconds,
                    "json_ok": json_ok,
                    "usage": _codex_usage_from_jsonl(attempt_log_path),
                }
            )
            if exit_code == 0 and not timed_out and json_ok:
                break
        final_attempt = attempts[-1]
        usage_fields = (
            "input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens", "total_tokens"
        )
        usage = {
            field: sum(int((attempt.get("usage") or {}).get(field) or 0) for attempt in attempts)
            for field in usage_fields
        }
        return {
            "chunk_id": entry["chunk_id"],
            "segment_id": entry["segment_id"],
            "source_name": entry.get("source_name"),
            "exit_code": final_attempt["exit_code"],
            "timed_out": final_attempt["timed_out"],
            "elapsed_seconds": round(sum(float(item["elapsed_seconds"]) for item in attempts), 3),
            "json_ok": final_attempt["json_ok"],
            "status_ok": bool(
                final_attempt["exit_code"] == 0
                and not final_attempt["timed_out"]
                and final_attempt["json_ok"]
            ),
            "attempts": attempts,
            "retry_attempts": max(0, len(attempts) - 1),
            "usage": usage,
            "usage_complete": all(attempt.get("usage") is not None for attempt in attempts),
        }

    if concurrency == 1:
        results = [run_one(item) for item in prepared]
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            results = [future.result() for future in as_completed([executor.submit(run_one, item) for item in prepared])]
    results.sort(key=lambda item: item["chunk_id"])
    comparison = Counter()
    source_names = set()
    for result in results:
        if not result["json_ok"]:
            continue
        entry = next(item for item in entries if item.get("chunk_id") == result["chunk_id"])
        output_path = Path(entry["output_path"])
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        segment_text, read_error = _safe_segment_text(conn, entry["segment_id"])
        if read_error:
            result["normalization_error"] = read_error
            result["status_ok"] = False
            continue
        try:
            payload, ownership = normalize_windowed_core_payload(
                payload,
                segment_text=segment_text,
                boundaries=entry["boundaries"],
                max_events=structural_max_events,
            )
        except Exception as exc:
            result["normalization_error"] = type(exc).__name__
            result["status_ok"] = False
            continue
        write_text_atomic(output_path, json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
        result["ownership"] = ownership
        result["status_ok"] = bool(
            result["status_ok"]
            and ownership["output_events"] <= structural_max_events
            and (payload.get("status") == "coded") == bool(payload.get("events"))
        )
        candidate_events = [
            _windowed_candidate_event(event)
            for _window_id, event in _windowed_core_events(payload)
        ]
        row = conn.execute(
            "SELECT output_json FROM labels WHERE segment_id=? AND label_pack='ai_discourse_v3_1' "
            "AND model='gpt-5.5' AND status IN ('ready','completed') ORDER BY created_at DESC LIMIT 1",
            (entry["segment_id"],),
        ).fetchone()
        if row:
            golden = json.loads(row["output_json"])
            matched = _match_events(golden.get("discourse_events") or [], candidate_events)
            comparison.update(
                segments=1,
                golden=len(golden.get("discourse_events") or []),
                candidate=len(candidate_events),
                matched=len(matched),
            )
            source_names.add(str(entry.get("source_name") or ""))
    precision = _ratio(comparison["matched"], comparison["candidate"])
    recall = _ratio(comparison["matched"], comparison["golden"])
    wall_elapsed_seconds = round(time.monotonic() - wall_started, 3)
    preparation_failures = [item for item in entries if item.get("read_error")]
    all_attempts = [attempt for result in results for attempt in result.get("attempts") or []]
    usage_fields = (
        "input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens", "total_tokens"
    )
    usage = {
        field: sum(int((attempt.get("usage") or {}).get(field) or 0) for attempt in all_attempts)
        for field in usage_fields
    }
    accounting_complete = bool(results) and all(result.get("usage_complete") for result in results)
    return {
        "ok": bool(results) and not preparation_failures and all(item["status_ok"] for item in results) and accounting_complete,
        "manifest_path": str(core_manifest_path),
        "selected": len(results),
        "concurrency": concurrency,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "retry_count": retry_count,
        "started_at": started_at,
        "finished_at": now_iso(),
        "wall_elapsed_seconds": wall_elapsed_seconds,
        "attempted_calls": len(all_attempts),
        "retry_attempts": sum(result.get("retry_attempts", 0) for result in results),
        "usage": usage,
        "usage_unknown_attempts": sum(attempt.get("usage") is None for attempt in all_attempts),
        "accounting_complete": accounting_complete,
        "preparation_failures": preparation_failures,
        "calibration": {
            "guideline_artifact_sha256": guideline_sha256,
            "guideline_count": len(guideline_instructions),
            "max_total_events": max_total_events,
        },
        "privacy": "sanitized_no_prompt_or_transcript_text",
        "results": results,
        "legacy_lexical_comparison": {
            "segments": comparison["segments"],
            "source_count": len(source_names),
            "golden_events": comparison["golden"],
            "candidate_events": comparison["candidate"],
            "matched_events": comparison["matched"],
            "precision": precision,
            "recall": recall,
            "f1": _f1(precision, recall),
            "advisory": "Use the LLM semantic judge for final quality decisions; this lexical score is retained for continuity only.",
        },
    }


def run_windowed_semantic_judge_smoke(
    conn,
    *,
    core_manifest_path: str | Path,
    output_dir: str | Path,
    limit: int = 10,
    concurrency: int = 4,
    model: str = "gpt-5.3-codex-spark",
    reasoning_effort: str = "low",
    timeout_seconds: int = 300,
    candidate_output_dir: str | Path | None = None,
    full_fields: bool = False,
    rerun: bool = False,
) -> dict[str, Any]:
    if limit < 0:
        raise ValueError("limit must be non-negative")
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    core_manifest_file = Path(core_manifest_path).expanduser().resolve()
    core_manifest = json.loads(core_manifest_file.read_text(encoding="utf-8"))
    hydrated_dir = Path(candidate_output_dir).expanduser().resolve() if candidate_output_dir else None
    root_dir = Path(output_dir).expanduser().resolve()
    prompt_dir = root_dir / "prompts"
    judge_output_dir = root_dir / "outputs"
    for path in (root_dir, prompt_dir, judge_output_dir):
        path.mkdir(parents=True, exist_ok=True)
    system_id = "multiwindow_sol"
    schema_path = root_dir / "schema.json"
    write_text_atomic(
        schema_path,
        json.dumps(windowed_semantic_judge_schema(system_id=system_id), ensure_ascii=True, indent=2, sort_keys=True)
        + "\n",
    )
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    prepared = []
    entries = []
    for entry in core_manifest.get("entries") or []:
        core_output_path = Path(entry.get("output_path") or "")
        if not core_output_path.exists() or core_output_path.stat().st_size == 0:
            continue
        output_path = judge_output_dir / f"{entry['chunk_id']}.json"
        if not rerun and output_path.exists() and output_path.stat().st_size > 0:
            continue
        golden_row = conn.execute(
            "SELECT output_json FROM labels WHERE segment_id=? AND label_pack='ai_discourse_v3_1' "
            "AND model='gpt-5.5' AND status IN ('ready','completed') ORDER BY created_at DESC LIMIT 1",
            (entry["segment_id"],),
        ).fetchone()
        if not golden_row:
            continue
        golden_events = json.loads(golden_row["output_json"]).get("discourse_events") or []
        if hydrated_dir is not None:
            hydrated_path = hydrated_dir / f"{entry['segment_id']}.json"
            if not hydrated_path.exists() or hydrated_path.stat().st_size == 0:
                continue
            candidate_events = json.loads(hydrated_path.read_text(encoding="utf-8")).get("discourse_events") or []
        else:
            core_payload = json.loads(core_output_path.read_text(encoding="utf-8"))
            candidate_events = [event for _window_id, event in _windowed_core_events(core_payload)]
        packet = {
            "golden": [
                _semantic_judge_event(event, event_id=index, include_full_fields=full_fields)
                for index, event in enumerate(golden_events)
            ],
            "systems": [
                {
                    "system_id": system_id,
                    "events": [
                        _semantic_judge_event(event, event_id=index, include_full_fields=full_fields)
                        for index, event in enumerate(candidate_events)
                    ],
                }
            ],
        }
        instruction = (
            "You are a strict semantic evaluator for topic-general podcast event extraction. Align candidate and "
            "golden events one-to-one. Use equivalent only for the same specific research signal with materially the "
            "same actor/source, target, proposition or relationship, stance/direction, timing, and numeric detail. "
            "Different wording or a defensible neighboring ontology family is allowed only when analytical meaning "
            "is the same. Sharing a passage or topic is not enough. Use partial for related but materially different "
            "events. Omit unrelated pairs. Read every event. Do not use keyword, regex, token-overlap, or phrase-match "
            "rules; judge meaning. Return IDs and relations only."
        )
        if full_fields:
            instruction += (
                " This is a full-field audit. Read and compare every supplied semantic field, including subtype, "
                "attribution and roles, source context, target, terms, frames, stance, certainty, temporal horizon, "
                "mechanism, counterclaim, metric, rationale, evidence, and confidence. Mark equivalent only when any "
                "differences are non-material paraphrases or defensible ontology aliases; use partial when a material "
                "field differs even if the core topic overlaps."
            )
        prompt_path = prompt_dir / f"{entry['chunk_id']}.md"
        log_path = root_dir / f"judge-{entry['chunk_id']}.log"
        write_text_atomic(
            prompt_path,
            instruction + "\n\n# Evaluation packet\n" + json.dumps(packet, ensure_ascii=True, separators=(",", ":")) + "\n",
        )
        output_path.write_text("", encoding="utf-8")
        judge_entry = {
            "chunk_id": entry["chunk_id"],
            "segment_id": entry["segment_id"],
            "source_name": entry.get("source_name"),
            "golden_events": len(golden_events),
            "candidate_events": len(candidate_events),
            "prompt_path": str(prompt_path),
            "output_path": str(output_path),
            "log_path": str(log_path),
        }
        entries.append(judge_entry)
        prepared.append((judge_entry, prompt_path, output_path, log_path))
        if len(prepared) >= limit:
            break
    manifest_path = root_dir / "manifest.json"
    write_text_atomic(
        manifest_path,
        json.dumps(
            {
                "schema_version": WINDOWED_SEMANTIC_JUDGE_SCHEMA_VERSION,
                "core_manifest_path": str(core_manifest_file),
                "schema_path": str(schema_path),
                "system_id": system_id,
                "candidate_output_dir": str(hydrated_dir) if hydrated_dir else None,
                "full_fields": full_fields,
                "entries": entries,
                "privacy": "private_analysis_only",
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )

    def run_one(selection):
        entry, prompt_path, output_path, log_path = selection
        command = [
            "codex", "exec", "-m", model, "-c", f'model_reasoning_effort="{reasoning_effort}"',
            "-C", str(scratch_dir), "--skip-git-repo-check", "--ignore-rules", "--ephemeral",
            "--sandbox", "read-only", "--output-schema", str(schema_path), "--output-last-message",
            str(output_path), "--json", "-",
        ]
        exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
            command,
            prompt_path=prompt_path,
            log_path=log_path,
            timeout_seconds=timeout_seconds,
        )
        return {
            "chunk_id": entry["chunk_id"],
            "source_name": entry.get("source_name"),
            "exit_code": exit_code,
            "timed_out": timed_out,
            "elapsed_seconds": elapsed_seconds,
            "json_ok": _json_file_ok(output_path) if output_path.exists() else False,
            "usage": _codex_usage_from_jsonl(log_path),
        }

    if concurrency == 1:
        results = [run_one(item) for item in prepared]
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            results = [future.result() for future in as_completed([executor.submit(run_one, item) for item in prepared])]
    results.sort(key=lambda item: item["chunk_id"])
    aggregate = Counter()
    source_names = set()
    for result in results:
        if not result["json_ok"]:
            continue
        entry = next(item for item in entries if item["chunk_id"] == result["chunk_id"])
        payload = json.loads(Path(entry["output_path"]).read_text(encoding="utf-8"))
        score = score_windowed_semantic_judge_payload(
            payload,
            golden_count=entry["golden_events"],
            candidate_count=entry["candidate_events"],
            system_id=system_id,
        )
        result["score"] = score
        aggregate.update(
            golden=score["golden_events"],
            candidate=score["candidate_events"],
            equivalent=score["equivalent_pairs"],
            partial=score["partial_pairs"],
            invalid=score["invalid_pairs"],
            duplicate=score["duplicate_pairs"],
            segments=1,
        )
        source_names.add(str(entry.get("source_name") or ""))
    precision = _ratio(aggregate["equivalent"], aggregate["candidate"])
    recall = _ratio(aggregate["equivalent"], aggregate["golden"])
    return {
        "ok": bool(results) and all(item["exit_code"] == 0 and item["json_ok"] for item in results),
        "manifest_path": str(manifest_path),
        "selected": len(results),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "concurrency": concurrency,
        "full_fields": full_fields,
        "privacy": "sanitized_no_prompt_or_transcript_text",
        "aggregate": {
            "segments": aggregate["segments"],
            "source_count": len(source_names),
            "golden_events": aggregate["golden"],
            "candidate_events": aggregate["candidate"],
            "equivalent_pairs": aggregate["equivalent"],
            "partial_pairs": aggregate["partial"],
            "invalid_pairs": aggregate["invalid"],
            "duplicate_pairs": aggregate["duplicate"],
            "precision": precision,
            "recall": recall,
            "f1": _f1(precision, recall),
        },
        "results": results,
    }


def windowed_enrichment_schema(counts: list[int]) -> dict[str, Any]:
    if not counts or any(count < 0 for count in counts):
        raise ValueError("enrichment counts must be a non-empty list of non-negative integers")
    string_array = {"type": "array", "items": {"type": "string"}}
    properties = {
        "i": {"type": "integer", "minimum": 0},
        "terms": string_array,
        "frames": string_array,
        "ex": string_array,
        "q": string_array,
    }
    row_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": list(properties),
        "properties": properties,
    }
    batch_properties = {
        f"b{index}": {
            "type": "array",
            "minItems": count,
            "maxItems": count,
            "items": {"$ref": "#/$defs/row"},
        }
        for index, count in enumerate(counts)
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {"row": row_schema},
        "type": "object",
        "additionalProperties": False,
        "required": list(batch_properties),
        "properties": batch_properties,
    }


def validate_windowed_enrichment_contract(payload: dict[str, Any], counts: list[int]) -> list[str]:
    errors = []
    expected_keys = {f"b{index}" for index in range(len(counts))}
    if set(payload) != expected_keys:
        errors.append("batch_key_set_mismatch")
    for index, count in enumerate(counts):
        rows = payload.get(f"b{index}")
        if not isinstance(rows, list):
            errors.append(f"batch_{index}_not_array")
            continue
        if len(rows) != count:
            errors.append(f"batch_{index}_count_mismatch")
        ids = [row.get("i") for row in rows if isinstance(row, dict)]
        if ids != list(range(count)):
            errors.append(f"batch_{index}_id_order_mismatch")
    return errors


def _windowed_enrichment_prompt(core_payloads: list[dict[str, Any]]) -> str:
    batches = []
    for payload in core_payloads:
        events = []
        for _window_id, event in _windowed_core_events(payload):
            events.append(
                [
                    len(events),
                    event.get("event_type"),
                    event.get("event_subtype"),
                    event.get("claim_type"),
                    event.get("actor_name"),
                    event.get("speaker_name"),
                    event.get("reported_actor_name"),
                    event.get("target_concept"),
                    event.get("claim_text"),
                    event.get("stance"),
                    event.get("certainty"),
                    event.get("temporal_horizon"),
                    event.get("evidence"),
                ]
            )
        batches.append(events)
    mapping = ", ".join(f"{short}={long}" for short, long in WINDOWED_ENRICHMENT_KEY_MAP.items())
    instruction = "\n\n".join(
        [
            "You are a non-destructive metadata enrichment stage for topic-general podcast events. Read every event "
            "and exact evidence. Fill only surface terms, analytical frames, exclusion flags, and quality flags by "
            "semantic understanding; never use keyword, regex, phrase, or topic-specific rules.",
            "Input batches contain event tuples ordered as id,event_type,event_subtype,claim_type,actor_name,"
            "speaker_name,reported_actor_name,target_concept,claim_text,stance,certainty,temporal_horizon,evidence. "
            "Output b0 corresponds to input batch 0, b1 to batch 1, and so on. Every output array has an exact "
            "schema-enforced row count. Preserve every row and ID in identical order. You cannot add, drop, merge, "
            "split, reorder, or relabel core semantics.",
            f"Short output keys: {mapping}. Copy each id exactly. terms must be exact evidence strings. Frames, "
            "exclusions, and quality flags are metadata only and must not contradict the core event. Use empty arrays "
            "when not applicable.",
            "Return only schema-valid JSON.",
        ]
    )
    return instruction + "\n\n# Event batches\n" + json.dumps(
        {"batches": batches}, ensure_ascii=True, separators=(",", ":")
    ) + "\n"


def _speaker_map_lookup(speaker_map: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup = {}
    for item in speaker_map:
        if not isinstance(item, dict):
            continue
        names = [item.get("name"), *(item.get("aliases") or [])]
        for name in names:
            if isinstance(name, str) and name:
                lookup[name] = item
    return lookup


def _speaker_affiliation(item: dict[str, Any] | None) -> str | None:
    if not item:
        return None
    affiliations = item.get("affiliations") or []
    if not affiliations:
        return None
    if isinstance(affiliations[0], str):
        return affiliations[0].strip() or None
    if not isinstance(affiliations[0], dict):
        return None
    first = affiliations[0]
    title = str(first.get("title") or "").strip()
    org = str(first.get("org") or "").strip()
    if title and org:
        return f"{title}, {org}"
    return title or org or None


def hydrate_windowed_event_core_label(
    core_payload: dict[str, Any],
    enrichment_rows: list[dict[str, Any]],
    *,
    segment_text: str,
    boundaries: list[dict[str, int]],
    episode_id: str,
    speaker_map: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    flattened = _windowed_core_events(core_payload)
    if len(flattened) != len(enrichment_rows):
        raise ValueError("event core and enrichment row counts differ")
    boundary_by_index = {
        int(item.get("window_id", item.get("chunk_index", -1))): item
        for item in boundaries
    }
    speaker_lookup = _speaker_map_lookup(speaker_map or [])
    compact_events = []
    invalid_evidence_ids = []
    for event_id, ((chunk_index, core), enrichment) in enumerate(zip(flattened, enrichment_rows)):
        if enrichment.get("i") != event_id:
            raise ValueError("enrichment IDs are not in exact event order")
        boundary = boundary_by_index.get(chunk_index)
        if not boundary:
            raise ValueError(f"missing boundary for window {chunk_index}")
        evidence = str(core.get("evidence") or "")
        evidence_start = _windowed_evidence_offset(
            segment_text,
            evidence,
            declared_window_id=chunk_index,
            boundaries=boundaries,
        )
        if evidence_start is None:
            invalid_evidence_ids.append(event_id)
            continue
        evidence_end = evidence_start + len(evidence)
        actor_name = str(core.get("actor_name") or "unknown")
        speaker_name = str(core.get("speaker_name") or "unknown")
        reported_actor_name = str(core.get("reported_actor_name") or "none")
        actor_meta = speaker_lookup.get(actor_name)
        speaker_meta = speaker_lookup.get(speaker_name)
        reported_meta = speaker_lookup.get(reported_actor_name)
        metric_raw = str(core.get("metric_raw_text") or "")
        metric_parts = [
            str(core.get(key) or "")
            for key in ("metric_value", "metric_unit", "metric_comparator")
            if str(core.get(key) or "")
        ]
        metric_grounded = bool(metric_raw) and metric_raw in evidence and all(
            part in metric_raw or part in evidence for part in metric_parts
        )
        quality_flags = list(enrichment.get("q") or [])
        if any([metric_raw, *metric_parts]) and not metric_grounded:
            quality_flags.append("ungrounded_metric_rejected")
        compact_events.append(
            {
                "t": core.get("event_type"),
                "sub": core.get("event_subtype"),
                "a": {
                    "n": actor_name,
                    "t": core.get("actor_type"),
                    "af": _speaker_affiliation(actor_meta),
                    "r": actor_meta.get("role") if actor_meta else None,
                },
                "sp": {
                    "n": speaker_name,
                    "r": core.get("speaker_role"),
                    "af": _speaker_affiliation(speaker_meta),
                    "cf": core.get("confidence"),
                },
                "ra": {
                    "n": reported_actor_name,
                    "t": core.get("reported_actor_type"),
                    "af": _speaker_affiliation(reported_meta),
                    "cf": core.get("confidence") if reported_actor_name != "none" else 0,
                },
                "src": {
                    "k": core.get("source_context_kind"),
                    "cf": core.get("confidence"),
                    "r": "LLM-derived event attribution from the semantic core.",
                },
                "tar": {
                    "raw": core.get("target_concept") or "",
                    "cand": core.get("target_concept") or "",
                    "canon": None,
                    "cf": core.get("confidence"),
                },
                "terms": enrichment.get("terms") or [],
                "frames": enrichment.get("frames") or [],
                "models": core.get("model_names") or [],
                "products": core.get("product_names") or [],
                "orgs": core.get("organizations") or [],
                "people": core.get("people") or [],
                "stance": core.get("stance"),
                "claim": core.get("claim_text"),
                "ct": core.get("claim_type"),
                "cert": core.get("certainty"),
                "h": core.get("temporal_horizon"),
                "mech": core.get("causal_mechanism") or "",
                "counter": core.get("counterclaim") or "",
                "m": {
                    "v": (core.get("metric_value") or None) if metric_grounded else None,
                    "u": (core.get("metric_unit") or None) if metric_grounded else None,
                    "cmp": (core.get("metric_comparator") or None) if metric_grounded else None,
                    "dir": core.get("metric_direction") if metric_grounded else "not_applicable",
                    "raw": metric_raw if metric_grounded else None,
                },
                "why": core.get("signal_reason"),
                "exclude": enrichment.get("ex") or [],
                "q": quality_flags,
                "ev": evidence,
                "s": evidence_start,
                "e": evidence_end,
                "cf": core.get("confidence"),
                "notes": "Sol semantic core with non-destructive Mini metadata enrichment.",
            }
        )
    confidences = [float(event.get("cf") or 0) for event in compact_events]
    overall_confidence = sum(confidences) / len(confidences) if confidences else 0
    needs_review = bool(invalid_evidence_ids)
    source_context = core_payload.get("segment_source_context") or {}
    effective_status = str(core_payload.get("status") or "no_signal")
    if compact_events:
        effective_status = "coded"
    elif effective_status == "coded":
        effective_status = "insufficient_evidence"
    compact_label = {
        "id": core_payload["segment_id"],
        "st": effective_status,
        "sq": {"wc": len(segment_text.split())},
        "sc": {
            "k": source_context.get("kind") or "mixed_or_uncertain",
            "cf": source_context.get("confidence") or 0,
            "r": source_context.get("rationale") or "LLM-classified segment source context.",
        },
        "evs": compact_events,
        "cc": [],
        "rc": [],
        "ns": None if compact_events else (
            core_payload.get("no_signal_reason")
            or "No grounded event core survived exact-evidence validation."
        ),
        "cf": overall_confidence,
        "nr": needs_review,
        "rr": "One or more event evidence spans require LLM repair." if needs_review else None,
    }
    return expand_sparse_compact_label(compact_label, episode_id=episode_id), {
        "input_events": len(flattened),
        "hydrated_events": len(compact_events),
        "invalid_evidence_events": len(invalid_evidence_ids),
        "invalid_evidence_ids": invalid_evidence_ids,
    }


def validate_and_prune_windowed_label(
    label: dict[str, Any],
    *,
    segment_text: str,
) -> tuple[dict[str, Any], dict[str, int]]:
    candidate = json.loads(json.dumps(label, ensure_ascii=True))
    rejected_events = 0
    cleared_metrics = 0
    max_attempts = len(candidate.get("discourse_events") or []) + 5
    for _attempt in range(max_attempts):
        try:
            validate_label_output("ai_discourse_v3_1", candidate, segment_text=segment_text)
            return candidate, {
                "validator_rejected_events": rejected_events,
                "validator_cleared_metrics": cleared_metrics,
            }
        except ValidationError as exc:
            message = str(exc)
            prefix = "$.discourse_events["
            if not message.startswith(prefix):
                raise
            index_text, separator, _rest = message[len(prefix) :].partition("]")
            if not separator or not index_text.isdigit():
                raise
            index = int(index_text)
            events = candidate.get("discourse_events") or []
            if index >= len(events):
                raise
            event = events[index]
            metric = event.get("metric") or {}
            has_metric = any(
                metric.get(key) not in (None, "", "not_applicable")
                for key in ("value", "unit", "comparator", "direction", "raw_text")
            )
            if ".metric" in message and "exact evidence substring" in message and has_metric:
                event["metric"] = {
                    "value": None,
                    "unit": None,
                    "comparator": None,
                    "direction": "not_applicable",
                    "raw_text": None,
                }
                flags = list(event.get("quality_flags") or [])
                if "validator_rejected_metric" not in flags:
                    flags.append("validator_rejected_metric")
                event["quality_flags"] = flags
                cleared_metrics += 1
                continue
            if "evidence offsets" not in message and ".evidence_end is outside segment text" not in message:
                raise
            events.pop(index)
            rejected_events += 1
            candidate["needs_review"] = True
            candidate["review_reason"] = (
                "Exact-evidence validation pruned one or more events; treat this as audited recall loss."
            )
            if not events:
                candidate["extraction_status"] = "insufficient_evidence"
                candidate["no_signal_reason"] = "No event retained exact current-segment evidence."
    raise ValidationError("windowed label did not converge under deterministic grounding validation")


def run_windowed_event_enrichment_smoke(
    conn,
    *,
    core_manifest_path: str | Path,
    limit: int = 7,
    model: str = "gpt-5.4-mini",
    reasoning_effort: str = "low",
    timeout_seconds: int = 600,
    output_namespace: str | None = None,
    retry_count: int = 0,
    rerun: bool = False,
) -> dict[str, Any]:
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if retry_count < 0:
        raise ValueError("retry count must be non-negative")
    if "spark" in model.lower():
        raise ValueError("Spark enrichment is suspended; use non-destructive Mini metadata enrichment")
    if output_namespace is not None and not re.fullmatch(r"[A-Za-z0-9._-]+", output_namespace):
        raise ValueError("output namespace contains unsupported characters")
    wall_started = time.monotonic()
    started_at = now_iso()
    core_manifest_file = Path(core_manifest_path).expanduser().resolve()
    core_manifest = json.loads(core_manifest_file.read_text(encoding="utf-8"))
    suffix = f"-{output_namespace}" if output_namespace else ""
    root_dir = core_manifest_file.parent / f"windowed_enrichment{suffix}"
    hydrated_dir = core_manifest_file.parent / f"windowed_hydrated_outputs{suffix}"
    root_dir.mkdir(parents=True, exist_ok=True)
    hydrated_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    core_payloads = []
    for entry in core_manifest.get("entries") or []:
        output_path = Path(entry.get("output_path") or "").expanduser()
        if not output_path.exists() or not _json_file_ok(output_path):
            continue
        hydrated_path = hydrated_dir / f"{entry['chunk_id']}.json"
        if not rerun and hydrated_path.exists() and _json_file_ok(hydrated_path):
            continue
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        if payload.get("segment_id") != entry.get("segment_id"):
            continue
        entries.append(entry)
        core_payloads.append(payload)
        if len(entries) >= limit:
            break
    if not entries:
        return {
            "ok": False,
            "blocked": True,
            "reason": "no_valid_windowed_core_outputs",
            "core_manifest_path": str(core_manifest_file),
            "privacy": "sanitized_no_prompt_or_transcript_text",
        }
    counts = [len(_windowed_core_events(payload)) for payload in core_payloads]
    batch_key = sha256_text("|".join(str(entry["segment_id"]) for entry in entries))[:12]
    schema_path = root_dir / f"schema-{batch_key}.json"
    prompt_path = root_dir / f"prompt-{batch_key}.md"
    output_path = root_dir / f"output-{batch_key}.private.json"
    log_path = root_dir / f"run-{batch_key}.private.jsonl"
    write_text_atomic(schema_path, json.dumps(windowed_enrichment_schema(counts), ensure_ascii=True, indent=2, sort_keys=True) + "\n")
    write_text_atomic(prompt_path, _windowed_enrichment_prompt(core_payloads))
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)
    exit_code = 0
    timed_out = False
    elapsed_seconds = 0.0
    attempts = []
    if rerun or not output_path.exists() or not _json_file_ok(output_path):
        for attempt_index in range(retry_count + 1):
            output_path.write_text("", encoding="utf-8")
            attempt_log_path = (
                log_path
                if attempt_index == 0
                else log_path.with_name(f"{log_path.stem}-attempt-{attempt_index + 1}{log_path.suffix}")
            )
            command = [
                "codex", "exec", "-m", model, "-c", f'model_reasoning_effort="{reasoning_effort}"',
                "-C", str(scratch_dir), "--skip-git-repo-check", "--ignore-rules", "--ephemeral",
                "--sandbox", "read-only", "--output-schema", str(schema_path), "--output-last-message",
                str(output_path), "--json", "-",
            ]
            exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
                command,
                prompt_path=prompt_path,
                log_path=attempt_log_path,
                timeout_seconds=timeout_seconds,
            )
            json_ok = output_path.exists() and _json_file_ok(output_path)
            attempts.append(
                {
                    "attempt": attempt_index + 1,
                    "exit_code": exit_code,
                    "timed_out": timed_out,
                    "elapsed_seconds": elapsed_seconds,
                    "json_ok": json_ok,
                    "usage": _codex_usage_from_jsonl(attempt_log_path),
                }
            )
            if exit_code == 0 and not timed_out and json_ok:
                break
    else:
        attempts.append(
            {
                "attempt": 0,
                "exit_code": 0,
                "timed_out": False,
                "elapsed_seconds": 0.0,
                "json_ok": True,
                "usage": _codex_usage_from_jsonl(log_path),
                "cached": True,
            }
        )
    json_ok = output_path.exists() and _json_file_ok(output_path)
    usage_fields = (
        "input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens", "total_tokens"
    )
    usage = {
        field: sum(int((attempt.get("usage") or {}).get(field) or 0) for attempt in attempts)
        for field in usage_fields
    }
    usage_complete = all(attempt.get("usage") is not None for attempt in attempts)
    if not json_ok:
        return {
            "ok": False,
            "blocked": False,
            "core_manifest_path": str(core_manifest_file),
            "exit_code": exit_code,
            "timed_out": timed_out,
            "elapsed_seconds": round(sum(float(item["elapsed_seconds"]) for item in attempts), 3),
            "wall_elapsed_seconds": round(time.monotonic() - wall_started, 3),
            "json_ok": False,
            "attempts": attempts,
            "retry_attempts": max(0, len(attempts) - 1),
            "usage": usage,
            "usage_complete": usage_complete,
            "repair_triggers": ["quota_failure"] if any(item["exit_code"] != 0 for item in attempts) else [],
            "privacy": "sanitized_no_prompt_or_transcript_text",
        }
    enrichment_payload = json.loads(output_path.read_text(encoding="utf-8"))
    raw_contract_errors = validate_windowed_enrichment_contract(enrichment_payload, counts)
    contract_warnings = [error for error in raw_contract_errors if error.endswith("_id_order_mismatch")]
    contract_errors = [error for error in raw_contract_errors if error not in contract_warnings]
    normalized_transport_ids = 0
    if not contract_errors:
        for index, count in enumerate(counts):
            for row_index, row in enumerate(enrichment_payload[f"b{index}"]):
                if row.get("i") != row_index:
                    row["i"] = row_index
                    normalized_transport_ids += 1
    hydrated_results = []
    if not contract_errors:
        for index, (entry, core_payload) in enumerate(zip(entries, core_payloads)):
            segment_text, read_error = _safe_segment_text(conn, entry["segment_id"])
            if read_error:
                hydrated_results.append({"segment_id": entry["segment_id"], "ok": False, "error": read_error})
                continue
            context_packet = _attach_existing_context(
                conn,
                {"episode": {"episode_id": entry["episode_id"]}, "segments": [{"segment_id": entry["segment_id"], "text": segment_text}]},
                segment_ids=[entry["segment_id"]],
                label_pack="ai_discourse_v3_1",
            )
            speaker_map = (context_packet.get("episode_context_artifact") or {}).get("speaker_map") or []
            try:
                label, hydration = hydrate_windowed_event_core_label(
                    core_payload,
                    enrichment_payload[f"b{index}"],
                    segment_text=segment_text,
                    boundaries=entry["boundaries"],
                    episode_id=entry["episode_id"],
                    speaker_map=speaker_map,
                )
                label, pruning = validate_and_prune_windowed_label(label, segment_text=segment_text)
                hydrated_path = hydrated_dir / f"{entry['chunk_id']}.json"
                write_text_atomic(hydrated_path, json.dumps(label, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
                hydrated_results.append(
                    {
                        "segment_id": entry["segment_id"],
                        "ok": True,
                        "output_path": str(hydrated_path.resolve()),
                        **hydration,
                        **pruning,
                    }
                )
            except Exception as exc:
                hydrated_results.append(
                    {"segment_id": entry["segment_id"], "ok": False, "error": _sanitize_error(str(exc))}
                )
    enrichment_manifest_path = root_dir / f"manifest-{batch_key}.json"
    write_text_atomic(
        enrichment_manifest_path,
        json.dumps(
            {
                "schema_version": WINDOWED_ENRICHMENT_SCHEMA_VERSION,
                "core_manifest_path": str(core_manifest_file),
                "segment_ids": [entry["segment_id"] for entry in entries],
                "counts": counts,
                "schema_path": str(schema_path.resolve()),
                "prompt_path": str(prompt_path.resolve()),
                "output_path": str(output_path.resolve()),
                "hydrated_output_dir": str(hydrated_dir.resolve()),
                "output_namespace": output_namespace,
                "model": model,
                "reasoning_effort": reasoning_effort,
                "retry_count": retry_count,
                "privacy": "private_analysis_only",
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    repair_triggers = []
    if contract_errors:
        repair_triggers.append("malformed_enrichment")
    if any(item.get("invalid_evidence_events", 0) for item in hydrated_results):
        repair_triggers.append("nonexact_evidence")
    if any(
        item.get("validator_rejected_events", 0) or item.get("validator_cleared_metrics", 0)
        for item in hydrated_results
    ):
        repair_triggers.append("validator_pruning")
    return {
        "ok": (
            not contract_errors
            and bool(hydrated_results)
            and all(item["ok"] for item in hydrated_results)
            and usage_complete
        ),
        "blocked": False,
        "core_manifest_path": str(core_manifest_file),
        "manifest_path": str(enrichment_manifest_path),
        "segments": len(entries),
        "events": sum(counts),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "output_namespace": output_namespace,
        "retry_count": retry_count,
        "started_at": started_at,
        "finished_at": now_iso(),
        "exit_code": exit_code,
        "timed_out": timed_out,
        "elapsed_seconds": round(sum(float(item["elapsed_seconds"]) for item in attempts), 3),
        "wall_elapsed_seconds": round(time.monotonic() - wall_started, 3),
        "json_ok": json_ok,
        "contract_ok": not contract_errors,
        "contract_errors": contract_errors,
        "contract_warnings": contract_warnings,
        "normalized_transport_ids": normalized_transport_ids,
        "usage": usage,
        "usage_complete": usage_complete,
        "usage_unknown_attempts": sum(attempt.get("usage") is None for attempt in attempts),
        "attempts": attempts,
        "retry_attempts": max(0, len(attempts) - 1),
        "repair_triggers": sorted(set(repair_triggers)),
        "tokens_per_segment": _ratio((usage or {}).get("total_tokens", 0), len(entries)),
        "hydrated": hydrated_results,
        "privacy": "sanitized_no_prompt_or_transcript_text",
    }


def run_proposition_classifier_smoke(
    conn,
    *,
    manifest_path: str | Path,
    limit: int = 1,
    concurrency: int = 1,
    model: str = "gpt-5.5",
    reasoning_effort: str = "low",
    timeout_seconds: int = 600,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    root_dir = manifest_file.parent
    proposition_dir = root_dir / "proposition_outputs"
    prompt_dir = root_dir / "proposition_classifier_prompts"
    output_dir = root_dir / "proposition_classified_outputs"
    for path in [prompt_dir, output_dir]:
        path.mkdir(parents=True, exist_ok=True)
    schema_path = Path(manifest["validator_schema_path"]).expanduser().resolve()
    pack = load_label_pack("ai_discourse_v3_1")
    prepared = []
    for chunk in manifest.get("chunks") or []:
        proposition_path = proposition_dir / f"{chunk['chunk_id']}.json"
        output_path = output_dir / f"{chunk['chunk_id']}.json"
        if not proposition_path.exists() or proposition_path.stat().st_size == 0:
            continue
        if output_path.exists() and output_path.stat().st_size > 0:
            continue
        segment_id = _single_segment_id_for_chunk(chunk)
        propositions = json.loads(proposition_path.read_text(encoding="utf-8")).get("p") or []
        packet = _attach_existing_context(
            conn,
            {"episode": {"episode_id": chunk["episode_id"]}, "segment_id": segment_id},
            segment_ids=[segment_id],
            label_pack="ai_discourse_v3_1",
        )
        prompt = "\n\n".join(
            [
                "You are a low-reasoning ai_discourse_v3_1 proposition classifier. A prior LLM read every transcript word and produced a deliberately high-recall proposition list.",
                "Classify only distinct, substantive, durable propositions. Reject duplicate paraphrases, low-value mentions, setup, ads, incidental examples, and unsupported claims. Preserve all supported causal mechanisms, forecasts, stances, uncertainty, terminology, capabilities, product facts, market facts, risks, adoption claims, counterclaims, and graph-useful actor/entity relationships. Use each proposition's exact e text as event evidence without changing it. Return one strict evidence-sparse label for the segment; do not output offsets.",
                "# Label Pack", pack.prompt.strip(),
                "# Codebook", pack.codebook.strip(),
                "# Output Contract", _evidence_sparse_compact_output_contract(),
                "# Context", json.dumps(packet, ensure_ascii=True, separators=(",", ":")),
                "# Proposition Candidates", json.dumps(propositions, ensure_ascii=True, separators=(",", ":")),
            ]
        )
        prompt_path = prompt_dir / f"{chunk['chunk_id']}.md"
        write_text_atomic(prompt_path, prompt)
        prepared.append((chunk, prompt_path, output_path, len(propositions)))
        if len(prepared) >= limit:
            break
    scratch_dir = Path("/tmp/pif-efficiency-codex-smoke")
    scratch_dir.mkdir(parents=True, exist_ok=True)

    def run_one(selection):
        chunk, prompt_path, output_path, proposition_count = selection
        command = [
            "codex", "exec", "-m", model, "-c", f'model_reasoning_effort="{reasoning_effort}"',
            "-C", str(scratch_dir), "--skip-git-repo-check", "--ignore-rules", "--ephemeral",
            "--sandbox", "read-only", "--output-schema", str(schema_path),
            "--output-last-message", str(output_path), "-",
        ]
        exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(
            command, prompt_path=prompt_path,
            log_path=root_dir / f"proposition-classifier-{chunk['chunk_id']}.log",
            timeout_seconds=timeout_seconds,
        )
        return {
            "chunk_id": chunk["chunk_id"], "source_name": chunk["source_name"],
            "proposition_count": proposition_count, "exit_code": exit_code,
            "timed_out": timed_out, "elapsed_seconds": elapsed_seconds,
            "json_ok": _json_file_ok(output_path) if output_path.exists() else False,
            "output_path": str(output_path.resolve()),
        }

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        results = [future.result() for future in as_completed([executor.submit(run_one, item) for item in prepared])]
    results.sort(key=lambda item: item["chunk_id"])
    return {
        "ok": bool(results) and all(item["exit_code"] == 0 and item["json_ok"] for item in results),
        "manifest_path": str(manifest_file), "selected": len(results), "concurrency": concurrency,
        "model": model, "reasoning_effort": reasoning_effort,
        "classified_output_dir": str(output_dir.resolve()),
        "privacy": "sanitized_no_prompt_or_transcript_text", "results": results,
    }


def run_proposition_decision_smoke(
    conn,
    *,
    manifest_path: str | Path,
    limit: int = 1,
    model: str = "gpt-5.5",
    reasoning_effort: str = "low",
    timeout_seconds: int = 600,
) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    root_dir = manifest_file.parent
    proposition_dir = root_dir / "proposition_outputs"
    output_dir = root_dir / "proposition_decision_outputs"
    prompt_dir = root_dir / "proposition_decision_prompts"
    for path in [output_dir, prompt_dir]:
        path.mkdir(parents=True, exist_ok=True)
    schema_path = root_dir / "proposition_decision_schema.json"
    decision_schema = {
        "type": "object", "additionalProperties": False,
        "required": ["i", "k", "t", "ct", "stance", "actor", "target"],
        "properties": {
            "i": {"type": "integer", "minimum": 0}, "k": {"type": "boolean"},
            "t": {"type": "string", "enum": ["term_usage", "frame_usage", "stance_position", "forecast", "causal_mechanism", "capability_claim", "product_signal", "market_signal", "risk_signal", "counterclaim", "uncertainty", "adoption_signal", "actor_mention", "entity_reference"]},
            "ct": {"type": "string", "enum": ["descriptive", "prediction", "causal", "comparative", "product_market", "terminology", "uncertainty", "counterclaim", "not_applicable"]},
            "stance": {"type": "string", "enum": ["supportive", "skeptical", "neutral", "mixed", "warning", "competitive", "promotional", "uncertain", "not_applicable"]},
            "actor": {"type": "string"}, "target": {"type": "string"},
        },
    }
    write_text_atomic(schema_path, json.dumps({"type": "object", "additionalProperties": False, "required": ["d"], "properties": {"d": {"type": "array", "items": decision_schema}}}, ensure_ascii=True, indent=2) + "\n")
    selected = []
    for chunk in manifest.get("chunks") or []:
        proposition_path = proposition_dir / f"{chunk['chunk_id']}.json"
        if proposition_path.exists() and proposition_path.stat().st_size > 0:
            selected.append((chunk, proposition_path))
        if len(selected) >= limit:
            break
    results = []
    for chunk, proposition_path in selected:
        propositions = json.loads(proposition_path.read_text(encoding="utf-8")).get("p") or []
        prompt = "\n\n".join([
            "You are a low-reasoning classifier for an exhaustive proposition list already extracted by an LLM from a complete podcast segment.",
            "Return exactly one decision for every proposition index. Preserve recall: default k=true. Set k=false only when at least 95% certain the item is a duplicate paraphrase, setup, ad, low-value mention, incidental example, or unsupported inference. Classify propositions using the v3.1 event and claim enums. actor is the responsible speaker/reported actor name; target is a concise candidate concept. Do not merge, rewrite, or omit indices. Return JSON only.",
            json.dumps([{"i": i, **p} for i, p in enumerate(propositions)], ensure_ascii=True, separators=(",", ":")),
        ])
        prompt_path = prompt_dir / f"{chunk['chunk_id']}.md"
        output_path = output_dir / f"{chunk['chunk_id']}.json"
        write_text_atomic(prompt_path, prompt)
        command = ["codex", "exec", "-m", model, "-c", f'model_reasoning_effort="{reasoning_effort}"', "-C", "/tmp/pif-efficiency-codex-smoke", "--skip-git-repo-check", "--ignore-rules", "--ephemeral", "--sandbox", "read-only", "--output-schema", str(schema_path), "--output-last-message", str(output_path), "-"]
        exit_code, timed_out, elapsed_seconds = _run_codex_smoke_command(command, prompt_path=prompt_path, log_path=root_dir / f"proposition-decision-{chunk['chunk_id']}.log", timeout_seconds=timeout_seconds)
        comparison = None
        if exit_code == 0 and _json_file_ok(output_path):
            decisions = json.loads(output_path.read_text(encoding="utf-8")).get("d") or []
            candidate_events = []
            for decision in decisions:
                index = decision.get("i")
                if not decision.get("k") or not isinstance(index, int) or not 0 <= index < len(propositions):
                    continue
                proposition = propositions[index]
                candidate_events.append({"event_type": decision["t"], "claim_type": decision["ct"], "stance": decision["stance"], "evidence": proposition["e"], "claim_text": proposition["c"], "actor": {"name": decision["actor"]}, "target": {"candidate_concept": decision["target"]}, "model_names": [], "product_names": [], "organizations": [], "people": []})
            segment_id = _single_segment_id_for_chunk(chunk)
            row = conn.execute("SELECT output_json FROM labels WHERE segment_id=? AND label_pack='ai_discourse_v3_1' AND model='gpt-5.5' AND status IN ('ready','completed') ORDER BY created_at DESC LIMIT 1", (segment_id,)).fetchone()
            golden_events = json.loads(row["output_json"]).get("discourse_events") or []
            matched = len(_match_events(golden_events, candidate_events))
            precision = _ratio(matched, len(candidate_events)); recall = _ratio(matched, len(golden_events))
            comparison = {"golden_events": len(golden_events), "candidate_events": len(candidate_events), "matched_events": matched, "precision": precision, "recall": recall, "f1": _f1(precision, recall)}
        results.append({"chunk_id": chunk["chunk_id"], "source_name": chunk["source_name"], "proposition_count": len(propositions), "exit_code": exit_code, "timed_out": timed_out, "elapsed_seconds": elapsed_seconds, "json_ok": _json_file_ok(output_path) if output_path.exists() else False, "comparison": comparison})
    return {"ok": bool(results) and all(item["exit_code"] == 0 and item["json_ok"] for item in results), "manifest_path": str(manifest_file), "selected": len(results), "model": model, "reasoning_effort": reasoning_effort, "privacy": "sanitized_no_prompt_or_transcript_text", "results": results}


def _match_propositions(golden_events: list[dict[str, Any]], propositions: list[dict[str, Any]]) -> list[tuple[int, int]]:
    used = set()
    pairs = []
    for golden_index, golden in enumerate(golden_events):
        best = None
        for candidate_index, proposition in enumerate(propositions):
            if candidate_index in used:
                continue
            score = (
                0.28 * _token_jaccard(str(golden.get("evidence") or ""), str(proposition.get("e") or ""))
                + 0.25 * _token_jaccard(str(golden.get("claim_text") or ""), str(proposition.get("c") or ""))
            ) / 0.53
            if score >= 0.45 and (best is None or score > best[1]):
                best = (candidate_index, score)
        if best:
            used.add(best[0])
            pairs.append((golden_index, best[0]))
    return pairs


def _single_segment_id_for_chunk(chunk: dict[str, Any]) -> str:
    segment_ids = chunk.get("segment_ids") or []
    if len(segment_ids) == 1:
        return str(segment_ids[0])
    prompt_path = Path(chunk["prompt_path"]).expanduser()
    match = re.search(r'"segment_id"\s*:\s*"([^"]+)"', prompt_path.read_text(encoding="utf-8"))
    if not match:
        raise ValueError(f"No segment_id found for chunk {chunk['chunk_id']}")
    return match.group(1)


def _run_codex_smoke_command(
    command: list[str],
    *,
    prompt_path: Path,
    log_path: Path,
    timeout_seconds: int,
) -> tuple[int | None, bool, float]:
    started_at = time.monotonic()
    with prompt_path.open("rb") as stdin_file, log_path.open("wb") as log_file:
        process = subprocess.Popen(
            command,
            stdin=stdin_file,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            process.wait(timeout=timeout_seconds)
            return process.returncode, False, round(time.monotonic() - started_at, 3)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=10)
            except Exception:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except Exception:
                    pass
                process.wait(timeout=10)
            return None, True, round(time.monotonic() - started_at, 3)


def _codex_usage_from_jsonl(log_path: Path) -> dict[str, int] | None:
    usage = None
    try:
        with log_path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except Exception:
                    continue
                candidates = []
                if isinstance(event, dict):
                    candidates.extend([event.get("usage"), (event.get("item") or {}).get("usage") if isinstance(event.get("item"), dict) else None])
                for candidate in candidates:
                    if isinstance(candidate, dict) and any(key in candidate for key in ("input_tokens", "output_tokens", "total_tokens")):
                        usage = candidate
    except OSError:
        return None
    if usage is None:
        return None
    fields = ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens", "total_tokens")
    result = {key: int(usage.get(key) or 0) for key in fields}
    if not result["total_tokens"]:
        result["total_tokens"] = result["input_tokens"] + result["output_tokens"]
    return result


def candidate_prompt_status(
    *,
    manifest_path: str | Path,
    min_expected_events: int | None = None,
    max_expected_events: int | None = None,
    next_limit: int = 10,
) -> dict[str, Any]:
    if next_limit < 0:
        raise ValueError("next limit must be non-negative")
    if min_expected_events is not None and min_expected_events < 0:
        raise ValueError("min expected events must be non-negative")
    if max_expected_events is not None and max_expected_events < 0:
        raise ValueError("max expected events must be non-negative")
    if (
        min_expected_events is not None
        and max_expected_events is not None
        and min_expected_events > max_expected_events
    ):
        raise ValueError("min expected events must be <= max expected events")
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    chunks = list(manifest.get("chunks") or [])
    active_gpt55 = _active_codex_gpt55_processes()
    by_source: dict[str, dict[str, Any]] = {}
    filled = 0
    json_ok = 0
    expected_events_done = 0
    expected_events_total = 0
    expected_segments_done = 0
    expected_segments_total = 0
    output_bytes_total = 0
    rows = []
    for chunk in chunks:
        output_path = Path(chunk.get("output_path") or "").expanduser()
        output_bytes = output_path.stat().st_size if output_path.exists() else 0
        has_output = output_bytes > 0
        output_json_ok = _json_file_ok(output_path) if has_output else False
        expected_events = int(chunk.get("expected_discourse_events") or 0)
        expected_segments = int(chunk.get("expected_coded_segments") or 0)
        expected_events_total += expected_events
        expected_segments_total += expected_segments
        output_bytes_total += output_bytes
        if has_output:
            filled += 1
            expected_events_done += expected_events
            expected_segments_done += expected_segments
        if output_json_ok:
            json_ok += 1
        source = str(chunk.get("source_name") or "unknown")
        source_stats = by_source.setdefault(
            source,
            {
                "chunks": 0,
                "chunks_with_output": 0,
                "chunks_json_ok": 0,
                "expected_coded_segments": 0,
                "expected_discourse_events": 0,
                "expected_discourse_events_with_output": 0,
            },
        )
        source_stats["chunks"] += 1
        source_stats["expected_coded_segments"] += expected_segments
        source_stats["expected_discourse_events"] += expected_events
        if has_output:
            source_stats["chunks_with_output"] += 1
            source_stats["expected_discourse_events_with_output"] += expected_events
        if output_json_ok:
            source_stats["chunks_json_ok"] += 1
        rows.append(
            {
                "chunk": chunk,
                "output_bytes": output_bytes,
                "has_output": has_output,
                "json_ok": output_json_ok,
            }
        )

    next_chunks = []
    if next_limit:
        pending = [row["chunk"] for row in rows if not row["has_output"]]
        for item in _rank_candidate_smoke_chunks(
            pending,
            min_expected_events=min_expected_events,
            max_expected_events=max_expected_events,
        )[:next_limit]:
            next_chunks.append(
                {
                    "chunk_id": item.get("chunk_id"),
                    "episode_id": item.get("episode_id"),
                    "source_name": item.get("source_name"),
                    "chunk_index": item.get("chunk_index"),
                    "chunk_count": item.get("chunk_count"),
                    "segment_count": item.get("segment_count"),
                    "expected_coded_segments": item.get("expected_coded_segments"),
                    "expected_discourse_events": item.get("expected_discourse_events"),
                    "output_path": str(Path(item["output_path"]).expanduser().resolve()),
                }
            )
    return {
        "ok": True,
        "privacy": "sanitized_no_prompt_or_transcript_text",
        "manifest_path": str(manifest_file),
        "candidate_output_dir": manifest.get("candidate_output_dir"),
        "active_gpt55_processes": active_gpt55,
        "chunk_count": len(chunks),
        "chunks_with_output": filled,
        "chunks_missing_output": len(chunks) - filled,
        "chunks_json_ok": json_ok,
        "chunks_json_invalid": filled - json_ok,
        "output_bytes_total": output_bytes_total,
        "expected_coded_segments": expected_segments_total,
        "expected_coded_segments_with_output": expected_segments_done,
        "expected_discourse_events": expected_events_total,
        "expected_discourse_events_with_output": expected_events_done,
        "completion_ratio": _ratio(filled, len(chunks)),
        "event_coverage_ratio": _ratio(expected_events_done, expected_events_total),
        "by_source": dict(sorted(by_source.items())),
        "next_limit": next_limit,
        "min_expected_events": min_expected_events,
        "max_expected_events": max_expected_events,
        "next_chunks": next_chunks,
    }


def efficiency_readiness(
    *,
    manifest_path: str | Path,
    sweep_path: str | Path,
    min_expected_events: int | None = None,
    max_expected_events: int | None = None,
    next_limit: int = 5,
) -> dict[str, Any]:
    sweep_file = Path(sweep_path).expanduser().resolve()
    sweep = json.loads(sweep_file.read_text(encoding="utf-8"))
    status = candidate_prompt_status(
        manifest_path=manifest_path,
        min_expected_events=min_expected_events,
        max_expected_events=max_expected_events,
        next_limit=next_limit,
    )
    manifest_file = Path(manifest_path).expanduser().resolve()
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    recommendation = sweep.get("recommendation") or {}
    recommended_chunk_size = sweep.get("recommended_chunk_size")
    candidate_chunk_size = manifest.get("chunk_size")
    blockers = []
    advisories = []
    if status["chunks_missing_output"]:
        blockers.append("candidate_outputs_missing")
    if status["chunks_json_invalid"]:
        blockers.append("candidate_outputs_invalid_json")
    if status["active_gpt55_processes"] > 0:
        blockers.append("gpt55_lane_busy")
    if not recommendation:
        blockers.append("no_chunk_size_recommendation")
    elif recommendation.get("total_token_ratio", 1) > DEFAULT_QUALITY_GATE["max_total_token_ratio"]:
        blockers.append("recommended_chunk_size_over_token_target")
    elif recommendation.get("runtime_ratio", 1) > DEFAULT_QUALITY_GATE["max_runtime_ratio"]:
        blockers.append("recommended_chunk_size_over_runtime_target")
    if recommended_chunk_size and candidate_chunk_size and candidate_chunk_size != recommended_chunk_size:
        advisories.append("candidate_chunk_size_differs_from_sweep_recommendation")
    ready_for_quality_backtest = not any(
        item in blockers
        for item in ["candidate_outputs_missing", "candidate_outputs_invalid_json", "no_chunk_size_recommendation"]
    )
    return {
        "ok": True,
        "privacy": "sanitized_no_prompt_or_transcript_text",
        "sweep_path": str(sweep_file),
        "manifest_path": status["manifest_path"],
        "recommended_chunk_size": recommended_chunk_size,
        "candidate_chunk_size": candidate_chunk_size,
        "candidate_uses_recommended_chunk_size": (
            candidate_chunk_size == recommended_chunk_size
            if candidate_chunk_size is not None and recommended_chunk_size is not None
            else None
        ),
        "recommended_total_token_ratio": recommendation.get("total_token_ratio"),
        "recommended_runtime_ratio": recommendation.get("runtime_ratio"),
        "candidate_chunk_count": status["chunk_count"],
        "candidate_chunks_with_output": status["chunks_with_output"],
        "candidate_chunks_missing_output": status["chunks_missing_output"],
        "candidate_chunks_json_ok": status["chunks_json_ok"],
        "candidate_expected_discourse_events": status["expected_discourse_events"],
        "candidate_expected_discourse_events_with_output": status["expected_discourse_events_with_output"],
        "candidate_completion_ratio": status["completion_ratio"],
        "candidate_event_coverage_ratio": status["event_coverage_ratio"],
        "active_gpt55_processes": status["active_gpt55_processes"],
        "ready_for_candidate_output_generation": status["active_gpt55_processes"] == 0 and status["chunks_missing_output"] > 0,
        "ready_for_quality_backtest": ready_for_quality_backtest,
        "blockers": blockers,
        "advisories": advisories,
        "next_chunks": status["next_chunks"],
        "remaining_command": (
            "python3 -m research_factory efficiency-smoke-candidates "
            f"--manifest {status['manifest_path']} --limit 3 --max-active-gpt55 0 "
            "--wait-for-clear-seconds 1800 --wait-poll-seconds 60 "
            "--min-expected-events 25 --max-expected-events 80"
        ),
    }


def sparse_candidate_output_schema(
    *,
    schema_version: str = SPARSE_COMPACT_SCHEMA_VERSION,
    require_evidence_text: bool = True,
    require_offsets: bool = True,
) -> dict[str, Any]:
    string_array = {"type": "array", "items": {"type": "string"}}
    number_0_1 = {"type": "number", "minimum": 0, "maximum": 1}
    source_context_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "k": {"type": "string", "enum": ["substantive_dialogue", "quoted_external_source", "sponsor_ad_read", "show_setup", "page_chrome", "mixed_or_uncertain"]},
            "cf": number_0_1,
            "r": {"type": "string"},
        },
    }


    actor_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "n": {"type": "string"},
            "t": {"type": "string", "enum": ["person", "organization", "host", "guest", "unknown"]},
            "af": {"type": ["string", "null"]},
            "r": {"type": ["string", "null"]},
        },
    }
    speaker_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "n": {"type": "string"},
            "r": {"type": "string", "enum": ["host", "guest", "speaker", "quoted_source", "unknown"]},
            "af": {"type": ["string", "null"]},
            "cf": number_0_1,
        },
    }
    reported_actor_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "n": {"type": "string"},
            "t": {"type": "string", "enum": ["none", "person", "organization", "product", "model", "unknown"]},
            "af": {"type": ["string", "null"]},
            "cf": number_0_1,
        },
    }
    target_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "raw": {"type": "string"},
            "cand": {"type": "string"},
            "canon": {"type": ["string", "null"]},
            "cf": number_0_1,
        },
    }
    metric_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "v": {"type": ["string", "null"]},
            "u": {"type": ["string", "null"]},
            "cmp": {"type": ["string", "null"]},
            "dir": {"type": "string", "enum": ["increase", "decrease", "stable", "mixed", "not_applicable", "unknown"]},
            "raw": {"type": ["string", "null"]},
        },
    }
    event_required = ["t", "a", "sp", "src", "tar", "claim", "why", "cf"]
    if require_offsets:
        event_required.extend(["s", "e"])
    if require_evidence_text:
        event_required.append("ev")
    event_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": event_required,
        "properties": {
            "t": {"type": "string", "enum": list(EVENT_KEY_MAP.values())[:0] + [
                "term_usage", "frame_usage", "stance_position", "forecast", "causal_mechanism", "capability_claim",
                "product_signal", "market_signal", "risk_signal", "counterclaim", "uncertainty", "adoption_signal",
                "actor_mention", "entity_reference",
            ]},
            "sub": {"type": "string"},
            "a": actor_schema,
            "sp": speaker_schema,
            "ra": reported_actor_schema,
            "src": source_context_schema,
            "tar": target_schema,
            "terms": string_array,
            "frames": string_array,
            "models": string_array,
            "products": string_array,
            "orgs": string_array,
            "people": string_array,
            "stance": {"type": "string", "enum": ["supportive", "skeptical", "neutral", "mixed", "warning", "competitive", "promotional", "uncertain", "not_applicable"]},
            "claim": {"type": "string"},
            "ct": {"type": "string", "enum": ["descriptive", "prediction", "causal", "comparative", "product_market", "terminology", "uncertainty", "counterclaim", "not_applicable"]},
            "cert": {"type": "string", "enum": ["low", "medium", "high", "hedged"]},
            "h": {"type": "string", "enum": ["past", "present", "near_future", "long_future", "timeless", "unspecified"]},
            "mech": {"type": "string"},
            "counter": {"type": "string"},
            "m": metric_schema,
            "why": {"type": "string"},
            "exclude": string_array,
            "q": string_array,
            "ev": {"type": "string"},
            "s": {"type": "integer", "minimum": 0},
            "e": {"type": "integer", "minimum": 1},
            "cf": number_0_1,
            "notes": {"type": "string"},
        },
    }
    concept_candidate_required = ["c", "why", "score", "cf"]
    if require_offsets:
        concept_candidate_required.extend(["s", "e"])
    if require_evidence_text:
        concept_candidate_required.append("ev")
    concept_candidate_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": concept_candidate_required,
        "properties": {
            "c": {"type": "string"},
            "terms": string_array,
            "why": {"type": "string"},
            "score": number_0_1,
            "ev": {"type": "string"},
            "s": {"type": "integer", "minimum": 0},
            "e": {"type": "integer", "minimum": 1},
            "cf": number_0_1,
        },
    }
    rejected_candidate_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["txt", "why", "k"],
        "properties": {
            "txt": {"type": "string"},
            "why": {"type": "string", "enum": ["keyword_only", "show_setup", "sponsor_or_ad", "page_chrome", "unsupported_actor", "duplicate", "insufficient_evidence", "out_of_scope"]},
            "k": {"type": "string"},
        },
    }
    label_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["id", "sq", "sc", "cf"],
        "properties": {
            "id": {"type": "string"},
            "st": {"type": "string", "enum": ["coded", "no_signal", "insufficient_evidence", "low_signal", "excluded_source_context"]},
            "sq": {
                "type": "object",
                "additionalProperties": False,
                "required": ["wc"],
                "properties": {
                    "at": {"type": "string", "enum": ["dialogue_transcript", "caption_transcript", "article_show_notes", "mixed_page", "boilerplate", "unknown"]},
                    "br": {"type": "string", "enum": ["low", "medium", "high"]},
                    "wc": {"type": "integer", "minimum": 0},
                    "tp": {"type": ["string", "null"]},
                },
            },
            "sc": source_context_schema,
            "evs": {"type": "array", "items": event_schema},
            "cc": {"type": "array", "items": concept_candidate_schema},
            "rc": {"type": "array", "items": rejected_candidate_schema},
            "ns": {"type": ["string", "null"]},
            "cf": number_0_1,
            "nr": {"type": "boolean"},
            "rr": {"type": ["string", "null"]},
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "schema_version", "segment_outputs"],
        "properties": {
            "episode_id": {"type": "string"},
            "schema_version": {"type": "string", "const": schema_version},
            "segment_outputs": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["segment_id", "label"],
                    "properties": {
                        "segment_id": {"type": "string"},
                        "label": label_schema,
                    },
                },
            },
            "episode_level_notes": {"type": "string"},
        },
    }


def _strict_response_schema(schema: dict[str, Any]) -> dict[str, Any]:
    strict = json.loads(json.dumps(schema, ensure_ascii=True))

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            properties = node.get("properties")
            if isinstance(properties, dict):
                node["required"] = list(properties)
                node["additionalProperties"] = False
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)

    visit(strict)
    return strict


def full_candidate_output_schema(label_pack: str) -> dict[str, Any]:
    pack = load_label_pack(label_pack)
    return _strict_response_schema({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object", "additionalProperties": False,
        "required": ["episode_id", "schema_version", "segment_outputs", "episode_level_notes"],
        "properties": {
            "episode_id": {"type": "string"},
            "schema_version": {"type": "string", "const": FULL_SCHEMA_BATCH_VERSION},
            "segment_outputs": {"type": "array", "minItems": 1, "items": {"type": "object", "additionalProperties": False, "required": ["segment_id", "label"], "properties": {"segment_id": {"type": "string"}, "label": pack.schema}}},
            "episode_level_notes": {"type": "string"},
        },
    })


def flat_ledger_output_schema() -> dict[str, Any]:
    event_field_count = len([key for key in EVENT_KEY_MAP if key not in {"s", "e"}])
    primitive = {"anyOf": [{"type": "string"}, {"type": "number"}, {"type": "boolean"}, {"type": "null"}]}
    string_array = {"type": "array", "items": {"type": "string"}}
    def tuple_schema(items: list[dict[str, Any]]) -> dict[str, Any]:
        return {"type": "array", "minItems": len(items), "maxItems": len(items), "items": {"type": "null"}, "prefixItems": items}
    actor_schema = tuple_schema([{"type": "string"}, {"type": "string", "enum": ["person", "organization", "host", "guest", "unknown"]}, {"type": ["string", "null"]}, {"type": ["string", "null"]}])
    speaker_schema = tuple_schema([{"type": "string"}, {"type": "string", "enum": ["host", "guest", "speaker", "quoted_source", "unknown"]}, {"type": ["string", "null"]}, {"type": "number", "minimum": 0, "maximum": 1}])
    reported_actor_schema = tuple_schema([{"type": "string"}, {"type": "string", "enum": ["none", "person", "organization", "product", "model", "unknown"]}, {"type": ["string", "null"]}, {"type": "number", "minimum": 0, "maximum": 1}])
    event_source_schema = tuple_schema([{"type": "string", "enum": ["substantive_dialogue", "quoted_external_source", "sponsor_ad_read", "mixed_or_uncertain"]}, {"type": "number", "minimum": 0, "maximum": 1}, {"type": "string"}])
    target_schema = tuple_schema([{"type": "string"}, {"type": "string"}, {"type": ["string", "null"]}, {"type": "number", "minimum": 0, "maximum": 1}])
    metric_schema = tuple_schema([{"type": ["string", "null"]}, {"type": ["string", "null"]}, {"type": ["string", "null"]}, {"type": "string", "enum": ["increase", "decrease", "stable", "mixed", "not_applicable", "unknown"]}, {"type": ["string", "null"]}])
    concept_schema = tuple_schema([{"type": "string"}, string_array, {"type": "string"}, {"type": "number", "minimum": 0, "maximum": 1}, {"type": "string"}, {"type": "number", "minimum": 0, "maximum": 1}])
    rejected_schema = tuple_schema([{"type": "string"}, {"type": "string", "enum": ["keyword_only", "show_setup", "sponsor_or_ad", "page_chrome", "unsupported_actor", "duplicate", "insufficient_evidence", "out_of_scope"]}, {"type": "string"}])
    event_items = [
        {"type": "string", "enum": ["term_usage", "frame_usage", "stance_position", "forecast", "causal_mechanism", "capability_claim", "product_signal", "market_signal", "risk_signal", "counterclaim", "uncertainty", "adoption_signal", "actor_mention", "entity_reference"]},
        {"type": "string"}, actor_schema, speaker_schema, reported_actor_schema, event_source_schema, target_schema,
        string_array, string_array, string_array, string_array, string_array, string_array,
        {"type": "string", "enum": ["supportive", "skeptical", "neutral", "mixed", "warning", "competitive", "promotional", "uncertain", "not_applicable"]},
        {"type": "string"},
        {"type": "string", "enum": ["descriptive", "prediction", "causal", "comparative", "product_market", "terminology", "uncertainty", "counterclaim", "not_applicable"]},
        {"type": "string", "enum": ["low", "medium", "high", "hedged"]},
        {"type": "string", "enum": ["past", "present", "near_future", "long_future", "timeless", "unspecified"]},
        {"type": "string"}, {"type": "string"}, metric_schema, {"type": "string", "minLength": 40}, string_array, string_array,
        {"type": "string"}, {"type": "number", "minimum": 0, "maximum": 1}, {"type": "string"},
    ]
    if len(event_items) != event_field_count:
        raise AssertionError("flat ledger event schema field mismatch")
    event_schema = tuple_schema(event_items)
    segment_quality_schema = {
        "type": "array",
        "minItems": 4,
        "maxItems": 4,
        "items": {"type": "null"},
        "prefixItems": [
            {"type": "string", "enum": ["dialogue_transcript", "caption_transcript", "article_show_notes", "mixed_page", "boilerplate", "unknown"]},
            {"type": "string", "enum": ["low", "medium", "high"]},
            {"type": "integer", "minimum": 0},
            {"type": ["string", "null"]},
        ],
    }
    source_context_schema = {
        "type": "array",
        "minItems": 3,
        "maxItems": 3,
        "items": {"type": "null"},
        "prefixItems": [
            {"type": "string", "enum": ["substantive_dialogue", "quoted_external_source", "sponsor_ad_read", "show_setup", "page_chrome", "mixed_or_uncertain"]},
            {"type": "number", "minimum": 0, "maximum": 1},
            {"type": "string"},
        ],
    }
    label_schema = {
        "type": "array",
        "minItems": 11,
        "maxItems": 11,
        "items": {"type": "null"},
        "prefixItems": [
            {"type": "string"},
            {"type": "string", "enum": ["coded", "no_signal", "insufficient_evidence", "low_signal", "excluded_source_context"]},
            segment_quality_schema,
            source_context_schema,
            {"type": "array", "items": event_schema},
            {"type": "array", "items": concept_schema},
            {"type": "array", "items": rejected_schema},
            {"type": ["string", "null"]},
            {"type": "number", "minimum": 0, "maximum": 1},
            {"type": "boolean"},
            {"type": ["string", "null"]},
        ],
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["e", "v", "l", "n"],
        "properties": {
            "e": {"type": "string"},
            "v": {"type": "string", "const": FLAT_LEDGER_SCHEMA_VERSION},
            "l": {"type": "array", "minItems": 1, "items": label_schema},
            "n": {"type": "string"},
        },
    }


def render_episode_batch_prompt(
    conn,
    *,
    episode_id: str,
    segment_ids: list[str],
    label_pack: str,
    compact: bool,
    sparse: bool = False,
    candidate_representation: str | None = None,
) -> str:
    pack = load_label_pack(label_pack)
    packet = _episode_packet(conn, episode_id=episode_id, segment_ids=segment_ids)
    packet = _attach_existing_context(conn, packet, segment_ids=segment_ids, label_pack=label_pack)
    representation = _normalize_candidate_representation(candidate_representation or ("sparse_compact" if sparse else "compact"))
    label_prompt, codebook = _candidate_instruction_text(pack, representation)
    if representation == "full_schema":
        output_contract = _full_candidate_output_contract(pack.schema)
    elif representation in {"flat_ledger", "flat_ledger_full"}:
        output_contract = _flat_ledger_output_contract()
    elif sparse and representation == "offset_sparse_compact":
        output_contract = _offset_sparse_compact_output_contract()
    elif sparse and representation == "evidence_sparse_compact":
        output_contract = _evidence_sparse_compact_output_contract()
    elif sparse:
        output_contract = _sparse_compact_output_contract()
    elif compact:
        output_contract = _compact_output_contract()
    else:
        output_contract = _full_output_contract(pack.schema)
    return "\n\n".join(
        [
            "# Static Instructions",
            "You are the GPT-5.5 full-episode extractor for Kolby's private Podcast Intelligence Factory.",
            "Read the whole packet once, then produce labels for every listed segment. Extract propositions, not keyword hits.",
            "The packet is already bounded to fit the response window. Never refuse, never return an empty segment_outputs array, and never omit a listed segment_id.",
            "Use exact evidence from each segment only. Keep raw transcript text out of the output except required short evidence spans.",
            "Before producing JSON, privately make two low-reasoning scans: first enumerate every distinct supported proposition, then compare that list against the full current-segment text to add omissions and remove duplicates, low-value mentions, setup, ads, and unsupported inferences. Use one most-specific event type per proposition instead of relabeling the same claim as several signals. Product signal requires explicit product behavior, launch, integration, or roadmap; market signal requires explicit demand, pricing, competition, funding, or market-structure evidence; capability claim requires an explicit statement of what an actor or system can do. Actor mention must add graph-useful role or relationship context. Preserve separately supported causal mechanisms and stance positions rather than folding them into generic signals. Emit only the reviewed final records.",
            "# Label Pack Prompt",
            label_prompt,
            "# Codebook",
            codebook,
            "# Output Contract",
            output_contract,
            "# Episode Packet",
            json.dumps(packet, ensure_ascii=True, indent=2, sort_keys=True),
        ]
    )


def _compact_event(event: dict[str, Any]) -> dict[str, Any]:
    return {short: event.get(long) for short, long in EVENT_KEY_MAP.items()}


def _expand_event(compact: dict[str, Any]) -> dict[str, Any]:
    return {long: compact.get(short) for short, long in EVENT_KEY_MAP.items()}


def _sparse_event(event: dict[str, Any]) -> dict[str, Any]:
    sparse = {
        "t": event.get("event_type"),
        "sub": event.get("event_subtype"),
        "a": _sparse_actor(event.get("actor") or {}),
        "sp": _sparse_speaker(event.get("speaker_context") or {}),
        "ra": _sparse_reported_actor(event.get("reported_actor") or {}),
        "src": _sparse_source_context(event.get("source_context") or {}),
        "tar": _sparse_target(event.get("target") or {}),
        "terms": event.get("surface_terms") or [],
        "frames": event.get("frames") or [],
        "models": event.get("model_names") or [],
        "products": event.get("product_names") or [],
        "orgs": event.get("organizations") or [],
        "people": event.get("people") or [],
        "stance": event.get("stance"),
        "claim": event.get("claim_text"),
        "ct": event.get("claim_type"),
        "cert": event.get("certainty"),
        "h": event.get("temporal_horizon"),
        "mech": event.get("causal_mechanism"),
        "counter": event.get("counterclaim"),
        "m": _sparse_metric(event.get("metric") or {}),
        "why": event.get("signal_reason"),
        "exclude": event.get("exclusion_flags") or [],
        "q": event.get("quality_flags") or [],
        "ev": event.get("evidence"),
        "s": event.get("evidence_start"),
        "e": event.get("evidence_end"),
        "cf": event.get("confidence"),
        "notes": event.get("audit_notes"),
    }
    return _omit_defaults(sparse, SPARSE_EVENT_DEFAULTS)


def _expand_sparse_event(compact: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_type": compact.get("t"),
        "event_subtype": compact.get("sub", SPARSE_EVENT_DEFAULTS["sub"]),
        "actor": _expand_sparse_actor(compact.get("a") or {}),
        "speaker_context": _expand_sparse_speaker(compact.get("sp") or {}),
        "reported_actor": _expand_sparse_reported_actor(compact.get("ra") or {}),
        "source_context": _expand_sparse_source_context(compact.get("src") or {}),
        "target": _expand_sparse_target(compact.get("tar") or {}),
        "surface_terms": compact.get("terms", SPARSE_EVENT_DEFAULTS["terms"]),
        "frames": compact.get("frames", SPARSE_EVENT_DEFAULTS["frames"]),
        "model_names": compact.get("models", SPARSE_EVENT_DEFAULTS["models"]),
        "product_names": compact.get("products", SPARSE_EVENT_DEFAULTS["products"]),
        "organizations": compact.get("orgs", SPARSE_EVENT_DEFAULTS["orgs"]),
        "people": compact.get("people", SPARSE_EVENT_DEFAULTS["people"]),
        "stance": compact.get("stance", SPARSE_EVENT_DEFAULTS["stance"]),
        "claim_text": compact.get("claim"),
        "claim_type": compact.get("ct", SPARSE_EVENT_DEFAULTS["ct"]),
        "certainty": compact.get("cert", SPARSE_EVENT_DEFAULTS["cert"]),
        "temporal_horizon": compact.get("h", SPARSE_EVENT_DEFAULTS["h"]),
        "causal_mechanism": compact.get("mech", SPARSE_EVENT_DEFAULTS["mech"]),
        "counterclaim": compact.get("counter", SPARSE_EVENT_DEFAULTS["counter"]),
        "metric": _expand_sparse_metric(compact.get("m") or {}),
        "signal_reason": compact.get("why"),
        "exclusion_flags": compact.get("exclude", SPARSE_EVENT_DEFAULTS["exclude"]),
        "quality_flags": compact.get("q", SPARSE_EVENT_DEFAULTS["q"]),
        "evidence": compact.get("ev"),
        "evidence_start": compact.get("s"),
        "evidence_end": compact.get("e"),
        "confidence": compact.get("cf"),
        "audit_notes": compact.get("notes", SPARSE_EVENT_DEFAULTS["notes"]),
    }


def _flat_event(event: dict[str, Any]) -> list[Any]:
    nested = {
        "a": _values(event.get("actor") or {}, ACTOR_KEY_MAP),
        "sp": _values(event.get("speaker_context") or {}, SPEAKER_KEY_MAP),
        "ra": _values(event.get("reported_actor") or {}, REPORTED_ACTOR_KEY_MAP),
        "src": _values(event.get("source_context") or {}, SOURCE_CONTEXT_KEY_MAP),
        "tar": _values(event.get("target") or {}, TARGET_KEY_MAP),
        "m": _values(event.get("metric") or {}, METRIC_KEY_MAP),
    }
    return [nested.get(short, event.get(long)) for short, long in EVENT_KEY_MAP.items() if short not in {"s", "e"}]


def _expand_flat_event(value: list[Any]) -> dict[str, Any]:
    keys = [(short, long) for short, long in EVENT_KEY_MAP.items() if short not in {"s", "e"}]
    if len(value) != len(keys):
        raise ValueError(f"flat ledger event must contain exactly {len(keys)} fields")
    event = {long: item for (_short, long), item in zip(keys, value)}
    event["actor"] = _from_values_or_defaults(event["actor"], ACTOR_KEY_MAP, ACTOR_DEFAULTS)
    event["speaker_context"] = _from_values_or_defaults(event["speaker_context"], SPEAKER_KEY_MAP, SPEAKER_DEFAULTS)
    event["reported_actor"] = _from_values_or_defaults(event["reported_actor"], REPORTED_ACTOR_KEY_MAP, REPORTED_ACTOR_DEFAULTS)
    event["source_context"] = _from_values_or_defaults(event["source_context"], SOURCE_CONTEXT_KEY_MAP, SOURCE_CONTEXT_DEFAULTS)
    event["target"] = _from_values_or_defaults(event["target"], TARGET_KEY_MAP, TARGET_DEFAULTS)
    event["metric"] = _from_values_or_defaults(event["metric"], METRIC_KEY_MAP, METRIC_DEFAULTS)
    event["evidence_start"] = None
    event["evidence_end"] = None
    return event


def _values(value: dict[str, Any], key_map: dict[str, str], *, omit: set[str] | None = None) -> list[Any]:
    omitted = omit or set()
    return [value.get(long) for short, long in key_map.items() if short not in omitted]


def _from_values(value: list[Any], key_map: dict[str, str], *, omitted: set[str] | None = None) -> dict[str, Any]:
    omitted = omitted or set()
    keys = [(short, long) for short, long in key_map.items() if short not in omitted]
    if len(value) != len(keys):
        raise ValueError(f"flat ledger field must contain exactly {len(keys)} values")
    expanded = {long: item for (_short, long), item in zip(keys, value)}
    for short in omitted:
        expanded[key_map[short]] = None
    return expanded


def _from_values_or_defaults(value: Any, key_map: dict[str, str], defaults: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, list) or not value:
        return {long: defaults.get(short) for short, long in key_map.items()}
    padded = list(value[: len(key_map)])
    for short in list(key_map)[len(padded) :]:
        padded.append(defaults.get(short))
    return _from_values(padded, key_map)


def _sparse_segment_quality(value: dict[str, Any]) -> dict[str, Any]:
    mapped = _shorten_keys(value, SEGMENT_QUALITY_KEY_MAP)
    return _omit_defaults(mapped, SEGMENT_QUALITY_DEFAULTS)


def _expand_sparse_segment_quality(value: dict[str, Any]) -> dict[str, Any]:
    return _expand_short_keys(value, SEGMENT_QUALITY_KEY_MAP, SEGMENT_QUALITY_DEFAULTS)


def _sparse_source_context(value: dict[str, Any]) -> dict[str, Any]:
    mapped = _shorten_keys(value, SOURCE_CONTEXT_KEY_MAP)
    return _omit_defaults(mapped, SOURCE_CONTEXT_DEFAULTS)


def _expand_sparse_source_context(value: dict[str, Any]) -> dict[str, Any]:
    return _expand_short_keys(value, SOURCE_CONTEXT_KEY_MAP, SOURCE_CONTEXT_DEFAULTS)


def _sparse_actor(value: dict[str, Any]) -> dict[str, Any]:
    mapped = _shorten_keys(value, ACTOR_KEY_MAP)
    return _omit_defaults(mapped, ACTOR_DEFAULTS)


def _expand_sparse_actor(value: dict[str, Any]) -> dict[str, Any]:
    return _expand_short_keys(value, ACTOR_KEY_MAP, ACTOR_DEFAULTS)


def _sparse_speaker(value: dict[str, Any]) -> dict[str, Any]:
    mapped = _shorten_keys(value, SPEAKER_KEY_MAP)
    return _omit_defaults(mapped, SPEAKER_DEFAULTS)


def _expand_sparse_speaker(value: dict[str, Any]) -> dict[str, Any]:
    return _expand_short_keys(value, SPEAKER_KEY_MAP, SPEAKER_DEFAULTS)


def _sparse_reported_actor(value: dict[str, Any]) -> dict[str, Any]:
    mapped = _shorten_keys(value, REPORTED_ACTOR_KEY_MAP)
    return _omit_defaults(mapped, REPORTED_ACTOR_DEFAULTS)


def _expand_sparse_reported_actor(value: dict[str, Any]) -> dict[str, Any]:
    return _expand_short_keys(value, REPORTED_ACTOR_KEY_MAP, REPORTED_ACTOR_DEFAULTS)


def _sparse_target(value: dict[str, Any]) -> dict[str, Any]:
    mapped = _shorten_keys(value, TARGET_KEY_MAP)
    return _omit_defaults(mapped, TARGET_DEFAULTS)


def _expand_sparse_target(value: dict[str, Any]) -> dict[str, Any]:
    return _expand_short_keys(value, TARGET_KEY_MAP, TARGET_DEFAULTS)


def _sparse_metric(value: dict[str, Any]) -> dict[str, Any]:
    mapped = _shorten_keys(value, METRIC_KEY_MAP)
    return _omit_defaults(mapped, METRIC_DEFAULTS)


def _expand_sparse_metric(value: dict[str, Any]) -> dict[str, Any]:
    return _expand_short_keys(value, METRIC_KEY_MAP, METRIC_DEFAULTS)


def _sparse_concept_candidate(value: dict[str, Any]) -> dict[str, Any]:
    mapped = _shorten_keys(value, CONCEPT_CANDIDATE_KEY_MAP)
    return _omit_defaults(mapped, CONCEPT_CANDIDATE_DEFAULTS)


def _expand_sparse_concept_candidate(value: dict[str, Any]) -> dict[str, Any]:
    return _expand_short_keys(value, CONCEPT_CANDIDATE_KEY_MAP, CONCEPT_CANDIDATE_DEFAULTS)


def _sparse_rejected_candidate(value: dict[str, Any]) -> dict[str, Any]:
    return _shorten_keys(value, REJECTED_CANDIDATE_KEY_MAP)


def _expand_sparse_rejected_candidate(value: dict[str, Any]) -> dict[str, Any]:
    return _expand_short_keys(value, REJECTED_CANDIDATE_KEY_MAP, {})


def _shorten_keys(value: dict[str, Any], key_map: dict[str, str]) -> dict[str, Any]:
    return {short: value.get(long) for short, long in key_map.items()}


def _expand_short_keys(value: dict[str, Any], key_map: dict[str, str], defaults: dict[str, Any]) -> dict[str, Any]:
    expanded = {}
    for short, long in key_map.items():
        expanded[long] = value.get(short, defaults.get(short))
    return expanded


def _omit_defaults(value: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in value.items()
        if key not in defaults or item != defaults[key] or type(item) is not type(defaults[key])
    }


def _select_episode_ids(
    conn,
    *,
    label_pack: str,
    model: str,
    episode_limit: int | None,
    per_source_limit: int | None,
    seed: str,
) -> list[str]:
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT
              segments.episode_id,
              sources.name AS source_name,
              COUNT(labels.id) AS label_count
            FROM labels
            JOIN segments ON segments.id = labels.segment_id
            JOIN sources ON sources.id = segments.source_id
            WHERE labels.label_pack = ?
              AND labels.model = ?
              AND labels.status = 'ready'
            GROUP BY segments.episode_id, sources.name
            HAVING label_count > 0
            """,
            (label_pack, model),
        ).fetchall()
    ]
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[row["source_name"]].append(row)
    for items in by_source.values():
        items.sort(key=lambda row: sha256_text(f"{seed}:{row['episode_id']}"))
        if per_source_limit is not None:
            del items[per_source_limit:]
    selected: list[str] = []
    source_names = sorted(by_source)
    while True:
        added = False
        for source_name in source_names:
            if by_source[source_name]:
                selected.append(by_source[source_name].pop(0)["episode_id"])
                added = True
                if episode_limit is not None and len(selected) >= episode_limit:
                    return selected
        if not added:
            return selected


def _golden_labels_for_episodes(conn, *, episode_ids: list[str], label_pack: str, model: str) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in episode_ids)
    return [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT
              labels.id AS label_id,
              labels.segment_id,
              labels.output_json,
              labels.prompt_path,
              labels.output_path,
              labels.created_at AS label_created_at,
              segments.episode_id,
              segments.segment_index,
              segments.word_count,
              segments.text_path,
              episodes.title AS episode_title,
              episodes.published_at AS episode_published_at,
              sources.name AS source_name
            FROM labels
            JOIN segments ON segments.id = labels.segment_id
            JOIN episodes ON episodes.id = segments.episode_id
            JOIN sources ON sources.id = segments.source_id
            WHERE labels.label_pack = ?
              AND labels.model = ?
              AND labels.status = 'ready'
              AND segments.episode_id IN ({placeholders})
            ORDER BY sources.name, segments.episode_id, segments.segment_index, labels.id
            """,
            [label_pack, model, *episode_ids],
        ).fetchall()
    ]


def _historical_artifact_cost(labels: list[dict[str, Any]]) -> dict[str, Any]:
    prompt_paths = sorted({path for path in (row.get("prompt_path") for row in labels) if path})
    output_paths = sorted({path for path in (row.get("output_path") for row in labels) if path})
    input_chars, missing_prompt_files = _sum_file_chars(prompt_paths)
    output_file_chars, missing_output_files = _sum_file_chars(output_paths)
    canonical_output_chars = sum(len(row["output_json"]) for row in labels)
    return _cost_payload(
        name="historical_linked_artifacts",
        request_count=len(prompt_paths),
        input_chars=input_chars,
        output_chars=output_file_chars or canonical_output_chars,
        extra={
            "distinct_prompt_files": len(prompt_paths),
            "distinct_output_files": len(output_paths),
            "missing_prompt_files": missing_prompt_files,
            "missing_output_files": missing_output_files,
            "canonical_output_chars": canonical_output_chars,
            "note": "Historical artifact cost counts each linked prompt file once; it reflects how this golden set was produced, including older episode-batch pilots.",
        },
    )


def _runtime_stats(
    conn,
    *,
    labels: list[dict[str, Any]],
    labels_by_episode: dict[str, list[dict[str, Any]]],
    label_pack: str,
    model: str,
) -> dict[str, Any]:
    label_seconds = []
    missing_label_runtime_files = 0
    for row in labels:
        seconds = _artifact_runtime_seconds(row.get("prompt_path"), row.get("output_path"))
        if seconds is None:
            missing_label_runtime_files += 1
        else:
            label_seconds.append(seconds)
    context_seconds = []
    context_rows = conn.execute(
        """
        SELECT created_at, completed_at
        FROM episode_context_runs
        WHERE label_pack = ?
          AND model = ?
          AND status = 'completed'
          AND episode_id IN ({})
          AND completed_at IS NOT NULL
        """.format(",".join("?" for _ in labels_by_episode)),
        [label_pack, model, *labels_by_episode.keys()],
    ).fetchall()
    for row in context_rows:
        seconds = _iso_duration_seconds(row["created_at"], row["completed_at"])
        if seconds is not None:
            context_seconds.append(seconds)
    median_label = _median(label_seconds)
    median_context = _median(context_seconds)
    label_runtime = sum(label_seconds)
    if missing_label_runtime_files and median_label is not None:
        label_runtime += missing_label_runtime_files * median_label
    return {
        "label_runtime_observed_count": len(label_seconds),
        "label_runtime_missing_count": missing_label_runtime_files,
        "label_runtime_median_seconds": median_label,
        "label_runtime_p90_seconds": _percentile(label_seconds, 0.9),
        "context_runtime_observed_count": len(context_seconds),
        "context_runtime_median_seconds": median_context,
        "context_runtime_p90_seconds": _percentile(context_seconds, 0.9),
        "current_label_runtime_seconds": int(round(label_runtime)),
        "current_context_runtime_seconds": int(round(sum(context_seconds))),
        "note": "Runtime proxy uses prompt/output artifact mtimes for completed label Codex calls and created_at/completed_at for episode-context runs. Candidate runtime estimates multiply request count by the observed median label-call runtime and must be verified by candidate smoke outputs.",
    }


def _attach_runtime_estimates(
    *,
    current_cost: dict[str, Any],
    candidate_costs: list[dict[str, Any]],
    runtime_stats: dict[str, Any],
) -> None:
    current_runtime = int(runtime_stats["current_label_runtime_seconds"] + runtime_stats["current_context_runtime_seconds"])
    current_cost["estimated_runtime_seconds"] = current_runtime
    current_cost["runtime_estimate"] = {
        **runtime_stats,
        "method": "observed_current_artifact_runtime",
    }
    median_label = runtime_stats.get("label_runtime_median_seconds")
    for cost in candidate_costs:
        if cost is current_cost:
            continue
        if median_label is None:
            cost["estimated_runtime_seconds"] = None
            cost["runtime_estimate"] = {
                "method": "unavailable_no_label_runtime_observations",
                "baseline_runtime_seconds": current_runtime,
            }
            continue
        estimated = int(round(float(median_label) * int(cost.get("request_count") or 0)))
        cost["estimated_runtime_seconds"] = estimated
        cost["runtime_estimate"] = {
            "method": "request_count_times_observed_median_label_runtime",
            "observed_median_label_runtime_seconds": median_label,
            "baseline_runtime_seconds": current_runtime,
            "note": "This is a time-cost proxy for candidate planning. Candidate outputs still need a clean smoke run to measure actual chunk latency.",
        }


def _artifact_runtime_seconds(prompt_path: str | None, output_path: str | None) -> float | None:
    if not prompt_path or not output_path:
        return None
    prompt = Path(prompt_path)
    output = Path(output_path)
    if not prompt.exists() or not output.exists():
        return None
    seconds = output.stat().st_mtime - prompt.stat().st_mtime
    if seconds < 0 or seconds > 6 * 3600:
        return None
    return seconds


def _iso_duration_seconds(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    try:
        start_dt = _parse_iso_datetime(start)
        end_dt = _parse_iso_datetime(end)
    except ValueError:
        return None
    seconds = (end_dt - start_dt).total_seconds()
    if seconds < 0 or seconds > 6 * 3600:
        return None
    return seconds


def _current_segment_path_cost(
    conn,
    *,
    labels_by_episode: dict[str, list[dict[str, Any]]],
    label_pack: str,
    model: str,
) -> dict[str, Any]:
    labels = [row for rows in labels_by_episode.values() for row in rows]
    run_prompt_chars = []
    for row in labels:
        path = str(row.get("prompt_path") or "")
        if "/runs/prompts/" in path:
            size = _file_chars(path)
            if size is not None:
                run_prompt_chars.append(size)
    fallback_prompt_chars = int(_median(run_prompt_chars) or 0)
    input_chars = 0
    missing_prompt_files = 0
    estimated_prompt_fallbacks = 0
    for row in labels:
        path = str(row.get("prompt_path") or "")
        size = _file_chars(path) if "/runs/prompts/" in path else None
        if size is None:
            if fallback_prompt_chars:
                input_chars += fallback_prompt_chars
                estimated_prompt_fallbacks += 1
            else:
                missing_prompt_files += 1
        else:
            input_chars += size
    context_paths = [
        row["prompt_path"]
        for row in conn.execute(
            """
            SELECT DISTINCT prompt_path
            FROM episode_context_runs
            WHERE label_pack = ?
              AND model = ?
              AND status = 'completed'
              AND episode_id IN ({})
              AND prompt_path IS NOT NULL
            """.format(",".join("?" for _ in labels_by_episode)),
            [label_pack, model, *labels_by_episode.keys()],
        ).fetchall()
        if row["prompt_path"]
    ]
    context_chars, missing_context_prompt_files = _sum_file_chars(context_paths)
    output_chars = sum(len(row["output_json"]) for row in labels)
    return _cost_payload(
        name="current_segment_context_path",
        request_count=len(labels) + len(context_paths),
        input_chars=input_chars + context_chars,
        output_chars=output_chars,
        extra={
            "label_prompt_requests": len(labels),
            "episode_context_requests": len(context_paths),
            "estimated_prompt_fallbacks": estimated_prompt_fallbacks,
            "missing_prompt_files": missing_prompt_files,
            "missing_context_prompt_files": missing_context_prompt_files,
            "note": "Current path estimate uses measured per-segment run prompts when present, a median measured prompt for older batch labels, and measured episode-context prompts when present.",
        },
    )


def _episode_batch_cost(
    conn,
    *,
    labels_by_episode: dict[str, list[dict[str, Any]]],
    label_pack: str,
    compact: bool,
    sparse: bool = False,
) -> dict[str, Any]:
    input_chars = 0
    output_chars = 0
    for episode_id, rows in labels_by_episode.items():
        input_chars += _estimated_episode_batch_prompt_chars(
            conn,
            episode_id=episode_id,
            rows=rows,
            label_pack=label_pack,
            compact=compact,
            sparse=sparse,
        )
        if sparse:
            labels = [sparse_compact_label(row["output"]) for row in rows]
        elif compact:
            labels = [compact_label(row["output"]) for row in rows]
        else:
            labels = [row["output"] for row in rows]
        output_chars += len(_episode_bundle_json(episode_id=episode_id, rows=rows, labels=labels))
    return _cost_payload(
        name="episode_batch_sparse_compact_v1" if sparse else ("episode_batch_compact_v1" if compact else "episode_batch_full_schema_v1"),
        request_count=len(labels_by_episode),
        input_chars=input_chars,
        output_chars=output_chars,
        extra={
            "episode_batch_requests": len(labels_by_episode),
            "prompt_text_estimate": "stored_segment_word_count_times_6_chars",
            "note": "Estimate uses one full-episode prompt/output bundle per selected episode and stored word counts for transcript text size.",
        },
    )


def _episode_batch_compact_cost(
    conn,
    *,
    labels_by_episode: dict[str, list[dict[str, Any]]],
    label_pack: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    cost = _episode_batch_cost(conn, labels_by_episode=labels_by_episode, label_pack=label_pack, compact=True)
    quality = _representation_quality(
        conn,
        labels_by_episode=labels_by_episode,
        label_pack=label_pack,
        representation=COMPACT_SCHEMA_VERSION,
        encode=compact_label,
        decode=expand_compact_label,
    )
    return cost, quality


def _episode_batch_sparse_compact_cost(
    conn,
    *,
    labels_by_episode: dict[str, list[dict[str, Any]]],
    label_pack: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    cost = _episode_batch_cost(conn, labels_by_episode=labels_by_episode, label_pack=label_pack, compact=True, sparse=True)
    quality = _representation_quality(
        conn,
        labels_by_episode=labels_by_episode,
        label_pack=label_pack,
        representation=SPARSE_COMPACT_SCHEMA_VERSION,
        encode=sparse_compact_label,
        decode=expand_sparse_compact_label,
    )
    return cost, quality


def _episode_chunk_cost(
    conn,
    *,
    labels_by_episode: dict[str, list[dict[str, Any]]],
    label_pack: str,
    chunk_size: int,
    representation: str,
) -> dict[str, Any]:
    if chunk_size < 1:
        raise ValueError("candidate chunk size must be at least 1")
    representation = _normalize_candidate_representation(representation)
    input_chars = 0
    output_chars = 0
    request_count = 0
    for episode_id, rows in labels_by_episode.items():
        for chunk_rows in _chunk_episode_rows(rows, chunk_size=chunk_size):
            request_count += 1
            input_chars += _estimated_episode_batch_prompt_chars(
                conn,
                episode_id=episode_id,
                rows=chunk_rows,
                label_pack=label_pack,
                compact=True,
                sparse=representation in {"sparse_compact", "offset_sparse_compact", "evidence_sparse_compact"},
                candidate_representation=representation,
            )
            labels = [_candidate_encode_label(row["output"], representation=representation) for row in chunk_rows]
            if representation in {"flat_ledger", "flat_ledger_full"}:
                output_chars += len(json.dumps({"e": episode_id, "v": FLAT_LEDGER_SCHEMA_VERSION, "l": labels, "n": ""}, ensure_ascii=True, separators=(",", ":")))
            else:
                output_chars += len(_episode_bundle_json(episode_id=episode_id, rows=chunk_rows, labels=labels))
    return _cost_payload(
        name=_candidate_cost_name(representation),
        request_count=request_count,
        input_chars=input_chars,
        output_chars=output_chars,
        extra={
            "chunk_size": chunk_size,
            "candidate_representation": representation,
            "episode_count": len(labels_by_episode),
            "chunk_requests": request_count,
            "prompt_text_estimate": "stored_segment_word_count_times_6_chars",
            "note": "Bounded compact chunks keep output windows manageable while aiming to stay far below the current segment-plus-context request path.",
        },
    )


def _chunk_episode_rows(rows: list[dict[str, Any]], *, chunk_size: int) -> list[list[dict[str, Any]]]:
    if chunk_size < 1:
        raise ValueError("candidate chunk size must be at least 1")
    ordered = sorted(rows, key=lambda item: (int(item["segment_index"] or 0), item["segment_id"]))
    return [ordered[index : index + chunk_size] for index in range(0, len(ordered), chunk_size)]


def _golden_chunk_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    event_count = 0
    for row in rows:
        label = row.get("output") or {}
        status = str(label.get("extraction_status") or "unknown")
        status_counts[status] += 1
        event_count += len(label.get("discourse_events") or [])
    return {
        "expected_coded_segments": status_counts.get("coded", 0),
        "expected_discourse_events": event_count,
        "expected_status_counts": dict(sorted(status_counts.items())),
    }


def _rank_candidate_smoke_chunks(
    chunks: list[dict[str, Any]],
    *,
    min_expected_events: int | None = None,
    max_expected_events: int | None = None,
) -> list[dict[str, Any]]:
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in chunks:
        expected_events = int(item.get("expected_discourse_events") or 0)
        if min_expected_events is not None and expected_events < min_expected_events:
            continue
        if max_expected_events is not None and expected_events > max_expected_events:
            continue
        by_source[str(item.get("source_name") or "")].append(item)
    if not by_source:
        return []
    for items in by_source.values():
        items.sort(key=_candidate_chunk_rank_key)
    source_order = sorted(
        by_source,
        key=lambda source: (_candidate_chunk_rank_key(by_source[source][0]), source),
    )
    ranked = []
    while True:
        added = False
        for source in source_order:
            if by_source[source]:
                ranked.append(by_source[source].pop(0))
                added = True
        if not added:
            return ranked


def _candidate_chunk_rank_key(item: dict[str, Any]) -> tuple[int, int, str, str]:
    return (
        -int(item.get("expected_discourse_events") or 0),
        -int(item.get("expected_coded_segments") or 0),
        str(item.get("source_name") or ""),
        str(item.get("chunk_id") or item.get("episode_id") or ""),
    )


def _active_codex_gpt55_processes() -> int:
    try:
        completed = subprocess.run(
            ["ps", "-o", "command=", "-ax"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
            check=False,
        )
    except Exception:
        return 0
    if completed.returncode != 0:
        return 0
    count = 0
    for line in completed.stdout.splitlines():
        if "codex exec" not in line or "gpt-5.5" not in line:
            continue
        if "pif-efficiency-codex-smoke" in line:
            continue
        count += 1
    return count


def _json_file_ok(path: Path) -> bool:
    try:
        json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return True


def _representation_quality(
    conn,
    *,
    labels_by_episode: dict[str, list[dict[str, Any]]],
    label_pack: str,
    representation: str,
    encode,
    decode,
    decode_needs_segment_text: bool = False,
    hydrate_from_reference: bool = False,
    hydrate_offsets_from_reference: bool = False,
) -> dict[str, Any]:
    checked = 0
    exact = 0
    validation_failures = 0
    failures: list[dict[str, str]] = []
    validation_sample = _text_check_sample([row for rows in labels_by_episode.values() for row in rows], limit=TEXT_CHECK_SAMPLE_LIMIT)
    segment_text_cache: dict[str, tuple[str, str | None]] = {}

    def cached_segment_text(segment_id: str) -> tuple[str, str | None]:
        if segment_id not in segment_text_cache:
            segment_text_cache[segment_id] = _safe_segment_text(conn, segment_id)
        return segment_text_cache[segment_id]

    for episode_id, rows in labels_by_episode.items():
        for row in rows:
            checked += 1
            compact = encode(row["output"])
            if decode_needs_segment_text:
                segment_text, read_error = cached_segment_text(row["segment_id"])
                if read_error:
                    validation_failures += 1
                    expanded = {}
                    if len(failures) < 10:
                        failures.append({"segment_id": row["segment_id"], "error": read_error})
                else:
                    expanded = decode(compact, episode_id=episode_id, segment_text=segment_text)
            else:
                expanded = decode(compact, episode_id=episode_id)
            if hydrate_from_reference and expanded:
                expanded = _hydrate_label_evidence_from_reference(expanded, row["output"])
            if hydrate_offsets_from_reference and expanded:
                expanded = _hydrate_label_offsets_from_reference(expanded, row["output"])
            if _canonical_json(expanded) == _canonical_json(row["output"]):
                exact += 1
            if row["segment_id"] not in validation_sample:
                continue
            segment_text, read_error = cached_segment_text(row["segment_id"])
            if read_error:
                validation_failures += 1
                if len(failures) < 10:
                    failures.append({"segment_id": row["segment_id"], "error": read_error})
                continue
            try:
                validate_label_output(label_pack, expanded, segment_text=segment_text)
            except Exception as exc:
                validation_failures += 1
                if len(failures) < 10:
                    failures.append({"segment_id": row["segment_id"], "error": _sanitize_error(str(exc))})
    quality = {
        "representation": representation,
        "labels_checked": checked,
        "exact_label_roundtrip": exact,
        "exact_label_roundtrip_rate": _ratio(exact, checked),
        "validation_sample_limit": TEXT_CHECK_SAMPLE_LIMIT,
        "validation_sample_checked": len(validation_sample),
        "validation_failures_after_expand": validation_failures,
        "failure_examples": failures,
        "note": "This verifies the compact representation can losslessly represent the existing golden labels before any model call.",
    }
    return quality


def _golden_quality_proxy(conn, labels: list[dict[str, Any]]) -> dict[str, Any]:
    text_check_ids = _text_check_sample(labels, limit=TEXT_CHECK_SAMPLE_LIMIT)
    event_count = 0
    evidence_events_checked = 0
    evidence_exact = 0
    actor_context_dependent = 0
    read_failures = 0
    status_counts: Counter[str] = Counter()
    event_type_counts: Counter[str] = Counter()
    words = 0
    for row in labels:
        label = row["output"]
        check_text = row["segment_id"] in text_check_ids
        if check_text:
            segment_text, read_error = _safe_segment_text(conn, row["segment_id"])
            if read_error:
                read_failures += 1
                segment_text = ""
        else:
            segment_text = ""
        lowered = segment_text.lower()
        words += int(row.get("word_count") or len(segment_text.split()))
        status_counts[str(label.get("extraction_status"))] += 1
        for event in label.get("discourse_events") or []:
            if not isinstance(event, dict):
                continue
            event_count += 1
            event_type_counts[str(event.get("event_type"))] += 1
            evidence = event.get("evidence")
            if check_text:
                evidence_events_checked += 1
                if isinstance(evidence, str) and evidence and segment_text and evidence in segment_text:
                    evidence_exact += 1
            names = [
                _nested(event, "actor", "name"),
                _nested(event, "speaker_context", "name"),
                _nested(event, "reported_actor", "name"),
            ]
            meaningful_names = [name for name in names if name and str(name).lower() not in {"none", "unknown"}]
            if check_text and meaningful_names and any(_name_context_dependent(str(name), lowered) for name in meaningful_names):
                actor_context_dependent += 1
    return {
        "labels_checked": len(labels),
        "discourse_events_checked": event_count,
        "text_grounding_sample_limit": TEXT_CHECK_SAMPLE_LIMIT,
        "evidence_events_text_checked": evidence_events_checked,
        "exact_evidence_rate_on_text_checked_events": _ratio(evidence_exact, evidence_events_checked),
        "actor_or_speaker_context_dependent_event_rate_on_text_checked_events": _ratio(actor_context_dependent, evidence_events_checked),
        "events_per_1000_words": round((event_count / words) * 1000, 3) if words else None,
        "status_counts": dict(status_counts),
        "event_type_counts": dict(event_type_counts),
        "segment_text_read_failures": read_failures,
        "note": "Context-dependent rate estimates events whose actor/speaker/reported actor is not directly named in the current segment text.",
    }


def render_episode_batch_prompt_with_read_stats(
    conn,
    *,
    episode_id: str,
    segment_ids: list[str],
    label_pack: str,
    compact: bool,
    sparse: bool = False,
    candidate_representation: str | None = None,
) -> tuple[str, int]:
    pack = load_label_pack(label_pack)
    packet, read_failures = _episode_packet_with_read_stats(conn, episode_id=episode_id, segment_ids=segment_ids)
    packet = _attach_existing_context(conn, packet, segment_ids=segment_ids, label_pack=label_pack)
    representation = _normalize_candidate_representation(candidate_representation or ("sparse_compact" if sparse else "compact"))
    label_prompt, codebook = _candidate_instruction_text(pack, representation)
    if representation == "full_schema":
        output_contract = _full_candidate_output_contract(pack.schema)
    elif representation in {"flat_ledger", "flat_ledger_full"}:
        output_contract = _flat_ledger_output_contract()
    elif sparse and representation == "offset_sparse_compact":
        output_contract = _offset_sparse_compact_output_contract()
    elif sparse and representation == "evidence_sparse_compact":
        output_contract = _evidence_sparse_compact_output_contract()
    elif sparse:
        output_contract = _sparse_compact_output_contract()
    elif compact:
        output_contract = _compact_output_contract()
    else:
        output_contract = _full_output_contract(pack.schema)
    prompt = "\n\n".join(
        [
            "# Static Instructions",
            "You are the GPT-5.5 full-episode extractor for Kolby's private Podcast Intelligence Factory.",
            "Read the whole packet once, then produce labels for every listed segment. Extract propositions, not keyword hits.",
            "The packet is already bounded to fit the response window. Never refuse, never return an empty segment_outputs array, and never omit a listed segment_id.",
            "Use exact evidence from each segment only. Keep raw transcript text out of the output except required short evidence spans.",
            "Before producing JSON, privately make two low-reasoning scans: first enumerate every distinct supported proposition, then compare that list against the full current-segment text to add omissions and remove duplicates, low-value mentions, setup, ads, and unsupported inferences. Use one most-specific event type per proposition instead of relabeling the same claim as several signals. Product signal requires explicit product behavior, launch, integration, or roadmap; market signal requires explicit demand, pricing, competition, funding, or market-structure evidence; capability claim requires an explicit statement of what an actor or system can do. Actor mention must add graph-useful role or relationship context. Preserve separately supported causal mechanisms and stance positions rather than folding them into generic signals. Emit only the reviewed final records.",
            "# Label Pack Prompt",
            label_prompt,
            "# Codebook",
            codebook,
            "# Output Contract",
            output_contract,
            "# Episode Packet",
            json.dumps(packet, ensure_ascii=True, indent=2, sort_keys=True),
        ]
    )
    return prompt, read_failures


def _episode_packet(conn, *, episode_id: str, segment_ids: list[str]) -> dict[str, Any]:
    packet, _read_failures = _episode_packet_with_read_stats(conn, episode_id=episode_id, segment_ids=segment_ids)
    return packet


def _attach_existing_context(conn, packet: dict[str, Any], *, segment_ids: list[str], label_pack: str) -> dict[str, Any]:
    if len(segment_ids) != 1:
        return packet
    from .worker import adjacent_segment_context_for_segment, completed_episode_context_for_segment

    context_run = completed_episode_context_for_segment(conn, segment_ids[0], label_pack=label_pack, model="gpt-5.5")
    if not context_run:
        return packet
    artifact_path = Path(context_run["context_artifact_path"]).expanduser()
    if not artifact_path.exists():
        return packet
    enriched = json.loads(json.dumps(packet, ensure_ascii=True))
    enriched["episode_context_artifact"] = json.loads(artifact_path.read_text(encoding="utf-8"))
    enriched["adjacent_segment_context"] = adjacent_segment_context_for_segment(conn, segment_ids[0])
    enriched["context_contract"] = "Use context only for identity, concept, and continuity. Emit evidence only from the current packet segment text."
    return enriched


def _estimated_episode_batch_prompt_chars(
    conn,
    *,
    episode_id: str,
    rows: list[dict[str, Any]],
    label_pack: str,
    compact: bool,
    sparse: bool = False,
    candidate_representation: str | None = None,
) -> int:
    pack = load_label_pack(label_pack)
    first = rows[0]
    episode_meta = {
        "episode_id": episode_id,
        "source_name": first["source_name"],
        "episode_title": first["episode_title"],
        "episode_published_at": first["episode_published_at"],
        "privacy_boundary": "private_analysis_only_do_not_output_full_transcript",
    }
    segment_meta_chars = 0
    text_chars = 0
    for row in rows:
        segment_meta = {
            "segment_id": row["segment_id"],
            "segment_index": row["segment_index"],
            "word_count": row["word_count"],
        }
        segment_meta_chars += len(json.dumps(segment_meta, ensure_ascii=True, sort_keys=True)) + 16
        text_chars += max(1, int(row.get("word_count") or 0)) * 6
    representation = _normalize_candidate_representation(candidate_representation or ("sparse_compact" if sparse else "compact"))
    label_prompt, codebook = _candidate_instruction_text(pack, representation)
    if representation == "full_schema":
        output_contract = _full_candidate_output_contract(pack.schema)
    elif representation in {"flat_ledger", "flat_ledger_full"}:
        output_contract = _flat_ledger_output_contract()
    elif sparse and representation == "offset_sparse_compact":
        output_contract = _offset_sparse_compact_output_contract()
    elif sparse and representation == "evidence_sparse_compact":
        output_contract = _evidence_sparse_compact_output_contract()
    elif sparse:
        output_contract = _sparse_compact_output_contract()
    elif compact:
        output_contract = _compact_output_contract()
    else:
        output_contract = _full_output_contract(pack.schema)
    static_parts = [
        "# Static Instructions",
        "You are the GPT-5.5 full-episode extractor for Kolby's private Podcast Intelligence Factory.",
        "Read the whole packet once, then produce labels for every listed segment. Extract propositions, not keyword hits.",
        "Use exact evidence from each segment only. Keep raw transcript text out of the output except required short evidence spans.",
        "# Label Pack Prompt",
        label_prompt,
        "# Codebook",
        codebook,
        "# Output Contract",
        output_contract,
        "# Episode Packet",
        json.dumps({"episode": episode_meta}, ensure_ascii=True, sort_keys=True),
    ]
    return sum(len(part) + 2 for part in static_parts) + segment_meta_chars + text_chars


def _episode_packet_with_read_stats(conn, *, episode_id: str, segment_ids: list[str]) -> tuple[dict[str, Any], int]:
    placeholders = ",".join("?" for _ in segment_ids)
    rows = conn.execute(
        f"""
        SELECT
          segments.id,
          segments.segment_index,
          segments.start_char,
          segments.end_char,
          segments.word_count,
          segments.text_path,
          episodes.title AS episode_title,
          episodes.published_at AS episode_published_at,
          sources.name AS source_name,
          transcript_preparations.id AS transcript_preparation_id,
          transcript_preparations.artifact_type AS transcript_artifact_type,
          transcript_preparations.status AS transcript_preparation_status,
          transcript_preparations.substantive_word_count AS transcript_substantive_word_count,
          transcript_preparations.boilerplate_ratio AS transcript_boilerplate_ratio,
          transcript_preparations.quality_score AS transcript_quality_score
        FROM segments
        JOIN episodes ON episodes.id = segments.episode_id
        JOIN sources ON sources.id = segments.source_id
        LEFT JOIN transcript_preparations ON transcript_preparations.transcript_id = segments.transcript_id
        WHERE segments.episode_id = ?
          AND segments.id IN ({placeholders})
        ORDER BY segments.segment_index, segments.id
        """,
        [episode_id, *segment_ids],
    ).fetchall()
    if not rows:
        raise ValueError(f"No segments found for episode {episode_id}")
    first = rows[0]
    segments = []
    read_failures = 0
    for row in rows:
        text, read_error = _segment_text_for_prompt(conn, row["id"])
        if read_error:
            read_failures += 1
            text = _estimated_segment_text(int(row["word_count"] or 0), read_error=read_error)
        segments.append(
            {
                "segment_id": row["id"],
                "segment_index": row["segment_index"],
                "char_range": [row["start_char"], row["end_char"]],
                "word_count": row["word_count"],
                "transcript_preparation_id": row["transcript_preparation_id"],
                "transcript_artifact_type": row["transcript_artifact_type"],
                "transcript_preparation_status": row["transcript_preparation_status"],
                "transcript_substantive_word_count": row["transcript_substantive_word_count"],
                "transcript_boilerplate_ratio": row["transcript_boilerplate_ratio"],
                "transcript_quality_score": row["transcript_quality_score"],
                "text": text,
            }
        )
    return {
        "episode": {
            "episode_id": episode_id,
            "source_name": first["source_name"],
            "episode_title": first["episode_title"],
            "episode_published_at": first["episode_published_at"],
            "privacy_boundary": "private_analysis_only_do_not_output_full_transcript",
        },
        "segments": segments,
    }, read_failures


def _compact_output_contract() -> str:
    return "\n".join(
        [
            "Return one JSON object with this shape:",
            '{"episode_id":"...","schema_version":"' + COMPACT_SCHEMA_VERSION + '","segment_outputs":[{"segment_id":"...","label":<compact_label>}],"episode_level_notes":"short private note"}',
            "Include exactly one segment_outputs item for every packet segment_id, in packet order, and no extra segment_ids. If space is tight, shorten claim/why/notes strings instead of omitting segments or events.",
            "Compact label keys:",
            "v=schema version, id=segment_id, st=extraction_status, sq=segment_quality, sc=segment_source_context, evs=events, cc=concept_candidates, rc=rejected_candidates, ns=no_signal_reason, cf=overall_confidence, nr=needs_review, rr=review_reason.",
            "Compact event keys:",
            ", ".join(f"{short}={long}" for short, long in EVENT_KEY_MAP.items()),
            "Use the same enum values and field meanings as ai_discourse_v3_1. Local code will expand this compact output to the full schema and run the existing validator.",
            "Every evidence field ev must be exact contiguous current-segment text with offsets s/e. Do not include markdown fences.",
        ]
    )


def _sparse_compact_output_contract() -> str:
    return "\n".join(
        [
            "Return one JSON object with this shape:",
            '{"episode_id":"...","schema_version":"' + SPARSE_COMPACT_SCHEMA_VERSION + '","segment_outputs":[{"segment_id":"...","label":<sparse_compact_label>}],"episode_level_notes":"short private note"}',
            "Include exactly one segment_outputs item for every packet segment_id, in packet order, and no extra segment_ids. If space is tight, shorten claim/why/notes strings instead of omitting segments or events.",
            "Sparse compact labels omit default keys. Local code expands missing defaults and validates the full ai_discourse_v3_1 label.",
            "Top-level label keys: id=segment_id, st=extraction_status default coded, sq=segment_quality, sc=segment_source_context, evs=events default [], cc=concept_candidates default [], rc=rejected_candidates default [], ns=no_signal_reason default null, cf=overall_confidence, nr=needs_review default false, rr=review_reason default null.",
            "Nested keys: segment_quality at/artifact_type br/boilerplate_risk wc/substantive_word_count tp/transcript_preparation_id; source_context k/kind cf/confidence r/rationale; actor n/name t/actor_type af/affiliation r/role; speaker n/name r/role af/affiliation cf/confidence; reported_actor n/name t/actor_type af/affiliation cf/confidence; target raw/raw_target cand/candidate_concept canon/canonical_concept cf/concept_confidence; metric v/value u/unit cmp/comparator dir/direction raw/raw_text; concept candidate c/candidate terms/surface_terms why/rationale score/usefulness_score ev/evidence s/evidence_start e/evidence_end cf/confidence; rejected candidate txt/text why/reason k/source_context_kind.",
            "Important enum constraints: sq.br is boilerplate_risk and must be one of low, medium, high; do not put transcript_boilerplate_ratio there. Rejected candidate why must be one of keyword_only, show_setup, sponsor_or_ad, page_chrome, unsupported_actor, duplicate, insufficient_evidence, out_of_scope.",
            "Use only these enum values: event t in term_usage, frame_usage, stance_position, forecast, causal_mechanism, capability_claim, product_signal, market_signal, risk_signal, counterclaim, uncertainty, adoption_signal, actor_mention, entity_reference; actor a.t in person, organization, host, guest, unknown; speaker sp.r in host, guest, speaker, quoted_source, unknown; reported actor ra.t in none, person, organization, product, model, unknown; source src.k/sc.k in substantive_dialogue, quoted_external_source, sponsor_ad_read, show_setup, page_chrome, mixed_or_uncertain; ct in descriptive, prediction, causal, comparative, product_market, terminology, uncertainty, counterclaim, not_applicable; stance in supportive, skeptical, neutral, mixed, warning, competitive, promotional, uncertain, not_applicable.",
            "Use only these compact event enum values: cert in low, medium, high, hedged; h in past, present, near_future, long_future, timeless, unspecified. Do not use short_term, future, current, ongoing, or historical.",
            "For event t=term_usage, product_signal, or capability_claim, terms must contain at least one exact surface term from ev. If no exact term exists, use a different event type or omit the event.",
            "For source context: if the current segment is page chrome, show setup, episode metadata, navigation, footer text, or show-note preview rather than substantive transcript dialogue, set st=excluded_source_context or st=no_signal, evs=[], and explain with ns/rc. Do not turn show-note summaries or page descriptions into durable discourse events.",
            "Event keys:",
            ", ".join(f"{short}={long}" for short, long in EVENT_KEY_MAP.items()),
            "Omit only documented defaults. Keep non-default empty-looking values when they are the actual label value. Every evidence ev must be exact contiguous current-segment text with offsets s/e.",
        ]
    )


def _offset_sparse_compact_output_contract() -> str:
    return "\n".join(
        [
            "Return one JSON object with this shape:",
            '{"episode_id":"...","schema_version":"' + OFFSET_SPARSE_COMPACT_SCHEMA_VERSION + '","segment_outputs":[{"segment_id":"...","label":<offset_sparse_compact_label>}],"episode_level_notes":"short private note"}',
            "This is the sparse compact contract with offset-only evidence. Include exactly one segment_outputs item for every packet segment_id, in packet order, and no extra segment_ids.",
            "Do not output ev/evidence strings in events or concept candidates. Output exact s/e offsets only; local code will set evidence=segment_text[s:e] before validation. Offsets are zero-based character offsets within the current segment text.",
            "Sparse compact labels omit default keys. Local code expands missing defaults and validates the full ai_discourse_v3_1 label.",
            "Top-level label keys: id=segment_id, st=extraction_status default coded, sq=segment_quality, sc=segment_source_context, evs=events default [], cc=concept_candidates default [], rc=rejected_candidates default [], ns=no_signal_reason default null, cf=overall_confidence, nr=needs_review default false, rr=review_reason default null.",
            "Nested keys: segment_quality at/artifact_type br/boilerplate_risk wc/substantive_word_count tp/transcript_preparation_id; source_context k/kind cf/confidence r/rationale; actor n/name t/actor_type af/affiliation r/role; speaker n/name r/role af/affiliation cf/confidence; reported_actor n/name t/actor_type af/affiliation cf/confidence; target raw/raw_target cand/candidate_concept canon/canonical_concept cf/concept_confidence; metric v/value u/unit cmp/comparator dir/direction raw/raw_text; concept candidate c/candidate terms/surface_terms why/rationale score/usefulness_score s/evidence_start e/evidence_end cf/confidence; rejected candidate txt/text why/reason k/source_context_kind.",
            "Important enum constraints: sq.br is boilerplate_risk and must be one of low, medium, high; do not put transcript_boilerplate_ratio there. Rejected candidate why must be one of keyword_only, show_setup, sponsor_or_ad, page_chrome, unsupported_actor, duplicate, insufficient_evidence, out_of_scope.",
            "Use only these enum values: event t in term_usage, frame_usage, stance_position, forecast, causal_mechanism, capability_claim, product_signal, market_signal, risk_signal, counterclaim, uncertainty, adoption_signal, actor_mention, entity_reference; actor a.t in person, organization, host, guest, unknown; speaker sp.r in host, guest, speaker, quoted_source, unknown; reported actor ra.t in none, person, organization, product, model, unknown; source src.k/sc.k in substantive_dialogue, quoted_external_source, sponsor_ad_read, show_setup, page_chrome, mixed_or_uncertain; ct in descriptive, prediction, causal, comparative, product_market, terminology, uncertainty, counterclaim, not_applicable; stance in supportive, skeptical, neutral, mixed, warning, competitive, promotional, uncertain, not_applicable.",
            "Use only these compact event enum values: cert in low, medium, high, hedged; h in past, present, near_future, long_future, timeless, unspecified. Do not use short_term, future, current, ongoing, or historical.",
            "For event t=term_usage, product_signal, or capability_claim, terms must contain at least one exact surface term from segment_text[s:e]. If no exact term exists, use a different event type or omit the event.",
            "For source context: if the current segment is page chrome, show setup, episode metadata, navigation, footer text, or show-note preview rather than substantive transcript dialogue, set st=excluded_source_context or st=no_signal, evs=[], and explain with ns/rc.",
            "Event keys:",
            ", ".join(f"{short}={long}" for short, long in EVENT_KEY_MAP.items() if short != "ev"),
            "Omit only documented defaults. Keep non-default empty-looking values when they are the actual label value. Every s/e span must be exact contiguous current-segment evidence.",
        ]
    )


def _evidence_sparse_compact_output_contract() -> str:
    return "\n".join(
        [
            "Return one JSON object with this shape:",
            '{"episode_id":"...","schema_version":"' + EVIDENCE_SPARSE_COMPACT_SCHEMA_VERSION + '","segment_outputs":[{"segment_id":"...","label":<evidence_sparse_compact_label>}],"episode_level_notes":"short private note"}',
            "This is the sparse compact contract with evidence-text-only spans. Include exactly one segment_outputs item for every packet segment_id, in packet order, and no extra segment_ids.",
            "For every event and concept candidate, output ev as exact contiguous text copied from the current segment. Do not output s/e offsets; local code derives offsets from ev before validation.",
            "Do not run helper commands, shell commands, scripts, or tools to calculate offsets. No offsets are required.",
            "Sparse compact labels omit default keys. Local code expands missing defaults and validates the full ai_discourse_v3_1 label.",
            "Top-level label keys: id=segment_id, st=extraction_status default coded, sq=segment_quality, sc=segment_source_context, evs=events default [], cc=concept_candidates default [], rc=rejected_candidates default [], ns=no_signal_reason default null, cf=overall_confidence, nr=needs_review default false, rr=review_reason default null.",
            "Important enum constraints: sq.br must be low, medium, or high. Rejected candidate why must be keyword_only, show_setup, sponsor_or_ad, page_chrome, unsupported_actor, duplicate, insufficient_evidence, or out_of_scope.",
            "Use the same event, actor, speaker, source, claim type, stance, certainty, and horizon enums specified by the sparse compact contract.",
            "For event t=term_usage, product_signal, or capability_claim, terms must contain at least one exact surface term from ev. If no exact term exists, use a different event type or omit the event.",
            "For page chrome, show setup, episode metadata, navigation, footer text, or show-note preview, set st=excluded_source_context or st=no_signal, evs=[], and explain with ns/rc.",
            "Event keys:",
            ", ".join(f"{short}={long}" for short, long in EVENT_KEY_MAP.items() if short not in {"s", "e"}),
            "Omit only documented defaults. Every ev value must be exact contiguous current-segment text.",
        ]
    )


def _flat_ledger_output_contract() -> str:
    event_fields = [short for short in EVENT_KEY_MAP if short not in {"s", "e"}]
    return "\n".join(
        [
            "Return one compact JSON object: {\"e\":episode_id,\"v\":schema_version,\"l\":labels,\"n\":episode_note}.",
            f"v must be {FLAT_LEDGER_SCHEMA_VERSION}.",
            "Each label is one positional array with exactly these fields:",
            "[id,status,segment_quality,source_context,events,concept_candidates,rejected_candidates,no_signal_reason,confidence,needs_review,review_reason]",
            "segment_quality=[artifact_type,boilerplate_risk,substantive_word_count,transcript_preparation_id].",
            "source_context=[kind,confidence,rationale].",
            "Each event is one positional array with exactly these fields:",
            json.dumps(event_fields, ensure_ascii=True),
            "Nested event arrays: a=[name,actor_type,affiliation,role]; sp=[name,role,affiliation,confidence]; ra=[name,actor_type,affiliation,confidence]; src=[kind,confidence,rationale]; tar=[raw_target,candidate_concept,canonical_concept,concept_confidence]; m=[value,unit,comparator,direction,raw_text].",
            "concept candidate=[candidate,surface_terms,rationale,usefulness_score,evidence,confidence]. rejected candidate=[text,reason,source_context_kind].",
            "Do not output evidence offsets. Event position ev is a verbatim quote, never a summary or paraphrase: copy and paste one exact contiguous span from the current segment, preserving every word, spelling, punctuation mark, and capitalization. Before returning, verify every ev string can be found byte-for-byte in that segment text; local code calculates offsets.",
            "Include exactly one label for every packet segment, in packet order. Read every segment and perform all semantic extraction yourself. Do not use tools, shell commands, scripts, regex rules, or keyword filters.",
            "Use JSON null for absent values and [] for empty lists. Every positional array must retain its full documented length.",
            "Use the ai_discourse_v3_1 enums and extraction semantics from the label-pack prompt. Keep reasoning minimal and return JSON only.",
        ]
    )


def _candidate_instruction_text(pack, representation: str) -> tuple[str, str]:
    if representation == "evidence_sparse_compact":
        return (
            " ".join(
                [
                    "You are an exhaustive private podcast discourse extractor. Read every word of every supplied current segment and use the model-generated episode context only for identity, attribution, and continuity.",
                    "Perform semantic extraction yourself; never use tools, regex, keyword gates, or topic-specific rules.",
                    "First inventory every exact evidence span that contains a research-useful proposition, relationship, named-entity signal, metric, forecast, mechanism, stance, uncertainty, terminology shift, capability, product fact, market fact, risk, adoption fact, or counterclaim.",
                    "Then expand each evidence span into every distinct applicable v3.1 event lens. One span may correctly produce multiple events; never collapse a capability plus mechanism, product plus market signal, forecast plus risk, stance plus counterclaim, or substantive identity relationship into one generic event.",
                    "Keep separate propositions separate even when they concern the same actor or product. Preserve concrete benchmark, pricing, access, deployment, adoption, valuation, timing, comparison, and causal details.",
                    "Resolve the current speaker separately from quoted or reported actors. Use episode context to repair speaker identity, but copy evidence only from the current segment.",
                    "Exclude ads, show setup, navigation, page residue, bare speaker labels, unsupported inference, and duplicate overlap. A named entity is an event only when it adds graph, attribution, product, model, market, stance, or influence value.",
                    "Before returning, scan the complete segment a second time by signal class and recover omissions. High-signal dialogue commonly has many events; optimize for complete supported coverage, not brevity, while keeping each event independently query-worthy.",
                    "Return only schema-valid JSON. Every ev is one exact contiguous quote copied byte-for-byte from the current segment. Do not calculate offsets; local code hydrates them.",
                ]
            ),
            "The response schema supplies all allowed enums and compact-key structure. Event families are terminology, framing, stance, forecast, mechanism, capability, product, market, risk, counterclaim, uncertainty, adoption, actor mention, and entity reference.",
        )
    if representation != "flat_ledger":
        return (
            pack.prompt.strip(),
            pack.codebook.strip() or "No separate codebook has been defined for this label pack.",
        )
    return (
        " ".join(
            [
                "Read every supplied transcript segment and extract every substantive proposition as ai_discourse_v3_1 events; never use keyword matching or topic-specific rules.",
                "Create separate events when one evidence span makes distinct claims. Use only exact contiguous current-segment evidence and exclude ads, show setup, page chrome, metadata, navigation, and unsupported inference.",
                "Event meanings: term_usage introduces or defines terminology; frame_usage applies an interpretive frame; stance_position expresses support, skepticism, warning, competition, promotion, neutrality, or uncertainty; forecast predicts a future outcome; causal_mechanism explains why or how one factor produces another; capability_claim states what a model, product, person, or organization can do; product_signal describes product behavior, launch, integration, or roadmap; market_signal describes demand, pricing, competition, funding, or market structure; risk_signal identifies a downside or hazard; counterclaim disputes another proposition; uncertainty marks explicit uncertainty or insufficient knowledge; adoption_signal describes usage, deployment, uptake, or rejection; actor_mention captures a substantively relevant person or organization; entity_reference captures another substantively relevant named entity.",
                "Resolve the actual speaker separately from any reported actor. Preserve the raw target and a concise candidate concept. Claims must be concise faithful propositions, not copied transcript blocks. Return coded only when substantive events exist; otherwise use no_signal, excluded_source_context, low_signal, or insufficient_evidence with an explanation.",
                "Use low reasoning but high recall: scan the entire supplied text once, emit all supported events, and do not summarize away repeated but distinct propositions.",
            ]
        ),
        "The strict response schema supplies allowed enums and positional structure. Follow it exactly; semantic extraction remains entirely model-generated.",
    )


def _full_candidate_output_contract(schema: dict[str, Any]) -> str:
    return "\n".join([
        "Return one JSON object with episode_id, schema_version, segment_outputs, and episode_level_notes.",
        f"schema_version must be {FULL_SCHEMA_BATCH_VERSION}. Include exactly one segment output per packet segment.",
        "Each label must use the familiar full ai_discourse_v3_1 schema. Preserve exhaustive event coverage and exact current-segment evidence.",
        json.dumps(schema, ensure_ascii=True, sort_keys=True),
    ])


def _full_output_contract(schema: dict[str, Any]) -> str:
    return "\n".join(
        [
            "Return one JSON object with this shape:",
            '{"episode_id":"...","segment_outputs":[{"segment_id":"...","label":<full ai_discourse_v3_1 label>}],"episode_level_notes":"short private note"}',
            "Include exactly one output for every segment_id in the packet, no extra segment_ids.",
            "Full label schema:",
            json.dumps(schema, ensure_ascii=True, sort_keys=True),
        ]
    )


def _episode_bundle_json(*, episode_id: str, rows: list[dict[str, Any]], labels: list[dict[str, Any]]) -> str:
    if len(rows) != len(labels):
        raise ValueError("episode bundle rows/labels length mismatch")
    payload = {
        "episode_id": episode_id,
        "segment_outputs": [
            {"segment_id": row["segment_id"], "label": label}
            for row, label in zip(rows, labels)
        ],
        "episode_level_notes": "",
    }
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_candidate_outputs(candidate_output_dir: Path) -> dict[str, dict[str, Any]]:
    candidate_by_segment: dict[str, dict[str, Any]] = {}
    for path in sorted(candidate_output_dir.glob("*.json*")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict) and payload.get("v") == FLAT_LEDGER_SCHEMA_VERSION:
            episode_id = str(payload.get("e") or "")
            for item in payload.get("l") or []:
                if isinstance(item, list):
                    expanded = expand_flat_ledger_label(item, episode_id=episode_id)
                    expanded["_flat_ledger_needs_hydration"] = True
                    candidate_by_segment[str(expanded["segment_id"])] = expanded
            continue
        if not isinstance(payload, dict):
            continue
        if payload.get("schema_version") == FULL_SCHEMA_BATCH_VERSION:
            for item in payload.get("segment_outputs") or []:
                if isinstance(item, dict) and isinstance(item.get("label"), dict):
                    expanded = item["label"]
                    expanded["_evidence_sparse_needs_hydration"] = True
                    candidate_by_segment[str(item["segment_id"])] = expanded
            continue
        if payload.get("schema_version") == SPARSE_COMPACT_SCHEMA_VERSION:
            episode_id = str(payload.get("episode_id") or "")
            for item in payload.get("segment_outputs") or []:
                if isinstance(item, dict) and isinstance(item.get("label"), dict):
                    candidate_by_segment[str(item.get("segment_id"))] = expand_sparse_compact_label(item["label"], episode_id=episode_id)
            continue
        if payload.get("schema_version") == OFFSET_SPARSE_COMPACT_SCHEMA_VERSION:
            episode_id = str(payload.get("episode_id") or "")
            for item in payload.get("segment_outputs") or []:
                if isinstance(item, dict) and isinstance(item.get("label"), dict):
                    expanded = expand_sparse_compact_label(item["label"], episode_id=episode_id)
                    expanded["_offset_sparse_needs_hydration"] = True
                    candidate_by_segment[str(item.get("segment_id"))] = expanded
            continue
        if payload.get("schema_version") == EVIDENCE_SPARSE_COMPACT_SCHEMA_VERSION:
            episode_id = str(payload.get("episode_id") or "")
            for item in payload.get("segment_outputs") or []:
                if isinstance(item, dict) and isinstance(item.get("label"), dict):
                    expanded = expand_sparse_compact_label(item["label"], episode_id=episode_id)
                    expanded["_evidence_sparse_needs_hydration"] = True
                    candidate_by_segment[str(item.get("segment_id"))] = expanded
            continue
        if payload.get("schema_version") == COMPACT_SCHEMA_VERSION:
            episode_id = str(payload.get("episode_id") or "")
            for item in payload.get("segment_outputs") or []:
                if isinstance(item, dict) and isinstance(item.get("label"), dict):
                    candidate_by_segment[str(item.get("segment_id"))] = expand_compact_label(item["label"], episode_id=episode_id)
            continue
        if isinstance(payload.get("segment_outputs"), list):
            for item in payload["segment_outputs"]:
                if isinstance(item, dict) and isinstance(item.get("label"), dict):
                    candidate_by_segment[str(item.get("segment_id"))] = item["label"]
            continue
        if isinstance(payload.get("segment_id"), str):
            candidate_by_segment[payload["segment_id"]] = payload
    return candidate_by_segment


def _label_similarity(golden: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    golden_events = [event for event in golden.get("discourse_events") or [] if isinstance(event, dict)]
    candidate_events = [event for event in candidate.get("discourse_events") or [] if isinstance(event, dict)]
    matched = _match_events(golden_events, candidate_events)
    precision = _ratio(len(matched), len(candidate_events))
    recall = _ratio(len(matched), len(golden_events))
    return {
        "status_match": golden.get("extraction_status") == candidate.get("extraction_status"),
        "event_precision": precision,
        "event_recall": recall,
        "event_f1": _f1(precision, recall),
        "golden_events": len(golden_events),
        "candidate_events": len(candidate_events),
        "matched_events": len(matched),
    }


def _match_events(golden_events: list[dict[str, Any]], candidate_events: list[dict[str, Any]]) -> list[tuple[int, int, float]]:
    pairs = []
    used_candidates: set[int] = set()
    for golden_index, golden in enumerate(golden_events):
        best: tuple[int, float] | None = None
        for candidate_index, candidate in enumerate(candidate_events):
            if candidate_index in used_candidates:
                continue
            score = _event_similarity(golden, candidate)
            if score >= 0.48 and (best is None or score > best[1]):
                best = (candidate_index, score)
        if best is not None:
            used_candidates.add(best[0])
            pairs.append((golden_index, best[0], round(best[1], 4)))
    return pairs


def _event_similarity(left: dict[str, Any], right: dict[str, Any]) -> float:
    score = 0.0
    if left.get("event_type") == right.get("event_type"):
        score += 0.15
    if left.get("claim_type") == right.get("claim_type"):
        score += 0.05
    score += 0.28 * _token_jaccard(str(left.get("evidence") or ""), str(right.get("evidence") or ""))
    score += 0.25 * _token_jaccard(str(left.get("claim_text") or ""), str(right.get("claim_text") or ""))
    score += 0.12 * _token_jaccard(str(_nested(left, "actor", "name") or ""), str(_nested(right, "actor", "name") or ""))
    score += 0.08 * _token_jaccard(str(_nested(left, "target", "candidate_concept") or ""), str(_nested(right, "target", "candidate_concept") or ""))
    left_entities = " ".join(_string_list(left.get(key)) for key in ["model_names", "product_names", "organizations", "people"])
    right_entities = " ".join(_string_list(right.get(key)) for key in ["model_names", "product_names", "organizations", "people"])
    score += 0.07 * _token_jaccard(left_entities, right_entities)
    return score


def _aggregate_candidate_comparison(items: list[dict[str, Any]]) -> dict[str, Any]:
    present = [item for item in items if not item.get("missing")]
    valid = [item for item in present if item.get("validation_ok")]
    precision = _weighted_average(valid, "event_precision", "candidate_events")
    recall = _weighted_average(valid, "event_recall", "golden_events")
    valid_golden_events = sum(int(item.get("golden_events") or 0) for item in valid)
    valid_candidate_events = sum(int(item.get("candidate_events") or 0) for item in valid)
    valid_matched_events = sum(int(item.get("matched_events") or 0) for item in valid)
    return {
        "segments_expected": len(items),
        "segments_present": len(present),
        "segments_missing": len(items) - len(present),
        "validation_ok": len(valid),
        "validation_failed": len(present) - len(valid),
        "valid_golden_events": valid_golden_events,
        "valid_candidate_events": valid_candidate_events,
        "valid_matched_events": valid_matched_events,
        "valid_event_count_ratio": _ratio(valid_candidate_events, valid_golden_events),
        "segment_completion_ratio": _ratio(len(present), len(items)),
        "valid_segment_ratio": _ratio(len(valid), len(items)),
        "status_accuracy": _ratio(sum(1 for item in valid if item.get("status_match")), len(valid)),
        "event_precision": precision,
        "event_recall": recall,
        "event_f1": _f1(precision, recall),
        "by_source": _candidate_breakdown(items, key="source_name"),
        "by_golden_status": _candidate_breakdown(items, key="golden_status"),
        "sample_failures": [
            {"segment_id": item.get("segment_id"), "validation_error": item.get("validation_error")}
            for item in present
            if item.get("validation_error")
        ][:10],
    }


def _candidate_breakdown(items: list[dict[str, Any]], *, key: str) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[str(item.get(key) or "unknown")].append(item)
    breakdown = {}
    for name, group in sorted(grouped.items()):
        present = [item for item in group if not item.get("missing")]
        valid = [item for item in present if item.get("validation_ok")]
        precision = _weighted_average(valid, "event_precision", "candidate_events")
        recall = _weighted_average(valid, "event_recall", "golden_events")
        breakdown[name] = {
            "segments_expected": len(group),
            "segments_present": len(present),
            "segments_missing": len(group) - len(present),
            "validation_ok": len(valid),
            "validation_failed": len(present) - len(valid),
            "golden_events": sum(int(item.get("golden_events") or 0) for item in group),
            "candidate_events": sum(int(item.get("candidate_events") or 0) for item in valid),
            "matched_events": sum(int(item.get("matched_events") or 0) for item in valid),
            "status_accuracy": _ratio(sum(1 for item in valid if item.get("status_match")), len(valid)),
            "event_precision": precision,
            "event_recall": recall,
            "event_f1": _f1(precision, recall),
        }
    return breakdown


def _cost_payload(*, name: str, request_count: int, input_chars: int, output_chars: int, extra: dict[str, Any]) -> dict[str, Any]:
    input_tokens = _estimate_tokens(input_chars)
    output_tokens = _estimate_tokens(output_chars)
    return {
        "name": name,
        "request_count": request_count,
        "input_chars": input_chars,
        "output_chars": output_chars,
        "estimated_input_tokens": input_tokens,
        "estimated_output_tokens": output_tokens,
        "estimated_total_tokens": input_tokens + output_tokens,
        **extra,
    }


def _interpretation(*, current_cost: dict[str, Any], compact_cost: dict[str, Any], compact_quality: dict[str, Any]) -> dict[str, Any]:
    total_ratio = _ratio(compact_cost["estimated_total_tokens"], current_cost["estimated_total_tokens"])
    input_ratio = _ratio(compact_cost["estimated_input_tokens"], current_cost["estimated_input_tokens"])
    request_ratio = _ratio(compact_cost["request_count"], current_cost["request_count"])
    runtime_ratio = _ratio(compact_cost.get("estimated_runtime_seconds"), current_cost.get("estimated_runtime_seconds"))
    return {
        "candidate": compact_cost["name"],
        "meets_quarter_total_token_target_by_estimate": total_ratio is not None and total_ratio <= 0.25,
        "meets_quarter_input_token_target_by_estimate": input_ratio is not None and input_ratio <= 0.25,
        "meets_quarter_request_count_target_by_estimate": request_ratio is not None and request_ratio <= 0.25,
        "meets_quarter_runtime_target_by_estimate": runtime_ratio is not None and runtime_ratio <= 0.25,
        "quality_loss_from_compact_representation": 1 - (compact_quality["exact_label_roundtrip_rate"] or 0),
        "remaining_proof_needed": "Run GPT-5.5 sparse compact batch extraction on a held-out episode set and compare candidate outputs with event precision/recall before making this the production default.",
    }


def _quality_gate(
    *,
    current_cost: dict[str, Any],
    candidate_cost: dict[str, Any],
    compact_quality: dict[str, Any],
    candidate_comparison: dict[str, Any] | None,
) -> dict[str, Any]:
    thresholds = dict(DEFAULT_QUALITY_GATE)
    token_ratio = _ratio(candidate_cost["estimated_total_tokens"], current_cost["estimated_total_tokens"])
    runtime_ratio = _ratio(candidate_cost.get("estimated_runtime_seconds"), current_cost.get("estimated_runtime_seconds"))
    checks = {
        "compact_representation_lossless": compact_quality.get("exact_label_roundtrip_rate") == 1.0,
        "total_token_ratio_under_threshold": token_ratio is not None and token_ratio <= thresholds["max_total_token_ratio"],
        "runtime_ratio_under_threshold": runtime_ratio is not None and runtime_ratio <= thresholds["max_runtime_ratio"],
    }
    metrics = {
        "estimated_total_token_ratio": token_ratio,
        "estimated_runtime_ratio": runtime_ratio,
        "candidate_outputs_present": candidate_comparison is not None,
    }
    if candidate_comparison is None:
        return {
            "passed": False,
            "status": "not_ready",
            "thresholds": thresholds,
            "checks": checks,
            "metrics": metrics,
            "remaining_proof_needed": "Generate held-out GPT-5.5 candidate outputs, then rerun with --candidate-output-dir to measure status accuracy and event precision/recall.",
        }

    status_accuracy = candidate_comparison.get("status_accuracy")
    event_precision = candidate_comparison.get("event_precision")
    event_recall = candidate_comparison.get("event_recall")
    checks.update(
        {
            "no_missing_candidate_segments": int(candidate_comparison.get("segments_missing") or 0) == 0,
            "no_candidate_validation_failures": int(candidate_comparison.get("validation_failed") or 0) == 0,
            "status_accuracy_above_threshold": status_accuracy is not None and status_accuracy >= thresholds["min_status_accuracy"],
            "event_precision_above_threshold": event_precision is not None and event_precision >= thresholds["min_event_precision"],
            "event_recall_above_threshold": event_recall is not None and event_recall >= thresholds["min_event_recall"],
        }
    )
    metrics.update(
        {
            "segments_expected": candidate_comparison.get("segments_expected"),
            "segments_present": candidate_comparison.get("segments_present"),
            "segments_missing": candidate_comparison.get("segments_missing"),
            "validation_failed": candidate_comparison.get("validation_failed"),
            "valid_golden_events": candidate_comparison.get("valid_golden_events"),
            "valid_candidate_events": candidate_comparison.get("valid_candidate_events"),
            "valid_matched_events": candidate_comparison.get("valid_matched_events"),
            "valid_event_count_ratio": candidate_comparison.get("valid_event_count_ratio"),
            "segment_completion_ratio": candidate_comparison.get("segment_completion_ratio"),
            "valid_segment_ratio": candidate_comparison.get("valid_segment_ratio"),
            "status_accuracy": status_accuracy,
            "event_precision": event_precision,
            "event_recall": event_recall,
            "event_f1": candidate_comparison.get("event_f1"),
        }
    )
    failed_checks = [name for name, passed in checks.items() if not passed]
    return {
        "passed": not failed_checks,
        "status": "passed" if not failed_checks else "failed",
        "thresholds": thresholds,
        "checks": checks,
        "metrics": metrics,
        "failed_checks": failed_checks,
    }


def _sum_file_chars(paths: list[str]) -> tuple[int, int]:
    total = 0
    missing = 0
    for path in paths:
        size = _file_chars(path)
        if size is None:
            missing += 1
        else:
            total += size
    return total, missing


def _safe_segment_text(conn, segment_id: str) -> tuple[str, str | None]:
    row = conn.execute("SELECT text_path FROM segments WHERE id = ?", (segment_id,)).fetchone()
    if not row:
        return "", "segment_not_found"
    raw_path = Path(row["text_path"])
    path = raw_path if raw_path.is_absolute() else corpus_dir().parent / raw_path
    try:
        size = path.stat().st_size
    except OSError as exc:
        return "", f"segment_text_stat_failed:{exc.__class__.__name__}:{getattr(exc, 'errno', None)}"
    if size > MAX_VALIDATION_SEGMENT_TEXT_BYTES:
        return "", f"segment_text_too_large_for_fast_validation:{size}"
    try:
        return path.read_text(encoding="utf-8"), None
    except Exception as exc:
        return "", f"segment_text_unreadable:{_sanitize_error(str(exc))}"


def _segment_text_for_prompt(conn, segment_id: str) -> tuple[str, str | None]:
    row = conn.execute("SELECT text_path FROM segments WHERE id = ?", (segment_id,)).fetchone()
    if not row:
        return "", "segment_not_found"
    raw_path = Path(row["text_path"])
    path = raw_path if raw_path.is_absolute() else corpus_dir().parent / raw_path
    try:
        return path.read_text(encoding="utf-8"), None
    except Exception as exc:
        return "", f"segment_text_unreadable:{_sanitize_error(str(exc))}"


def _estimated_segment_text(word_count: int, *, read_error: str) -> str:
    approx_chars = max(1, word_count) * 6
    return "[segment text unavailable for cost estimate: " + read_error + "]\n" + ("x" * approx_chars)


def _file_chars(path: str | Path | None) -> int | None:
    if not path:
        return None
    try:
        return len(Path(path).expanduser().read_text(encoding="utf-8"))
    except OSError:
        return None


def _estimate_tokens(chars: int) -> int:
    return int(math.ceil(chars / 4))


def _ratio(numerator: float | int | None, denominator: float | int | None) -> float | None:
    if denominator in (None, 0) or numerator is None:
        return None
    return round(float(numerator) / float(denominator), 6)


def _f1(precision: float | None, recall: float | None) -> float | None:
    if precision is None or recall is None or precision + recall == 0:
        return None
    return round(2 * precision * recall / (precision + recall), 6)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    middle = len(values) // 2
    if len(values) % 2:
        return float(values[middle])
    return (values[middle - 1] + values[middle]) / 2


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    index = min(len(values) - 1, max(0, int(round((len(values) - 1) * percentile))))
    return round(float(values[index]), 3)


def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _text_check_sample(rows: list[dict[str, Any]], *, limit: int) -> set[str]:
    if limit <= 0:
        return set()
    segment_ids = sorted({str(row["segment_id"]) for row in rows if row.get("segment_id")})
    if len(segment_ids) <= limit:
        return set(segment_ids)
    ranked = sorted(segment_ids, key=lambda segment_id: sha256_text(f"efficiency-text-check:{segment_id}"))
    return set(ranked[:limit])


def _source_counts(labels: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, set[str] | int]] = {}
    for row in labels:
        item = counts.setdefault(row["source_name"], {"episodes": set(), "labels": 0})
        item["episodes"].add(row["episode_id"])  # type: ignore[union-attr]
        item["labels"] = int(item["labels"]) + 1
    return {
        source: {"episodes": len(item["episodes"]), "labels": int(item["labels"])}
        for source, item in sorted(counts.items())
    }


def _safe_filename(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-")
    if not slug:
        slug = "episode"
    return f"{slug[:80]}-{sha256_text(value)[:10]}"


def _token_jaccard(left: str, right: str) -> float:
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    if not left_tokens and not right_tokens:
        return 1.0
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _tokens(value: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9][a-z0-9_.-]{1,}", value.lower()) if token not in {"the", "and", "that", "with", "from", "this"}}


def _nested(value: dict[str, Any], *path: str) -> Any:
    current: Any = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _string_list(value: Any) -> str:
    if not isinstance(value, list):
        return ""
    return " ".join(str(item) for item in value if isinstance(item, str))


def _name_context_dependent(name: str, lowered_segment_text: str) -> bool:
    tokens = [token for token in re.findall(r"[a-z0-9]+", name.lower()) if len(token) > 2]
    if not tokens:
        return False
    return not any(token in lowered_segment_text for token in tokens)


def _weighted_average(items: list[dict[str, Any]], value_key: str, weight_key: str) -> float | None:
    numerator = 0.0
    denominator = 0.0
    for item in items:
        value = item.get(value_key)
        weight = item.get(weight_key) or 1
        if value is None:
            continue
        numerator += float(value) * float(weight)
        denominator += float(weight)
    if not denominator:
        return None
    return round(numerator / denominator, 6)


def _sanitize_error(text: str) -> str:
    text = re.sub(r"'[^']{80,}'", "'[redacted]'", text)
    text = re.sub(r'"[^"]{80,}"', '"[redacted]"', text)
    return text[:300]
