from __future__ import annotations

"""Run a compact global LLM delta editor over the strongest measured candidate."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5_selection_v249_explicit_applicability as v249
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .labels import ValidationError, _validate_schema
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v271_compact_delta_set_editor_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v271_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v271_terminal_v1"
PHASE_ID = "development_selection_v5_4_v271_compact_delta_set_editor"
MODEL = "gpt-5.6-sol"
EFFORT = "low"
ADOPTED_V249_TOKENS = 44_474
MAX_TOTAL_TOKENS = 28_000
MAX_COMBINED_TOKENS = ADOPTED_V249_TOKENS + MAX_TOTAL_TOKENS
MAX_REPLACEMENT_EVENTS = 8
TIMEOUT_SECONDS = 1200.0
MAX_PROMPT_BYTES = 100_000
MAX_BASE_BYTES = 20_000
MAX_SCHEMA_BYTES = 90_000
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
MIN_DENSE_EVENTS = 27
MAX_RESIDUAL_EVENTS = 1
USAGE_FIELDS = v249.USAGE_FIELDS
PROJECT_ROOT = v249.PROJECT_ROOT
PIPELINE_ROOT = v249.PIPELINE_ROOT
V249_ROOT = v249.DEFAULT_OUTPUT_ROOT
V254_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v254-local-owner-adjudication-v249"
).resolve()
V270_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v270-graph-canonicalizer-recovery"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v271-compact-delta-set-editor"
).resolve()
PINNED_CODEX_0_144_1 = v249.PINNED_CODEX_0_144_1


class V271DeltaSetEditorError(RuntimeError):
    """The v271 architecture cannot proceed or be adopted safely."""


class V271OutputContractError(V271DeltaSetEditorError):
    """A completed canonicalizer output violated the frozen projection contract."""


class V271ArchitectureStop(V271DeltaSetEditorError):
    """The measured architecture failed a predeclared gate."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V271DeltaSetEditorError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V271DeltaSetEditorError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V271DeltaSetEditorError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    data = resolved.read_bytes()
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": len(data),
    }


def _verify_record(record: Mapping[str, Any]) -> bool:
    try:
        return _record(Path(str(record["path"]))) == dict(record)
    except (KeyError, OSError, TypeError, ValueError):
        return False


def _turn_root(root: Path, turn_name: str) -> Path:
    return root / "turns" / turn_name.replace("_", "-")


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = _turn_root(root, turn_name)
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "base": turn_root / "base-instructions.private.md",
        "schema": turn_root / "schema.json",
        "projection_schema": turn_root / "projection-schema.json",
        "direct_schema": turn_root / "direct-schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
    }


def _source_turn_root(root: Path, prefix: str) -> Path:
    return next(root.glob(f"turns/{prefix}*"))


def _lineage_paths() -> dict[str, Path]:
    v249_turn = _source_turn_root(V249_ROOT, "v249-explicit-applicability-")
    v270_turn = _source_turn_root(V270_ROOT, "v270-graph-canonicalizer-")
    return {
        "v249_terminal": V249_ROOT / "terminal.json",
        "v249_runtime_lock": V249_ROOT / "runtime-lock.json",
        "v249_sidecar": v249_turn / "sidecar.json",
        "v249_source_input": v249_turn / "input.private.json",
        "v249_source_prompt": v249_turn / "prompt.private.md",
        "v249_direct_schema": v249_turn / "projection-schema.json",
        "v249_raw_output": v249_turn / "output.private.json",
        "v249_normalized_output": V249_ROOT / "normalized-output.private.json",
        "v249_provenance": V249_ROOT / "evidence-provenance.private.json",
        "v254_terminal": V254_ROOT / "terminal.json",
        "v254_score": V254_ROOT / "reconciled-alignment-score.json",
        "v270_terminal": V270_ROOT / "terminal.json",
        "v270_runtime_lock": V270_ROOT / "runtime-lock.json",
        "v270_capacity": v270_turn / "capacity.json",
        "v270_sidecar": v270_turn / "sidecar.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    v249.verify_runtime_lock(paths["v249_runtime_lock"])
    t249 = _load_json(paths["v249_terminal"], "v249 terminal")
    s249 = _load_json(paths["v249_sidecar"], "v249 sidecar")
    t254 = _load_json(paths["v254_terminal"], "v254 terminal")
    q254 = _load_json(paths["v254_score"], "v254 score")
    t270 = _load_json(paths["v270_terminal"], "v270 terminal")
    s270 = _load_json(paths["v270_sidecar"], "v270 sidecar")
    if (
        t249.get("terminal_reason")
        != "v249_explicit_applicability_structural_cost_gate_passed"
        or (s249.get("usage") or {}).get("total_tokens") != ADOPTED_V249_TOKENS
        or s249.get("usage_status") != "measured"
        or s249.get("usage_complete") is not True
        or s249.get("auth_type") != "chatgpt"
        or t254.get("terminal_reason")
        != "v254_local_owner_adjudication_quality_gate_not_passed"
        or (q254.get("metrics") or {}).get("development_strict_full_field_macro_f1")
        != 0.96
        or (q254.get("metrics") or {}).get(
            "strictly_equivalent_reference_semantic_unit_count"
        )
        != 23
        or q254.get("failed_checks")
        != ["strict_full_field_macro_noninferior_margin_0_03", "no_material_source_macro_regression"]
        or t270.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or t270.get("error_message_sha256")
        != "a34941799b9e25bd8b4d4935d5774f08db2b114ecb2d589da9a665e3ee018d4a"
        or t270.get("usage_status") != "unknown"
        or s270.get("state") != "failed"
        or s270.get("usage_status") != "unknown"
        or s270.get("error_class") != "turn_failed"
        or t270.get("production_mutated") is not False
    ):
        raise V271DeltaSetEditorError("v271 architecture lineage drifted")
    source = v249._load_frozen(V249_ROOT)["turn"]
    return {
        "paths": paths,
        "records": records,
        "source_turn": source,
        "base_raw": _load_json(paths["v249_raw_output"], "v249 raw output"),
        "base_normalized": _load_json(
            paths["v249_normalized_output"], "v249 normalized output"
        ),
        "base_provenance": _load_json(paths["v249_provenance"], "v249 provenance"),
        "v254_terminal": t254,
        "v254_score": q254,
        "v270_terminal": t270,
        "v270_sidecar": s270,
    }


DELTA_INSTRUCTIONS = """You are the second and final semantic stage of a compact global delta-set architecture for a topic-general podcast research pipeline. The supplied base events came from a blind extractor, are not a reference answer, and may contain unsupported events, wrong boundaries, duplicates, or material field errors. Re-read every source unit and independently review every opaque base event. You cannot see a target count, reference, density label, expected topic, model identity, or quality hint. Never use keywords, regex, phrase lists, or fixed topics to decide meaning.

Return exactly one receipt for every base event in input order. keep means the event and every material field already match the source. drop means it is unsupported or not independently event-worthy. replace means one event remains but needs one materially corrected complete event. merge means two or more base events have the same truth conditions or are fragments of one atomic event and map to one replacement event. The LLM alone decides all keep, drop, replace, merge, boundaries, evidence, actors, attribution, stance, certainty, temporal horizon, metrics, and replacement fields.

Merge only when actor, reported actor, speaker, target, mechanism, stance, certainty, temporal scope, attribution, event type, metric, and truth conditions do not conflict. Preserve independently truth-valued premises, mechanisms, outcomes, comparisons, constraints, stances, and legitimate multi-lens events. Drop greetings, logistics, acknowledgements, unasserted questions, bare mentions, and unsupported inference. Review every source unit and base event with unresolved_count=0.

Emit complete replacement_events only for replace or merge actions; kept events are retained byte-for-byte and must not be rewritten. Each replacement uses explicit applicability objects for every optional field and selects the smallest contiguous evidence-unit range supporting every material field. Every present metric string must be an exact literal from the selected evidence. Return at most eight replacement events, keep final events in source order, and return schema-valid JSON only."""


def _compact_event(event: Mapping[str, Any], provenance: Mapping[str, Any], proposal_id: str) -> dict[str, Any]:
    compact: dict[str, Any] = {
        "proposal_id": proposal_id,
        "evidence_start_unit_id": provenance["evidence_start_unit_id"],
        "evidence_end_unit_id": provenance["evidence_end_unit_id"],
    }
    for field, value in event.items():
        if field in {"evidence", "window_id"} or value in ("", None, []):
            continue
        compact[field] = copy.deepcopy(value)
    return compact


def _base_packet(lineage: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, list[str]]]:
    source = lineage["source_turn"]
    raw_segments = list(lineage["base_raw"]["segments"])
    normalized_segments = list(lineage["base_normalized"]["segments"])
    provenance_by_key = {
        (str(row["segment_id"]), int(row["event_index"])): row
        for row in lineage["base_provenance"]["events"]
    }
    source_by_id = {
        str(row["segment_id"]): row for row in source["private_input"]["segments"]
    }
    packet_segments = []
    proposal_ids: dict[str, list[str]] = {}
    if [row["segment_id"] for row in raw_segments] != [
        row["segment_id"] for row in normalized_segments
    ]:
        raise V271DeltaSetEditorError("v249 base segment order drifted")
    for position, (raw, normalized) in enumerate(zip(raw_segments, normalized_segments)):
        segment_id = str(raw["segment_id"])
        if len(raw["events"]) != len(normalized["events"]):
            raise V271DeltaSetEditorError("v249 raw and normalized event counts drifted")
        ids = [f"S{position}P{index:03d}" for index in range(len(raw["events"]))]
        proposal_ids[segment_id] = ids
        packet_segments.append(
            {
                "segment_id": segment_id,
                "source_units": copy.deepcopy(source_by_id[segment_id]["units"]),
                "base_events": [
                    _compact_event(
                        event,
                        provenance_by_key[(segment_id, index)],
                        ids[index],
                    )
                    for index, event in enumerate(normalized["events"])
                ],
            }
        )
    return {"episode_id": source["episode_id"], "segments": packet_segments}, proposal_ids


def _delta_schema(source: Mapping[str, Any], proposal_ids: Mapping[str, Sequence[str]]) -> dict[str, Any]:
    direct_event = source["direct_schema"]["properties"]["segments"]["items"][
        "properties"
    ]["events"]["items"]
    replacement = v249._explicit_event_schema(direct_event)
    all_proposals = [item for values in proposal_ids.values() for item in values]
    replacement_ids = [
        f"S{position}R{index:03d}"
        for position in range(len(proposal_ids))
        for index in range(MAX_REPLACEMENT_EVENTS)
    ]
    replacement["properties"]["event_id"] = {
        "type": "string",
        "enum": replacement_ids,
    }
    replacement["properties"]["source_proposal_ids"] = {
        "type": "array",
        "minItems": 1,
        "maxItems": len(all_proposals),
        "uniqueItems": True,
        "items": {"type": "string", "enum": all_proposals},
    }
    replacement["required"].extend(["event_id", "source_proposal_ids"])
    source_segment = source["schema"]["properties"]["segments"]["items"]
    receipt = {
        "type": "object",
        "additionalProperties": False,
        "required": ["proposal_id", "action", "final_event_id"],
        "properties": {
            "proposal_id": {"type": "string", "enum": all_proposals},
            "action": {"type": "string", "enum": ["keep", "drop", "replace", "merge"]},
            "final_event_id": {"type": "string", "enum": ["", *all_proposals, *replacement_ids]},
        },
    }
    segment = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "segment_id",
            "final_status",
            "segment_source_context",
            "no_signal_reason",
            "coverage_audit",
            "proposal_receipts",
            "replacement_events",
        ],
        "properties": {
            "segment_id": copy.deepcopy(source_segment["properties"]["segment_id"]),
            "final_status": copy.deepcopy(source_segment["properties"]["status"]),
            "segment_source_context": copy.deepcopy(
                source_segment["properties"]["segment_source_context"]
            ),
            "no_signal_reason": copy.deepcopy(source_segment["properties"]["no_signal_reason"]),
            "coverage_audit": {
                "type": "object",
                "additionalProperties": False,
                "required": ["all_source_units_reviewed", "all_base_events_reviewed", "unresolved_count"],
                "properties": {
                    "all_source_units_reviewed": {"type": "boolean"},
                    "all_base_events_reviewed": {"type": "boolean"},
                    "unresolved_count": {"type": "integer", "minimum": 0},
                },
            },
            "proposal_receipts": {
                "type": "array",
                "minItems": 0,
                "maxItems": len(all_proposals),
                "items": receipt,
            },
            "replacement_events": {
                "type": "array",
                "minItems": 0,
                "maxItems": MAX_REPLACEMENT_EVENTS,
                "items": replacement,
            },
        },
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["episode_id", "segments"],
        "properties": {
            "episode_id": {"type": "string", "enum": [source["episode_id"]]},
            "segments": {
                "type": "array",
                "minItems": len(proposal_ids),
                "maxItems": len(proposal_ids),
                "items": segment,
            },
        },
    }


def _episode_context(base: str) -> Any:
    marker = "# Immutable episode context\n"
    if marker not in base:
        raise V271DeltaSetEditorError("immutable episode context marker missing")
    payload = base.split(marker, 1)[1].split("\n\n# Explicit applicability contract", 1)[0]
    try:
        return json.loads(payload.strip())
    except json.JSONDecodeError as exc:
        raise V271DeltaSetEditorError("immutable episode context is malformed") from exc


def prepare_turn(lineage: Mapping[str, Any]) -> dict[str, Any]:
    source = lineage["source_turn"]
    packet, proposal_ids = _base_packet(lineage)
    schema = _delta_schema(source, proposal_ids)
    prompt = "# Compact global delta set-editor packet\n" + _canonical_json(packet) + "\n"
    base = (
        DELTA_INSTRUCTIONS
        + "\n\n# Immutable episode context\n"
        + _canonical_json(_episode_context(source["base"]))
        + "\n"
    )
    sizes = {
        "prompt_bytes": len(prompt.encode("utf-8")),
        "base_bytes": len(base.encode("utf-8")),
        "schema_bytes": len(_canonical_json(schema).encode("utf-8")),
    }
    if (
        [len(proposal_ids[item]) for item in source["segment_ids"]] != [32, 1]
        or sizes["prompt_bytes"] > MAX_PROMPT_BYTES
        or sizes["base_bytes"] > MAX_BASE_BYTES
        or sizes["schema_bytes"] > MAX_SCHEMA_BYTES
    ):
        raise V271DeltaSetEditorError("v271 request size or base coverage drifted")
    return {
        "turn_name": "v271_compact_delta_set_editor_" + sha256_text(source["episode_id"])[:20],
        "episode_id": source["episode_id"],
        "segment_ids": list(source["segment_ids"]),
        "private_input": packet,
        "prompt": prompt,
        "base": base,
        "schema": schema,
        "projection_schema": copy.deepcopy(source["schema"]),
        "direct_schema": copy.deepcopy(source["direct_schema"]),
        "source_turn": source,
        "base_raw": copy.deepcopy(lineage["base_raw"]),
        "proposal_ids": proposal_ids,
        **sizes,
    }


def project_output(
    output: Mapping[str, Any], turn: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    try:
        _validate_schema(turn["schema"], output, path="$")
    except (ValidationError, ValueError, TypeError) as exc:
        raise V271OutputContractError("delta set-editor output schema failed") from exc
    if output.get("episode_id") != turn["episode_id"]:
        raise V271OutputContractError("delta set-editor episode id drifted")
    rows = list(output.get("segments") or [])
    if [row.get("segment_id") for row in rows] != list(turn["segment_ids"]):
        raise V271OutputContractError("delta set-editor segment coverage or order drifted")
    base_by_id = {str(row["segment_id"]): row for row in turn["base_raw"]["segments"]}
    source_by_id = {
        str(row["segment_id"]): row
        for row in turn["source_turn"]["private_input"]["segments"]
    }
    projected_segments = []
    coverage_rows = []
    for position, row in enumerate(rows):
        segment_id = str(row["segment_id"])
        expected = list(turn["proposal_ids"][segment_id])
        expected_set = set(expected)
        receipts = list(row["proposal_receipts"])
        audit = row["coverage_audit"]
        if (
            [receipt.get("proposal_id") for receipt in receipts] != expected
            or audit.get("all_source_units_reviewed") is not True
            or audit.get("all_base_events_reviewed") is not True
            or audit.get("unresolved_count") != 0
        ):
            raise V271OutputContractError("delta set-editor receipt coverage failed")
        base_raw_events = list(base_by_id[segment_id]["events"])
        base_event_by_id = {
            expected[index]: copy.deepcopy(event)
            for index, event in enumerate(base_raw_events)
        }
        replacement_by_id: dict[str, dict[str, Any]] = {}
        replacement_sources: dict[str, list[str]] = {}
        for index, raw_event in enumerate(row["replacement_events"]):
            event = copy.deepcopy(dict(raw_event))
            event_id = str(event.pop("event_id"))
            source_ids = [str(item) for item in event.pop("source_proposal_ids")]
            if (
                event_id != f"S{position}R{index:03d}"
                or event_id in replacement_by_id
                or not source_ids
                or any(item not in expected_set for item in source_ids)
            ):
                raise V271OutputContractError("delta replacement identity or ownership drifted")
            replacement_by_id[event_id] = event
            replacement_sources[event_id] = source_ids
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for receipt in receipts:
            proposal_id = str(receipt["proposal_id"])
            action = str(receipt["action"])
            final_event_id = str(receipt["final_event_id"])
            if action == "keep":
                if final_event_id != proposal_id:
                    raise V271OutputContractError("kept proposal final ID drifted")
            elif action == "drop":
                if final_event_id:
                    raise V271OutputContractError("dropped proposal names a final event")
            else:
                if final_event_id not in replacement_by_id:
                    raise V271OutputContractError("changed proposal lacks replacement event")
                grouped.setdefault(final_event_id, []).append(receipt)
        for event_id, source_ids in replacement_sources.items():
            group = grouped.get(event_id, [])
            if source_ids != [str(item["proposal_id"]) for item in group]:
                raise V271OutputContractError("replacement receipt mapping drifted")
            if len(group) == 1 and group[0]["action"] != "replace":
                raise V271OutputContractError("single changed proposal must be replace")
            if len(group) > 1 and any(item["action"] != "merge" for item in group):
                raise V271OutputContractError("multi-proposal replacement must be merge")
        if set(grouped) != set(replacement_by_id):
            raise V271OutputContractError("unmapped replacement event")
        final_events = []
        emitted = set()
        for receipt in receipts:
            proposal_id = str(receipt["proposal_id"])
            action = str(receipt["action"])
            final_event_id = str(receipt["final_event_id"])
            if action == "keep":
                final_events.append(copy.deepcopy(base_event_by_id[proposal_id]))
            elif action in {"replace", "merge"} and final_event_id not in emitted:
                final_events.append(copy.deepcopy(replacement_by_id[final_event_id]))
                emitted.add(final_event_id)
        units = list(source_by_id[segment_id]["units"])
        unit_index = {str(unit["unit_id"]): index for index, unit in enumerate(units)}
        try:
            final_events.sort(
                key=lambda event: unit_index[str(event["evidence_start_unit_id"])]
            )
        except KeyError as exc:
            raise V271OutputContractError("final event evidence belongs to another segment") from exc
        if len(final_events) > 32 or (row["final_status"] == "coded") != bool(final_events):
            raise V271OutputContractError("final status or event cap drifted")
        starts = Counter(str(event["evidence_start_unit_id"]) for event in final_events)
        projected_segment = copy.deepcopy(base_by_id[segment_id])
        projected_segment.update(
            {
                "status": row["final_status"],
                "segment_source_context": row["segment_source_context"],
                "no_signal_reason": row["no_signal_reason"],
                "coverage_audit": {
                    "all_source_units_reviewed": True,
                    "unresolved_count": 0,
                },
                "unit_receipts": [
                    {
                        "unit_id": unit["unit_id"],
                        "eligible_event_count": starts[str(unit["unit_id"])],
                        "unresolved_count": 0,
                    }
                    for unit in units
                ],
                "events": final_events,
            }
        )
        projected_segments.append(projected_segment)
        coverage_rows.append(
            {
                "segment_id": segment_id,
                "base_event_count": len(expected),
                "kept_count": sum(item["action"] == "keep" for item in receipts),
                "dropped_count": sum(item["action"] == "drop" for item in receipts),
                "replaced_count": sum(item["action"] == "replace" for item in receipts),
                "merged_receipt_count": sum(item["action"] == "merge" for item in receipts),
                "replacement_event_count": len(replacement_by_id),
                "final_event_count": len(final_events),
                "all_base_events_reviewed_exactly_once": True,
            }
        )
    projection_turn = copy.deepcopy(turn["source_turn"])
    try:
        normalized, provenance, diagnostics, applicability = v249.project_output(
            {"episode_id": turn["episode_id"], "segments": projected_segments},
            projection_turn,
        )
    except v249.V249OutputContractError as exc:
        raise V271OutputContractError(str(exc)) from exc
    coverage = {
        "schema_version": SCHEMA_VERSION,
        "architecture_id": "compact_global_delta_set_editor_over_v249",
        "segments": coverage_rows,
        "all_base_events_reviewed_exactly_once": True,
        "all_keep_drop_replace_merge_semantics_selected_by_llm": True,
        "empty_input_fields_omitted_only": True,
        "deterministic_projection_only": True,
    }
    return normalized, provenance, diagnostics, applicability, coverage


def _production_ratio(combined_tokens: int) -> tuple[int, float]:
    return v249._production_ratio(combined_tokens)


def _gate(
    *, usage: Mapping[str, int], diagnostics: Sequence[Mapping[str, Any]], coverage: Mapping[str, Any]
) -> dict[str, Any]:
    by_id = {str(row["segment_id"]): row for row in diagnostics}
    dense = int((by_id.get(v249.v239.DENSE_SEGMENT_ID) or {}).get("event_count", -1))
    residual = int((by_id.get(v249.v239.NO_SIGNAL_SEGMENT_ID) or {}).get("event_count", -1))
    combined = ADOPTED_V249_TOKENS + int(usage["total_tokens"])
    production_total, ratio = _production_ratio(combined)
    checks = {
        "both_segments_validated": set(by_id)
        == {v249.v239.DENSE_SEGMENT_ID, v249.v239.NO_SIGNAL_SEGMENT_ID},
        "dense_event_count_gte_27": dense >= MIN_DENSE_EVENTS,
        "nominal_no_signal_event_count_lte_1": 0 <= residual <= MAX_RESIDUAL_EVENTS,
        "all_source_units_reviewed": all(
            int(row["source_unit_count"]) == int(row["reviewed_source_unit_count"])
            for row in diagnostics
        ),
        "unresolved_count_0": all(int(row["unresolved_count"]) == 0 for row in diagnostics),
        "all_base_events_reviewed_exactly_once": coverage.get(
            "all_base_events_reviewed_exactly_once"
        ) is True,
        "replacement_event_count_lte_8": sum(
            int(row["replacement_event_count"])
            for row in coverage.get("segments") or []
        )
        <= MAX_REPLACEMENT_EVENTS,
        "explicit_applicability_projection_passed": True,
        "exact_evidence_rate_1": True,
        "metric_grounding_error_events_0": True,
        "event_cap_violations_0": True,
        "exact_identity_duplicates_0": True,
        "new_turn_tokens_lte_28000": int(usage["total_tokens"]) <= MAX_TOTAL_TOKENS,
        "combined_tokens_lte_72474": combined <= MAX_COMBINED_TOKENS,
        "production_amortized_total_token_ratio_lte_0_28": ratio <= 0.28,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase_id": PHASE_ID,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "new_turn_usage": dict(usage),
        "adopted_v249_tokens": ADOPTED_V249_TOKENS,
        "combined_tokens": combined,
        "production_amortized_total_tokens": production_total,
        "production_amortized_total_token_ratio": round(ratio, 6),
        "dense_event_count": dense,
        "candidate_only_nominal_no_signal_event_count": residual,
        "residual_support_audit_required": residual > 0,
        "diagnostics": list(diagnostics),
        "coverage": dict(coverage),
        "support_alignment_authorized": not failed,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
    }


def _capacity_policy(root: Path, turn_name: str) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": 1,
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
            "phase_total_token_bound": MAX_TOTAL_TOKENS,
            "projected_phase_quota_points": projected,
            "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        },
    }
    _write_stable_time(audit_path, audit, "created_at")
    policy = {
        "schema_version": "pif_app_server_capacity_policy_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "managed_chatgpt_auth_only": True,
        "official_persistent_codex_app_server_only": True,
        "retry_count_per_turn": 0,
        "production_mutation_allowed": False,
        "rate_limit_reached_type_must_be_null": True,
        "unknown_usage_hard_stop": True,
        "ordered_turn_names": [turn_name],
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS,
        "phase_total_token_bound": MAX_TOTAL_TOKENS,
        "projected_phase_quota_points": projected,
        "semantic_output_root": str(root),
        "audit": _record(audit_path),
    }
    _write_stable_time(policy_path, policy, "created_at")
    reserve.load_reserve_capacity_policy(policy_path)
    return {"audit": audit_path, "policy": policy_path}


def _runtime_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            {
                *v249._runtime_files(),
                Path(__file__).resolve(),
                Path(v249.__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
            },
            key=str,
        )
    )


def _request_records(paths: Mapping[str, Path]) -> list[dict[str, Any]]:
    return [
        _record(paths[name])
        for name in ("input", "prompt", "base", "schema", "projection_schema", "direct_schema")
    ]


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v271 runtime lock")
    root = path.parent.resolve()
    spec = _load_json(root / "attempt-spec.json", "v271 spec")
    paths = _turn_paths(root, str(spec["turn_name"]))
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("retry_count") != 0
        or lock.get("max_total_tokens") != MAX_TOTAL_TOKENS
        or lock.get("adopted_v249_tokens") != ADOPTED_V249_TOKENS
        or lock.get("max_combined_tokens") != MAX_COMBINED_TOKENS
        or lock.get("semantic_regex_or_keyword_filtering") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(item) for item in _runtime_files()}
        or {row["path"] for row in lock.get("request") or []}
        != {row["path"] for row in _request_records(paths)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V271DeltaSetEditorError("v271 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("v270_failure_audit"),
        lock.get("authorization"),
        lock.get("ranking"),
        lock.get("design"),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        *(lock.get("request") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V271DeltaSetEditorError("v271 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V271DeltaSetEditorError("v271 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    spec_path = root / "attempt-spec.json"
    spec = _load_json(spec_path, "v271 spec")
    paths = _turn_paths(root, str(spec["turn_name"]))
    lineage = _validate_lineage()
    turn = prepare_turn(lineage)
    return {
        "root": root,
        "spec_path": spec_path,
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "turn": {
            **turn,
            "private_input": _load_json(paths["input"], "v271 input"),
            "prompt": paths["prompt"].read_text(encoding="utf-8"),
            "base": paths["base"].read_text(encoding="utf-8"),
            "schema": _load_json(paths["schema"], "v271 schema"),
            "projection_schema": _load_json(paths["projection_schema"], "v271 projection schema"),
            "direct_schema": _load_json(paths["direct_schema"], "v271 direct schema"),
            "paths": paths,
        },
    }


def freeze_v271(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v271 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V271DeltaSetEditorError("unfinished v271 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    turn = prepare_turn(lineage)
    paths = _turn_paths(root, turn["turn_name"])
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], turn["private_input"])
    _write_private_text(paths["prompt"], turn["prompt"])
    _write_private_text(paths["base"], turn["base"])
    _write_immutable(paths["schema"], turn["schema"])
    _write_immutable(paths["projection_schema"], turn["projection_schema"])
    _write_immutable(paths["direct_schema"], turn["direct_schema"])
    capacity_paths = _capacity_policy(root, turn["turn_name"])
    v270_audit_path = root / "v270-infrastructure-failure-audit.json"
    _write_stable_time(
        v270_audit_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "predecessor_terminal": lineage["records"]["v270_terminal"],
            "predecessor_sidecar": lineage["records"]["v270_sidecar"],
            "predecessor_runtime_lock": lineage["records"]["v270_runtime_lock"],
            "terminal_reported_usage_status": "unknown",
            "sidecar_usage_status": "unknown",
            "accounting_complete": False,
            "failed_turn_error_class": "turn_failed",
            "predecessor_output_absent": True,
            "failure_class": "external_transport_turn_failed_unknown_usage",
            "predecessor_output_adopted": False,
            "predecessor_retried": False,
            "same_architecture_retry_authorized": False,
            "production_mutated": False,
        },
        "created_at",
    )
    authorization_path = root / "authorization.json"
    _write_stable_time(
        authorization_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "authority": "direct_operator_architecture_steering_2026_07_17",
            "scope": "one bounded compact global delta set-editor canary over frozen v249",
            "semantic_prompt_or_schema_delta": True,
            "isolated_field_patch": False,
            "semantic_attempt_count": 1,
            "retry_count": 0,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    ranking_path = root / "architecture-ranking.json"
    _write_stable_time(
        ranking_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "selected_architecture_id": "compact_global_delta_set_editor_over_v249",
            "architectures": [
                {"rank": 1, "id": "compact_global_delta_set_editor_over_v249"},
                {"rank": 2, "id": "episode_bootstrap_source_owner_specialists_global_join"},
                {"rank": 3, "id": "independent_segment_threads_episode_level_reducer"},
            ],
            "measured_basis": {
                "v249_adopted_tokens": ADOPTED_V249_TOKENS,
                "v249_dense_events": 32,
                "v254_reconciled_macro_f1": 0.96,
                "v254_strict_reference_units": 23,
                "v254_mismatch_fields": {"event_boundary": 2, "evidence": 2, "target": 1},
                "v270_semantic_output_observed": False,
                "v270_failure_class": "repeated_external_transport_turn_failed",
            },
            "on_failure": "freeze and reject compact delta editor; advance to episode bootstrap source-owner specialists",
        },
        "created_at",
    )
    projected_total, projected_ratio = _production_ratio(MAX_COMBINED_TOKENS)
    design_path = root / "architecture-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "architecture_id": "compact_global_delta_set_editor_over_v249",
            "hypothesis": (
                "a compact Sol-low global set editor can retain correct v249 events byte-for-byte while changing "
                "only source-supported duplicate, boundary, evidence, or material-field errors; omitting only "
                "empty input fields leaves enough token budget for a decision-changing second stage"
            ),
            "representative_canary": {
                "episode_count": 1,
                "segment_count": 2,
                "dense_segment_count": 1,
                "nominal_no_signal_segment_count": 1,
                "adopted_v249_semantic_turn_count": 1,
                "new_semantic_turn_count": 1,
                "base_event_counts": [32, 1],
                "maximum_replacement_event_count": MAX_REPLACEMENT_EVENTS,
                "reference_visible_to_model": False,
                "target_count_visible_to_model": False,
            },
            "production_cost_projection": {
                "adopted_v249_tokens": ADOPTED_V249_TOKENS,
                "new_turn_hard_max": MAX_TOTAL_TOKENS,
                "combined_hard_max": MAX_COMBINED_TOKENS,
                "production_amortized_total_tokens": projected_total,
                "projected_production_amortized_total_token_ratio": round(projected_ratio, 6),
                "required_ratio_max": 0.28,
            },
            "predeclared_stop_rules": {
                "retry_count": 0,
                "all_base_events_reviewed_exactly_once": True,
                "empty_input_fields_omitted_only": True,
                "replacement_event_count_max": MAX_REPLACEMENT_EVENTS,
                "dense_event_count_minimum": MIN_DENSE_EVENTS,
                "nominal_no_signal_event_count_maximum": MAX_RESIDUAL_EVENTS,
                "exact_evidence_rate": 1.0,
                "metric_grounding_error_events": 0,
                "event_cap_violations": 0,
                "exact_identity_duplicates": 0,
                "new_turn_tokens_max": MAX_TOTAL_TOKENS,
                "combined_tokens_max": MAX_COMBINED_TOKENS,
                "production_amortized_total_token_ratio_max": 0.28,
            },
            "semantic_regex_or_keyword_filtering": False,
            "deterministic_semantic_decisions": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    spec_path = root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "state": "frozen_before_one_turn_compact_delta_set_editor",
            "turn_name": turn["turn_name"],
            "model": MODEL,
            "effort": EFFORT,
            "declared_turn_count": 1,
            "retry_count": 0,
            "max_total_tokens": MAX_TOTAL_TOKENS,
            "adopted_v249_tokens": ADOPTED_V249_TOKENS,
            "max_combined_tokens": MAX_COMBINED_TOKENS,
            "prompt_bytes": turn["prompt_bytes"],
            "base_bytes": turn["base_bytes"],
            "schema_bytes": turn["schema_bytes"],
            "semantic_regex_or_keyword_filtering": False,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    lock_path = root / "runtime-lock.json"
    lock = {
        "schema_version": RUNTIME_LOCK_VERSION,
        "frozen_at": now_iso(),
        "phase_id": PHASE_ID,
        "model": MODEL,
        "effort": EFFORT,
        "declared_turn_count": 1,
        "retry_count": 0,
        "max_total_tokens": MAX_TOTAL_TOKENS,
        "adopted_v249_tokens": ADOPTED_V249_TOKENS,
        "max_combined_tokens": MAX_COMBINED_TOKENS,
        "semantic_regex_or_keyword_filtering": False,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "v270_failure_audit": _record(v270_audit_path),
        "authorization": _record(authorization_path),
        "ranking": _record(ranking_path),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "request": _request_records(paths),
        "holdout_authorized": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def _measured_usage(sidecar: Mapping[str, Any]) -> Optional[dict[str, int]]:
    values = sidecar.get("usage") or {}
    try:
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
    except (KeyError, TypeError, ValueError):
        return None
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
    ):
        return None
    return usage


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    paths = frozen["turn"]["paths"]
    attempted = int(paths["capacity"].exists())
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = attempted
    sidecar_record = None
    measured_over_cap = False
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        sidecar = _load_json(paths["sidecar"], "v271 sidecar")
        measured = _measured_usage(sidecar)
        if measured is not None:
            usage = measured
            unknown = 0
            measured_over_cap = usage["total_tokens"] > MAX_TOTAL_TOKENS
    semantic = isinstance(exc, (V271OutputContractError, V271ArchitectureStop)) or measured_over_cap
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v271_compact_delta_set_editor_structural_quality_or_cost_gate_not_passed"
            if semantic
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "semantic_attempt_count": attempted,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": usage,
        "unknown_usage_attempt_count": unknown,
        "measured_token_bound_exceeded": measured_over_cap,
        "sidecar": sidecar_record,
        "architecture_strategy_rejected": semantic,
        "next_distinct_architecture_authorized": semantic,
        "isolated_field_repair_authorized": False,
        "support_alignment_authorized": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "exact_next_action": (
            "advance to episode bootstrap plus segment specialists with LLM join"
            if semantic
            else "audit immutable v271 infrastructure attempt; no retry"
        ),
    }
    gate_path = root / "architecture-structural-gate.json"
    if gate_path.is_file():
        terminal["gate"] = _record(gate_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_v271(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v271 terminal")
    frozen = freeze_v271(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V271DeltaSetEditorError("launch exists; replay prohibited")
        )
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 1,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "managed_chatgpt_auth_only": True,
            "holdout_authorized": False,
            "production_mutation_allowed": False,
        },
    )
    started = time.monotonic()
    paths = frozen["turn"]["paths"]
    try:
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=frozen["turn"]["base"],
                prompt=frozen["turn"]["prompt"],
                output_schema=frozen["turn"]["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                batch_size=2,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=paths["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise V271DeltaSetEditorError("v271 turn did not complete")
        usage = _measured_usage(_load_json(paths["sidecar"], "v271 sidecar"))
        if usage is None:
            raise V271DeltaSetEditorError("v271 sidecar accounting or auth failed")
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise V271ArchitectureStop("v271 measured turn exceeded frozen token bound")
        normalized, provenance, diagnostics, applicability, coverage = project_output(
            result.output, frozen["turn"]
        )
        artifacts = {
            "normalized": root / "normalized-output.private.json",
            "provenance": root / "evidence-provenance.private.json",
            "diagnostics": root / "diagnostics.private.json",
            "applicability": root / "applicability-receipt.json",
            "coverage": root / "delta-ownership-receipt.json",
            "gate": root / "architecture-structural-gate.json",
        }
        _write_immutable(artifacts["normalized"], normalized)
        _write_immutable(artifacts["provenance"], provenance)
        _write_immutable(artifacts["diagnostics"], {"segments": diagnostics})
        _write_immutable(artifacts["applicability"], applicability)
        _write_immutable(artifacts["coverage"], coverage)
        gate = _gate(usage=usage, diagnostics=diagnostics, coverage=coverage)
        _write_immutable(artifacts["gate"], gate)
        if not gate["passed"]:
            raise V271ArchitectureStop("v271 structural quality or cost gate failed")
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "v271_architecture_structural_gate_passed",
            "terminal_reason": "v271_compact_delta_set_editor_structural_cost_gate_passed",
            "semantic_attempt_count": 1,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "new_turn_usage": usage,
            "adopted_v249_tokens": ADOPTED_V249_TOKENS,
            "combined_tokens": gate["combined_tokens"],
            "production_amortized_total_token_ratio": gate[
                "production_amortized_total_token_ratio"
            ],
            "dense_event_count": gate["dense_event_count"],
            "candidate_only_nominal_no_signal_event_count": gate[
                "candidate_only_nominal_no_signal_event_count"
            ],
            "residual_support_audit_required": gate["residual_support_audit_required"],
            "support_alignment_authorized": True,
            "development_winner_frozen": False,
            "holdout_authorized": False,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "wall_seconds": round(time.monotonic() - started, 6),
            "gate": _record(artifacts["gate"]),
            "normalized_output": _record(artifacts["normalized"]),
            "evidence_provenance": _record(artifacts["provenance"]),
            "applicability_receipt": _record(artifacts["applicability"]),
            "delta_ownership_receipt": _record(artifacts["coverage"]),
            "sidecar": _record(paths["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": "run frozen side-free support and neutral alignment before holdout",
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v271 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v271 compact global delta set editor")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v271(output_dir=Path(args.output_dir))
        result = {
            "state": "frozen",
            "root": str(frozen["root"]),
            "prompt_bytes": frozen["turn"]["prompt_bytes"],
            "base_bytes": frozen["turn"]["base_bytes"],
            "schema_bytes": frozen["turn"]["schema_bytes"],
        }
    else:
        terminal = asyncio.run(
            run_v271(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("new_turn_usage") or terminal.get("usage") or {}).get(
                "total_tokens"
            ),
            "support_alignment_authorized": terminal.get("support_alignment_authorized", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
