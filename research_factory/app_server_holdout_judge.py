from __future__ import annotations

"""Fail-closed semantic and cost gate for the frozen app-server holdout.

The gate is deliberately read-only with respect to the production database.  It
consumes the already-frozen reference, candidate, and baseline artifacts, builds
one blinded augmented-reference witness pool, and delegates every semantic
decision to the managed-ChatGPT Codex app-server judge.  Deterministic code is
limited to provenance, exact spans, schema/accounting checks, sampling math, and
the precommitted terminal gates.
"""

import argparse
import asyncio
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from contextlib import AsyncExitStack
from copy import deepcopy
from fractions import Fraction
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from . import app_server_evaluation as evaluation
from . import app_server_expanded_cap_episode_batch as expanded_cap
from .app_server_dev_selection import (
    DEV_MEMBERSHIP_VERSION,
    SelectionBlocked,
    _canonical_json,
    _event_hash,
    _inside,
    _output_hash_matches,
    _sum_usage,
    _valid_usage,
    merge_judge_consensuses,
    plan_judge_shards,
    score_dev_shared_reference,
    source_cluster_paired_bootstrap,
)
from .app_server_evaluation import APP_SERVER_DEVELOPMENT_MANIFEST_V2
from .app_server_holdout import (
    HOLDOUT_COVENANT_VERSION,
    verify_frozen_holdout,
    verify_holdout_model_call_authorization,
)
from .app_server_holdout_execution import (
    HOLDOUT_BASELINE_PHASE_VERSION,
    HOLDOUT_CANDIDATE_PHASE_VERSION,
    HOLDOUT_CONTEXT_PHASE_VERSION,
    HOLDOUT_EXECUTION_PLAN_VERSION,
    verify_stratified_selection,
)
from .app_server_holdout_stratifier import (
    REFERENCE_CASE_VERSION,
    REFERENCE_INDEX_VERSION,
)
from .app_server_capacity import CapacityGatedCodexAppServerClient
from .app_server_checkpoint import (
    validate_holdout_leaf_binding,
    validate_managed_sidecar_execution_lineage,
    verify_instruction_contract,
)
from .app_server_holdout_client import (
    resolve_holdout_client_factory,
    verified_holdout_execution_lineage,
)
from .app_server_llm_judge import (
    JUDGE_CONSENSUS_VERSION,
    make_shared_witness_pool,
    run_app_server_semantic_judge,
    write_immutable_json,
)
from .codex_app_server import APP_SERVER_CLIENT_VERSION, CodexAppServerClient
from .labels import validate_label_output
from .paths import db_path, root as factory_root
from .util import sha256_text


HOLDOUT_JUDGE_PLAN_VERSION = "pif_app_server_holdout_judge_plan_v2"
HOLDOUT_POOL_ASSEMBLY_VERSION = "pif_app_server_holdout_witness_assembly_v2"
HOLDOUT_MEMBERSHIP_VERSION = "pif_app_server_holdout_membership_index_v2"
HOLDOUT_FULL_JUDGE_VERSION = "pif_app_server_holdout_full_judge_v2"
HOLDOUT_SCORE_VERSION = "pif_app_server_holdout_shared_reference_score_v2"
HOLDOUT_GATE_VERSION = "pif_app_server_holdout_gate_v3"
HOLDOUT_TERMINAL_RECEIPT_VERSION = "pif_app_server_holdout_terminal_receipt_v1"

REFERENCE_SYSTEM = "holdout_preexecution_reference"
BASELINE_RAW_SYSTEM = "holdout_baseline_raw"
BASELINE_REPAIRED_SYSTEM = "baseline_repaired_db"  # scorer's frozen baseline ID
CANDIDATE_RAW_SYSTEM = "holdout_frozen_winner_raw"
CANDIDATE_WINNER_SYSTEM = "holdout_frozen_winner_normalized"

JUDGE_MODEL = "gpt-5.6-sol"
JUDGE_REASONING_EFFORT = "high"
HOLDOUT_GATES = {
    "paired_quality_cases": 60,
    "clean_no_signal_cases": 60,
    "bootstrap_iterations": 10000,
    "bootstrap_confidence": 0.95,
    "bootstrap_seed": "app-server-holdout-paired-source-bootstrap-v1",
    "max_paired_f1_drop": 0.03,
    "max_macro_source_f1_drop": 0.05,
    "max_worst_source_f1_drop": 0.05,
    "max_stratum_f1_drop": {
        "no_signal": 0.05,
        "low": 0.05,
        "medium": 0.05,
        "dense": 0.05,
    },
    "max_no_signal_false_positive_rate": 0.05,
    "no_signal_one_sided_confidence": 0.95,
    "max_abstained_case_rate": 0.05,
    "max_production_amortized_total_token_ratio": 0.28,
}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, purpose: str) -> Dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise SelectionBlocked("%s is missing: %s" % (purpose, resolved))
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SelectionBlocked("%s is not valid UTF-8 JSON" % purpose) from exc
    if not isinstance(value, dict):
        raise SelectionBlocked("%s must be a JSON object" % purpose)
    return value


def _resolve_artifact(value: Any, *, base: Optional[Path] = None) -> Path:
    if not isinstance(value, str) or not value:
        raise SelectionBlocked("artifact path is missing")
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return ((base or factory_root()) / path).resolve()


def _phase_report(
    execution_root: Path,
    relative: str,
    *,
    schema_version: str,
    purpose: str,
) -> Tuple[Path, Dict[str, Any]]:
    path = execution_root / relative
    payload = _load_json(path, purpose=purpose)
    if payload.get("schema_version") != schema_version:
        raise SelectionBlocked("%s schema version mismatch" % purpose)
    if (
        payload.get("ok") is not True
        or payload.get("execution_complete") is not True
        or payload.get("accounting_complete") is not True
        or payload.get("retry_count") != 0
        or payload.get("production_database_mutation") is not False
        or payload.get("production_promotion") is not False
        or not _valid_usage(payload.get("usage"))
    ):
        raise SelectionBlocked("%s is incomplete, unmeasured, retried, or unsafe" % purpose)
    return path, payload


_TERMINAL_ARTIFACT_PATHS = {
    "judge_plan": "judge-plan.json",
    "witness_assembly": "witness-pool/assembly-report.json",
    "witness_pool": "witness-pool/shared-witness-pool.private.json",
    "witness_private_mapping": "witness-pool/private-mapping.json",
    "witness_membership_index": "witness-pool/membership-index.private.json",
    "full_judge_report": "full-judge/report.json",
    "full_judge_consensus": "full-judge/consensus.private.json",
    "score_report": "score-report.private.json",
}
_PASSED_TERMINAL_ARTIFACTS = frozenset(_TERMINAL_ARTIFACT_PATHS)


def _terminal_artifact_record(path: Path, *, root: Path) -> Dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not _inside(resolved, root) or not resolved.is_file():
        raise SelectionBlocked("terminal receipt artifact is missing or outside its root")
    return {
        "path": str(resolved),
        "sha256": _sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _terminal_input_bindings(plan: Mapping[str, Any]) -> Dict[str, Any]:
    fields = (
        "covenant_sha256",
        "selection_sha256",
        "execution_plan_sha256",
        "phase_report_sha256s",
        "reference_index_sha256",
        "calibration_report_sha256",
        "candidate_report_sha256",
        "candidate_mapping_sha256",
        "witness_pool_sha256",
        "private_mapping_sha256",
        "membership_index_sha256",
        "judge_model",
        "judge_reasoning_effort",
        "instruction_contract",
        "execution_lineage",
        "turn_semantic_outputs",
        "deterministic_consensus_additional_model_calls",
    )
    return {field: deepcopy(plan.get(field)) for field in fields}


def _build_terminal_receipt(
    root: Path, *, terminal_status: str, plan_sha256: Optional[str]
) -> Dict[str, Any]:
    records = {
        name: _terminal_artifact_record(root / relative, root=root)
        for name, relative in _TERMINAL_ARTIFACT_PATHS.items()
        if (root / relative).is_file()
    }
    plan = (
        _load_json(root / "judge-plan.json", purpose="terminal receipt judge plan")
        if "judge_plan" in records
        else {}
    )
    if plan_sha256 is not None and (
        "judge_plan" not in records
        or records["judge_plan"]["sha256"] != plan_sha256
    ):
        raise SelectionBlocked("terminal receipt judge-plan binding drift")
    return {
        "schema_version": HOLDOUT_TERMINAL_RECEIPT_VERSION,
        "state": "closed_no_retry",
        "terminal_status": terminal_status,
        "judge_plan_sha256": plan_sha256,
        "input_bindings": _terminal_input_bindings(plan) if plan else {},
        "artifact_records": records,
        "artifact_count": len(records),
        "retry_count": 0,
        "automatic_retry_prohibited": True,
        "production_changed": False,
    }


def _validate_terminal_full_judge_lineage(
    root: Path,
    *,
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any],
) -> None:
    """Recursively validate every terminal holdout-judge process receipt.

    The outer terminal receipt checksum-binds the aggregate report, but the
    per-shard sidecars are intentionally private leaf artifacts.  Validate
    those leaves on every adoption so changing or deleting execution evidence
    cannot be hidden behind an unchanged aggregate report.
    """

    full_root = (Path(root) / "full-judge").resolve()
    report_path = full_root / "report.json"
    if not report_path.is_file():
        raise SelectionBlocked("terminal holdout full-judge report is missing")
    report = _load_json(report_path, purpose="terminal holdout full-judge report")
    shards = report.get("shards")
    if (
        report.get("instruction_contract") != instruction_contract
        or report.get("execution_lineage") != execution_lineage
        or report.get("turn_semantic_outputs") != ["support", "alignment"]
        or report.get("deterministic_consensus_additional_model_calls") != 0
        or not isinstance(shards, list)
        or report.get("shard_count") != len(shards)
    ):
        raise SelectionBlocked("terminal holdout full-judge execution lineage drift")

    for index, shard_record in enumerate(shards):
        if not isinstance(shard_record, Mapping):
            raise SelectionBlocked("terminal holdout judge shard record is malformed")
        shard_judge_root = full_root / ("shard-%03d" % index) / "judge"
        shard_report_path = shard_judge_root / "report.json"
        if (
            Path(str(shard_record.get("report_path") or "")).expanduser().resolve()
            != shard_report_path
            or not shard_report_path.is_file()
            or shard_record.get("report_sha256") != _sha256_file(shard_report_path)
        ):
            raise SelectionBlocked("terminal holdout judge shard report drift")
        shard_report = _load_json(
            shard_report_path, purpose="terminal holdout judge shard report"
        )
        spec_path = shard_judge_root / "judge-spec.json"
        sidecar_paths = shard_report.get("sidecar_paths")
        leaf_bindings = shard_report.get("leaf_bindings")
        expected_sidecars = [
            shard_judge_root / "sidecars" / "ab.json",
            shard_judge_root / "sidecars" / "ba.json",
        ]
        if (
            shard_report.get("state") != "completed"
            or shard_report.get("instruction_contract") != instruction_contract
            or shard_report.get("execution_lineage") != execution_lineage
            or shard_report.get("turn_semantic_outputs") != ["support", "alignment"]
            or shard_report.get("deterministic_consensus_additional_model_calls") != 0
            or not spec_path.is_file()
            or shard_report.get("spec_sha256") != _sha256_file(spec_path)
            or not isinstance(sidecar_paths, list)
            or [Path(str(item)).expanduser().resolve() for item in sidecar_paths]
            != expected_sidecars
            or not isinstance(leaf_bindings, list)
            or len(leaf_bindings) != 2
            or shard_record.get("leaf_bindings_sha256")
            != sha256_text(_canonical_json(leaf_bindings))
        ):
            raise SelectionBlocked("terminal holdout judge shard lineage drift")
        spec = _load_json(spec_path, purpose="terminal holdout judge shard spec")
        if (
            spec.get("instruction_contract") != instruction_contract
            or spec.get("execution_lineage") != execution_lineage
            or spec.get("turn_semantic_outputs") != ["support", "alignment"]
            or spec.get("deterministic_consensus_additional_model_calls") != 0
        ):
            raise SelectionBlocked("terminal holdout judge shard specification drift")
        try:
            validated_bindings = [
                validate_holdout_leaf_binding(
                    binding,
                    instruction_contract=instruction_contract,
                    execution_lineage=execution_lineage,
                    expected_model=str(shard_report.get("model") or ""),
                    expected_effort=str(
                        shard_report.get("reasoning_effort") or ""
                    ),
                )
                for binding in leaf_bindings
            ]
        except ValueError as exc:
            raise SelectionBlocked(str(exc)) from exc
        if (
            [item["artifacts"]["sidecar"]["path"] for item in validated_bindings]
            != [str(path) for path in expected_sidecars]
            or len({item["turn_id"] for item in validated_bindings}) != 2
        ):
            raise SelectionBlocked("terminal holdout judge leaf identity drift")


def _validate_terminal_gate(
    path: Path,
    payload: Mapping[str, Any],
    *,
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any],
) -> Dict[str, Any]:
    if payload.get("schema_version") != HOLDOUT_GATE_VERSION:
        raise SelectionBlocked("terminal holdout gate schema drift")
    if (
        payload.get("terminal_status") not in {"passed", "blocked"}
        or not isinstance(payload.get("gate_passed"), bool)
        or not isinstance(payload.get("quality_noninferior"), bool)
        or not isinstance(
            payload.get("production_amortized_total_token_ratio_lte_0_28"), bool
        )
        or payload.get("retry_count") != 0
        or payload.get("automatic_retry_prohibited") is not True
        or payload.get("production_database_mutation") is not False
        or payload.get("production_promotion") is not False
        or payload.get("production_changed") is not False
        or payload.get("instruction_contract") != instruction_contract
        or payload.get("execution_lineage") != execution_lineage
    ):
        raise SelectionBlocked("terminal holdout gate invariant drift")
    expected_pass = bool(
        payload["quality_noninferior"]
        and payload["production_amortized_total_token_ratio_lte_0_28"]
    )
    if payload["gate_passed"] != expected_pass or (
        payload["terminal_status"] == "passed"
    ) != expected_pass:
        raise SelectionBlocked("terminal holdout gate decision fields disagree")
    receipt = payload.get("terminal_receipt")
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("schema_version") != HOLDOUT_TERMINAL_RECEIPT_VERSION
        or receipt.get("state") != "closed_no_retry"
        or receipt.get("terminal_status") != payload.get("terminal_status")
        or receipt.get("judge_plan_sha256") != payload.get("judge_plan_sha256")
        or receipt.get("retry_count") != 0
        or receipt.get("automatic_retry_prohibited") is not True
        or receipt.get("production_changed") is not False
        or not isinstance(receipt.get("artifact_records"), Mapping)
        or receipt.get("artifact_count") != len(receipt["artifact_records"])
    ):
        raise SelectionBlocked("terminal holdout receipt invariant drift")
    records = receipt["artifact_records"]
    if any(name not in _TERMINAL_ARTIFACT_PATHS for name in records):
        raise SelectionBlocked("terminal holdout receipt contains an unknown artifact")
    verified_records: Dict[str, Dict[str, Any]] = {}
    for name, record in records.items():
        if not isinstance(record, Mapping):
            raise SelectionBlocked("terminal holdout receipt artifact is malformed")
        artifact_path = _resolve_artifact(record.get("path"))
        expected_path = (path.parent / _TERMINAL_ARTIFACT_PATHS[name]).resolve()
        if (
            artifact_path != expected_path
            or not _inside(artifact_path, path.parent)
            or not artifact_path.is_file()
            or record.get("sha256") != _sha256_file(artifact_path)
            or record.get("size_bytes") != artifact_path.stat().st_size
        ):
            raise SelectionBlocked("terminal holdout receipt artifact hash drift")
        verified_records[name] = dict(record)
    plan_sha = payload.get("judge_plan_sha256")
    plan_path = path.parent / "judge-plan.json"
    if plan_sha is not None and (
        not isinstance(plan_sha, str)
        or not plan_path.is_file()
        or _sha256_file(plan_path) != plan_sha
    ):
        raise SelectionBlocked("terminal holdout gate plan hash drift")
    if plan_sha is None:
        if "judge_plan" in records or receipt.get("input_bindings") != {}:
            raise SelectionBlocked("terminal receipt has unbound plan evidence")
    else:
        if (
            "judge_plan" not in records
            or records["judge_plan"].get("sha256") != plan_sha
        ):
            raise SelectionBlocked("terminal receipt omits its judge plan")
        plan = _load_json(plan_path, purpose="terminal holdout judge plan")
        if (
            plan.get("instruction_contract") != instruction_contract
            or plan.get("execution_lineage") != execution_lineage
            or plan.get("turn_semantic_outputs") != ["support", "alignment"]
            or plan.get("deterministic_consensus_additional_model_calls") != 0
        ):
            raise SelectionBlocked("terminal holdout judge execution lineage drift")
        if receipt.get("input_bindings") != _terminal_input_bindings(plan):
            raise SelectionBlocked("terminal receipt input bindings differ from judge plan")
        cross_bindings = {
            "witness_pool": "witness_pool_sha256",
            "witness_private_mapping": "private_mapping_sha256",
            "witness_membership_index": "membership_index_sha256",
        }
        if any(
            name in records and records[name].get("sha256") != plan.get(plan_field)
            for name, plan_field in cross_bindings.items()
        ):
            raise SelectionBlocked("terminal receipt witness artifacts differ from judge plan")
        if "full_judge_report" in records:
            _validate_terminal_full_judge_lineage(
                path.parent,
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
    if expected_pass:
        quality = payload.get("quality") or {}
        cost = payload.get("cost") or {}
        judge = payload.get("judge") or {}
        if (
            quality.get("passed") is not True
            or cost.get("passed_lte_0_28") is not True
            or judge.get("accounting_complete") is not True
            or not _valid_usage(judge.get("usage"))
        ):
            raise SelectionBlocked("passed holdout gate no longer has passing evidence")
        if set(records) != _PASSED_TERMINAL_ARTIFACTS:
            raise SelectionBlocked("passed holdout gate artifact closure is incomplete")
        artifacts = payload.get("artifacts") or {}
        if (
            records["full_judge_report"]["sha256"]
            != judge.get("report_sha256")
            or records["full_judge_consensus"]["sha256"]
            != judge.get("consensus_sha256")
            or records["witness_assembly"]["sha256"]
            != artifacts.get("witness_assembly_sha256")
            or records["score_report"]["sha256"]
            != artifacts.get("score_report_sha256")
        ):
            raise SelectionBlocked("passed holdout gate evidence differs from terminal receipt")
    return dict(payload)


def _load_reservoir_rows(covenant_path: Path, covenant: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for name, reservoir in (
        ("paired_quality_reservoir", "paired_quality_reservoir"),
        (
            "terminal_position_no_signal_candidates",
            "terminal_position_no_signal_candidates",
        ),
    ):
        record = (covenant.get("artifacts") or {}).get(name)
        if not isinstance(record, dict) or not isinstance(record.get("filename"), str):
            raise SelectionBlocked("holdout covenant reservoir is missing: %s" % name)
        path = (covenant_path.parent / record["filename"]).resolve()
        if not path.is_file() or _sha256_file(path) != record.get("sha256"):
            raise SelectionBlocked("holdout covenant reservoir hash drift: %s" % name)
        payload = _load_json(path, purpose=name)
        for item in payload.get("segments") or []:
            if not isinstance(item, dict):
                raise SelectionBlocked("holdout reservoir contains a malformed segment")
            rows.append({**item, "reservoir": reservoir})
    ids = [str(item.get("segment_id") or "") for item in rows]
    if not ids or len(ids) != len(set(ids)) or any(not item for item in ids):
        raise SelectionBlocked("holdout reservoirs are empty, overlapping, or malformed")
    return rows


def _verified_sources(
    conn: sqlite3.Connection,
    selection_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    result = {}
    for frozen in selection_rows:
        segment_id = str(frozen["segment_id"])
        row = conn.execute(
            """
            SELECT sg.id, sg.episode_id, sg.transcript_id, sg.source_id,
                   sg.segment_index, sg.text_path, sg.text_sha256,
                   s.name AS source_name
            FROM segments sg JOIN sources s ON s.id = sg.source_id
            WHERE sg.id = ?
            """,
            (segment_id,),
        ).fetchone()
        if row is None:
            raise SelectionBlocked("frozen holdout segment is missing: %s" % segment_id)
        for field in ("episode_id", "transcript_id", "text_sha256"):
            if str(row[field]) != str(frozen[field]):
                raise SelectionBlocked("frozen holdout %s drift: %s" % (field, segment_id))
        path = _resolve_artifact(row["text_path"])
        if not path.is_file() or _sha256_file(path) != str(frozen["text_sha256"]):
            raise SelectionBlocked("frozen holdout DB/file text hash drift: %s" % segment_id)
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            raise SelectionBlocked("frozen holdout segment is not UTF-8") from exc
        result[segment_id] = {
            "text": text,
            "path": str(path),
            "file_sha256": _sha256_file(path),
            "source_id": str(row["source_id"]),
            "source_name": str(row["source_name"]),
            "segment_index": int(row["segment_index"]),
            "episode_id": str(row["episode_id"]),
        }
    return result


def _validate_managed_sidecar(
    sidecar_path: Path,
    *,
    raw_output_path: Path,
    model: str,
    effort: str,
    thread_mode: str,
    batch_size: int,
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any],
) -> Dict[str, Any]:
    sidecar = _load_json(sidecar_path, purpose="app-server sidecar")
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("transport") != "stdio"
        or sidecar.get("client_version") != APP_SERVER_CLIENT_VERSION
        or sidecar.get("model") != model
        or sidecar.get("effort") != effort
        or sidecar.get("thread_mode") != thread_mode
        or int(sidecar.get("batch_size") or -1) != batch_size
        or sidecar.get("usage_complete") is not True
        or not _valid_usage(sidecar.get("usage"))
        or not _output_hash_matches(raw_output_path, sidecar.get("output_sha256"))
    ):
        raise SelectionBlocked("app-server sidecar is not a complete managed-ChatGPT turn")
    try:
        validate_managed_sidecar_execution_lineage(
            sidecar=sidecar,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
    except ValueError as exc:
        raise SelectionBlocked(str(exc)) from exc
    return sidecar


def _load_reference_cases(
    *,
    selection_path: Path,
    selection: Mapping[str, Any],
    selection_rows: Sequence[Mapping[str, Any]],
    sources: Mapping[str, Mapping[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    index_path = selection_path.parent / "reference-index.private.json"
    if not index_path.is_file() or _sha256_file(index_path) != selection.get(
        "reference_artifact_sha256"
    ):
        raise SelectionBlocked("frozen holdout reference index is missing or changed")
    index = _load_json(index_path, purpose="holdout reference index")
    if (
        index.get("schema_version") != REFERENCE_INDEX_VERSION
        or index.get("covenant_sha256") != selection.get("covenant_sha256")
        or not isinstance(index.get("cases"), list)
    ):
        raise SelectionBlocked("holdout reference index provenance mismatch")
    index_by_segment = {
        str(item.get("segment_id")): item
        for item in index["cases"]
        if isinstance(item, dict) and item.get("segment_id")
    }
    result = {}
    artifacts = []
    for selected in selection_rows:
        segment_id = str(selected["segment_id"])
        item = index_by_segment.get(segment_id)
        if item is None:
            raise SelectionBlocked("selected holdout case is absent from the reference index")
        case_path = _resolve_artifact(item.get("reference_case_path"), base=selection_path.parent)
        expected_hash = selected.get("reference_case_sha256")
        if (
            not _inside(case_path, selection_path.parent)
            or not case_path.is_file()
            or _sha256_file(case_path) != expected_hash
            or item.get("reference_case_sha256") != expected_hash
        ):
            raise SelectionBlocked("frozen holdout reference case hash drift: %s" % segment_id)
        case = _load_json(case_path, purpose="holdout reference case")
        events = case.get("supported_events")
        if (
            case.get("schema_version") != REFERENCE_CASE_VERSION
            or case.get("segment_id") != segment_id
            or case.get("episode_id") != selected.get("episode_id")
            or case.get("text_sha256") != selected.get("text_sha256")
            or case.get("coverage_complete") is not True
            or case.get("selection_eligible") is not True
            or not isinstance(events, list)
            or len(events) != int(selected.get("reference_event_count") or 0)
        ):
            raise SelectionBlocked("frozen holdout reference case is incomplete: %s" % segment_id)
        source_text = str(sources[segment_id]["text"])
        for event in events:
            if not isinstance(event, dict):
                raise SelectionBlocked("reference event is malformed")
            evidence = event.get("evidence")
            if not isinstance(evidence, str) or not evidence or evidence not in source_text:
                raise SelectionBlocked("reference event evidence is not exact")
        if selected.get("evaluation_set") == "clean_no_signal_power" and (
            events or case.get("clean_no_signal") is not True
        ):
            raise SelectionBlocked("clean no-signal power case is not reference-confirmed clean")
        result[segment_id] = case
        artifacts.append({"segment_id": segment_id, "path": str(case_path), "sha256": expected_hash})
    return result, {
        "index_path": str(index_path),
        "index_sha256": _sha256_file(index_path),
        "cases": artifacts,
    }


def _manifest_rows(manifest: Mapping[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    rows = []
    by_id = {}
    for episode in manifest.get("episodes") or []:
        if not isinstance(episode, dict) or not isinstance(episode.get("segments"), list):
            raise SelectionBlocked("holdout candidate manifest is malformed")
        for item in episode["segments"]:
            if not isinstance(item, dict):
                raise SelectionBlocked("holdout candidate manifest segment is malformed")
            row = {
                **item,
                "episode_id": episode.get("episode_id"),
                "source_id": episode.get("source_id"),
                "source_name": episode.get("source_name"),
            }
            segment_id = str(row.get("segment_id") or "")
            if not segment_id or segment_id in by_id:
                raise SelectionBlocked("holdout candidate manifest IDs are invalid")
            rows.append(row)
            by_id[segment_id] = row
    return rows, by_id


def _verified_record(
    record: Any, *, root: Path, purpose: str
) -> Tuple[Path, Dict[str, Any]]:
    if not isinstance(record, Mapping):
        raise SelectionBlocked("%s record is missing" % purpose)
    path = _resolve_artifact(record.get("path"))
    if (
        not _inside(path, root)
        or not path.is_file()
        or record.get("sha256") != _sha256_file(path)
        or record.get("size_bytes") != path.stat().st_size
    ):
        raise SelectionBlocked("%s record drift" % purpose)
    return path, _load_json(path, purpose=purpose)


def _load_candidate_outputs(
    *,
    conn: sqlite3.Connection,
    execution_root: Path,
    candidate_phase: Mapping[str, Any],
    context_phase: Mapping[str, Any],
    covenant: Mapping[str, Any],
    selection_rows: Sequence[Mapping[str, Any]],
    sources: Mapping[str, Mapping[str, Any]],
    instruction_contract: Mapping[str, Any] | None = None,
    execution_lineage: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    if (instruction_contract is None) != (execution_lineage is None):
        raise SelectionBlocked("holdout candidate execution lineage inputs are incomplete")
    manifest_path = _resolve_artifact(context_phase.get("manifest_path"))
    if (
        not _inside(manifest_path, execution_root / "phase-a-contexts")
        or _sha256_file(manifest_path) != context_phase.get("manifest_sha256")
    ):
        raise SelectionBlocked("holdout candidate manifest hash drift")
    manifest = _load_json(manifest_path, purpose="holdout candidate manifest")
    manifest_rows, manifest_by_id = _manifest_rows(manifest)
    selected_ids = [str(item["segment_id"]) for item in selection_rows]
    if (
        manifest.get("schema_version") != APP_SERVER_DEVELOPMENT_MANIFEST_V2
        or manifest.get("evaluation_role")
        != "untouched_private_holdout_never_production"
        or manifest.get("covenant_sha256") != covenant.get("covenant_sha256")
        or manifest.get("event_cap") != expanded_cap.MAX_EVENTS_PER_SEGMENT
        or len(manifest_rows) != len(selected_ids)
        or set(manifest_by_id) != set(selected_ids)
        or int(manifest.get("segment_count") or -1) != len(selected_ids)
    ):
        raise SelectionBlocked("holdout candidate manifest does not match the frozen selection")
    for selected in selection_rows:
        row = manifest_by_id[str(selected["segment_id"])]
        if (
            str(row.get("episode_id")) != str(selected["episode_id"])
            or str(row.get("text_sha256")) != str(selected["text_sha256"])
        ):
            raise SelectionBlocked("holdout candidate manifest selection drift")

    report_path = _resolve_artifact(candidate_phase.get("candidate_arm_report_path"))
    phase_root = execution_root / "phase-b-candidate"
    if (
        not _inside(report_path, phase_root)
        or not report_path.is_file()
        or _sha256_file(report_path) != candidate_phase.get("candidate_arm_report_sha256")
    ):
        raise SelectionBlocked("holdout candidate arm report hash drift")
    report = _load_json(report_path, purpose="holdout candidate arm report")
    winner = covenant.get("winner") or {}
    winner_config = winner.get("frozen_configuration")
    if (
        not isinstance(winner_config, dict)
        or sha256_text(_canonical_json(winner_config))
        != winner.get("frozen_configuration_sha256")
    ):
        raise SelectionBlocked("holdout covenant winner has no hash-bound extraction config")
    try:
        verified_config = expanded_cap.verify_frozen_configuration(winner_config)
    except expanded_cap.ExpandedCapEpisodeBatchError as exc:
        raise SelectionBlocked("holdout candidate frozen configuration drifted") from exc
    expected = len(selected_ids)
    if (
        candidate_phase.get("evaluation_role")
        != "untouched_private_holdout_never_production"
        or candidate_phase.get("holdout_authorized") is not True
        or candidate_phase.get("fixture_mode") is not False
        or candidate_phase.get("automatic_retry_prohibited") is not True
        or candidate_phase.get("production_changed") is not False
        or candidate_phase.get("frozen_configuration_sha256")
        != winner.get("frozen_configuration_sha256")
        or candidate_phase.get("capacity_admission_sha256") in (None, "")
        or int(candidate_phase.get("requested_segments") or -1) != expected
        or report.get("schema_version") != expanded_cap.RUN_REPORT_VERSION
        or report.get("state") != "passed"
        or report.get("evaluation_role")
        != "development_cold_warm_regression_precommit"
        or report.get("development_regression_eligible") is not True
        or report.get("holdout_authorized") is not False
        or report.get("winner_system_id") != expanded_cap.WINNER_SYSTEM_ID
        or report.get("batch_size") != winner.get("batch_size")
        or report.get("thread_mode") != winner.get("thread_mode")
        or report.get("model") != winner.get("model")
        or report.get("effort") != winner.get("reasoning_effort")
        or report.get("max_events_per_segment")
        != expanded_cap.MAX_EVENTS_PER_SEGMENT
        or report.get("semantic_postprocessing") is not False
        or report.get("accounting_complete") is not True
        or report.get("usage_status") != "complete"
        or report.get("attempt_contract_failures") != 0
        or report.get("sidecar_contract_failures") != 0
        or report.get("usage_unknown_attempts") != 0
        or report.get("ambiguous_outcome_attempts") != 0
        or report.get("ambiguous_retry_count") != 0
        or report.get("retry_count") != 0
        or report.get("all_emitted_events_preserved") is not True
        or report.get("capacity_binding_valid") is not True
        or report.get("fixture_mode") is not False
        or report.get("preflight_lineage_valid") is not True
        or report.get("turn_ids_unique") is not True
        or report.get("thread_lineage_valid") is not True
        or report.get("production_mutated") is not False
        or not _valid_usage(report.get("usage"))
        or candidate_phase.get("usage") != report.get("usage")
    ):
        raise SelectionBlocked("holdout candidate arm is incomplete or differs from the frozen winner")

    arm_root = report_path.parent
    _config_path, recorded_config = _verified_record(
        report.get("frozen_configuration"),
        root=arm_root,
        purpose="holdout candidate frozen configuration",
    )
    _capacity_binding_path, capacity_binding = _verified_record(
        report.get("capacity_binding"),
        root=arm_root,
        purpose="holdout candidate capacity binding",
    )
    _run_configuration_path, run_configuration = _verified_record(
        report.get("run_configuration"),
        root=arm_root,
        purpose="holdout candidate run configuration",
    )
    _preflight_path, preflight = _verified_record(
        report.get("no_model_preflight"),
        root=arm_root,
        purpose="holdout candidate no-model preflight",
    )
    capacity_sha256 = candidate_phase.get("capacity_admission_sha256")
    _capacity_admission_path, capacity_admission = _verified_record(
        capacity_binding.get("capacity_admission"),
        root=arm_root,
        purpose="holdout candidate capacity admission",
    )
    if (
        recorded_config != verified_config
        or capacity_binding.get("schema_version")
        != expanded_cap.CAPACITY_BINDING_VERSION
        or capacity_binding.get("state") != "verified_before_app_server_start"
        or capacity_binding.get("expected_canonical_sha256") != capacity_sha256
        or capacity_binding.get("observed_canonical_sha256") != capacity_sha256
        or capacity_binding.get("fixture") is not False
        or capacity_binding.get("contract_sha256")
        != verified_config["capacity_admission_contract"]["contract_sha256"]
        or expanded_cap.capacity_admission_sha256(capacity_admission)
        != capacity_sha256
        or run_configuration.get("schema_version")
        != expanded_cap.RUN_CONFIGURATION_VERSION
        or run_configuration.get("state") != "capacity_bound_configuration"
        or run_configuration.get("winner_system_id")
        != expanded_cap.WINNER_SYSTEM_ID
        or run_configuration.get("batch_size") != winner.get("batch_size")
        or run_configuration.get("thread_mode") != winner.get("thread_mode")
        or run_configuration.get("model") != winner.get("model")
        or run_configuration.get("effort") != winner.get("reasoning_effort")
        or run_configuration.get("concurrency") != 1
        or run_configuration.get("frozen_configuration")
        != report.get("frozen_configuration")
        or run_configuration.get("capacity_binding")
        != report.get("capacity_binding")
        or run_configuration.get("capacity_admission_canonical_sha256")
        != capacity_sha256
        or run_configuration.get("fixture_mode") is not False
        or preflight.get("schema_version")
        != expanded_cap.NO_MODEL_PREFLIGHT_VERSION
        or preflight.get("state") != "passed"
        or preflight.get("semantic_model_call_count") != 0
        or preflight.get("semantic_turn_started") is not False
    ):
        raise SelectionBlocked("holdout candidate configuration/capacity binding drift")

    prepared_episodes = evaluation._load_prepared_episodes(  # noqa: SLF001
        conn,
        manifest=manifest,
        window_count=int(winner["window_count"]),
        context_chars=int(winner["context_chars"]),
    )
    requests = [
        request
        for episode in prepared_episodes
        for request in expanded_cap.prepare_episode_batches(
            episode,
            batch_size=int(winner["batch_size"]),
            thread_mode=str(winner["thread_mode"]),
        )
    ]
    requests_by_batch = {str(row["batch_id"]): row for row in requests}
    mapping_path = _resolve_artifact(candidate_phase.get("candidate_mapping_path"))
    if (
        not _inside(mapping_path, arm_root)
        or _sha256_file(mapping_path) != candidate_phase.get("candidate_mapping_sha256")
    ):
        raise SelectionBlocked("holdout candidate private mapping hash drift")
    mapping = _load_json(mapping_path, purpose="holdout candidate private mapping")
    if (
        mapping.get("schema_version") != expanded_cap.PRIVATE_MAPPING_VERSION
        or mapping.get("winner_system_id") != expanded_cap.WINNER_SYSTEM_ID
        or not isinstance(mapping.get("batches"), list)
        or len(mapping["batches"]) != len(requests)
    ):
        raise SelectionBlocked("holdout candidate private mapping provenance mismatch")
    if instruction_contract is not None and execution_lineage is not None:
        candidate_bindings = candidate_phase.get("leaf_bindings")
        if not isinstance(candidate_bindings, list):
            raise SelectionBlocked("holdout candidate leaf bindings are missing")
        try:
            validated_candidate_bindings = [
                validate_holdout_leaf_binding(
                    binding,
                    instruction_contract=instruction_contract,
                    execution_lineage=execution_lineage,
                    expected_model=expanded_cap.MODEL,
                    expected_effort=expanded_cap.EFFORT,
                )
                for binding in candidate_bindings
            ]
        except ValueError as exc:
            raise SelectionBlocked(str(exc)) from exc
        mapped_sidecars = {
            str(_resolve_artifact(batch.get("sidecar_path")))
            for batch in mapping["batches"]
            if isinstance(batch, Mapping)
        }
        bound_sidecars = {
            item["artifacts"]["sidecar"]["path"]
            for item in validated_candidate_bindings
        }
        if mapped_sidecars != bound_sidecars:
            raise SelectionBlocked("holdout candidate leaf sidecar partition drift")
    mapped_ids = []
    raw_by_segment = {}
    normalized_by_segment = {}
    sidecar_usages = []
    artifacts = []
    telemetry_by_batch = {
        str(row.get("batch_id")): row
        for row in report.get("turn_telemetry") or []
        if isinstance(row, Mapping) and row.get("batch_id")
    }
    for batch in mapping["batches"]:
        if not isinstance(batch, dict) or not isinstance(batch.get("segment_ids"), list):
            raise SelectionBlocked("holdout candidate batch mapping is malformed")
        batch_id = str(batch.get("batch_id") or "")
        request = requests_by_batch.get(batch_id)
        segment_ids = [str(item) for item in batch["segment_ids"]]
        if (
            request is None
            or not segment_ids
            or segment_ids != [str(item) for item in request["segment_ids"]]
            or str(batch.get("episode_id")) != str(request["episode_id"])
        ):
            raise SelectionBlocked("holdout candidate batch is outside the frozen manifest")
        mapped_ids.extend(segment_ids)
        input_path = _resolve_artifact(batch.get("input_path"))
        attempt_path = _resolve_artifact(batch.get("attempt_receipt_path"))
        raw_path = _resolve_artifact(batch.get("raw_output_path"))
        normalized_path = _resolve_artifact(batch.get("normalized_output_path"))
        sidecar_path = _resolve_artifact(batch.get("sidecar_path"))
        if not all(
            _inside(path, arm_root)
            for path in (input_path, attempt_path, raw_path, normalized_path, sidecar_path)
        ):
            raise SelectionBlocked("holdout candidate mapping points outside its arm directory")
        private_input = _load_json(input_path, purpose="holdout candidate private input")
        raw = _load_json(raw_path, purpose="holdout candidate raw output")
        normalized = _load_json(normalized_path, purpose="holdout candidate normalized output")
        try:
            attempt = expanded_cap._validate_attempt_receipt(  # noqa: SLF001
                request, attempt_path
            )
            telemetry = expanded_cap.validate_turn_sidecar(
                request,
                sidecar_path,
                output_path=raw_path,
                require_completed=True,
                expected_thread_id=str(attempt["thread_id"]),
            )
            projected = expanded_cap.validate_and_project_output(request, raw)
        except expanded_cap.ExpandedCapEpisodeBatchError as exc:
            raise SelectionBlocked("holdout candidate final-adapter artifact failed validation") from exc
        if private_input != request["private_input"]:
            raise SelectionBlocked("holdout candidate private input differs from frozen request")
        sidecar = telemetry["sidecar"]
        if instruction_contract is not None and execution_lineage is not None:
            try:
                validate_managed_sidecar_execution_lineage(
                    sidecar=sidecar,
                    instruction_contract=instruction_contract,
                    execution_lineage=execution_lineage,
                )
            except ValueError as exc:
                raise SelectionBlocked(str(exc)) from exc
        sidecar_usages.append(telemetry["usage"])
        raw_rows = raw.get("segments")
        normalized_rows = normalized.get("segments")
        if (
            raw.get("episode_id") != request["episode_id"]
            or not isinstance(raw_rows, list)
            or [item.get("segment_id") for item in raw_rows if isinstance(item, dict)]
            != segment_ids
            or _canonical_json(projected["normalized"])
            != _canonical_json(normalized)
            or not isinstance(normalized_rows, list)
            or [item.get("segment_id") for item in normalized_rows if isinstance(item, dict)]
            != segment_ids
        ):
            raise SelectionBlocked("holdout candidate raw or normalized output drift")
        report_telemetry = telemetry_by_batch.get(batch_id)
        if (
            not isinstance(report_telemetry, Mapping)
            or report_telemetry.get("state") != "validated"
            or report_telemetry.get("attempted") is not True
            or report_telemetry.get("attempt_contract_valid") is not True
            or report_telemetry.get("sidecar_contract_valid") is not True
            or report_telemetry.get("thread_matches_attempt") is not True
            or report_telemetry.get("usage_status") != "complete"
            or report_telemetry.get("usage") != telemetry["usage"]
        ):
            raise SelectionBlocked("holdout candidate terminal telemetry receipt drift")
        for item in raw_rows:
            raw_by_segment[str(item["segment_id"])] = item
        for item in normalized_rows:
            normalized_by_segment[str(item["segment_id"])] = item
        artifacts.append(
            {
                "batch_id": batch_id,
                "input_sha256": _sha256_file(input_path),
                "attempt_receipt_sha256": _sha256_file(attempt_path),
                "raw_output_path": str(raw_path),
                "raw_output_sha256": _sha256_file(raw_path),
                "normalized_output_path": str(normalized_path),
                "normalized_output_sha256": _sha256_file(normalized_path),
                "sidecar_path": str(sidecar_path),
                "sidecar_sha256": _sha256_file(sidecar_path),
            }
        )
    if (
        len(mapped_ids) != expected
        or len(set(mapped_ids)) != expected
        or set(mapped_ids) != set(selected_ids)
        or set(raw_by_segment) != set(selected_ids)
        or set(normalized_by_segment) != set(selected_ids)
        or set(telemetry_by_batch) != set(requests_by_batch)
        or int(report.get("requested_calls") or -1) != len(mapping["batches"])
        or int(report.get("attempted_calls") or -1) != len(mapping["batches"])
        or int(report.get("validated_calls") or -1) != len(mapping["batches"])
        or int(report.get("terminal_sidecars") or -1) != len(mapping["batches"])
        or _sum_usage(sidecar_usages) != report["usage"]
    ):
        raise SelectionBlocked("holdout candidate artifacts do not exactly partition the selection")
    return {
        "raw_by_segment": raw_by_segment,
        "normalized_by_segment": normalized_by_segment,
        "usage": report["usage"],
        "report_path": str(report_path),
        "report_sha256": _sha256_file(report_path),
        "mapping_path": str(mapping_path),
        "mapping_sha256": _sha256_file(mapping_path),
        "manifest_path": str(manifest_path),
        "manifest_sha256": _sha256_file(manifest_path),
        "artifacts": artifacts,
    }


def _load_baseline_outputs(
    *,
    execution_root: Path,
    baseline_phase: Mapping[str, Any],
    selection_rows: Sequence[Mapping[str, Any]],
    sources: Mapping[str, Mapping[str, Any]],
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any],
) -> Dict[str, Any]:
    phase_root = execution_root / "phase-c-baseline"
    plan = _load_json(phase_root / "plan.json", purpose="holdout baseline plan")
    if (
        plan.get("schema_version") != HOLDOUT_BASELINE_PHASE_VERSION
        or plan.get("raw_and_repaired_outputs_separate") is not True
        or plan.get("retry_count") != 0
    ):
        raise SelectionBlocked("holdout baseline plan is not the frozen raw/repaired design")
    outcomes = baseline_phase.get("outcomes")
    expected_ids = [str(item["segment_id"]) for item in selection_rows]
    if (
        not isinstance(outcomes, list)
        or len(outcomes) != len(expected_ids)
        or int(baseline_phase.get("requested_segments") or -1) != len(expected_ids)
        or int(baseline_phase.get("validated_segments") or -1) != len(expected_ids)
        or baseline_phase.get("failed_segments_intent_to_treat") != 0
    ):
        raise SelectionBlocked("holdout baseline does not exactly cover the frozen selection")
    by_id = {}
    sidecar_usages = []
    artifacts = []
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            raise SelectionBlocked("holdout baseline outcome is malformed")
        segment_id = str(outcome.get("segment_id") or "")
        if (
            segment_id not in set(expected_ids)
            or segment_id in by_id
            or outcome.get("status") != "validated"
            or outcome.get("status_ok") is not True
            or outcome.get("usage_complete") is not True
            or not _valid_usage(outcome.get("usage"))
        ):
            raise SelectionBlocked("holdout baseline outcome is failed or unmeasured")
        raw_path = _resolve_artifact(outcome.get("raw_output_path"))
        repaired_path = _resolve_artifact(outcome.get("repaired_output_path"))
        sidecar_path = _resolve_artifact(outcome.get("sidecar_path"))
        if (
            raw_path == repaired_path
            or not all(_inside(path, phase_root) for path in (raw_path, repaired_path, sidecar_path))
            or _sha256_file(raw_path) != outcome.get("raw_output_sha256")
            or _sha256_file(repaired_path) != outcome.get("repaired_output_sha256")
            or _sha256_file(sidecar_path) != outcome.get("sidecar_sha256")
        ):
            raise SelectionBlocked("holdout baseline artifact path or hash drift")
        raw = _load_json(raw_path, purpose="holdout raw baseline output")
        repaired = _load_json(repaired_path, purpose="holdout repaired baseline output")
        if (
            raw.get("segment_id") != segment_id
            or repaired.get("segment_id") != segment_id
            or raw.get("episode_id") != outcome.get("episode_id")
            or repaired.get("episode_id") != outcome.get("episode_id")
        ):
            raise SelectionBlocked("holdout baseline output identity drift")
        try:
            validate_label_output("ai_discourse_v3_1", repaired, segment_text=str(sources[segment_id]["text"]))
        except Exception as exc:
            raise SelectionBlocked("holdout repaired baseline is no longer schema-valid") from exc
        sidecar = _validate_managed_sidecar(
            sidecar_path,
            raw_output_path=raw_path,
            model="gpt-5.5",
            effort="high",
            thread_mode="new_thread",
            batch_size=1,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        if sidecar["usage"] != outcome["usage"]:
            raise SelectionBlocked("holdout baseline outcome usage differs from its sidecar")
        sidecar_usages.append(sidecar["usage"])
        by_id[segment_id] = {"raw": raw, "repaired": repaired}
        artifacts.append(
            {
                "segment_id": segment_id,
                "raw_output_sha256": _sha256_file(raw_path),
                "repaired_output_sha256": _sha256_file(repaired_path),
                "sidecar_sha256": _sha256_file(sidecar_path),
            }
        )
    if set(by_id) != set(expected_ids) or _sum_usage(sidecar_usages) != baseline_phase["usage"]:
        raise SelectionBlocked("holdout baseline outputs or accounting do not partition the selection")
    return {"by_segment": by_id, "usage": baseline_phase["usage"], "artifacts": artifacts}


def _load_context_accounting(
    conn: sqlite3.Connection,
    *,
    execution_root: Path,
    context_phase: Mapping[str, Any],
    selection_rows: Sequence[Mapping[str, Any]],
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any],
) -> Dict[str, Any]:
    outcomes = context_phase.get("outcomes")
    selected_by_episode = Counter(str(item["episode_id"]) for item in selection_rows)
    if not isinstance(outcomes, list) or set(selected_by_episode) != {
        str(item.get("episode_id")) for item in outcomes if isinstance(item, dict)
    }:
        raise SelectionBlocked("holdout context outcomes do not partition selected episodes")
    usages = []
    allocation = Fraction(0, 1)
    rows = []
    for outcome in outcomes:
        episode_id = str(outcome.get("episode_id") or "")
        if (
            outcome.get("status") != "validated"
            or outcome.get("status_ok") is not True
            or outcome.get("usage_complete") is not True
            or not _valid_usage(outcome.get("usage"))
        ):
            raise SelectionBlocked("holdout context outcome is failed or unmeasured")
        artifact_path = _resolve_artifact(outcome.get("artifact_path"))
        sidecar_path = _resolve_artifact(outcome.get("sidecar_path"))
        raw_path = sidecar_path.parent / "raw-output.json"
        if (
            not _inside(artifact_path, execution_root / "phase-a-contexts")
            or not _inside(sidecar_path, execution_root / "phase-a-contexts")
            or _sha256_file(artifact_path) != outcome.get("artifact_sha256")
            or _sha256_file(sidecar_path) != outcome.get("sidecar_sha256")
            or not raw_path.is_file()
        ):
            raise SelectionBlocked("holdout context artifact hash drift")
        sidecar = _validate_managed_sidecar(
            sidecar_path,
            raw_output_path=raw_path,
            model="gpt-5.5",
            effort="high",
            thread_mode="new_thread",
            batch_size=1,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        if sidecar["usage"] != outcome["usage"]:
            raise SelectionBlocked("holdout context outcome usage differs from its sidecar")
        transcript_ids = {
            str(item["transcript_id"])
            for item in selection_rows
            if str(item["episode_id"]) == episode_id
        }
        if len(transcript_ids) != 1:
            raise SelectionBlocked("holdout context episode has ambiguous canonical transcript")
        transcript_id = next(iter(transcript_ids))
        total_segments = int(
            conn.execute(
                "SELECT COUNT(*) FROM segments WHERE episode_id = ? AND transcript_id = ?",
                (episode_id, transcript_id),
            ).fetchone()[0]
        )
        selected_segments = int(selected_by_episode[episode_id])
        if total_segments < selected_segments or selected_segments < 1:
            raise SelectionBlocked("holdout context production amortization denominator is invalid")
        episode_allocation = Fraction(int(sidecar["usage"]["total_tokens"]) * selected_segments, total_segments)
        allocation += episode_allocation
        usages.append(sidecar["usage"])
        rows.append(
            {
                "episode_id": episode_id,
                "selected_segments": selected_segments,
                "canonical_production_segments": total_segments,
                "measured_context_total_tokens": int(sidecar["usage"]["total_tokens"]),
                "allocated_context_tokens_numerator": episode_allocation.numerator,
                "allocated_context_tokens_denominator": episode_allocation.denominator,
            }
        )
    if _sum_usage(usages) != context_phase["usage"]:
        raise SelectionBlocked("holdout context report usage differs from measured sidecars")
    return {
        "usage": context_phase["usage"],
        "production_allocated_total_tokens": allocation,
        "episodes": rows,
    }


def load_holdout_execution_inputs(
    conn: sqlite3.Connection,
    *,
    covenant_path: Path,
    selection_path: Path,
    execution_dir: Path,
) -> Dict[str, Any]:
    covenant_file, covenant = verify_holdout_model_call_authorization(covenant_path)
    before_changes = conn.total_changes
    instruction_contract = verify_instruction_contract()
    execution_lineage = verified_holdout_execution_lineage(instruction_contract)
    selection_file = Path(selection_path).expanduser().resolve()
    execution_root = Path(execution_dir).expanduser().resolve()
    verified = verify_frozen_holdout(conn, covenant_path=covenant_file)
    if (
        covenant.get("schema_version") != HOLDOUT_COVENANT_VERSION
        or covenant.get("holdout_model_calls_authorized") is not True
        or (covenant.get("winner_gates") or {}).get("calibrated_app_server_llm_judge") is not True
        or (covenant.get("winner_gates") or {}).get("quality_noninferior") is not True
        or (covenant.get("winner_gates") or {}).get(
            "production_amortized_total_token_ratio_lte_0_28"
        )
        is not True
    ):
        raise SelectionBlocked("holdout covenant does not authorize the frozen calibrated judge")
    calibration_sha = (covenant.get("winner_frozen_artifact_hashes") or {}).get(
        "calibration_report"
    )
    if not (
        isinstance(calibration_sha, str)
        and len(calibration_sha) == 64
        and all(character in "0123456789abcdef" for character in calibration_sha)
    ):
        raise SelectionBlocked("holdout covenant does not bind the frozen judge calibration")
    covenant_sha = verified["covenant_sha256"]
    reservoirs = _load_reservoir_rows(covenant_file, covenant)
    selection_verified = verify_stratified_selection(
        selection_file,
        covenant_sha256=covenant_sha,
        reservoir_segments=reservoirs,
        require_acceptance_shape=True,
    )
    selection = selection_verified["payload"]
    selection_rows = selection_verified["segments"]
    sources = _verified_sources(conn, selection_rows)
    stratifier_root = selection_file.parent
    stratifier_plan = _load_json(
        stratifier_root / "plan.json", purpose="holdout stratifier plan"
    )
    stratifier_report = _load_json(
        stratifier_root / "report.json", purpose="holdout stratifier report"
    )
    if (
        stratifier_plan.get("instruction_contract") != instruction_contract
        or stratifier_plan.get("execution_lineage") != execution_lineage
        or stratifier_report.get("instruction_contract") != instruction_contract
        or stratifier_report.get("execution_lineage") != execution_lineage
        or stratifier_report.get("selection_sha256") != _sha256_file(selection_file)
    ):
        raise SelectionBlocked("holdout stratifier terminal lineage drift")
    stratifier_sidecars = sorted(stratifier_root.glob("extractions/*/sidecar.json")) + sorted(
        stratifier_root.glob("support/packet-*/*/sidecar.json")
    )
    expected_stratifier_sidecars = int(
        stratifier_report.get("requested_extraction_calls") or 0
    ) + int(stratifier_report.get("requested_support_calls") or 0)
    if len(stratifier_sidecars) != expected_stratifier_sidecars:
        raise SelectionBlocked("holdout stratifier sidecar lineage evidence is incomplete")
    stratifier_bindings = stratifier_report.get("leaf_bindings")
    if (
        not isinstance(stratifier_bindings, list)
        or len(stratifier_bindings) != expected_stratifier_sidecars
    ):
        raise SelectionBlocked("holdout stratifier leaf binding evidence is incomplete")
    try:
        validated_stratifier_bindings = [
            validate_holdout_leaf_binding(
                binding,
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
            for binding in stratifier_bindings
        ]
    except ValueError as exc:
        raise SelectionBlocked(str(exc)) from exc
    if {
        item["artifacts"]["sidecar"]["path"]
        for item in validated_stratifier_bindings
    } != {str(path) for path in stratifier_sidecars}:
        raise SelectionBlocked("holdout stratifier leaf sidecar partition drift")
    for sidecar_path in stratifier_sidecars:
        sidecar = _load_json(sidecar_path, purpose="holdout stratifier sidecar")
        try:
            validate_managed_sidecar_execution_lineage(
                sidecar=sidecar,
                instruction_contract=instruction_contract,
                execution_lineage=execution_lineage,
            )
        except ValueError as exc:
            raise SelectionBlocked(str(exc)) from exc

    plan_path = execution_root / "execution-plan.json"
    plan = _load_json(plan_path, purpose="holdout execution plan")
    if (
        plan.get("schema_version") != HOLDOUT_EXECUTION_PLAN_VERSION
        or plan.get("status") != "frozen_no_production_promotion"
        or plan.get("covenant_sha256") != covenant_sha
        or plan.get("stratified_selection_sha256") != _sha256_file(selection_file)
        or plan.get("winner") != covenant.get("winner")
        or plan.get("segment_ids") != [str(item["segment_id"]) for item in selection_rows]
        or plan.get("segment_text_sha256s") != [str(item["text_sha256"]) for item in selection_rows]
        or plan.get("instruction_contract") != instruction_contract
        or plan.get("execution_lineage") != execution_lineage
    ):
        raise SelectionBlocked("holdout execution plan does not bind the frozen inputs")

    context_path, context = _phase_report(
        execution_root,
        "phase-a-contexts/report.json",
        schema_version=HOLDOUT_CONTEXT_PHASE_VERSION,
        purpose="holdout context phase",
    )
    candidate_path, candidate = _phase_report(
        execution_root,
        "phase-b-candidate/report.json",
        schema_version=HOLDOUT_CANDIDATE_PHASE_VERSION,
        purpose="holdout candidate phase",
    )
    baseline_path, baseline = _phase_report(
        execution_root,
        "phase-c-baseline/report.json",
        schema_version=HOLDOUT_BASELINE_PHASE_VERSION,
        purpose="holdout baseline phase",
    )
    for phase_name, phase in (
        ("context", context),
        ("candidate", candidate),
        ("baseline", baseline),
    ):
        if (
            phase.get("instruction_contract") != instruction_contract
            or phase.get("execution_lineage") != execution_lineage
        ):
            raise SelectionBlocked(
                "holdout %s phase execution lineage drift" % phase_name
            )
        bindings = phase.get("leaf_bindings")
        expected_leaf = {
            "context": ("gpt-5.5", "high", "requested_episodes"),
            "candidate": (
                expanded_cap.MODEL,
                expanded_cap.EFFORT,
                "requested_calls",
            ),
            "baseline": ("gpt-5.5", "high", "requested_segments"),
        }[phase_name]
        if not isinstance(bindings, list):
            raise SelectionBlocked("holdout %s leaf bindings are missing" % phase_name)
        try:
            validated_bindings = [
                validate_holdout_leaf_binding(
                    binding,
                    instruction_contract=instruction_contract,
                    execution_lineage=execution_lineage,
                    expected_model=expected_leaf[0],
                    expected_effort=expected_leaf[1],
                )
                for binding in bindings
            ]
        except ValueError as exc:
            raise SelectionBlocked(str(exc)) from exc
        if (
            phase.get("ok") is True
            and len(validated_bindings) != int(phase.get(expected_leaf[2]) or 0)
        ):
            raise SelectionBlocked(
                "holdout %s leaf binding count mismatch" % phase_name
            )
        if len({item["turn_id"] for item in validated_bindings}) != len(
            validated_bindings
        ):
            raise SelectionBlocked("holdout %s leaf turn identity duplicated" % phase_name)
        if phase_name in {"context", "baseline"}:
            expected_sidecars = {
                str(Path(str(outcome["sidecar_path"])).expanduser().resolve())
                for outcome in phase.get("outcomes") or []
                if isinstance(outcome, Mapping)
                and outcome.get("status") == "validated"
            }
            observed_sidecars = {
                item["artifacts"]["sidecar"]["path"]
                for item in validated_bindings
            }
            if observed_sidecars != expected_sidecars:
                raise SelectionBlocked(
                    "holdout %s leaf sidecar partition drift" % phase_name
                )
    references, reference_artifacts = _load_reference_cases(
        selection_path=selection_file,
        selection=selection,
        selection_rows=selection_rows,
        sources=sources,
    )
    candidate_outputs = _load_candidate_outputs(
        conn=conn,
        execution_root=execution_root,
        candidate_phase=candidate,
        context_phase=context,
        covenant={**covenant, "covenant_sha256": covenant_sha},
        selection_rows=selection_rows,
        sources=sources,
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    baseline_outputs = _load_baseline_outputs(
        execution_root=execution_root,
        baseline_phase=baseline,
        selection_rows=selection_rows,
        sources=sources,
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    context_accounting = _load_context_accounting(
        conn,
        execution_root=execution_root,
        context_phase=context,
        selection_rows=selection_rows,
        instruction_contract=instruction_contract,
        execution_lineage=execution_lineage,
    )
    if conn.total_changes != before_changes:
        raise RuntimeError("holdout judge preflight mutated the production database")
    return {
        "covenant": covenant,
        "covenant_path": str(covenant_file),
        "covenant_sha256": covenant_sha,
        "selection": selection,
        "selection_path": str(selection_file),
        "selection_sha256": _sha256_file(selection_file),
        "selection_rows": selection_rows,
        "sources": sources,
        "references": references,
        "reference_artifacts": reference_artifacts,
        "candidate": candidate_outputs,
        "baseline": baseline_outputs,
        "context": context_accounting,
        "execution_plan_path": str(plan_path),
        "execution_plan_sha256": _sha256_file(plan_path),
        "phase_reports": {
            "context": {"path": str(context_path), "sha256": _sha256_file(context_path)},
            "candidate": {"path": str(candidate_path), "sha256": _sha256_file(candidate_path)},
            "baseline": {"path": str(baseline_path), "sha256": _sha256_file(baseline_path)},
        },
        "calibration_report_sha256": calibration_sha,
        "instruction_contract": instruction_contract,
        "execution_lineage": execution_lineage,
    }


def _extract_events(payload: Mapping[str, Any], *, key: str) -> List[Dict[str, Any]]:
    events = payload.get(key)
    if events is None:
        events = []
    if not isinstance(events, list) or any(not isinstance(item, dict) for item in events):
        raise SelectionBlocked("%s is not a valid event array" % key)
    return [deepcopy(item) for item in events]


def assemble_holdout_shared_witness_pool(
    *,
    inputs: Mapping[str, Any],
    output_dir: Path,
) -> Dict[str, Any]:
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    systems = {
        REFERENCE_SYSTEM: {"kind": "preexecution_llm_reference", "representation": "supported_events"},
        BASELINE_RAW_SYSTEM: {"kind": "baseline", "representation": "raw"},
        BASELINE_REPAIRED_SYSTEM: {"kind": "baseline", "representation": "repaired"},
        CANDIDATE_RAW_SYSTEM: {"kind": "frozen_winner", "representation": "raw"},
        CANDIDATE_WINNER_SYSTEM: {"kind": "frozen_winner", "representation": "normalized", "selection_target": True},
    }
    system_cases = {system_id: {} for system_id in systems}
    raw_cases = []
    exact_dedup_removed = 0
    submitted_nonexact = 0
    for selected in inputs["selection_rows"]:
        segment_id = str(selected["segment_id"])
        source_excerpt = str(inputs["sources"][segment_id]["text"])
        side_entries = {"a": {}, "b": {}}  # type: Dict[str, Dict[str, Dict[str, Any]]]

        def add_payload(
            system_id: str,
            side: str,
            payload: Mapping[str, Any],
            key: str,
            provenance: Mapping[str, Any],
            require_exact: bool,
        ) -> None:
            nonlocal exact_dedup_removed, submitted_nonexact
            events = _extract_events(payload, key=key)
            hashes = []
            exact_count = 0
            for event_index, event in enumerate(events):
                canonical_hash = _event_hash(event)
                canonical = _canonical_json(event)
                evidence = event.get("evidence")
                exact = isinstance(evidence, str) and bool(evidence) and evidence in source_excerpt
                if require_exact and not exact:
                    raise SelectionBlocked("scored normalized/reference event evidence is not exact")
                submitted_nonexact += int(not exact)
                exact_count += int(exact)
                hashes.append(canonical_hash)
                membership = {
                    "system_id": system_id,
                    "event_index": event_index,
                    "submitted_evidence_exact": exact,
                    **dict(provenance),
                }
                prior = side_entries[side].get(canonical_hash)
                if prior is None:
                    side_entries[side][canonical_hash] = {
                        "event": event,
                        "canonical_json": canonical,
                        "memberships": [membership],
                    }
                else:
                    if prior["canonical_json"] != canonical:
                        raise SelectionBlocked("canonical event hash collision")
                    prior["memberships"].append(membership)
                    exact_dedup_removed += 1
            system_cases[system_id][segment_id] = {
                "status": "coded" if events else "no_signal",
                "event_hashes": sorted(set(hashes)),
                "submitted_event_count": len(events),
                "exact_evidence_event_count": exact_count,
            }

        reference_case = inputs["references"][segment_id]
        add_payload(
            REFERENCE_SYSTEM,
            "a",
            {"events": reference_case["supported_events"]},
            "events",
            {"reference_case_sha256": selected["reference_case_sha256"]},
            True,
        )
        baseline = inputs["baseline"]["by_segment"][segment_id]
        add_payload(BASELINE_RAW_SYSTEM, "a", baseline["raw"], "discourse_events", {}, False)
        add_payload(BASELINE_REPAIRED_SYSTEM, "a", baseline["repaired"], "discourse_events", {}, True)
        add_payload(
            CANDIDATE_RAW_SYSTEM,
            "b",
            inputs["candidate"]["raw_by_segment"][segment_id],
            "events",
            {},
            False,
        )
        add_payload(
            CANDIDATE_WINNER_SYSTEM,
            "b",
            inputs["candidate"]["normalized_by_segment"][segment_id],
            "events",
            {},
            True,
        )
        for canonical_hash in sorted(set(side_entries["a"]) & set(side_entries["b"])):
            left = side_entries["a"][canonical_hash]
            right = side_entries["b"].pop(canonical_hash)
            if left["canonical_json"] != right["canonical_json"]:
                raise SelectionBlocked("canonical event hash collision across judge sides")
            left["memberships"].extend(right["memberships"])
            exact_dedup_removed += 1
        rendered = {}
        for side in ("a", "b"):
            rendered[side] = [
                {
                    "event": item["event"],
                    "provenance": {
                        "canonical_event_sha256": canonical_hash,
                        "memberships": sorted(
                            item["memberships"],
                            key=lambda row: (str(row["system_id"]), int(row["event_index"])),
                        ),
                        "structural_sentinel": False,
                    },
                }
                for canonical_hash, item in sorted(side_entries[side].items())
            ]
        raw_cases.append(
            {
                "case_key": segment_id,
                "source_excerpt": source_excerpt,
                "event_set_a": rendered["a"],
                "event_set_b": rendered["b"],
                "provenance": {
                    "segment_id": segment_id,
                    "episode_id": selected["episode_id"],
                    "source_id": selected["source_id"],
                    "evaluation_set": selected["evaluation_set"],
                    "density_stratum": selected["reference_stratum"],
                    "text_sha256": selected["text_sha256"],
                },
            }
        )
    pool, private_mapping = make_shared_witness_pool(
        raw_cases, seed="app-server-holdout-shared-witness-pool-v1"
    )
    pool_path = root / "shared-witness-pool.private.json"
    mapping_path = root / "private-mapping.json"
    membership_path = root / "membership-index.private.json"
    write_immutable_json(pool_path, pool)
    write_immutable_json(mapping_path, private_mapping)
    membership = {
        "schema_version": DEV_MEMBERSHIP_VERSION,
        "holdout_membership_version": HOLDOUT_MEMBERSHIP_VERSION,
        "manifest_path": inputs["candidate"]["manifest_path"],
        "manifest_sha256": inputs["candidate"]["manifest_sha256"],
        "systems": systems,
        "system_cases": system_cases,
        "case_order": [item["case_id"] for item in pool["cases"]],
        "segment_order": [str(item["segment_id"]) for item in inputs["selection_rows"]],
        "privacy": "private_ids_memberships_and_local_hashes_no_source_or_event_text",
    }
    write_immutable_json(membership_path, membership)
    report = {
        "schema_version": HOLDOUT_POOL_ASSEMBLY_VERSION,
        "covenant_sha256": inputs["covenant_sha256"],
        "selection_sha256": inputs["selection_sha256"],
        "segment_count": len(raw_cases),
        "paired_quality_cases": sum(item["evaluation_set"] == "paired_quality" for item in inputs["selection_rows"]),
        "clean_no_signal_cases": sum(item["evaluation_set"] == "clean_no_signal_power" for item in inputs["selection_rows"]),
        "systems": systems,
        "pool_path": str(pool_path),
        "pool_sha256": _sha256_file(pool_path),
        "private_mapping_path": str(mapping_path),
        "private_mapping_sha256": _sha256_file(mapping_path),
        "membership_index_path": str(membership_path),
        "membership_index_sha256": _sha256_file(membership_path),
        "exact_canonical_duplicates_removed": exact_dedup_removed,
        "submitted_nonexact_raw_events_preserved_for_judgment": submitted_nonexact,
        "deduplication": "exact_canonical_event_json_within_segment_across_all_systems",
        "semantic_pruning": False,
        "production_database_mutation": False,
        "production_promotion": False,
    }
    write_immutable_json(root / "assembly-report.json", report)
    return {
        "pool": pool,
        "private_mapping": private_mapping,
        "membership_index": membership,
        "report": report,
    }


async def run_holdout_judge_shards(
    *,
    pool: Mapping[str, Any],
    output_dir: Path,
    model: str,
    reasoning_effort: str,
    timeout_seconds: float,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
    judge_runner: Callable[..., Awaitable[Dict[str, Any]]] = run_app_server_semantic_judge,
    instruction_contract: Mapping[str, Any] | None = None,
    execution_lineage: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    if (instruction_contract is None) != (execution_lineage is None):
        raise ValueError("holdout judge instruction contract and lineage are inseparable")
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    # No-signal cases can share a packet; witness and byte caps keep dense cases
    # small.  This is materially faster than a fixed two-case shard while
    # preserving the same AB/BA schema and exact case order.
    shards = plan_judge_shards(
        pool,
        max_cases=8,
        max_witnesses=160,
        max_canonical_bytes=400000,
    )
    reports = []
    consensuses = []

    class BorrowedClientContext:
        def __init__(self, acquire: Callable[[], Awaitable[Any]]):
            self.acquire = acquire

        async def __aenter__(self) -> CodexAppServerClient:
            return await self.acquire()

        async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
            return False

    # One lazily entered app-server process serves every shard.  A terminal or
    # adopted shard validates its frozen spec and sidecars before the borrowed
    # context is entered, so tampered lineage cannot start a process.
    async with AsyncExitStack() as stack:
        persistent_client: Any | None = None

        async def acquire_persistent_client() -> Any:
            nonlocal persistent_client
            if persistent_client is None:
                persistent_client = await stack.enter_async_context(client_factory())
            return persistent_client

        borrowed_factory = lambda: BorrowedClientContext(acquire_persistent_client)
        for index, shard in enumerate(shards):
            shard_root = root / ("shard-%03d" % index)
            pool_path = shard_root / "pool.private.json"
            write_immutable_json(pool_path, shard)
            runner_kwargs: Dict[str, Any] = {
                "pool_path": pool_path,
                "output_dir": shard_root / "judge",
                "model": model,
                "reasoning_effort": reasoning_effort,
                "timeout_seconds": timeout_seconds,
                "client_factory": borrowed_factory,
            }
            if instruction_contract is not None and execution_lineage is not None:
                runner_kwargs["instruction_contract"] = instruction_contract
                runner_kwargs["execution_lineage"] = execution_lineage
            report = await judge_runner(
                **runner_kwargs
            )
            if report.get("accounting_complete") is not True or not _valid_usage(report.get("usage")):
                raise SelectionBlocked("holdout semantic judge shard has incomplete accounting")
            if instruction_contract is not None and (
                report.get("instruction_contract") != instruction_contract
                or report.get("execution_lineage") != execution_lineage
                or report.get("turn_semantic_outputs") != ["support", "alignment"]
                or report.get("deterministic_consensus_additional_model_calls") != 0
            ):
                raise SelectionBlocked("holdout semantic judge shard execution lineage drift")
            if instruction_contract is not None and execution_lineage is not None:
                bindings = report.get("leaf_bindings")
                if not isinstance(bindings, list) or len(bindings) != 2:
                    raise SelectionBlocked(
                        "holdout semantic judge shard leaf bindings are incomplete"
                    )
                try:
                    validated_bindings = [
                        validate_holdout_leaf_binding(
                            binding,
                            instruction_contract=instruction_contract,
                            execution_lineage=execution_lineage,
                            expected_model=model,
                            expected_effort=reasoning_effort,
                        )
                        for binding in bindings
                    ]
                except ValueError as exc:
                    raise SelectionBlocked(str(exc)) from exc
                expected_sidecars = [
                    str((shard_root / "judge" / "sidecars" / name).resolve())
                    for name in ("ab.json", "ba.json")
                ]
                if (
                    [
                        binding["artifacts"]["sidecar"]["path"]
                        for binding in validated_bindings
                    ]
                    != expected_sidecars
                    or len({binding["turn_id"] for binding in validated_bindings})
                    != 2
                ):
                    raise SelectionBlocked(
                        "holdout semantic judge shard leaf identity drift"
                    )
            consensus_path = shard_root / "judge" / "consensus.private.json"
            consensus = _load_json(consensus_path, purpose="holdout judge shard consensus")
            reports.append(
                {
                    "shard_index": index,
                    "case_count": len(shard["cases"]),
                    "pool_sha256": _sha256_file(pool_path),
                    "report_path": str(shard_root / "judge" / "report.json"),
                    "report_sha256": _sha256_file(shard_root / "judge" / "report.json"),
                    "leaf_bindings_sha256": sha256_text(
                        _canonical_json(report.get("leaf_bindings"))
                    ),
                    "consensus_path": str(consensus_path),
                    "consensus_sha256": _sha256_file(consensus_path),
                    "usage": report["usage"],
                }
            )
            consensuses.append(consensus)
    merged = merge_judge_consensuses(master_pool=pool, shard_consensuses=consensuses)
    consensus_path = root / "consensus.private.json"
    write_immutable_json(consensus_path, merged)
    report = {
        "schema_version": HOLDOUT_FULL_JUDGE_VERSION,
        "state": "completed",
        "model": model,
        "reasoning_effort": reasoning_effort,
        "transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
        "retry_count": 0,
        "shard_count": len(shards),
        "case_count": len(pool["cases"]),
        "accounting_complete": True,
        "usage": _sum_usage(item["usage"] for item in reports),
        "abstentions": merged["abstentions"],
        "instruction_contract": (
            dict(instruction_contract) if instruction_contract is not None else None
        ),
        "execution_lineage": (
            dict(execution_lineage) if execution_lineage is not None else None
        ),
        "turn_semantic_outputs": ["support", "alignment"],
        "deterministic_consensus_additional_model_calls": 0,
        "consensus_path": str(consensus_path),
        "consensus_sha256": _sha256_file(consensus_path),
        "shards": reports,
        "production_database_mutation": False,
        "production_promotion": False,
    }
    write_immutable_json(root / "report.json", report)
    return report


def _one_sided_binomial_upper(k: int, n: int, confidence: float = 0.95) -> float:
    """Exact Clopper-Pearson one-sided upper confidence limit."""

    if n < 1 or k < 0 or k > n or not 0 < confidence < 1:
        raise ValueError("invalid binomial confidence inputs")
    if k == n:
        return 1.0
    alpha = 1.0 - confidence

    def cdf(probability: float) -> float:
        return sum(
            math.comb(n, i)
            * (probability ** i)
            * ((1.0 - probability) ** (n - i))
            for i in range(k + 1)
        )

    low, high = 0.0, 1.0
    for _iteration in range(100):
        middle = (low + high) / 2.0
        if cdf(middle) > alpha:
            low = middle
        else:
            high = middle
    return high


def evaluate_holdout_quality_gates(
    *,
    score: Mapping[str, Any],
    membership_index: Mapping[str, Any],
    selection_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    baseline_rows = {
        item["segment_id"]: item for item in score["systems"][BASELINE_REPAIRED_SYSTEM]["cases"]
    }
    candidate_rows = {
        item["segment_id"]: item for item in score["systems"][CANDIDATE_WINNER_SYSTEM]["cases"]
    }
    selected_by_id = {str(item["segment_id"]): item for item in selection_rows}
    paired_ids = [
        str(item["segment_id"])
        for item in selection_rows
        if item["evaluation_set"] == "paired_quality"
    ]
    clean_ids = [
        str(item["segment_id"])
        for item in selection_rows
        if item["evaluation_set"] == "clean_no_signal_power"
    ]
    if len(paired_ids) != 60 or len(clean_ids) != 60:
        raise SelectionBlocked("holdout quality gate requires frozen 60 paired plus 60 clean cases")
    paired = [
        {
            "segment_id": segment_id,
            "source_id": selected_by_id[segment_id]["source_id"],
            "density_stratum": selected_by_id[segment_id]["reference_stratum"],
            "baseline_f1": baseline_rows[segment_id]["f1"],
            "candidate_f1": candidate_rows[segment_id]["f1"],
        }
        for segment_id in paired_ids
    ]
    bootstrap = source_cluster_paired_bootstrap(
        paired,
        iterations=HOLDOUT_GATES["bootstrap_iterations"],
        confidence=HOLDOUT_GATES["bootstrap_confidence"],
        seed=HOLDOUT_GATES["bootstrap_seed"],
    )
    by_source = {}
    for source_id in sorted({str(item["source_id"]) for item in paired}):
        values = [item for item in paired if str(item["source_id"]) == source_id]
        baseline = sum(float(item["baseline_f1"]) for item in values) / len(values)
        candidate = sum(float(item["candidate_f1"]) for item in values) / len(values)
        by_source[source_id] = {
            "segments": len(values),
            "baseline_f1": round(baseline, 6),
            "candidate_f1": round(candidate, 6),
            "candidate_minus_baseline": round(candidate - baseline, 6),
        }
    macro_delta = sum(item["candidate_minus_baseline"] for item in by_source.values()) / len(by_source)
    worst_source_delta = min(item["candidate_minus_baseline"] for item in by_source.values())
    by_stratum = {}
    stratum_deltas = {}
    source_by_stratum = {}
    for stratum in ("no_signal", "low", "medium", "dense"):
        values = [item for item in paired if item["density_stratum"] == stratum]
        if len(values) != 15:
            raise SelectionBlocked("holdout paired density strata are not exactly 15 cases each")
        baseline = sum(float(item["baseline_f1"]) for item in values) / len(values)
        candidate = sum(float(item["candidate_f1"]) for item in values) / len(values)
        stratum_deltas[stratum] = candidate - baseline
        by_stratum[stratum] = {
            "segments": len(values),
            "baseline_f1": round(baseline, 6),
            "candidate_f1": round(candidate, 6),
            "candidate_minus_baseline": round(candidate - baseline, 6),
        }
    for source_id in sorted({str(item["source_id"]) for item in paired}):
        source_by_stratum[source_id] = {}
        for stratum in ("no_signal", "low", "medium", "dense"):
            values = [
                item
                for item in paired
                if str(item["source_id"]) == source_id
                and item["density_stratum"] == stratum
            ]
            if not values:
                continue
            baseline = sum(float(item["baseline_f1"]) for item in values) / len(values)
            candidate = sum(float(item["candidate_f1"]) for item in values) / len(values)
            source_by_stratum[source_id][stratum] = {
                "segments": len(values),
                "baseline_f1": round(baseline, 6),
                "candidate_f1": round(candidate, 6),
                "candidate_minus_baseline": round(candidate - baseline, 6),
            }
    abstained_case_ids = set(score["abstained_cases"])
    candidate_case_memberships = membership_index["system_cases"][CANDIDATE_WINNER_SYSTEM]
    false_positive_ids = [
        segment_id
        for segment_id in clean_ids
        if int(candidate_case_memberships[segment_id]["submitted_event_count"]) > 0
        or bool(candidate_rows[segment_id]["case_abstained_worst_case"])
    ]
    false_positive_count = len(false_positive_ids)
    false_positive_rate = false_positive_count / len(clean_ids)
    upper = _one_sided_binomial_upper(
        false_positive_count,
        len(clean_ids),
        confidence=HOLDOUT_GATES["no_signal_one_sided_confidence"],
    )
    nonexact = sum(
        int(candidate_case_memberships[segment_id]["submitted_event_count"])
        - int(candidate_case_memberships[segment_id]["exact_evidence_event_count"])
        for segment_id in candidate_case_memberships
    )
    checks = {
        "paired_bootstrap_noninferiority": bootstrap["ci_lower"]
        >= -HOLDOUT_GATES["max_paired_f1_drop"],
        "macro_source_noninferiority": macro_delta
        >= -HOLDOUT_GATES["max_macro_source_f1_drop"],
        "worst_source_noninferiority": worst_source_delta
        >= -HOLDOUT_GATES["max_worst_source_f1_drop"],
        "clean_no_signal_point_rate": false_positive_rate
        <= HOLDOUT_GATES["max_no_signal_false_positive_rate"],
        "clean_no_signal_exact_upper_bound": upper
        <= HOLDOUT_GATES["max_no_signal_false_positive_rate"],
        "abstention_rate": float(score["abstained_case_rate"])
        <= HOLDOUT_GATES["max_abstained_case_rate"],
        "winner_normalized_exact_evidence": nonexact == 0,
    }
    for stratum, threshold in HOLDOUT_GATES["max_stratum_f1_drop"].items():
        checks["%s_stratum_noninferiority" % stratum] = (
            stratum_deltas[stratum] >= -float(threshold)
        )
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "paired_bootstrap": bootstrap,
        "macro_source_delta": round(macro_delta, 6),
        "worst_source_delta": round(worst_source_delta, 6),
        "by_source": by_source,
        "by_stratum": by_stratum,
        "source_by_stratum": source_by_stratum,
        "clean_no_signal": {
            "cases": len(clean_ids),
            "false_positive_or_abstained_worst_case": false_positive_count,
            "false_positive_rate": round(false_positive_rate, 6),
            "one_sided_confidence": HOLDOUT_GATES["no_signal_one_sided_confidence"],
            "clopper_pearson_upper": round(upper, 6),
            "policy": "any_winner_event_or_judge_abstention_is_a_false_positive",
        },
        "abstained_case_count": len(abstained_case_ids),
        "abstained_case_rate": score["abstained_case_rate"],
        "winner_normalized_nonexact_evidence_events": nonexact,
    }


def production_amortized_holdout_cost(
    *,
    candidate_usage: Mapping[str, Any],
    baseline_usage: Mapping[str, Any],
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    if not _valid_usage(candidate_usage) or not _valid_usage(baseline_usage):
        raise SelectionBlocked("holdout extraction accounting is incomplete")
    # Phase A is measured in full, then allocated exactly per episode by the
    # selected/canonical production segment fraction.  The same production-
    # amortized Fraction is included in both systems.  Evaluation-only
    # reference and judge usage is excluded.
    allocated = context.get("production_allocated_total_tokens")
    if not isinstance(allocated, Fraction) or allocated < 0:
        raise SelectionBlocked("holdout context production allocation is incomplete")
    context_total = int(context["usage"]["total_tokens"])
    candidate_end = Fraction(int(candidate_usage["total_tokens"]), 1) + allocated
    baseline_end = Fraction(int(baseline_usage["total_tokens"]), 1) + allocated
    if baseline_end <= 0:
        raise SelectionBlocked("holdout baseline end-to-end token denominator is zero")
    ratio = candidate_end / baseline_end
    threshold = Fraction(7, 25)
    return {
        "candidate_extraction_total_tokens": int(candidate_usage["total_tokens"]),
        "baseline_extraction_total_tokens": int(baseline_usage["total_tokens"]),
        "measured_context_total_tokens": int(context["usage"]["total_tokens"]),
        "production_allocated_context_tokens_numerator": allocated.numerator,
        "production_allocated_context_tokens_denominator": allocated.denominator,
        "candidate_end_to_end_tokens_numerator": candidate_end.numerator,
        "candidate_end_to_end_tokens_denominator": candidate_end.denominator,
        "baseline_end_to_end_tokens_numerator": baseline_end.numerator,
        "baseline_end_to_end_tokens_denominator": baseline_end.denominator,
        "production_amortized_total_token_ratio_numerator": ratio.numerator,
        "production_amortized_total_token_ratio_denominator": ratio.denominator,
        "production_amortized_total_token_ratio": round(float(ratio), 9),
        "threshold": 0.28,
        "passed_lte_0_28": ratio <= threshold,
        "allocation_policy": "each_measured_episode_context_allocated_by_selected_over_canonical_production_segments_then_included_symmetrically",
        "episode_allocations": context["episodes"],
    }


def _blocked_gate(
    *,
    reasons: Sequence[str],
    instruction_contract: Mapping[str, Any],
    execution_lineage: Mapping[str, Any],
    plan_sha256: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "schema_version": HOLDOUT_GATE_VERSION,
        "terminal_status": "blocked",
        "gate_passed": False,
        "quality_noninferior": False,
        "production_amortized_total_token_ratio_lte_0_28": False,
        "fail_closed_reasons": list(reasons),
        "judge_plan_sha256": plan_sha256,
        "retry_count": 0,
        "automatic_retry_prohibited": True,
        "instruction_contract": dict(instruction_contract),
        "execution_lineage": dict(execution_lineage),
        "production_database_mutation": False,
        "production_promotion": False,
        "production_changed": False,
    }


async def run_app_server_holdout_judge(
    conn: sqlite3.Connection,
    *,
    covenant_path: Path,
    selection_path: Path,
    execution_dir: Path,
    output_dir: Path,
    gate_output: Optional[Path] = None,
    judge_model: str = JUDGE_MODEL,
    judge_reasoning_effort: str = JUDGE_REASONING_EFFORT,
    timeout_seconds: float = 1200.0,
    client_factory: Callable[[], Any] = CapacityGatedCodexAppServerClient,
    judge_runner: Callable[..., Awaitable[Dict[str, Any]]] = run_app_server_semantic_judge,
) -> Dict[str, Any]:
    verify_holdout_model_call_authorization(covenant_path)
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    instruction_contract = verify_instruction_contract()
    execution_lineage = verified_holdout_execution_lineage(instruction_contract)
    effective_client_factory = resolve_holdout_client_factory(
        client_factory, instruction_contract
    )
    terminal_path = Path(gate_output or (root / "holdout-gate-report.json")).expanduser().resolve()
    if not _inside(terminal_path, root):
        raise ValueError("holdout terminal gate must be inside the output directory")
    if terminal_path.exists():
        prior = _load_json(terminal_path, purpose="terminal holdout gate")
        return _validate_terminal_gate(
            terminal_path,
            prior,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
    before_changes = conn.total_changes
    plan_path = root / "judge-plan.json"
    plan_sha = None
    try:
        if judge_model != JUDGE_MODEL or judge_reasoning_effort != JUDGE_REASONING_EFFORT:
            raise SelectionBlocked("holdout judge model and effort are frozen; model shopping is prohibited")
        inputs = load_holdout_execution_inputs(
            conn,
            covenant_path=Path(covenant_path),
            selection_path=Path(selection_path),
            execution_dir=Path(execution_dir),
        )
        assembled = assemble_holdout_shared_witness_pool(inputs=inputs, output_dir=root / "witness-pool")
        plan = {
            "schema_version": HOLDOUT_JUDGE_PLAN_VERSION,
            "state": "frozen_before_holdout_judge_calls",
            "covenant_sha256": inputs["covenant_sha256"],
            "selection_sha256": inputs["selection_sha256"],
            "execution_plan_sha256": inputs["execution_plan_sha256"],
            "phase_report_sha256s": {key: value["sha256"] for key, value in inputs["phase_reports"].items()},
            "reference_index_sha256": inputs["reference_artifacts"]["index_sha256"],
            "calibration_report_sha256": inputs["calibration_report_sha256"],
            "candidate_report_sha256": inputs["candidate"]["report_sha256"],
            "candidate_mapping_sha256": inputs["candidate"]["mapping_sha256"],
            "witness_pool_sha256": assembled["report"]["pool_sha256"],
            "private_mapping_sha256": assembled["report"]["private_mapping_sha256"],
            "membership_index_sha256": assembled["report"]["membership_index_sha256"],
            "judge_model": judge_model,
            "judge_reasoning_effort": judge_reasoning_effort,
            "judge_transport": "official_codex_app_server_stdio_managed_chatgpt_auth",
            "judge_orientations": ["ab", "ba"],
            "turn_semantic_outputs": ["support", "alignment"],
            "deterministic_consensus_additional_model_calls": 0,
            "judge_abstention_allowed": True,
            "retry_count": 0,
            "automatic_retry_prohibited": True,
            "quality_gates": HOLDOUT_GATES,
            "instruction_contract": instruction_contract,
            "execution_lineage": execution_lineage,
            "production_database_mutation": False,
            "production_promotion": False,
        }
        write_immutable_json(plan_path, plan)
        plan_sha = _sha256_file(plan_path)
        full_judge = await run_holdout_judge_shards(
            pool=assembled["pool"],
            output_dir=root / "full-judge",
            model=judge_model,
            reasoning_effort=judge_reasoning_effort,
            timeout_seconds=timeout_seconds,
            client_factory=effective_client_factory,
            judge_runner=judge_runner,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        if (
            full_judge.get("instruction_contract") != instruction_contract
            or full_judge.get("execution_lineage") != execution_lineage
            or full_judge.get("turn_semantic_outputs") != ["support", "alignment"]
            or full_judge.get("deterministic_consensus_additional_model_calls") != 0
        ):
            raise SelectionBlocked("holdout full-judge execution lineage drift")
        _validate_terminal_full_judge_lineage(
            root,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
        consensus_path = root / "full-judge" / "consensus.private.json"
        consensus = _load_json(consensus_path, purpose="merged holdout semantic consensus")
        if consensus.get("schema_version") != JUDGE_CONSENSUS_VERSION:
            raise SelectionBlocked("merged holdout semantic consensus schema mismatch")
        # Reuse the proven shared-reference scorer with a holdout-specific
        # membership artifact.  It sees all five systems and one consensus.
        manifest_rows = [
            {
                **item,
                "density_stratum": item["reference_stratum"],
            }
            for item in inputs["selection_rows"]
        ]
        score = score_dev_shared_reference(
            private_mapping=assembled["private_mapping"],
            membership_index=assembled["membership_index"],
            consensus=consensus,
            manifest_rows=manifest_rows,
        )
        score_report = {
            **score,
            "schema_version": HOLDOUT_SCORE_VERSION,
            "evaluation_scope": "60_frozen_paired_quality_plus_separate_60_clean_no_signal_power",
            "baseline_raw_system_id": BASELINE_RAW_SYSTEM,
            "baseline_repaired_system_id": BASELINE_REPAIRED_SYSTEM,
            "winner_system_id": CANDIDATE_WINNER_SYSTEM,
            "preexecution_reference_system_id": REFERENCE_SYSTEM,
            "shared_augmented_reference": True,
        }
        write_immutable_json(root / "score-report.private.json", score_report)
        quality = evaluate_holdout_quality_gates(
            score=score,
            membership_index=assembled["membership_index"],
            selection_rows=inputs["selection_rows"],
        )
        cost = production_amortized_holdout_cost(
            candidate_usage=inputs["candidate"]["usage"],
            baseline_usage=inputs["baseline"]["usage"],
            context=inputs["context"],
        )
        quality_noninferior = bool(quality["passed"] and full_judge["accounting_complete"])
        cost_passed = bool(cost["passed_lte_0_28"])
        gate_passed = bool(quality_noninferior and cost_passed)
        reasons = []
        if not quality_noninferior:
            reasons.append("holdout_semantic_quality_or_abstention_gate_failed")
        if not cost_passed:
            reasons.append("holdout_production_amortized_total_token_ratio_exceeded_0_28")
        report = {
            "schema_version": HOLDOUT_GATE_VERSION,
            "terminal_status": "passed" if gate_passed else "blocked",
            "gate_passed": gate_passed,
            "quality_noninferior": quality_noninferior,
            "production_amortized_total_token_ratio_lte_0_28": cost_passed,
            "fail_closed_reasons": reasons,
            "judge_plan_sha256": plan_sha,
            "quality": quality,
            "cost": cost,
            "judge": {
                "model": full_judge["model"],
                "reasoning_effort": full_judge["reasoning_effort"],
                "transport": full_judge["transport"],
                "accounting_complete": full_judge["accounting_complete"],
                "usage": full_judge["usage"],
                "abstentions": full_judge["abstentions"],
                "report_sha256": _sha256_file(root / "full-judge" / "report.json"),
                "consensus_sha256": _sha256_file(consensus_path),
            },
            "artifacts": {
                "witness_assembly_sha256": _sha256_file(root / "witness-pool" / "assembly-report.json"),
                "score_report_sha256": _sha256_file(root / "score-report.private.json"),
            },
            "retry_count": 0,
            "automatic_retry_prohibited": True,
            "instruction_contract": instruction_contract,
            "execution_lineage": execution_lineage,
            "production_database_mutation": False,
            "production_promotion": False,
            "production_changed": False,
        }
        report["terminal_receipt"] = _build_terminal_receipt(
            root,
            terminal_status=str(report["terminal_status"]),
            plan_sha256=plan_sha,
        )
        if conn.total_changes != before_changes:
            raise RuntimeError("holdout semantic judge mutated the production database")
        write_immutable_json(terminal_path, report)
        return _validate_terminal_gate(
            terminal_path,
            report,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )
    except asyncio.CancelledError:
        report = _blocked_gate(
            reasons=["holdout_judge_interrupted_no_retry"],
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
            plan_sha256=plan_sha,
        )
        report["terminal_receipt"] = _build_terminal_receipt(
            root, terminal_status="blocked", plan_sha256=plan_sha
        )
        if not terminal_path.exists():
            write_immutable_json(terminal_path, report)
        raise
    except Exception as exc:  # unattended work must always leave a terminal stop artifact
        reason = str(exc) if isinstance(exc, SelectionBlocked) else "unexpected_%s" % type(exc).__name__
        report = _blocked_gate(
            reasons=[reason],
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
            plan_sha256=plan_sha,
        )
        report["terminal_receipt"] = _build_terminal_receipt(
            root, terminal_status="blocked", plan_sha256=plan_sha
        )
        if not terminal_path.exists():
            write_immutable_json(terminal_path, report)
        return _validate_terminal_gate(
            terminal_path,
            report,
            instruction_contract=instruction_contract,
            execution_lineage=execution_lineage,
        )


def _read_only_connection(path: Path) -> sqlite3.Connection:
    resolved = Path(path).expanduser().resolve()
    connection = sqlite3.connect("file:%s?mode=ro" % resolved.as_posix(), uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Frozen managed-app-server holdout semantic gate")
    parser.add_argument("--covenant", required=True)
    parser.add_argument("--selection", required=True)
    parser.add_argument("--execution-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--gate-output")
    parser.add_argument("--database", default=str(db_path()))
    parser.add_argument("--judge-model", default=JUDGE_MODEL)
    parser.add_argument("--judge-reasoning-effort", default=JUDGE_REASONING_EFFORT)
    parser.add_argument("--timeout-seconds", type=float, default=1200.0)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    connection = _read_only_connection(Path(args.database))
    try:
        report = asyncio.run(
            run_app_server_holdout_judge(
                connection,
                covenant_path=Path(args.covenant),
                selection_path=Path(args.selection),
                execution_dir=Path(args.execution_dir),
                output_dir=Path(args.output_dir),
                gate_output=Path(args.gate_output) if args.gate_output else None,
                judge_model=args.judge_model,
                judge_reasoning_effort=args.judge_reasoning_effort,
                timeout_seconds=args.timeout_seconds,
            )
        )
    finally:
        connection.close()
    print(json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if report.get("gate_passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
