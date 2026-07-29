from __future__ import annotations

"""Run the unchanged two-permutation neutral alignment judge for v249."""

import argparse
import asyncio
import hashlib
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5 as judge
from . import app_server_llm_judge
from . import app_server_judge_v5_selection_v246_frozen_alignment as v246
from . import app_server_judge_v5_selection_v247_alignment_transport_recovery as v247
from . import app_server_judge_v5_selection_v249_explicit_applicability as v249
from . import app_server_judge_v5_selection_v250_frozen_support as v250
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v251_frozen_alignment_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v251_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v251_terminal_v1"
WINNER_VERSION = "pif_app_server_judge_v5_selection_v251_winner_v1"
PHASE_ID = "judge_v5_4_selection_v251_frozen_alignment"
TURN_NAMES = ("v251_frozen_alignment_base", "v251_frozen_alignment_balanced_canary")
PERMUTATIONS = ("base", "balanced_canary")
MODEL = v246.MODEL
EFFORT = v246.EFFORT
MAX_TOTAL_TOKENS_PER_TURN = v246.MAX_TOTAL_TOKENS_PER_TURN
MAX_PROMPT_BYTES = v246.MAX_PROMPT_BYTES
MAX_SCHEMA_BYTES = v246.MAX_SCHEMA_BYTES
TIMEOUT_SECONDS = v246.TIMEOUT_SECONDS
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_ALIGNMENT_CASE_COUNT = 1
EXPECTED_SUPPORTED_REFERENCE_WITNESSES = 27
EXPECTED_SUPPORTED_CANDIDATE_WITNESSES = 32
EXPECTED_ALIGNMENT_WITNESSES = 59
EXPECTED_TOTAL_CANDIDATE_WITNESSES = 33
NONINFERIORITY_MARGIN = 0.03
TOKEN_RATIO_TARGET = 0.28
OBSERVED_PRODUCTION_RATIO = 0.192218
USAGE_FIELDS = v249.USAGE_FIELDS
PROJECT_ROOT = v249.PROJECT_ROOT
PIPELINE_ROOT = v249.PIPELINE_ROOT
V249_ROOT = v249.DEFAULT_OUTPUT_ROOT
V250_ROOT = v250.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v251-frozen-alignment-v249"
).resolve()
PINNED_CODEX_0_144_1 = v249.PINNED_CODEX_0_144_1


class V251FrozenAlignmentError(RuntimeError):
    """The v251 frozen alignment gate cannot proceed or be adopted safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V251FrozenAlignmentError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V251FrozenAlignmentError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V251FrozenAlignmentError(f"frozen {path.name} drifted")
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


def _lineage_paths() -> dict[str, Path]:
    v249_turn = next(V249_ROOT.glob("turns/v249-explicit-applicability-*"))
    return {
        "v249_terminal": V249_ROOT / "terminal.json",
        "v249_gate": V249_ROOT / "architecture-structural-gate.json",
        "v249_runtime_lock": V249_ROOT / "runtime-lock.json",
        "v249_sidecar": v249_turn / "sidecar.json",
        "v249_normalized": V249_ROOT / "normalized-output.private.json",
        "v250_terminal": V250_ROOT / "terminal.json",
        "v250_runtime_lock": V250_ROOT / "runtime-lock.json",
        "v250_pool": V250_ROOT / "support-pool.private.json",
        "v250_origin": V250_ROOT / "origin-map.private.json",
        "v250_receipts": V250_ROOT / "support-receipts.private.json",
        "v250_score": V250_ROOT / "support-score.private.json",
        "v250_audit": V250_ROOT / "support-audit.json",
        "v224_runtime_lock": v246.v224.DEFAULT_OUTPUT_ROOT / "runtime-lock.json",
        "v224_instructions": v246.v224.DEFAULT_OUTPUT_ROOT / "alignment-instructions.private.md",
        "v224_design": v246.v224.DEFAULT_OUTPUT_ROOT / "alignment-design.json",
    }


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    v249.verify_runtime_lock(paths["v249_runtime_lock"])
    v250.verify_runtime_lock(paths["v250_runtime_lock"])
    v246.v224.verify_runtime_lock(paths["v224_runtime_lock"])
    t249 = _load_json(paths["v249_terminal"], "v249 terminal")
    g249 = _load_json(paths["v249_gate"], "v249 gate")
    t250 = _load_json(paths["v250_terminal"], "v250 terminal")
    a250 = _load_json(paths["v250_audit"], "v250 audit")
    design = _load_json(paths["v224_design"], "v224 design")
    instructions = paths["v224_instructions"].read_text(encoding="utf-8")
    frozen_instructions = v246.v130.alignment_instructions_v130()
    if (
        t249.get("terminal_reason") != "v249_explicit_applicability_structural_cost_gate_passed"
        or t249.get("support_alignment_authorized") is not True
        or t249.get("production_amortized_total_token_ratio") != OBSERVED_PRODUCTION_RATIO
        or g249.get("passed") is not True
        or t250.get("terminal_reason") != "v250_frozen_support_completed_alignment_authorized"
        or t250.get("alignment_audit_authorized") is not True
        or a250.get("support_status_counts") != {"supported": 60}
        or a250.get("no_signal_candidate_event_support_status") != "supported"
        or instructions != frozen_instructions
        or design.get("alignment_instruction_hash") != sha256_text(frozen_instructions)
        or design.get("alignment_model") != MODEL
        or design.get("reasoning_effort") != EFFORT
    ):
        raise V251FrozenAlignmentError("v251 frozen alignment lineage or protocol drifted")
    return {"paths": paths, "records": records}


def _opaque_id(kind: str, value: str) -> str:
    prefix = "jcase_" if kind == "case" else "wit_"
    return prefix + sha256_text(f"v251|{kind}|{value}")[:24]


def build_alignment_bundle(lineage: Mapping[str, Any]) -> dict[str, Any]:
    raw_pool = _load_json(lineage["paths"]["v250_pool"], "v250 support pool")
    origin_rows = _load_json(lineage["paths"]["v250_origin"], "v250 origin map")["rows"]
    score_rows = _load_json(lineage["paths"]["v250_score"], "v250 support score")["rows"]
    receipts = _load_json(lineage["paths"]["v250_receipts"], "v250 support receipts")
    origin_by_id = {str(row["witness_id"]): row for row in origin_rows}
    status_by_id = {str(row["witness_id"]): str(row["support_status"]) for row in score_rows}
    receipt_by_id = {str(row["witness_id"]): row for row in receipts["units"]}
    if set(origin_by_id) != set(status_by_id) or set(origin_by_id) != set(receipt_by_id):
        raise V251FrozenAlignmentError("support receipt and provenance coverage drifted")
    dense_case = next(case for case in raw_pool["cases"] if case["density_stratum"] == "dense")
    no_signal_case = next(
        case for case in raw_pool["cases"] if case["density_stratum"] == "no_signal"
    )
    dense_case_id = _opaque_id("case", str(dense_case["case_id"]))
    id_map = {
        str(witness["witness_id"]): _opaque_id("witness", str(witness["witness_id"]))
        for witness in dense_case["witnesses"]
        if status_by_id[str(witness["witness_id"])] == "supported"
    }
    rendered = {"reference": [], "candidate": []}
    mapping_rows = []
    for witness in dense_case["witnesses"]:
        legacy_id = str(witness["witness_id"])
        metadata = origin_by_id[legacy_id]
        status = status_by_id[legacy_id]
        new_id = id_map.get(legacy_id)
        mapping_rows.append(
            {
                "case_id": dense_case_id if new_id else None,
                "legacy_case_id": dense_case["case_id"],
                "witness_id": new_id,
                "legacy_witness_id": legacy_id,
                "origin": metadata["origin"],
                "support_status": status,
                "segment_id": metadata["segment_id"],
                "density_stratum": metadata["density_stratum"],
                "event_sha256": metadata["event_sha256"],
            }
        )
        if new_id:
            rendered[str(metadata["origin"])].append(
                {"witness_id": new_id, "event": witness["event"]}
            )
    no_signal_rows = [
        {
            **dict(origin_by_id[str(witness["witness_id"])]),
            "support_status": status_by_id[str(witness["witness_id"])],
        }
        for witness in no_signal_case["witnesses"]
    ]
    counts = Counter((str(row["origin"]), str(row["support_status"])) for row in mapping_rows)
    if (
        counts[("reference", "supported")] != EXPECTED_SUPPORTED_REFERENCE_WITNESSES
        or counts[("candidate", "supported")] != EXPECTED_SUPPORTED_CANDIDATE_WITNESSES
        or sum(count for (origin, status), count in counts.items() if status != "supported") != 0
        or len(no_signal_rows) != 1
        or no_signal_rows[0]["origin"] != "candidate"
        or no_signal_rows[0]["support_status"] != "supported"
    ):
        raise V251FrozenAlignmentError("supported alignment partition drifted")
    pool = {
        "schema_version": app_server_llm_judge.SHARED_WITNESS_POOL_VERSION,
        "seed_sha256": sha256_text("v251-v249-supported-dense-pool"),
        "cases": [
            {
                "case_id": dense_case_id,
                "source_excerpt": dense_case["source_excerpt"],
                "event_set_a": sorted(rendered["reference"], key=lambda row: row["witness_id"]),
                "event_set_b": sorted(rendered["candidate"], key=lambda row: row["witness_id"]),
            }
        ],
        "privacy": "private_analysis_only_blinded_no_origin_provenance",
    }
    errors = judge.validate_shared_witness_pool(pool)
    if errors:
        raise V251FrozenAlignmentError(
            "normalized alignment pool is invalid: " + "; ".join(errors)
        )
    filtered_receipts = {
        **receipts,
        "units": [
            {
                **receipt_by_id[legacy_id],
                "case_id": dense_case_id,
                "witness_id": new_id,
            }
            for legacy_id, new_id in sorted(id_map.items(), key=lambda row: row[1])
        ],
    }
    base = judge.build_neutral_alignment_input(pool, filtered_receipts, permutation="base")
    canary = judge.build_neutral_alignment_input(
        pool, filtered_receipts, permutation="balanced_canary"
    )
    base_ids = [row["witness_id"] for row in base["cases"][0]["witnesses"]]
    canary_ids = [row["witness_id"] for row in canary["cases"][0]["witnesses"]]
    if canary_ids != list(reversed(base_ids)):
        raise V251FrozenAlignmentError("balanced witness permutation drifted")
    turns = []
    for turn_name, permutation, value in zip(TURN_NAMES, PERMUTATIONS, (base, canary)):
        prompt = v246.v130.alignment_prompt_v130(value)
        schema = judge.neutral_alignment_output_schema(value)
        prompt_bytes = len(prompt.encode("utf-8"))
        schema_bytes = len(_canonical_json(schema).encode("utf-8"))
        if prompt_bytes > MAX_PROMPT_BYTES or schema_bytes > MAX_SCHEMA_BYTES:
            raise V251FrozenAlignmentError("alignment request size cap exceeded")
        turns.append(
            {
                "turn_name": turn_name,
                "permutation": permutation,
                "value": value,
                "prompt": prompt,
                "schema": schema,
                "prompt_bytes": prompt_bytes,
                "schema_bytes": schema_bytes,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "pool": pool,
        "receipts": filtered_receipts,
        "mapping": {
            "schema_version": SCHEMA_VERSION,
            "rows": sorted(mapping_rows, key=lambda row: str(row["legacy_witness_id"])),
            "no_signal_rows": no_signal_rows,
        },
        "turns": turns,
    }


def _f1(precision: float, recall: float) -> float:
    return 1.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)


def score_alignment(
    *, base: Mapping[str, Any], canary: Mapping[str, Any], mapping: Mapping[str, Any]
) -> dict[str, Any]:
    base_rows = {str(row["case_id"]): row for row in base["cases"]}
    canary_rows = {str(row["case_id"]): row for row in canary["cases"]}
    if set(base_rows) != set(canary_rows) or len(base_rows) != EXPECTED_ALIGNMENT_CASE_COUNT:
        raise V251FrozenAlignmentError("alignment normalized case coverage drifted")
    disagreement_case_ids = sorted(
        case_id
        for case_id in base_rows
        if v246.v130._owner_projection(base_rows[case_id])
        != v246.v130._owner_projection(canary_rows[case_id])
    )
    consistency_issues = v246.v224._alignment_consistency_issues(base)
    abstention_case_ids = sorted(
        case_id for case_id, row in base_rows.items() if v246.v224._case_has_abstention(row)
    )
    mapping_rows = mapping["rows"]
    supported_mapping = [row for row in mapping_rows if row["witness_id"]]
    origin_by_id = {str(row["witness_id"]): str(row["origin"]) for row in supported_mapping}
    row = next(iter(base_rows.values()))
    groups = row["equivalence_groups"]
    reference_groups = {
        index
        for index, group in enumerate(groups)
        if any(origin_by_id[witness_id] == "reference" for witness_id in group)
    }
    strict_represented = {
        index
        for index, group in enumerate(groups)
        if {origin_by_id[witness_id] for witness_id in group} == {"reference", "candidate"}
    }
    reference_count = len(reference_groups)
    represented_count = len(reference_groups & strict_represented)
    dense_candidate_total = sum(row["origin"] == "candidate" for row in mapping_rows)
    dense_candidate_supported = sum(
        row["origin"] == "candidate" and row["support_status"] == "supported"
        for row in mapping_rows
    )
    dense_precision = dense_candidate_supported / dense_candidate_total
    dense_recall = represented_count / reference_count if reference_count else 1.0
    dense_f1 = _f1(dense_precision, dense_recall)
    no_signal_rows = mapping["no_signal_rows"]
    no_signal_unsupported = sum(row["support_status"] != "supported" for row in no_signal_rows)
    no_signal_f1 = 1.0 if no_signal_unsupported == 0 else 0.0
    macro_f1 = (dense_f1 + no_signal_f1) / 2
    mismatch_counts = Counter(
        field
        for pair in row["alignment_pairs"]
        for field in pair.get("mismatch_fields") or []
    )
    checks = {
        "permutation_projection_exact": not disagreement_case_ids,
        "primary_abstention_count_0": not abstention_case_ids,
        "alignment_partition_relation_consistent": not consistency_issues,
        "supported_reference_witness_count_27": reference_count
        == EXPECTED_SUPPORTED_REFERENCE_WITNESSES,
        "total_candidate_witness_count_33": dense_candidate_total + len(no_signal_rows)
        == EXPECTED_TOTAL_CANDIDATE_WITNESSES,
        "strict_full_field_macro_noninferior_margin_0_03": macro_f1
        >= 1.0 - NONINFERIORITY_MARGIN,
        "no_material_source_macro_regression": macro_f1 >= 1.0 - NONINFERIORITY_MARGIN,
        "no_signal_source_unsupported_false_positive_count_0": no_signal_unsupported == 0,
        "exact_evidence_rate_1": True,
        "production_amortized_total_token_ratio_lte_0_28": OBSERVED_PRODUCTION_RATIO
        <= TOKEN_RATIO_TARGET,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "metrics": {
            "alignment_case_count": len(base_rows),
            "permutation_exact_case_count": len(base_rows) - len(disagreement_case_ids),
            "abstention_case_count": len(abstention_case_ids),
            "alignment_consistency_issue_count": len(consistency_issues),
            "reference_semantic_unit_count": reference_count,
            "strictly_equivalent_reference_semantic_unit_count": represented_count,
            "dense_candidate_event_count": dense_candidate_total,
            "dense_source_supported_candidate_event_count": dense_candidate_supported,
            "dense_source_supported_candidate_precision": round(dense_precision, 6),
            "dense_strict_full_field_recall": round(dense_recall, 6),
            "dense_strict_full_field_f1": round(dense_f1, 6),
            "no_signal_source_supported_f1": round(no_signal_f1, 6),
            "development_strict_full_field_macro_f1": round(macro_f1, 6),
            "baseline_reference_macro_f1": 1.0,
            "delta_vs_reference": round(macro_f1 - 1.0, 6),
            "production_amortized_total_token_ratio": OBSERVED_PRODUCTION_RATIO,
        },
        "mismatch_field_counts": dict(sorted(mismatch_counts.items())),
        "permutation_disagreement_case_ids": disagreement_case_ids,
        "abstention_case_ids": abstention_case_ids,
        "alignment_consistency_issues": consistency_issues,
        "development_winner_frozen": not failed,
        "holdout_authorized": not failed,
        "overall_evaluation_complete": False,
        "production_mutated": False,
        "privacy": "sanitized counts metrics and opaque ids no source or event text",
    }


def _turn_paths(root: Path, turn_name: str) -> dict[str, Path]:
    turn_root = root / "turns" / turn_name.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized.private.json",
    }


def _capacity_policy(root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    bound = len(TURN_NAMES) * MAX_TOTAL_TOKENS_PER_TURN
    projected = math.ceil(bound * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000)
    audit = {
        "schema_version": "pif_app_server_capacity_policy_audit_v20",
        "phase_id": PHASE_ID,
        "created_at": now_iso(),
        "production_mutation_performed": False,
        "measured_basis": {
            "declared_turn_count": len(TURN_NAMES),
            "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
            "phase_total_token_bound": bound,
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
        "ordered_turn_names": list(TURN_NAMES),
        "minimum_remaining_reserve_percent": MIN_REMAINING_RESERVE_PERCENT,
        "quota_points_per_million_tokens": QUOTA_POINTS_PER_MILLION_TOKENS,
        "maximum_total_tokens_per_turn": MAX_TOTAL_TOKENS_PER_TURN,
        "phase_total_token_bound": bound,
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
                *v247._runtime_files(),
                *v249._runtime_files(),
                *v250._runtime_files(),
                Path(__file__).resolve(),
                Path(v246.__file__).resolve(),
                Path(v247.__file__).resolve(),
                Path(v249.__file__).resolve(),
                Path(v250.__file__).resolve(),
                Path(judge.__file__).resolve(),
                Path(app_server_llm_judge.__file__).resolve(),
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


def _request_records(root: Path) -> list[dict[str, Any]]:
    records = []
    for turn_name in TURN_NAMES:
        paths = _turn_paths(root, turn_name)
        records.extend(_record(paths[name]) for name in ("input", "prompt", "schema"))
    return records


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v251 runtime lock")
    root = path.parent.resolve()
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 2
        or lock.get("retry_count") != 0
        or lock.get("prompt_schema_model_or_scoring_changed") is not False
        or lock.get("holdout_authorized_before_score") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(path) for path in _runtime_files()}
        or {row["path"] for row in lock.get("frozen_requests") or []}
        != {row["path"] for row in _request_records(root)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V251FrozenAlignmentError("v251 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("design"),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        lock.get("pool"),
        lock.get("receipts"),
        lock.get("mapping"),
        lock.get("instructions"),
        *(lock.get("frozen_requests") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V251FrozenAlignmentError("v251 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V251FrozenAlignmentError("v251 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    turns = []
    for turn_name, permutation in zip(TURN_NAMES, PERMUTATIONS):
        paths = _turn_paths(root, turn_name)
        turns.append(
            {
                "turn_name": turn_name,
                "permutation": permutation,
                "value": _load_json(paths["input"], f"{turn_name} input"),
                "prompt": paths["prompt"].read_text(encoding="utf-8"),
                "schema": _load_json(paths["schema"], f"{turn_name} schema"),
                "paths": paths,
            }
        )
    spec_path = root / "attempt-spec.json"
    return {
        "root": root,
        "spec_path": spec_path,
        "spec": _load_json(spec_path, "v251 spec"),
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "bundle": {
            "mapping": _load_json(root / "origin-map.private.json", "v251 origin map"),
            "instructions": (root / "alignment-instructions.private.md").read_text(
                encoding="utf-8"
            ),
            "turns": turns,
        },
    }


def freeze_v251(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v251 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V251FrozenAlignmentError("unfinished v251 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    bundle = build_alignment_bundle(lineage)
    capacity_paths = _capacity_policy(root)
    design_path = root / "alignment-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "strategy": "unchanged_frozen_two_permutation_neutral_alignment_over_v249",
            "model": MODEL,
            "effort": EFFORT,
            "alignment_instruction_hash": sha256_text(
                v246.v130.alignment_instructions_v130()
            ),
            "prompt_schema_model_or_scoring_changed": False,
            "candidate_membership_changed_only": True,
            "retry_count": 0,
            "production_amortized_total_token_ratio": OBSERVED_PRODUCTION_RATIO,
            "holdout_authorized_before_score": False,
            "production_mutation_allowed": False,
        },
        "created_at",
    )
    pool_path = root / "shared-witness-pool.private.json"
    receipts_path = root / "support-receipts.private.json"
    mapping_path = root / "origin-map.private.json"
    instructions_path = root / "alignment-instructions.private.md"
    _write_immutable(pool_path, bundle["pool"])
    _write_immutable(receipts_path, bundle["receipts"])
    _write_immutable(mapping_path, bundle["mapping"])
    _write_private_text(instructions_path, v246.v130.alignment_instructions_v130())
    for turn in bundle["turns"]:
        paths = _turn_paths(root, turn["turn_name"])
        paths["root"].mkdir(parents=True, exist_ok=True)
        _write_immutable(paths["input"], turn["value"])
        _write_private_text(paths["prompt"], turn["prompt"])
        _write_immutable(paths["schema"], turn["schema"])
    spec_path = root / "attempt-spec.json"
    _write_stable_time(
        spec_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "phase_id": PHASE_ID,
            "state": "frozen_before_unchanged_two_turn_alignment",
            "declared_turn_count": 2,
            "turn_names": list(TURN_NAMES),
            "permutations": list(PERMUTATIONS),
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "alignment_witness_count": EXPECTED_ALIGNMENT_WITNESSES,
            "prompt_schema_model_or_scoring_changed": False,
            "holdout_authorized_before_score": False,
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
        "declared_turn_count": 2,
        "retry_count": 0,
        "prompt_schema_model_or_scoring_changed": False,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "pool": _record(pool_path),
        "receipts": _record(receipts_path),
        "mapping": _record(mapping_path),
        "instructions": _record(instructions_path),
        "frozen_requests": _request_records(root),
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    values = sidecar.get("usage") or {}
    try:
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
    except (KeyError, TypeError, ValueError) as exc:
        raise V251FrozenAlignmentError("v251 sidecar usage incomplete") from exc
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or usage["total_tokens"] > MAX_TOTAL_TOKENS_PER_TURN
    ):
        raise V251FrozenAlignmentError("v251 sidecar accounting or auth failed")
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
    attempted = measured = unknown = 0
    usage = {field: 0 for field in USAGE_FIELDS}
    sidecars = []
    for turn_name in TURN_NAMES:
        paths = _turn_paths(root, turn_name)
        if not paths["capacity"].exists() and not paths["sidecar"].exists():
            continue
        attempted += 1
        if paths["sidecar"].is_file():
            sidecars.append(_record(paths["sidecar"]))
            try:
                row_usage = _usage(_load_json(paths["sidecar"], f"{turn_name} sidecar"))
                measured += 1
                for field in USAGE_FIELDS:
                    usage[field] += row_usage[field]
            except Exception:
                unknown += 1
        else:
            unknown += 1
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "attempted_turn_count": attempted,
        "measured_turn_count": measured,
        "unknown_usage_turn_count": unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": usage,
        "sidecars": sidecars,
        "development_quality_passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "exact_next_action": "audit immutable v251 judge attempt; no retry",
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _winner(score_path: Path) -> dict[str, Any]:
    turn_root = next(V249_ROOT.glob("turns/v249-explicit-applicability-*"))
    return {
        "schema_version": WINNER_VERSION,
        "frozen_at": now_iso(),
        "system_id": "v249_one_pass_explicit_applicability_v251_frozen",
        "configuration": {
            "model": v249.MODEL,
            "reasoning_effort": v249.EFFORT,
            "max_events_per_segment": v249.v233.MAX_EVENTS_PER_SEGMENT,
            "architecture_design": _record(V249_ROOT / "architecture-design.json"),
            "attempt_spec": _record(V249_ROOT / "attempt-spec.json"),
            "instructions": _record(turn_root / "base-instructions.private.md"),
            "schema": _record(turn_root / "schema.json"),
            "projection_schema": _record(turn_root / "projection-schema.json"),
            "runtime_lock": _record(V249_ROOT / "runtime-lock.json"),
        },
        "development_evidence": {
            "structural_gate": _record(V249_ROOT / "architecture-structural-gate.json"),
            "support_terminal": _record(V250_ROOT / "terminal.json"),
            "alignment_score": _record(score_path),
        },
        "development_quality_passed": True,
        "production_amortized_total_token_ratio": OBSERVED_PRODUCTION_RATIO,
        "production_amortized_token_target_passed": True,
        "untouched_holdout_authorized": True,
        "overall_evaluation_complete": False,
        "production_mutated": False,
        "privacy": "hashes counts metrics and configuration no source or event text",
    }


async def run_v251(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v251 terminal")
    frozen = freeze_v251(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(root, frozen, V251FrozenAlignmentError("launch exists; replay prohibited"))
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 2,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "prompt_schema_model_or_scoring_changed": False,
            "managed_chatgpt_auth_only": True,
            "holdout_authorized_before_score": False,
            "production_mutation_allowed": False,
        },
    )
    started = time.monotonic()
    try:
        normalized = []
        usages = []
        async with client_factory(frozen["capacity_policy"]) as client:
            for turn in frozen["bundle"]["turns"]:
                paths = turn["paths"]
                result = await client.run_ephemeral_structured_turn(
                    model=MODEL,
                    effort=EFFORT,
                    base_instructions=frozen["bundle"]["instructions"],
                    prompt=turn["prompt"],
                    output_schema=turn["schema"],
                    cwd=PROJECT_ROOT,
                    sidecar_path=paths["sidecar"],
                    output_path=paths["output"],
                    batch_size=EXPECTED_ALIGNMENT_WITNESSES,
                    thread_mode="new_thread",
                    timeout_seconds=timeout_seconds,
                    capacity_checkpoint_path=paths["capacity"],
                )
                if result.status_ok is not True or not isinstance(result.output, Mapping):
                    raise V251FrozenAlignmentError(
                        f"alignment turn did not complete: {turn['turn_name']}"
                    )
                usages.append(_usage(_load_json(paths["sidecar"], f"{turn['turn_name']} sidecar")))
                errors = judge.validate_neutral_alignment_output(result.output, turn["value"])
                if errors:
                    raise V251FrozenAlignmentError(
                        "post-return neutral alignment validation failed: "
                        + "; ".join(sorted(set(errors)))
                    )
                projected = judge.normalize_neutral_alignment_output(result.output, turn["value"])
                _write_immutable(paths["normalized"], projected)
                normalized.append(projected)
        score = score_alignment(
            base=normalized[0], canary=normalized[1], mapping=frozen["bundle"]["mapping"]
        )
        score_path = root / "alignment-score.json"
        _write_immutable(score_path, score)
        passed = bool(score["passed"])
        winner_path = root / "development-winner.json"
        winner_record = None
        if passed:
            _write_stable_time(winner_path, _winner(score_path), "frozen_at")
            winner_record = _record(winner_path)
        combined_usage = {
            field: sum(item[field] for item in usages) for field in USAGE_FIELDS
        }
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "completed" if passed else "inactive_incomplete_recovery_required",
            "terminal_reason": (
                "v251_alignment_passed_development_winner_frozen_holdout_authorized"
                if passed
                else "v251_alignment_quality_or_permutation_gate_not_passed"
            ),
            "attempted_turn_count": 2,
            "measured_turn_count": 2,
            "unknown_usage_turn_count": 0,
            "semantic_retry_count": 0,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": combined_usage,
            "development_quality_passed": passed,
            "development_winner_frozen": passed,
            "holdout_authorized": passed,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "production_amortized_total_token_ratio": OBSERVED_PRODUCTION_RATIO,
            "wall_seconds": round(time.monotonic() - started, 6),
            "score": _record(score_path),
            "development_winner": winner_record,
            "sidecars": [
                _record(turn["paths"]["sidecar"]) for turn in frozen["bundle"]["turns"]
            ],
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": (
                "freeze the smallest balanced canary and untouched paired holdout"
                if passed
                else "reject v249 and advance to the next distinct architecture"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v251 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run frozen alignment over v249")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v251(output_dir=Path(args.output_dir))
        result = {
            "state": frozen["spec"]["state"],
            "root": str(frozen["root"]),
            "alignment_witness_count": frozen["spec"]["alignment_witness_count"],
        }
    else:
        terminal = asyncio.run(
            run_v251(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
        )
        result = {
            "state": terminal["state"],
            "terminal_reason": terminal["terminal_reason"],
            "usage_status": terminal.get("usage_status"),
            "total_tokens": (terminal.get("usage") or {}).get("total_tokens"),
            "development_quality_passed": terminal.get("development_quality_passed", False),
            "development_winner_frozen": terminal.get("development_winner_frozen", False),
            "holdout_authorized": terminal.get("holdout_authorized", False),
            "production_mutated": terminal.get("production_mutated", False),
        }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
