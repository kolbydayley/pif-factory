from __future__ import annotations

"""Adjudicate only the mechanically observable v251 ownership-difference closure."""

import argparse
import asyncio
import copy
import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from . import app_server_capacity as capacity
from . import app_server_capacity_reserve as reserve
from . import app_server_judge_v5 as judge
from . import app_server_judge_v5_selection_v251_frozen_alignment as v251
from . import codex_app_server
from . import labels as labels_module
from . import util as util_module
from .app_server_capacity_reserve import ReserveCapacityGatedCodexAppServerClient
from .util import now_iso, sha256_text


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v254_local_owner_adjudication_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v254_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v254_terminal_v1"
WINNER_VERSION = "pif_app_server_judge_v5_selection_v254_winner_v1"
PHASE_ID = "judge_v5_4_selection_v254_local_owner_adjudication"
TURN_NAME = "v254_local_side_free_owner_adjudication"
MODEL = "gpt-5.5"
EFFORT = "high"
TIMEOUT_SECONDS = 900.0
MAX_TOTAL_TOKENS = 65_000
MAX_PROMPT_BYTES = 200_000
MAX_SCHEMA_BYTES = 64_000
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_LOCAL_WITNESS_COUNT = 19
USAGE_FIELDS = v251.USAGE_FIELDS
PROJECT_ROOT = v251.PROJECT_ROOT
PIPELINE_ROOT = v251.PIPELINE_ROOT
V251_ROOT = v251.DEFAULT_OUTPUT_ROOT
V252_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v252-capped-adjudication-v249"
).resolve()
V253_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v253-graph-canonicalizer"
).resolve()
V153_ROOT = (
    PIPELINE_ROOT / "judge-calibration-v5_4-v153-capped-alignment-adjudication"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v254-local-owner-adjudication-v249"
).resolve()
PINNED_CODEX_0_144_1 = v251.PINNED_CODEX_0_144_1
FROZEN_V153_INSTRUCTION_HASH = (
    "f295e1bad136c68aa6ecf1ee061a8f96d1225c641813970f792da482d686e79b"
)


class V254LocalOwnerError(RuntimeError):
    """The v254 local owner adjudication cannot proceed or be adopted safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V254LocalOwnerError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V254LocalOwnerError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V254LocalOwnerError(f"frozen {path.name} drifted")
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


def _turn_paths(root: Path) -> dict[str, Path]:
    turn_root = root / "turns" / TURN_NAME.replace("_", "-")
    return {
        "root": turn_root,
        "input": turn_root / "input.private.json",
        "packet": turn_root / "local-owner-packet.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized.private.json",
    }


def _lineage_paths() -> dict[str, Path]:
    base = V251_ROOT / "turns" / "v251-frozen-alignment-base"
    canary = V251_ROOT / "turns" / "v251-frozen-alignment-balanced-canary"
    v252_turn = V252_ROOT / "turns" / "v252-capped-side-free-adjudication"
    v253_turn = next(V253_ROOT.glob("turns/v253-graph-canonicalizer-*"))
    v153_turn = V153_ROOT / "turns" / "capped-alignment-disagreement-adjudication"
    return {
        "v251_terminal": V251_ROOT / "terminal.json",
        "v251_score": V251_ROOT / "alignment-score.json",
        "v251_runtime_lock": V251_ROOT / "runtime-lock.json",
        "v251_mapping": V251_ROOT / "origin-map.private.json",
        "v251_receipts": V251_ROOT / "support-receipts.private.json",
        "v251_base_input": base / "input.private.json",
        "v251_base_output": base / "output.private.json",
        "v251_canary_input": canary / "input.private.json",
        "v251_canary_output": canary / "output.private.json",
        "v252_terminal": V252_ROOT / "terminal.json",
        "v252_sidecar": v252_turn / "sidecar.json",
        "v252_output_not_adopted": v252_turn / "output.private.json",
        "v253_terminal": V253_ROOT / "terminal.json",
        "v253_sidecar": v253_turn / "sidecar.json",
        "v153_terminal": V153_ROOT / "terminal.json",
        "v153_protocol": V153_ROOT / "alignment-judge-protocol-v153.json",
        "v153_sidecar": v153_turn / "sidecar.json",
    }


def adjudication_instructions() -> str:
    value = v251.v246.v130.alignment_instructions_v130() + (
        " This is the sole capped side-free owner pass for four observable permutation "
        "disagreements. Anonymous candidate positions are balanced and have no vote meaning. "
        "Re-evaluate each source and every visible witness independently before considering "
        "the candidate projections. Account for every visible witness exactly once. Resolve "
        "one-to-one ownership before field checklists; for merge/split cases compare the full "
        "source-supported atomic proposition scope. Return abstain rather than guessing when "
        "a material decision is not source-resolvable."
    )
    if sha256_text(value) != FROZEN_V153_INSTRUCTION_HASH:
        raise V254LocalOwnerError("frozen v153 adjudication instructions drifted")
    return value


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    v251.verify_runtime_lock(paths["v251_runtime_lock"])
    t251 = _load_json(paths["v251_terminal"], "v251 terminal")
    q251 = _load_json(paths["v251_score"], "v251 score")
    t252 = _load_json(paths["v252_terminal"], "v252 terminal")
    s252 = _load_json(paths["v252_sidecar"], "v252 sidecar")
    t253 = _load_json(paths["v253_terminal"], "v253 terminal")
    s253 = _load_json(paths["v253_sidecar"], "v253 sidecar")
    protocol = _load_json(paths["v153_protocol"], "v153 protocol")
    t153 = _load_json(paths["v153_terminal"], "v153 terminal")
    s153 = _load_json(paths["v153_sidecar"], "v153 sidecar")
    if (
        t251.get("terminal_reason") != "v251_alignment_quality_or_permutation_gate_not_passed"
        or q251.get("failed_checks") != ["permutation_projection_exact"]
        or (q251.get("metrics") or {}).get("development_strict_full_field_macro_f1")
        != 0.980769
        or t252.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or t252.get("error_class") != "ReserveCapacityError"
        or s252.get("usage_status") != "measured"
        or s252.get("usage_complete") is not True
        or (s252.get("usage") or {}).get("total_tokens") != 98_728
        or t253.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or t253.get("error_class") != "ReserveCapacityError"
        or t253.get("error_message_sha256")
        != "a34941799b9e25bd8b4d4935d5774f08db2b114ecb2d589da9a665e3ee018d4a"
        or s253.get("state") != "failed"
        or s253.get("usage_status") != "unknown"
        or s253.get("usage_complete") is not False
        or float(s253.get("wall_elapsed_seconds") or 0) >= 10
        or protocol.get("adjudication_model") != MODEL
        or protocol.get("adjudication_effort") != EFFORT
        or protocol.get("adjudication_instructions_sha256")
        != FROZEN_V153_INSTRUCTION_HASH
        or t153.get("alignment_protocol_frozen") is not True
        or s153.get("base_instructions_sha256") != FROZEN_V153_INSTRUCTION_HASH
        or t251.get("production_mutated") is not False
        or t252.get("production_mutated") is not False
        or t253.get("production_mutated") is not False
    ):
        raise V254LocalOwnerError("v254 frozen lineage or adjudication protocol drifted")
    adjudication_instructions()
    return {"paths": paths, "records": records}


def _group_memberships(case: Mapping[str, Any]) -> dict[str, frozenset[str]]:
    result: dict[str, frozenset[str]] = {}
    for group in case.get("equivalence_groups") or []:
        members = frozenset(str(item) for item in group)
        for witness_id in members:
            result[witness_id] = members
    return result


def _pair_rows(case: Mapping[str, Any]) -> dict[tuple[str, str], Mapping[str, Any]]:
    return {
        tuple(sorted(str(item) for item in row["witness_ids"])): row
        for row in case.get("alignment_pairs") or []
    }


def observable_owner_closure(
    base_case: Mapping[str, Any], canary_case: Mapping[str, Any]
) -> set[str]:
    base_groups = _group_memberships(base_case)
    canary_groups = _group_memberships(canary_case)
    all_ids = set(base_groups) | set(canary_groups)
    changed = {
        witness_id
        for witness_id in all_ids
        if base_groups.get(witness_id) != canary_groups.get(witness_id)
    }
    base_pairs = _pair_rows(base_case)
    canary_pairs = _pair_rows(canary_case)
    for pair_id in set(base_pairs) | set(canary_pairs):
        if base_pairs.get(pair_id) != canary_pairs.get(pair_id):
            changed.update(pair_id)
    base_unpaired = set(str(item) for item in base_case.get("unpaired_witness_ids") or [])
    canary_unpaired = set(str(item) for item in canary_case.get("unpaired_witness_ids") or [])
    changed.update(base_unpaired ^ canary_unpaired)
    closure = set(changed)
    pending = list(changed)
    while pending:
        witness_id = pending.pop()
        related = set(base_groups.get(witness_id, ())) | set(
            canary_groups.get(witness_id, ())
        )
        new = related - closure
        closure.update(new)
        pending.extend(new)
    return closure


def _restrict_normalized_case(
    case: Mapping[str, Any], witness_ids: set[str]
) -> dict[str, Any]:
    return {
        "case_id": case["case_id"],
        "equivalence_groups": sorted(
            [sorted(set(group) & witness_ids) for group in case["equivalence_groups"] if set(group) & witness_ids],
            key=lambda row: tuple(row),
        ),
        "alignment_pairs": sorted(
            [copy.deepcopy(row) for row in case["alignment_pairs"] if set(row["witness_ids"]) <= witness_ids],
            key=lambda row: tuple(row["witness_ids"]),
        ),
        "unpaired_witness_ids": sorted(
            set(case["unpaired_witness_ids"]) & witness_ids
        ),
    }


def _restrict_raw_case(
    case: Mapping[str, Any], witness_ids: set[str]
) -> dict[str, Any]:
    return {
        "case_id": case["case_id"],
        "equivalence_groups": [
            {**copy.deepcopy(group), "witness_ids": sorted(set(group["witness_ids"]) & witness_ids)}
            for group in case["equivalence_groups"]
            if set(group["witness_ids"]) & witness_ids
        ],
        "alignment_pairs": [
            copy.deepcopy(row)
            for row in case["alignment_pairs"]
            if {str(row["witness_id_1"]), str(row["witness_id_2"])} <= witness_ids
        ],
        "unpaired_witness_ids": sorted(
            set(str(item) for item in case["unpaired_witness_ids"]) & witness_ids
        ),
    }


def build_local_bundle(lineage: Mapping[str, Any]) -> dict[str, Any]:
    paths = lineage["paths"]
    base_input = _load_json(paths["v251_base_input"], "v251 base input")
    base_output = _load_json(paths["v251_base_output"], "v251 base output")
    canary_input = _load_json(paths["v251_canary_input"], "v251 canary input")
    canary_output = _load_json(paths["v251_canary_output"], "v251 canary output")
    receipts = _load_json(paths["v251_receipts"], "v251 support receipts")
    base = judge.normalize_neutral_alignment_output(base_output, base_input)
    canary = judge.normalize_neutral_alignment_output(canary_output, canary_input)
    if len(base["cases"]) != 1 or len(canary["cases"]) != 1:
        raise V254LocalOwnerError("v254 full alignment case coverage drifted")
    closure = observable_owner_closure(base["cases"][0], canary["cases"][0])
    full_case = base_input["cases"][0]
    local_case = {
        **copy.deepcopy(full_case),
        "witnesses": [
            copy.deepcopy(row)
            for row in full_case["witnesses"]
            if str(row["witness_id"]) in closure
        ],
    }
    local_input = {
        **copy.deepcopy(base_input),
        "cases": [local_case],
        "permutation": "base",
        "adjudication_only": True,
        "mechanical_subset_rule": "owner_membership_pair_unpaired_difference_then_group_closure",
    }
    packet = judge.build_disagreement_adjudication_input(
        base_input=base_input,
        base_output=base_output,
        canary_input=canary_input,
        canary_output=canary_output,
        support_receipts=receipts,
    )
    packet_case = packet["cases"][0]
    packet_case["witnesses"] = copy.deepcopy(local_case["witnesses"])
    packet_case["anonymous_candidate_1"] = _restrict_normalized_case(
        base["cases"][0], closure
    )
    packet_case["anonymous_candidate_2"] = _restrict_normalized_case(
        canary["cases"][0], closure
    )
    swap = int(sha256_text(f"v254|candidate-order|{packet_case['case_id']}")[:2], 16) % 2 == 1
    if swap:
        packet_case["anonymous_candidate_1"], packet_case["anonymous_candidate_2"] = (
            packet_case["anonymous_candidate_2"],
            packet_case["anonymous_candidate_1"],
        )
    packet["mechanical_subset_rule"] = local_input["mechanical_subset_rule"]
    packet["full_witness_count"] = len(full_case["witnesses"])
    packet["local_witness_count"] = len(closure)
    packet["anonymous_candidate_order_rule"] = "opaque_case_hash_swap"
    packet["anonymous_candidate_swapped"] = swap
    prompt = judge.build_disagreement_adjudication_prompt(
        adjudication_input=packet,
        adjudication_alignment=local_input,
    )
    schema = judge.neutral_alignment_output_schema(local_input)
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    if (
        packet.get("adjudication_required") is not True
        or packet.get("call_cap") != 1
        or len(packet["cases"]) != 1
        or len(closure) != EXPECTED_LOCAL_WITNESS_COUNT
        or len(local_case["witnesses"]) != EXPECTED_LOCAL_WITNESS_COUNT
        or prompt_bytes >= 60_000
        or prompt_bytes > MAX_PROMPT_BYTES
        or schema_bytes > MAX_SCHEMA_BYTES
        or local_input.get("side_labels_present") is not False
        or local_input.get("system_identity_present") is not False
    ):
        raise V254LocalOwnerError("v254 local owner bundle drifted")
    return {
        "closure": closure,
        "packet": packet,
        "local_input": local_input,
        "base_input": base_input,
        "base_output": base_output,
        "canary_input": canary_input,
        "canary_output": canary_output,
        "base_normalized": base,
        "canary_normalized": canary,
        "mapping": _load_json(paths["v251_mapping"], "v251 mapping"),
        "prompt": prompt,
        "schema": schema,
        "prompt_bytes": prompt_bytes,
        "schema_bytes": schema_bytes,
    }


def _merge_local_owner(
    full_case: Mapping[str, Any], local_case: Mapping[str, Any], closure: set[str]
) -> dict[str, Any]:
    unaffected_groups = [
        copy.deepcopy(group)
        for group in full_case["equivalence_groups"]
        if not (set(group) & closure)
    ]
    if any(set(group) & closure and not set(group) <= closure for group in full_case["equivalence_groups"]):
        raise V254LocalOwnerError("mechanical closure omitted a base group member")
    unaffected_pairs = [
        copy.deepcopy(row)
        for row in full_case["alignment_pairs"]
        if not (set(row["witness_ids"]) & closure)
    ]
    if any(
        set(row["witness_ids"]) & closure and not set(row["witness_ids"]) <= closure
        for row in full_case["alignment_pairs"]
    ):
        raise V254LocalOwnerError("mechanical closure omitted a base pair member")
    return {
        "case_id": full_case["case_id"],
        "equivalence_groups": sorted(
            [*unaffected_groups, *copy.deepcopy(local_case["equivalence_groups"])],
            key=lambda row: tuple(row),
        ),
        "alignment_pairs": sorted(
            [*unaffected_pairs, *copy.deepcopy(local_case["alignment_pairs"])],
            key=lambda row: tuple(row["witness_ids"]),
        ),
        "unpaired_witness_ids": sorted(
            [item for item in full_case["unpaired_witness_ids"] if item not in closure]
            + list(local_case["unpaired_witness_ids"])
        ),
    }


def reconcile_and_score(
    *, bundle: Mapping[str, Any], output: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    errors = judge.validate_neutral_alignment_output(output, bundle["local_input"])
    if errors:
        raise V254LocalOwnerError(
            "v254 local adjudication output did not validate: "
            + "; ".join(sorted(set(errors)))
        )
    normalized = judge.normalize_neutral_alignment_output(output, bundle["local_input"])
    if len(normalized["cases"]) != 1:
        raise V254LocalOwnerError("v254 local output case coverage drifted")
    merged_case = _merge_local_owner(
        bundle["base_normalized"]["cases"][0],
        normalized["cases"][0],
        set(bundle["closure"]),
    )
    reconciled = {
        "schema_version": "pif_app_server_v5_reconciled_alignment_v4",
        "cases": [{"status": "adjudicated", **merged_case}],
        "observable_disagreement_case_count": 1,
        "mechanically_local_adjudication_witness_count": len(bundle["closure"]),
        "adjudication_call_count": 1,
        "adjudication_call_cap": 1,
        "majority_voting_used": False,
        "unresolved_cases_abstained": [],
    }
    score = v251.score_alignment(
        base=reconciled,
        canary=reconciled,
        mapping=bundle["mapping"],
    )
    return normalized, reconciled, score


def _capacity_policy(root: Path) -> dict[str, Path]:
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
        "ordered_turn_names": [TURN_NAME],
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
                *v251._runtime_files(),
                Path(__file__).resolve(),
                Path(v251.__file__).resolve(),
                Path(judge.__file__).resolve(),
                Path(capacity.__file__).resolve(),
                Path(reserve.__file__).resolve(),
                Path(codex_app_server.__file__).resolve(),
                Path(labels_module.__file__).resolve(),
                Path(util_module.__file__).resolve(),
                PROJECT_ROOT
                / "research_factory/app_server_judge_v5_selection_v252_capped_adjudication.py",
                PROJECT_ROOT
                / "research_factory/app_server_judge_v5_selection_v253_graph_canonicalizer.py",
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
            },
            key=str,
        )
    )


def _request_records(root: Path) -> list[dict[str, Any]]:
    paths = _turn_paths(root)
    return [_record(paths[name]) for name in ("input", "packet", "prompt", "schema")]


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v254 runtime lock")
    root = path.parent.resolve()
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("adjudication_call_cap") != 1
        or lock.get("retry_count") != 0
        or lock.get("mechanical_local_witness_count") != EXPECTED_LOCAL_WITNESS_COUNT
        or lock.get("frozen_adjudication_instruction_hash")
        != FROZEN_V153_INSTRUCTION_HASH
        or lock.get("holdout_authorized_before_score") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(item) for item in _runtime_files()}
        or {row["path"] for row in lock.get("frozen_requests") or []}
        != {row["path"] for row in _request_records(root)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V254LocalOwnerError("v254 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("failure_audit"),
        lock.get("design"),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        lock.get("instructions"),
        *(lock.get("frozen_requests") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V254LocalOwnerError("v254 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V254LocalOwnerError("v254 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    paths = _turn_paths(root)
    lineage = _validate_lineage()
    bundle = build_local_bundle(lineage)
    if (
        _load_json(paths["input"], "v254 input") != bundle["local_input"]
        or _load_json(paths["packet"], "v254 packet") != bundle["packet"]
        or paths["prompt"].read_text(encoding="utf-8") != bundle["prompt"]
        or _load_json(paths["schema"], "v254 schema") != bundle["schema"]
    ):
        raise V254LocalOwnerError("v254 frozen request drifted")
    return {
        "root": root,
        "spec_path": root / "attempt-spec.json",
        "runtime_lock": root / "runtime-lock.json",
        "capacity_policy": root / "capacity-policy.json",
        "paths": paths,
        "bundle": bundle,
        "instructions": (root / "adjudication-instructions.private.md").read_text(
            encoding="utf-8"
        ),
    }


def freeze_v254(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v254 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V254LocalOwnerError("unfinished v254 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    bundle = build_local_bundle(lineage)
    capacity_paths = _capacity_policy(root)
    paths = _turn_paths(root)
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], bundle["local_input"])
    _write_immutable(paths["packet"], bundle["packet"])
    _write_private_text(paths["prompt"], bundle["prompt"])
    _write_immutable(paths["schema"], bundle["schema"])
    instructions_path = root / "adjudication-instructions.private.md"
    _write_private_text(instructions_path, adjudication_instructions())
    failure_audit_path = root / "predecessor-attempt-audit.json"
    _write_stable_time(
        failure_audit_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "v252_terminal": lineage["records"]["v252_terminal"],
            "v252_sidecar": lineage["records"]["v252_sidecar"],
            "v252_output_not_adopted": lineage["records"]["v252_output_not_adopted"],
            "v252_measured_tokens": 98_728,
            "v252_declared_bound": 70_000,
            "v253_terminal": lineage["records"]["v253_terminal"],
            "v253_sidecar": lineage["records"]["v253_sidecar"],
            "v253_usage_status": "unknown",
            "v253_wall_seconds": 3.469,
            "v253_failure_class": "preoutput_turn_failed_unknown_usage",
            "predecessor_outputs_adopted": False,
            "predecessors_retried": False,
            "production_mutated": False,
        },
        "created_at",
    )
    design_path = root / "adjudication-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "strategy": "frozen_side_free_owner_adjudication_over_mechanical_observable_difference_closure",
            "model": MODEL,
            "effort": EFFORT,
            "full_witness_count": 59,
            "local_witness_count": EXPECTED_LOCAL_WITNESS_COUNT,
            "subset_rule": "owner_membership_pair_unpaired_difference_then_group_closure",
            "semantic_subset_selection_used": False,
            "prompt_or_rubric_changed": False,
            "frozen_v153_instruction_hash": FROZEN_V153_INSTRUCTION_HASH,
            "adjudication_call_cap": 1,
            "retry_count": 0,
            "majority_voting_used": False,
            "production_amortized_extractor_ratio": v251.OBSERVED_PRODUCTION_RATIO,
            "holdout_authorized_before_score": False,
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
            "state": "frozen_before_single_local_owner_adjudication",
            "declared_turn_count": 1,
            "turn_name": TURN_NAME,
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "prompt_bytes": bundle["prompt_bytes"],
            "schema_bytes": bundle["schema_bytes"],
            "local_witness_count": EXPECTED_LOCAL_WITNESS_COUNT,
            "adjudication_call_cap": 1,
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
        "declared_turn_count": 1,
        "adjudication_call_cap": 1,
        "retry_count": 0,
        "mechanical_local_witness_count": EXPECTED_LOCAL_WITNESS_COUNT,
        "frozen_adjudication_instruction_hash": FROZEN_V153_INSTRUCTION_HASH,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
        "failure_audit": _record(failure_audit_path),
        "design": _record(design_path),
        "spec": _record(spec_path),
        "capacity_audit": _record(capacity_paths["audit"]),
        "capacity_policy": _record(capacity_paths["policy"]),
        "instructions": _record(instructions_path),
        "frozen_requests": _request_records(root),
        "holdout_authorized_before_score": False,
        "production_mutation_allowed": False,
    }
    _write_stable_time(lock_path, lock, "frozen_at")
    verify_runtime_lock(lock_path)
    return _load_frozen(root)


def _measured_usage(sidecar: Mapping[str, Any]) -> Optional[dict[str, int]]:
    try:
        usage = {field: int((sidecar.get("usage") or {})[field]) for field in USAGE_FIELDS}
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
    paths = frozen["paths"]
    attempted = paths["capacity"].exists() or paths["sidecar"].exists()
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = int(attempted)
    sidecar_record = None
    measured_over_cap = False
    if paths["sidecar"].is_file():
        sidecar_record = _record(paths["sidecar"])
        measured = _measured_usage(_load_json(paths["sidecar"], "v254 sidecar"))
        if measured is not None:
            usage = measured
            unknown = 0
            measured_over_cap = usage["total_tokens"] > MAX_TOTAL_TOKENS
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": (
            "v254_local_adjudication_quality_or_cost_gate_not_passed"
            if measured_over_cap
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "attempted_turn_count": int(attempted),
        "measured_turn_count": int(attempted and not unknown),
        "unknown_usage_turn_count": unknown,
        "semantic_retry_count": 0,
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "usage": usage,
        "measured_token_bound_exceeded": measured_over_cap,
        "sidecar": sidecar_record,
        "development_quality_passed": False,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
        "attempt_spec": _record(frozen["spec_path"]),
        "exact_next_action": "audit immutable v254 attempt; no retry",
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _winner(score_path: Path, audit_path: Path) -> dict[str, Any]:
    winner = v251._winner(score_path)
    winner.update(
        {
            "schema_version": WINNER_VERSION,
            "system_id": "v249_one_pass_explicit_applicability_v254_local_owner_adjudicated",
            "adjudication": {
                "model": MODEL,
                "effort": EFFORT,
                "call_count": 1,
                "call_cap": 1,
                "mechanical_local_witness_count": EXPECTED_LOCAL_WITNESS_COUNT,
                "instruction_hash": FROZEN_V153_INSTRUCTION_HASH,
                "pre_adjudication_score": _record(V251_ROOT / "alignment-score.json"),
                "reconciliation_audit": _record(audit_path),
            },
        }
    )
    return winner


async def run_v254(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v254 terminal")
    frozen = freeze_v254(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V254LocalOwnerError("launch exists; replay prohibited")
        )
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 1,
            "adjudication_call_cap": 1,
            "mechanical_local_witness_count": EXPECTED_LOCAL_WITNESS_COUNT,
            "retry_count": 0,
            "model": MODEL,
            "effort": EFFORT,
            "runtime_lock": _record(frozen["runtime_lock"]),
            "managed_chatgpt_auth_only": True,
            "holdout_authorized_before_score": False,
            "production_mutation_allowed": False,
        },
    )
    started = time.monotonic()
    try:
        paths = frozen["paths"]
        async with client_factory(frozen["capacity_policy"]) as client:
            result = await client.run_ephemeral_structured_turn(
                model=MODEL,
                effort=EFFORT,
                base_instructions=frozen["instructions"],
                prompt=frozen["bundle"]["prompt"],
                output_schema=frozen["bundle"]["schema"],
                cwd=PROJECT_ROOT,
                sidecar_path=paths["sidecar"],
                output_path=paths["output"],
                batch_size=1,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=paths["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise V254LocalOwnerError("local owner adjudication did not complete")
        usage = _measured_usage(_load_json(paths["sidecar"], "v254 sidecar"))
        if usage is None:
            raise V254LocalOwnerError("v254 sidecar accounting or auth failed")
        if usage["total_tokens"] > MAX_TOTAL_TOKENS:
            raise V254LocalOwnerError("v254 measured turn exceeded frozen token bound")
        normalized, reconciled, score = reconcile_and_score(
            bundle=frozen["bundle"], output=result.output
        )
        _write_immutable(paths["normalized"], normalized)
        reconciled_path = root / "reconciled-alignment.private.json"
        score_path = root / "reconciled-alignment-score.json"
        audit_path = root / "reconciliation-audit.json"
        _write_immutable(reconciled_path, reconciled)
        _write_immutable(score_path, score)
        _write_stable_time(
            audit_path,
            {
                "schema_version": SCHEMA_VERSION,
                "created_at": now_iso(),
                "full_witness_count": 59,
                "mechanical_local_witness_count": EXPECTED_LOCAL_WITNESS_COUNT,
                "subset_rule": "owner_membership_pair_unpaired_difference_then_group_closure",
                "semantic_subset_selection_used": False,
                "adjudication_call_count": 1,
                "adjudication_call_cap": 1,
                "majority_voting_used": False,
                "pre_adjudication_score": _record(V251_ROOT / "alignment-score.json"),
                "post_adjudication_passed": score["passed"],
                "post_adjudication_failed_checks": score["failed_checks"],
                "post_adjudication_metrics": score["metrics"],
                "semantic_decision_owner": "gpt-5.5_frozen_v153_side_free_owner",
                "deterministic_work": "opaque_set_difference_group_closure_splice_validation_scoring_accounting_only",
            },
            "created_at",
        )
        passed = bool(score["passed"])
        winner_path = root / "development-winner.json"
        winner_record = None
        if passed:
            _write_stable_time(winner_path, _winner(score_path, audit_path), "frozen_at")
            winner_record = _record(winner_path)
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "completed" if passed else "inactive_incomplete_recovery_required",
            "terminal_reason": (
                "v254_local_owner_adjudication_passed_development_winner_frozen_holdout_authorized"
                if passed
                else "v254_local_owner_adjudication_quality_gate_not_passed"
            ),
            "attempted_turn_count": 1,
            "measured_turn_count": 1,
            "unknown_usage_turn_count": 0,
            "semantic_retry_count": 0,
            "adjudication_call_count": 1,
            "adjudication_call_cap": 1,
            "mechanical_local_witness_count": EXPECTED_LOCAL_WITNESS_COUNT,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "development_quality_passed": passed,
            "development_winner_frozen": passed,
            "holdout_authorized": passed,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "production_amortized_total_token_ratio": v251.OBSERVED_PRODUCTION_RATIO,
            "wall_seconds": round(time.monotonic() - started, 6),
            "score": _record(score_path),
            "reconciliation_audit": _record(audit_path),
            "development_winner": winner_record,
            "sidecar": _record(paths["sidecar"]),
            "runtime_lock": _record(frozen["runtime_lock"]),
            "attempt_spec": _record(frozen["spec_path"]),
            "exact_next_action": (
                "freeze the smallest balanced canary and untouched paired holdout"
                if passed
                else "reject v249 and resume ranked distinct architecture search"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v254 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v254 local owner adjudication")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v254(output_dir=Path(args.output_dir))
        result = {
            "state": "frozen",
            "root": str(frozen["root"]),
            "local_witness_count": len(frozen["bundle"]["closure"]),
            "prompt_bytes": frozen["bundle"]["prompt_bytes"],
            "schema_bytes": frozen["bundle"]["schema_bytes"],
        }
    else:
        terminal = asyncio.run(
            run_v254(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
