from __future__ import annotations

"""Run the frozen one-call side-free adjudication for v251's sole disagreement."""

import argparse
import asyncio
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


SCHEMA_VERSION = "pif_app_server_judge_v5_selection_v252_capped_adjudication_v1"
RUNTIME_LOCK_VERSION = "pif_app_server_judge_v5_selection_v252_runtime_lock_v1"
TERMINAL_VERSION = "pif_app_server_judge_v5_selection_v252_terminal_v1"
WINNER_VERSION = "pif_app_server_judge_v5_selection_v252_winner_v1"
PHASE_ID = "judge_v5_4_selection_v252_capped_adjudication"
TURN_NAME = "v252_capped_side_free_adjudication"
MODEL = "gpt-5.5"
EFFORT = "high"
TIMEOUT_SECONDS = 900.0
MAX_TOTAL_TOKENS = 70_000
MAX_PROMPT_BYTES = 512_000
MAX_SCHEMA_BYTES = 128_000
MIN_REMAINING_RESERVE_PERCENT = 20
QUOTA_POINTS_PER_MILLION_TOKENS = 17
EXPECTED_DISAGREEMENT_CASE_COUNT = 1
EXPECTED_ALIGNMENT_WITNESSES = v251.EXPECTED_ALIGNMENT_WITNESSES
USAGE_FIELDS = v251.USAGE_FIELDS
PROJECT_ROOT = v251.PROJECT_ROOT
PIPELINE_ROOT = v251.PIPELINE_ROOT
V251_ROOT = v251.DEFAULT_OUTPUT_ROOT
V153_ROOT = (
    PIPELINE_ROOT / "judge-calibration-v5_4-v153-capped-alignment-adjudication"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT / "development-selection-v5_4-v252-capped-adjudication-v249"
).resolve()
PINNED_CODEX_0_144_1 = v251.PINNED_CODEX_0_144_1
FROZEN_V153_INSTRUCTION_HASH = (
    "f295e1bad136c68aa6ecf1ee061a8f96d1225c641813970f792da482d686e79b"
)


class V252CappedAdjudicationError(RuntimeError):
    """The v252 frozen adjudication cannot proceed or be adopted safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise V252CappedAdjudicationError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise V252CappedAdjudicationError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise V252CappedAdjudicationError(f"frozen {path.name} drifted")
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
        "packet": turn_root / "adjudication-packet.private.json",
        "prompt": turn_root / "prompt.private.md",
        "schema": turn_root / "schema.json",
        "capacity": turn_root / "capacity.json",
        "sidecar": turn_root / "sidecar.json",
        "output": turn_root / "output.private.json",
        "normalized": turn_root / "normalized.private.json",
    }


def _lineage_paths() -> dict[str, Path]:
    base_root = V251_ROOT / "turns" / "v251-frozen-alignment-base"
    canary_root = V251_ROOT / "turns" / "v251-frozen-alignment-balanced-canary"
    v153_turn = V153_ROOT / "turns" / "capped-alignment-disagreement-adjudication"
    return {
        "v251_terminal": V251_ROOT / "terminal.json",
        "v251_score": V251_ROOT / "alignment-score.json",
        "v251_runtime_lock": V251_ROOT / "runtime-lock.json",
        "v251_mapping": V251_ROOT / "origin-map.private.json",
        "v251_receipts": V251_ROOT / "support-receipts.private.json",
        "v251_base_input": base_root / "input.private.json",
        "v251_base_output": base_root / "output.private.json",
        "v251_base_sidecar": base_root / "sidecar.json",
        "v251_canary_input": canary_root / "input.private.json",
        "v251_canary_output": canary_root / "output.private.json",
        "v251_canary_sidecar": canary_root / "sidecar.json",
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
        raise V252CappedAdjudicationError("frozen v153 adjudication instructions drifted")
    return value


def _validate_lineage() -> dict[str, Any]:
    paths = _lineage_paths()
    records = {name: _record(path) for name, path in paths.items()}
    v251.verify_runtime_lock(paths["v251_runtime_lock"])
    terminal = _load_json(paths["v251_terminal"], "v251 terminal")
    score = _load_json(paths["v251_score"], "v251 score")
    protocol = _load_json(paths["v153_protocol"], "v153 protocol")
    v153_terminal = _load_json(paths["v153_terminal"], "v153 terminal")
    v153_sidecar = _load_json(paths["v153_sidecar"], "v153 sidecar")
    if (
        terminal.get("terminal_reason")
        != "v251_alignment_quality_or_permutation_gate_not_passed"
        or terminal.get("accounting_complete") is not True
        or terminal.get("production_mutated") is not False
        or score.get("failed_checks") != ["permutation_projection_exact"]
        or score.get("permutation_disagreement_case_ids")
        != ["jcase_1931e2b3b04ae6f32251e5f9"]
        or score.get("checks", {}).get("strict_full_field_macro_noninferior_margin_0_03")
        is not True
        or score.get("checks", {}).get(
            "production_amortized_total_token_ratio_lte_0_28"
        )
        is not True
        or protocol.get("adjudication_model") != MODEL
        or protocol.get("adjudication_effort") != EFFORT
        or protocol.get("adjudication_instructions_sha256")
        != FROZEN_V153_INSTRUCTION_HASH
        or protocol.get("adjudication_call_cap") != 1
        or protocol.get("retry_count_per_turn") != 0
        or protocol.get("majority_voting_used") is not False
        or v153_terminal.get("terminal_reason")
        != "v153_alignment_protocol_frozen_full_development_calibration_authorized"
        or v153_terminal.get("alignment_protocol_frozen") is not True
        or v153_terminal.get("production_mutated") is not False
        or v153_sidecar.get("auth_type") != "chatgpt"
        or v153_sidecar.get("model") != MODEL
        or v153_sidecar.get("effort") != EFFORT
        or v153_sidecar.get("base_instructions_sha256")
        != FROZEN_V153_INSTRUCTION_HASH
    ):
        raise V252CappedAdjudicationError("v252 frozen lineage or protocol drifted")
    adjudication_instructions()
    return {"paths": paths, "records": records}


def build_adjudication_bundle(lineage: Mapping[str, Any]) -> dict[str, Any]:
    paths = lineage["paths"]
    base_input = _load_json(paths["v251_base_input"], "v251 base input")
    base_output = _load_json(paths["v251_base_output"], "v251 base output")
    canary_input = _load_json(paths["v251_canary_input"], "v251 canary input")
    canary_output = _load_json(paths["v251_canary_output"], "v251 canary output")
    receipts = _load_json(paths["v251_receipts"], "v251 support receipts")
    packet = judge.build_disagreement_adjudication_input(
        base_input=base_input,
        base_output=base_output,
        canary_input=canary_input,
        canary_output=canary_output,
        support_receipts=receipts,
    )
    if len(packet.get("cases") or []) == 1:
        row = packet["cases"][0]
        swap = int(sha256_text(f"v252|candidate-order|{row['case_id']}")[:2], 16) % 2 == 1
        if swap:
            row["anonymous_candidate_1"], row["anonymous_candidate_2"] = (
                row["anonymous_candidate_2"],
                row["anonymous_candidate_1"],
            )
        packet["anonymous_candidate_order_rule"] = "opaque_case_hash_swap"
        packet["anonymous_candidate_swapped"] = swap
    alignment_input = judge.adjudication_alignment_input(
        base_input=base_input,
        adjudication_input=packet,
    )
    prompt = judge.build_disagreement_adjudication_prompt(
        adjudication_input=packet,
        adjudication_alignment=alignment_input,
    )
    schema = judge.neutral_alignment_output_schema(alignment_input)
    prompt_bytes = len(prompt.encode("utf-8"))
    schema_bytes = len(_canonical_json(schema).encode("utf-8"))
    case_ids = [str(row["case_id"]) for row in packet.get("cases") or []]
    witness_count = sum(len(row.get("witnesses") or []) for row in alignment_input["cases"])
    if (
        packet.get("adjudication_required") is not True
        or packet.get("call_cap") != 1
        or packet.get("candidate_order_has_no_vote_meaning") is not True
        or case_ids != ["jcase_1931e2b3b04ae6f32251e5f9"]
        or packet["cases"][0].get("observed_disagreement_reasons")
        != ["permutation_output_changed"]
        or len(alignment_input["cases"]) != EXPECTED_DISAGREEMENT_CASE_COUNT
        or witness_count != EXPECTED_ALIGNMENT_WITNESSES
        or alignment_input.get("side_labels_present") is not False
        or alignment_input.get("system_identity_present") is not False
        or prompt_bytes > MAX_PROMPT_BYTES
        or schema_bytes > MAX_SCHEMA_BYTES
    ):
        raise V252CappedAdjudicationError("v252 adjudication bundle drifted")
    return {
        "packet": packet,
        "alignment_input": alignment_input,
        "base_input": base_input,
        "base_output": base_output,
        "canary_input": canary_input,
        "canary_output": canary_output,
        "support_receipts": receipts,
        "mapping": _load_json(paths["v251_mapping"], "v251 mapping"),
        "prompt": prompt,
        "schema": schema,
        "prompt_bytes": prompt_bytes,
        "schema_bytes": schema_bytes,
    }


def reconcile_and_score(
    *, bundle: Mapping[str, Any], output: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    errors = judge.validate_neutral_alignment_output(output, bundle["alignment_input"])
    if errors:
        raise V252CappedAdjudicationError(
            "v252 adjudication output did not validate: "
            + "; ".join(sorted(set(errors)))
        )
    normalized = judge.normalize_neutral_alignment_output(
        output, bundle["alignment_input"]
    )
    reconciled = judge.reconcile_neutral_alignment(
        base_output=bundle["base_output"],
        base_input=bundle["base_input"],
        canary_output=bundle["canary_output"],
        canary_input=bundle["canary_input"],
        support_receipts=bundle["support_receipts"],
        adjudication_output=output,
        adjudication_input=bundle["alignment_input"],
    )
    if (
        reconciled.get("observable_disagreement_case_count") != 1
        or reconciled.get("adjudication_call_count") != 1
        or reconciled.get("adjudication_call_cap") != 1
        or reconciled.get("majority_voting_used") is not False
    ):
        raise V252CappedAdjudicationError("v252 reconciliation contract drifted")
    score = v251.score_alignment(
        base=reconciled,
        canary=reconciled,
        mapping=bundle["mapping"],
    )
    return normalized, reconciled, score


def _capacity_policy(root: Path) -> dict[str, Path]:
    audit_path = root / "capacity-policy-audit.json"
    policy_path = root / "capacity-policy.json"
    projected = math.ceil(
        MAX_TOTAL_TOKENS * QUOTA_POINTS_PER_MILLION_TOKENS / 1_000_000
    )
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
                codex_app_server.PROTOCOL_SCHEMA_PATH.resolve(),
            },
            key=str,
        )
    )


def _request_records(root: Path) -> list[dict[str, Any]]:
    paths = _turn_paths(root)
    return [_record(paths[name]) for name in ("input", "packet", "prompt", "schema")]


def verify_runtime_lock(path: Path) -> dict[str, Any]:
    lock = _load_json(path, "v252 runtime lock")
    root = path.parent.resolve()
    if (
        lock.get("schema_version") != RUNTIME_LOCK_VERSION
        or lock.get("phase_id") != PHASE_ID
        or lock.get("model") != MODEL
        or lock.get("effort") != EFFORT
        or lock.get("declared_turn_count") != 1
        or lock.get("adjudication_call_cap") != 1
        or lock.get("retry_count") != 0
        or lock.get("frozen_adjudication_instruction_hash")
        != FROZEN_V153_INSTRUCTION_HASH
        or lock.get("holdout_authorized_before_score") is not False
        or lock.get("production_mutation_allowed") is not False
        or {str(Path(row["path"]).resolve()) for row in lock.get("runtime_files") or []}
        != {str(path) for path in _runtime_files()}
        or {row["path"] for row in lock.get("frozen_requests") or []}
        != {row["path"] for row in _request_records(root)}
        or lock.get("pinned_codex_cli") != _record(PINNED_CODEX_0_144_1)
    ):
        raise V252CappedAdjudicationError("v252 runtime lock contract drifted")
    records = [
        lock.get("pinned_codex_cli"),
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_lineage") or []),
        lock.get("design"),
        lock.get("spec"),
        lock.get("capacity_audit"),
        lock.get("capacity_policy"),
        lock.get("instructions"),
        *(lock.get("frozen_requests") or []),
    ]
    if any(not _verify_record(record or {}) for record in records):
        raise V252CappedAdjudicationError("v252 runtime lock record drifted")
    lineage = _validate_lineage()
    if {row["path"] for row in lock["direct_lineage"]} != {
        row["path"] for row in lineage["records"].values()
    }:
        raise V252CappedAdjudicationError("v252 direct lineage set drifted")
    reserve.load_reserve_capacity_policy(Path(lock["capacity_policy"]["path"]))
    return lock


def _load_frozen(root: Path) -> dict[str, Any]:
    paths = _turn_paths(root)
    lineage = _validate_lineage()
    bundle = build_adjudication_bundle(lineage)
    frozen_input = _load_json(paths["input"], "v252 input")
    frozen_packet = _load_json(paths["packet"], "v252 packet")
    prompt = paths["prompt"].read_text(encoding="utf-8")
    schema = _load_json(paths["schema"], "v252 schema")
    if (
        frozen_input != bundle["alignment_input"]
        or frozen_packet != bundle["packet"]
        or prompt != bundle["prompt"]
        or schema != bundle["schema"]
    ):
        raise V252CappedAdjudicationError("v252 frozen request drifted")
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


def freeze_v252(*, output_dir: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if (root / "terminal.json").exists():
        return {"root": root, "terminal": _load_json(root / "terminal.json", "v252 terminal")}
    if any(root.iterdir()):
        if not (root / "runtime-lock.json").is_file():
            raise V252CappedAdjudicationError("unfinished v252 root is not replayable")
        verify_runtime_lock(root / "runtime-lock.json")
        return _load_frozen(root)
    lineage = _validate_lineage()
    bundle = build_adjudication_bundle(lineage)
    capacity_paths = _capacity_policy(root)
    paths = _turn_paths(root)
    paths["root"].mkdir(parents=True, exist_ok=True)
    _write_immutable(paths["input"], bundle["alignment_input"])
    _write_immutable(paths["packet"], bundle["packet"])
    _write_private_text(paths["prompt"], bundle["prompt"])
    _write_immutable(paths["schema"], bundle["schema"])
    instructions_path = root / "adjudication-instructions.private.md"
    _write_private_text(instructions_path, adjudication_instructions())
    design_path = root / "adjudication-design.json"
    _write_stable_time(
        design_path,
        {
            "schema_version": SCHEMA_VERSION,
            "created_at": now_iso(),
            "strategy": "one_capped_side_free_owner_adjudication_for_observable_v251_permutation_disagreement",
            "model": MODEL,
            "effort": EFFORT,
            "frozen_v153_instruction_hash": FROZEN_V153_INSTRUCTION_HASH,
            "observable_disagreement_case_count": 1,
            "adjudication_call_cap": 1,
            "majority_voting_used": False,
            "prompt_or_rubric_changed": False,
            "retry_count": 0,
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
            "state": "frozen_before_single_capped_adjudication",
            "declared_turn_count": 1,
            "turn_name": TURN_NAME,
            "model": MODEL,
            "effort": EFFORT,
            "retry_count": 0,
            "maximum_total_tokens": MAX_TOTAL_TOKENS,
            "prompt_bytes": bundle["prompt_bytes"],
            "schema_bytes": bundle["schema_bytes"],
            "adjudication_call_cap": 1,
            "majority_voting_used": False,
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
        "frozen_adjudication_instruction_hash": FROZEN_V153_INSTRUCTION_HASH,
        "pinned_codex_cli": _record(PINNED_CODEX_0_144_1),
        "runtime_files": [_record(path) for path in _runtime_files()],
        "direct_lineage": list(lineage["records"].values()),
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


def _usage(sidecar: Mapping[str, Any]) -> dict[str, int]:
    values = sidecar.get("usage") or {}
    try:
        usage = {field: int(values[field]) for field in USAGE_FIELDS}
    except (KeyError, TypeError, ValueError) as exc:
        raise V252CappedAdjudicationError("v252 sidecar usage incomplete") from exc
    if (
        sidecar.get("state") != "completed"
        or sidecar.get("status") != "completed"
        or sidecar.get("usage_status") != "measured"
        or sidecar.get("usage_complete") is not True
        or sidecar.get("auth_type") != "chatgpt"
        or sidecar.get("model") != MODEL
        or sidecar.get("effort") != EFFORT
        or sidecar.get("error_class") is not None
        or usage["total_tokens"] > MAX_TOTAL_TOKENS
    ):
        raise V252CappedAdjudicationError("v252 sidecar accounting or auth failed")
    return usage


def _inner_factory() -> codex_app_server.CodexAppServerClient:
    return codex_app_server.CodexAppServerClient(
        command=[str(PINNED_CODEX_0_144_1), "app-server", "--stdio", "--strict-config"]
    )


def _client_factory(policy_path: Path) -> ReserveCapacityGatedCodexAppServerClient:
    return ReserveCapacityGatedCodexAppServerClient(
        policy_path=policy_path, inner_factory=_inner_factory
    )


def _failure_terminal(
    root: Path, frozen: Mapping[str, Any], exc: BaseException
) -> dict[str, Any]:
    paths = _turn_paths(root)
    attempted = paths["capacity"].exists() or paths["sidecar"].exists()
    usage = {field: 0 for field in USAGE_FIELDS}
    unknown = 0
    sidecars: list[dict[str, Any]] = []
    if attempted:
        if paths["sidecar"].is_file():
            sidecars.append(_record(paths["sidecar"]))
            try:
                usage = _usage(_load_json(paths["sidecar"], "v252 sidecar"))
            except Exception:
                unknown = 1
        else:
            unknown = 1
    message = str(exc).encode("utf-8", errors="replace")
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "inactive_incomplete_recovery_required",
        "terminal_reason": "infrastructure_or_judge_attempt_failed",
        "error_class": type(exc).__name__,
        "error_message_sha256": hashlib.sha256(message).hexdigest(),
        "error_message_bytes": len(message),
        "attempted_turn_count": int(attempted),
        "measured_turn_count": int(attempted and unknown == 0),
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
        "exact_next_action": "audit immutable v252 adjudication attempt; no retry",
    }
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


def _winner(score_path: Path, audit_path: Path) -> dict[str, Any]:
    winner = v251._winner(score_path)
    winner.update(
        {
            "schema_version": WINNER_VERSION,
            "system_id": "v249_one_pass_explicit_applicability_v252_adjudicated",
            "adjudication": {
                "model": MODEL,
                "effort": EFFORT,
                "call_count": 1,
                "call_cap": 1,
                "instruction_hash": FROZEN_V153_INSTRUCTION_HASH,
                "pre_adjudication_score": _record(V251_ROOT / "alignment-score.json"),
                "reconciliation_audit": _record(audit_path),
            },
        }
    )
    return winner


async def run_v252(
    *,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = _client_factory,
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").exists():
        return _load_json(root / "terminal.json", "v252 terminal")
    frozen = freeze_v252(output_dir=root)
    verify_runtime_lock(frozen["runtime_lock"])
    if (root / "launch-receipt.json").exists():
        return _failure_terminal(
            root, frozen, V252CappedAdjudicationError("launch exists; replay prohibited")
        )
    _write_immutable(
        root / "launch-receipt.json",
        {
            "schema_version": SCHEMA_VERSION,
            "launched_at": now_iso(),
            "phase_id": PHASE_ID,
            "declared_turn_count": 1,
            "adjudication_call_cap": 1,
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
                batch_size=EXPECTED_DISAGREEMENT_CASE_COUNT,
                thread_mode="new_thread",
                timeout_seconds=timeout_seconds,
                capacity_checkpoint_path=paths["capacity"],
            )
        if result.status_ok is not True or not isinstance(result.output, Mapping):
            raise V252CappedAdjudicationError("adjudication turn did not complete")
        usage = _usage(_load_json(paths["sidecar"], "v252 sidecar"))
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
                "observable_disagreement_case_count": 1,
                "adjudication_call_count": 1,
                "adjudication_call_cap": 1,
                "majority_voting_used": False,
                "unresolved_cases_abstained": reconciled[
                    "unresolved_cases_abstained"
                ],
                "pre_adjudication_score": _record(V251_ROOT / "alignment-score.json"),
                "post_adjudication_passed": score["passed"],
                "post_adjudication_failed_checks": score["failed_checks"],
                "post_adjudication_metrics": score["metrics"],
                "semantic_decision_owner": "gpt-5.5_frozen_v153_side_free_adjudication",
                "deterministic_work": "schema_validation_normalization_projection_scoring_accounting_only",
            },
            "created_at",
        )
        passed = bool(score["passed"])
        winner_path = root / "development-winner.json"
        winner_record = None
        if passed:
            _write_stable_time(winner_path, _winner(score_path, audit_path), "frozen_at")
            winner_record = _record(winner_path)
        v251_usage = _load_json(V251_ROOT / "terminal.json", "v251 terminal")["usage"]
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "completed" if passed else "inactive_incomplete_recovery_required",
            "terminal_reason": (
                "v252_capped_adjudication_passed_development_winner_frozen_holdout_authorized"
                if passed
                else "v252_capped_adjudication_quality_gate_not_passed"
            ),
            "attempted_turn_count": 1,
            "measured_turn_count": 1,
            "unknown_usage_turn_count": 0,
            "semantic_retry_count": 0,
            "adjudication_call_count": 1,
            "adjudication_call_cap": 1,
            "majority_voting_used": False,
            "usage_status": "complete",
            "accounting_complete": True,
            "usage": usage,
            "predecessor_v251_usage": v251_usage,
            "cumulative_v251_v252_usage": {
                field: int(v251_usage[field]) + usage[field] for field in USAGE_FIELDS
            },
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
                else "reject v249 and advance to the next distinct architecture"
            ),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if (root / "terminal.json").exists():
            return _load_json(root / "terminal.json", "v252 terminal")
        return _failure_terminal(root, frozen, exc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run v252 capped side-free adjudication")
    parser.add_argument("action", choices=["freeze", "run"])
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--timeout-seconds", type=float, default=TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    if args.action == "freeze":
        frozen = freeze_v252(output_dir=Path(args.output_dir))
        result = {"state": "frozen", "root": str(frozen["root"])}
    else:
        terminal = asyncio.run(
            run_v252(output_dir=Path(args.output_dir), timeout_seconds=args.timeout_seconds)
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
