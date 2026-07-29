from __future__ import annotations

"""Adopt measured evaluator turns after exact duplicate-span normalization."""

import argparse
import asyncio
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import app_server_candidate_evaluation_bundle as evaluator
from . import app_server_candidate_evaluation_runner as runner
from . import app_server_judge_v5 as judge
from .app_server_runtime_verifier import ContentHashCache
from .util import now_iso, sha256_text


RECOVERY_VERSION = "pif_candidate_evaluation_duplicate_span_recovery_v1"
LOCK_VERSION = "pif_candidate_evaluation_duplicate_span_recovery_lock_v1"
TERMINAL_VERSION = "pif_candidate_evaluation_duplicate_span_recovery_terminal_v1"
PROJECT_ROOT = evaluator.PROJECT_ROOT
PIPELINE_ROOT = evaluator.PIPELINE_ROOT
DEFAULT_PREDECESSOR_ROOT = (
    PIPELINE_ROOT
    / "development-canary-v249-source-complete-columnar-owner-v2-semantic-evaluation-v1"
).resolve()
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-canary-v249-source-complete-columnar-owner-v2-semantic-evaluation-v2-duplicate-span-recovery"
).resolve()
HASH_CACHE = ContentHashCache()


class CandidateProjectionRecoveryError(RuntimeError):
    """The additive postprocess recovery cannot proceed safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateProjectionRecoveryError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise CandidateProjectionRecoveryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _write_private_text(path: Path, value: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") != value:
            raise CandidateProjectionRecoveryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _write_stable_time(path: Path, value: dict[str, Any], key: str) -> None:
    if path.exists():
        value[key] = _load_json(path, f"existing {path.name}").get(key)
    _write_immutable(path, value)


def _record(path: Path) -> dict[str, Any]:
    return HASH_CACHE.record(path.expanduser().resolve())


def _verify_record(record: Mapping[str, Any]) -> None:
    if not HASH_CACHE.verify_record(record):
        raise CandidateProjectionRecoveryError("frozen recovery artifact drifted")


def _digest(records: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json(list(records)).encode("utf-8")).hexdigest()


def project_exact_duplicate_spans(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Remove only byte-identical duplicate entries from exact-span arrays."""

    projected = copy.deepcopy(dict(output))
    changes = []
    for case in projected.get("cases") or []:
        for pair_index, pair in enumerate(case.get("alignment_pairs") or []):
            for row in pair.get("checklist") or []:
                spans = row.get("source_evidence_spans")
                if not isinstance(spans, list):
                    continue
                unique = list(dict.fromkeys(spans))
                if len(unique) == len(spans):
                    continue
                removed = len(spans) - len(unique)
                row["source_evidence_spans"] = unique
                changes.append(
                    {
                        "case_id": case.get("case_id"),
                        "pair_index": pair_index,
                        "field": row.get("field"),
                        "removed_exact_duplicate_count": removed,
                        "unique_span_hashes": [sha256_text(span) for span in unique],
                    }
                )
    errors = judge.validate_neutral_alignment_output(projected, alignment_input)
    if errors:
        raise CandidateProjectionRecoveryError(
            "identity-only span projection did not produce a valid alignment output"
        )
    return projected, {
        "schema_version": RECOVERY_VERSION,
        "changed_checklist_row_count": len(changes),
        "removed_exact_duplicate_span_count": sum(
            row["removed_exact_duplicate_count"] for row in changes
        ),
        "changes": changes,
        "semantic_fields_changed": False,
        "witness_assignments_changed": False,
        "relations_changed": False,
        "checklist_decisions_changed": False,
        "span_values_added_or_rewritten": False,
        "normalization_scope": "byte_identical_duplicate_exact_span_entries_only",
        "production_mutated": False,
    }


def _predecessor_paths(root: Path) -> dict[str, Path]:
    return {
        "terminal": root / "terminal.json",
        "runtime_lock": root / "runtime-lock.json",
        "config": root / "evaluation-config.json",
        "support_terminal": root / "support" / "terminal.json",
        "support_bundle": root / "support" / "support-bundle.private.json",
        "support_audit": root / "support" / "support-audit.json",
        "support_score": root / "support" / "support-score.private.json",
        "support_receipts": root / "support" / "support-receipts.private.json",
        "alignment_lock": root / "alignment" / "runtime-lock.json",
        "alignment_bundle": root / "alignment" / "alignment-bundle.private.json",
        "alignment_mapping": root / "alignment" / "origin-map.private.json",
        "base_output": root / "alignment" / "turns" / "neutral-alignment-base" / "output.private.json",
        "base_sidecar": root / "alignment" / "turns" / "neutral-alignment-base" / "sidecar.json",
        "base_capacity": root / "alignment" / "turns" / "neutral-alignment-base" / "capacity.json",
        "canary_output": root / "alignment" / "turns" / "neutral-alignment-balanced-canary" / "output.private.json",
        "canary_sidecar": root / "alignment" / "turns" / "neutral-alignment-balanced-canary" / "sidecar.json",
        "canary_capacity": root / "alignment" / "turns" / "neutral-alignment-balanced-canary" / "capacity.json",
        "support_sidecar": root / "support" / "turns" / "pointwise-support" / "sidecar.json",
        "support_capacity": root / "support" / "turns" / "pointwise-support" / "capacity.json",
    }


def _validate_predecessor(root: Path) -> dict[str, Any]:
    paths = _predecessor_paths(root)
    for path in paths.values():
        if not path.is_file():
            raise CandidateProjectionRecoveryError("predecessor artifact is missing")
    runner.verify_run_lock(paths["runtime_lock"])
    runner.verify_phase_lock(paths["alignment_lock"])
    terminal = _load_json(paths["terminal"], "predecessor terminal")
    if (
        terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("error_class") != "JudgeV5ProtocolError"
        or terminal.get("accounting_complete") is not True
        or terminal.get("measured_turn_count") != 3
        or terminal.get("unknown_usage_turn_count") != 0
        or terminal.get("production_mutated") is not False
        or (root / "adjudication").exists()
        or (root / "alignment" / "terminal.json").exists()
    ):
        raise CandidateProjectionRecoveryError("predecessor is not the exact postprocess failure")
    config = _load_json(paths["config"], "predecessor config")
    bundle = _load_json(paths["alignment_bundle"], "alignment bundle")
    turns = {str(row["permutation"]): row for row in bundle["turns"]}
    base_raw = _load_json(paths["base_output"], "base output")
    canary_raw = _load_json(paths["canary_output"], "canary output")
    base_projected, base_audit = project_exact_duplicate_spans(
        base_raw, turns["base"]["value"]
    )
    canary_projected, canary_audit = project_exact_duplicate_spans(
        canary_raw, turns["balanced_canary"]["value"]
    )
    if (
        base_audit["removed_exact_duplicate_span_count"] != 0
        or canary_audit["removed_exact_duplicate_span_count"] <= 0
    ):
        raise CandidateProjectionRecoveryError("predecessor duplicate-span signature drifted")
    return {
        "paths": paths,
        "records": [_record(path) for path in paths.values()],
        "config": config,
        "alignment_bundle": bundle,
        "base_projected": base_projected,
        "base_audit": base_audit,
        "canary_projected": canary_projected,
        "canary_audit": canary_audit,
    }


def freeze_recovery(
    *,
    predecessor_root: Path = DEFAULT_PREDECESSOR_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    predecessor_root = predecessor_root.expanduser().resolve()
    root = output_dir.expanduser().resolve()
    source = _validate_predecessor(predecessor_root)
    base_path = root / "projected-base-output.private.json"
    canary_path = root / "projected-canary-output.private.json"
    audit_path = root / "duplicate-span-projection-audit.json"
    _write_immutable(base_path, source["base_projected"])
    _write_immutable(canary_path, source["canary_projected"])
    _write_immutable(
        audit_path,
        {
            "schema_version": RECOVERY_VERSION,
            "base": source["base_audit"],
            "balanced_canary": source["canary_audit"],
            "raw_outputs_preserved": True,
            "semantic_model_calls_replayed": 0,
            "semantic_normalization_performed": False,
            "identity_only_exact_span_projection": True,
            "production_mutated": False,
        },
    )
    adjudication_bundle = evaluator.build_adjudication_bundle(
        base_output=source["base_projected"],
        canary_output=source["canary_projected"],
        alignment_bundle=source["alignment_bundle"],
    )
    adjudication_bundle_path = root / "adjudication" / "adjudication-bundle.private.json"
    _write_immutable(adjudication_bundle_path, adjudication_bundle)
    turn_paths = None
    policy_paths = None
    request_records: list[dict[str, Any]] = []
    if adjudication_bundle["adjudication_required"]:
        turn = adjudication_bundle["turn"]
        if not isinstance(turn, Mapping):
            raise CandidateProjectionRecoveryError("required adjudication has no turn")
        turn_paths = runner._turn_paths(  # noqa: SLF001
            root / "adjudication", str(turn["name"])
        )
        _write_immutable(
            turn_paths["input"],
            {
                "packet": adjudication_bundle["packet"],
                "alignment_input": turn["value"],
            },
        )
        _write_private_text(turn_paths["base"], str(turn["base_instructions"]))
        _write_private_text(turn_paths["prompt"], str(turn["prompt"]))
        _write_immutable(turn_paths["schema"], turn["schema"])
        policy_paths = runner._capacity_policy(  # noqa: SLF001
            phase_root=root / "adjudication",
            phase_id=f"{source['config']['evaluation_id']}_duplicate_span_recovery_adjudication",
            turn_names=(str(turn["name"]),),
            maximum_total_tokens_per_turn=runner.ADJUDICATION_TOTAL_TOKEN_MAXIMUM,
        )
        request_records = [
            _record(adjudication_bundle_path),
            *[
                _record(turn_paths[key])
                for key in ("input", "base", "prompt", "schema")
            ],
            _record(policy_paths["audit"]),
            _record(policy_paths["policy"]),
        ]
    direct_records = [
        *source["records"],
        _record(base_path),
        _record(canary_path),
        _record(audit_path),
        _record(adjudication_bundle_path),
        *request_records,
    ]
    runtime_records = [
        _record(Path(__file__).resolve()),
        _record(Path(evaluator.__file__).resolve()),
        _record(Path(runner.__file__).resolve()),
        _record(Path(judge.__file__).resolve()),
        _record(runner.PINNED_CODEX),
    ]
    all_records = [*runtime_records, *direct_records]
    lock_path = root / "runtime-lock.json"
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "predecessor_root": str(predecessor_root),
        "predecessor_records": source["records"],
        "runtime_files": runtime_records,
        "direct_records": direct_records,
        "direct_record_digest": _digest(all_records),
        "adjudication_required": adjudication_bundle["adjudication_required"],
        "adjudication_call_cap": 1,
        "extraction_replay_count": 0,
        "support_replay_count": 0,
        "alignment_replay_count": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
    }
    if lock_path.exists():
        lock["frozen_at"] = _load_json(lock_path, "runtime lock")["frozen_at"]
    _write_immutable(lock_path, lock)
    verify_recovery_lock(lock_path)
    return {
        "root": root,
        "predecessor": source,
        "runtime_lock": lock_path,
        "base": base_path,
        "canary": canary_path,
        "audit": audit_path,
        "adjudication_bundle": adjudication_bundle,
        "adjudication_bundle_path": adjudication_bundle_path,
        "turn": turn_paths,
        "policy": policy_paths["policy"] if policy_paths else None,
    }


def verify_recovery_lock(path: Path) -> Path:
    lock_path = path.expanduser().resolve()
    lock = _load_json(lock_path, "recovery runtime lock")
    records = [
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_records") or []),
    ]
    if (
        lock.get("schema_version") != LOCK_VERSION
        or lock.get("adjudication_call_cap") != 1
        or lock.get("extraction_replay_count") != 0
        or lock.get("support_replay_count") != 0
        or lock.get("alignment_replay_count") != 0
        or lock.get("production_mutation_allowed") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("direct_record_digest") != _digest(records)
    ):
        raise CandidateProjectionRecoveryError("recovery runtime lock drifted")
    for record in records:
        _verify_record(record)
    _validate_predecessor(Path(str(lock["predecessor_root"])))
    return lock_path


def _sum_usage(rows: Sequence[Mapping[str, int]]) -> dict[str, int]:
    return {
        field: sum(int(row[field]) for row in rows) for field in runner.USAGE_FIELDS
    }


def _predecessor_usage(source: Mapping[str, Any]) -> list[dict[str, int]]:
    paths = source["paths"]
    return [
        runner._usage(  # noqa: SLF001
            paths["support_sidecar"],
            model=evaluator.SUPPORT_MODEL,
            effort=evaluator.SUPPORT_EFFORT,
            maximum=runner.SUPPORT_TOTAL_TOKEN_MAXIMUM,
        ),
        runner._usage(  # noqa: SLF001
            paths["base_sidecar"],
            model=evaluator.ALIGNMENT_MODEL,
            effort=evaluator.ALIGNMENT_EFFORT,
            maximum=runner.ALIGNMENT_TOTAL_TOKEN_MAXIMUM,
        ),
        runner._usage(  # noqa: SLF001
            paths["canary_sidecar"],
            model=evaluator.ALIGNMENT_MODEL,
            effort=evaluator.ALIGNMENT_EFFORT,
            maximum=runner.ALIGNMENT_TOTAL_TOKEN_MAXIMUM,
        ),
    ]


def _failure_terminal(root: Path, frozen: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    rows = _predecessor_usage(frozen["predecessor"])
    unknown = 0
    own_sidecar = (
        frozen["turn"]["sidecar"]
        if isinstance(frozen.get("turn"), Mapping)
        else None
    )
    if own_sidecar is not None and own_sidecar.is_file():
        try:
            rows.append(
                runner._usage(  # noqa: SLF001
                    own_sidecar,
                    model=evaluator.ADJUDICATION_MODEL,
                    effort=evaluator.ADJUDICATION_EFFORT,
                    maximum=runner.ADJUDICATION_TOTAL_TOKEN_MAXIMUM,
                )
            )
        except Exception:
            unknown = 1
    quality = isinstance(exc, runner.CandidateSemanticQualityStop)
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "development_quality_failed" if quality else "failed",
        "terminal_reason": (
            "candidate_semantic_quality_gate_not_passed"
            if quality
            else "infrastructure_or_judge_attempt_failed"
        ),
        "error_class": type(exc).__name__,
        "error_message_sha256": sha256_text(str(exc)),
        "error_message_size_bytes": len(str(exc).encode("utf-8")),
        "usage_status": "unknown" if unknown else "complete",
        "accounting_complete": unknown == 0,
        "measured_turn_count": len(rows),
        "unknown_usage_turn_count": unknown,
        "usage": _sum_usage(rows) if not unknown else None,
        "semantic_retry_count": 0,
        "development_winner_frozen": False,
        "holdout_authorized": False,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(frozen["runtime_lock"]),
    }
    score_path = root / "alignment-score.json"
    if score_path.is_file():
        terminal["score"] = _record(score_path)
    _write_stable_time(root / "terminal.json", terminal, "terminal_at")
    return terminal


async def run_recovery(
    *,
    predecessor_root: Path = DEFAULT_PREDECESSOR_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    timeout_seconds: float = runner.TIMEOUT_SECONDS,
    client_factory: Callable[[Path], Any] = runner._client_factory,  # noqa: SLF001
) -> dict[str, Any]:
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "terminal")
    frozen = freeze_recovery(
        predecessor_root=predecessor_root,
        output_dir=root,
    )
    try:
        verify_recovery_lock(frozen["runtime_lock"])
        adjudication_output = None
        own_usage = None
        if frozen["adjudication_bundle"]["adjudication_required"]:
            turn = frozen["turn"]
            if not isinstance(turn, Mapping) or frozen["policy"] is None:
                raise CandidateProjectionRecoveryError("adjudication request was not frozen")
            if not (root / "launch-receipt.json").exists():
                _write_stable_time(
                    root / "launch-receipt.json",
                    {
                        "schema_version": "pif_candidate_projection_recovery_launch_v1",
                        "launched_at": now_iso(),
                        "semantic_attempt_count": 1,
                        "semantic_retry_count": 0,
                        "managed_chatgpt_auth_only": True,
                        "runtime_lock": _record(frozen["runtime_lock"]),
                        "production_mutation_allowed": False,
                        "holdout_authorized": False,
                    },
                    "launched_at",
                )
            verify_recovery_lock(frozen["runtime_lock"])
            async with client_factory(frozen["policy"]) as client:
                adjudication_output, own_usage, _adopted = await runner._run_turn(  # noqa: SLF001
                    client=client,
                    paths=turn,
                    model=evaluator.ADJUDICATION_MODEL,
                    effort=evaluator.ADJUDICATION_EFFORT,
                    maximum_total_tokens=runner.ADJUDICATION_TOTAL_TOKEN_MAXIMUM,
                    timeout_seconds=timeout_seconds,
                )
        source = frozen["predecessor"]
        score = evaluator.score_alignment(
            base_output=source["base_projected"],
            canary_output=source["canary_projected"],
            alignment_bundle=source["alignment_bundle"],
            support_bundle=_load_json(
                source["paths"]["support_bundle"], "support bundle"
            ),
            support_private_score=_load_json(
                source["paths"]["support_score"], "support score"
            ),
            production_amortized_total_token_ratio=float(
                source["config"]["production_amortized_total_token_ratio"]
            ),
            adjudication_bundle=(
                frozen["adjudication_bundle"]
                if frozen["adjudication_bundle"]["adjudication_required"]
                else None
            ),
            adjudication_output=adjudication_output,
        )
        score_path = root / "alignment-score.json"
        _write_immutable(score_path, score)
        usages = _predecessor_usage(source)
        if own_usage is not None:
            usages.append(own_usage)
        total_usage = _sum_usage(usages)
        if not score["passed"]:
            raise runner.CandidateSemanticQualityStop(
                "candidate semantic quality gate failed after identity-only projection"
            )
        winner_path = root / "development-winner.json"
        _write_immutable(
            winner_path,
            {
                "schema_version": "pif_candidate_projection_recovery_winner_v1",
                "frozen_at": now_iso(),
                "evaluation_id": source["config"]["evaluation_id"],
                "candidate": source["config"]["candidate"],
                "candidate_terminal": source["config"]["candidate_terminal"],
                "candidate_gate": source["config"]["candidate_gate"],
                "semantic_score": _record(score_path),
                "projection_audit": _record(frozen["audit"]),
                "production_amortized_total_token_ratio": source["config"][
                    "production_amortized_total_token_ratio"
                ],
                "development_winner_frozen": True,
                "holdout_authorized": True,
                "production_mutated": False,
            },
        )
        terminal = {
            "schema_version": TERMINAL_VERSION,
            "terminal_at": now_iso(),
            "state": "development_winner_frozen",
            "terminal_reason": "candidate_semantic_quality_and_cost_gates_passed_holdout_authorized",
            "usage_status": "complete",
            "accounting_complete": True,
            "measured_turn_count": len(usages),
            "unknown_usage_turn_count": 0,
            "usage": total_usage,
            "semantic_retry_count": 0,
            "development_winner_frozen": True,
            "holdout_authorized": True,
            "production_mutated": False,
            "overall_goal_complete": False,
            "goal_status_required": "active",
            "runtime_lock": _record(frozen["runtime_lock"]),
            "score": _record(score_path),
            "winner": _record(winner_path),
            "projection_audit": _record(frozen["audit"]),
        }
        _write_stable_time(root / "terminal.json", terminal, "terminal_at")
        return terminal
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return _failure_terminal(root, frozen, exc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "verify", "run"))
    parser.add_argument("--predecessor-root", type=Path, default=DEFAULT_PREDECESSOR_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        frozen = freeze_recovery(
            predecessor_root=args.predecessor_root,
            output_dir=args.output_dir,
        )
        print(
            _canonical_json(
                {
                    "root": str(frozen["root"]),
                    "adjudication_required": frozen["adjudication_bundle"][
                        "adjudication_required"
                    ],
                    "semantic_turns_started": 0,
                }
            )
        )
        return 0
    if args.command == "verify":
        verified = verify_recovery_lock(args.output_dir / "runtime-lock.json")
        print(_canonical_json({"verified": True, "runtime_lock": str(verified)}))
        return 0
    terminal = asyncio.run(
        run_recovery(
            predecessor_root=args.predecessor_root,
            output_dir=args.output_dir,
        )
    )
    print(_canonical_json(terminal))
    return 0 if terminal.get("development_winner_frozen") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
