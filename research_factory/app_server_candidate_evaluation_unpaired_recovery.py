from __future__ import annotations

"""Project an LLM alignment's exact unpaired-ID complement and freeze its score."""

import argparse
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_candidate_evaluation_bundle as evaluator
from . import app_server_candidate_evaluation_projection_recovery as projection
from . import app_server_candidate_evaluation_runner as runner
from . import app_server_judge_v5 as judge
from .app_server_runtime_verifier import ContentHashCache
from .util import now_iso


RECOVERY_VERSION = "pif_candidate_evaluation_unpaired_complement_recovery_v1"
LOCK_VERSION = "pif_candidate_evaluation_unpaired_complement_recovery_lock_v1"
TERMINAL_VERSION = "pif_candidate_evaluation_unpaired_complement_recovery_terminal_v1"
PIPELINE_ROOT = evaluator.PIPELINE_ROOT
DEFAULT_PREDECESSOR_ROOT = projection.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = (
    PIPELINE_ROOT
    / "development-canary-v249-source-complete-columnar-owner-v2-semantic-evaluation-v3-unpaired-complement-recovery"
).resolve()
HASH_CACHE = ContentHashCache()


class CandidateUnpairedRecoveryError(RuntimeError):
    """The deterministic unpaired-ID recovery cannot be frozen safely."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateUnpairedRecoveryError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != payload:
            raise CandidateUnpairedRecoveryError(f"frozen {path.name} drifted")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _record(path: Path) -> dict[str, Any]:
    return HASH_CACHE.record(path.expanduser().resolve())


def _verify_record(record: Mapping[str, Any]) -> None:
    if not HASH_CACHE.verify_record(record):
        raise CandidateUnpairedRecoveryError("frozen direct artifact drifted")


def _digest(records: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json(list(records)).encode("utf-8")).hexdigest()


def project_unpaired_complement(
    output: Mapping[str, Any], alignment_input: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    projected = copy.deepcopy(dict(output))
    expected = {
        str(case["case_id"]): {
            str(witness["witness_id"]) for witness in case["witnesses"]
        }
        for case in alignment_input["cases"]
    }
    changes = []
    for case in projected.get("cases") or []:
        case_id = str(case["case_id"])
        if case_id not in expected:
            raise CandidateUnpairedRecoveryError("adjudication case coverage drifted")
        pairs = list(case.get("alignment_pairs") or [])
        used = {
            str(witness_id)
            for pair in pairs
            for witness_id in (pair["witness_id_1"], pair["witness_id_2"])
        }
        prior = list(case.get("unpaired_witness_ids") or [])
        complement = sorted(expected[case_id] - used)
        if not set(prior) <= set(complement):
            raise CandidateUnpairedRecoveryError("unpaired IDs conflict with LLM pairs")
        added = sorted(set(complement) - set(prior))
        case["unpaired_witness_ids"] = complement
        changes.append(
            {
                "case_id": case_id,
                "prior_unpaired_count": len(prior),
                "projected_unpaired_count": len(complement),
                "added_exact_complement_id_count": len(added),
                "added_id_sha256": hashlib.sha256(
                    _canonical_json(added).encode("utf-8")
                ).hexdigest(),
            }
        )
    errors = judge.validate_neutral_alignment_output(projected, alignment_input)
    if errors:
        raise CandidateUnpairedRecoveryError("unpaired complement remains invalid")
    return projected, {
        "schema_version": RECOVERY_VERSION,
        "changes": changes,
        "added_exact_complement_id_count": sum(
            row["added_exact_complement_id_count"] for row in changes
        ),
        "semantic_fields_changed": False,
        "witness_pairs_changed": False,
        "relations_changed": False,
        "checklist_decisions_changed": False,
        "equivalence_groups_changed": False,
        "normalization_scope": "exact_unpaired_id_complement_only",
        "semantic_model_turns_started": 0,
        "production_mutated": False,
    }


def _predecessor_paths(root: Path) -> dict[str, Path]:
    return {
        "terminal": root / "terminal.json",
        "runtime_lock": root / "runtime-lock.json",
        "base": root / "projected-base-output.private.json",
        "canary": root / "projected-canary-output.private.json",
        "projection_audit": root / "duplicate-span-projection-audit.json",
        "adjudication_bundle": root / "adjudication" / "adjudication-bundle.private.json",
        "adjudication_capacity": root / "adjudication" / "turns" / "observable-disagreement-adjudication" / "capacity.json",
        "adjudication_sidecar": root / "adjudication" / "turns" / "observable-disagreement-adjudication" / "sidecar.json",
        "adjudication_output": root / "adjudication" / "turns" / "observable-disagreement-adjudication" / "output.private.json",
    }


def _validate_predecessor(root: Path) -> dict[str, Any]:
    paths = _predecessor_paths(root)
    for path in paths.values():
        if not path.is_file():
            raise CandidateUnpairedRecoveryError("predecessor artifact is missing")
    projection.verify_recovery_lock(paths["runtime_lock"])
    terminal = _load_json(paths["terminal"], "predecessor terminal")
    if (
        terminal.get("terminal_reason") != "infrastructure_or_judge_attempt_failed"
        or terminal.get("error_class") != "CandidateEvaluationError"
        or terminal.get("accounting_complete") is not True
        or terminal.get("measured_turn_count") != 4
        or terminal.get("unknown_usage_turn_count") != 0
        or terminal.get("production_mutated") is not False
    ):
        raise CandidateUnpairedRecoveryError("predecessor terminal signature drifted")
    adjudication_bundle = _load_json(paths["adjudication_bundle"], "adjudication bundle")
    raw_output = _load_json(paths["adjudication_output"], "adjudication output")
    projected, audit = project_unpaired_complement(
        raw_output, adjudication_bundle["turn"]["value"]
    )
    if audit["added_exact_complement_id_count"] <= 0:
        raise CandidateUnpairedRecoveryError("predecessor has no unpaired coverage defect")
    return {
        "paths": paths,
        "records": [_record(path) for path in paths.values()],
        "terminal": terminal,
        "adjudication_bundle": adjudication_bundle,
        "projected": projected,
        "audit": audit,
    }


def freeze_and_score(
    *,
    predecessor_root: Path = DEFAULT_PREDECESSOR_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    predecessor_root = predecessor_root.expanduser().resolve()
    root = output_dir.expanduser().resolve()
    if (root / "terminal.json").is_file():
        return _load_json(root / "terminal.json", "terminal")
    source = _validate_predecessor(predecessor_root)
    projected_path = root / "projected-adjudication-output.private.json"
    audit_path = root / "unpaired-complement-projection-audit.json"
    _write_immutable(projected_path, source["projected"])
    _write_immutable(audit_path, source["audit"])
    v1_root = projection.DEFAULT_PREDECESSOR_ROOT
    v1_paths = projection._predecessor_paths(v1_root)  # noqa: SLF001
    alignment_bundle = _load_json(v1_paths["alignment_bundle"], "alignment bundle")
    support_bundle = _load_json(v1_paths["support_bundle"], "support bundle")
    support_score = _load_json(v1_paths["support_score"], "support score")
    config = _load_json(v1_paths["config"], "evaluation config")
    score = evaluator.score_alignment(
        base_output=_load_json(source["paths"]["base"], "projected base"),
        canary_output=_load_json(source["paths"]["canary"], "projected canary"),
        alignment_bundle=alignment_bundle,
        support_bundle=support_bundle,
        support_private_score=support_score,
        production_amortized_total_token_ratio=float(
            config["production_amortized_total_token_ratio"]
        ),
        adjudication_bundle=source["adjudication_bundle"],
        adjudication_output=source["projected"],
    )
    score_path = root / "alignment-score.json"
    _write_immutable(score_path, score)
    direct_records = [
        *source["records"],
        *[_record(path) for path in v1_paths.values()],
        _record(projected_path),
        _record(audit_path),
        _record(score_path),
    ]
    runtime_records = [
        _record(Path(__file__).resolve()),
        _record(Path(evaluator.__file__).resolve()),
        _record(Path(projection.__file__).resolve()),
        _record(Path(runner.__file__).resolve()),
        _record(Path(judge.__file__).resolve()),
    ]
    records = [*runtime_records, *direct_records]
    lock_path = root / "runtime-lock.json"
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "predecessor_root": str(predecessor_root),
        "runtime_files": runtime_records,
        "direct_records": direct_records,
        "direct_record_digest": _digest(records),
        "semantic_turn_count": 0,
        "semantic_retry_count": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
    }
    if lock_path.exists():
        lock["frozen_at"] = _load_json(lock_path, "runtime lock")["frozen_at"]
    _write_immutable(lock_path, lock)
    verify_lock(lock_path)
    passed = score["passed"] is True
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "development_winner_frozen" if passed else "development_quality_failed",
        "terminal_reason": (
            "candidate_semantic_quality_and_cost_gates_passed_holdout_authorized"
            if passed
            else "candidate_semantic_quality_gate_not_passed"
        ),
        "usage_status": "complete",
        "accounting_complete": True,
        "measured_turn_count": source["terminal"]["measured_turn_count"],
        "unknown_usage_turn_count": 0,
        "usage": source["terminal"]["usage"],
        "semantic_turn_count": 0,
        "semantic_retry_count": 0,
        "development_winner_frozen": passed,
        "holdout_authorized": passed,
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(lock_path),
        "score": _record(score_path),
        "projection_audit": _record(audit_path),
        "exact_next_action": (
            "open the frozen untouched holdout"
            if passed
            else "freeze this architecture as rejected; do not create a field-repair successor"
        ),
    }
    _write_immutable(root / "terminal.json", terminal)
    return terminal


def verify_lock(path: Path) -> Path:
    lock_path = path.expanduser().resolve()
    lock = _load_json(lock_path, "runtime lock")
    records = [
        *(lock.get("runtime_files") or []),
        *(lock.get("direct_records") or []),
    ]
    if (
        lock.get("schema_version") != LOCK_VERSION
        or lock.get("semantic_turn_count") != 0
        or lock.get("semantic_retry_count") != 0
        or lock.get("production_mutation_allowed") is not False
        or lock.get("holdout_authorized") is not False
        or lock.get("direct_record_digest") != _digest(records)
    ):
        raise CandidateUnpairedRecoveryError("runtime lock drifted")
    for record in records:
        _verify_record(record)
    _validate_predecessor(Path(str(lock["predecessor_root"])))
    return lock_path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "verify"))
    parser.add_argument("--predecessor-root", type=Path, default=DEFAULT_PREDECESSOR_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    if args.command == "verify":
        verified = verify_lock(args.output_dir / "runtime-lock.json")
        print(_canonical_json({"verified": True, "runtime_lock": str(verified)}))
        return 0
    terminal = freeze_and_score(
        predecessor_root=args.predecessor_root,
        output_dir=args.output_dir,
    )
    print(_canonical_json(terminal))
    return 0 if terminal.get("development_winner_frozen") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
