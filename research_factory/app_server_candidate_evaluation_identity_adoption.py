from __future__ import annotations

"""Adopt a frozen semantic score for a byte-identical candidate at a new cost."""

import argparse
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import app_server_candidate_evaluation_bundle as evaluator
from . import app_server_configured_experiment as configured
from .app_server_runtime_verifier import (
    ContentHashCache,
    RuntimeVerificationError,
    closure_receipt,
    normalize_record,
    verify_runtime_lock,
)
from .util import now_iso


CONFIG_VERSION = "pif_candidate_evaluation_identity_adoption_config_v1"
LOCK_VERSION = "pif_candidate_evaluation_identity_adoption_runtime_lock_v1"
TERMINAL_VERSION = "pif_candidate_evaluation_identity_adoption_terminal_v1"
LINEAGE_KEYS = (
    "target_candidate",
    "evaluated_candidate",
    "target_candidate_terminal",
    "target_candidate_gate",
    "evaluated_config",
    "evaluated_score",
    "evaluated_terminal",
    "evaluated_projection_audit",
    "source",
    "shared_reference",
    "evaluator_protocol_lock",
    "evaluator_protocol_receipt",
)


class CandidateIdentityAdoptionError(RuntimeError):
    pass


def _load_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateIdentityAdoptionError(f"cannot read {label}") from exc


def _write_immutable(path: Path, value: Any) -> None:
    configured._write_immutable(path, value)  # noqa: SLF001


def _record(path: Path, *, cache: ContentHashCache | None = None) -> dict[str, Any]:
    return (cache or ContentHashCache()).record(path)


def _verify_record(record: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    if not cache.verify_record(record):
        raise CandidateIdentityAdoptionError("frozen artifact record drifted")


def _validate_config(config_path: Path) -> dict[str, Any]:
    value = _load_json(config_path, "identity-adoption config")
    ratio = value.get("production_amortized_total_token_ratio") if isinstance(value, dict) else None
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != CONFIG_VERSION
        or value.get("semantic_turn_count") != 0
        or value.get("production_mutation_allowed") is not False
        or value.get("holdout_authorized") is not False
        or isinstance(ratio, bool)
        or not isinstance(ratio, (int, float))
        or not 0 <= float(ratio) <= evaluator.TOKEN_RATIO_TARGET
    ):
        raise CandidateIdentityAdoptionError("identity-adoption config drifted")
    if Path(str(value.get("output_root") or "")).expanduser().resolve() != config_path.parent.resolve():
        raise CandidateIdentityAdoptionError("identity-adoption output root drifted")
    for key in LINEAGE_KEYS:
        normalize_record(value.get(key) or {})
    return value


def _runtime_files() -> tuple[Path, ...]:
    return tuple(sorted({Path(__file__).resolve(), Path(evaluator.__file__).resolve()}, key=str))


def _same_content(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return (
        left.get("sha256") == right.get("sha256")
        and left.get("size_bytes") == right.get("size_bytes")
    )


def _verify_identity_lineage(config: Mapping[str, Any], *, cache: ContentHashCache) -> None:
    for key in LINEAGE_KEYS:
        _verify_record(config[key], cache=cache)
    if not _same_content(config["target_candidate"], config["evaluated_candidate"]):
        raise CandidateIdentityAdoptionError("candidate content identity does not match")
    evaluated_config = _load_json(
        Path(config["evaluated_config"]["path"]), "evaluated config"
    )
    for key in ("candidate", "source", "shared_reference", "evaluator_protocol_lock", "evaluator_protocol_receipt"):
        expected = config["evaluated_candidate"] if key == "candidate" else config[key]
        if not _same_content(evaluated_config.get(key) or {}, expected):
            raise CandidateIdentityAdoptionError(f"evaluated {key} lineage drifted")
    target_terminal = _load_json(
        Path(config["target_candidate_terminal"]["path"]), "target candidate terminal"
    )
    target_gate = _load_json(
        Path(config["target_candidate_gate"]["path"]), "target candidate gate"
    )
    ratio = float(config["production_amortized_total_token_ratio"])
    if (
        target_terminal.get("support_alignment_authorized") is not True
        or target_terminal.get("production_mutated") is not False
        or target_gate.get("passed") is not True
        or target_gate.get("production_amortized_total_token_ratio") != ratio
    ):
        raise CandidateIdentityAdoptionError("target candidate gate lineage drifted")
    evaluated_terminal = _load_json(
        Path(config["evaluated_terminal"]["path"]), "evaluated terminal"
    )
    if (
        evaluated_terminal.get("terminal_reason") != "candidate_semantic_quality_gate_not_passed"
        or evaluated_terminal.get("accounting_complete") is not True
        or evaluated_terminal.get("unknown_usage_turn_count") != 0
        or evaluated_terminal.get("production_mutated") is not False
    ):
        raise CandidateIdentityAdoptionError("evaluated terminal is not adoptable")


def _adopt_score(score: Mapping[str, Any], ratio: float) -> dict[str, Any]:
    if score.get("schema_version") != evaluator.SCORE_VERSION:
        raise CandidateIdentityAdoptionError("evaluated score schema drifted")
    adopted = copy.deepcopy(dict(score))
    adopted["metrics"]["production_amortized_total_token_ratio"] = ratio
    adopted["checks"]["production_amortized_total_token_ratio_lte_0_28"] = (
        ratio <= evaluator.TOKEN_RATIO_TARGET
    )
    adopted["failed_checks"] = [
        name for name, passed in adopted["checks"].items() if not passed
    ]
    adopted["passed"] = not adopted["failed_checks"]
    adopted["development_winner_frozen"] = adopted["passed"]
    adopted["holdout_authorized"] = adopted["passed"]
    adopted["production_mutated"] = False
    return adopted


def _semantic_projection(score: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(dict(score))
    value.get("metrics", {}).pop("production_amortized_total_token_ratio", None)
    value.get("checks", {}).pop("production_amortized_total_token_ratio_lte_0_28", None)
    value.pop("failed_checks", None)
    value.pop("passed", None)
    value.pop("development_winner_frozen", None)
    value.pop("holdout_authorized", None)
    return value


def freeze_adoption(config_path: Path) -> dict[str, Any]:
    config_path = config_path.expanduser().resolve()
    config = _validate_config(config_path)
    root = config_path.parent
    if (root / "runtime-lock.json").exists():
        return verify_adoption(root)
    cache = ContentHashCache()
    _verify_identity_lineage(config, cache=cache)
    lock = {
        "schema_version": LOCK_VERSION,
        "frozen_at": now_iso(),
        "adoption_id": config["adoption_id"],
        "semantic_turn_count": 0,
        "production_mutation_allowed": False,
        "holdout_authorized": False,
        "runtime_files": [_record(path, cache=cache) for path in _runtime_files()],
        "config": _record(config_path, cache=cache),
        "direct_lineage": [copy.deepcopy(config[key]) for key in LINEAGE_KEYS],
    }
    lock_path = root / "runtime-lock.json"
    configured._write_stable_time(lock_path, lock, "frozen_at")  # noqa: SLF001
    _write_immutable(
        root / "runtime-lock-closure.json",
        closure_receipt(lock_path, cache=ContentHashCache()),
    )
    return verify_adoption(root)


def verify_adoption(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    config = _validate_config(root / "adoption-config.json")
    receipt = _load_json(root / "runtime-lock-closure.json", "runtime closure")
    try:
        verify_runtime_lock(
            root / "runtime-lock.json",
            cache=ContentHashCache(),
            expected_manifest_record=receipt.get("manifest"),
            expected_closure_digest=receipt.get("closure_digest"),
            required_fields={
                "schema_version": LOCK_VERSION,
                "adoption_id": config["adoption_id"],
                "semantic_turn_count": 0,
                "production_mutation_allowed": False,
            },
            required_record_paths=_runtime_files(),
        )
    except RuntimeVerificationError as exc:
        raise CandidateIdentityAdoptionError("runtime lock verification failed") from exc
    _verify_identity_lineage(config, cache=ContentHashCache())
    return {"root": root, "config": config}


def run_adoption(config_path: Path) -> dict[str, Any]:
    root = config_path.expanduser().resolve().parent
    terminal_path = root / "terminal.json"
    if terminal_path.exists():
        return _load_json(terminal_path, "terminal")
    frozen = freeze_adoption(config_path)
    config = frozen["config"]
    source_score = _load_json(Path(config["evaluated_score"]["path"]), "evaluated score")
    adopted = _adopt_score(
        source_score, float(config["production_amortized_total_token_ratio"])
    )
    if _semantic_projection(source_score) != _semantic_projection(adopted):
        raise CandidateIdentityAdoptionError("semantic score changed during cost adoption")
    score_path = root / "adopted-alignment-score.json"
    _write_immutable(score_path, adopted)
    audit_path = root / "identity-adoption-audit.json"
    _write_immutable(
        audit_path,
        {
            "schema_version": "pif_candidate_evaluation_identity_adoption_audit_v1",
            "candidate_sha256": config["target_candidate"]["sha256"],
            "candidate_size_bytes": config["target_candidate"]["size_bytes"],
            "semantic_projection_sha256": hashlib.sha256(
                json.dumps(
                    _semantic_projection(adopted),
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest(),
            "changed_fields": [
                "metrics.production_amortized_total_token_ratio",
                "checks.production_amortized_total_token_ratio_lte_0_28",
                "failed_checks",
                "passed",
                "development_winner_frozen",
                "holdout_authorized",
            ],
            "semantic_decision_changed": False,
            "semantic_turn_count": 0,
            "production_mutated": False,
        },
    )
    terminal = {
        "schema_version": TERMINAL_VERSION,
        "terminal_at": now_iso(),
        "state": "development_winner_frozen" if adopted["passed"] else "development_quality_failed",
        "terminal_reason": (
            "candidate_semantic_quality_and_cost_gates_passed_holdout_authorized"
            if adopted["passed"]
            else "candidate_semantic_quality_gate_not_passed"
        ),
        "semantic_turn_count": 0,
        "semantic_retry_count": 0,
        "usage_status": "reused_complete",
        "accounting_complete": True,
        "development_winner_frozen": adopted["passed"],
        "holdout_authorized": adopted["passed"],
        "production_mutated": False,
        "overall_goal_complete": False,
        "goal_status_required": "active",
        "runtime_lock": _record(root / "runtime-lock.json"),
        "score": _record(score_path),
        "audit": _record(audit_path),
        "source_measured_semantic_usage": _load_json(
            Path(config["evaluated_terminal"]["path"]), "evaluated terminal"
        )["usage"],
        "unknown_usage_turn_count": 0,
        "exact_next_action": (
            "freeze this candidate as the development winner and enter the authorized holdout gate"
            if adopted["passed"]
            else "retain the candidate as rejected; do not replay the evaluator or open holdout"
        ),
    }
    configured._write_stable_time(terminal_path, terminal, "terminal_at")  # noqa: SLF001
    return terminal


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "verify", "run"))
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        result = freeze_adoption(args.config)
        print(json.dumps({"root": str(result["root"]), "semantic_turn_count": 0}, sort_keys=True))
        return 0
    if args.command == "verify":
        result = verify_adoption(args.config.expanduser().resolve().parent)
        print(json.dumps({"root": str(result["root"]), "verified": True}, sort_keys=True))
        return 0
    terminal = run_adoption(args.config)
    print(json.dumps(terminal, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
    return 0 if terminal["development_winner_frozen"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
