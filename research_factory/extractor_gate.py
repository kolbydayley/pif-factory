"""One-shot shadow-only gate for the final Sol extractor promotion attempt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from .util import dumps_json, now_iso, stable_id


SCHEMA_VERSION = "pif_sol_final_gate_v1"
THRESHOLDS: dict[str, tuple[str, float]] = {
    "exact_evidence_rate": ("min", 1.0),
    "terminal_schema_completion": ("min", 0.95),
    "supported_claim_precision": ("min", 0.90),
    "speaker_attribution_precision": ("min", 0.90),
    "dense_medium_recall": ("min", 0.80),
    "no_signal_false_positive_rate": ("max", 0.05),
    "paired_f1_delta": ("min", -0.05),
}


class ExtractorGateError(ValueError):
    pass


def initialize_gate(
    state_path: str | Path,
    *,
    development_manifest: str | Path,
    holdout_manifest: str | Path,
    evaluator_sha256: str,
) -> dict[str, Any]:
    path = Path(state_path).expanduser().resolve()
    if path.exists():
        raise FileExistsError("final Sol gate state already exists")
    dev_path = Path(development_manifest).expanduser().resolve()
    holdout_path = Path(holdout_manifest).expanduser().resolve()
    dev = _object(dev_path)
    holdout = _object(holdout_path)
    if int(dev.get("segment_count", -1)) != 60 or int(dev.get("source_count", -1)) != 22:
        raise ExtractorGateError("development manifest must freeze 60 segments across 22 sources")
    if int(holdout.get("episode_count", 0)) < 1 or int(holdout.get("source_count", 0)) < 1:
        raise ExtractorGateError("holdout manifest must contain at least one untouched source and episode")
    dev_sources = set(map(str, dev.get("source_ids") or []))
    holdout_sources = set(map(str, holdout.get("source_ids") or []))
    dev_episodes = set(map(str, dev.get("episode_ids") or []))
    holdout_episodes = set(map(str, holdout.get("episode_ids") or []))
    if dev_sources & holdout_sources or dev_episodes & holdout_episodes:
        raise ExtractorGateError("holdout source and episode must be untouched")
    evaluator = _sha256(evaluator_sha256, "evaluator_sha256")
    created = now_iso()
    state = {
        "schema_version": SCHEMA_VERSION,
        "id": stable_id(_file_sha256(dev_path), _file_sha256(holdout_path), evaluator, prefix="solgate_"),
        "state": "development",
        "development_manifest": str(dev_path),
        "development_manifest_sha256": _file_sha256(dev_path),
        "holdout_manifest": str(holdout_path),
        "holdout_manifest_sha256": _file_sha256(holdout_path),
        "evaluator_sha256": evaluator,
        "development_attempts": [],
        "holdout_attempt": None,
        "shadow_attempt": None,
        "promotion_eligible": False,
        "production_switch_allowed": False,
        "closed_reason": None,
        "created_at": created,
        "updated_at": created,
    }
    _write_new(path, state)
    _receipt(path, "initialized", state)
    return state


def record_development_attempt(
    state_path: str | Path, metrics_path: str | Path
) -> dict[str, Any]:
    path, state = _state(state_path)
    if state["state"] != "development":
        raise ExtractorGateError("development stage is closed")
    attempts = list(state["development_attempts"])
    if len(attempts) >= 2:
        raise ExtractorGateError("only one development repair is permitted")
    result = _evaluate(metrics_path, state)
    result["attempt"] = len(attempts) + 1
    result["kind"] = "initial" if not attempts else "single_repair"
    attempts.append(result)
    state["development_attempts"] = attempts
    if result["passed"]:
        state["state"] = "holdout_ready"
    elif len(attempts) == 2:
        state["state"] = "closed"
        state["closed_reason"] = "development_failed_after_single_repair"
    state["updated_at"] = now_iso()
    _replace(path, state)
    _receipt(path, f"development-{len(attempts)}", state)
    return state


def record_holdout_attempt(state_path: str | Path, metrics_path: str | Path) -> dict[str, Any]:
    path, state = _state(state_path)
    if state["state"] != "holdout_ready" or state["holdout_attempt"] is not None:
        raise ExtractorGateError("exactly one untouched holdout is permitted after development passes")
    result = _evaluate(metrics_path, state)
    state["holdout_attempt"] = result
    state["state"] = "shadow_ready" if result["passed"] else "closed"
    if not result["passed"]:
        state["closed_reason"] = "untouched_holdout_failed"
    state["updated_at"] = now_iso()
    _replace(path, state)
    _receipt(path, "holdout", state)
    return state


def record_shadow_attempt(state_path: str | Path, metrics_path: str | Path) -> dict[str, Any]:
    path, state = _state(state_path)
    if state["state"] != "shadow_ready" or state["shadow_attempt"] is not None:
        raise ExtractorGateError("exactly one prospective shadow is permitted after holdout")
    metrics = _object(Path(metrics_path).expanduser().resolve())
    if int(metrics.get("episode_count", 0)) < 20:
        raise ExtractorGateError("shadow requires at least 20 new episodes")
    if int(metrics.get("network_count", 0)) < 10:
        raise ExtractorGateError("shadow requires at least ten networks")
    if int(metrics.get("audited_claim_count", 0)) != 200:
        raise ExtractorGateError("shadow audit must be frozen at exactly 200 claims")
    result = _evaluate(metrics_path, state)
    state["shadow_attempt"] = result
    state["promotion_eligible"] = bool(result["passed"])
    state["production_switch_allowed"] = False
    state["state"] = "promotion_eligible" if result["passed"] else "closed"
    if not result["passed"]:
        state["closed_reason"] = "prospective_shadow_failed"
    state["updated_at"] = now_iso()
    _replace(path, state)
    _receipt(path, "shadow", state)
    return state


def read_gate(state_path: str | Path) -> dict[str, Any]:
    return _state(state_path)[1]


def _evaluate(metrics_path: str | Path, state: Mapping[str, Any]) -> dict[str, Any]:
    path = Path(metrics_path).expanduser().resolve()
    metrics = _object(path)
    if _sha256(metrics.get("evaluator_sha256"), "evaluator_sha256") != state["evaluator_sha256"]:
        raise ExtractorGateError("evaluator changed after gate initialization")
    checks: dict[str, bool] = {}
    values: dict[str, float] = {}
    for name, (direction, threshold) in THRESHOLDS.items():
        try:
            value = float(metrics[name])
        except (KeyError, TypeError, ValueError) as exc:
            raise ExtractorGateError(f"metric {name} is required") from exc
        values[name] = value
        checks[name] = value >= threshold if direction == "min" else value <= threshold
    return {
        "metrics_path": str(path),
        "metrics_sha256": _file_sha256(path),
        "evaluator_sha256": state["evaluator_sha256"],
        "values": values,
        "checks": checks,
        "passed": all(checks.values()),
        "recorded_at": now_iso(),
    }


def _state(value: str | Path) -> tuple[Path, dict[str, Any]]:
    path = Path(value).expanduser().resolve()
    state = _object(path)
    if state.get("schema_version") != SCHEMA_VERSION:
        raise ExtractorGateError("final Sol gate state schema is invalid")
    return path, state


def _object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ExtractorGateError(f"JSON artifact must be an object: {path}")
    return value


def _sha256(value: Any, field: str) -> str:
    text = str(value or "").strip().lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ExtractorGateError(f"{field} must be a SHA-256 hex digest")
    return text


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_new(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        handle.write(dumps_json(value) + "\n")
    os.chmod(path, 0o600)


def _replace(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        handle.write(dumps_json(value) + "\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def _receipt(state_path: Path, action: str, state: Mapping[str, Any]) -> None:
    directory = state_path.parent / f"{state_path.stem}-receipts"
    directory.mkdir(parents=True, exist_ok=True)
    sequence = len(list(directory.glob("*.json"))) + 1
    receipt = {
        "schema_version": "pif_sol_final_gate_receipt_v1",
        "sequence": sequence,
        "action": action,
        "gate_id": state["id"],
        "state": state["state"],
        "state_sha256": hashlib.sha256(dumps_json(state).encode()).hexdigest(),
        "promotion_eligible": state["promotion_eligible"],
        "production_switch_allowed": False,
        "created_at": now_iso(),
    }
    _write_new(directory / f"{sequence:03d}-{action}.json", receipt)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pif lab sol-final-gate")
    parser.add_argument("--state", required=True)
    sub = parser.add_subparsers(dest="action", required=True)
    initialize = sub.add_parser("init")
    initialize.add_argument("--development-manifest", required=True)
    initialize.add_argument("--holdout-manifest", required=True)
    initialize.add_argument("--evaluator-sha256", required=True)
    for action in ("development", "holdout", "shadow"):
        command = sub.add_parser(action)
        command.add_argument("--metrics", required=True)
    sub.add_parser("status")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.action == "init":
        result = initialize_gate(
            args.state,
            development_manifest=args.development_manifest,
            holdout_manifest=args.holdout_manifest,
            evaluator_sha256=args.evaluator_sha256,
        )
    elif args.action == "development":
        result = record_development_attempt(args.state, args.metrics)
    elif args.action == "holdout":
        result = record_holdout_attempt(args.state, args.metrics)
    elif args.action == "shadow":
        result = record_shadow_attempt(args.state, args.metrics)
    else:
        result = read_gate(args.state)
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


__all__ = [
    "ExtractorGateError",
    "THRESHOLDS",
    "initialize_gate",
    "read_gate",
    "record_development_attempt",
    "record_holdout_attempt",
    "record_shadow_attempt",
]
