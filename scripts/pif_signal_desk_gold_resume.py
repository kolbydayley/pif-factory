#!/usr/bin/env python3
"""Resume only the user-authorized Signal Desk Gold authoring stages.

This supervisor deliberately owns no A1 calibration or A2 scorer work.  It
uses the frozen staged order in ``signal_desk_gold_runner`` and leaves the
operator-controlled Gold kill/grant receipts to the budget subsystem.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import re
import sys
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any


PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_gold_runner import (  # noqa: E402
    GoldRunnerError,
    run_gold_resume_supervisor,
)
from research_factory.signal_desk_background_admission import (  # noqa: E402
    CURRENT_TURN_FOREGROUND_OVERRIDE_MAX_CONFIGURED_CONCURRENCY,
    CURRENT_TURN_FOREGROUND_OVERRIDE_PROVIDER_CAP,
    current_turn_foreground_override,
)


CURRENT_TURN_OVERRIDE_RECEIPT_VERSION = (
    "pif_signal_desk_gold_current_turn_foreground_override_v1"
)
_SOURCE_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


def _write_json(target: Path, payload: dict[str, Any], *, mode: int = 0o600) -> None:
    """Atomically persist a sanitized supervisor artifact."""

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.chmod(temporary, mode)
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()


def _validate_override_source(source: str | None) -> str:
    """Accept a compact audit label, never arbitrary user content or a token."""

    label = str(source or "").strip()
    if not _SOURCE_LABEL.fullmatch(label):
        raise ValueError(
            "--foreground-override-source must be a 1-128 character audit label "
            "using letters, digits, '.', '_', ':', or '-'"
        )
    return label


def _override_fields(source: str) -> dict[str, object]:
    return {
        "enabled": True,
        "source": source,
        "scope": "current_process_only",
        "configured_concurrency_cap": CURRENT_TURN_FOREGROUND_OVERRIDE_MAX_CONFIGURED_CONCURRENCY,
        "provider_concurrency_cap": CURRENT_TURN_FOREGROUND_OVERRIDE_PROVIDER_CAP,
        "normal_foreground_policy_preserved_outside_scope": True,
    }


def _write_current_turn_override_receipt(*, root: Path, source: str) -> tuple[Path, dict[str, object]]:
    """Record the explicit throttled launch before it can reach a provider."""

    created_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    timestamp = created_at.strftime("%Y%m%dT%H%M%SZ")
    receipt = {
        "schema_version": CURRENT_TURN_OVERRIDE_RECEIPT_VERSION,
        "created_at": created_at.isoformat().replace("+00:00", "Z"),
        "status": "active",
        "provider_calls_started": 0,
        "contains_a1_or_a2": False,
        "foreground_override": _override_fields(source),
    }
    target = (
        root
        / "artifacts"
        / "receipts"
        / f"gold-current-turn-foreground-override-{timestamp}.json"
    )
    _write_json(target, receipt)
    return target, receipt


def _write_checkpoint(root: Path, receipt: dict[str, object]) -> None:
    """Persist only aggregate stage state; never transcript-bearing packets."""

    target = root / "artifacts" / "gold-resume-supervisor.json"
    _write_json(target, receipt)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allow-sealed-holdout",
        action="store_true",
        help="required explicit authorization before any sealed-holdout Gold call",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="maximum adaptive Gold worker pool (fresh lane still begins at 2)",
    )
    parser.add_argument(
        "--allow-current-turn-foreground",
        action="store_true",
        help=(
            "explicitly allow this one process to run Gold despite foreground Codex; "
            "requires a source label and fixes the cap at one provider call"
        ),
    )
    parser.add_argument(
        "--foreground-override-source",
        help="required compact audit label for --allow-current-turn-foreground",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="return after one foreground-safe attempt or deferred checkpoint",
    )
    parser.add_argument("--binary", default="codex")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if not args.allow_sealed_holdout:
        parser.error("--allow-sealed-holdout is required for this authorized staged run")
    if not 2 <= args.concurrency <= 8:
        parser.error("--concurrency must be between 2 and 8")
    if args.foreground_override_source and not args.allow_current_turn_foreground:
        parser.error(
            "--foreground-override-source requires --allow-current-turn-foreground"
        )
    override_source: str | None = None
    if args.allow_current_turn_foreground:
        try:
            override_source = _validate_override_source(args.foreground_override_source)
        except ValueError as exc:
            parser.error(str(exc))
        if args.concurrency != CURRENT_TURN_FOREGROUND_OVERRIDE_MAX_CONFIGURED_CONCURRENCY:
            parser.error(
                "--allow-current-turn-foreground requires "
                f"--concurrency {CURRENT_TURN_FOREGROUND_OVERRIDE_MAX_CONFIGURED_CONCURRENCY}; "
                "the actual provider cap remains 1"
            )

    gold_root = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
    dev_seed_roots = {
        "A": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/semantic-canary-v2/private-development",
        "B": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/j2-j3-v1/private-b",
        "C": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/j2-j3-v1/private-c",
        "AUDIT": PIF_ROOT / "work/signal-desk-rebuild/gold-authoring/j2-j3-v1/private-audit",
    }
    context = (
        current_turn_foreground_override(
            source=override_source,
            configured_concurrency=args.concurrency,
        )
        if override_source is not None
        else nullcontext()
    )
    with context:
        override_receipt_path: Path | None = None
        if override_source is not None:
            override_receipt_path, override_receipt = _write_current_turn_override_receipt(
                root=gold_root,
                source=override_source,
            )
            _write_checkpoint(
                gold_root,
                {
                    "schema_version": "pif_signal_desk_gold_resume_supervision_v1",
                    "status": "foreground_override_authorized",
                    "provider_calls_started": 0,
                    "contains_a1_or_a2": False,
                    "foreground_override": override_receipt["foreground_override"],
                    "foreground_override_receipt": str(override_receipt_path),
                },
            )
        while True:
            try:
                receipt = asyncio.run(
                    run_gold_resume_supervisor(
                        manifest_path=PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json",
                        project_root=PIF_ROOT,
                        gold_root=gold_root,
                        budget_database=PIF_ROOT / "data/factory.sqlite",
                        grant_path=PIF_ROOT / "config/signal_desk_gold_authoring_budget_grant.json",
                        session_root=Path.home() / ".codex/sessions",
                        budget_dir=PIF_ROOT / "work/pif-ops/budget",
                        allow_sealed_holdout=True,
                        seed_roots_by_split={"development": dev_seed_roots},
                        concurrency=args.concurrency,
                        binary=args.binary,
                    )
                )
                if override_source is not None:
                    receipt = {
                        **receipt,
                        "foreground_override": _override_fields(override_source),
                        "foreground_override_receipt": str(override_receipt_path),
                    }
                _write_checkpoint(gold_root, receipt)
                print(json.dumps(receipt, indent=2, sort_keys=True))
                if receipt["status"] == "complete":
                    return 0
                # Only explicitly transient, zero-call states are supervised.
                # A manifest, prerequisite, contract, or semantic failure must
                # remain fail-closed for operator adjudication.
                reason = str(receipt.get("reason") or "")
                transient = (
                    reason == "foreground_codex_active"
                    or reason == "recent_local_input"
                    or reason == "foreground_state_unknown"
                    or reason.startswith("gold_model_capacity_")
                )
                if not transient:
                    failure = {
                        "schema_version": "pif_signal_desk_gold_resume_supervision_v1",
                        "status": "checkpointed_failure",
                        "error": f"unexpected non-transient deferred state: {reason}",
                        "contains_a1_or_a2": False,
                    }
                    if override_source is not None:
                        failure["foreground_override"] = _override_fields(override_source)
                        failure["foreground_override_receipt"] = str(override_receipt_path)
                    _write_checkpoint(gold_root, failure)
                    return 2
                if args.once:
                    return 0
                time.sleep(min(max(1, int(receipt["retry_after_seconds"])), 60))
            except GoldRunnerError as exc:
                failure = {
                    "schema_version": "pif_signal_desk_gold_resume_supervision_v1",
                    "status": "checkpointed_failure",
                    "error": str(exc),
                    "contains_a1_or_a2": False,
                }
                if override_source is not None:
                    failure["foreground_override"] = _override_fields(override_source)
                    failure["foreground_override_receipt"] = str(override_receipt_path)
                _write_checkpoint(gold_root, failure)
                print(json.dumps(failure, indent=2, sort_keys=True), file=sys.stderr)
                # All runner exceptions are non-transient by construction.  The
                # capacity and foreground paths return a deferred receipt above.
                return 2


if __name__ == "__main__":
    raise SystemExit(main())
