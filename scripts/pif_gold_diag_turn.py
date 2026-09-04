#!/usr/bin/env python3
"""Run ONE diagnostic Gold turn for a window and show the provider's error text.

The production runner hashes provider error messages in its sidecars for
privacy, so a window that keeps failing with ``turn_failed / other`` is opaque.
This operator tool reproduces the runner's exact call for one window with the
client's ``synthetic_debug_errors`` flag (which only adds ``message_text`` to
the sidecar; it injects nothing), prints the error text to the terminal, and
deletes its temporary files.  It never writes into the sealed results tree,
and a successful output is discarded (re-author through the runner instead).

Governance: the call is metered into the subscription ledger like any other
started call, and it refuses to run when the provider cap is already full.

    python3 -B scripts/pif_gold_diag_turn.py --split sealed_holdout --turn A \
        --window sdw_29fbb40c7b5e7e07a801 --allow-sealed-holdout
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

MANIFEST = PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json"
BUDGET_DB = PIF_ROOT / "data/factory.sqlite"
PROVIDER_CAP = 8


def validate_request(*, split: str, turn: str, allow_sealed_holdout: bool, live_capacity: int) -> None:
    """Refuse anything the production runner would refuse."""

    if split not in ("development", "validation", "sealed_holdout"):
        raise SystemExit(f"unknown split: {split}")
    if turn not in ("A", "B"):
        raise SystemExit("diagnostic turns are limited to A or B (C needs the A/B outputs; use the runner)")
    if split == "sealed_holdout" and not allow_sealed_holdout:
        raise SystemExit("sealed-holdout diagnostics require --allow-sealed-holdout")
    if live_capacity >= PROVIDER_CAP:
        raise SystemExit(f"provider cap is full ({live_capacity}/{PROVIDER_CAP} live admissions); try later")


def _live_capacity() -> int:
    conn = sqlite3.connect(BUDGET_DB)
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM signal_desk_gold_capacity_leases "
            "WHERE lease_until > strftime('%Y-%m-%dT%H:%M:%f+00:00','now')"
        ).fetchone()[0])
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def _meter(*, window_id: str, tokens: int) -> str:
    from research_factory.signal_desk_gold_budget import EXPECTED_SCOPE, normalize_weekly_reset, read_weekly_snapshot
    from research_factory.subscription_budget import record_usage

    snapshot = read_weekly_snapshot(Path.home() / ".codex/sessions") or {}
    resets = normalize_weekly_reset(int(snapshot.get("resets_at") or 0))
    conn = sqlite3.connect(BUDGET_DB)
    try:
        row = record_usage(
            conn, day=f"weekly:{resets}", provider_lane="codex_subscription",
            lane=EXPECTED_SCOPE, run_id=f"gold-diag:{window_id}", tokens=int(tokens), provider_calls=1,
        )
        conn.commit()
        return row
    finally:
        conn.close()


async def _run(args: argparse.Namespace) -> int:
    from research_factory.codex_app_server import CodexAppServerClient
    from research_factory.signal_desk_gold_runner import SYSTEM_PROMPTS
    from research_factory.signal_desk_rebuild_gold import build_gold_packets
    from research_factory.signal_desk_rebuild_gold_canary import _prompt

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    packets = build_gold_packets(
        manifest, project_root=PIF_ROOT, gold_pass=args.turn, splits=(args.split,),
        allow_sealed=args.split == "sealed_holdout",
    )
    packet = next((p for p in packets if str(p["input"]["window_id"]) == args.window), None)
    if packet is None:
        raise SystemExit(f"window {args.window} is not in split {args.split}")

    tmp = Path(tempfile.mkdtemp(prefix="pif-gold-diag-"))
    os.chmod(tmp, 0o700)
    sidecar = tmp / "sidecar.json"
    output = tmp / "output.json"
    tokens = 0
    try:
        async with CodexAppServerClient(
            command=[args.binary, "app-server", "--stdio", "--strict-config"],
            expected_cli_version="0.147.0",
            synthetic_debug_errors=True,  # include error TEXT in the temp sidecar; injects nothing
        ) as client:
            result = await client.run_ephemeral_structured_turn(
                model=args.model, effort=args.effort,
                base_instructions=SYSTEM_PROMPTS[args.turn], prompt=_prompt(packet),
                output_schema=packet["output_schema"], cwd=PIF_ROOT,
                sidecar_path=sidecar, output_path=output, timeout_seconds=args.timeout,
            )
        tokens = int(result.usage.total_tokens) if result.usage else 0
        report = json.loads(sidecar.read_text(encoding="utf-8")) if sidecar.exists() else {}
        err = report.get("turn_error") or {}
        print(f"status={result.status} status_ok={result.status_ok} error_class={result.error_class} "
              f"wall={result.wall_elapsed_seconds:.0f}s tokens={tokens}")
        print(f"codex_error_info={err.get('codex_error_info')}")
        for key in ("message_text", "additional_details_text"):
            text = err.get(key)
            if text:
                print(f"--- {key} ({err.get(key.replace('_text','_bytes'))} bytes; shown truncated) ---")
                print(text[:args.show_bytes])
        if result.status_ok and result.output is not None:
            print("NOTE: the turn SUCCEEDED here - failure is stochastic; re-author via the runner. Output discarded.")
        return 0 if result.status_ok else 2
    finally:
        if tokens:
            print(f"metered {tokens} tokens into the subscription ledger -> {_meter(window_id=args.window, tokens=tokens)}")
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split", required=True)
    parser.add_argument("--turn", default="A")
    parser.add_argument("--window", required=True)
    parser.add_argument("--allow-sealed-holdout", action="store_true")
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--effort", default="medium")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--binary", default="codex")
    parser.add_argument("--show-bytes", type=int, default=600, help="How much error text to print.")
    args = parser.parse_args(argv)
    validate_request(
        split=args.split, turn=args.turn, allow_sealed_holdout=args.allow_sealed_holdout,
        live_capacity=_live_capacity(),
    )
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
