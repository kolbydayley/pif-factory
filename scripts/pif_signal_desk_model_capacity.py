#!/usr/bin/env python3
"""Write and print the sanitized Signal Desk GPT-5.6-sol capacity pulse."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path


PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_model_capacity import (  # noqa: E402
    build_capacity_pulse,
    ensure_capacity_pulse_schema,
    load_capacity_policy,
    write_capacity_pulse,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--policy",
        type=Path,
        default=PIF_ROOT / "config/signal_desk_model_capacity_policy.json",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=PIF_ROOT / "data/factory.sqlite",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PIF_ROOT / "work/pif-ops/model-capacity/signal-desk-gpt-5.6-sol.json",
    )
    args = parser.parse_args()
    policy = load_capacity_policy(args.policy)
    conn = sqlite3.connect(args.database)
    conn.row_factory = sqlite3.Row
    try:
        ensure_capacity_pulse_schema(conn)
        pulse = build_capacity_pulse(conn, policy=policy)
    finally:
        conn.close()
    write_capacity_pulse(args.output, pulse)
    print(json.dumps(pulse, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
