#!/usr/bin/env python3
"""Adjust the live Gold adaptive limit without stopping or restarting workers."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path


PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

from research_factory.signal_desk_adaptive_concurrency import set_effective_limit  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("limit", type=int, choices=range(2, 9))
    parser.add_argument("--reason", required=True)
    parser.add_argument("--database", type=Path, default=PIF_ROOT / "data/factory.sqlite")
    args = parser.parse_args(argv)
    conn = sqlite3.connect(args.database)
    conn.row_factory = sqlite3.Row
    try:
        status = set_effective_limit(
            conn, lane="gold", effective_limit=args.limit, reason=args.reason
        )
    finally:
        conn.close()
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
