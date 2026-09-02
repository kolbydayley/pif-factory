#!/usr/bin/env python3
"""Emit sanitized Gold throughput and capacity telemetry every ten minutes."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from pif_signal_desk_model_capacity import PIF_ROOT

from research_factory.signal_desk_model_capacity import (
    append_capacity_report,
    build_capacity_pulse,
    ensure_capacity_pulse_schema,
    load_capacity_policy,
    write_capacity_pulse,
)


def main() -> int:
    database = PIF_ROOT / "data/factory.sqlite"
    policy_path = PIF_ROOT / "config/signal_desk_model_capacity_policy.json"
    output = PIF_ROOT / "work/pif-ops/model-capacity/signal-desk-gpt-5.6-sol.json"
    history = PIF_ROOT / "work/pif-ops/model-capacity/signal-desk-gpt-5.6-sol-10m.jsonl"
    while True:
        conn = sqlite3.connect(database)
        conn.row_factory = sqlite3.Row
        try:
            ensure_capacity_pulse_schema(conn)
            pulse = build_capacity_pulse(conn, policy=load_capacity_policy(policy_path))
        finally:
            conn.close()
        write_capacity_pulse(output, pulse)
        append_capacity_report(history, pulse)
        print(json.dumps({"measured_at": pulse["measured_at"], **pulse["telemetry"]}), flush=True)
        time.sleep(600)


if __name__ == "__main__":
    raise SystemExit(main())
