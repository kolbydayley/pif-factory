#!/usr/bin/env python3
"""Explicit recovery of inspected C/AUDIT offset failures; no model dispatch."""
import argparse
import fcntl
import json
import sqlite3
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from scripts.pif_signal_desk_full_event_qualification import prepare, OUT, BASE
from scripts.pif_signal_desk_gold_merge_provenance import immutable_json
from research_factory.signal_desk_exact_offset_recovery import recover
from research_factory.signal_desk_full_event_prompts import packet
from research_factory.signal_desk_rebuild_dispatch import resurrect_task, acquire_lease, complete_attempt
from research_factory.signal_desk_rubric_reference_packets import digest

WID = "sdw_4117ea30ae4ec3f116ef"


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--execute", action="store_true")
    parser.add_argument("--role", choices=("C", "AUDIT"), default="C"); args = parser.parse_args()
    plan = prepare()
    with (OUT / "runner.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source = json.loads((BASE / f"{plan['source_packets'][WID]}.packet.json").read_text())
        authors = {}
        for role in (("A", "B") if args.role == "C" else ()):
            p = packet(source, role)
            authors[role] = json.loads((OUT / "calls" / WID / role / f"{p['packet_sha256']}.result.json").read_text())
        expected = packet(source, args.role, author_a=authors.get("A"), author_b=authors.get("B"))
        target = OUT / "calls" / WID / args.role; sha = expected["packet_sha256"]
        if json.loads((target / "packet.json").read_text()) != expected: raise ValueError("source/author binding changed")
        original = json.loads((target / f"{sha}.output.json").read_text())
        fixed, receipt = recover(original, source=source["transcript_window"], window_id=WID)
        print(json.dumps(receipt), flush=True)
        if not args.execute: return
        db = sqlite3.connect(target / "dispatch.sqlite"); db.row_factory = sqlite3.Row
        try:
            key = "full-event-qualification-v3:" + sha
            row = db.execute("SELECT a.* FROM signal_desk_rebuild_tasks t JOIN signal_desk_rebuild_attempts a ON a.id=t.current_attempt_id WHERE t.task_key=?", (key,)).fetchone()
            if row is None or row["status"] != "terminal_failed" or row["semantic_failure_detail"] != "evidence not exact at source offsets":
                raise ValueError("not the inspected terminal offset failure; no mutation")
            immutable_json(target / "offset-recovery.json", receipt)
            immutable_json(target / f"{sha}.result.json", fixed)
            resurrect_task(db, task_key=key, resurrected_by="01a04040-77ee-77f2-9d26-a5ca1ae56986",
                reason="Source-verified unique exact excerpt offset-only projection; original response and failure retained; no semantic acceptance")
            lease = acquire_lease(db, lease_owner="explicit-offset-recovery", lease_seconds=1800, task_key_prefix=key)
            complete_attempt(db, attempt_id=lease["current_attempt_id"], lease_owner=lease["lease_owner"],
                lease_generation=lease["lease_generation"], output=fixed)
        finally: db.close()


if __name__ == "__main__": main()
