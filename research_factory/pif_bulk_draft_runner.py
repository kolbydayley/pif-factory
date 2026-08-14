"""Shadow bulk draft runner for cheap-lane labeling.

Reads the canonical DB READ-ONLY (unlabeled-segment selection), drafts labels
via cheap-lane adapters, validates event-granularly, and writes to a shadow
sqlite under ``work/bulk-drafts`` — never to canonical tables. A Codex audit
sample runs inside the budget governor's allowance; audit spend is attributed
to the weekly ledger.

Usage:
    python3 -m research_factory.pif_bulk_draft_runner --lane grok --count 100
    python3 -m research_factory.pif_bulk_draft_runner --lane glm --count 100 --audit-rate 0.1

Spec: docs/superpowers/specs/2026-08-13-glm-primary-labeling-budget-governor-design.md
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sqlite3
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

from .cheap_lane_adapters import (
    GLM_JSON_INSTRUCTION,
    draft_glm,
    draft_grok,
    draft_with_omission,
    merge_window_labels,
    validate_label,
    window_text,
)
from .lane_profiles import LANE_PROFILES, OMISSION_SUFFIX
from .pif_budget_governor import WeeklyLedger, allowance, read_weekly_snapshot

PIF_ROOT = Path.home() / "pif-factory"
CANONICAL_DB = PIF_ROOT / "data" / "factory.sqlite"
SHADOW_ROOT = PIF_ROOT / "work" / "bulk-drafts"
SHADOW_DB = SHADOW_ROOT / "drafts.sqlite"
LEDGER_DB = SHADOW_ROOT / "codex_weekly_ledger.sqlite"
CODEX_SESSIONS = Path.home() / ".codex" / "sessions"

LANE_CONCURRENCY = {lane: prof["concurrency"] for lane, prof in LANE_PROFILES.items()}
MIN_AUDIT_RATE = 0.05

PROMPT_V2_PATH = PIF_ROOT / "work" / "loadtest-20260813" / "prompt_v2.py"

JUDGE_PROMPT = """You are a strict quality judge for podcast discourse extraction.

Below: a transcript segment and a CANDIDATE label from a cheap model. Judge:
1. support: fraction of candidate claims fully supported by the transcript (0.0-1.0).
2. junk_rate: fraction of candidate claims that are noise (jokes, chatter, sponsor copy, non-claims) (0.0-1.0).
3. verdict: "pass" if support >= 0.9 and junk_rate <= 0.15, else "fail".

Reply with ONLY a JSON object: {{"support": n, "junk_rate": n, "verdict": "pass"|"fail", "notes": "one sentence"}}

SEGMENT:
{SEG}

CANDIDATE:
{CAND}
"""


def load_prompt_template() -> str:
    namespace: Dict[str, Any] = {}
    exec(PROMPT_V2_PATH.read_text(), namespace)  # noqa: S102 - local trusted file
    return namespace["PROMPT_V2"]


def init_shadow_db() -> None:
    SHADOW_ROOT.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(SHADOW_DB) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS draft_labels (
                   segment_id TEXT NOT NULL,
                   lane TEXT NOT NULL,
                   label_json TEXT NOT NULL,
                   validation_json TEXT NOT NULL,
                   audit_json TEXT,
                   created_at TEXT NOT NULL DEFAULT (datetime('now')),
                   PRIMARY KEY (segment_id, lane)
               )""")


def select_unlabeled_segments(count: int, *, exclude_drafted_lane: str) -> List[Dict[str, Any]]:
    """Unlabeled segments not already drafted by ANY lane. Read-only.

    Lanes partition the backlog: a segment drafted by one lane is not
    re-drafted by another (fleet throughput sums instead of overlapping).
    Cross-lane quality checking comes from the Codex audit sample, not
    duplicate drafting. ``exclude_drafted_lane`` is kept for signature
    stability; the exclusion is global.
    """
    drafted: set = set()
    if SHADOW_DB.exists():
        with sqlite3.connect(SHADOW_DB) as conn:
            drafted = {row[0] for row in conn.execute(
                "SELECT DISTINCT segment_id FROM draft_labels")}
    conn = sqlite3.connect(f"file:{CANONICAL_DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT s.id AS segment_id, s.text_path
           FROM segments s
           LEFT JOIN labels l ON l.segment_id = s.id
           WHERE l.id IS NULL
           ORDER BY s.created_at DESC
           LIMIT ?""", (count + len(drafted),)).fetchall()
    conn.close()
    return [dict(r) for r in rows if r["segment_id"] not in drafted][:count]


def judge_candidate(segment_text: str, label_json: str) -> Dict[str, Any]:
    prompt = JUDGE_PROMPT.format(SEG=segment_text[:24000], CAND=label_json[:12000])
    proc = subprocess.run(
        ["codex", "exec", "-m", "gpt-5.5", "--sandbox", "read-only",
         "--skip-git-repo-check", "--output-last-message", "/dev/stdout", "-"],
        input=prompt, capture_output=True, text=True, timeout=300,
        cwd=str(SHADOW_ROOT))
    if proc.returncode != 0:
        return {"ok": False, "error": proc.stderr[-300:]}
    text = proc.stdout.strip()
    match = re.search(r"\{[^{}]*\}", text.splitlines()[-1] if text else "", re.S)
    if not match:
        return {"ok": False, "error": "no JSON in judge reply"}
    return {"ok": True, **json.loads(match.group(0))}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lane", choices=sorted(LANE_PROFILES), required=True)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--audit-rate", type=float, default=0.10)
    parser.add_argument("--concurrency", type=int, default=None)
    args = parser.parse_args()

    # single-writer lock: a stale or concurrent run must not share the shadow DB
    SHADOW_ROOT.mkdir(parents=True, exist_ok=True)
    lock_path = SHADOW_ROOT / "runner.lock"
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, str(os.getpid()).encode())
        os.close(lock_fd)
    except FileExistsError:
        holder = lock_path.read_text().strip()
        try:
            alive = holder.isdigit() and (os.kill(int(holder), 0) is None)
        except (ProcessLookupError, PermissionError, ValueError):
            alive = False
        if alive:
            print(json.dumps({"aborted": "runner_lock_held", "holder_pid": holder}))
            return
        lock_path.write_text(str(os.getpid()))
    try:
        _run(args)
    finally:
        lock_path.unlink(missing_ok=True)


def _run(args) -> None:
    init_shadow_db()
    template = load_prompt_template()
    concurrency = args.concurrency or LANE_CONCURRENCY[args.lane]
    run_id = f"bulk-{args.lane}-{time.strftime('%Y%m%dT%H%M%S')}"

    ledger = WeeklyLedger(LEDGER_DB)
    snapshot = read_weekly_snapshot(CODEX_SESSIONS)
    budget = allowance(snapshot, ledger, now=int(time.time()))
    if not budget["allowed"]:
        print(json.dumps({"run_id": run_id, "aborted": "codex_budget",
                          "budget": budget}))
        return

    rows = select_unlabeled_segments(args.count, exclude_drafted_lane=args.lane)
    glm_state = SHADOW_ROOT / "glm-state"
    profile = LANE_PROFILES[args.lane]
    template = template + profile["prompt_addendum"]

    def draft_prompt(prompt: str) -> Dict[str, Any]:
        if args.lane == "grok":
            return draft_grok(prompt, reasoning_effort=profile["reasoning_effort"])
        return draft_glm(prompt + GLM_JSON_INSTRUCTION, glm_state,
                         model=profile.get("model", "opencode-go/glm-5.2"))

    def draft_window(window: str) -> Dict[str, Any]:
        """Draft one window, plus the profile's omission-audit passes."""
        return draft_with_omission(draft_prompt, template, window,
                                   profile["omission_passes"], OMISSION_SUFFIX)

    def draft_one(row: Dict[str, Any]) -> Dict[str, Any]:
        text = (PIF_ROOT / row["text_path"]).read_text()
        windows = window_text(text, max_chars=profile["window_chars"])
        labels = []
        elapsed = 0.0
        calls = 0
        for window in windows:
            result = draft_window(window)
            elapsed += result["elapsed"]
            calls += result.get("calls", 1)
            if not result["ok"]:
                return {"ok": False, "error": result["error"], "elapsed": elapsed,
                        "error_class": result.get("error_class", "other"),
                        "calls": calls,
                        "segment_id": row["segment_id"], "segment_text": text}
            labels.append(result["label"])
        label = labels[0] if len(labels) == 1 else merge_window_labels(labels)
        return {"ok": True, "label": label, "elapsed": elapsed,
                "windows": len(windows), "calls": calls,
                "segment_id": row["segment_id"], "segment_text": text}

    drafted, failed, dropped_events = 0, 0, 0
    calls_made = 0
    failure_counts: Dict[str, int] = {}
    stored: List[Dict[str, Any]] = []
    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(draft_one, row) for row in rows]
        for future in as_completed(futures):
            res = future.result()
            calls_made += res.get("calls", 0)
            if not res["ok"]:
                failed += 1
                klass = res.get("error_class", "other")
                failure_counts[klass] = failure_counts.get(klass, 0) + 1
                continue
            validation = validate_label(res["label"], res["segment_text"])
            if not validation["schema_ok"]:
                failed += 1
                continue
            dropped_events += validation["dropped"]
            record = {"segment_id": res["segment_id"],
                      "label": validation["label"],
                      "segment_text": res["segment_text"],
                      "validation": {"dropped": validation["dropped"]}}
            with sqlite3.connect(SHADOW_DB) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO draft_labels"
                    " (segment_id, lane, label_json, validation_json) VALUES (?, ?, ?, ?)",
                    (res["segment_id"], args.lane,
                     json.dumps(validation["label"]),
                     json.dumps(record["validation"])))
            stored.append(record)
            drafted += 1
            if drafted % 10 == 0:
                print(f"[{run_id}] drafted={drafted} failed={failed}", flush=True)
    wall = time.monotonic() - start

    # Codex audit sample inside governor allowance
    audit_n = max(1, int(len(stored) * max(args.audit_rate, MIN_AUDIT_RATE))) if stored else 0
    random.seed(run_id)
    sample = random.sample(stored, min(audit_n, len(stored)))
    before = read_weekly_snapshot(CODEX_SESSIONS)
    audits = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(judge_candidate, rec["segment_text"],
                               json.dumps(rec["label"])): rec for rec in sample}
        for future in as_completed(futures):
            rec = futures[future]
            verdict = future.result()
            audits.append({"segment_id": rec["segment_id"], **verdict})
            with sqlite3.connect(SHADOW_DB) as conn:
                conn.execute(
                    "UPDATE draft_labels SET audit_json = ? WHERE segment_id = ? AND lane = ?",
                    (json.dumps(verdict), rec["segment_id"], args.lane))
    after = read_weekly_snapshot(CODEX_SESSIONS)
    if before and after:
        ledger.record(before, after, run_id=run_id)

    ok_audits = [a for a in audits if a.get("ok")]
    receipt = {
        "run_id": run_id, "lane": args.lane,
        "drafted": drafted, "failed": failed,
        "calls_made": calls_made,
        "failure_counts": failure_counts,
        "dropped_events": dropped_events,
        "wall_seconds": round(wall, 1),
        "throughput_per_hour": round(3600 * drafted / wall, 1) if wall else 0,
        "audit_sample": len(audits),
        "audit_pass_rate": (round(sum(1 for a in ok_audits if a.get("verdict") == "pass")
                                  / len(ok_audits), 3) if ok_audits else None),
        "audit_mean_support": (round(sum(a["support"] for a in ok_audits) / len(ok_audits), 3)
                               if ok_audits else None),
        "budget_before": before, "budget_after": after,
        "budget_at_start": {k: v for k, v in budget.items() if k != "reason"},
    }
    receipt_path = SHADOW_ROOT / f"receipt-{run_id}.json"
    receipt_path.write_text(json.dumps(receipt, indent=1))
    print(json.dumps(receipt, indent=1))


if __name__ == "__main__":
    main()
