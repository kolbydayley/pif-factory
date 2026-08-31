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
    draft_codex,
    draft_glm,
    draft_glm_http,
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
DRAFT_LEDGER_DB = SHADOW_ROOT / "codex_draft_ledger.sqlite"
CODEX_SESSIONS = Path.home() / ".codex" / "sessions"

LANE_CONCURRENCY = {lane: prof["concurrency"] for lane, prof in LANE_PROFILES.items()}
MIN_AUDIT_RATE = 0.05
DETERMINISTIC_FAILURE_QUARANTINE_ATTEMPTS = 3

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
    with _shadow_conn() as conn:
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
        conn.execute(
            """CREATE TABLE IF NOT EXISTS draft_failures (
                   segment_id TEXT NOT NULL,
                   lane TEXT NOT NULL,
                   failure_class TEXT NOT NULL,
                   attempts INTEGER NOT NULL DEFAULT 1,
                   last_reason TEXT,
                   first_at TEXT NOT NULL DEFAULT (datetime('now')),
                   last_at TEXT NOT NULL DEFAULT (datetime('now')),
                   quarantined_at TEXT,
                   PRIMARY KEY (segment_id, lane, failure_class)
               )""")


def _shadow_conn() -> sqlite3.Connection:
    """Shadow DB connection safe for concurrent per-lane writers."""
    conn = sqlite3.connect(SHADOW_DB, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# Deterministic hash partitions so lanes can run CONCURRENTLY without ever
# selecting the same segment (selection-time exclusion alone races when two
# lanes start together). 8 slices weighted by lane throughput; rebalance by
# editing this table when a lane exhausts its slice.
N_PARTITIONS = 8
# Rebalanced 2026-08-28 06:30: the z.ai lanes exhausted slices 0-4 (74
# segments left) while ~23.5k sat in the bounded lanes' slices — and grok's
# pool and codex's budget are both spent until their resets. The z.ai lanes
# take over those slices; grok/codex share 5/6 with them (their brief daily
# runs may rarely double-draft a segment; canonical promotion dedups).
# 2026-08-28 15:20 endgame: slices 0-6 are drafted out except flash's
# tail; slice 7 (8k) would take the Go route a month. glm-zai takes it;
# the Go lane becomes episodic on the shared slice (its continuous job is
# retired — 50/hr vs glm-zai's 1,500/hr on identical segments).
# 2026-08-31 final sweep: glm-zai's slices drafted out (192 left) while
# ~2.1k sat in flash's; slice 6 moves to glm-zai so both lanes finish the
# backlog together.
LANE_PARTITIONS = {"glm-zai": (0, 1, 2, 5, 6, 7), "glm-zai-flash": (3, 4),
                   "grok": (5,), "codex": (6,), "glm": (7,)}


def segment_partition(segment_id: str) -> int:
    import hashlib as _hashlib
    return int(_hashlib.md5(segment_id.encode()).hexdigest(), 16) % N_PARTITIONS


def select_unlabeled_segments(count: int, *, exclude_drafted_lane: str,
                              order: str = "newest") -> List[Dict[str, Any]]:
    """Unlabeled segments not already drafted by ANY lane, restricted to the
    requesting lane's hash partitions. Read-only.

    Cross-lane quality checking comes from the Codex audit sample, not
    duplicate drafting. ``exclude_drafted_lane`` names the requesting lane
    and selects its partition slice.
    """
    drafted: set = set()
    quarantined: set = set()
    if SHADOW_DB.exists():
        with _shadow_conn() as conn:
            drafted = {row[0] for row in conn.execute(
                "SELECT DISTINCT segment_id FROM draft_labels")}
            if conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table'"
                " AND name='draft_failures'"
            ).fetchone():
                quarantined = {row[0] for row in conn.execute(
                    "SELECT segment_id FROM draft_failures"
                    " WHERE lane = ? AND quarantined_at IS NOT NULL",
                    (exclude_drafted_lane,))}
    order_sql = {"newest": "s.created_at DESC",
                 "longest": "s.word_count DESC"}[order]
    conn = sqlite3.connect(f"file:{CANONICAL_DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"""SELECT s.id AS segment_id, s.text_path
           FROM segments s
           LEFT JOIN labels l ON l.segment_id = s.id
           WHERE l.id IS NULL
           ORDER BY {order_sql}""").fetchall()
    conn.close()
    partitions = LANE_PARTITIONS.get(exclude_drafted_lane)
    picked: List[Dict[str, Any]] = []
    for row in rows:
        if row["segment_id"] in drafted:
            continue
        if row["segment_id"] in quarantined:
            continue
        if partitions is not None and \
                segment_partition(row["segment_id"]) not in partitions:
            continue
        picked.append(dict(row))
        if len(picked) >= count:
            break
    return picked


def record_draft_failure(
    *,
    segment_id: str,
    lane: str,
    failure_class: str,
    reason: str | None,
    deterministic: bool = False,
) -> bool:
    """Persist a shadow failure and return whether it is now quarantined."""

    threshold = DETERMINISTIC_FAILURE_QUARANTINE_ATTEMPTS
    with _shadow_conn() as conn:
        conn.execute(
            """
            INSERT INTO draft_failures
              (segment_id, lane, failure_class, attempts, last_reason,
               quarantined_at)
            VALUES (?, ?, ?, 1, ?, CASE WHEN ? THEN datetime('now') END)
            ON CONFLICT(segment_id, lane, failure_class) DO UPDATE SET
              attempts = draft_failures.attempts + 1,
              last_reason = excluded.last_reason,
              last_at = datetime('now'),
              quarantined_at = CASE
                WHEN ? AND draft_failures.attempts + 1 >= ?
                THEN COALESCE(draft_failures.quarantined_at, datetime('now'))
                ELSE draft_failures.quarantined_at
              END
            """,
            (
                segment_id,
                lane,
                failure_class,
                (reason or "")[:500],
                int(deterministic and threshold <= 1),
                int(deterministic),
                threshold,
            ),
        )
        row = conn.execute(
            "SELECT quarantined_at FROM draft_failures"
            " WHERE segment_id = ? AND lane = ? AND failure_class = ?",
            (segment_id, lane, failure_class),
        ).fetchone()
    return bool(row and row[0])


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
    parser.add_argument("--transport", choices=["cli", "http"], default=None,
                        help="Override the GLM transport (default: profile).")
    parser.add_argument("--call-timeout", type=int, default=300,
                        help="Per-call GLM timeout seconds. Under heavy worker"
                             " counts z.ai queues requests; a longer timeout"
                             " converts queued-but-successful calls from"
                             " discarded timeouts into drafts.")
    args = parser.parse_args()

    # Per-lane single-writer lock: one run per LANE. Lanes are safe to run
    # concurrently because hash partitioning makes their selections disjoint
    # and SQLite (busy_timeout) serializes row inserts. The legacy global
    # runner.lock is still honored while any old-code run holds it.
    SHADOW_ROOT.mkdir(parents=True, exist_ok=True)
    legacy = SHADOW_ROOT / "runner.lock"
    if legacy.exists():
        holder = legacy.read_text().strip()
        try:
            legacy_alive = holder.isdigit() and (os.kill(int(holder), 0) is None)
        except (ProcessLookupError, PermissionError, ValueError):
            legacy_alive = False
        if legacy_alive:
            print(json.dumps({"aborted": "runner_lock_held",
                              "holder_pid": holder, "legacy": True}))
            return
    lock_path = SHADOW_ROOT / f"runner-{args.lane}.lock"
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

    profile_early = LANE_PROFILES[args.lane]
    is_codex_lane = args.lane == "codex"
    ledger = WeeklyLedger(DRAFT_LEDGER_DB if is_codex_lane else LEDGER_DB)
    snapshot = read_weekly_snapshot(CODEX_SESSIONS)
    budget = allowance(
        snapshot, ledger, now=int(time.time()),
        **({"cap_points": profile_early["draft_budget_cap_points"]}
           if is_codex_lane else {}))
    audits_skipped_budget = False
    if not budget["allowed"]:
        if is_codex_lane:
            # The codex lane spends Codex tokens directly — a hard stop is
            # correct.
            print(json.dumps({"run_id": run_id, "aborted": "codex_budget",
                              "budget": budget}))
            return
        # Cheap lanes cost zero Codex to DRAFT; only their audit sampling
        # spends budget. An exhausted governor must not stop free
        # throughput (2026-08-27: cap hit with 5 days to reset would have
        # stalled the whole GLM fleet). Draft on, skip audits, and flag the
        # receipt so the roll records a skip — never a green built on
        # unaudited work, never a red for a governor artifact.
        audits_skipped_budget = True
        args.audit_rate = 0.0

    rows = select_unlabeled_segments(
        args.count, exclude_drafted_lane=args.lane,
        order=profile_early.get("select_order", "newest"))
    glm_state = SHADOW_ROOT / "glm-state"
    profile = profile_early
    template = template + profile["prompt_addendum"]

    def draft_prompt(prompt: str) -> Dict[str, Any]:
        if args.lane == "grok":
            return draft_grok(prompt, reasoning_effort=profile["reasoning_effort"])
        if is_codex_lane:
            return draft_codex(prompt + GLM_JSON_INSTRUCTION, model=profile["model"])
        if (args.transport or profile.get("transport")) == "http":
            return draft_glm_http(prompt + GLM_JSON_INSTRUCTION,
                                  timeout=args.call_timeout,
                                  model=profile.get("model", "glm-5.2")
                                  .split("/")[-1])
        return draft_glm(prompt + GLM_JSON_INSTRUCTION, glm_state,
                         model=profile.get("model", "opencode-go/glm-5.2"),
                         timeout=args.call_timeout)

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

    drafted, failed, dropped_events, quarantined = 0, 0, 0, 0
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
                record_draft_failure(
                    segment_id=res["segment_id"], lane=args.lane,
                    failure_class=klass, reason=res.get("error"),
                    deterministic=False)
                continue
            validation = validate_label(res["label"], res["segment_text"])
            if not validation["schema_ok"]:
                failed += 1
                failure_counts["schema"] = failure_counts.get("schema", 0) + 1
                if record_draft_failure(
                    segment_id=res["segment_id"], lane=args.lane,
                    failure_class="schema", reason=validation.get("reason"),
                    deterministic=True,
                ):
                    quarantined += 1
                continue
            dropped_events += validation["dropped"]
            record = {"segment_id": res["segment_id"],
                      "label": validation["label"],
                      "segment_text": res["segment_text"],
                      "validation": {"dropped": validation["dropped"]}}
            with _shadow_conn() as conn:
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

    # Codex audit sample inside governor allowance. The codex lane skips
    # audits (the reference model auditing itself is circular); its whole
    # run delta — drafting spend — is attributed to the draft ledger instead.
    if is_codex_lane:
        after_draft = read_weekly_snapshot(CODEX_SESSIONS)
        if snapshot and after_draft:
            ledger.record(snapshot, after_draft, run_id=run_id)
    audit_n = (max(1, int(len(stored) * max(args.audit_rate, MIN_AUDIT_RATE)))
               if stored and not is_codex_lane else 0)
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
            with _shadow_conn() as conn:
                conn.execute(
                    "UPDATE draft_labels SET audit_json = ? WHERE segment_id = ? AND lane = ?",
                    (json.dumps(verdict), rec["segment_id"], args.lane))
    after = read_weekly_snapshot(CODEX_SESSIONS)
    if before and after and not is_codex_lane:
        ledger.record(before, after, run_id=run_id)

    ok_audits = [a for a in audits if a.get("ok")]
    receipt = {
        "run_id": run_id,
        "audits_skipped_budget": audits_skipped_budget, "lane": args.lane,
        "drafted": drafted, "failed": failed,
        "calls_made": calls_made,
        "failure_counts": failure_counts,
        "newly_quarantined": quarantined,
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
