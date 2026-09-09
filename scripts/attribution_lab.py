#!/usr/bin/env python3
"""Speaker-attribution accuracy lab (Kolby 2026-08-28: near-100% or bust).

Gold method (Kolby's design): take episodes with trustworthy NAMED speaker
tags, strip the tags, run the naked text through the production v3_1
attribution pipeline (episode-context stage -> speaker map -> labeling ->
actor_positions), then score every attributed event against the tags we
removed. Exact ground truth, zero cross-transcript alignment.

The lab drives the REAL production surfaces (claim-context / submit-context
/ claim / submit CLIs and their validation) against a sandbox DB
(work/attribution-lab/lab.sqlite) — never the production DB. Executor is
the GLM HTTP lane for iteration speed; the winning configuration gets a
final verification pass through the production executor.

Subcommands: seed | run | score
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
sys.path.insert(0, str(PIF_ROOT))

LAB = PIF_ROOT / "work" / "attribution-lab"
LAB_DB = LAB / "lab.sqlite"
GOLD_DIR = LAB / "gold"
SEG_CHARS = 4000
TAG_RE = re.compile(r"^([A-Z][a-zA-Z'\.\-]+(?: [A-Z][a-zA-Z'\.\-]+){0,3}):\s*$",
                    re.M)


def _cli(*args: str) -> dict:
    r = subprocess.run([sys.executable, "-m", "research_factory",
                        "--db", str(LAB_DB), *args],
                       capture_output=True, text=True, cwd=PIF_ROOT)
    out = r.stdout.strip()
    try:
        return json.loads(out) if out else {"_rc": r.returncode,
                                            "_err": r.stderr[-400:]}
    except json.JSONDecodeError:
        return {"_rc": r.returncode, "_raw": out[-400:],
                "_err": r.stderr[-400:]}


def parse_turns(text: str) -> list[dict]:
    """Split a named-tag transcript into [(speaker, text), ...]."""
    turns = []
    pos = 0
    current = None
    for m in TAG_RE.finditer(text):
        if current is not None:
            turns.append({"speaker": current,
                          "text": text[pos:m.start()].strip()})
        current = m.group(1)
        pos = m.end()
    if current is not None:
        turns.append({"speaker": current, "text": text[pos:].strip()})
    return [t for t in turns if t["text"]]


def strip_and_map(turns: list[dict]) -> tuple[str, list[tuple[int, int, str]]]:
    """Concatenate turn texts; return stripped text + (start,end,speaker)."""
    parts, spans, cursor = [], [], 0
    for t in turns:
        body = t["text"]
        parts.append(body)
        spans.append((cursor, cursor + len(body), t["speaker"]))
        cursor += len(body) + 1  # joining newline
    return "\n".join(parts), spans


def truth_at(spans, lo: int, hi: int) -> str | None:
    hits = {}
    for a, b, sp in spans:
        ov = min(b, hi) - max(a, lo)
        if ov > 0:
            hits[sp] = hits.get(sp, 0) + ov
    return max(hits, key=hits.get) if hits else None


def seed(episode_ids: list[str], *, reset: bool = True) -> None:
    from research_factory.util import stable_id
    LAB.mkdir(parents=True, exist_ok=True)
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    if reset and LAB_DB.exists():
        LAB_DB.unlink()
    _cli("init")
    prod = sqlite3.connect("file:" + str(PIF_ROOT / "data" / "factory.sqlite")
                           + "?mode=ro", uri=True)
    prod.row_factory = sqlite3.Row
    lab = sqlite3.connect(LAB_DB)
    sys.path.insert(0, str(PIF_ROOT))
    from research_factory import db as fdb
    from research_factory.worker import EPISODE_CONTEXT_SCHEMA_VERSION
    for ep_id in episode_ids:
        ep = prod.execute("SELECT * FROM episodes WHERE id=?",
                          (ep_id,)).fetchone()
        src = prod.execute("SELECT * FROM sources WHERE id=?",
                           (ep["source_id"],)).fetchone()
        segs = prod.execute(
            "SELECT s.*, t.id AS tid FROM segments s JOIN transcripts t"
            " ON t.id=s.transcript_id WHERE s.episode_id=?"
            " ORDER BY s.segment_index", (ep_id,)).fetchall()
        full = "\n".join(Path(s["text_path"]).read_text(errors="replace")
                         for s in segs)
        turns = parse_turns(full)
        if len(turns) < 10:
            print(f"skip {ep_id}: only {len(turns)} tagged turns")
            continue
        stripped, spans = strip_and_map(turns)
        (GOLD_DIR / f"{ep_id}.json").write_text(json.dumps(
            {"episode_id": ep_id, "title": ep["title"],
             "spans": spans, "n_turns": len(turns),
             "speakers": sorted({s for _, _, s in spans})}))
        (GOLD_DIR / f"{ep_id}.txt").write_text(stripped)
        lab.execute("INSERT OR IGNORE INTO sources (id, name, rss_url,"
                    " category, policy, transcript_policy, enabled,"
                    " created_at, updated_at) VALUES (?,?,?,?,?,?,1,"
                    " datetime('now'), datetime('now'))",
                    (src["id"], src["name"], src["rss_url"],
                     src["category"], src["policy"],
                     src["transcript_policy"]))
        lab.execute("INSERT INTO episodes (id, source_id, guid, title,"
                    " description, url, published_at, created_at,"
                    " updated_at) VALUES (?,?,?,?,?,?,?,datetime('now'),"
                    " datetime('now'))",
                    (ep_id, ep["source_id"], ep["guid"], ep["title"],
                     ep["description"], ep["url"], ep["published_at"]))
        tid = stable_id(ep_id, "lab", prefix="tr_")
        lab.execute("INSERT INTO transcripts (id, episode_id, source_kind,"
                    " source_url, content_type, raw_text_path,"
                    " raw_text_sha256, status, word_count, created_at,"
                    " updated_at) VALUES (?,?,?,?,?,?,?, 'ready', ?,"
                    " datetime('now'), datetime('now'))",
                    (tid, ep_id, "youtube_captions", "lab://gold",
                     "text/plain", str(GOLD_DIR / f"{ep_id}.txt"),
                     __import__('hashlib').sha256(stripped.encode()).hexdigest(),
                     len(stripped.split())))
        # window at turn-ish boundaries near SEG_CHARS
        cursor, idx = 0, 0
        seg_dir = LAB / "segments"
        seg_dir.mkdir(exist_ok=True)
        while cursor < len(stripped):
            end = min(len(stripped), cursor + SEG_CHARS)
            nl = stripped.rfind("\n", cursor + SEG_CHARS // 2, end)
            if nl > 0 and end < len(stripped):
                end = nl
            chunk = stripped[cursor:end]
            sid = stable_id(ep_id, str(idx), prefix="seg_")
            p = seg_dir / f"{sid}.txt"
            p.write_text(chunk)
            lab.execute(
                "INSERT INTO segments (id, transcript_id, episode_id,"
                " source_id, segment_index, start_char, end_char,"
                " text_path, text_sha256, word_count, created_at) VALUES"
                " (?,?,?,?,?,?,?,?,?,?,datetime('now'))",
                (sid, tid, ep_id, ep["source_id"], idx, cursor, end,
                 str(p),
                 __import__('hashlib').sha256(chunk.encode()).hexdigest(),
                 len(chunk.split())))
            fdb.enqueue_job(lab, lane="podcast", job_type="label_segment",
                            target_id=sid,
                            payload={"label_pack": "ai_discourse_v3_1"},
                            priority=50)
            cursor, idx = end + 1, idx + 1
        fdb.enqueue_job(lab, lane="podcast", job_type="episode_context",
                        target_id=ep_id,
                        payload={
                            "label_pack": "ai_discourse_v3_1",
                            "model": "gpt-5.5",
                            "episode_context_version": (
                                EPISODE_CONTEXT_SCHEMA_VERSION
                            ),
                        },
                        priority=10)
        lab.commit()
        print(f"seeded {ep_id}: {idx} segments, {len(turns)} turns,"
              f" {len({s for _,_,s in spans})} speakers")
    lab.close()


def _exec_glm(prompt: str, timeout: int = 900) -> dict:
    # Grok lane first (qualified above the Codex drafting baseline, own
    # pool, handles the big v3_1 prompts); flash as fallback.
    from research_factory.cheap_lane_adapters import draft_grok,         draft_glm_http
    raw = draft_grok(prompt, timeout=timeout)
    if not (isinstance(raw, dict) and raw.get("label") is not None):
        raw = draft_glm_http(prompt, timeout=timeout,
                             model="glm-5.3-flash", max_429_retries=4)
    if isinstance(raw, dict) and raw.get("label") is not None:
        return raw["label"]
    if isinstance(raw, dict) and raw.get("ok") is False:
        raise RuntimeError(f"model call failed: {raw.get('error')}")
    return raw if isinstance(raw, dict) else {}


def run(model: str, max_jobs: int, workers: int = 6) -> None:
    if workers > 1:
        import threading
        counters = {"n": 0}
        lock = threading.Lock()
        def loop():
            while True:
                with lock:
                    if counters["n"] >= max_jobs:
                        return
                status = _run_one(model, counters, lock)
                if status == "empty":
                    return
        ts = [threading.Thread(target=loop) for _ in range(workers)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        return
    _run_serial(model, max_jobs)


def _run_one(model: str, counters, lock) -> str:
    import time as _t
    c = _cli("claim-context", "--label-pack", "ai_discourse_v3_1",
             "--model", model, "--worker-id", "lab")
    kind = "ctx"
    if not c.get("prompt_path"):
        c = _cli("claim", "--label-pack", "ai_discourse_v3_1",
                 "--model", model, "--worker-id", "lab")
        kind = "label"
    if not c.get("prompt_path"):
        return "empty"
    out = None
    for attempt in range(4):
        try:
            out = _exec_glm(Path(c["prompt_path"]).read_text())
            break
        except Exception as exc:
            if attempt == 3:
                print(kind, c.get("job_id"), "gave up:", str(exc)[:100],
                      flush=True)
                _cli("fail", "--job-id", str(c["job_id"]),
                     "--reason", str(exc)[:150])
                return "error"
            _t.sleep(20 + attempt * 30)
    op = Path(c["output_path"]) if c.get("output_path") else         LAB / f"out_{c['job_id']}.json"
    op.write_text(json.dumps(out))
    sub = "submit-context" if kind == "ctx" else "submit"
    r = _cli(sub, "--job-id", str(c["job_id"]), "--output-json", str(op),
             "--worker-id", "lab")
    ok = "_err" not in r or r.get("status")
    with lock:
        counters["n"] += 1
        n = counters["n"]
    print(f"[{n}] {kind} job {c.get('job_id')} -> "
          f"{'ok' if ok else str(r)[:180]}", flush=True)


def _run_serial(model: str, max_jobs: int) -> None:
    done = 0
    while done < max_jobs:
        c = _cli("claim-context", "--label-pack", "ai_discourse_v3_1",
                 "--model", model, "--worker-id", "lab")
        if c.get("prompt_path"):
            prompt = Path(c["prompt_path"]).read_text()
            out = _exec_glm(prompt)
            op = Path(c["output_path"])
            op.write_text(json.dumps(out))
            r = _cli("submit-context", "--job-id", str(c["job_id"]),
                     "--output-json", str(op), "--worker-id", "lab")
            print("ctx", c.get("episode_context_run_id", "?")[:16],
                  "->", r.get("status") or r)
            done += 1
            continue
        c = _cli("claim", "--label-pack", "ai_discourse_v3_1",
                 "--model", model, "--worker-id", "lab")
        if not c.get("prompt_path"):
            print("no more claimable jobs:", c)
            break
        prompt = Path(c["prompt_path"]).read_text()
        out = _exec_glm(prompt)
        op = Path(c["output_path"]) if c.get("output_path") else \
            LAB / f"out_{c['job_id']}.json"
        op.write_text(json.dumps(out))
        r = _cli("submit", "--job-id", str(c["job_id"]),
                 "--output-json", str(op), "--worker-id", "lab")
        print("label job", c["job_id"], "->", r.get("status") or r)
        done += 1


def _norm_name(n: str) -> str:
    return re.sub(r"[^a-z ]", "", (n or "").lower()).strip()


def name_match(pred: str, true: str) -> bool:
    p, t = _norm_name(pred), _norm_name(true)
    if not p or not t:
        return False
    if p == t:
        return True
    ps, ts = set(p.split()), set(t.split())
    # last-name or full-containment match tolerates ASR/format variants
    return bool(ps & ts and (p.split()[-1] == t.split()[-1]
                             or ps <= ts or ts <= ps))


def is_unknown_actor(actor_name: str | None, actor_type: str | None) -> bool:
    """Return true for explicit model abstentions, including named placeholders."""
    if _norm_name(actor_type or "") == "unknown":
        return True
    return _norm_name(actor_name or "") in (
        "", "unknown", "unknown speaker", "unattributed voice"
    )


def derive_confidence_gate(
    scored: list[tuple[bool, float | None]],
    *,
    target_precision: float = 0.995,
) -> dict:
    """Find the least-discarding observed confidence gate meeting precision."""
    usable = [(correct, float(conf)) for correct, conf in scored
              if conf is not None]
    if not usable:
        return {
            "target_precision": target_precision,
            "threshold": None,
            "kept": 0,
            "discarded": len(scored),
            "discard_fraction": 1.0 if scored else 0.0,
            "precision": None,
        }
    candidates = sorted({conf for _, conf in usable})
    passing = []
    for threshold in candidates:
        kept = [correct for correct, conf in usable if conf >= threshold]
        precision = sum(kept) / len(kept)
        if precision >= target_precision:
            passing.append((len(kept), -threshold, threshold, precision))
    if not passing:
        return {
            "target_precision": target_precision,
            "threshold": None,
            "kept": 0,
            "discarded": len(scored),
            "discard_fraction": 1.0 if scored else 0.0,
            "precision": None,
        }
    kept_count, _, threshold, precision = max(passing)
    discarded = len(scored) - kept_count
    return {
        "target_precision": target_precision,
        "threshold": threshold,
        "kept": kept_count,
        "discarded": discarded,
        "discard_fraction": round(discarded / len(scored), 6),
        "precision": round(precision, 6),
    }


def score() -> None:
    lab = sqlite3.connect(f"file:{LAB_DB}?mode=ro", uri=True)
    lab.row_factory = sqlite3.Row
    rows = lab.execute("""
        SELECT de.actor_name, de.actor_type, de.evidence_text,
               de.evidence_start, de.evidence_end, de.confidence,
               s.episode_id, s.start_char, s.text_path
        FROM discourse_events de JOIN segments s ON s.id = de.segment_id
        WHERE de.evidence_text IS NOT NULL AND de.actor_name IS NOT NULL
    """).fetchall()
    golds = {p.stem: json.loads(p.read_text())
             for p in GOLD_DIR.glob("*.json")}
    total = correct = swaps = 0
    reported = unknown = unlocatable = gold_unknown = 0
    misses = []
    scored = []
    scored_by_episode: dict[str, list[tuple[bool, float | None]]] = {}
    by_episode: dict[str, dict[str, int]] = {}
    for r in rows:
        gold = golds.get(r["episode_id"])
        if not gold:
            continue
        speakers = gold["speakers"]
        pred = (r["actor_name"] or "").strip()
        if is_unknown_actor(pred, r["actor_type"]):
            unknown += 1
            continue
        # locate evidence: stored offsets first, substring fallback
        seg_text = Path(r["text_path"]).read_text(errors="replace")
        lo = None
        if r["evidence_start"] is not None and r["evidence_text"] and                 seg_text[r["evidence_start"]:r["evidence_end"]] ==                 r["evidence_text"]:
            lo = r["start_char"] + r["evidence_start"]
            hi = r["start_char"] + r["evidence_end"]
        else:
            i2 = seg_text.find((r["evidence_text"] or "")[:120])
            if i2 >= 0:
                lo = r["start_char"] + i2
                hi = lo + len(r["evidence_text"])
        if lo is None:
            unlocatable += 1
            continue
        true = truth_at([tuple(x) for x in gold["spans"]], lo, hi)
        if true is None:
            unlocatable += 1
            continue
        if _norm_name(true) in ("unknown", "unknown speaker"):
            gold_unknown += 1
            continue
        episode_counts = by_episode.setdefault(
            r["episode_id"], {"correct": 0, "speaker_swaps": 0}
        )
        pred_is_speaker = any(name_match(pred, sp) for sp in speakers)
        if name_match(pred, true):
            total += 1
            correct += 1
            episode_counts["correct"] += 1
            outcome = (True, r["confidence"])
            scored.append(outcome)
            scored_by_episode.setdefault(r["episode_id"], []).append(outcome)
        elif pred_is_speaker:
            # claims a real speaker said it — but the WRONG one: the
            # dangerous misattribution class
            total += 1
            swaps += 1
            episode_counts["speaker_swaps"] += 1
            outcome = (False, r["confidence"])
            scored.append(outcome)
            scored_by_episode.setdefault(r["episode_id"], []).append(outcome)
            misses.append({"pred": pred, "true": true,
                           "evidence": (r["evidence_text"] or "")[:90],
                           "episode": r["episode_id"],
                           "conf": r["confidence"], "kind": "speaker_swap"})
        else:
            # actor is not any speaker: a reported/quoted actor event —
            # correct pack semantics, not a speaker-attribution claim
            reported += 1
    named = max(total, 1)
    confidence_flag_threshold = 0.70
    gate = derive_confidence_gate(scored)
    episode_accuracy = {
        episode_id: {
            **counts,
            "accuracy": round(
                counts["correct"]
                / max(1, counts["correct"] + counts["speaker_swaps"]),
                4,
            ),
            "confidence_gate_for_99_5_precision": derive_confidence_gate(
                scored_by_episode.get(episode_id, [])
            ),
            "misses_below_confidence_0_70": sum(
                1 for miss in misses
                if miss["episode"] == episode_id
                and miss["conf"] is not None
                and miss["conf"] < confidence_flag_threshold
            ),
            "silent_swaps_at_or_above_0_70": sum(
                1 for miss in misses
                if miss["episode"] == episode_id
                and miss["conf"] is not None
                and miss["conf"] >= confidence_flag_threshold
            ),
        }
        for episode_id, counts in sorted(by_episode.items())
    }
    print(json.dumps({
        "events_scored_as_speaker_claims": total,
        "correct": correct,
        "speaker_swaps_WRONG": swaps,
        "attribution_accuracy": round(correct / named, 4),
        "reported_actor_events_excluded": reported,
        "abstained_unknown": unknown,
        "gold_unknown_excluded": gold_unknown,
        "unlocatable_evidence": unlocatable,
        "by_episode": episode_accuracy,
        "misses_below_confidence_0_70": sum(
            1 for miss in misses
            if miss["conf"] is not None
            and miss["conf"] < confidence_flag_threshold
        ),
        "silent_swaps_at_or_above_0_70": sum(
            1 for miss in misses
            if miss["conf"] is not None
            and miss["conf"] >= confidence_flag_threshold
        ),
        "confidence_gate_for_99_5_precision": gate,
    }, indent=1))
    for m in misses[:15]:
        print(f"  SWAP pred={m['pred']!r} true={m['true']!r} "
              f"conf={m['conf']} :: {m['evidence']!r}")
    (LAB / "misses.json").write_text(json.dumps(misses, indent=1))


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("episode_ids", nargs="+")
    s.add_argument(
        "--append",
        action="store_true",
        help="Add gold episodes to the existing sandbox instead of resetting it.",
    )
    r = sub.add_parser("run")
    r.add_argument("--model", default="gpt-5.5")
    r.add_argument("--max-jobs", type=int, default=260)
    r.add_argument("--workers", type=int, default=3)
    sub.add_parser("score")
    args = ap.parse_args()
    if args.cmd == "seed":
        seed(args.episode_ids, reset=not args.append)
    elif args.cmd == "run":
        run(args.model, args.max_jobs, args.workers)
    else:
        score()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
