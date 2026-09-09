#!/usr/bin/env python3
"""Fable-driven first-pass audit of gold claims.

Samples N gold claims (with surrounding transcript context), writes them into
batch files for Fable subagents to judge legit / not-legit, then ingests the
subagents' JSONL verdicts into an ISOLATED machine-verdict table.

The operator's manual verdicts live in table ``gold_claim_flags`` and are never
read or written here; machine verdicts go to ``gold_claim_machine_flags`` in the
same SQLite file so the two never collide.

    python3 -B scripts/pif_gold_fable_audit.py prep --n 1000 --batches 10 --seed 7
    # ... launch Fable agents on batches/*.json, each writing verdicts/*.jsonl ...
    python3 -B scripts/pif_gold_fable_audit.py ingest
    python3 -B scripts/pif_gold_fable_audit.py report
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

PIF_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIF_ROOT))

GOLD_ROOT = PIF_ROOT / "work/signal-desk-rebuild/gold-authoring-v2"
MANIFEST = PIF_ROOT / "work/signal-desk-rebuild/benchmark/partial-manifest.json"
AUDIT_DB = PIF_ROOT / "work/pif-ops/audit/gold_audit.sqlite"
WORK = PIF_ROOT / "work/pif-ops/audit/machine"
BATCH_DIR = WORK / "batches"
VERDICT_DIR = WORK / "verdicts"

MACHINE_VERDICTS = ("legit", "not_legit", "unsure")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _machine_conn() -> sqlite3.Connection:
    AUDIT_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(AUDIT_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gold_claim_machine_flags (
            event_id TEXT NOT NULL,
            window_id TEXT NOT NULL,
            split TEXT NOT NULL,
            verdict TEXT NOT NULL CHECK (verdict IN ('legit','not_legit','unsure')),
            reason TEXT NOT NULL DEFAULT '',
            model TEXT NOT NULL DEFAULT 'fable',
            updated_at TEXT NOT NULL,
            PRIMARY KEY (window_id, event_id)
        )
        """
    )
    # migrate a v1 table (event_id-only PK) in place; event_ids repeat across windows
    pk = [r[1] for r in conn.execute("PRAGMA table_info(gold_claim_machine_flags)") if r[5]]
    if pk == ["event_id"]:
        conn.executescript(
            """
            ALTER TABLE gold_claim_machine_flags RENAME TO _machine_v1;
            CREATE TABLE gold_claim_machine_flags (
                event_id TEXT NOT NULL, window_id TEXT NOT NULL, split TEXT NOT NULL,
                verdict TEXT NOT NULL CHECK (verdict IN ('legit','not_legit','unsure')),
                reason TEXT NOT NULL DEFAULT '', model TEXT NOT NULL DEFAULT 'fable',
                updated_at TEXT NOT NULL, PRIMARY KEY (window_id, event_id)
            );
            INSERT INTO gold_claim_machine_flags SELECT event_id, window_id, split, verdict, reason, model, updated_at FROM _machine_v1;
            DROP TABLE _machine_v1;
            """
        )
    conn.commit()
    return conn


def _load_all_claims() -> list[dict]:
    from research_factory.gold_audit import load_claims, load_window_shows, load_window_texts

    wt = load_window_texts(MANIFEST, project_root=PIF_ROOT)
    ws = load_window_shows(MANIFEST)
    return load_claims(GOLD_ROOT, window_texts=wt, window_shows=ws)


def cmd_prep(args: argparse.Namespace) -> int:
    claims = _load_all_claims()
    # never re-judge a claim that already has a machine verdict
    conn = _machine_conn()
    judged = {(r["window_id"], r["event_id"]) for r in conn.execute("SELECT window_id, event_id FROM gold_claim_machine_flags")}
    conn.close()
    pool = [c for c in claims if (c["window_id"], c["event_id"]) not in judged]
    rng = random.Random(args.seed)
    n = min(args.n, len(pool))
    sample = rng.sample(pool, n)
    print(f"pool {len(pool)} unjudged of {len(claims)} total (already judged: {len(judged)})")

    base = WORK / args.subdir if getattr(args, "subdir", "") else WORK
    batch_dir, verdict_dir = base / "batches", base / "verdicts"
    for d in (batch_dir, verdict_dir):
        d.mkdir(parents=True, exist_ok=True)
    # archive a prior run's batch/verdict files instead of deleting them
    stale = list(batch_dir.glob("*.json")) + list(verdict_dir.glob("*.jsonl"))
    if stale:
        archive = base / "archive" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archive.mkdir(parents=True, exist_ok=True)
        for old in stale:
            old.rename(archive / f"{old.parent.name}__{old.name}")

    per = (n + args.batches - 1) // args.batches
    manifest_rows = []
    batch_paths = []
    for bi in range(args.batches):
        chunk = sample[bi * per:(bi + 1) * per]
        if not chunk:
            break
        payload = []
        for c in chunk:
            payload.append({
                "event_id": c["event_id"],
                "window_id": c["window_id"],
                "split": c["split"],
                "show": c["show"],
                "claim_text": c["claim_text"],
                "speech_act": c["speech_act"],
                "stance": c["stance"],
                "issue_label": c["issue_label"],
                "speaker_id": c["speaker_id"],
                "attribution_type": c["attribution_type"],
                "attribution_confidence": c["attribution_confidence"],
                "evidence_text": c["evidence_text"],
                # reconstructed local transcript with the evidence span inline
                "context": (c["context_before"] + c["context_evidence"] + c["context_after"]),
            })
            manifest_rows.append({"event_id": c["event_id"], "window_id": c["window_id"], "split": c["split"]})
        bp = batch_dir / f"batch_{bi:02d}.json"
        bp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        batch_paths.append(str(bp.relative_to(PIF_ROOT)))
    (base / "sample_manifest.json").write_text(
        json.dumps({"seed": args.seed, "n": len(manifest_rows), "rows": manifest_rows}, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"sampled {len(manifest_rows)} claims into {len(batch_paths)} batches")
    for p in batch_paths:
        print(f"  {p}  ->  verdicts/{Path(p).stem}.jsonl")
    return 0


def _resolve_window(batch: list[dict], event_id: str, line_no: int) -> dict | None:
    """Find the batch row for a verdict line.  Unique event_id wins; else same position."""

    rows = [r for r in batch if r["event_id"] == event_id]
    if len(rows) == 1:
        return rows[0]
    if 0 <= line_no < len(batch) and batch[line_no]["event_id"] == event_id:
        return batch[line_no]
    return rows[0] if rows else None


def cmd_ingest(args: argparse.Namespace) -> int:
    conn = _machine_conn()
    base = WORK / args.subdir if getattr(args, "subdir", "") else WORK
    verdict_dir, batch_dir = base / "verdicts", base / "batches"
    files = sorted(verdict_dir.glob("*.jsonl"))
    # per-batch model attribution (batch stem -> model); falls back to --model
    models_path = base / "models.json"
    model_map = json.loads(models_path.read_text(encoding="utf-8")) if models_path.exists() else {}
    written = bad = unmatched = 0
    for f in files:
        model = str(model_map.get(f.stem, args.model))
        batch_path = batch_dir / f"{f.stem}.json"
        batch = json.loads(batch_path.read_text(encoding="utf-8")) if batch_path.exists() else []
        for line_no, line in enumerate(l for l in f.read_text(encoding="utf-8").splitlines() if l.strip()):
            try:
                row = json.loads(line)
                v = row["verdict"]
                if v not in MACHINE_VERDICTS:
                    raise ValueError(f"bad verdict {v!r}")
                eid = str(row["event_id"])
            except (json.JSONDecodeError, KeyError, ValueError):
                bad += 1
                continue
            m = _resolve_window(batch, eid, line_no)
            if not m:
                unmatched += 1
                continue
            conn.execute(
                """
                INSERT INTO gold_claim_machine_flags (event_id, window_id, split, verdict, reason, model, updated_at)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(window_id, event_id) DO UPDATE SET
                    verdict=excluded.verdict, reason=excluded.reason, model=excluded.model, updated_at=excluded.updated_at
                """,
                (eid, m["window_id"], m["split"], v, str(row.get("reason", ""))[:500], model, _now()),
            )
            written += 1
    conn.commit()
    mp = base / "sample_manifest.json"
    manifest = json.loads(mp.read_text(encoding="utf-8")) if mp.exists() else {"n": written}
    print(f"ingested {written} verdicts from {len(files)} files (malformed {bad}, unmatched {unmatched})")
    print("coverage:", written, "/", manifest["n"], "sampled")
    return 0


def cmd_prep_review(args: argparse.Namespace) -> int:
    """Collect claims flagged by the bulk model (not_legit/unsure) for a small review pass."""

    conn = _machine_conn()
    flagged = {
        (r["window_id"], r["event_id"]): dict(r) for r in conn.execute(
            "SELECT * FROM gold_claim_machine_flags WHERE model=? AND verdict IN ('not_legit','unsure')",
            (args.model,),
        )
    }
    rows = []
    for bp in sorted(BATCH_DIR.glob("*.json")):
        for c in json.loads(bp.read_text(encoding="utf-8")):
            v = flagged.get((c["window_id"], c["event_id"]))
            if v:
                rows.append({**c, "bulk_verdict": v["verdict"], "bulk_reason": v["reason"]})
    review = WORK / "review"
    for d in (review / "batches", review / "verdicts"):
        d.mkdir(parents=True, exist_ok=True)
    for old in list((review / "batches").glob("*.json")) + list((review / "verdicts").glob("*.jsonl")):
        old.unlink()
    per = max(1, args.per_batch)
    paths = []
    for i in range(0, len(rows), per):
        bp = review / "batches" / f"review_{i // per:02d}.json"
        bp.write_text(json.dumps(rows[i:i + per], ensure_ascii=False, indent=1), encoding="utf-8")
        paths.append(bp)
    (review / "models.json").write_text(json.dumps({p.stem: "fable-review" for p in paths}), encoding="utf-8")
    print(f"{len(rows)} {args.model}-flagged claims -> {len(paths)} review batch(es)")
    for bp in paths:
        print(f"  {bp.relative_to(PIF_ROOT)}  ->  review/verdicts/{bp.stem}.jsonl")
    return 0


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]


def cmd_calibrate(args: argparse.Namespace) -> int:
    """Score the bulk model against gold labels on the calibration set; write rubric v2."""

    calib = WORK / args.subdir
    gold: dict[tuple[str, str], dict] = {}
    bulk: dict[tuple[str, str], dict] = {}
    claims: dict[tuple[str, str], dict] = {}
    for bp in sorted((calib / "batches").glob("*.json")):
        batch = json.loads(bp.read_text(encoding="utf-8"))
        for c in batch:
            claims[(c["window_id"], c["event_id"])] = c
        for src, store in (("verdicts", gold), (args.bulk_dir, bulk)):
            vp = calib / src / f"{bp.stem}.jsonl"
            if not vp.exists():
                continue
            for i, row in enumerate(_read_jsonl(vp)):
                m = _resolve_window(batch, str(row["event_id"]), i)
                if m:
                    store[(m["window_id"], m["event_id"])] = row
    both = [k for k in gold if k in bulk]
    labels = ("legit", "not_legit", "unsure")
    conf = {g: {b: 0 for b in labels} for g in labels}
    for k in both:
        conf[gold[k]["verdict"]][bulk[k]["verdict"]] += 1
    n = len(both)
    agree = sum(conf[l][l] for l in labels)
    tp = conf["not_legit"]["not_legit"]
    fp = sum(conf[g]["not_legit"] for g in labels if g != "not_legit")
    fn = sum(conf["not_legit"][b] for b in labels if b != "not_legit")
    print(f"calibration pairs: {n}  exact agreement: {agree}/{n} = {agree / max(n, 1):.1%}")
    print("confusion (rows=gold Fable, cols=bulk):")
    print(f"{'':>12}" + "".join(f"{b:>11}" for b in labels))
    for g in labels:
        print(f"{g:>12}" + "".join(f"{conf[g][b]:>11}" for b in labels))
    prec = tp / max(tp + fp, 1); rec = tp / max(tp + fn, 1)
    print(f"not_legit precision={prec:.1%} recall={rec:.1%}  (tp={tp} fp={fp} fn={fn})")

    # disagreement exemplars where gold is decisive (legit/not_legit) and bulk differs
    over = [k for k in both if gold[k]["verdict"] == "legit" and bulk[k]["verdict"] != "legit"]
    under = [k for k in both if gold[k]["verdict"] == "not_legit" and bulk[k]["verdict"] != "not_legit"]
    # earlier review overturns: bulk not_legit/unsure that the reviewer called legit
    review_over = []
    for rb in sorted((WORK / "review" / "batches").glob("*.json")) if (WORK / "review").exists() else []:
        rbatch = json.loads(rb.read_text(encoding="utf-8"))
        rv = WORK / "review" / "verdicts" / f"{rb.stem}.jsonl"
        if not rv.exists():
            continue
        for i, row in enumerate(_read_jsonl(rv)):
            c = _resolve_window(rbatch, str(row["event_id"]), i)
            if c and row["verdict"] == "legit" and c.get("bulk_verdict") != "legit":
                review_over.append((c, row))

    def _ex(c: dict, note: str) -> list[str]:
        return [f"- **{note}**", f"  - claim: {c.get('claim_text')}",
                f"  - evidence: {str(c.get('evidence_text'))[:220]}", ""]

    v1 = (WORK / "rubric_exemplars.md").read_text(encoding="utf-8")
    out = [v1.rstrip(), "", "## Calibration corrections (gold = Fable; these are where the bulk model drifted)", "",
           f"Calibration on {n} claims: exact agreement {agree / max(n, 1):.0%}; not_legit precision {prec:.0%}, recall {rec:.0%}.",
           "", "### DO NOT flag these — they are legit (bulk over-flagged)", ""]
    for k in over[:args.max_examples]:
        out += _ex(claims[k], f"bulk said {bulk[k]['verdict']} ('{bulk[k].get('reason', '')}') but it is LEGIT: {gold[k].get('reason', '')}")
    for c, row in review_over[:args.max_examples]:
        out += _ex(c, f"bulk said {c.get('bulk_verdict')} ('{c.get('bulk_reason', '')}') but reviewer ruled LEGIT: {row.get('reason', '')}")
    out += ["### DO flag these — they are not_legit (bulk missed them)", ""]
    for k in under[:args.max_examples]:
        out += _ex(claims[k], f"bulk said {bulk[k]['verdict']} but it is NOT_LEGIT: {gold[k].get('reason', '')}")
    dest = WORK / "rubric_exemplars_v2.md"
    dest.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"over-flags={len(over)} under-flags={len(under)} review-overturns={len(review_over)} -> {dest.relative_to(PIF_ROOT)}")
    return 0


def cmd_export_exclusions(args: argparse.Namespace) -> int:
    """Write the exclusion ledger the gold loaders apply (machine not_legit + manual illegitimate)."""

    from research_factory.gold_exclusions import LEDGER_FILENAME, build_exclusion_ledger, load_exclusions

    out = Path(args.output) if args.output else GOLD_ROOT / "artifacts" / LEDGER_FILENAME
    ledger = build_exclusion_ledger(_machine_conn(), now=_now())
    # every entry must point at a real (window_id, event_id) in the gold outputs
    from research_factory.gold_audit import AUDITABLE_SPLITS, iter_window_outputs
    present = {
        (str(w.get("window_id")), str(e.get("event_id")))
        for split in AUDITABLE_SPLITS for w in iter_window_outputs(GOLD_ROOT, split)
        for e in (w.get("events") or [])
    }
    ghosts = [e for e in ledger["entries"] if (e["window_id"], e["event_id"]) not in present]
    if ghosts:
        raise SystemExit(f"refusing to export: {len(ghosts)} exclusion(s) do not exist in gold: "
                         + ", ".join(f"{g['window_id']}/{g['event_id']}" for g in ghosts[:5]))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    keys = load_exclusions(out, required=True)  # round-trip proves the hash
    print(f"wrote {out} ({len(keys)} exclusions; {ledger['counts']})")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    conn = _machine_conn()
    rows = conn.execute("SELECT verdict, COUNT(*) c FROM gold_claim_machine_flags GROUP BY verdict").fetchall()
    print("=== machine verdict summary ===")
    for r in rows:
        print(f"  {r['verdict']:>10}: {r['c']}")
    if args.dump_not_legit:
        print("\n=== not_legit ===")
        for r in conn.execute(
            "SELECT event_id, window_id, split, reason FROM gold_claim_machine_flags "
            "WHERE verdict='not_legit' ORDER BY updated_at DESC"
        ):
            print(f"  {r['event_id']} [{r['split']}] {r['reason']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    pp = sub.add_parser("prep")
    pp.add_argument("--n", type=int, default=1000)
    pp.add_argument("--batches", type=int, default=10)
    pp.add_argument("--seed", type=int, default=7)
    pp.add_argument("--subdir", default="", help="Write batches under <machine>/<subdir>/ (e.g. calib).")
    pp.set_defaults(func=cmd_prep)
    pi = sub.add_parser("ingest")
    pi.add_argument("--model", default="fable", help="Which model produced the verdict files being ingested.")
    pi.add_argument("--subdir", default="", help="Ingest <machine>/<subdir>/verdicts instead (e.g. review).")
    pi.set_defaults(func=cmd_ingest)
    pv = sub.add_parser("prep-review")
    pv.add_argument("--model", default="opus", help="Bulk model whose flagged claims get reviewed.")
    pv.add_argument("--per-batch", type=int, default=150)
    pv.set_defaults(func=cmd_prep_review)
    pc = sub.add_parser("calibrate")
    pc.add_argument("--subdir", default="calib")
    pc.add_argument("--bulk-dir", default="opus_verdicts")
    pc.add_argument("--max-examples", type=int, default=25)
    pc.set_defaults(func=cmd_calibrate)
    pe = sub.add_parser("export-exclusions")
    pe.add_argument("--output", default=None)
    pe.set_defaults(func=cmd_export_exclusions)
    pr = sub.add_parser("report")
    pr.add_argument("--dump-not-legit", action="store_true")
    pr.set_defaults(func=cmd_report)
    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
