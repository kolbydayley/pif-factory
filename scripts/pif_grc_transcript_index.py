#!/usr/bin/env python3
"""Register GRC.com full-text transcripts for Security Now episodes.

GRC publishes plain-text transcripts at grc.com/sn/sn-{episode}.txt for
every episode back to 2005; corpus titles carry 'SN {number}:'. Discovery
only — fetch belongs to the acquisition session's lanes (its duration
guard covers any future format drift). Usage: --apply to write.
"""
import argparse, json, re, sqlite3, sys
from pathlib import Path
PIF_ROOT = Path.home() / "pif-factory"
sys.path.insert(0, str(PIF_ROOT))
from research_factory.ingest import record_transcript_source_found

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    conn = sqlite3.connect(PIF_ROOT / "data" / "factory.sqlite")
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT e.id, e.title FROM episodes e
           LEFT JOIN transcripts t ON t.episode_id = e.id
           LEFT JOIN transcript_acquisition_status s ON s.episode_id = e.id
           WHERE e.source_id = 'security-now' AND t.id IS NULL
             AND (s.status IS NULL OR s.status NOT LIKE '%found')""").fetchall()
    matched = skipped = 0
    for r in rows:
        m = re.match(r"\s*SN\s*#?\s*(\d{1,4})\b", r["title"] or "")
        if not m:
            skipped += 1
            continue
        url = f"https://www.grc.com/sn/sn-{int(m.group(1))}.txt"
        matched += 1
        if args.apply:
            conn.execute("UPDATE episodes SET verified_transcript_url = ?"
                         " WHERE id = ? AND verified_transcript_url IS NULL",
                         (url, r["id"]))
            record_transcript_source_found(
                conn, episode_id=r["id"],
                source_kind="official_show_transcript",
                result_url=url, worker_id="grc-index")
    if args.apply:
        conn.commit()
    print(json.dumps({"candidates": len(rows), "registered": matched,
                      "no_episode_number": skipped, "applied": args.apply}))

if __name__ == "__main__":
    main()
