#!/usr/bin/env python3
"""Build the pif-gold-100-v1 cohort manifest (balanced 10 shows x 10 episodes).

Deterministic selection policy:
1. An episode qualifies when EVERY segment has a ready ai_discourse_v3_1
   label AND the episode has a completed gpt-5.5 v3_1 context run.
2. Shows: retain every original pif-gold-25-v1 show with >= 10 qualified
   episodes, then fill to 10 shows by qualified-episode count (desc),
   source_id as the tiebreak.
3. Episodes per show: the original cohort's episodes first (audited pilot
   continuity), then remaining qualified episodes by published_at desc,
   id as the tiebreak, to exactly 10.

Refuses to emit unless a balanced 10x10 is achievable. --report shows the
composition (including which original shows/episodes fall out) without
writing. Output: config/production_cohort_100_v1.json.
"""
import argparse
import datetime as dt
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

PIF_ROOT = Path.home() / "pif-factory"
DB = PIF_ROOT / "data" / "factory.sqlite"
V1_PATH = PIF_ROOT / "config" / "production_cohort_v1.json"
OUT_PATH = PIF_ROOT / "config" / "production_cohort_100_v1.json"

QUALIFIED_SQL = """
WITH full_eps AS (
  SELECT s.episode_id FROM segments s
  LEFT JOIN labels l ON l.segment_id = s.id
       AND l.label_pack = 'ai_discourse_v3_1' AND l.status = 'ready'
  GROUP BY s.episode_id
  HAVING COUNT(*) = SUM(CASE WHEN l.id IS NOT NULL THEN 1 ELSE 0 END)
     AND COUNT(*) > 0),
ctx AS (
  SELECT DISTINCT episode_id FROM episode_context_runs
  WHERE label_pack = 'ai_discourse_v3_1' AND model = 'gpt-5.5'
    AND status = 'completed')
SELECT e.id, e.source_id, e.published_at
FROM episodes e
JOIN full_eps f ON f.episode_id = e.id
JOIN ctx c ON c.episode_id = e.id
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", action="store_true",
                        help="Print composition without writing the manifest.")
    args = parser.parse_args()

    original = json.loads(V1_PATH.read_text())
    original_by_show = defaultdict(list)
    for ep in original["episodes"]:
        original_by_show[ep["source_id"]].append(ep)

    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    by_show = defaultdict(list)
    for row in conn.execute(QUALIFIED_SQL):
        by_show[row["source_id"]].append(dict(row))
    conn.close()

    eligible_shows = {s for s, eps in by_show.items() if len(eps) >= 10}
    retained = sorted(s for s in original_by_show if s in eligible_shows)
    fillers = sorted((s for s in eligible_shows if s not in original_by_show),
                     key=lambda s: (-len(by_show[s]), s))
    shows = retained + fillers[:10 - len(retained)]
    dropped_original = sorted(s for s in original_by_show
                              if s not in eligible_shows)

    report = {
        "retained_original_shows": retained,
        "added_shows": [s for s in shows if s not in original_by_show],
        "dropped_original_shows": dropped_original,
        "dropped_original_episodes": sum(
            len(original_by_show[s]) for s in dropped_original),
        "eligible_show_pool": {s: len(by_show[s])
                               for s in sorted(eligible_shows)},
    }
    if len(shows) < 10:
        print(json.dumps({"error": "fewer_than_10_eligible_shows", **report},
                         indent=1))
        return 1

    episodes = []
    for show in sorted(shows):
        qualified = {r["id"]: r for r in by_show[show]}
        picked = [
            {"id": ep["id"], "source_id": show,
             "published_at": ep["published_at"]}
            for ep in original_by_show.get(show, []) if ep["id"] in qualified]
        rest = sorted((r for r in by_show[show]
                       if r["id"] not in {p["id"] for p in picked}),
                      key=lambda r: (r["published_at"], r["id"]),
                      reverse=True)
        for r in rest[:10 - len(picked)]:
            picked.append({"id": r["id"], "source_id": show,
                           "published_at": r["published_at"]})
        if len(picked) != 10:
            print(json.dumps({"error": f"show_{show}_underfilled", **report}))
            return 1
        episodes.extend(picked)

    manifest = {
        "schema_version": "pif_production_cohort_v1",
        "cohort_id": "pif-gold-100-v1",
        "content_adapter": "podcast",
        "episode_count": 100,
        "show_count": 10,
        "parent_cohort_id": original["cohort_id"],
        "selection_policy": {
            "baseline": "completed GPT-5.5 ai_discourse_v3_1 full-episode"
                        " extraction with every segment ready-labeled",
            "shape": "balanced 10 shows x 10 episodes, extending the 5x5"
                     " symmetry of pif-gold-25-v1",
            "show_selection": "retain original shows with >=10 qualified"
                              " episodes, fill by qualified-episode count"
                              " (desc, source_id tiebreak)",
            "episode_selection": "original cohort episodes first (audited"
                                 " pilot continuity), then published_at desc"
                                 " (id tiebreak)",
            "generated_by": "scripts/build_production_cohort_100.py",
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(
                timespec="seconds"),
        },
        "composition": report,
        "episodes": episodes,
    }
    print(json.dumps({**report, "total_episodes": len(episodes),
                      "shows": sorted(shows)}, indent=1))
    if args.report:
        return 0
    OUT_PATH.write_text(json.dumps(manifest, indent=1) + "\n")
    print(f"wrote {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
