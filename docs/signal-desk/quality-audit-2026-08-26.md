# Signal Desk data-quality audit — 2026-08-26 evening

Kolby reviewed the live dashboard and judged the data "not very valuable."
A skeptical audit of `work/pif-ops/dashboard/data.json` confirmed it and
found five concrete causes. Claude drafted fixes but reverted them uncommitted
on discovering Codex actively editing the same module (commits 8644cb9,
03e6730 landed mid-edit) — this doc hands the findings and fix designs to the
owner instead. Verified numbers below are from the post-promotion data.json.

## Findings (each verified against the data)

1. **Fake "mind changes."** The front-page move (Suleyman,
   `consumer ai value counterclaim`, positive→negative) is same-day,
   same-episode — disagreement within one conversation, not a changed mind.
   *Fix:* a move requires different episodes AND >= 14 days between the
   endpoints.

2. **People ranked by verbosity.** Top card sort ~ position count: hosts
   (Nilay Patel, 1,005 positions) and one-interview guests (Harvey Mason,
   514 positions = one episode) dominate. *Fix:* rank by distinct-episode
   presence (require >= 2 episodes to appear at all), then genuine moves,
   then authority; display "N episodes" not raw position counts.

3. **Topic fragmentation.** 122 of 171 topics are single-episode singletons;
   obvious families unmerged ("agents" / "agentic ai systems" / "agentic
   software development"; "enterprise ai" / "enterprise ai adoption").
   *Fix:* cluster by stemmed-token containment or Jaccard >= 0.6,
   highest-volume member names the cluster. IMPORTANT: bound the pass —
   only the top ~400 topics may found clusters, everything else matches
   against those heads. The naive all-pairs version is quadratic over the
   10k+ raw vocabulary and ran >5 min; bounded runs in ~30s.

4. **Junk bucket ranks #1.** `other` (the packs' catch-all) tops the topic
   list. *Fix:* JUNK_TOPICS = {other, misc, miscellaneous, unknown, none,
   general} excluded everywhere (list, detectors, cards).

5. **Quotes carry transcript artifacts.** Evidence strings interleave
   "Speaker N:" tags and newlines. *Fix:* strip
   `\s*Speaker \d+:\s*` and newlines at aggregation time.
   (Codex's 03e6730 may already cover part of the evidence pipeline —
   merge judgment is the owner's.)

## Sequencing note

Clustering (3) must apply to BOTH topic series and people-entry topics via
one canonical mapper — that is also what fixes the voices-match rate
(3 of the top 40 topic pages had any quotes). Apply it at insertion time
with a pre-pass frequency scan, not as a post-pass.

## Status

- V3 was committed in `12db15f` after the shared edits cleared. It implements
  all five data fixes with the bounded clustering pass.
- The renderer now shows episode presence instead of raw position counts,
  preserves the aggregator's breadth-first people order, and caps the quick
  scan at eight detector cards even though V3 activates more detectors.
- Canonical rebuild and responsive route verification remain the release gate;
  a commit alone is not a published dashboard.
