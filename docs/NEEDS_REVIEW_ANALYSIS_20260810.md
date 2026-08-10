# needs_review Frequency Analysis — 2026-08-10 (Phase 3, step 1)

Read-only analysis of all 8,341 `needs_review = 1` labels in the live DB
(`~/pif-factory/data/factory.sqlite`). No rows modified. This is the analysis
the drain-path ruling was waiting on.

## The contradiction, resolved

The deterministic auditor passes ~everything (mean ≈ 1.0) while ~85% of wave
output carries `needs_review = 1`. The reason distribution shows why — **they
measure different things**:

| Reason family | n | share | What it actually is |
|---|---:|---:|---|
| Speaker attribution inferred from context | 3,238 | 38.8% | **Input-condition advisory** — the transcript lacks explicit speaker turns, so the extractor inferred them and disclosed it |
| Transcript-quality advisories (caption/ASR artifacts, non-speaker) | 2,349 | 28.2% | **Input-condition advisory** — the source transcript is captions/ASR with known defects |
| Segment-overlap advisories (non-speaker) | 724 | 8.7% | **Input-condition advisory** — segment boundaries overlap adjacent content |
| `local_draft_bootstrap` | 417 | 5.0% | Bootstrap-era draft labels (pre-production lineage) |
| `high_impact_signal` (+ variants) | 362 | 4.3% | Flagged *important*, not defective — arguably the opposite of a defect |
| Evidence/entity grounding removals | 179 | 2.1% | **Real recall loss** — generated items removed for failing exact grounding |
| Metric grounding suppressions | 136 | 1.6% | **Real recall loss** — ungrounded metric objects suppressed |
| Legacy metric quarantines | 83 | 1.0% | Known legacy defect class, already quarantined |
| No reason recorded | 80 | 1.0% | Bookkeeping gap |
| Other free-text | remainder | ~9% | Mostly further speaker/transcript phrasing variants |

**~76% of the backlog is input-condition disclosure, not output defect.** The
extractor is honestly documenting that its *input* was degraded (no speaker
turns, ASR captions, overlapping segments). The auditor then correctly verifies
that the *output* is exactly grounded in that input — and passes it. Both are
right; the single `needs_review` bit conflates them.

The real quality-loss families (grounding removals + suppressions) are ~4% of
the backlog: **~400 labels**, a reviewable number.

## Proposed drain semantics (needs Kolby's ruling)

1. **Advisory families** (speaker-inference, transcript-quality, overlap,
   high_impact_signal): reclassify as structured advisory metadata —
   `review_resolution = advisory_acknowledged`, bulk-applied
   deterministically by reason-pattern. They remain queryable; they stop
   counting as "review backlog." Rationale: no human decision exists to make
   per-label; the condition is a property of the source transcript.
2. **Real recall-loss families** (~400): keep flagged; drain via the bounded
   25/day sampled review stage, outcomes `cleared / corrected / quarantined`.
3. **`local_draft_bootstrap` (417)**: pre-production lineage — propose
   `review_resolution = superseded_bootstrap` (they predate the v3.1
   production contract).
4. **No-reason (80)**: backfill from validation flags where recoverable, else
   route to the sampled review stage.
5. Forward-looking: split the flag at write time — `input_advisories[]`
   (structured, never a review gate) vs `needs_review` (output defect only).
   That keeps the honest disclosure without re-creating this backlog.

Under this ruling the actionable backlog drops from 8,341 to ~500, and the
25/day drain stage clears it in ~3 weeks.

## APPLIED — 2026-08-10 (ruling approved by Kolby)

Executed as `research_factory/review_drain.py`,
`method = bulk_deterministic_20260810_kolby_ruling`, append-only rows in
`label_review_resolutions` (labels rows untouched; fully reversible by
deleting rows for that method).

| Outcome | n |
|---|---:|
| `advisory_acknowledged` | 5,885 |
| `superseded_bootstrap` | 417 |
| Kept — real defect families | 402 |
| Kept — no reason recorded | 80 |
| Kept — unclassified free text | **1,557** |
| **Actionable backlog** | **8,341 → 2,039** |

**Correction to the estimate above:** the "~9% other" bucket was 1,557 labels
of which **1,553 are distinct free-prose texts** (sponsor-copy/page-chrome
disclosures, one-off audit suggestions, per-segment observations). They cannot
be bulk-pattern-matched safely and were deliberately kept per the ruling's
"never silently clear unrecognized text." The actionable backlog is therefore
**2,039, not ~500** — at 25/day the sampled drain clears it in ~82 days, or
faster if triage batches obvious advisory prose. Deterministic classification
lives in `review_drain.classify_review_reason`; defect patterns are checked
before advisory patterns so mixed reasons fail toward keeping review.

## Verification queries

All numbers reproducible read-only against `labels.output_json`:
`json_extract(output_json, '$.review_reason')` grouped by the LIKE-patterns in
this doc's families (speaker: `%peaker%`; transcript-quality:
`%transcript%|%caption%|%ASR%` excluding speaker; overlap: `%overlap%`
excluding speaker).
