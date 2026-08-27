# Phase 7 — prediction track records: unblock spec

Owner split (proposed 2026-08-27): the extraction/canonical side is the
Codex agent's lane; the dashboard surface is Claude's. The scoring engine
already exists and needs NO new code: `research_factory/accuracy.py`
(`person_accuracy_history` :91, `person_accuracy_rankings` :301,
`person_contrarian_success` :393 — Brier + categorical, as-of cutoffs,
minimum 10 resolved claims per person).

The lane is starved upstream, not broken. Verified 2026-08-27:

| gap | evidence |
|---|---|
| 1. `forecast_probability` never populated | `SELECT count(*) FROM current_accepted_atomic_claims WHERE forecast_probability IS NOT NULL` → 0; 56 claims have `claim_type='forecast'` with NULL probability. The due-outcomes queue (production_ops.py:2167) filters on this column, so the nightly `due_outcomes` stage always finds zero work and reports healthy_no_work. |
| 2. `canonical_person_id` NULL on all accepted atomic claims | the person↔outcome join in accuracy.py returns 0 rows; only 19 people have position observations at all (100 rows). |
| 3. Existing 25 resolutions are transcript-attestation, not predictions | all from one 2026-07-29 resolver run; "did X explicitly say Y" questions; `due_at` NULL and `brier_score` NULL on all 25. Scoring them as a track record would say everyone is 100% right. |
| 4. `contrarian_snapshots` empty | `person_contrarian_success` has nothing to compare against. |

## Unblock sequence (strict order)

1. **Extractor emits `forecast_probability`** on forecast-type claims:
   extend the claim-extraction prompt/schema in the active label pack —
   "if the speaker states or implies a probability and/or a time horizon
   for a future-resolvable claim, extract `forecast_probability` (0-1)
   and `temporal_horizon`; else null". Re-run over the 56 existing
   forecast claims first (cheap validation batch) before enabling in the
   nightly lane.
2. **Person linkage**: backfill `canonical_person_id` on accepted atomic
   claims by matching claim speaker strings against `canonical_people`
   (1,129 rows / 61 accepted); ambiguous matches go to a review queue,
   never auto-linked.
3. **Real resolution questions**: due-outcomes contracts must ask about
   the WORLD with a concrete `due_at` ("did X ship/happen by <date>"),
   not about the transcript. Draft from claim text; Kolby approves the
   question set (human-in-loop on what "resolved true" means).
4. **Contrarian snapshots**: populate `contrarian_snapshots` at claim
   time so contrarian-success has a field-consensus baseline.

## Dashboard surface (Claude's side, AFTER data exists)

People cards gain a track-record panel (Brier, n resolved, calibration)
that combines with the network-trust chip. Hard rule mirrored from
accuracy.py: show NO accuracy numbers for anyone under 10 resolved
claims — the renderer must respect the same floor.

## Definition of done

`person_accuracy_rankings()` returns a non-empty, floor-respecting
ranking against the live DB, and the scale-gate's
`accepted_outcome_resolutions` count grows from real-world resolutions,
not attestation checks.
