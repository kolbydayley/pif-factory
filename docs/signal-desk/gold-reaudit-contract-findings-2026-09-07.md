# Gold re-audit: contract defects and next qualification

Status: diagnostic complete; gold unaccepted. No further full reauthoring or
adjudication cycle is justified by this diagnostic alone.

## Evidence

The repaired candidate preserves 189 dev windows, 6,156 events and the 1,909-event
audit denominator. Its independent audit completed 64 windows and generated 831
disagreements. These are not 831 confirmed errors. The bounded diagnostic sampled
40 cases across seven disagreement-history cohorts, with show diversity within
each cohort. GPT-5.5 returned 24 gold-supported, 11 audit-supported, two both and
three neither. This purposive sample cannot estimate corpus prevalence.

The existing `CONTEXT_FIRST_ADDENDUM` in `signal_desk_gold_prompt_variant.py`
instructs the author to treat explicit assertions as supportive. Later repair
verification and diagnostic prompts explicitly distinguish assertions from
endorsement. This is a documented disagreement in instructions, not merely model
randomness. Do not retroactively change the frozen variant or its scores.

Diagnostic rationales also assign Mike/Scott from question-answer context in some
cases while rejecting analogous flattened-interview assignments elsewhere. Names
present in text are not, by themselves, utterer provenance. Stable labeled speaker
maps and unlabelled conversational inference need distinct treatment.

Some judgments equate factual support with consequence: podcast announcements,
show naming, routine biographical statements, and incidental commercial details
are called consequential without explaining relevance to an industry decision.
This is a publication-relevance defect, not permission to remove benchmark rows.

Surface forms and canonical person names also produce disagreement. Recorded alias
equivalence must be handled by the qualified scorer, not ad-hoc normalization by
each judge. Quoted/mentioned people and utterers remain separate.

## Implemented boundary

`signal_desk_gold_shared_rubric.py` defines a hash-addressed **candidate** contract
for authors, independent auditors, and final approvers. It is connected only to
the isolated 16-window development qualification lane, not the production fleet,
and has no passed qualification receipt. Existing baseline outputs and gate
thresholds remain unchanged. Its guard rejects incomplete or stale qualification
metadata; a receipt hash alone is not evidence that semantic quality passed.

The qualification authoring command runs 48 A/B/C calls with the same rubric.
The reference-review and independent-audit commands require complete authoring
inputs. The independent auditor receives source text, not A/B/C answers. Its
bounded receipt explicitly has `reliability_gate_eligible: false`. All 16 members
remain required even if a worker quarantines one. Completion of these commands
is evidence of execution, not evidence that the rubric or full corpus passed.

Qualification reporting must retain the source-stratum and per-role denominators,
including empty windows and unresolved judgments. Report evidence grounding,
speaker attribution, stance, strategic relevance, and split/merge disagreement
separately; factual support must not mask irrelevant material. Do not substitute
raw scorer disagreement for a source-adjudicated error, or infer absence of
missing claims solely because the supplied Gold-C candidates were supported.

## Next bounded work, in order

### Attribution representation finding during qualification

Read-only inspection of two completed ASR development windows found that the
model-visible text had no turn labels, despite the acquisition stratum being
`asr_diarized`. For `sdw_0ffa09f4e0354c6dd896`, the frozen manifest explicitly
records `alignment: flattened`, raw transcript selection, and the diarized-word-
timestamp acquisition contract. Its 3,800-character window is one line. The
host identifies himself near the end; that does not establish every preceding
utterance's boundary under the current strict contract. A assigned the host,
while B/C abstained. This is diagnostic evidence, not an adjudicated error rate.

Report acquisition stratum and actual window alignment separately. Reference
packets now carry both. Do not assume that diarization metadata reached the
model, or infer names from a show title. If a future representation experiment
reconstructs verified turns from timestamp artifacts, freeze it as a separate
representation family, retain the current baseline, and verify the production
selection contract. Do not silently rewrite these frozen inputs mid-run.

### Remaining sequence

1. Build a blinded development-only qualification set from the frozen source:
   explicit speaker labels, stable speaker maps, unlabeled dialogue, mentions,
   quoted speech, descriptive assertions, explicit endorsements, warnings, and
   supported-but-strategically-incidental events. Include contrasts from multiple
   shows; do not use validation/holdout answers to tune the rubric.
2. Create source-bound expected judgments with SOL-medium and GPT-5.5 adjudication.
   Record explicit rationale and allowed ambiguity; no desired score is evidence.
3. Test **all five roles** against the same hashed rubric. Hold model, source
   representation and schema constant per variant. Measure inter-role agreement,
   fabricated attribution, stance consistency and relevance classification
   separately. Freeze measured qualification gates before selecting a variant.
4. Qualify matching/split-merge/alias handling under A2. A scorer change needs a new
   version and reaggregation; do not compare scores as if the harness were unchanged.
5. Only then choose a bounded gold repair/re-audit run. Preserve the 16 unresolved
   records and original failure receipts. A recheck of tuned dev data is not an
   untouched-holdout evaluation. A1/A2 and the tournament remain blocked.

No repeated voting to turn quarantines into approvals. No gate lowering. No
fresh full-corpus paid campaign until the instruction contradiction is measured
and resolved. Completion hooks remain the primary continuation mechanism; Kolby's
2026-09-07 authorization adds one quiet 15-minute native recovery backstop and an
active task goal. The older supervisor remains paused. See
`gold-completion-control.md`. This document does not authorize a new spend scope
or grant extension.
