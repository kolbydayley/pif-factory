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
for authors, independent auditors, and final approvers. It is not wired into the
live fleet and has no qualification receipt. Existing outputs and gate thresholds
remain unchanged. Its guard rejects incomplete or stale qualification metadata.

## Next bounded work, in order

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
and resolved. Completion hooks remain the continuation mechanism; schedules stay
paused. This document does not authorize a new spend scope or grant extension.
