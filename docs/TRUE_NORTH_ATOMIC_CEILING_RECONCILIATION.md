# Review note: the atomic-count gate ceiling — reconciliation

Date: 2026-07-29
Author: review thread (Claude), responding to the T6 full ceiling audit in
`docs/TRUE_NORTH_GATE_DENOMINATOR_ALIGNMENT.md`.

## The disagreement

The T6 audit states that gold cannot pass the atomic-count gate, citing:

- pass-A vs pass-B exact count agreement: 0.8404 vs the 0.90 gate;
- post-consensus exact agreement: 0.8698, also below 0.90;

and concludes "no extractor can be more consistent about decomposition than
the gold it is scored against," i.e. ~6 of the 12 remaining points are
unreachable.

## Why that comparator is not the gate's analogue

The live gate does not score exact count agreement. It scores
**acceptable-range acceptance**: a prediction passes when its count falls
within the consensus minimum–maximum, which is built from the independently
observed pass-A and pass-B counts (`docs/TRUE_NORTH_BENCHMARK.md`: "any atomic
count between the independently observed minimum and maximum is acceptable").

The honest ceiling for that rule is a third independent annotator scored the
way the extractor is scored. That measurement exists, from the frozen pass
artifacts:

- pass-C count within the A/B acceptable range: **1132/1140 = 99.30%**.

(C adjudicated contested items, which are excluded from strict gates, so the
comparison is not materially circular for the range itself.)

Exact A/B agreement of 84.04% is precisely *why* the acceptance rule is a
range: the range absorbs legitimate decomposition variation between competent
annotators. Using exact agreement as the ceiling compares the gate against a
stricter rule than the gate applies — the mirror image of the denominator
defect T6 correctly identified for the three field gates, where the ceiling
was computed on an easier rule than the gate applies.

## Standing conclusions

1. The atomic-count gate (≥0.90, range-acceptance) remains **fair**: an
   independent annotator reaches 99.30% under the gate's own rule.
2. The extractor's 78.02% (80.60% after split-default) remains a **real
   capability shortfall**, not a metric artifact. The unflagged subset already
   scores 92.13%; the gap is concentrated in compound candidates.
3. T6's core finding about the three field gates (speaker, reported-actor,
   faithfulness measured on a coupled denominator neither gold nor extractor
   can satisfy) is **unaffected** by this note and remains before Kolby as a
   measurement-contract decision.
4. No further decomposition experiments run until Kolby rules on the
   compound-decomposition path (Spark × split-default probe vs strong-lane
   routing vs disclosed-limitation certification).

Nothing in this note changes a gate, a scorer, gold, or any frozen artifact.
