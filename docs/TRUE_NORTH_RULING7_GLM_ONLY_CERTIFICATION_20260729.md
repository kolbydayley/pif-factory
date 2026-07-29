# True-North Ruling 7: GLM-Only Recertification

Date: 2026-07-29

Provider calls: **0**

Sealed holdouts opened: **false**

## Decision

The recommended runtime architecture is now GLM-only plus deterministic
composition. The C2 best-of-lanes stack is retained as a measured
higher-quality variant, but it is no longer recommended because its 21
Codex-lane calls add no passing gate.

Among the two frozen GLM-only decompositions:

| Lane | Atomicity | Faithfulness | Gates | Syntax GLM call floor |
|---|---:|---:|---:|---:|
| Task 5 only + Phase D | 0.776824 | 0.726634 | 7/9 | 42 |
| Split-default + Task-5 fallback + Phase D | **0.802575** | **0.729336** | **7/9** | 56 |

Ruling 7 explicitly asks for the best-scoring frozen GLM-only decomposition,
so the split-default composition is selected. It uses validated split-default
outputs on 17 development packets and frozen Task-5 fallback on the two
historically invalid packets. That fallback is part of the certified
composition and is disclosed.

## Zero-Codex disposition

The prior certified checkpoint included one Spark conflict packet. It could
not simply be relabeled zero-Codex. The replacement runtime rule is:

1. run two independent GLM disposition passes;
2. when value states agree, use the second decision;
3. on disagreement, retain if either GLM pass retains;
4. apply the existing intrinsic chrome rule and hold-resolution rule; and
5. require the unchanged relational merge contamination check.

This gold-blind composition produces:

- macro F1 0.873162;
- retained-value recall 0.974895;
- 6 false rejects;
- zero intrinsic escapes;
- two relational escapes, of which one materially merges and one produces
  zero atomic claims; and
- zero relational contamination.

## Nine gates

| Gate | Result | Gate | Coupled diagnostic | Pass |
|---|---:|---:|---:|:---:|
| Candidate-state macro F1 | 0.873162 | >= 0.790664 | gold A/B raw agreement 0.807018; aspiration 0.900000 | Yes |
| Retained-value recall | 0.974895 | >= 0.900000 | n/a | Yes |
| Intrinsic junk escape rate | 0.000000 | <= 0.020000 | contamination 0 | Yes |
| Acceptable atomic-count rate | 0.802575 | >= 0.900000 | n/a | **No** |
| Claim-text faithfulness | 0.729336 | >= 0.744435 | 0.541858 | **No** |
| Speaker exactness | 0.991561 | >= 0.954615 | 0.701493 | Yes |
| Reported-actor exactness | 0.793249 | >= 0.735385 | 0.561194 | Yes |
| Hallucination rate | 0.040323 | <= 0.093684 | 0.048387 | Yes |
| Schema parse success | 1.000000 | >= 0.990000 | n/a | Yes |

Result: **7/9**, with the same two decomposition-alignment gates failing as in
the C2 stack.

## Runtime lane accounting

For the median Syntax episode:

| Runtime lane | Optimistic calls |
|---|---:|
| Two GLM disposition passes | 28 |
| Frozen Task-5 base decomposition | 14 |
| GLM split-default compound pass | 14 |
| **Total GLM** | **56** |
| **Sol/Codex** | **0** |
| Historical all-Codex baseline | 17 |

The architecture moves all runtime work off the Codex lane by call count:
0 versus 17, a **100% reduction**. It adds 56 GLM calls. A token ratio is not
claimed because the historical baseline has zero token receipts.

The C2 variant reaches 0.810345 atomicity and 0.732059 faithfulness but still
passes only 7/9 while requiring at least 21 Codex calls. It is strictly worse
for the stated objective.

## Phase E measurement limitation

The July 20 snapshot has 17 readable GPT-5.5 output artifacts for Syntax but
zero token-usage receipts. It contains 184 upstream claim rows and zero atomic
claim rows. Therefore true token cost per atomic and downstream
decomposition/canonicalization drift are not recoverable from that snapshot.

A valid future production comparison requires:

1. token receipts for every baseline and hybrid call, separated by provider
   and model;
2. a production or shadow all-Codex run that actually emits atomic claims and
   canonical outputs under the same downstream contract;
3. identical episode and candidate scope;
4. retained-valid-atomic lineage on both sides; and
5. strict read-only production access with all outputs in distinct shadow
   stores.

Cumulative campaign spend remains **309 calls / 3,583,991 known tokens**.
