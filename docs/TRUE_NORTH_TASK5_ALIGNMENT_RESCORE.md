# True-North Task 5: Atomic-Alignment Conditional Rescore

## Decision

The proposed dual-pass decomposition experiment did **not** run because its
required precondition failed.

Atomic alignment substantially improves speaker exactness and claim-text
faithfulness, but it does not repair reported-actor exactness or the
hallucination proxy. The latter remains dominated by reported-actor false
positives even when atomic count is acceptable. The claim that all four
metrics recover mechanically from atomic alignment is therefore false for the
current scorer and composed Task 5 output.

No model calls were made, no sealed holdout was opened, and the frozen Task 5
run was not modified.

## Rescore population

- Frozen run: `task5-adjudication-20260729-v1`
- Frozen Task 5 acceptance score SHA-256:
  `263d8510fcc6417cc3b8e5dfae1bb5d29e602e76bdd450298360dd04f3db6763`
- Frozen private candidate-score SHA-256:
  `5f5b615b58d2e1580e9ea9b569d03d03a9e7d48bd43c2fb6db3585b204bda2f9`
- Atomic gate population: strictly scoreable consensus-value candidates that
  the system retained as value
- Atomic gate denominator: 232 candidates
- Acceptable atomic count: 181 candidates
- Acceptable atomic-count rate: 181/232 = 78.02%

This population exactly reproduces the reported Task 5 atomic-count metric.
The subset is not the broader 256-candidate semantic-score population.

## Alignment-bound metric check

| Metric | Full Search fold | Atomic-count-acceptable subset | Change |
|---|---:|---:|---:|
| Speaker exactness | 68.25% | 87.38% | +19.13 points |
| Reported-actor exactness | 18.69% | 24.27% | +5.58 points |
| Claim-text faithfulness | 54.73% | 74.49% | +19.75 points |
| Hallucination proxy | 59.27% | 62.43% | **+3.16 points, worse** |

Unmatched gold atomics fall from 101 on the full fold to 24 on the aligned
subset, and unmatched predicted atomics fall from 4 to 1. This verifies that
the subset is much better aligned while the hallucination proxy still fails to
recover.

## Hallucination diagnosis

Among the 181 aligned candidates:

- 113 candidates carry at least one hallucination-severity flag.
- There are 114 hallucination-severity flags in total.
- 113 flags are for `reported_actor`.
- 1 flag is for `atomic_claim`.

Reported-actor positive classification on the aligned subset is:

- True positives: 50
- Predicted positives: 182
- Gold positives: 76
- Precision: 27.47%
- Recall: 65.79%

The hallucination proxy is consequently not just capped by missing atomic
alignment. It is primarily flagging a separate reported-actor overprediction
problem in the adopted/composed priors. Better decomposition alone cannot make
that failure disappear.

## Consequence

The approved dual-pass design was conditional on all four dependent metrics
rising sharply on the aligned subset. That condition is false. Launching the
design would violate the review directive and could improve atomicity while
leaving the actor-driven safety metric structurally broken.

Atomic-count recalibration remains prohibited: the independent pass-C ceiling
shows that the existing 90% gate is reachable. The existing faithfulness
recalibration is also unchanged. A subsequent design requires a new review
decision about whether actor confirmation is a separate stage after all, or
whether the hallucination contract should stop treating adopted
reported-actor priors as an atomic-decomposition failure.
