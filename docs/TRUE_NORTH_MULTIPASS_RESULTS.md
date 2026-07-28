# True-North Multipass Results — 2026-07-28

## Decision

The three-pass GLM architecture is mechanically sound but does **not** improve
the extraction stack enough to justify replication, Spark escalation, or
certification runs in its current form.

The full two-episode Search fold passes **2 of 9** gates. On the exact
60-candidate cohort used by the prior combined-stack comparison, it passes
**3 of 9**, matching the pass count but not the quality of the prior stack.

No sealed episode or production database was opened.

## Frozen configuration

Run:

```text
multipass-20260728-search-v1
```

Configuration:

```text
49258b782fa3b665d96d81d6180b99df7fbce7f95fcdcb3f90f56c11f18bce6c
```

Stages:

1. Disposition-only GLM call.
2. Evidence-only proposition inventory and decomposition.
3. Closed-roster speaker and focal-actor attribution.

Provider:

```text
zai-coding-plan/glm-5.2
```

Budget ceiling:

- 120 calls
- 4,000,000 tokens
- 6 hours wall time

Actual usage:

- 69 paid calls
- 762,398 tokens
- 1,089.677 seconds wall time
- 256 candidates
- 21 segment packets

The 69 calls include five decomposition outputs and six attribution outputs
that initially failed local validation. Valid sibling packets survived and
were resumed without recall. Deterministically repaired checkpoints were not
charged as new calls.

## Full Search-fold result

| Gate | Multipass | Target | Result |
|---|---:|---:|---|
| Candidate-state macro F1 | 79.56% | ≥90% | FAIL |
| Retained-value recall | 91.63% | ≥90% | PASS |
| Junk escape | 11.11% | ≤2% | FAIL |
| Acceptable atomic count | 78.08% | ≥90% | FAIL |
| Claim-text faithfulness proxy | 40.45% | ≥90% | FAIL |
| Speaker exactness | 62.91% | ≥97% | FAIL |
| Reported-actor exactness | 41.76% | ≥95% | FAIL |
| Hallucination rate | 27.82% | ≤2% | FAIL |
| Schema parse success | 100.00% | ≥99% | PASS |

## Exact-cohort comparison

The previous combined-stack score used 60 candidates, of which 52 were
strictly scoreable. Re-scoring multipass on those exact IDs avoids comparing a
full 256-candidate episode run with a smaller stratified sample.

| Gate | Prior single-pass stack | Multipass, same 60 candidates | Change |
|---|---:|---:|---:|
| Candidate-state macro F1 | 97.66% | 95.52% | -2.14 pp |
| Retained-value recall | 100.00% | 97.67% | -2.33 pp |
| Junk escape | 11.11% | 11.11% | unchanged |
| Acceptable atomic count | 65.12% | 66.67% | +1.55 pp |
| Claim-text faithfulness proxy | 53.80% | 40.19% | -13.61 pp |
| Speaker exactness | 64.71% | 58.14% | -6.57 pp |
| Reported-actor exactness | 38.82% | 44.19% | +5.37 pp |
| Hallucination rate | 3.85% | 25.00% | +21.15 pp |
| Schema parse success | 100.00% | 100.00% | unchanged |

## Stage falloff

### Disposition

Across the two full episodes, strict scoring found:

- 219 value candidates retained.
- 20 value candidates incorrectly rejected.
- 8 junk candidates correctly rejected.
- 1 junk candidate escaped.

False-reject reasons were:

- `question_or_setup`: 9
- `fragment`: 7
- `repetition`: 2
- `banter`: 1
- `metadata`: 1

The frozen gold considers many context-resolvable questions, fragments,
repeated constructions, publication forecasts, and explicit skeptical stances
to be useful claims. The strict Stage-A policy therefore conflicts with the
actual gold policy in recurring, non-edge cases.

### Decomposition

Among retained strict-value candidates:

- 171 had acceptable atomic counts.
- 27 under-extracted.
- 21 over-extracted.

The error is bidirectional. Stronger split instructions increase over-splitting,
while stronger cohesion instructions increase under-extraction. The separate
inventory pass did not remove the semantic-boundary problem.

### Speaker

The main speaker error is structural. In the Decoder bundle, segment and
candidate evidence text omit dialogue speaker labels. The frozen upstream
candidate contains a speaker hypothesis, but the multipass packet deliberately
withheld that proposed answer. Without turn attribution in the evidence, Stage
C repeatedly swapped the host and guest:

- Hayden Field predicted where gold has Nilay Patel: 31 aligned cases.
- Nilay Patel predicted where gold has Hayden Field: 5 aligned cases.

Closed-set selection cannot recover information absent from its packet.

### Reported actor

The gold field behaves primarily as a focal-actor label: the organization or
collective whose action, state, or outcome the proposition describes. It is
not consistently the source of reported speech. This differs from both the
field name and the original Stage-C prompt contract. Explicit actor-presence
and focal-actor instructions improved the same-cohort result by 5.37 points,
but accuracy remained 44.19%.

### Faithfulness and hallucination

Even count-correct strict candidates averaged only 0.543 on the lexical
faithfulness proxy; only 4 of 171 reached 0.90. Count-error candidates averaged
0.242. This means decomposition repair alone cannot reach the 0.90 gate.

The multipass claim rewriter also created substantially more unmatched or
low-reference-precision claims, raising the same-cohort hallucination rate from
3.85% to 25%.

## Why Phase 3 is stopped

The planned Spark escalation lane is capped at 20% of candidates. Even under
the impossible best case that every escalated candidate becomes perfect and
every replacement improves the aggregate maximally, the same-cohort ceilings
are approximately:

- Faithfulness: 60.2%
- Speaker exactness: 78.1%
- Reported-actor exactness: 64.2%

All remain far below their 90%, 97%, and 95% gates. Therefore the proposed
20% escalation policy cannot close the measured gaps and would not be a
responsible use of paid calls.

Replication and certification are also stopped: the Search configuration did
not clear its development gates, so opening additional development or sealed
episodes would add cost without satisfying the benchmark's progression rule.

## What the experiment established

- The three-stage CLI harness, schemas, validators, hash binding, lineage,
  checkpoint reuse, and budget enforcement work.
- Smaller calls control aggregate latency despite variable foreign-provider
  response times.
- Separating decomposition produces only a small same-cohort atomic-count gain.
- Attribution quality cannot exceed the information included in the packet.
- The current faithfulness target is not reachable through prompt optimization
  or a 20% tail escalation policy.
- The next intervention must change either the information contract or the
  measurement contract; more tuning of this same three-pass prompt stack is not
  justified by the observed data.
