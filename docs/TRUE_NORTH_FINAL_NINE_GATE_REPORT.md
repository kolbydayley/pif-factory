# True-North Final Nine-Gate Development Report

Date: 2026-07-29

This is the consolidated result under measurement contract v6. The selected
eligible baseline is frozen Task 5 plus the contract-normative deterministic
actor-span rule. The incomplete Spark probe is not substituted into this
table.

| Gate | Result | Requirement | Coupled diagnostic | Pass |
|---|---:|---:|---:|:---:|
| Candidate-state macro F1 | 0.863223 | >= 0.900000 | n/a | No |
| Retained-value recall | 0.970711 | >= 0.900000 | n/a | Yes |
| Intrinsic junk escape rate | 0.000000 | <= 0.020000 | n/a | Yes |
| Acceptable atomic-count rate | 0.780172 | >= 0.900000 | n/a | No |
| Claim-text faithfulness | 0.726634 | >= 0.744435 | 0.529081 | No |
| Speaker exactness | 0.991379 | >= 0.954615 | 0.682493 | Yes |
| Reported-actor exactness | 0.780172 | >= 0.735385 | 0.537092 | Yes |
| Hallucination proxy | 0.133065 | <= 0.020000 | n/a | No |
| Schema parse success | 1.000000 | >= 0.990000 | n/a | Yes |

Overall: **5 of 9 gates pass. The stack is not certified to scale.**

Safety accounting remains intact: intrinsic escapes are zero, relational
contamination is zero, the one materialized relational merge is auditable,
and no sealed holdout or production database was opened or mutated.

The unresolved bottleneck is not hidden by the denominator ruling. Atomic
count remains 11.98 percentage points below gate. Matched-pair faithfulness is
1.78 points below gate. Candidate-state macro F1 and the hallucination proxy
also fail independently. The Spark probe supplied no complete eligible
replacement and the decomposition lane is stopped pending Kolby's next
architecture decision.

Eligible baseline rescore SHA-256:
`84b82fc289b8f288297fe682eebc04d94a57634f2f044ba6c4281328dfd5889c`.

