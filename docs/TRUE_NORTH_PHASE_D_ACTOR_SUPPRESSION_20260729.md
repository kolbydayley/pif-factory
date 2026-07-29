# True-North Phase D Actor-Emission Suppression

Date: 2026-07-29

Status: **accepted zero-call rule; actor and hallucination pass jointly**

## Rule

The frozen span-enforced composition was swept with one deterministic risk
score. At runtime it reads only:

- the proposed `reported_actor`;
- the atomic `claim_text`; and
- the atomic `evidence_text`.

Risk rises when the actor is not fully named in the claim, has no proper-name
surface, is a generic collective, or resembles a product/document/artifact.
Risk falls when the evidence begins with the actor. Gold labels are never an
input to the rule; they are used only to score the frozen offline frontier.

The adopted threshold is `risk >= 2`. It suppresses 52 of 124 span-valid actor
emissions.

## Frontier

| Threshold | Suppressed | Actor exactness | Hallucination | Joint pass |
|---:|---:|---:|---:|:---:|
| -1 | 124 | 0.621094 | 0.012097 | No |
| 0 | 99 | 0.699219 | 0.016129 | No |
| 1 | 75 | 0.750000 | 0.028226 | Yes |
| **2** | **52** | **0.785156** | **0.044355** | **Yes — adopted** |
| 3 | 32 | 0.785156 | 0.076613 | Yes |
| 4 | 16 | 0.781250 | 0.104839 | No |
| 5 | 11 | 0.785156 | 0.108871 | No |
| 6 | 3 | 0.785156 | 0.120968 | No |
| 7 | 0 | 0.777344 | 0.129032 | No |

Threshold 2 dominates threshold 3 on hallucination at the same measured actor
exactness. Relative to the coherent no-suppression baseline, actor exactness
improves by 0.78 points while hallucination falls by 8.47 points.

## Updated nine-gate table

| Gate | Result | Gate | Pass |
|---|---:|---:|:---:|
| Candidate-state macro F1 | 0.863223 | >= 0.790664 | Yes |
| Retained-value recall | 0.970711 | >= 0.900000 | Yes |
| Intrinsic junk escape rate | 0.000000 | <= 0.020000 | Yes |
| Acceptable atomic-count rate | 0.797414 | >= 0.900000 | No |
| Claim-text faithfulness | 0.733522 | >= 0.744435 | No |
| Speaker exactness | 0.992188 | >= 0.954615 | Yes |
| Reported-actor exactness | 0.785156 | >= 0.735385 | Yes |
| Hallucination rate | 0.044355 | <= 0.093684 | Yes |
| Schema parse success | 1.000000 | >= 0.990000 | Yes |

The development stack now passes **7/9 gates**. Junk and contamination remain
zero. This development-fitted threshold remains a transfer risk and has not
been tested on sealed episodes.

## Audit

- Provider calls/tokens: `0 / 0`
- Frontier result SHA-256:
  `0b804f327282baee0ff2be08a9282c4c8f5ba58705ae51ddeeb278433a53e201`
- Adopted rescore SHA-256:
  `2cafec7a890600f93ddc465f0c12162d88dfc6b938e12c2985c95d378152437c`
- Production mutation: `false`
- Sealed holdouts opened: `false`
