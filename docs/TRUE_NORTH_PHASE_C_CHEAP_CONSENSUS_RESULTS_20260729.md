# True-North Phase C A/B/C Cheap-Consensus Result

Date: 2026-07-29

Suite: `ai-safety-v1`

Run: `task5-ab-c-cheap-consensus-20260729-v1`

Measurement contract: `pif_true_north_measurement_contract_v8`

Status: **terminal quality failure; no self-iteration**

## Ruling 4

The review loop, acting under Kolby's explicit delegation, re-referenced
candidate-state macro F1 to:

```text
min(0.90, 0.830664 - 0.04) = 0.790664
```

The original 0.90 aspiration remains diagnostic. The exact live scorer's raw
disposition agreement is 0.807018 and remains diagnostic. The prior v7
manifest is preserved by SHA-256 in `manifest-history`; contract v8 and gate
policy v5 record the ruling provenance.

The certified stack's 0.863223 candidate-state macro F1 therefore passes the
re-referenced gate.

## Frozen experiment

- Pass A reused the frozen GLM Task-5 decompositions: zero calls.
- Pass B reused 96 validated `gpt-5.6-sol` candidates and completed the
  remaining 10 candidates in four calls using the 35,000-token reservation.
- Pass C used 19 packet-batched GLM 5.2 calls. Every output claim had to be an
  exact string from the union of Pass A and Pass B; the validator rejected
  third structures.
- Unflagged candidates retained frozen Task-5 outputs.
- The whole-run preflight reserved 23 calls and 710,000 tokens against the
  40-call / 900,000-token ceiling.

The first Sol response encountered a local validator-shape defect: a
single-packet response was passed to the provider-envelope validator. Its exact
returned JSON and usage stream were preserved, validated under the existing
single-packet contract, and charged once. No second provider call was made for
that packet; prompts, packets, schema, scoring, and model routing were
unchanged.

## Phase C result

| Measure | Result | Gate | Pass |
|---|---:|---:|:---:|
| Full-fold acceptable atomic count | 0.801724 | >= 0.900000 | No |
| Flagged-subset acceptable atomic count | 0.657143 | diagnostic | — |
| Matched faithfulness | 0.735388 | >= 0.744435 | No |
| Matched hallucination | 0.125000 | <= 0.093684 | No |
| Candidate-state macro F1 | 0.863223 | >= 0.790664 | Yes |
| Intrinsic junk escapes | 0 | = 0 | Yes |
| Relational contamination | 0 | = 0 | Yes |
| Materialized relational merges | 1 | diagnostic | — |

The experiment fails acceptance. Cheap A/B/C adjudication raised the frozen
Task-5 atomic rate from 0.780172 to 0.801724, but it did not approach 0.90.
The flagged subset remained the bottleneck at 0.657143. Matched faithfulness
and hallucination also failed. Per the dispatch contract, the design is frozen
and the decomposition lane stops here.

## Updated nine-gate table

This table scores the completed Phase C composition under final contract v8.

| Gate | Result | Gate | Margin | Coupled diagnostic | Pass |
|---|---:|---:|---:|---:|:---:|
| Candidate-state macro F1 | 0.863223 | >= 0.790664 | +0.072559 | raw agreement 0.807018; aspiration 0.900000 | Yes |
| Retained-value recall | 0.970711 | >= 0.900000 | +0.070711 | terminal-hold 0.899582 | Yes |
| Intrinsic junk escape rate | 0.000000 | <= 0.020000 | +0.020000 | contamination 0; one merge | Yes |
| Acceptable atomic-count rate | 0.801724 | >= 0.900000 | -0.098276 | flagged subset 0.657143 | No |
| Claim-text faithfulness | 0.735388 | >= 0.744435 | -0.009047 | 0.561569 | No |
| Speaker exactness | 0.992063 | >= 0.954615 | +0.037448 | 0.722543 | Yes |
| Reported-actor exactness | 0.773810 | >= 0.735385 | +0.038425 | 0.563584 | Yes |
| Hallucination rate | 0.125000 | <= 0.093684 | -0.031316 | 0.177419; aspiration 0.020000 | No |
| Schema parse success | 1.000000 | >= 0.990000 | +0.010000 | n/a | Yes |

Result: **6/9 gates pass.** The development benchmark does not pass, and the
sealed transfer episodes remain closed.

## Spend and isolation

- Phase C: **23 calls / 246,443 tokens**
- Cumulative campaign: **287 calls / 3,029,113 known tokens**
- Historical actor-gold-repair tokens remain unavailable, so the token total
  is still a known minimum.
- Holdout opened: `false`
- Production mutation: `false`
- Score SHA-256:
  `f8a113b1788f1dbbffceb9309800e1cac7e5fc58556674da0913837b0b5b8ac2`
