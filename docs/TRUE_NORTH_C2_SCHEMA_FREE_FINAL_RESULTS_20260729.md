# True-North C2 Final Schema-Free Adjudication

Date: 2026-07-29

Run: `task5-c2-sol-adjudicator-schema-free-final-20260729-v1`

Status: **complete semantic failure; decomposition lane permanently closed**

## Transport and scope

The provider-side output schema was removed. The prompt specified the flat JSON
shape, and the existing local validator remained authoritative for candidate
scope, decision consistency, exact A/B-union membership, and uniqueness. The
semantic contract, packets, A/B proposals, evidence, model, and scorer were
unchanged.

The mandatory smoke returned one locally valid `chose_a` decision before the
batch was released. All 19 envelopes then validated:

- provider/model: `gpt-5.6-sol` through ephemeral Codex subscription
- calls: 19
- tokens: 554,878
- flagged candidates: 106
- decision distribution: 72 `chose_a`, 33 `chose_b`, 1 `merged`
- validation failures: 0

## Result

| Metric | Result | Gate | Pass |
|---|---:|---:|:---:|
| Full-fold acceptable atomic count | 0.810345 | >= 0.900000 | No |
| Flagged-subset acceptable atomic count | 0.676190 | diagnostic | — |
| Matched faithfulness | 0.732059 | >= 0.744435 | No |
| Aligned-subset faithfulness | 0.751302 | diagnostic | — |
| Matched hallucination | 0.125000 | <= 0.093684 | No |
| Candidate-state macro F1 | 0.863223 | >= 0.790664 | Yes |
| Intrinsic junk escapes | 0 | 0 | Yes |
| Relational contamination | 0 | 0 | Yes |

The final attempt improved Phase C atomicity from 0.801724 to 0.810345, but it
remained 8.97 points below the atomicity gate. It also failed faithfulness and
hallucination. This is a valid semantic result, so the decomposition lane is
permanently closed under the review-loop directive.

## Audit

- Result SHA-256:
  `b689cf1e0fe4b95523334b453e5ae2d2563a80e03cdf61fa6f24e471f46790be`
- Score SHA-256:
  `a8e56f14026606e4ef10ef26ac90ec2124669300e657e779a34863f7778322c8`
- Cumulative spend:
  **309 calls / 3,583,991 known tokens**
- Production mutation: `false`
- Sealed holdouts opened: `false`
