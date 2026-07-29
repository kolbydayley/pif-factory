# True-North C2 Provider-Compatible Re-attempt

Date: 2026-07-29

Run: `task5-c2-sol-adjudicator-reattempt-20260729-v1`

Status: **VOID; mandatory smoke rejected before inference**

## Result

The Ruling-5 re-attempt replaced the provider-facing schema with a flat object:
a closed `decision` field (`chose_a`, `chose_b`, or `merged`) and a resulting
claim list. `oneOf` and `uniqueItems` were absent. Union membership,
uniqueness, and decision consistency remained enforced by the local validator.
The semantic contract, A/B proposals, evidence, model, and scorer did not
change.

The mandatory one-packet smoke test was the only invocation. The provider
rejected the schema before inference because the `schema_version` property,
expressed with `const`, did not also declare an explicit JSON `type`. The
provider returned `invalid_json_schema`; no inference usage was recorded.

Per the directive, execution stopped immediately:

- batch dispatched: `false`
- validated envelopes: `0 / 19`
- semantic outputs: `0`
- decision distribution: not measurable
- flagged-subset accuracy: not measurable
- calls: `1`
- inference tokens: `0`

No third schema shape was attempted. Because no semantic result exists, the
semantic lane-closure condition did not fire. No further schema attempt is
authorized by Ruling 5; disposition of the lane is left to the review loop.

## Acceptance

Atomic count, matched faithfulness, hallucination, macro F1, and adjudicator
decision statistics cannot be scored. This is an operationally ineligible
result, not a semantic failure.

The frozen development certification metrics did not change. No production
database was opened or mutated, and sealed holdouts remained closed.

## Audit

- Result SHA-256:
  `2370d516f10b7b80bdf90bfbed3076daa394f336970bcbd55cb087fbc7a2e752`
- Cumulative campaign spend:
  **290 calls / 3,029,113 known tokens**
- Historical actor-gold-repair tokens remain unavailable.
