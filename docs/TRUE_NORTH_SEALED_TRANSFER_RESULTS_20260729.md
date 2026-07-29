# True-North Sealed-Transfer Result

Date: 2026-07-29

Episode: `ep_97a45100ce0d305f58d7dd69`

Architecture: Ruling-7 certified GLM-only stack

Result: **4/9 gates pass; the development certification materially overfits**

## Protocol integrity

The certified stack ran first, while sealed gold did not exist. Its 312
candidate predictions were frozen at SHA-256
`ab18dba79ff78d069850d1c7c8f7946d20040ce99fa465410c0bf08f7a496778`.
Only after that freeze did the selected-episode gold build run: independent
Sol pass A, independent Sol pass B, Sol adjudication pass C, compilation, and
span-enforced actor repair.

No sealed gold entered an extraction packet. No prompt, threshold, rule,
composition, scorer, or gate changed after a sealed result was observed.
The second sealed episode, `ep_044f1d2d020e021cfaf99e90`, remains unopened.

## Nine-gate transfer table

| Gate | Development | Transfer | Delta | Gate | Transfer pass |
|---|---:|---:|---:|---:|:---:|
| Candidate-state macro F1 | 0.873162 | 0.414597 | -0.458565 | >= 0.790664 | No |
| Retained-value recall | 0.974895 | 0.820690 | -0.154205 | >= 0.900000 | No |
| Intrinsic junk escape rate | 0.000000 | 0.000000 | 0.000000 | <= 0.020000 | Yes |
| Acceptable atomic-count rate | 0.802575 | 0.722689 | -0.079886 | >= 0.900000 | No |
| Claim-text faithfulness | 0.729336 | 0.719775 | -0.009561 | >= 0.744435 | No |
| Speaker exactness | 0.991561 | 1.000000 | +0.008439 | >= 0.954615 | Yes |
| Reported-actor exactness | 0.793249 | 0.734694 | -0.058555 | >= 0.735385 | No |
| Hallucination rate | 0.040323 | 0.075163 | +0.034840 | <= 0.093684 | Yes |
| Schema parse success | 1.000000 | 1.000000 | 0.000000 | >= 0.990000 | Yes |

Transfer passes **4/9**, down from **7/9** on development. Reported actor
misses its gate by 0.000691. Coupled diagnostics on transfer are faithfulness
0.430827, speaker 0.470250, actor 0.345489, and hallucination 0.091503.

The intrinsic-junk gate remains clean. One relational-junk candidate escaped,
but this extraction-only protocol deliberately did not build canonical gold
or a canonical merge map, so downstream relational contamination is not
measured and is not represented as zero.

## Interpretation

This is not a small transfer fluctuation. The two largest declines are in the
development-tuned disposition composition: macro F1 falls 45.86 points and
retained-value recall falls 15.42 points. Atomic count, the already disclosed
limitation, falls another 7.99 points. Phase-D actor suppression falls 5.86
points and narrowly misses its re-referenced gate. These are concentrated in
exactly the components whose development behavior was used to select policy or
thresholds.

The development-fold certification therefore does **not** generalize to this
sealed healthcare-safety episode. It remains an accurate description of the
development fold, but it must not be presented as transfer-validated or
scale-ready. Per Ruling 8, this result explains the failure and does not
authorize tuning on the sealed episode.

## Gold and usage

- Blind GLM runtime: 68 calls, 869,461 tokens.
- Atomic Sol gold A/B/C: 51 calls, 2,596,188 tokens.
- Codex-lane actor repair: 15 calls, 623,699 tokens.
- Ruling-8 total: **134 calls, 4,089,348 tokens**.
- Authorized ceiling: 150 calls, 4,800,000 tokens.
- Campaign cumulative: **443 calls, 7,673,339 known tokens**.
- Gold: 312 candidates, 515 atomics; 290 strict value, 15 strict junk,
  1 strict hold, and 6 contested candidates.
- Blind output: 312 candidates and 252 atomics.

The 132-call estimate was two calls low because the sealed fold produced 515
gold atomics and therefore required 15 actor-repair calls rather than the
projected 13. The run remained inside both authorized ceilings.

## Test and audit bindings

- Blind freeze SHA-256:
  `1d66958c8bce2dc1f7964ec7cad62351340e735f2cc9a09d69cffcbbdb86df52`
- Repaired gold semantic SHA-256:
  `fc7ae9a51b49876873daac56814fa41b4012f20ff5e96868b1479683da5bccc0`
- Consensus semantic SHA-256:
  `6450d284c6bcb09d6b9c7306c24fa53bd3370764f61007ff5c58ddbf46e65db1`
- Transfer score artifact:
  `sealed-transfer/runs/ruling8-sealed-transfer-blind-20260729-v1/score/transfer-score.json`
- Transfer score semantic SHA-256:
  `4b020f6d21673c04f2363ba5219a089648bbfd08db234c356bd2a65dd516ec46`
- Production mutation: none.
- Post-result tuning: none.
- Second sealed episode opened: no.
