# True-North Final Scoreboard and Decision Memo

Date: 2026-07-29

Campaign status: **closed**

Decision: **NO-GO on replacing the Codex-primary pipeline with the certified
cheap-lane stack**

## Headline finding

The GLM-only stack passed 7/9 gates on its small development fold and only 4/9
on the blind sealed episode. The development certification overfit.

Structurally derived components transferred best: speaker prior adoption
improved to 1.000000, intrinsic-junk containment remained zero, schema stayed
perfect, and hallucination control remained below its calibrated gate. Fitted
components did not preserve their development certification: the disposition
ensemble collapsed, the actor operating point narrowly failed, and the known
decomposition weakness worsened.

The diagnosis adds important precision:

- Hold admission was dormant on sealed data, so its development-fitted base
  rate did not cause this result.
- Phase-D suppression remained directionally useful versus disabling it, but
  did not preserve joint actor/hallucination compliance because actor finished
  0.000691 below gate.
- The sealed fold is denser and structurally different: **515 / 312 =
  1.650641 atomics per candidate**, versus **1,458 / 1,140 = 1.278947** on
  development.

## Final four-way gate table

For lower-is-better hallucination and junk, the comparison direction is shown
explicitly. “Ceiling” is the measured gold-vs-gold reference used to calibrate
the final gate; `n/a` means the original mechanical target was retained.

| Gate | Original aspiration | Final calibrated gate and ceiling | Development | Sealed transfer |
|---|---:|---|---:|---:|
| Candidate-state macro F1 | >= 0.900000 | >= 0.790664; ceiling 0.830664 | 0.873162 **pass** | 0.414597 **fail** |
| Retained-value recall | >= 0.900000 | >= 0.900000; n/a | 0.974895 **pass** | 0.820690 **fail** |
| Intrinsic junk escape | <= 0.020000 | <= 0.020000; n/a | 0.000000 **pass** | 0.000000 **pass** |
| Acceptable atomic count | >= 0.900000 | >= 0.900000; ceiling 0.992982 | 0.802575 **fail** | 0.722689 **fail** |
| Claim-text faithfulness | >= 0.900000 | >= 0.744435; ceiling 0.784435 | 0.729336 **fail** | 0.719775 **fail** |
| Speaker exactness | >= 0.970000 | >= 0.954615; ceiling 0.994615 | 0.991561 **pass** | 1.000000 **pass** |
| Reported-actor exactness | >= 0.950000 | >= 0.735385; ceiling 0.775385 | 0.793249 **pass** | 0.734694 **fail** |
| Hallucination rate | <= 0.020000 | <= 0.093684; gold rate 0.073684 | 0.040323 **pass** | 0.075163 **pass** |
| Schema parse success | >= 0.990000 | >= 0.990000; n/a | 1.000000 **pass** | 1.000000 **pass** |

Development: **7/9**. Sealed transfer: **4/9**.

The transfer column is authoritative for generalization. Calibrated
development gates do not convert a sealed failure into a pass.

## Collapse attribution

Development strict classes were 239 value, 9 junk, and 0 hold. Sealed strict
classes were 290 value, 15 junk, and 1 hold. Introducing one hold changes the
scorer's hold F1 from its empty-class value of 1.0 to 0.0. Reweighting sealed
conditional decisions to development priors raises macro F1 from 0.414597 to
0.726363:

- 0.311766, or 68.0% of the drop, is class-prior/scorer discontinuity;
- 0.146799, or 32.0%, is genuine conditional decision degradation.

The genuine failure is still large. All 52 retained-value misses were
disposition rejects. No miss came from hold handling or an accepted candidate
producing zero atomics. See
`docs/TRUE_NORTH_TRANSFER_COLLAPSE_DIAGNOSTIC_20260729.md`.

## Decomposition process property

Six designs across three model families—count-first prompting, split-default
input inversion, Spark substitution, Sol substitution, GLM-adjudicated
consensus, and Sol-adjudicated consensus—converged between roughly 0.78 and
0.81 atomic-count accuracy against a fair 0.90 gate. Gold pass C reaches
0.992982 under that gate's range rule.

The best cheap production candidate scored 0.802575 on development and
degraded to 0.722689 on sealed data. Gold quality is a property of the
independent two-pass-plus-adjudication process, not an available single-pass
model or prompt. Reproducing that quality means running the process, with its
corresponding cost.

## Campaign accounting

Total: **443 provider calls / 7,673,339 known tokens**.

Call attribution reconstructed from the frozen ledger and receipts:

| Lane | Calls | Share |
|---|---:|---:|
| GLM / Z.AI | 265 | 59.82% |
| Codex subscription: Spark or Sol | 178 | 40.18% |
| **Total** | **443** | **100%** |

The token total is a lower bound because the original 45-call actor-gold
repair did not record token usage. Historical mixed-stage receipts do not
support a complete lane-exact token split. Directly attributable known-token
lower bounds are 1,709,293 GLM tokens and 4,539,789 Codex-lane tokens; the
remaining 1,424,257 known tokens belong to historical or mixed-provider stages.
The sealed transfer itself is exact: 68 GLM calls / 869,461 tokens and 66
Codex-lane calls / 3,219,887 tokens.

### Every bounded experiment

| Experiment | Calls | Known tokens | Outcome |
|---|---:|---:|---|
| Actor-gold repair | 45 | unavailable | Reference repaired |
| Disposition/junk campaign | 51 | grouped | Option-2 passed on development |
| Frozen Task-5 | 21 | 242,878 | Atomicity failed |
| Dual-pass decomposition | 22 | grouped | Identical systematic count errors |
| Spark conjunction probe | 12 | 169,236 | Tied GLM; failed |
| Actor-value probe | 9 | 192,274 | Failed |
| Count-first prompt | 25 | 249,496 | Partial; net-zero gain |
| Two-stage actor | 23 | 261,654 | Partial; emission failed |
| Input split-default | 28 | 328,682 | Partial; gate still failed |
| Spark split-default | 14 | 309,122 | Budget-terminal; no advantage |
| Sol split-default | 14 | 455,902 | Below GLM control |
| GLM-adjudicated A/B/C | 23 | 246,443 | 0.801724; failed |
| C2 structured schema | 2 | 0 | Pre-inference void |
| C2 flat-schema smoke | 1 | 0 | Pre-inference void |
| C2 schema-free Sol adjudication | 19 | 554,878 | 0.810345; failed |
| Phase-D suppression frontier | 0 | 0 | Development joint pass |
| Production truth discovery/readiness | 0 | 0 | Target found; trial armed |
| Three-episode production trial | 0 | 0 | Isolation blocked before dispatch |
| Hydrated three-episode trial | 0 | 0 | Exact stack exceeded budget |
| One-episode 60-call pilot | 0 | 0 | True floor 63; stopped |
| One-episode 90-call pilot | 0 | 0 | Baseline unmeasurable; stopped |
| Ruling-7 GLM-only recertification | 0 | 0 | 7/9 development, zero runtime Codex |
| Ruling-8 sealed transfer | 134 | 4,089,348 | 4/9; material overfit |
| Closeout diagnosis and scoreboard | 0 | 0 | Campaign closed |

The prospective cost trial was cancelled, saving approximately **90 calls**.
Its design remains documented and armed for a future campaign.

## Ruling provenance

1. **Ruling 1:** matched-pair scoring for speaker, actor, and faithfulness;
   coupled readings retained as diagnostics.
2. **Ruling 2:** atomicity retained at the fair 0.90 range-acceptance gate.
3. **Ruling 3:** hallucination re-referenced to gold rate plus 0.02;
   development-only certification issued.
4. **Ruling 4:** candidate-state macro F1 re-referenced to ceiling minus 0.04.
5. **Ruling 5:** pre-inference C2 schema failures declared void; one valid
   semantic attempt reserved.
6. **Ruling 6:** faithfulness and atomicity identified as one decomposition
   alignment problem; rewriting and decomposition lanes closed.
7. **Ruling 7:** GLM-only 7/9 stack replaced the costlier Sol variant because
   Sol crossed no additional gate.
8. **Ruling 8:** the closeout plan's initial 120-call estimate was overridden
   by the measured untrimmed protocol floor; one blind sealed episode was
   authorized at 150 calls / 4.8M tokens.

Ruling 8 executed in the required order: predictions froze before gold,
independent gold followed, and scoring occurred last. No post-result tuning
occurred. The second episode remains permanently closed for this campaign.

## Decision memo to Kolby

### Recommendation: NO-GO

Do not promote the cheap-lane stack as a replacement for the Codex-primary
pipeline. It loses three previously passing gates on transfer and worsens the
already failing decomposition gate. The central thesis—equal-enough quality
with Codex work removed—is not supported.

Established: **this stack cannot replace Codex on the available evidence.**

Not established: **the individual cheap or deterministic components are
worthless.** Several remain useful:

- speaker prior adoption transferred perfectly at 1.000000;
- intrinsic-junk containment remained at zero;
- deterministic schema and evidence constraints remained perfect;
- Phase-D suppression remained directionally beneficial and preserved the
  hallucination gate, although actor narrowly failed;
- GLM outputs can still serve as bounded suggestions, candidate triage, or
  assistive evidence inside a Codex-primary pipeline.

Those derived components are deployable as assistive elements only when Codex
remains the final quality authority and no automatic production promotion is
inferred from their output.

## Future work

A second campaign would require all of the following:

- multi-fold validation for every fitted threshold, ensemble rule, or
  hold-resolution policy;
- episode-profile robustness across claim density, candidate density, domain,
  speaker structure, and rare-class regimes;
- explicit treatment of empty or ultra-rare macro-F1 classes;
- the full independent two-pass-plus-adjudication decomposition process, or a
  newly demonstrated equivalent, priced honestly against Codex primary;
- fresh held-out data that has never informed a diagnosis, threshold, prompt,
  rule, or architecture choice.

All future work requires fresh authorization. The remaining sealed episode
cannot be used for it under this campaign.

## Absolute diagnostic boundary

The sealed ablations are diagnostic only. No variant, threshold, rule, policy,
prompt, composition, scorer, or gate may be adopted from them. No
sealed-derived configuration may be re-certified.
