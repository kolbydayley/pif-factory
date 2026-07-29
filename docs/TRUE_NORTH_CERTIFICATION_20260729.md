# True-North Development-Fold GLM-Only Certification

Date: 2026-07-29

Suite: `ai-safety-v1`

Measurement contract: `pif_true_north_measurement_contract_v8`

Certification: **DEVELOPMENT-FOLD ONLY; SEALED TRANSFER FAILED 4/9**

Benchmark progression: **not cleared; the remaining sealed episode is closed**

## Ruling 8 sealed-transfer finding

Ruling 8 authorized one complete, blind transfer check on
`ep_97a45100ce0d305f58d7dd69`. The certified GLM-only output was frozen and
hashed before sealed gold existed. Independent atomic A/B/C gold and
span-enforced actor repair were then built without feeding gold to extraction.

The transfer stack passes **4/9 gates**, versus 7/9 on development. Candidate
macro F1 drops from 0.873162 to 0.414597, retained-value recall from 0.974895
to 0.820690, atomic count from 0.802575 to 0.722689, and actor exactness from
0.793249 to 0.734694. The latter misses its gate by 0.000691. Speaker,
intrinsic junk, hallucination, and schema still pass.

This is material development overfit, concentrated in the selected
disposition and actor components as well as the known decomposition weakness.
The development certification remains a truthful development-fold result but
does **not** stand as transfer validation, production readiness, or evidence
that the stack is ready to scale. Nothing was tuned after observing sealed
results. See `docs/TRUE_NORTH_SEALED_TRANSFER_RESULTS_20260729.md`.

## Authority and scope

Kolby explicitly delegated the final ruling to the True-North review loop.
Ruling 3 is recorded as
`owner-delegated-final-ruling-20260729-v1` in both the campaign ledger and
suite manifest. It supersedes only the v6 hallucination measurement contract;
it does not erase earlier results.

This certification says the measured components form the recommended
development-fold architecture with disclosed limitations. It does **not** say
all benchmark gates pass, authorize production promotion, or authorize sealed
transfer execution.

## Ruling 4 update

Ruling 4 re-referenced candidate-state macro F1 to `0.790664`, derived as
`min(0.90, 0.830664 - 0.04)` under the exact scorer. The 0.90 aspiration and
raw disposition agreement remain diagnostics. The then-certified C2 stack's
`0.863223` therefore passed this gate, moving that historical certification
baseline to **6/9 gates** without changing any model output. Ruling 7's
current GLM-only composition scores `0.873162`.

The subsequent Phase C A/B/C cheap-consensus run is reported separately in
`docs/TRUE_NORTH_PHASE_C_CHEAP_CONSENSUS_RESULTS_20260729.md`. It also passed
6/9 gates but failed its atomicity, faithfulness, and hallucination acceptance
criteria, so it does not clear benchmark progression.

Three provider-schema forms failed before inference and remain void. The final
transport-only attempt removed the provider schema, retained the exact local
validator, and completed all 19 Sol envelopes. It produced a valid semantic
failure: 0.810345 full-fold atomicity, 0.676190 on flagged candidates,
0.732059 faithfulness, and 0.125000 hallucination. The decomposition lane is
therefore permanently closed. See
`docs/TRUE_NORTH_C2_SCHEMA_FREE_FINAL_RESULTS_20260729.md`.

Phase D subsequently added a zero-call actor-emission suppression rule. The
historical best-of-lanes composition applied that frozen rule to C2's
actor-span output. It reached 0.810345 atomicity, 0.782101 actor exactness,
and 0.040323 hallucination, so that coherent development stack passed 7/9
gates. Ruling 7 supersedes it as the recommendation because it crossed no
additional gate despite requiring the Codex lane.

Ruling 6 is terminal for the quality program. It records the matched-pair
diagnostic showing faithfulness at 0.751302 when atomic count is acceptable
and 0.652774 when it is not. Even a pairwise oracle choosing the better of
emitted and candidate-verbatim wording reaches only 0.736442. Faithfulness is
therefore the same decomposition-alignment limitation as atomicity, receives
no rewriting lane, and is not re-referenced. The program closes at **7/9**.

Ruling 7 corrects the certification objective. The prior C2 best-of-lanes
stack was selected on quality alone, even though its 21 Codex-lane calls
crossed no additional gate. The recommended runtime is now the best frozen
GLM-only composition: split-default on validated compound packets, frozen
Task-5 fallback on the two invalid development packets, Phase-D actor
suppression, speaker prior adoption, and a two-GLM option-2 disposition
composition with no Spark tiebreaker. It also passes **7/9**, with zero runtime
Codex-lane calls.

## Final nine-gate table

The table is component-based. Disposition uses two GLM passes with a
deterministic value-state OR on disagreements, followed by the option-2
intrinsic, hold, and relational safety rules. Decomposition uses the frozen
GLM split-default outputs with Task-5 fallback. Actor and speaker use their
contract-normative deterministic/prior lanes.

| Gate | Result | Final gate | Calibration margin | Coupled diagnostic | Pass |
|---|---:|---:|---:|---:|:---:|
| Candidate-state macro F1 | 0.873162 | >= 0.790664 | 0.040000 below 0.830664 ceiling | gold A/B raw agreement 0.807018; aspiration 0.900000 | Yes |
| Retained-value recall | 0.974895 | >= 0.900000 | none; option-2 contract | n/a | Yes |
| Intrinsic junk escape rate | 0.000000 | <= 0.020000 | none; option-2 contract | contamination 0 | Yes |
| Acceptable atomic-count rate | 0.802575 | >= 0.900000 | none; explicitly not re-referenced | n/a | No |
| Claim-text faithfulness | 0.729336 | >= 0.744435 | 0.040000 below 0.784435 ceiling | 0.541858 | No |
| Speaker exactness | 0.991561 | >= 0.954615 | 0.040000 below 0.994615 ceiling | 0.701493 | Yes |
| Reported-actor exactness | 0.793249 | >= 0.735385 | 0.040000 below 0.775385 ceiling | 0.561194 | Yes |
| Hallucination rate | 0.040323 | <= 0.093684 | 0.020000 above 0.073684 gold-vs-gold rate | 0.048387 live reading; 0.020000 aspiration | Yes |
| Schema parse success | 1.000000 | >= 0.990000 | none; original mechanical target | n/a | Yes |

Result: **7/9 gates pass.** The architecture is certified with limitations;
the benchmark as a whole has not passed.

## Per-gate provenance

| Gate | Authority | Measured basis |
|---|---|---|
| Candidate-state macro F1 | Ruling 4, under explicit owner delegation | Exact three-class gold-vs-gold macro F1 ceiling 0.830664 minus 0.04; original 0.90 aspiration retained as diagnostic |
| Retained-value recall | Task 4d option 2, hold-resolution ruling, and Ruling 7 | GLM-only OR composition retains 233/239 gold-value candidates |
| Intrinsic junk | Task 4d option 2 plus Ruling 7 | Zero intrinsic escapes; one relational duplicate materially merges, one relational item contributes zero atomics, and contamination is zero |
| Atomic count | Rulings 2 and 3 | Gate remains 0.90 because pass C reaches 1132/1140 = 0.992982 under range acceptance |
| Faithfulness | Ruling 1 | Matched-pair ceiling 0.784435 minus 0.04 |
| Speaker | Ruling 1 | Matched-pair ceiling 0.994615 minus 0.04; the authoritative formula yields 0.954615 |
| Reported actor | Ruling 1 plus Phase-D directive | Matched-pair ceiling 0.775385 minus 0.04; span rule plus packet-only emission suppression |
| Hallucination | Ruling 3, under explicit owner delegation | Matched-pair gold-vs-gold rate 84/1140 = 0.073684 plus the lower-is-better 0.02 margin |
| Schema | Original benchmark | Mechanical parse target; no semantic ceiling |

The hallucination live/coupled gold-vs-gold rate is 166/1140 = 0.145614.
It remains a mandatory diagnostic. The original <=0.02 target remains visible
as an aspiration and is not represented as the attainable inter-annotator
gate.

## Disclosed limitations

### Decomposition alignment: atomicity and faithfulness

Recommended-stack atomicity is **0.802575 against 0.90**, a 9.74-point
shortfall. The single-pass
GLM Task-5 result was 0.780172. Count-first framing did not change it. Spark
tied GLM on its direct measured subset. The sol-class probe scored 0.631579 on
its 96 validated flagged candidates versus 0.652632 for the GLM/Task-5 control
on the same candidates; 21 sol errors were under-splits and 15 were
over-splits.

The measured finding is that gold-level decomposition belongs to the
independent A/B plus adjudication process. No available measured single-pass
model reproduces it. The gate remains honest for that process and is not
recalibrated around extractor weakness.

Phase C's GLM adjudicator produced 0.801724 full-fold atomicity and failed.
Three provider-schema forms then failed before inference and were correctly
voided. The final schema-free C2 transport completed 19/19 Sol envelopes and
selected A 72 times, B 33 times, and a union merge once. Its 0.810345
full-fold atomicity remained below 0.90; faithfulness and hallucination also
failed. This is the terminal semantic evidence. The decomposition lane is
permanently closed.

Six independent designs—count-first prompting, split-default input inversion,
Spark substitution, Sol substitution, GLM-adjudicated consensus, and
Sol-adjudicated consensus—converged between 0.780 and 0.811 atomicity against
the fair 0.90 gate. This convergence across three model families closes the
implementation lane.

Matched-pair faithfulness is **0.729336 against 0.744435**, 1.51 points low.
On acceptable-count pairs it is 0.751302 and passes; on unacceptable-count
pairs it is 0.652774. The 9.85-point alignment penalty, plus the failed
0.736442 wording oracle, establishes one root cause rather than a separate
wording defect. Ruling 6 forbids a rewriting lane and retains the existing
ceiling-referenced gate.

### Hallucination

Phase D's deterministic actor-emission suppression on the GLM-only composition
moves the matched-pair reading to **0.040323 against 0.093684**, a pass. The
live coupled diagnostic is **0.048387**, and the original aspiration remains
0.02.

### Scope

All measurements are development-only. The two sealed transfer episodes have
not been opened. This certification cannot establish transfer performance,
production reliability, or production economics.

## GLM-only runtime architecture recommendation

```text
Frozen candidate bundles
        |
        v
GLM 5.2 disposition ensemble
  + deterministic value-state OR
  + intrinsic chrome rule
  + canonical merge contamination check
        |
        +--> speaker: frozen prior adoption
        |
        +--> actor: deterministic evidence-span rule
        |
        +--> base decomposition: frozen Task-5 GLM contract
        |
        `--> flagged compounds: GLM split-default contract
              with Task-5 fallback on validation failure
```

Recommended component responsibilities:

- **GLM 5.2:** disposition and decomposition workhorse. The disposition result
  is zero intrinsic escapes, zero contamination, one materialized relational
  merge, and 0.974895 retained-value recall. Split-default plus Task-5 fallback
  is the best frozen GLM-only decomposition at 0.802575 atomicity.
- **Deterministic harness:** hold resolution, intrinsic chrome filtering,
  relational merge verification, speaker-prior carriage, and actor span
  enforcement. These require zero provider calls at composition.
- **Codex/Sol runtime lane:** none. C2 remains a diagnostic variant only. Its
  0.810345 atomicity and 0.732059 faithfulness cross no additional gate and do
  not justify 21 Codex-lane calls.

The compound screen selected 106/256 Search candidates, or **41.41%**. Those
candidates remain on the subsidized GLM lane.

## Measured cost picture

These are subscription-token measurements, not dollar-equivalent prices.

| Stage or reference | Model/route | Calls | Known tokens | Outcome |
|---|---|---:|---:|---|
| Ruling-7 disposition source passes | GLM 5.2 | 42 | 281,418 | Deterministic value-state OR; no Spark runtime dependency |
| Option-2 intrinsic, hold, merge certification | Deterministic | 0 | 0 | Pass |
| Speaker prior adoption | Deterministic | 0 | 0 | Pass |
| Actor span plus Phase-D suppression | Deterministic | 0 | 0 | Pass |
| Frozen Task-5 all-candidate decomposition | GLM 5.2 | 21 | 242,878 | 0.780172 atomicity; fail |
| GLM split-default | GLM 5.2 | 28 | 328,682 | Selected with Task-5 fallback; 0.802575 coherent atomicity |
| C2 best-of-lanes variant | GLM plus `gpt-5.6-sol` | historical only | historical only | 0.810345 atomicity, 7/9, not recommended |
| Gold reference A/B/C, direct base receipts | `gpt-5.6-sol` | 207 | 9,552,192 | Reference construction, not campaign or production cost |

The direct gold-reference receipt count is 69 packets for each of A, B, and C.
It demonstrates why blindly reproducing the full reference process in
production would be expensive. It does not price the smaller, untested
compound-only consensus design.

For the median Syntax episode, the recommended composition has an optimistic
floor of **56 GLM calls and zero Codex calls**: 28 disposition, 14 Task-5 base,
and 14 split-default. The historical production baseline used 17 all-Codex
calls. By call count, Ruling 7 moves 100% of runtime work off the Codex lane
while adding 56 subsidized GLM calls.

Actual token and per-atomic production cost remain unmeasured. The July 20
snapshot contains 17 readable GPT-5.5 outputs but zero token-usage receipts,
184 upstream claim rows, and zero atomic-claim rows. It cannot support a true
token ratio or downstream drift comparison.

## Campaign ledger summary

Authoritative cumulative campaign spend:

- **309 calls**
- **3,583,991 known tokens**
- token total excludes the 45-call actor-gold-repair stage because its
  historical ledger did not capture tokens
- zero calls were made for Ruling 3

### First 160-call campaign

| Component | Calls | Outcome |
|---|---:|---|
| Phase-B reported-actor gold repair | 45 | Repaired the reference; token count absent |
| Phase-C disposition and junk work | 51 | Option-2 deterministic composition ultimately passed disposition safety |
| Frozen Task-5 single pass | 21 | Atomicity fail |
| Dual-pass decomposition | 22 | Counts identical across passes; uncertainty signal refuted |
| Spark conjunction measurement | 12 | Spark tied GLM at 0.636364 on measured subset |
| Actor-value measurement | 9 | Emission/value design failed |
| **Total** | **160** | Hard stop later lifted by owner ruling |

Historical sub-experiments inside the Phase-C allocation include the two
21-call GLM disposition runs, one Spark conflict call, the 8-call Task-4b
verifier, the one-receipt Task-4c budget-preflight failure, and the zero-call
option-2 hold/merge correction. Their receipt reports overlap the grouped
160-call audit, so they are not added a second time.

### Owner-authorized post-160 experiments

| Experiment | Calls | Tokens | Outcome |
|---|---:|---:|---|
| Count-first prompt | 25 | 249,496 | Partial; net-zero atomicity change |
| Two-stage actor | 23 | 261,654 | Partial; emission remained limiting |
| Input split-default | 28 | 328,682 | Partial; 0.806034 diagnostic atomicity |
| Spark split-default | 14 | 309,122 | Partial; budget breach; no measured advantage |
| Sol split-default | 14 | 455,902 | Partial; below GLM control; decomposition lane stopped |
| A/B/C cheap consensus | 23 | 246,443 | Complete; 0.801724 atomicity; failed acceptance |
| C2 Sol adjudicator | 2 | 0 | Structured-output schema rejected before inference; void under Ruling 5 |
| C2 provider-compatible smoke | 1 | 0 | Flat schema rejected before inference; no batch dispatched |
| C2 schema-free final | 19 | 554,878 | Complete semantic failure; 0.810345 atomicity; lane permanently closed |
| Phase-D actor suppression | 0 | 0 | Actor and hallucination pass jointly |
| Phase-E production shadow (initial) | 0 | 0 | Isolation preflight blocked on unreadable dataless production database |
| Phase-E production shadow (hydrated final) | 0 | 0 | Isolation passed; exact-contract 144-call floor exceeded 60-call ceiling |
| Phase-E production discovery | 0 | 0 | Local SQLite identified as corpus authority; Railway is observer/broker only |
| Ruling-7 GLM-only recertification | 0 | 0 | 7/9 preserved; runtime Codex lane eliminated |
| **Post-160 total** | **149** | **2,406,177** | Includes the final terminal decomposition result and accepted Phase D |

### Ruling provenance chain

1. Task 4d option 2 redefined disposition safety around intrinsic junk plus a
   blocking canonical contamination check.
2. Hold resolution admitted holds uniformly while preserving conservative
   recall accounting.
3. Ruling 1 decoupled speaker, actor, and faithfulness onto matched pairs and
   retained coupled diagnostics.
4. Ruling 2 retained the 0.90 atomic range-acceptance gate.
5. Kolby authorized the gold-author-class sol probe and hallucination
   calibration.
6. Kolby explicitly delegated the final decision to the review loop.
7. Ruling 3 created contract v7, re-referenced hallucination, retained
   atomicity, and issued this development-only hybrid certification.
8. Ruling 4 created contract v8 and re-referenced candidate-state macro F1 to
   0.790664 against its measured ceiling, retaining the 0.90 aspiration and
   raw agreement as diagnostics.
9. Ruling 5 voided pre-inference C2, reversed its lane closure, authorized one
   smoke-gated provider-compatible re-attempt, and independently dispatched
   Phase E under strict production isolation.
10. The final review directive removed provider-side schema transport,
    permanently closed decomposition after the resulting semantic failure,
    and authorized the zero-call actor frontier and production-truth discovery.
11. Ruling 6 identified atomicity and faithfulness as one
    decomposition-alignment limitation, prohibited a wording lane, retained
    both gates, closed the quality program at 7/9, and authorized the hydrated
    Phase-E attempt under the existing ceiling.
12. Ruling 7 corrected the optimization objective: the best-scoring frozen
    GLM-only composition replaces C2 as the recommendation because both pass
    7/9 while only the GLM-only runtime eliminates Codex-lane calls.

## Post-certification experiment result

### Compound A/B/C consensus recipe

The bounded compound-only design was executed once under Ruling 4. It reached
0.801724 full-fold atomicity and 0.657143 on the flagged subset, with
faithfulness and hallucination also below gate. The design is frozen as a
terminal quality failure; no self-iteration occurred. After three void
provider-schema incompatibilities, the final schema-free C2 attempt completed
and failed semantically at 0.810345 atomicity. The decomposition lane is now
permanently closed.

### Actor suppression

The zero-call Phase-D rule suppresses span-valid actor emissions when a
packet-only risk score reaches 2. It passes actor and hallucination
simultaneously and moves the stack to 7/9 gates. See
`docs/TRUE_NORTH_PHASE_D_ACTOR_SUPPRESSION_20260729.md`.

The recommended composition applies the frozen rule to the GLM split-default
plus Task-5-fallback output. C2 remains documented in
`docs/TRUE_NORTH_BEST_STACK_AND_PHASE_E_READINESS_20260729.md` as the
higher-quality-but-costlier variant that crosses no additional gate.

### Production shadow trial

After hydration, isolation passed in strict read-only mode. The final
one-episode preflight then established that the July 20 Syntax baseline has 16
label calls and one context call, all on GPT-5.5, but **0/17 token receipts**.
It has **184 upstream claims and 0 atomic claims**. End-to-end downstream drift,
tokens per retained atomic, and a true hybrid/all-Codex token ratio are
therefore not recoverable. No provider call was spent.

A valid measurement requires instrumented per-call token receipts for both
lanes, an all-Codex baseline that actually emits downstream atomic and
canonical outputs under the same contract, identical episode/candidate scope,
and retained-valid-atomic lineage on both sides. See
`docs/TRUE_NORTH_PHASE_E_EXECUTION_STOP_20260729.md`.

### Future work

The decomposition problem remains open research, not an implementation gap.
Gold quality came from a two-pass-plus-adjudication process; every tested
practical approximation still fails the same two gates. A future cost trial
should instrument a fresh, comparable all-Codex downstream baseline rather
than attempting to reconstruct missing usage or atomics from the July 20
snapshot.

### Actor value residual

The deterministic actor-span baseline leaves **157 atomics** on which a value
selector could add value. Any future actor model should be sized and scored
only against that residual rather than rerunning emission over every atomic.
This work is not authorized.

### Transfer progression

The sealed transfer episodes may open only after the governing progression
rule is satisfied: all required development gates must pass under one frozen
configuration for the required consecutive runs, with unchanged prompts,
schemas, thresholds, router, contamination certification, and no production
mutation. This certification does not satisfy that condition, so transfer
remains closed.

## Audit bindings

- Manifest v8 SHA-256:
  `c9facbbe1d63788f70e91d8a71d9c51cea3abf8b163ecda3bb3f05690e7a111f`
- Contract v8 SHA-256:
  `ef522915cbd6632e58f52a8463ceb6529b844889d5d36372ce2ecfe636053e30`
- Gate policy v5 thresholds:
  `consensus_candidate_state_macro_f1 >= 0.790664`;
  `hallucination_rate_proxy <= 0.093684`
- Hallucination calibration SHA-256:
  `f9b99c77045e0ad5daa183badc49426a17fdc9510bc7d414298479fc4c877ed8`
- Final hybrid diagnostic SHA-256:
  `11ce7d70d3c090d7b717e66cf4e33e668067115a81b567efa6e4b89165d2ea6f`
- Ruling-7 GLM-only certification SHA-256:
  `40265922d904ce6b56596c8c5080f1a3f208bdfb1851b379fc68537b5cb1c1f6`
- Phase-E executable isolation preflight SHA-256:
  `b6687e42c6e13566b2b9b859fea1f35806ee5cb3b0393794e814487363eecd67`
- Phase-E exact-contract budget preflight SHA-256:
  `2a56742b7f8e15e87b2f42cbbdecd2420f0a7b79afe8de9121141521b62538b0`
- Provider calls for Ruling 4 contract migration: `0`
- Provider calls for Phase C: `23`
- Provider calls for Ruling 6 and Phase E final attempt: `0`
- Provider calls for Ruling 7 recertification: `0`
- Provider calls for final schema-free C2: `19`
- Phase-D frontier result SHA-256:
  `0b804f327282baee0ff2be08a9282c4c8f5ba58705ae51ddeeb278433a53e201`
- Holdout opened: `false`
- Production source mutated: `false`
