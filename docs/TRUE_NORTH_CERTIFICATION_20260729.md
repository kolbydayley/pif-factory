# True-North Development-Fold Hybrid Certification

Date: 2026-07-29

Suite: `ai-safety-v1`

Measurement contract: `pif_true_north_measurement_contract_v8`

Certification: **HYBRID ARCHITECTURE, DEVELOPMENT FOLD ONLY**

Benchmark progression: **not cleared; sealed transfer episodes remain closed**

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
raw disposition agreement remain diagnostics. The certified stack's
`0.863223` therefore passes this gate, moving the certification baseline to
**6/9 gates** without changing any model output.

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
definitive zero-call composition applies that frozen rule to C2's actor-span
output. It reaches 0.810345 atomicity, 0.782101 actor exactness, and 0.040323
hallucination, so the coherent development stack passes 7/9 gates.

## Final nine-gate table

The table is component-based because Ruling 3 certifies a hybrid architecture.
Disposition metrics come from the option-2 certified ensemble, actor and
speaker from their contract-normative zero-call lanes, and decomposition,
faithfulness, and hallucination from the final frozen hybrid composition.

| Gate | Result | Final gate | Calibration margin | Coupled diagnostic | Pass |
|---|---:|---:|---:|---:|:---:|
| Candidate-state macro F1 | 0.863223 | >= 0.790664 | 0.040000 below 0.830664 ceiling | raw agreement 0.807018; aspiration 0.900000 | Yes |
| Retained-value recall | 0.970711 | >= 0.900000 | none; option-2 contract | n/a | Yes |
| Intrinsic junk escape rate | 0.000000 | <= 0.020000 | none; option-2 contract | contamination 0 | Yes |
| Acceptable atomic-count rate | 0.810345 | >= 0.900000 | none; explicitly not re-referenced | n/a | No |
| Claim-text faithfulness | 0.732059 | >= 0.744435 | 0.040000 below 0.784435 ceiling | 0.562784 | No |
| Speaker exactness | 0.992218 | >= 0.954615 | 0.040000 below 0.994615 ceiling | 0.732759 | Yes |
| Reported-actor exactness | 0.782101 | >= 0.735385 | 0.040000 below 0.775385 ceiling | 0.577586 | Yes |
| Hallucination rate | 0.040323 | <= 0.093684 | 0.020000 above 0.073684 gold-vs-gold rate | 0.100806 live reading; 0.020000 aspiration | Yes |
| Schema parse success | 1.000000 | >= 0.990000 | none; original mechanical target | n/a | Yes |

Result: **7/9 gates pass.** The architecture is certified with limitations;
the benchmark as a whole has not passed.

## Per-gate provenance

| Gate | Authority | Measured basis |
|---|---|---|
| Candidate-state macro F1 | Ruling 4, under explicit owner delegation | Exact three-class gold-vs-gold macro F1 ceiling 0.830664 minus 0.04; original 0.90 aspiration retained as diagnostic |
| Retained-value recall | Task 4d option 2 and hold-resolution ruling | 232/239 gold-value candidates reach the corpus; terminal-hold reading remains 0.899582 |
| Intrinsic junk | Task 4d option 2 | Zero intrinsic escapes plus blocking canonical merge certification; relational contamination is zero and one duplicate is materially merged |
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

### Atomic decomposition

Atomicity is **0.810345 against 0.90**, an 8.97-point shortfall. The single-pass
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

### Faithfulness

Matched-pair faithfulness is **0.732059 against 0.744435**, 1.24 points low.
Candidate-state macro F1 now passes its Ruling-4 ceiling-referenced gate; its
original 0.90 aspiration remains visible.

### Hallucination

Phase D's deterministic actor-emission suppression on the definitive C2
composition moves the matched-pair reading to **0.040323 against 0.093684**, a
pass. The live coupled diagnostic is **0.100806**, and the original aspiration
remains 0.02.

### Scope

All measurements are development-only. The two sealed transfer episodes have
not been opened. This certification cannot establish transfer performance,
production reliability, or production economics.

## Hybrid production architecture recommendation

```text
Frozen candidate bundles
        |
        v
GLM 5.2 disposition ensemble
  + deterministic composition
  + intrinsic chrome rule
  + canonical merge contamination check
        |
        +--> speaker: frozen prior adoption
        |
        +--> actor: deterministic evidence-span rule
        |
        +--> non-compound candidates: retain frozen minimal-edit path
        |
        `--> compound candidates: Codex-class decomposition lane
              with atomicity limitation disclosed
```

Recommended component responsibilities:

- **GLM 5.2:** disposition workhorse. Certified result: zero intrinsic
  escapes, zero contamination, one materialized relational merge, and 0.970711
  retained-value recall.
- **Deterministic harness:** hold resolution, intrinsic chrome filtering,
  relational merge verification, speaker-prior carriage, and actor span
  enforcement. These require zero provider calls at composition.
- **Codex-class lane:** compound-candidate decomposition. This is a designed
  production component because the reference process requires Codex-class
  independent judgment and adjudication. The measured single-pass sol probe
  does not itself qualify as a passing implementation.

The compound screen selected 106/256 Search candidates, or **41.41%**. That is
the current measured upper bound on the fraction routed away from the cheap
base lane under this design.

## Measured cost picture

These are subscription-token measurements, not dollar-equivalent prices.

| Stage or reference | Model/route | Calls | Known tokens | Outcome |
|---|---|---:|---:|---|
| Certified Phase-C disposition inputs | GLM 5.2 plus one Spark conflict packet | 43 | 311,822 | Inputs later recovered by deterministic option-2 composition |
| Option-2 intrinsic, hold, merge certification | Deterministic | 0 | 0 | Pass |
| Speaker prior adoption | Deterministic | 0 | 0 | Pass |
| Actor evidence-span rule | Deterministic | 0 | 0 | Pass |
| Frozen Task-5 all-candidate decomposition | GLM 5.2 | 21 | 242,878 | 0.780172 atomicity; fail |
| Sol flagged-candidate probe | `gpt-5.6-sol` | 14 | 455,902 | 96/106 candidates validated; incomplete and fail |
| Gold reference A/B/C, direct base receipts | `gpt-5.6-sol` | 207 | 9,552,192 | Reference construction, not campaign or production cost |

The direct gold-reference receipt count is 69 packets for each of A, B, and C.
It demonstrates why blindly reproducing the full reference process in
production would be expensive. It does not price the smaller, untested
compound-only consensus design.

The practical split remains approximately 58.59% cheap/non-compound handling
and at most 41.41% compound routing, plus zero-call speaker and actor rules.
The completed Sol adjudication recipe did not clear atomicity or faithfulness,
so no passing compound recipe has been established. Actual production cost is
not yet certified. The Phase-E production shadow trial was stopped before
dispatch because the production database was an unreadable APFS dataless
placeholder, so neither stage drift nor amortized cost could be measured.

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
| Phase-E production shadow | 0 | 0 | Isolation preflight blocked on unreadable dataless production database |
| Phase-E production discovery | 0 | 0 | Local SQLite identified as corpus authority; Railway is observer/broker only |
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

The definitive composition applies the frozen rule to C2 and records the
faithfulness-headroom finding plus Phase-E readiness in
`docs/TRUE_NORTH_BEST_STACK_AND_PHASE_E_READINESS_20260729.md`.

### Production shadow trial

Phase E stopped before provider dispatch because `data/factory.sqlite` was an
unreadable APFS dataless placeholder. No episode could be safely selected or
matched to a production Codex baseline, so agreement, drift, and amortized
cost remain unmeasured. Production state was not opened or mutated; no queue,
canonical, release, or label action occurred. See
`docs/TRUE_NORTH_PHASE_E_SHADOW_RESULTS_20260729.md`.

Configuration-only discovery then established that this local SQLite file,
not Railway Postgres, is the legacy podcast corpus and queue authority.
Railway hosts sanitized observer state and MCP broker metadata only. Current
episode count remains unknown without a separately authorized read; the latest
historical consistent snapshot records 464 episodes on 2026-07-11. See
`docs/TRUE_NORTH_PHASE_E_PRODUCTION_DISCOVERY_20260729.md`.

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
- Provider calls for Ruling 4 contract migration: `0`
- Provider calls for Phase C: `23`
- Provider calls for final schema-free C2: `19`
- Phase-D frontier result SHA-256:
  `0b804f327282baee0ff2be08a9282c4c8f5ba58705ae51ddeeb278433a53e201`
- Holdout opened: `false`
- Production source mutated: `false`
