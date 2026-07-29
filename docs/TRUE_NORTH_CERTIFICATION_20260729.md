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

The single authorized C2 revision attempted to put `gpt-5.6-sol` in the
adjudicator seat using the exact Phase C packets. Codex's response-format API
rejected the frozen schema and one bounded compatibility adapter before model
execution. C2 therefore has no semantic metrics. The full gold-class recipe
did not transfer operationally through the current structured-output harness;
this is not represented as a Sol quality result. Per the terminal ruling, the
decomposition experiment lane is permanently closed. See
`docs/TRUE_NORTH_C2_SOL_ADJUDICATOR_RESULTS_20260729.md`.

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
| Acceptable atomic-count rate | 0.797414 | >= 0.900000 | none; explicitly not re-referenced | n/a | No |
| Claim-text faithfulness | 0.733522 | >= 0.744435 | 0.040000 below 0.784435 ceiling | 0.558353 | No |
| Speaker exactness | 0.991379 | >= 0.954615 | 0.040000 below 0.994615 ceiling | 0.682493 | Yes |
| Reported-actor exactness | 0.780172 | >= 0.735385 | 0.040000 below 0.775385 ceiling | 0.537092 | Yes |
| Hallucination rate | 0.129032 | <= 0.093684 | 0.020000 above 0.073684 gold-vs-gold rate | 0.193548 live reading; 0.020000 aspiration | No |
| Schema parse success | 1.000000 | >= 0.990000 | none; original mechanical target | n/a | Yes |

Result: **6/9 gates pass.** The architecture is certified with limitations;
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
| Reported actor | Ruling 1 | Matched-pair ceiling 0.775385 minus 0.04; deterministic span rule is normative |
| Hallucination | Ruling 3, under explicit owner delegation | Matched-pair gold-vs-gold rate 84/1140 = 0.073684 plus the lower-is-better 0.02 margin |
| Schema | Original benchmark | Mechanical parse target; no semantic ceiling |

The hallucination live/coupled gold-vs-gold rate is 166/1140 = 0.145614.
It remains a mandatory diagnostic. The original <=0.02 target remains visible
as an aspiration and is not represented as the attainable inter-annotator
gate.

## Disclosed limitations

### Atomic decomposition

Atomicity is **0.797414 against 0.90**, a 10.26-point shortfall. The single-pass
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
C2 then attempted the gold-author-class adjudicator, but the Codex
structured-output API rejected `oneOf` and the single bounded flattened-schema
adapter rejected `uniqueItems`, both before model execution. Consequently the
gold-class adjudicator's semantic quality remains unmeasured. The recipe failed
to transfer operationally under the authorized frozen harness, and the
decomposition lane is permanently closed rather than adapted again.

### Faithfulness

Matched-pair faithfulness is **0.733522 against 0.744435**, 1.09 points low.
Candidate-state macro F1 now passes its Ruling-4 ceiling-referenced gate; its
original 0.90 aspiration remains visible.

### Hallucination

The final matched-pair reading is **0.129032 against 0.093684**. The live
coupled diagnostic is **0.193548**, and the original aspiration is 0.02.
Ruling 3 fixes the measurement-reference defect but does not turn the current
stack into a pass.

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

The practical split is therefore approximately 58.59% cheap/non-compound
handling and at most 41.41% Codex-class compound routing, plus zero-call
speaker and actor rules. Actual production cost is not yet certified because
the Codex-class compound recipe remains untested.

## Campaign ledger summary

Authoritative cumulative campaign spend:

- **289 calls**
- **3,029,113 known tokens**
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
| C2 Sol adjudicator | 2 | 0 | Structured-output schema rejected before inference; lane closed |
| **Post-160 total** | **129** | **1,851,299** | All negative, ineligible, or operationally blocked |

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

## Post-certification experiment result

### Compound A/B/C consensus recipe

The bounded compound-only design was executed once under Ruling 4. It reached
0.801724 full-fold atomicity and 0.657143 on the flagged subset, with
faithfulness and hallucination also below gate. The design is frozen as a
terminal quality failure; no self-iteration occurred. The one authorized C2
revision could not invoke the Sol adjudicator because the frozen structured
output schema and one bounded compatibility form were rejected before
inference. The lane is permanently closed.

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
- Holdout opened: `false`
- Production source mutated: `false`
