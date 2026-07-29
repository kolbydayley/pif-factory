# True-North Development-Fold Hybrid Certification

Date: 2026-07-29

Suite: `ai-safety-v1`

Measurement contract: `pif_true_north_measurement_contract_v7`

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

## Final nine-gate table

The table is component-based because Ruling 3 certifies a hybrid architecture.
Disposition metrics come from the option-2 certified ensemble, actor and
speaker from their contract-normative zero-call lanes, and decomposition,
faithfulness, and hallucination from the final frozen hybrid composition.

| Gate | Result | Final gate | Calibration margin | Coupled diagnostic | Pass |
|---|---:|---:|---:|---:|:---:|
| Candidate-state macro F1 | 0.863223 | >= 0.900000 | none; original benchmark target | n/a | No |
| Retained-value recall | 0.970711 | >= 0.900000 | none; option-2 contract | n/a | Yes |
| Intrinsic junk escape rate | 0.000000 | <= 0.020000 | none; option-2 contract | contamination 0 | Yes |
| Acceptable atomic-count rate | 0.797414 | >= 0.900000 | none; explicitly not re-referenced | n/a | No |
| Claim-text faithfulness | 0.733522 | >= 0.744435 | 0.040000 below 0.784435 ceiling | 0.558353 | No |
| Speaker exactness | 0.991379 | >= 0.954615 | 0.040000 below 0.994615 ceiling | 0.682493 | Yes |
| Reported-actor exactness | 0.780172 | >= 0.735385 | 0.040000 below 0.775385 ceiling | 0.537092 | Yes |
| Hallucination rate | 0.129032 | <= 0.093684 | 0.020000 above 0.073684 gold-vs-gold rate | 0.193548 live reading; 0.020000 aspiration | No |
| Schema parse success | 1.000000 | >= 0.990000 | none; original mechanical target | n/a | Yes |

Result: **5/9 gates pass.** The architecture is certified with limitations;
the benchmark as a whole has not passed.

## Per-gate provenance

| Gate | Authority | Measured basis |
|---|---|---|
| Candidate-state macro F1 | Original benchmark, retained by Ruling 3 | Three-class development consensus score; no ceiling recalibration |
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

### Faithfulness and macro F1

Matched-pair faithfulness is **0.733522 against 0.744435**, 1.09 points low.
Candidate-state macro F1 is **0.863223 against 0.90**, 3.68 points low. Both
remain visible certification limitations.

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

- **264 calls**
- **2,782,670 known tokens**
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
| **Post-160 total** | **104** | **1,604,856** | All negative or ineligible |

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

## Identified future work — not authorized

### Compound A/B/C consensus recipe

Test one bounded compound-only design with independent GLM and sol
decompositions followed by a cheap adjudicator that sees both. This mirrors the
reference A/B/C structure without assuming a single model can reproduce it.
It is identified work only; the decomposition experiment lane remains stopped.

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

- Manifest v7 SHA-256:
  `dad1321fa7d6a70c896710ef1bf65345bb8ee781b7659878e4151f4f7dca8c9e`
- Contract v7 SHA-256:
  `694feecd209c133dff80edcc407045509c924d72d73d4cf9ba09579082cd31fb`
- Gate policy v4 threshold:
  `hallucination_rate_proxy <= 0.093684`
- Hallucination calibration SHA-256:
  `f9b99c77045e0ad5daa183badc49426a17fdc9510bc7d414298479fc4c877ed8`
- Final hybrid diagnostic SHA-256:
  `11ce7d70d3c090d7b717e66cf4e33e668067115a81b567efa6e4b89165d2ea6f`
- Provider calls for Ruling 3: `0`
- Holdout opened: `false`
- Production source mutated: `false`

