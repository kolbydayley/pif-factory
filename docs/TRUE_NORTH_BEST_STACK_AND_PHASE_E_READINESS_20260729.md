# True-North Definitive Stack, Faithfulness Headroom, and Phase-E Readiness

Date: 2026-07-29

Provider calls: **0**

Sealed holdouts opened: **false**

Production accessed or mutated: **false**

## Definitive coherent composition

The frozen C2 output already includes the contract-normative actor-span rule.
Phase D suppression changes only `reported_actor`, using the actor, claim text,
and evidence text on each resulting atomic. It does not change decomposition,
disposition, speaker, evidence, or alignment. The composition is therefore
coherent:

- disposition: certified option-2 ensemble;
- decomposition: final schema-free C2 Sol adjudication;
- speaker: frozen candidate-prior adoption;
- actor: deterministic span rule plus Phase-D suppression at threshold 2.

The threshold suppresses 51 of 124 actor emissions on the C2 decomposition.

| Gate | Result | Gate | Coupled diagnostic | Pass |
|---|---:|---:|---:|:---:|
| Candidate-state macro F1 | 0.863223 | >= 0.790664 | raw agreement 0.807018 | Yes |
| Retained-value recall | 0.970711 | >= 0.900000 | terminal-hold 0.899582 | Yes |
| Intrinsic junk escape rate | 0.000000 | <= 0.020000 | contamination 0 | Yes |
| Acceptable atomic-count rate | 0.810345 | >= 0.900000 | n/a | No |
| Claim-text faithfulness | 0.732059 | >= 0.744435 | 0.562784 | No |
| Speaker exactness | 0.992218 | >= 0.954615 | 0.732759 | Yes |
| Reported-actor exactness | 0.782101 | >= 0.735385 | 0.577586 | Yes |
| Hallucination rate | 0.040323 | <= 0.093684 | 0.100806 | Yes |
| Schema parse success | 1.000000 | >= 0.990000 | n/a | Yes |

Result: **7/9 gates pass**. This is the definitive development-fold stack.
It improves atomicity over the prior coherent stack without sacrificing either
actor or hallucination compliance.

## Faithfulness headroom

All system comparisons below use the exact scorer's 256 strictly scoreable
matched pairs. Gold A versus gold B is the frozen 1,300-pair calibration.

| Distribution | Mean | Min | P10 | P25 | Median | P75 | P90 | Max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Candidate verbatim vs gold | 0.706890 | 0.287500 | 0.514924 | 0.606834 | 0.703065 | 0.817448 | 0.899254 | 1.000000 |
| Emitted text vs gold | 0.732059 | 0.375000 | 0.534473 | 0.632117 | 0.743726 | 0.844530 | 0.916558 | 1.000000 |
| Gold A vs gold B | 0.784435 | 0.092742 | 0.573451 | 0.670069 | 0.801834 | 0.913613 | 1.000000 | 1.000000 |
| Pairwise best of emitted or candidate | 0.736442 | 0.375000 | 0.540985 | 0.634674 | 0.747066 | 0.848750 | 0.922324 | 1.000000 |

Emitted wording is below the candidate-verbatim baseline on only **17/256
pairs (6.64%)**. Replacing just those emissions with the better candidate
wording would add at most 0.004383 and reach only **0.736442**, still 0.007993
below the gate.

The decomposition-conditioned split is decisive:

| Pair group | Pairs | Mean faithfulness |
|---|---:|---:|
| Candidate atomic count acceptable | 206 | 0.751302 |
| Candidate atomic count unacceptable | 50 | 0.652774 |

The aligned group already passes the 0.744435 gate. The unacceptable-count
group is 9.85 points lower. Candidate wording is not suppressing performance:
the emitted minimal edits improve over candidate verbatim by 2.52 points.

**Finding:** wording-only correction cannot close the gate, even under the
optimistic pairwise wording oracle. The remaining shortfall is structurally
tied to atomic alignment and the permanently closed decomposition lane. No
rewriting stage was proposed or run.

## Phase-E readiness

The 3-episode shadow trial is prepared but not opened. Its ceiling is recorded
as **60 calls / 1,500,000 tokens**.

The executable preflight now fails closed unless all of these are true:

1. the local production SQLite is no longer APFS dataless;
2. the shadow root is neither the production file nor its ancestor/descendant;
3. SQLite opens through `mode=ro`;
4. `PRAGMA query_only=ON` is observed;
5. an attempted schema write is rejected;
6. the shadow root accepts its own isolated write probe; and
7. production file identity, size, flags, and mtime remain unchanged.

Once separately authorized, selection chooses the three newest
transcript-ready episodes that already have accepted/completed Codex or GPT
baseline outputs. It fails if fewer than three qualify.

The frozen measurement plan compares:

- disposition macro F1, retained recall, and junk escape;
- atomic count, claim alignment, and faithfulness;
- speaker, actor, and hallucination;
- subject clusters, relations, and singleton preservation;
- provider calls and input/output/total tokens per retained valid atomic for
  hybrid and all-Codex;
- hybrid/all-Codex call and token ratios.

Missing all-Codex usage receipts fail the cost measurement; they are never
estimated.

Current readiness: **prepared but blocked**. The production SQLite remains a
dataless placeholder, production access has not been separately authorized,
and the recorded provider budget is not open.

## Audit bindings

- Best-stack predictions SHA-256:
  `c1f90d1847e18c32b9423881b956c9b45eb9fe69d94f3d64205ee9ac6f7a486f`
- Best-stack rescore SHA-256:
  `c43c574261026e6b022c961147b61ae41d73531ae25e2c5b485e5fa75e2c24c1`
- Faithfulness diagnostic SHA-256:
  `fbb1357aa5b21a4272dc290ee7eed5f96715d63863064bc42c4b9114ce88d416`
- Phase-E measurement-plan SHA-256:
  `29d206d67b1db8bee5ff2b9430864a6aacdab8f866c85acca2655951a82d4e24`
- Cumulative spend:
  **309 calls / 3,583,991 known tokens**
