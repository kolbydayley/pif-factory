# True-North Phase E One-Episode Execution Stop

Date: 2026-07-29

Experiment: `phase-e-one-episode-production-shadow-executed-20260729-v1`

Snapshot basis: **July 20 local snapshot, not live production state**

Status: **stopped before provider dispatch because the required historical
comparison is not measurable**

## Authorized scope and episode

The ceiling was declared as **90 calls / 1,800,000 tokens** before any provider
call. The selected episode remains Syntax, “1017, We need to stop calling it
‘AI’” (`ep_7edbaadb04dc6b1ec57bfb70`), the exact 16-segment median of the prior
13/16/19 set.

The isolation preflight passed. The complete-stack budget preflight also
passed: the optimistic certified floor is 63 calls.

## Measurement preflight

The production snapshot does not contain the reference required by the
directive:

| Baseline fact | Measured |
|---|---:|
| Historical model | GPT-5.5 |
| Completed label calls | 16 |
| Completed context calls | 1 |
| Readable output artifacts | 17 / 17 |
| Output artifacts containing token usage | 0 / 17 |
| Production claim rows | 184 |
| Production atomic-claim rows | 0 |

The 184 `claims` rows are upstream extraction outputs. They are not atomic
downstream claims and cannot serve as a decomposition or canonicalization
reference. Consequently:

- candidate-extraction comparison is partially available;
- decomposition comparison is unavailable;
- canonicalization comparison is unavailable; and
- true historical Codex tokens and tokens per retained valid atomic are
  unavailable.

The governing missing-receipt rule says to fail measurement, never estimate.
Running the hybrid would spend at least 63 calls while still being unable to
produce the required true token ratio or downstream stage drift.

## Lane-level sizing finding

Even before execution, the exact topology answers the Codex-call direction:

| Lane | Optimistic calls |
|---|---:|
| Hybrid GLM | 42 |
| Hybrid Sol/Codex | 21 |
| Historical all-Codex production | 17 |

The hybrid Codex lane is therefore at least **21 / 17 = 1.2353×** the
historical all-Codex call count, a **23.53% increase**, before retries or
tiebreakers. It does not move work off the Codex lane on this episode by call
count. Token consumption cannot be compared truthfully because the historical
receipts contain no usage.

This is a structural preflight finding, not an amortized cost result: no hybrid
atomic corpus was produced, so no per-retained-atomic denominator exists.

## Result

| Measure | Result |
|---|---:|
| Provider calls | 0 / 90 |
| Provider tokens | 0 / 1,800,000 |
| Per-stage agreement/drift | Not measurable end to end |
| GLM tokens per retained atomic | Not measured |
| Sol tokens per retained atomic | Not measured |
| Historical Codex tokens per retained atomic | Unavailable |
| Queue/canonical/release/publication mutations | 0 |
| Shadow semantic writes | 0 |

Preflight bindings:

- isolation:
  `01e34c0569c2923d88238674ee0bfe558004dacd3e1ffd374f3f5f82598115c5`
- complete-stack budget:
  `9cb8bba91cb8b3ca85903bf79bb15fa4e96e7c0d585cc62718bfd6e6fe795dac`
- baseline measurement:
  `34f85820fd3fa6d22009b8c38ab86428aa22235f6527e728461dc22040e7e39a`

Cumulative campaign spend remains **309 calls / 3,583,991 known tokens**.
Sealed holdouts remained closed.
