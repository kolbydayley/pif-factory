# True-North Phase E One-Episode Pilot

Date: 2026-07-29

Experiment: `phase-e-one-episode-production-shadow-20260729-v1`

Snapshot basis: **July 20 local snapshot, not live production state**

Status: **stopped before provider dispatch at the complete-stack budget
preflight**

## Representative episode

Selected: Syntax, “1017, We need to stop calling it ‘AI’”
(`ep_7edbaadb04dc6b1ec57bfb70`), published 2026-07-01.

The three earlier selections contain 13, 16, and 19 stored segments. Syntax
has 16, the exact median, so it is representative by the review directive
rather than chosen as the smallest episode. Fourteen segments contain 261
downstream candidates; the remaining two have no candidate packet.

## Isolation

The executable preflight passed under the declared **60-call /
1,200,000-token** ceiling:

- the hydrated 4,052,508,672-byte database is not dataless;
- SQLite opened through `mode=ro` with `query_only=ON`;
- the production write probe was rejected;
- schema version 245 was present;
- the shadow root was distinct and writable; and
- production identity remained unchanged.

Isolation preflight SHA-256:
`4ca869d2166ff82bfb67a49b3c7470184bed53fc3bb4d3b8371e0ae1f0a85665`.

## Complete-stack sizing

The compound screen flags 104 candidates across all 14 candidate-bearing
segments. The optimistic exact-contract floor is:

| Certified lane | Calls |
|---|---:|
| GLM disposition pass A | 14 |
| GLM disposition pass B | 14 |
| Frozen Task-5 GLM decomposition | 14 |
| Sol compound decomposition, at most two immutable packets per envelope | 7 |
| Schema-free C2 Sol adjudication, one packet per call | 14 |
| **Optimistic floor** | **63** |
| Authorized ceiling | **60** |

The 63-call figure excludes disposition operational tiebreakers and all
provider or validation retries. It is therefore a lower bound, not a padded
reserve. Trimming either Sol lane or collapsing the segment-level semantic
contract would no longer run the certified best-of-lanes stack.

Budget preflight SHA-256:
`f295ffa987eb08f58af71f2d5dc0aa42ed1575da641a32eb5bf3202204a8379a`.

## Result

| Measure | Result |
|---|---:|
| Provider calls | 0 / 60 |
| Provider tokens | 0 / 1,200,000 |
| Measured optimistic per-episode call floor | 63 |
| Per-stage agreement/drift | Not measured; dispatch ineligible |
| Hybrid cost per retained valid atomic | Not measured |
| All-Codex cost per retained valid atomic | Not measurable from existing receipts |
| Production/shadow semantic mutations | 0 |
| Queue, canonical, release, or publication mutations | 0 |

The production label receipts for this episode contain no token-usage fields,
so a true all-Codex token ratio would remain unavailable even if the hybrid
ran. The benchmark's missing-receipt policy forbids estimating it.

This is an operational budget stop, not a quality or cost result. A future
pilot needs a ceiling above 63 calls with explicit contingency for
tiebreakers/retries; the exact reserve should be set before dispatch. The
cumulative campaign spend remains **309 calls / 3,583,991 known tokens**.
Sealed holdouts remained closed.
