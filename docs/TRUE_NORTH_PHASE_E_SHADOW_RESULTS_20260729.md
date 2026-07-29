# True-North Phase E Production Shadow Trial

Date: 2026-07-29

Experiment: `phase-e-three-episode-production-shadow-final-20260729-v1`

Snapshot basis: **July 20 local snapshot; historical corpus count 464
episodes as of July 11**

Status: **stopped before provider dispatch at the exact-contract budget
preflight**

## Isolation preflight

Kolby hydrated `data/factory.sqlite` before this run. The executable preflight
independently verified:

- 4,052,508,672 readable bytes and no APFS `dataless` flag;
- SQLite URI `mode=ro` plus `PRAGMA query_only=ON`;
- a rejected production write probe;
- a distinct writable shadow path;
- production file identity unchanged through the check; and
- production schema version 245.

Isolation preflight SHA-256:
`b6687e42c6e13566b2b9b859fea1f35806ee5cb3b0393794e814487363eecd67`.

## Selected episodes

The read-only selector chose the three most recent transcript-ready episodes
with existing GPT/Codex baseline outputs:

| Published | Show | Episode | Segments | Baseline claims |
|---|---|---|---:|---:|
| 2026-07-03 | Odd Lots | How a Major Grocery Store Chain Can Dramatically Lower the Cost of Food | 19 | 217 |
| 2026-07-02 | Practical AI | Image Generation and Visual Intelligence with Black Forest Labs | 13 | 110 |
| 2026-07-01 | Syntax | 1017, We need to stop calling it “AI” | 16 | 184 |
| **Total** |  |  | **48** | **511** |

The three episodes contain 714 production discourse-event candidates. They
have no production `atomic_claims` rows, so the existing baseline is a
claim-level GPT-5.5 output rather than an atomic downstream reference.

## Whole-run budget preflight

The certified architecture is segment scoped. Preserving its evaluated input
contract requires:

| Fixed stage | Minimum calls |
|---|---:|
| GLM disposition pass A | 48 |
| GLM disposition pass B | 48 |
| Frozen Task-5 GLM decomposition | 48 |
| **Fixed-stage floor** | **144** |
| Authorized ceiling | **60** |

This 144-call floor excludes the compound-only Sol decomposition and Sol
adjudication calls, so no additional measurement is needed to prove that the
run cannot fit. Cross-segment batching could reduce the call count, but it
would change the packet contract and would no longer be the certified stack.

Budget preflight SHA-256:
`2a56742b7f8e15e87b2f42cbbdecd2420f0a7b79afe8de9121141521b62538b0`.

## Result

| Measure | Result |
|---|---:|
| Episodes selected | 3 / 3 |
| Provider calls | 0 / 60 |
| Provider tokens | 0 / 1,500,000 |
| Production access | Read-only, preflight and selection only |
| Shadow semantic writes | 0 |
| Queue/canonical/release/label mutations | 0 |
| Per-stage agreement/drift | Not measured; dispatch ineligible |
| Hybrid cost per retained valid atomic | Not measured |
| All-Codex token baseline | Unavailable; production label receipts omit usage |

This is an operationally ineligible Phase-E run, not a quality or
business-value result. It does not support or refute production savings. The
cost experiment needs either a larger explicitly authorized call ceiling or a
newly benchmarked cross-segment packet contract. Neither change was authorized
here.

Cumulative campaign spend remains **309 calls / 3,583,991 known tokens**.
Sealed holdouts remained closed.
