# True-North Phase E Production Shadow Trial

Date: 2026-07-29

Experiment: `phase-e-three-episode-production-shadow-20260729-v1`

Status: **blocked before dispatch; isolation and comparability not provable**

## Strict-isolation preflight

The declared ceiling was recorded before preflight:
**60 calls / 1,500,000 tokens**. The intended shadow root was outside the
production database path, with production access restricted to SQLite
read-only URI mode plus `query_only`.

The production source at `data/factory.sqlite` is a 4,052,508,672-byte APFS
`dataless` placeholder (`st_flags=1073741920`). Read-only and immutable SQLite
metadata probes could not open it. A hydration request did not make it locally
available.

Without readable source state, the trial cannot safely establish:

- which three episodes are recent and eligible;
- whether matching production Codex outputs exist;
- production-table counts or content identity before and after the trial;
- stage-by-stage agreement or drift;
- the all-Codex production cost baseline.

The directive requires ambiguity about isolation to stop the phase. Provider
dispatch therefore remained disabled.

## Result

| Measure | Result |
|---|---:|
| Episodes selected or measured | 0 / 3 |
| Provider calls | 0 / 60 |
| Provider tokens | 0 / 1,500,000 |
| Production database opened | No |
| Shadow semantic writes | 0 |
| Queue mutations | 0 |
| Canonical mutations | 0 |
| Releases or labels published | 0 |
| Stage agreement/drift | Not measurable |
| Hybrid cost per retained valid atomic | Not measurable |
| All-Codex baseline cost | Not measurable |

This is an isolation-preflight block, not a business-value result. It neither
supports nor refutes the hybrid's production savings.

## Audit

- Preflight SHA-256:
  `385b7ad2555e946a55cf7414befe6897ed3fccad40d0d406ea6c4931455f4ab6`
- Cumulative campaign spend remains:
  **290 calls / 3,029,113 known tokens**
- Sealed holdouts remained closed.
