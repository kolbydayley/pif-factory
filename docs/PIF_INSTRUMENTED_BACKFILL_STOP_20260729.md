# Instrumented backfill stop — 2026-07-29

## Authorized scope

One context-first production backfill invocation was authorized after daily
run `pdr_2fa4e2f3d7cba4ef3145d43f` started the scale clock at day one.
The declared bounds were 40 episode contexts or 400 segment labels, 3,600
seconds, and concurrency 3, using only `gpt-5.5` through the Codex
subscription.

## Result

The invocation stopped during its local historical-baseline preflight, before
selecting or claiming production work and before starting a provider process:

```text
ValueError: segment_read_timeout
```

The timeout occurred while `_historic_baseline()` was computing comparison
metrics over older completed labels and `segment_for_job()` attempted to read
an older corpus segment. This was an operational preflight failure, not a
semantic model result.

## Verified safety state

- Provider calls made by the backfill: **0**
- Model tokens used by the backfill: **0**
- Episode-context jobs created or claimed by the backfill: **0**
- Label-segment jobs created or claimed by the backfill: **0**
- New labels produced by the backfill: **0**
- GLM initialized: **no**
- Pipeline lock contention: **no**
- Launchd controller enabled: **no**
- Release or promotion side effect: **none**

The invocation was not retried. This preserves the directive's one-batch and
no-loop constraint. The instrumentation implementation remains committed for
review, but the historic-baseline corpus-read timeout must be resolved under a
separate authorization before another production backfill attempt.

## Daily-cycle prerequisite that passed

The immediately preceding daily cycle completed five `gpt-5.5` Codex
subscription labels, moving `label_segment.completed` from 8,193 to 8,198.
All eight scale gates passed, `genuinely_successful=1`,
`consecutive_success_days=1`, `promotion_eligible=0`, and the required-stage
blocker list was empty.
