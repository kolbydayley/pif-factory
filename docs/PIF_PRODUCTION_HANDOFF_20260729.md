# PIF Production Operations Handoff — 2026-07-29

This handoff is the starting point for the fresh production-operations thread.
The approved scale-up sequence remains in
`/Users/kolbydayley/.claude/plans/okay-how-do-we-virtual-tide.md`; this document
records only state and operational facts that are easy to rediscover
incorrectly.

## Verified current state

- Production schema is at migration 4. Migration-1 drift was repaired through
  an auditable baseline-adoption record; the original migration row and strict
  checksum enforcement remain intact.
- Release `crel_afe6f7df6b8db0fbd19590fe` is active at tier 25.
- Daily run `pdr_2fa4e2f3d7cba4ef3145d43f` is the first fully successful
  scale-gate run: all eight gates passed, blockers were empty,
  `genuinely_successful = 1`, and `consecutive_success_days = 1`.
- The streak counts consecutive distinct calendar dates for the same release
  and tier. Multiple successful runs on one date do not add days. A missing
  release ID, unrecognized tier, or unsuccessful current receipt produces a
  zero streak.
- At the latest backfill preflight there were 69,411 pending v3.1
  `label_segment` jobs, 4,978 context-gated episodes, 5,111 completed
  `ai_discourse_v3_1`/`gpt-5.5` labels, and zero `paid_api` pipeline rows.
- Subscription-only routing and the `gpt-5.5` production pin remain unchanged.

## Disabled-while-work-due stages

Three stages had the same latent failure mode. Each now defaults off, requires
explicit CLI enablement, measures satisfaction against the recent work window,
and reports the absolute backlog only as telemetry.

1. `bounded_baseline_extraction`: `--execute-extraction`
2. `rss_ingestion_and_due_transcript_strategies`: `--execute-ingestion`
3. `due_outcomes`: `--execute-outcomes`

The final handler audit found no fourth stage with this shape. Do not remove
the opt-in defaults.

## Controller status

The launchd controller is unloaded and must remain unloaded pending separate
authorization. `sdk_pipeline_controller.py` does not pass
`execute_extraction` (nor the newer ingestion/outcomes enables), so enabling
it now would restore skipped required-work stages. Do not load the agent or
modify its plist until the controller invocation is explicitly wired,
reviewed, and smoke-tested.

## Backup and restore point

The verified hydrated local backup is:

`~/pif-backups/factory-20260729T1527ET-before-schema-repair.sqlite`

SHA-256 begins `424a15b0` and ends `d70ed`. It is outside the iCloud-managed
Documents tree and is the schema-repair restore point. Off-device redundancy
still needs a separately authorized solution.

## Latest backfill result

Instrumentation and the `due_outcomes` enablement are committed at `e6f5dd8`.
The historical comparison is now post-batch, time/row bounded, and best-effort;
individual unreadable historical segments are skipped and counted.

The single authorized batch `pib_15f2efa5cf4f914f75633f4f` made zero provider
calls and produced zero labels. Its selected episode segment files were
dataless, so no context prompts were claimed; a zero-worker executor then
raised before inference. It was not retried. Full evidence is in
`docs/PIF_INSTRUMENTED_BACKFILL_STOP_20260729_V2.md`.

## Open items, in priority order

1. Resolve the dataless-input readiness problem for candidate episode
   selection and add a fail-closed zero-claimed-job result. Authorize a new
   batch separately; never treat this stop as throughput or quality evidence.
2. Continue the seven-day scale streak with at most one reviewed production
   cycle per distinct date and verify all eight gates each time.
3. Wire all three explicit execution flags into the controller, then review
   and smoke-test before any launchd re-enable.
4. Run the instrumented Codex backfill only after its input set is confirmed
   hydrated. Preserve the 40-context/400-label, 3-concurrency, 3,600-second
   bounds and all stop conditions.
5. Begin Phase B tier work only under a new directive. Phase C, GLM, and
   `extractor_gate` remain unopened.

