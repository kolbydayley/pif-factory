# Instrumented Backfill Stop — 2026-07-29

## Outcome

The one authorized instrumented backfill invocation stopped before provider
dispatch.

- Run ID: `pib_15f2efa5cf4f914f75633f4f`
- Model/route intended: `gpt-5.5`, Codex subscription
- Provider calls: 0
- Tokens: 0
- Episode contexts completed: 0
- Label segments completed: 0
- Labels written: 0
- GLM initialized: no
- Extractor gate initialized: no
- Cost telemetry: 0 new `paid_api` rows

The preflight selected a context batch, but the two newly enqueued
`episode_context` jobs were released as not ready because every segment file
needed by those episodes had the APFS `dataless` flag. The prompt claimer
therefore returned zero prompt jobs. The runner then called
`ThreadPoolExecutor(max_workers=0)` and raised:

```text
ValueError: max_workers must be greater than 0
```

This is an operational pre-inference stop, not a quality or throughput result.
The invocation was not retried.

## Production movement

The only observed database movement was two new pending `episode_context` jobs
with readiness errors:

- `437424` for `ep_4acb31130f3125e4b3b3a973`
- `437425` for `ep_41d276e16fe1ef9e6bef2dda`

Both remain pending with `attempts = 0`, no lease owner, and
`corpus_input_not_hydrated:dataless...` recorded as the error. Counts after the
stop were:

- All labels: 7,576
- `ai_discourse_v3_1` / `gpt-5.5` labels: 5,111
- Episode context runs: 544
- Episode-context jobs: 532 completed, 48 failed, 2 pending

No manifest of produced segment or label IDs exists because the batch produced
none. Audit distribution, validation-failure rate, repair/drop rate,
events-per-segment, evidence-span validity, historical comparison, throughput,
and days-to-clear are therefore unavailable rather than zero-valued.

## Follow-up blocker

Before another batch is authorized:

1. Hydrate candidate episodes' segment files or make candidate selection
   readiness-aware without materializing them during inspection.
2. Make an empty claimed-context batch a recorded fail-closed outcome rather
   than constructing a zero-worker executor.
3. Preserve the now non-blocking historical-baseline behavior and all existing
   quality/cost/isolation guards.

