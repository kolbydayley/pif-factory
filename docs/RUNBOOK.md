# Runbook

## Normal Controller Cycle

```bash
cd /Users/kolbydayley/Documents/Codex/podcast-intelligence-factory
python3 -m research_factory init
python3 -m research_factory verify-sources --source-list config/sources.yaml
python3 -m research_factory enqueue --lane podcast --since 2025-01-01 --source-list config/sources.yaml --label-pack ai_discourse_v3_1
python3 -m research_factory queue sync-envelopes
python3 -m research_factory queue status --by lane,content_type,role,status
python3 -m research_factory railway-cost-guard
python3 -m research_factory transcript-candidates --lane podcast --limit 40 --claim --worker-id transcript-discovery
python3 -m research_factory run --lane podcast --limit 100 --job-types fetch_transcript,prepare_transcript --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller
python3 -m research_factory prepare-transcripts --lane podcast --limit 50 --label-pack ai_discourse_v3_1 --enqueue-labels --priority 25 --pilot-id controller-v31
python3 -m research_factory run --lane podcast --limit 25 --job-types episode_context,label_segment --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller
python3 -m research_factory scale-batch-report --pilot-id controller-v31
python3 -m research_factory snapshot --output exports/observer-snapshot.json
```

For v3.1, GPT-5.5 should read full episode context before extraction. Deterministic code may validate schemas, offsets, privacy, queue state, and graph invariants, but it should not replace AI-led extraction with regex matching.

Efficiency backtest for the golden v3.1 labels:

```bash
python3 -m research_factory efficiency-backtest --output exports/efficiency-backtest-full-chunk10-v31-20260711.json
python3 -m research_factory efficiency-chunk-sweep --chunk-sizes 10,12,15,20 --report-dir exports/efficiency-chunk-sweep-v31-20260711 --output exports/efficiency-chunk-sweep-v31-20260711.json
```

The latest consistent-snapshot 2026-07-11 full golden sweep covered `464` episodes, `4,966` labels, `82,977` discourse events, and `28` sources. `episode_chunk_sparse_compact_v1` is the current cost candidate. The sweep recommends `10` segments per chunk as the smallest size that clears the quarter targets: estimated total tokens were `0.247179` of the current segment-plus-episode-context path, input tokens were `0.100875`, request count was `0.138325`, and runtime proxy was `0.123396`. `12` segments per chunk gives better margin (`0.243676` total-token ratio and `0.110727` runtime proxy) while staying bounded enough for the next moderate live smoke. Chunk `20` gives the largest measured margin in the refreshed sweep (`0.235943` total-token ratio and `0.082757` runtime proxy) but should be treated as higher output-window risk until live quality is healthy on smaller chunks. The sparse representation expanded back to exact `ai_discourse_v3_1` labels for all `4,966` labels and passed sampled validation on `300` segments.

Full-episode sparse compact extraction remains cheaper by estimate (`0.233753` total-token ratio), but live Codex smoke tests on coded-heavy prompts showed response-window or stream-retry stalls. Keep the bounded chunk path as the candidate and do not make it the production default until a held-out GPT-5.5 candidate-output directory is compared with `--candidate-output-dir` and the `candidate_quality_gate` passes. The gate currently requires total-token ratio <= `0.25`, runtime ratio <= `0.25`, exact compact roundtrip, no missing candidate segments, no validation failures, status accuracy >= `0.95`, event precision >= `0.90`, and event recall >= `0.90`.

To create local Codex/GPT-5.5 handoffs for that held-out test without API billing:

```bash
python3 -m research_factory efficiency-backtest --per-source-limit 1 --export-candidate-prompts work/efficiency-sparse-chunk10-heldout-20260710
python3 -m research_factory efficiency-candidate-status --manifest work/efficiency-sparse-chunk10-heldout-20260710/manifest.json --min-expected-events 25 --max-expected-events 80 --next-limit 5
python3 -m research_factory efficiency-smoke-candidates --manifest work/efficiency-sparse-chunk10-heldout-20260710/manifest.json --limit 5 --dry-run --min-expected-events 25 --max-expected-events 80
python3 -m research_factory efficiency-smoke-candidates --manifest work/efficiency-sparse-chunk10-heldout-20260710/manifest.json --limit 3 --max-active-gpt55 0 --min-expected-events 25 --max-expected-events 80
python3 -m research_factory efficiency-backtest --per-source-limit 1 --candidate-output-dir work/efficiency-sparse-chunk10-heldout-20260710/outputs
```

Prompt files are private local artifacts and may contain transcript text; the manifest, candidate status, and backtest report stay sanitized. The held-out chunk export on 2026-07-10 produced `49` chunk prompts across `28` sources and estimated `0.248056` total-token ratio, `0.100155` input-token ratio, `0.140805` request ratio, and `0.136980` runtime proxy.

The better-margin held-out candidate is `12` segments per chunk:

```bash
python3 -m research_factory efficiency-backtest --per-source-limit 1 --candidate-chunk-size 12 --candidate-representation sparse_compact --export-candidate-prompts work/efficiency-sparse-chunk12-heldout-20260711 --output exports/efficiency-backtest-heldout-chunk12-v31-20260711.json
python3 -m research_factory efficiency-candidate-status --manifest work/efficiency-sparse-chunk12-heldout-20260711/manifest.json --min-expected-events 25 --max-expected-events 80 --next-limit 5
python3 -m research_factory efficiency-readiness --manifest work/efficiency-sparse-chunk12-heldout-20260711/manifest.json --sweep exports/efficiency-chunk-sweep-v31-20260711.json --next-limit 5
python3 -m research_factory efficiency-smoke-candidates --manifest work/efficiency-sparse-chunk12-heldout-20260711/manifest.json --limit 1 --max-active-gpt55 0 --wait-for-clear-seconds 1800 --wait-poll-seconds 60 --min-expected-events 25 --max-expected-events 80
python3 -m research_factory efficiency-backtest --per-source-limit 1 --candidate-chunk-size 12 --candidate-representation sparse_compact --candidate-output-dir work/efficiency-sparse-chunk12-heldout-20260711/outputs --output exports/efficiency-backtest-heldout-chunk12-candidate-comparison-v31-20260711.json
```

The chunk-12 held-out export produced `44` chunks across the same `28` sources. After the compact prompt added explicit `h` temporal-horizon enums plus the targeted `terms` requirement for `term_usage`, `product_signal`, and `capability_claim`, it estimates `0.245007` total-token ratio, `0.095576` input-token ratio, `0.126437` request ratio, and `0.123005` runtime proxy. The readiness command stays sanitized and currently reports `candidate_outputs_missing` until candidate outputs are present; it also reports advisory `candidate_chunk_size_differs_from_sweep_recommendation` because the conservative sweep recommendation is chunk `10` while chunk `12` is the better-margin live-smoke candidate. Its next moderate smoke queue starts with JS Party, Talk Python To Me, and BG2Pod chunks in the `25-80` expected-event band.

Live smoke evidence on 2026-07-10/11: the JS Party chunk `ep_06b9e423405b86b50749d5a5-7387ae6100-c03` has `5` coded segments and `74` expected discourse events. The enum-plus-targeted-terms prompt produced valid JSON and all `5` labels validated, but quality was below gate: `54` candidate events, `44` matched events, status accuracy `1.0`, event precision `0.814815`, event recall `0.594595`, and F1 `0.6875`. Two prompt-only recall variants regressed and should not be restored without a new hypothesis: added density/type-check language produced `1` validation failure and F1 `0.645833`; schema-enforced non-empty `terms` produced `3` validation failures and F1 `0.634147`. A final rerun of the best-known prompt timed out at the wrapper limit with no JSON output; process-group cleanup left no active GPT-5.5 child process and status treated the zero-byte output as missing. Next quality work should test a different representation or validator-assisted repair/comparison path rather than adding more admonitions to the sparse prompt.

The 2026-07-11 full-corpus flat-ledger sweep covered the same `464` episodes, `4,966` labels, `82,977` events, and `28` sources. Positional `flat_ledger` encoding is lossless after local offset hydration and estimates `0.227186` total-token ratio even at one segment per request when using the concise extraction brief. It is not quality-ready: a valid one-segment low-reasoning run reached F1 `0.666667`; full v3.1 instructions improved low-reasoning F1 to `0.785714`; high reasoning regressed to `0.716981`. Do not promote positional ledgers based on token estimates alone.

Named `evidence_sparse_compact` output with strict structured generation, one segment per call, and the existing GPT-5.5 episode-context plus adjacent-segment artifacts is the best live direction so far. A low-reasoning two-scan run on a 30-event Decoder holdout segment validated cleanly with status accuracy `1.0`, precision `0.742857`, recall `0.866667`, and F1 `0.8`. The context-free equivalent was worse. This still fails the `0.90` precision/recall gate. Next work should add an LLM event verifier/omission repair stage and measure cached input tokens plus bounded parallel wall time; do not add regex, keyword, or local-language extraction gates, because every transcript segment must remain LLM-read and all semantic extraction must remain model-generated.

Later 2026-07-11 controls refined that conclusion. Generic event-boundary guidance raised the same segment to precision `0.8`, recall `0.933333`, and F1 `0.861538`; a conservative low-reasoning verifier took under `15` seconds but could not raise precision without losing matched events. Four-way candidate execution completed eight additional sources in roughly one-eighth of the original sequential baseline wall time, with individual calls taking `109`-`176` seconds, but nine-source aggregate quality was only precision `0.762712`, recall `0.551020`, and F1 `0.639810`. Do not treat the strong Decoder-with-Nilay segment as representative.

Original-pipeline self-consistency reruns are the required quality control. On four attempted sources, high-reasoning full-schema calls took `295`-`548` seconds and one timed out at `600` seconds. Against stored golden labels: BG2Pod rerun F1 was `0.884615` (and failed validation), Decoder was `0.949152`, Lex Fridman was `0.777778`, and Odd Lots timed out. The efficient candidate matched the Lex ceiling but badly under-extracted BG2Pod and Decoder. Medium/high reasoning, unconstrained JSON, proposition-first extraction, and proposition classification did not repair this consistently. Keep the goal active: quarter wall time is demonstrated through low reasoning plus concurrency, but broad same-quality extraction and quarter token cost remain unproven.

Actual `codex exec --json` telemetry on one Twenty Minute VC segment established the prompt-only frontier. The original high-reasoning full-schema path used `177,089` raw tokens in `257.753` seconds and reproduced the stored label at F1 `0.818182`. Low-reasoning evidence-sparse extraction used `38,399` raw tokens (`0.216836` of baseline) in `84.260` seconds but reached only F1 `0.761905`. Slim-input full-schema high reasoning reached F1 `0.952381`, but used `126,754` raw tokens (`0.715770` of baseline) in `205.263` seconds. Prompt/schema changes therefore trade quality against cost rather than satisfying both gates.

The next candidate is supervised LLM distillation, not lexical gating. `efficiency-distillation-export` creates private source-held-out splits from all `28` sources, `464` episodes, `4,966` labels, and `82,977` events. Compact context retains the complete current transcript segment plus model-produced summary, identities, entity/concept seeds, and extraction guidance; it removes adjacent transcript duplication, section maps, verbose speaker notes, and operational metadata. The resulting train/validation/test source split contains `16/6/6` sources and `2,724/954/1,288` examples.

Whole-label QLoRA is not viable on the 32 GB M4 host: compact full-label training sequences have median `9,543` tokens and only `24.52%` fit within 8,192 tokens; 16k runs exhausted Metal memory. Use a two-stage all-LLM pipeline instead. Stage one reads the complete segment and emits evidence-backed event detections; stage two enriches detections into every v3.1 field, with no regex, keyword, or local semantic classifier. The compact detection representation fits all `2,724` training examples within 8,192 tokens (median `4,959`, maximum `8,191`). A Qwen3 1.7B 4-bit, 8-layer LoRA backprop smoke reached finite validation loss with about `11.1 GB` peak memory. Held-out generation and full v3.1 enrichment quality remain required before promotion.

Local micro-pilots identified a second stop rule. Unbounded event-list targets caused greedy decoding collapse after 80 cumulative updates on Qwen3 1.7B and after 40 updates on Qwen3 4B: outputs repeated events or fields until the token cap. A line ledger did not fix collapse inside long event lines. Bounded generation does fix structure: predict event count, then generate exactly one ranked event array per call, followed by per-event enrichment. Its `5,336`-example pilot corpus has median full sequence `2,965`, maximum `5,607`, and target maximum `256` tokens. After 60 rank-only updates, held-out calls produced short JSON arrays in `11`-`24` seconds, but all three sampled events missed the existing event-similarity threshold. Do not scale these adapters yet. A meaningful local epoch over `2,612` rank examples is overnight-scale because every update still reads the full transcript; the next training attempt should use a substantially faster GPU lane or an optimized batched/cached trainer, then rerun source-held-out event matching before building enrichment.

Later controls rejected the remaining local and prompt-only variants. Extending Qwen3 1.7B rank-only training to `300` cumulative updates yielded only one matched event across six held-out podcast sources, four structurally valid outputs, zero event-type matches, and mean similarity `0.2303`. Pretrained Qwen3 8B did not improve that task. High-reasoning GPT-5.5 proposition extraction across six sources reached precision `0.452962`, recall `0.769231`, and F1 `0.570176`; proposition units collapse the multiple v3.1 event labels that can legitimately share evidence. Medium-reasoning compact typed-event cores produced precision `0.448718`, recall `0.414201`, and F1 `0.430769`. High-reasoning evidence-sparse full extraction validated all six labels but reached precision `0.75`, recall `0.479290`, and F1 `0.584838`, with calls taking `117`-`182` seconds. Do not spend more smoke calls on prompt/schema combinations without a new model or training hypothesis.

No approved cloud-training lane or credentials were present on the local machine when checked. The fastest remaining path is supervised distillation on substantially faster GPU hardware, using the existing source-held-out private corpus and the all-LLM constraint. External paid compute requires explicit approval; until then, preserve the exports and adapters locally and keep the production extractor unchanged.

This training conclusion was superseded by the Codex-only constraint on 2026-07-11: do not custom-train or fine-tune models for this optimization. Use subscription-backed headless Codex calls only. Exact locally available lanes were `gpt-5.6-luna`, `gpt-5.4-mini`, `gpt-5.3-codex-spark`, `gpt-5.6-sol`, and `gpt-5.6-terra`.

The first controlled 30-event Decoder-with-Nilay holdout established the model matrix. With compact evidence-sparse prompting, Luna used `39,049` tokens in `90.5` seconds at F1 `0.372093`; Mini used `39,413` tokens in `103.9` seconds at F1 `0.352941`; Spark used `38,750` tokens in `23.7` seconds at F1 `0.190476`. With the original full-schema prompt, Luna used `41,738` tokens in `94.0` seconds at F1 `0.45`; Mini used `214,606` tokens in `202.9` seconds at F1 `0.465116`; Spark used `180,369` tokens in `24.6` seconds at F1 `0.243903`; Terra used `85,486` tokens in `142.7` seconds at F1 `0.558140`. Sol was the only quality-capable new lane: original full schema used `53,446` tokens in `218.9` seconds with precision `0.92`, recall `0.766667`, and F1 `0.836364`.

Additional Sol prompt structures did not improve the frontier. Evidence-sparse Sol met the token target at `39,494` but fell to F1 `0.566038`. Three-segment full-schema batching used only `48,103` tokens for `71` expected events but underextracted to `21` events and F1 `0.347826`. Familiar-name lean schema used `42,736` tokens at F1 `0.608696`. Model-generated self-count plus a full label used `41,402` tokens at F1 `0.716981`; adding the codebook used `45,028` at F1 `0.703704`; dual independent scans used `45,195` at F1 `0.548387`. A six-source, four-concurrent self-count/full-label run completed in `287.405` wall seconds and averaged `38,039` tokens per segment, but aggregate precision was `0.681319`, recall `0.407895`, and F1 `0.510288`. Keep Sol original full schema as the quality anchor; do not promote the lower-token variants based on one strong segment.

`efficiency-smoke-candidates` now adds `--json` headless telemetry and reports sanitized `input_tokens`, `cached_input_tokens`, `output_tokens`, `reasoning_output_tokens`, and `total_tokens`. Logs and outputs remain private artifacts. Continue the Codex-only goal from the measured Sol quality anchor; Spark is suitable for fast auxiliary checks, not final extraction, and Luna/Mini are undercoverage-prone on dense segments.

The expanded matrix is stored at `exports/headless-model-matrix-20260711.json`. On the identical 30-event dense holdout, GPT-5.4 low used `45,512` tokens at F1 `0.612245`; GPT-5.4 medium regressed to `185,314` tokens and `289.9` seconds at F1 `0.653061`; Luna high regressed to `278,824` tokens and `449.5` seconds at F1 `0.642857`. Sol minimal is unsupported for this structured extraction call. Do not escalate reasoning on Luna or GPT-5.4: retries and accumulated input dominate any small recall gain.

Persistent `codex exec resume` sessions are also dominated for raw-token optimization. A resumed second segment accumulated `197,893` input and `214,790` total tokens because prior prompts and outputs remained in conversation, while F1 stayed `0.545454`. Same-episode golden calibration anchors confused Luna into `insufficient_evidence` with zero events. These mechanisms may increase cache discounts but do not meet the raw-token objective.

A broader original full-schema Sol run covered seven dense segments across six sources: aggregate precision `0.762376`, recall `0.425414`, and F1 `0.546099`, averaging `51,900` tokens per segment. Segment F1 ranged from `0` to `0.836364`, so the strong initial Sol result did not generalize by itself. This full-schema setup remains rejected; the later windowed-core architecture below replaced it.

The no-repeat semantic-cascade hypothesis also failed. Spark read the complete dense segment once and emitted `27` exact-evidence inventory items with `68` atomic claims using `27,634` tokens. Sol then received only that inventory plus model-generated episode context and produced the full label using `39,937` tokens. Combined usage was `67,571` tokens and quality was precision `0.5`, recall `0.433333`, F1 `0.464286`. Atomic inventories do not preserve the event-boundary, multi-lens, and attribution decisions needed to reproduce v3.1 labels; do not add an inventory/enrichment cascade without a materially different hypothesis.

Sol semantic partitioning clarified the recall/precision frontier. Four non-overlapping full-event partitions in one call used `45,441` tokens and found `24/30` golden events (recall `0.8`) but emitted `46` events (precision `0.521739`, F1 `0.631579`). Model confidence could not separate matches from false positives, and there were no exact duplicates to remove mechanically. Asking Sol to rewrite a consolidated final list used `42,821` tokens but retained only `14/30` matches. Asking it to select immutable candidate IDs used `39,714` tokens, but it kept all `26` candidates in that run and reached F1 `0.571429`. Partitioning is the only single-call setup to reach `0.8` recall near the token target, but no tested in-call precision pass preserves that recall.

A separate Spark precision judge over the strongest 46 Sol partition candidates used another `38,151` tokens. It kept `40` events, reduced matched events from `24` to `21`, changed precision only from `0.521739` to `0.525`, reduced recall from `0.8` to `0.7`, and produced F1 `0.6`; combined extraction-plus-judge usage was `83,592` tokens. Model separation does not repair partition precision within the quarter-token envelope.

Dense compact chunks are not a viable quarter-cost candidate without another compression step. The 2026-07-11 full golden compact sweep covered `464` episodes, `4,966` labels, `82,977` discourse events, and `28` sources. `episode_chunk_compact_v1` preserved exact label roundtrip, but even large chunks missed the total-token gate: chunk `10` estimated `0.291214`, chunk `20` estimated `0.280913`, and chunk `40` still estimated `0.279328` total-token ratio. Runtime ratios were good, but output-token cost alone keeps this representation above the `0.25` target, so do not spend live GPT-5.5 smoke time on dense compact unless paired with a new compression or selective-repair strategy.

### Codex-only windowed extraction candidate

The first configuration to meet the measured token target is a two-stage headless Codex path:

1. `gpt-5.6-sol` at low reasoning reads four non-overlapping windows in one call. Each window includes bounded local context plus the existing model-generated episode summary, speaker map, entity/concept context, relevant section, extraction guidance, and adjacent segments. Sol emits an immutable lean event core with exact evidence.
2. One `gpt-5.3-codex-spark` low-reasoning call enriches a batch of immutable event cores. Exact-count `b0`...`bN` arrays and short semantic keys remove copied segment IDs and enforce every event ID. Local code joins by position, hydrates documented defaults, resolves exact evidence offsets, grounds metrics by exact evidence substrings, and validates full `ai_discourse_v3_1` labels.

Run it from an existing one-segment candidate manifest:

```bash
python3 -m research_factory efficiency-windowed-core-smoke \
  --manifest <candidate-manifest.json> \
  --limit 20 --concurrency 4 \
  --model gpt-5.6-sol --reasoning-effort low

python3 -m research_factory efficiency-windowed-enrich \
  --core-manifest <windowed_core_manifest.json> \
  --limit 7 \
  --model gpt-5.3-codex-spark --reasoning-effort low
```

Keep Spark enrichment batches at seven segments or fewer. A 20-segment batch preserved exact row counts but lost event-ID order after the second segment. Seven-segment exact-count batches preserve every ID and remain below the quarter-token target; repeated enrichment commands skip already hydrated outputs and drain larger canaries as bounded batches.

On the seven-segment, six-source dense holdout (`181` golden events), the context-aware Sol stage emitted `204` events and recovered `155/181` golden evidence signals: evidence precision `0.759804`, recall `0.856354`, and F1 `0.805195`. `201/204` evidence spans were exact; the three non-exact spans were excluded and marked for batched LLM repair rather than regex repair. Spark and Mini one-to-one semantic judges scored the immutable cores at F1 `0.774026` and `0.753247`, versus `0.624113` and `0.546099` for original full-schema Sol on the same segments. Confidence filtering did not separate matched from unmatched events and must not be added.

The Sol stage averaged `34,804.9` tokens and `86.862` seconds per segment. Exact-count Spark enrichment used `62,299` tokens and `36.32` seconds for all seven segments, or `8,899.9` tokens per segment. End-to-end cost was therefore `43,704.7` tokens per segment, `24.68%` of the `177,089`-token baseline. The two measured concurrent extraction batches plus enrichment took `244.73` wall seconds, `13.56%` of seven serial `257.753`-second baseline calls. Individual Sol latency remains `33.7%` of baseline, so the quarter-time claim is a bounded-concurrency throughput claim, not a single-call latency claim.

All seven enriched outputs hydrated into validator-clean full v3.1 labels. Metric enrichment survives only when the LLM-provided raw metric span and non-empty components are exact evidence substrings; otherwise the metric is discarded and quality-flagged. No semantic field is inferred with regex, keywords, or topic-specific code.

The final generalization canary excluded all six development sources and selected one dense segment from each of `20` held-out podcast sources (`484` golden events). Sol completed all `20` with zero JSON failures, `416` candidate events, mean `30,596.55` tokens, mean `64.675` seconds, and max `80.672` seconds. Exact-evidence matching reached precision `0.810096`, recall `0.696281`, and F1 `0.748889`; `407/416` candidate evidence spans were exact. Strict one-to-one semantic judging reached F1 `0.728889` with Spark and corrected F1 `0.748889` with Mini. Mini's first pass failed on EconTalk and JS Party; blind retries scored those two at `0.791667` and `0.68`. No source was below F1 `0.6` under both judges.

Seven-segment Spark enrichment batches used about `7,000`-`7,821` tokens per segment. A clean `7+7+6` drain is estimated at about `37,961` end-to-end tokens per segment, `21.44%` of the `177,089`-token baseline. The full core run took `398.32` wall seconds and three bounded enrichment batches add roughly `67` seconds, about `9%` of twenty serial baseline calls. Mean individual core latency is `25.09%` of baseline before enrichment, so strict single-item end-to-end latency is just above one quarter; the wall-time target is met as queue throughput with four Sol workers and bounded Spark batches.

All `20` held-out outputs hydrated into validator-clean full v3.1 labels. Across the drain, nine non-exact evidence events were excluded, two validator-rejected events were removed, and eight unsupported metric objects were cleared; labels retain review flags for these losses. A generic clause-by-clause coverage paragraph improved evidence overlap on four low-recall sources but regressed full-canary semantic F1 to `0.691385` Spark and `0.682661` Mini, so it was removed. Do not reintroduce it without a new held-out result.

The accepted prompt calibration is not that rejected paragraph. Sol synthesized eight topic-neutral extraction guidelines from unmatched development pairs, with no source names, copied examples, topic vocabulary, keywords, regexes, or code-side semantic filters. The frozen artifact is `research_factory/prompt_guidelines/windowed_event_core_v1.json`; `efficiency-windowed-core-smoke` now loads it by default and applies the v3.1 ceiling of `25` total events as a prompt constraint. The nested four-window schema cannot structurally enforce a cross-array total, and two final outputs contained `28` events, so treat `25` as a soft ceiling until the core schema is flattened or gains a cross-window constrained representation.

Final acceptance used a new episode-level holdout created after scanning prior manifests. It found `21` podcast sources with fully untouched episodes and selected one fresh dense segment per source (`398` golden events). Sol completed all `21` with zero JSON failures and emitted `411` event cores. Spark judged strict one-to-one core precision `0.756691`, recall `0.781407`, and F1 `0.76885`; Mini judged precision `0.778589`, recall `0.80402`, and F1 `0.7911`. Their mean F1 was `0.779975`.

All `21` outputs hydrated into validator-clean full v3.1 labels; `410/411` events survived exact-evidence grounding and one label retained a review flag for the excluded span. A full-field Mini audit scored F1 `0.819307`; GPT-5.4 scored `0.678218`; Sol's deliberately strict interpretation scored `0.292079`. Those raw judge scores are not directly interchangeable. On an independent GPT-5.5 full-schema rerun of the golden system, the same judges scored `0.823529`, `0.666667`, and `0.392156`, respectively. Mini and GPT-5.4 therefore place the new architecture at parity with GPT-5.5 repeatability; Sol remains the conservative residual-risk signal rather than an uncalibrated veto.

The final core stage averaged `30,492.48` tokens and `60.234` seconds per segment, with max latency `81.205` seconds. Two Spark enrichment batches and one quota-fallback Mini batch used `140,360` tokens total, or `6,683.81` per segment. End-to-end usage was `37,176.29` tokens per segment, `20.99%` of the `177,089`-token baseline. The measured concurrent core run plus all three enrichment batches took about `564` seconds, `10.42%` of `21` serial `257.753`-second baseline calls. Mean core latency alone was `23.37%` of baseline. The quarter target is met for raw tokens and queue wall time; strict per-item end-to-end latency still depends on waiting for a bounded enrichment batch.

Use the first-class holdout and judge commands for future changes:

```bash
python3 -m research_factory efficiency-windowed-holdout \
  --output <fresh-holdout>/manifest.json \
  --exclude-root work --source-limit 26 \
  --min-events 14 --max-events 25 --seed <frozen-seed>

python3 -m research_factory efficiency-windowed-judge \
  --core-manifest <windowed_core_manifest.json> \
  --output-dir <judge-output> --limit 21 --concurrency 4 \
  --model gpt-5.4-mini --reasoning-effort low
```

Spark enrichment is preferred for latency. If its subscription quota is exhausted, rerun the unhydrated remainder with `--model gpt-5.4-mini`; the final seven-segment fallback was contract-clean and used `5,608.57` tokens per segment, but took `156.062` seconds versus roughly `21`-`27` seconds for Spark batches.

This architecture follows the training-free decomposition and constrained-formatting direction reported by [UIEPrompter](https://aclanthology.org/2025.xllm-1.28/) and the draft-then-constrain result in [Draft-Conditioned Constrained Decoding](https://arxiv.org/abs/2603.03305). It also reflects the measured structured-output constraint cost described by [The Constraint Tax](https://arxiv.org/abs/2605.26128). Naive few-shot examples, unconstrained full extraction, semantic category partitions, confidence gates, and separate precision judges all failed locally and remain stop rules.

Do not interpret Codex smoke-test stalls while scale waves are already occupying the GPT-5.5 lane. On 2026-07-10, `scale-v31-wave47`, `scale-v31-wave48`, and `scale-v31-wave49` were running `complete_v31_pilot_labels.py` with concurrency `4` each; chunk smoke tests should be rerun when those workers are idle. `efficiency-smoke-candidates` refuses to start by default when external GPT-5.5 Codex exec processes are active, writes only local output/log artifacts, and returns a sanitized JSON status without prompt or transcript text. Its dry-run mode ranks event-bearing chunks across sources first; start with moderate event-bearing chunks (`--min-expected-events 25 --max-expected-events 80`) to prove coded extraction, then run dense stress chunks separately after the basic candidate comparison is healthy.

Do not invoke Codex `/fast` for GPT-5.5 extraction, reviewer, identity-judge, or claim-judge work. The fast QA loop, failure-bank checks, and delta audits are still correct; `/fast` mode is not.

The roadmap for canonical expert/guest graphing, identity resolution, claim agreement, and authority scoring lives in `docs/ROADMAP.md`.

## Production Cycle

The production control plane is local. Use Railway only for the sanitized observer snapshot.

Observer-only local cycle:

```bash
python3 -m research_factory production-cycle --worker-mode none
```

Bounded local compute cycle:

```bash
python3 -m research_factory production-cycle --worker-mode bounded --publish
```

This sequence syncs queue envelopes, optionally runs bounded local workers, refreshes scale readiness, writes a sanitized snapshot, privacy-scans exports, and publishes only when `--publish` is explicit. Do not run extraction, review, graph, acquisition, queue processing, cron, or model calls on Railway.

## Local Headless Worker Cycle

Use these commands to run separate bounded local Codex-style workers. They share the same SQLite queue and record `worker_runs` for observer visibility:

```bash
python3 -m research_factory worker run --role acquisition --lane podcast --limit 4 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id headless-acquisition
python3 -m research_factory worker run --role extractor --lane podcast --limit 4 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id headless-extractor
python3 -m research_factory worker run --role reviewer --lane quality --limit 4 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id headless-reviewer
```

Default worker runs are capped at `4` jobs. Use `--burst` only for small clean remediation batches up to `6`.

The future remote/MCP queue contract is intentionally disabled but testable:

```bash
python3 -m research_factory remote-queue claim-job --worker-id future-chatgpt-task --capability extractor --max-items 1
```

Only jobs with a remote-allowed privacy tier can be claimed through that future contract. Local SQLite remains the source of truth, and remote-style submissions must be validated/imported locally before canonical tables change.

## Broad Format Expansion

Do not broad-scale new content formats until the podcast v3.1 gate passes. Then add adapters in this order:

1. public blog/article pages
2. public Substack/newsletter pages
3. public creator YouTube captions
4. PDFs/papers
5. release notes and docs pages

Each adapter must emit normalized `content_sources`, `content_items`, `content_artifacts`, and `content_spans` with provenance, privacy tier, policy flags, parser quality, and acquisition state. Use `tech_discourse_v1` only after a mixed-format holdout gate validates quality.

## Scale-Readiness Gate

Do not call v3.1 scale ready just because every selected episode has a GPT-5.5 episode-context run. Scale readiness requires complete selected-segment extraction plus reviewer and graph gates:

```bash
python3 -m research_factory scale-batch-report --pilot-id scale-gate-v31-2026-07-02
python3 -m research_factory retry-failed-labels --label-pack ai_discourse_v3_1 --mode repair-or-requeue
python3 -m research_factory run --lane podcast --limit 25 --job-types label_segment --max-label-prompts 25 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller
python3 -m research_factory audit --sample 1.0 --label-pack ai_discourse_v3_1 --model gpt-5.5
python3 -m research_factory run --lane quality --limit 250 --job-types audit_label --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller-quality
python3 -m research_factory reviewer-audit --pilot-id scale-gate-v31-2026-07-02 --episodes 10 --model gpt-5.5
python3 -m research_factory judge-identities --pilot-id scale-gate-v31-2026-07-02 --model gpt-5.5 --limit 250
python3 -m research_factory cluster-claims --pilot-id scale-gate-v31-2026-07-02 --model gpt-5.5
python3 -m research_factory judge-claim-edges --pilot-id scale-gate-v31-2026-07-02 --model gpt-5.5 --limit 100
python3 -m research_factory scale-batch-report --pilot-id scale-gate-v31-2026-07-02
```

`reviewer-audit` creates local GPT-5.5 review handoffs. A reviewer must read the prompt, write strict JSON to the output path, and submit it with `submit-reviewer-audit`. Candidate claim edges are not final semantic judgments until a GPT-5.5 judge rationale is stored with evidence.

If the semantic reviewer gate fails, do not scale. Use the remediation loop:

```bash
python3 -m research_factory reviewer-findings --pilot-id scale-gate-v31-2026-07-02 --severity P0,P1
python3 -m research_factory requeue-reviewed-segments --pilot-id scale-gate-v31-2026-07-02 --mode reviewed-episodes
python3 work/complete_v31_pilot_labels.py --pilot-id scale-gate-v31-2026-07-02 --model gpt-5.5 --concurrency 4 --max-jobs <pending-remediation-count>
python3 -m research_factory audit --sample 1.0 --label-pack ai_discourse_v3_1 --model gpt-5.5
python3 -m research_factory run --lane quality --limit 250 --job-types audit_label --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id codex-controller-quality
python3 -m research_factory reviewer-audit --pilot-id scale-gate-v31-2026-07-02 --episodes 10 --model gpt-5.5 --fresh
python3 work/run_reviewer_audits.py --pilot-id scale-gate-v31-2026-07-02 --model gpt-5.5 --concurrency 2
```

`reviewer-findings` is sanitized and does not print transcript excerpts. Use `failed-review-only` for a narrow P0/P1 segment retry; use `reviewed-episodes` when reviewer failure is broad across identity usefulness, precision, or product/market coverage. Stop on Codex usage-limit signals and resume later; `complete_v31_pilot_labels.py` releases missing-output handoffs before exiting.

Only after the 25-episode pilot passes should the controller enqueue a controlled 100-episode batch: `60` high-signal ready AI episodes, `20` newly recovered/acquired transcripts, and `20` broad-tech AI-heavy episodes. Keep concurrency at `4` GPT-5.5 workers until failed jobs, leases, and reviewer scores stay healthy.

## Transcript Discovery Cycle

When the observer shows transcript gaps, use the source strategy catalog before browser work. Transcript acquisition should be an ongoing per-source pipeline, not a one-time pull.

```bash
python3 -m research_factory transcript-strategy-report
python3 -m research_factory enqueue-transcript-backlog --lane podcast --label-pack ai_discourse_v3_1 --limit 250
python3 -m research_factory run --lane podcast --limit 75 --job-types fetch_transcript,prepare_transcript --no-claim-prompts --max-label-prompts 0 --model gpt-5.5 --label-pack ai_discourse_v3_1 --worker-id transcript-discovery
python3 -m research_factory transcript-candidates --lane podcast --limit 40 --claim --worker-id transcript-discovery
python3 -m research_factory attach-transcript --episode-id <id> --transcript-url <official-url> --transcript-type text/html --source-kind official_show_transcript
python3 -m research_factory record-transcript-attempt --episode-id <id> --method youtube_caption --status youtube_caption_blocked --source-kind youtube_captions --error-class youtube_caption_blocked --notes "<short safe reason>"
python3 -m research_factory mark-transcript-exhausted --episode-id <id> --worker-id transcript-discovery --notes "<official pages/RSS/YouTube exhausted>"
python3 -m research_factory enqueue-transcription --lane podcast --provider voyager --label-pack ai_discourse_v3_1 --limit 20 --dry-run
```

Only attach creator RSS transcript links, official public transcript pages/assets, or public creator-channel captions. Leave candidates pending when provenance is unclear. Generic backlog refill intentionally excludes YouTube and document transcripts by default; use those flags only after their lanes are explicitly enabled. Remove `--dry-run` from `enqueue-transcription` only when paid transcription fallback has been explicitly enabled for that run. The transcribe worker will fail closed unless `ALLOW_PAID_TRANSCRIPTION=true` and the configured provider credentials are present.

The source strategy inventory and lane definitions live in `docs/TRANSCRIPT_ACQUISITION_PIPELINE.md` and `config/transcript_strategies.json`.

If `run` creates prompt handoffs, a Codex app worker should process a bounded batch:

1. Run `python3 -m research_factory claim --lane podcast --label-pack ai_discourse_v3_1 --model gpt-5.5`.
2. Read the returned `prompt_path`.
3. Write only strict JSON to the returned `output_path`.
4. Run `python3 -m research_factory submit --job-id <id> --output-json <output_path>`.
5. If the output cannot be produced cleanly, run `python3 -m research_factory fail --job-id <id> --reason "<short reason>"`.

## Observer Snapshot Cycle

```bash
python3 -m research_factory railway-cost-guard
python3 -m research_factory snapshot --output exports/observer-snapshot.json
python3 -m research_factory publish-snapshot \
  --snapshot exports/observer-snapshot.json \
  --url https://observer-ui-production.up.railway.app \
  --token-file .railway-ingest-token.local
```

The live observer is `https://observer-ui-production.up.railway.app`.

## Intervention Checks

- `failed > 0`: inspect failed jobs before increasing concurrency.
- many `claimed` jobs with expired leases: run a small controller cycle to reclaim leases.
- segments without labels: workers are not submitting model outputs.
- missing transcript jobs: run transcript discovery; source feeds did not expose direct transcript links.
- `youtube_caption_blocked`: use browser/manual public discovery first; do not jump straight to transcription.
- `transcription_eligible`: public online routes were exhausted and audio fallback can be queued if paid transcription is intentionally enabled.

## Privacy Checks

```bash
python3 -m research_factory privacy-scan
```

Do not move `corpus/` into public hosting. Publish only `exports/observer-snapshot.json` or generated reports that cite segment IDs and short evidence spans.
