# True-North Input Optimization Protocol

## Purpose and status

This protocol measures how much first-round extraction quality can improve by changing the five inputs shown in the GLM flowchart:

1. system prompt;
2. task rules;
3. surrounding context;
4. candidate priors;
5. output schema.

The registry is `config/true_north_input_variants_v1.json`. It predeclares exactly 20 variants per factor, including the current baseline. Registration is not evidence that a variant has been run. A run counts only when it has a hash-bound packet, model receipt, normalized output, score record, and falloff ledger.

Extraction inputs are private and local. No raw transcript, answer key, or long evidence excerpt may appear in reports. Nothing in this campaign may write to production data or authorize promotion.

## Experimental unit and isolation

The experimental unit is one variant applied to one hash-pinned packet. During single-factor screening, the selected factor is the only mutable input. Model, decoding controls, task corpus, packet membership, all other inputs, normalizer, scorer, and thresholds remain frozen.

Every result must preserve:

- candidate IDs;
- exact parent-candidate `evidence_text`, `evidence_start`, and `evidence_end`;
- candidate-to-atomic parent lineage, including zero, one, or many atomics;
- segment `segment_id`, `segment_index`, and `text_sha256`;
- transcript and packet provenance;
- actual provider, model, latency, tokens, retry reason, prompt hash, and output hash.

Schema experiments may emit different surface shapes, but each must declare `normalization_contract: canonical_atomic_output_v1`. Scoring occurs only after deterministic normalization. A normalization failure is a schema-arm failure, not a missing observation. Normalization may reshape or rename fields; it may not infer claims, identities, evidence, or decisions.

A complete JSON answer that fails surface, normalization, or canonical
validation is terminalized without another paid call. Its packet is marked
complete, every bounded candidate is represented as an invalid-output
observation, and `schema_parse_success_rate` becomes a hard-gate metric. Empty,
truncated, timed-out, or otherwise undecodable transport output remains
mechanically retryable within the three-attempt lifetime ceiling. This
separation prevents retry-until-valid survivorship bias.

The executable bundle shape is fixed. `episode_context` contains only `artifact_path`, `concept_seed`, `context_run_id`, `entity_seed`, `extraction_guidance`, `model`, `section_map`, and `speaker_map`. A segment contains `segment_id`, `segment_index`, `text`, and `text_sha256`. Candidates carry `candidate_id` and their exact `evidence_text`, `evidence_start`, and `evidence_end`; there is no candidate evidence ID.

OpenCode receives the complete hash-bound packet inline in the private user
message. It is not attached through OpenCode's file reader because that reader
clips long single-line JSON files. A regression test proves that the complete
packet sentinel is present and `--file` is absent. The packet remains local,
tools remain denied, and packet semantics and presentation hashes do not
change.

Context arms may only select/reorder those eight episode-context fields or crop a segment deterministically around the union of its candidate evidence offsets with a predeclared character margin. Crops retain original offset coordinates and segment identity. They may not retrieve, summarize, resolve coreferences, infer topics, generate glossaries, or add text.

Candidate-prior arms are likewise finite: select or order existing live candidate fields, deterministically order candidate rows, project top-level confidence into three fixed buckets, or add one of two predeclared policy markers. Field projection cannot remove candidate/segment lineage, exact evidence text/offsets, label lineage, or the private frozen source context used by the packet builder. It may not derive duplicate groups, quality flags, rationales, routes, or any field absent from the bundle.

Schema arms are limited to reversible property ordering, wrappers, aliases, description levels, and candidate-keyed versus canonical item shapes. Every canonical semantic field remains represented. The normalizer performs only declared inverse renames, unwrapping, reordering, or candidate-key restoration; it cannot supply a field the model omitted or infer a semantic value.

No prompt or packet may contain an answer key, scorer feedback, expected labels, sealed episode material, or examples copied from evaluation errors. Operational fallback never occurs because of semantic disagreement.

## Frozen folds

The folds are assigned before the first campaign call:

| Fold | Episodes | Permitted use |
|---|---|---|
| Search | `ep_90c3b5c995bce501c9aef55c`, `ep_7ec9f808a3955c720aeb94ff` | Screen all registered variants and inspect aggregate errors. |
| Replication | `ep_903cc763f0f106d7f4610f17` | Replicate preselected finalists; do not use item-level results to invent new variants. |
| Locked validation | `ep_8a0c6919d7cfe6bcd9cacd30` | One opening after factor finalists and interaction plan are frozen. |
| Final confirmation | `ep_e7630540911fc5dea9850204` | One opening after the complete configuration is frozen. |
| Sealed transfer holdouts | IDs withheld from the registry and campaign packets | Never tune, screen, or debug against them. Open only under the parent benchmark's transfer gate. |

Search and replication episodes have already been opened, so they are development data. The two lockbox episodes must be guarded by executable state checks, not convention. If a lockbox is opened early, it is retired and replaced before further tuning. Transfer holdouts remain governed by the True-North benchmark and are not part of this optimization campaign.

## Predeclared metrics

All metrics are computed both micro-averaged and by episode. Confidence intervals use paired episode-aware bootstrap resampling; paired item deltas are retained for diagnosis.

Quality metrics:

- candidate-disposition macro F1;
- retained-value recall;
- junk escape rate;
- avoidable rejection rate;
- revision recovery rate and hold rate;
- atomic-boundary F1;
- materially compound accepted-atomic rate;
- exact evidence and evidence-offset validity;
- unsupported-claim acceptance count;
- primary-speaker exact accuracy;
- direct-speaker versus reported-actor precision;
- confidently wrong speaker rate;
- unresolved-case recall;
- canonical-normalized field completeness;
- schema parse and normalization success;
- candidate ID survival, exact parent-evidence preservation, and lineage completeness.

Efficiency metrics:

- calls, input tokens, output tokens, wall time, retries, and hard timeouts;
- tokens and seconds per retained valid atomic;
- mechanical failure rate.

No single weighted score selects the winner. The retained-value, precision, attribution, atomicity, validity, latency, and token dimensions remain visible.

## Hard gates and minimum denominators

An arm is ineligible if any hard gate fails:

- zero unsupported accepted claims;
- 100% valid candidate IDs, exact evidence text/offsets, segment identity, and parent lineage among accepted outputs;
- at least 99% schema parse and deterministic-normalization success, with no silent row loss;
- candidate-disposition macro F1 at least 0.90;
- retained-value recall at least 0.90;
- junk escape no greater than 2%;
- atomic-boundary F1 at least 0.90;
- materially compound accepted atomics no greater than 3%;
- primary-speaker accuracy at least 97%;
- direct-speaker versus reported-actor precision at least 95%;
- confidently wrong speaker assignments no greater than 1%.

Per-arm screening requires at least 50 candidate decisions, 20 gold-positive retained items, 20 accepted atomics, and 10 attribution-scored items across at least two episodes. A relation- or identity-specific rate with fewer than 10 eligible examples is reported as underpowered and cannot establish superiority. A safety-gate violation is decisive even below these denominators. Missing outputs count as failures, not denominator reductions.

## Campaign sequence

### 1. Mechanical qualification

Before model calls:

- validate registry counts, IDs, factor isolation, and normalization declarations;
- verify packet and scorer hashes;
- prove outputs target a fresh shadow run directory and shadow SQLite database;
- inject malformed, duplicate, and out-of-scope fixtures;
- verify exact replay, complete rollback, and unchanged production database hashes and release counts.

### 2. Single-factor search

Run every one of the 20 variants for a factor on the two Search episodes. Use a shared cached baseline response only when its complete packet and model configuration hashes are identical; otherwise rerun the baseline. Finish and score an entire factor before interpreting it.

Advance only nondominated, hard-gate-passing arms. A variant dominates another only if it is no worse on every quality metric, materially better on at least one predeclared quality metric, and does not exceed the other's token or latency cost by more than 10%. Retain at most four Pareto candidates per factor, including baseline when it remains nondominated.

### 3. Independent replication

Run the preselected Pareto candidates on the Replication episode. Freeze the choice of at most one finalist per factor using aggregate Search plus Replication results. A claimed improvement must have the same direction on at least two of the three opened development episodes and must not introduce a new hard-gate failure.

Each selected factor finalist then receives three independent, fresh calls on each of the three opened development episodes with identical inputs and model configuration. Report mean, range, and worst replicate. The finalist must pass every hard gate in all three replicates; a lucky single run cannot advance.

### 4. Bounded interaction screen

After the five factor finalists are frozen, run one predeclared `2^5` full-factorial screen: baseline versus finalist for each of the five factors, exactly 32 combinations. Run it only on the three already-opened development episodes. This is the sole authorized multi-factor search.

Use factorial main effects and pairwise interaction contrasts as diagnostics. Select the final combination from the hard-gate-passing Pareto frontier. Do not add combinations, edit finalists, or create new variants after inspecting this screen. Interaction findings are provisional unless their direction is consistent on at least two development episodes.

### 5. Locked validation and confirmation

Freeze the complete prompt, packet builder, schema, normalizer, model route, thresholds, and hashes before opening Locked validation. Compare only the final combination and untouched baseline. If it passes, run the same two configurations once on Final confirmation. Item-level lockbox errors may explain failure but may not guide another edit.

Passing requires all hard gates, no material regression on any primary quality metric, and a consistent improvement in at least one quality metric or at least 15% lower median latency/token cost at equivalent quality. Failure ends this version of the campaign; it does not convert a lockbox into development data.

## Budgets

The controller must enforce all of these campaign-wide ceilings:

- 450 model calls;
- 15 million total input plus output tokens;
- 18 hours cumulative provider wall time;
- 3 attempts per operationally failed packet, including the original attempt;
- 1 locked-validation opening and 1 final-confirmation opening.

The five factor campaigns share a cohort ledger. Before each call it reserves
one worst-case call, token allowance, and timeout under a cross-process lock;
completed campaign receipts replace that reservation. This prevents parallel
workers from independently passing a per-factor check and collectively
exceeding the global envelope. A packet may accumulate no more than three
attempts across process restarts.

Planned calls are approximately 200 for the 100 single-factor arms on two Search episodes, up to 20 for replication finalists, up to 45 for three-replicate factor-finalist checks, 96 for the `32 × 3` interaction screen, and 4 for baseline/final lockboxes. Remaining budget is contingency for approved operational retries. Unused budget is not a reason to run more variants.

## Stop conditions

Stop immediately when:

- any production database, release ledger, or production artifact changes;
- packet, transcript, evidence, scorer, or configuration hash drifts;
- answer-key or sealed material enters a model input;
- candidate IDs, exact evidence text/offsets, segment identity, or parent lineage are lost or rewritten;
- normalization performs semantic inference;
- the call, token, time, retry, or lockbox budget is exhausted;
- provider/model identity cannot be recorded;
- two consecutive operational failure waves affect more than 20% of scheduled packets;
- no arm passes hard gates for a factor;
- locked validation or final confirmation fails.

Stop a factor early only for a campaign-wide integrity failure, not because an early variant looks strong. All 20 predeclared variants must otherwise run, including baseline.

## Outputs

The campaign writes only to a dated, immutable lab directory and isolated shadow database. It produces:

- registry, packet, prompt, schema, normalizer, scorer, and configuration hashes;
- per-call provider receipts;
- normalized machine-readable outputs;
- stage and factor score tables;
- Pareto-frontier decisions and exclusion reasons;
- replicate dispersion and interaction contrasts;
- token, latency, retry, and retained-value costs;
- production pre/post hashes proving non-mutation.

Reports contain identifiers, metrics, and short sanitized diagnostics only. They do not publish transcript text. Successful confirmation authorizes a recommendation for the parent benchmark; it does not authorize production promotion.
