# Research Intelligence Factory Roadmap

This project is a private, local-first research factory for systematic-review-grade technology discourse intelligence. Podcasts are the first production adapter. The broad target includes podcasts, blogs, Substacks, YouTube captions, papers, release notes, docs, newsletters, and other public tech discourse formats.

The active production direction remains `ai_discourse_v3_1`: GPT-5.5 reads full episode context, extracts proposition-level discourse events, and deterministic validators enforce schema, exact evidence spans, privacy boundaries, and reproducible queue state. The broad-format foundation adds portable queue envelopes, content abstractions, local headless Codex worker roles, and a disabled future MCP/ChatGPT worker seam.

## Current Scale Gate

Do not broadly scale v3.1 yet. The current readiness definition is complete-episode extraction, not a breadth check where one segment per episode lands.

Token spend is not the objective. Expensive model passes are acceptable when they materially improve accuracy, coverage, adjudication, or graph quality. Do not invoke Codex `/fast` for GPT-5.5 extraction, reviewer, identity-judge, or claim-judge work. Keep the fast QA loop, failure bank, and delta audits; the restriction is only about the Codex `/fast` runtime mode.

Current baseline to preserve:

- `scale-gate-v31-2026-07-02` selected `25` episodes and has `25` GPT-5.5 episode-context runs.
- The full pilot produced `216/216` v3.1 labels, more than `4,000` discourse events, `0` offset failures, and `100%` deterministic audit pass.
- It is not scale ready because GPT-5.5 semantic review failed: overall, precision, identity usefulness, and P0 false-signal thresholds were not all met.
- Current remediation uses reviewer findings to relabel the 10 reviewed episodes before rerunning the semantic reviewer gate.
- Global blockers remain visible: failed/requeued v3.1 jobs, labels needing adjudication, concept adjudications, and candidate-only claim/agreement graph layers.

Next gate before the controlled 100-episode batch:

- complete all `216/216` selected segment labels for the 25-episode pilot with GPT-5.5
- require zero failed pilot label jobs and no unretried v3.1 failures
- require exact evidence offsets and deterministic audit pass rate >= `95%`
- require event density in the `10-35` events per 1k substantive words band
- run `10` full-episode GPT-5.5 semantic reviewer audits against direct transcript context
- require reviewer overall >= `85`, coverage >= `82`, precision >= `90`, grounding >= `95`, identity graph usefulness >= `85`, and zero P0 false/weak events
- drain concept/label adjudication blockers before calling observer health green
- create first claim clusters plus agreement/disagreement edge candidates, then promote only GPT-5.5-reviewed edges as semantic judgments
- preserve transcript acquisition evidence: RSS, official pages, official/public YouTube captions, blocked routes, and `transcription_eligible` only after public routes are exhausted
- if semantic review fails, run `reviewer-findings`, requeue the reviewed episodes or affected P0/P1 segments, rerun GPT-5.5 extraction, rerun deterministic audits, then rerun reviewer audits with `--fresh`

Use `python3 -m research_factory scale-batch-report --pilot-id scale-gate-v31-2026-07-02` as the operator-facing readiness check.

## Broad Scale Architecture

The system now separates the control plane from extraction workers:

- Local SQLite remains the canonical queue and corpus store.
- Railway remains a minimal observer/storage edge and must not run extraction, ingestion, queue processing, graph jobs, semantic review, cron, or model calls.
- Existing podcast jobs are wrapped in portable queue envelopes.
- Content abstractions sit above podcast-specific rows: `content_sources`, `content_items`, `content_artifacts`, `content_spans`, `context_packages`, and `extraction_runs`.
- Worker observability tables track `worker_runs`, `job_claims`, `output_submissions`, and `queue_events`.
- Local headless Codex workers run bounded role-specific jobs first.
- Queue envelopes default to `model_speed:regular`; if a worker surface exposes speed choices, do not use Codex `/fast` for extraction and review gates.
- A future Railway MCP server can expose the same queue contract to ChatGPT scheduled jobs, but remote workers stay disabled until local quality gates pass.

Worker roles:

- `acquisition`: source discovery, transcript/article/caption acquisition, parser repair, transcription eligibility.
- `extractor`: GPT-5.5 context and extraction jobs.
- `reviewer`: GPT-5.5 semantic review and deterministic quality jobs.
- `identity_judge`: canonical identity review.
- `claim_judge`: semantic claim cluster and agreement/disagreement review.
- `snapshot_publisher`: sanitized observer snapshot publication.

Privacy tiers:

- `local_only`: never leaves the local machine.
- `public_link_only`: remote worker may receive a public URL and instructions.
- `excerpt_context`: remote worker may receive bounded excerpts.
- `full_text_allowed`: remote worker may receive full acquired text.

Remote queue/MCP acceptance requires scoped auth, lease heartbeats, idempotent output submission, local validation/import, and no direct remote mutation of canonical tables.

## Broad Codebooks

`tech_discourse_v1` is the broad-format codebook scaffold. It keeps stable event families while allowing open vocabulary for concepts, terms, products, models, organizations, people, benchmarks, mechanisms, and frames.

Do not make `tech_discourse_v1` the production default until it has its own mixed-format gate. It should first pass a holdout across podcasts, blogs, Substacks, public captions, papers, release notes, and docs.

## Transcript Acquisition Gate

Every episode should preserve a transcript-acquisition trail before fallback transcription:

- creator RSS transcript links
- official show transcript pages
- public creator-channel YouTube captions
- explicit blocked/failed attempts with safe notes
- `transcription_eligible` only after public online routes are exhausted
- `voyager_transcription` only through configured `transcribe_audio` jobs with paid fallback intentionally enabled

Observer snapshots should show acquisition status counts, attempt counts by method/status, transcript source-kind mix, and transcription run status without exposing raw transcript text or private local paths.

## Phase 1: Dense Discourse Evidence

v3.1 discourse events are the evidence layer, not the final product. The extractor should capture:

- claims, forecasts, counterclaims, stances, causal mechanisms, uncertainty, adoption signals, market signals, product signals, risk signals, frame usage, and terminology shifts
- important actor and entity mentions, including people, orgs, products, models, labs, standards, papers, benchmarks, and institutions
- speaker context, reported actor context, source context, target concepts, surface terms, evidence spans, confidence, quality flags, and `signal_reason`

Acceptance:

- high-signal dialogue reaches useful density without boilerplate inflation
- all evidence spans are exact substrings with offsets
- every event traces to episode, segment, model, label-pack version, and audit status
- footnote markers, timestamps, list numbers, page residue, ASR numeric corruption, sponsor copy, and show setup are rejected instead of becoming discourse events
- numeric or metric events require grounded metric evidence with unit/context in the evidence span

## Phase 2: Canonical Identity Governance

Build a canonical graph layer above raw v3.1 events. Do not rely on raw transcript spellings or one-off model outputs as canonical truth.

Durable tables to add or verify:

- `raw_speaker_mentions`
- `raw_actor_mentions`
- `canonical_people`
- `canonical_orgs`
- `canonical_products`
- `canonical_models`
- `identity_aliases`
- `identity_resolution_candidates`
- `identity_merge_decisions`
- `identity_split_decisions`
- `speaker_identity_runs`
- `guest_appearances`
- `person_org_affiliations`
- `podcast_guest_edges`

Rules:

- Store every raw mention separately from the canonical entity it maps to.
- Preserve misspellings, transcript variants, nicknames, partial names, handles, org titles, and role descriptions as aliases/evidence.
- Never overwrite historical raw mentions.
- Canonicalization must be versioned and reproducible.
- Maintain states: `unresolved`, `candidate_match`, `canonical`, `needs_human_review`, `split_required`, `merge_required`.
- Resolve "Sam", "Altman", "Sam Altman", "OpenAI CEO", and transcript misspellings into one candidate only when full episode context, metadata, role, affiliation, co-mentions, and surrounding claims support it.
- Keep separate people separate when names collide.

GPT-5.5 judge review is required for:

- high-centrality people
- ambiguous aliases
- new guests
- identity merges
- org/person affiliation changes
- cross-podcast duplicate candidates

Deterministic validators should block unsafe auto-merges when evidence is weak, aliases collide, affiliations are impossible, raw evidence is absent, or the mapping would affect high-centrality nodes.

## Phase 3: Claim And Network Graph Layer

Create durable graph tables:

- `claim_clusters`
- `agreement_edges`
- `disagreement_edges`
- `person_concept_edges`
- `person_person_mentions`
- `person_org_edges`
- `expert_authority_scores`
- `podcast_reach_scores`
- `forecast_outcome_checks`
- `graph_score_runs`

Agreement and disagreement edges must be claim-level semantic judgments, not keyword overlap. GPT-5.5 should judge whether two claims are equivalent, supporting, conflicting, orthogonal, or incomparable. Store rationale and evidence for every edge.

Graph outputs:

- `guest_network`
- `podcast_cluster_graph`
- `concept_expert_graph`
- `person_mention_graph`
- `agreement_disagreement_graph`
- `org_affiliation_graph`
- `authority_score_report`

## Authority Scoring

Popularity is not authority. Store component scores separately and label all scores preliminary until the corpus has enough identity confidence, episode coverage, and outcome checks.

Components:

- appearance count
- cross-podcast reach
- topical claim density
- claim specificity and strength
- forecast specificity
- evidence quality
- agreement/disagreement with other experts
- centrality in guest/concept/org graph
- freshness/recency
- later outcome accuracy when available
- source/podcast quality weighting
- identity confidence penalty

Every score component must trace to episode, segment, discourse event, evidence span, model, label-pack version, canonical version, and audit status.

## Grooming Jobs

Scheduled GPT-5.5 judge jobs:

- identity deduplication
- alias promotion
- merge/split review
- speaker role correction
- guest appearance verification
- claim cluster review
- agreement/disagreement review
- high-impact edge audit
- stale or low-confidence canonical mapping review

Deterministic checks:

- duplicate canonical entities
- alias collisions
- impossible affiliations
- edges without evidence
- scores using unresolved identities
- raw transcript text leaking into exports or observer state

## Observer Metrics

The Railway observer should show:

- research queue depth by worker role, content type, privacy tier, and status
- local headless worker runs and failures
- future MCP/remote-worker status as disabled until intentionally enabled
- content coverage by source type, content type, artifact type, and acquisition state
- Railway footprint health: observer-only service, no cron, no volumes, no worker services, no remote compute

- unresolved identities
- pending merge/split candidates
- identity confidence distribution
- unique guests/speakers
- actor mentions
- person-person mention edges
- claim clusters
- agreement/disagreement edges
- authority score runs
- top emerging experts by topic
- podcasts with highest AI-discourse density

## Pilot Acceptance For Expert Graph

For the current v3.1 pilot episodes:

- produce a first canonical guest/concept/mention graph and authority-score draft
- at least 90% of sampled guest/speaker identities are correct
- at least 90% of sampled canonical merges are justified by evidence
- no high-centrality merge is automatic without GPT-5.5 judge rationale
- every graph edge and score component traces to episode, segment, discourse event, evidence span, model, label-pack version, canonical version, and audit status
- agreement/disagreement edges are claim-level semantic judgments
- authority reports show component scores separately and clearly label preliminary scores
