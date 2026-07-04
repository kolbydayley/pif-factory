# ai_discourse_v3_1 Codebook

## Purpose

`ai_discourse_v3_1` fixes the sparse v3 extractor by making the unit of analysis a proposition-level `discourse_event`. Low-level entities, terms, products, and frames are dimensions attached to an event, not the main output.

## Valid Event Test

An event is valid only if all are true:
- It captures a specific claim, stance, frame, term use, forecast, counterclaim, metric, product signal, risk signal, or causal mechanism.
- It identifies the speaker/source context well enough for actor-specific analysis.
- It has exact evidence in the current segment.
- It would help a later query detect discourse movement over time.

## Event Families

- `term_usage`: notable term, phrase, acronym, model name, product name, or wording that matters for drift.
- `frame_usage`: interpretive frame such as capability race, platform control, cost discipline, safety governance, public legitimacy, or deployment bottleneck.
- `stance_position`: actor position toward a concept, model, company, release, policy, or trend.
- `forecast`: concrete prediction about timing, capability, adoption, market impact, labor, regulation, or financing.
- `causal_mechanism`: stated mechanism linking cause, constraint, enabler, or consequence.
- `capability_claim`: claim about model, agent, benchmark, infrastructure, product capability, or limitation.
- `product_signal`: product, model, launch, roadmap, pricing, integration, restriction, access, or distribution signal.
- `market_signal`: demand, budget, investment, IPO, valuation, capex, competitive position, labor, pricing, margin, or adoption signal.
- `risk_signal`: safety, alignment, misuse, security, reliability, governance, access-control, regulatory, or deployment risk.
- `counterclaim`: disagreement, rebuttal, skepticism, exception, or alternative explanation.
- `uncertainty`: explicit uncertainty, hedging, weak evidence warning, methodology warning, or forecast humility.
- `adoption_signal`: workflow, developer, consumer, enterprise, organization, or customer adoption pattern.
- `actor_mention`: important person or organization reference useful for influence, guest, affiliation, or who-talks-about-whom graphing, even when the mentioned actor is not the claimant.
- `entity_reference`: important product, model, lab, standard, dataset, benchmark, paper, or institution reference useful for concept, product, or authority graphing.

Use `event_subtype` for the specific open-vocabulary code. Do not invent new `event_type` values. Examples: `market_signal` + `ipo_timing`, `term_usage` + `category_renaming`, `entity_reference` + `model_name_variant`, `actor_mention` + `who_talks_about_whom`, `product_signal` + `pricing_packaging_shift`.

## Minimum Coverage Pass

For every substantive segment, the extractor must make a deliberate pass over:
- terminology and framing shifts;
- product, model, launch, pricing, access, packaging, restriction, quality, benchmark, integration, roadmap, qualification, manufacturing, validation, and deployment-friction signals;
- market and investment metrics such as fundraising, valuation, ARR, IPO timing, capex, compute leases, customer demand, pricing pressure, adoption metrics, labor substitution, margins, commercial proof points, or competitive position;
- technical mechanisms such as architecture, infrastructure, evaluation method, agent workflow, memory/RAG, inference scaling, compute bottlenecks, tool use, reliability, or safety technique;
- claims, counterclaims, uncertainty, evidence caveats, and methodology warnings;
- expert graph inputs: speakers, guests, quoted sources, mentioned people/orgs/products/models, affiliations, who talks about whom, and aliases or transcript variants.

Emit every distinct high-value proposition. A segment with separate product, market, actor, forecast, and risk signals should normally produce separate events for each, not one summary event.

## Benchmark And Market Detail Rules

Do not flatten concrete evidence into generic product narrative events. Preserve details that later trend, market, or product-release analysis would need:
- benchmark names, scores, ranks, and comparison targets;
- price, cost, access, packaging, preview, customer class, qualification cycle, manufacturing barrier, validation gate, or distribution constraints;
- adoption, deployment, usage, migration, overbilling, reliability, or security-control signals;
- valuation, fundraising, ARR, revenue, capex, compute lease, margin, ROI, enterprise-pull, commercial-scale, or market-analysis claims;
- product/model capability details such as context length, modalities, generation length, quality claims, integrations, and safety/evaluation requirements.

Each materially distinct benchmark, access, pricing, deployment, or adoption point should usually become its own event with a precise `event_subtype`.

## Research Feedback And Productization Rules

Do not miss evidence about how technical systems become useful products. Code these as distinct events when present:
- active learning loops, experimental feedback, negative results, dataset-quality-over-size claims, or data scarcity;
- manufacturing, materials qualification, regulatory validation, deployment friction, or reliability gates;
- business-scale evidence such as funding rounds, valuation, customer traction, ARR, or commercial proof points;
- named use cases described as killer apps, wedge markets, or adoption bottlenecks.
- labor-market trigger claims where hiring, substitution, layoffs, or reversal of headcount effects depends on AI spend increasing by an order of magnitude, crossing a cost-output threshold, or reaching a stated timeline such as "months away, not years."
- materials, climate, food-system, cement, aerospace, defense, or extreme-environment application areas where market magnitude, emissions share, qualification burden, manufacturing feasibility, or deployment friction determines commercial value.
- AI-lab/product feedback-loop strategy, especially claims that deployment data, user traces, product telemetry, or real-world failure cases are being discarded or underused as valuable model-training data.

These are often more useful than generic entity mentions because they explain why a product, model, or research direction may accelerate, stall, or change framing over time.

Do not reject these trigger/timeline sentences as insufficient evidence merely because they are compact. If the sentence links spend growth, capability curve movement, or a cost-output crossover to a market/adoption/labor transition, it is a valid event with the compact sentence as evidence.

For interview segments, speaker attribution is part of the code. A host question and a guest answer in the same segment must not be collapsed into one host-authored stance. Attribute each proposition to the speaker whose answer or claim contains it, using adjacent context and episode speaker roster when the turn boundary is clipped.

For OCR/ASR-damaged numbers, prefer a reviewed qualitative event over a false precise metric. If overlapping text or nearby wording suggests the count is corrupted, mark the metric as not_applicable or needs_review instead of storing the suspicious exact number.

## Source Context

Use `segment_source_context` for the whole segment and `source_context` for each event. Events may use `substantive_dialogue`, `quoted_external_source`, `sponsor_ad_read`, or `mixed_or_uncertain`.

Sponsor/ad-read copy is excluded from durable ai_discourse_v3_1 extraction. Even concrete commercial claims from ad reads should return no durable event and be explained in `rejected_candidates` or `no_signal_reason`. Generic sponsor copy, promo codes, unrelated ads, show setup, page chrome, and agenda language should also return no event.

## Actor Rules

`actor` is the person or organization whose stance/claim is represented. `speaker_context` is who is speaking in the transcript. `reported_actor` is the quoted or discussed person, organization, product, or model if different from the speaker. Do not collapse these fields.

## Claim Text Rules

`claim_text` must be a faithful one-sentence proposition. It must not start with generic templates like "Segment discusses" or "Segment contains." Use concrete language: "Krieger says Fable produced unusually strong user pull after removal" is valid; "Segment discusses Anthropic product narrative" is not.

## Signal Reason Rules

`signal_reason` must explain the downstream analytical use of the event in 8-25 words. It should say what the event helps track, compare, or test over time. Invalid: "Current compute scarcity signal." Valid: "This gives a dated compute-price pressure signal that can be compared against model capability and capex narratives."

## Mention Event Rules

Use `actor_mention` and `entity_reference` sparingly but deliberately. They are valid when the mention helps build canonical identity, expert influence, affiliation, product/model, or claim-attribution graphs. Record the surface form in `surface_terms`, the likely target in `target.raw_target`, the speaker in `speaker_context`, and the mentioned entity in `reported_actor` when applicable. Mark likely misspellings, transcript variants, nicknames, handles, partial names, or titles in `quality_flags`.

Mention events should capture graph-useful context even when the mentioned actor is not making a claim: who mentioned the actor/entity, whether it was quoted/reported/compared, the role or affiliation implied by the surrounding context, and why the mention matters. Preserve raw surface forms separately from the canonical candidate; canonical identity is a later governed graph-layer decision, not something to overwrite in extraction.

Do not emit `actor_mention` for a bare transcript label, anonymous/pseudonymous commenter handle, generic "unknown speaker," or first-name fragment unless the current evidence adds graph-useful context such as role, affiliation, quoted-source attribution, who-mentioned-whom, or a reason the mention should influence authority mapping. If the handle or fragment is only the source of a substantive claim, code the substantive claim and put the surface form in speaker/source context with low confidence instead of creating a standalone identity event.

## Numeric And Page Artifact Rules

Quantitative events are valid only when the evidence span itself contains a substantive metric proposition with unit/context, such as revenue, valuation, users, institutions, latency, capability change, benchmark score, token count, price, or time horizon. Do not infer a metric from isolated digits, rendered footnote markers, or ASR-corrupted standalone numbers.

Reject or mark as `rejected_candidates`:
- footnote/citation markers like `[4]`, `4.`, `(12)`, or superscript-like residue;
- timestamps, chapter markers, list numbers, page counters, comment/view counts, URL fragments, and show-page chrome;
- ASR numeric corruption where the text around the number does not support the claimed quantity;
- quantitative claims where `metric.raw_text` or `metric.value` is not present in the evidence span and the span lacks another clear numeric unit.

When a segment contains both a real claim and page residue, extract only the real claim and reject the residue. A single footnote marker must never become a capability, quality, market, or product signal.

## Identity Usefulness Rules

For every substantive segment, check whether the evidence improves the canonical graph layer:
- speaker or guest identity, affiliation, role, title, alias, handle, or misspelling;
- one person/org/product/model mentioning, quoting, comparing, endorsing, criticizing, or contextualizing another;
- host introductions and guest biographies when they establish authority or topical expertise;
- reported actors distinct from the speaker, such as "OpenAI said," "Google researchers," or "the Anthropic team."

Use conservative raw mentions. Do not canonically merge identities in extraction; emit raw surface forms and confidence so GPT-5.5 identity judge passes can decide later.

When the full episode context supplies a speaker roster or aliases, use it for speaker attribution, but keep entity arrays tied to the current evidence. If a name is inferred from the speaker turn rather than repeated in the segment text, capture it in `speaker_context`, not as a free-floating `people` mention unless the segment evidence supports the mention.

If a segment is a continuation of a prior speaker and the full-episode context strongly identifies that speaker, prefer the contextual speaker over `Unknown speaker`. Lower confidence and add `speaker_inferred_from_episode_context` in `quality_flags` when the current segment evidence does not repeat the speaker label.

If ASR or transcript cleanup damages a person or organization name, do not promote the damaged spelling as canonical truth. Use the stronger episode title, guest list, speaker roster, affiliation, and surrounding context to populate `actor.name` or `reported_actor.name`; preserve the raw surface form in `surface_terms` and add `asr_name_variant_preserved` or `alias_variant_preserved`. Ambiguous cases should remain unresolved and marked `needs_review`.

For reported speech, keep the speaking person and the represented actor separate. If a host recounts what a guest, lab, publication, or company previously said, put the host in `speaker_context` and the guest/lab/publication/company in `reported_actor`; do not make a clean direct stance edge unless the evidence supports that direct attribution.
