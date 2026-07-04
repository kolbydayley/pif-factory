# ai_discourse_v1 Codebook

Unit of analysis: one transcript segment. Code only what is supported by the segment text and supplied episode/source context.

General coding rules:
- Use short evidence quotes only. Evidence must be a compact phrase from the segment, not a long passage.
- Evidence should be copied exactly from the segment when possible. Prefer the shortest phrase that proves the code.
- When the prompt context includes `start_char` and `end_char`, treat those as the segment's corpus span; do not invent global offsets.
- If a claim, stance, speaker, or entity is ambiguous, set `needs_review` to true and explain why.
- If any confidence score is below 0.60, set `needs_review` to true unless the output is an intentionally empty/minimal label for a low-signal segment.
- Do not infer private company intent, internal strategy, or causality. Label narrative movement as an observed claim, not as proof.
- Prefer empty arrays over invented labels when the segment does not contain the construct.
- Do not code host introductions, ads, housekeeping, sponsorship reads, or biography unless they contain substantive AI discourse.
- Do not include entities that are only part of a URL, ad read, title card, or boilerplate.

Topic inclusion rules:
- `agi`: explicit AGI or artificial general intelligence discussion.
- `singularity`: explicit singularity framing or replacement of AGI language with singularity language.
- `agents`: agentic systems, tool use, computer use, autonomous workflows, or multi-step task execution.
- `test_time_compute`: explicit test-time compute, reasoning compute, inference-time search, or similar framing.
- `inference_scaling`: inference scaling as a deployment or capability trend.
- `ai_coding`: coding agents, code assistants, software engineering automation, Codex/Cursor/Claude Code style products.
- `open_weights`: open weights, open model releases, Llama/Mistral/open-source model strategy.
- `safety_alignment`: safety, alignment, misuse, evals, risk, governance, or deployment safeguards.
- `enterprise_ai`: enterprise adoption, workflow automation, ROI, copilots, procurement, or production deployment.
- `other`: use sparingly for AI discourse that is important but not covered by the named topics.

Topic exclusion and edge cases:
- Mentioning "agent" as a human representative, talent agent, or generic actor is not `agents`.
- Mentioning "open source" for non-model software is not `open_weights` unless model weights or model-release strategy are discussed.
- Mentioning "safety" as workplace safety, financial safety, or generic caution is not `safety_alignment`.
- Enterprise customer names alone are not `enterprise_ai`; code only when adoption, deployment, procurement, ROI, or workflow use is discussed.
- Code `singularity` separately from `agi` only when singularity language is explicit or the segment contrasts the two frames.

Stance rules:
- `bullish`: speaker frames the topic as advancing, useful, commercially attractive, or strategically important.
- `skeptical`: speaker doubts capability, demand, timelines, or strategic value.
- `mixed_or_descriptive`: speaker describes the topic without a clear positive/negative stance or includes balanced tradeoffs.
- `warning`: speaker emphasizes risk, harm, misuse, safety, or negative externalities.
- `unknown`: text is too ambiguous to infer stance.

Claim rules:
- `prediction`: forward-looking claim about future capability, adoption, market behavior, or timelines.
- `inside_baseball_signal`: claim framed as an ecosystem signal, internal language shift, strategy hint, or expert-community movement.
- `product_observation`: concrete statement about a product, launch, roadmap, feature, or company offering.
- `technical_claim`: claim about model behavior, architecture, evaluation, scaling, infrastructure, or engineering properties.
- `market_claim`: claim about demand, competition, spending, investment, regulation, or business impact.
- `descriptive`: neutral summary of what the segment discusses.

Evidence requirements:
- Every topic and claim must have evidence that appears as an exact contiguous substring copied from the segment.
- Keep evidence under the schema limit and avoid combining unrelated phrases.
- Do not use ellipses, stitched-together quotes, paraphrases, summaries, or normalized wording in `evidence`.
- If the supporting phrase is absent, omit the code. Use summaries only in non-evidence fields.
- A terminology shift requires both sides of the shift or a clear contrast in framing; otherwise use a normal topic or claim.
- Product or release observations must identify the product/company in the segment or context; otherwise mark ambiguous.

Confidence guidance:
- 0.80-1.00: explicit, direct, and well-supported by the segment.
- 0.60-0.79: supported but with some ambiguity or missing context.
- 0.30-0.59: weakly supported, indirect, or speaker/entity unclear; usually set `needs_review`.
- 0.00-0.29: only use for empty/minimal labels when the segment barely supports coding.

Required review triggers:
- Evidence quote is paraphrased instead of exact.
- Speaker or organization is inferred from context rather than explicit.
- The segment contains sarcasm, disagreement, fast topic switching, or unclear pronouns.
- The label would support a product-release or investment narrative if wrong.
- The segment appears to be mostly transcript noise, ad text, or malformed captions.

Complete-output examples:
- For a low-signal segment, return empty `topics`, `terminology_shifts`, and `claims`, set `overall_confidence` below 0.30, and set `needs_review` only if the text is noisy or ambiguous.
- For a strong AGI-to-singularity signal, include `agi` and/or `singularity` topics only when those exact frames appear, add a `terminology_shifts` item only when the segment explicitly contrasts the terms, and cite the shortest quote proving the contrast.
