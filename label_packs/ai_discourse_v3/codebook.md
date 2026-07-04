# ai_discourse_v3 Codebook

## Purpose

Extract dynamic discourse intelligence, not summaries. The primary unit is a `discourse_event`: one evidence-backed observation about how an actor uses language, frames a concept, takes a stance, makes a claim, signals a product or market movement, or changes terminology.

## What To Optimize For

- Capture many atomic events from substantive transcript text.
- Preserve open vocabulary: surface terms, targets, frames, product names, model names, and candidate concepts should be copied or named naturally instead of forced into a fixed enum.
- Keep stable event families so analysis remains queryable over time.
- Use exact evidence and offsets for every event and candidate.
- Use entities and relationship edges only as dimensions attached to events.

## Stable Event Families

- `term_usage`: notable term, phrase, acronym, model name, product name, or industry wording.
- `frame_usage`: interpretive frame such as capability race, deployment bottleneck, safety governance, compute constraint, enterprise adoption, or platform shift.
- `stance_position`: actor position toward a concept, model, company, release, or trend.
- `forecast`: explicit or implied prediction about timing, capability, adoption, market impact, or regulation.
- `causal_mechanism`: stated mechanism linking causes, constraints, enablers, or consequences.
- `capability_claim`: claim about model, agent, benchmark, infrastructure, or product capability.
- `product_signal`: product, model, launch, roadmap, pricing, integration, or distribution signal.
- `market_signal`: demand, budget, investment, capex, competitive position, labor, or customer adoption signal.
- `risk_signal`: safety, alignment, misuse, governance, security, reliability, or deployment risk.
- `counterclaim`: disagreement, rebuttal, skepticism, exception, or alternative explanation.
- `uncertainty`: explicit uncertainty, hedging, epistemic humility, unknown timeline, or weak evidence warning.
- `adoption_signal`: workflow, developer, consumer, enterprise, or organization adoption pattern.

## Dynamic Concept Rules

- `target.raw_target` is the thing being discussed in local language.
- `target.candidate_concept` is a stable analysis handle proposed by the extractor, e.g. `frontier_ai_end_state`, `agentic_software_development`, `enterprise_ai_procurement`, `inference_time_scaling`, or `open_weight_competition`.
- `surface_terms` are exact terms used or closely present in evidence, e.g. `AGI`, `singularity`, `agents`, `inference scaling`, `GPT-5`, `Gemini`, `Claude`, `Copilot`.
- `frames` are open strings that capture how the issue is framed, e.g. `capability_timeline`, `platform_shift`, `deployment_bottleneck`, `safety_governance`, `cost_curve`, `developer_workflow`.
- Do not invent private intent. Record public narrative movement only.

## Evidence Rules

- Every `evidence` must be an exact contiguous substring from the segment.
- Offsets must point to that exact evidence.
- Do not use transcript title, URL, boilerplate, ads, sponsor copy, or show notes as evidence unless the episode is explicitly an article-style source and the claim appears in substantive show content.
- If the segment has no useful signal, use `no_signal`, `insufficient_evidence`, or `low_signal` and explain why.

