# ai_discourse_v2 Codebook

Unit of analysis: one evidence-backed observation inside a transcript segment. A segment-level JSON label may contain zero, one, or many observations.

General rules:
- Prefer dense coding. If a segment contains five distinct relevant ideas, output five observations.
- Evidence must be copied exactly from the segment and paired with `evidence_start` and `evidence_end` character offsets.
- Do not infer private company intent, internal strategy, or causality. Code narrative movement and product-release correlation as signals, not proof.
- Separate descriptive coding from extracted claims. A mention of "agents" can be a `topic`; "agents will replace routine work" is also a `forecast` or `adoption_pattern` claim.
- Use `not_present` when the segment is clean but contains no relevant AI/technology discourse.
- Use `insufficient_evidence` when the segment hints at a construct but lacks enough text to code it safely.
- Use `low_signal` for boilerplate, ads, table-of-contents fragments, repeated show descriptions, links, or transcript noise.

Code families:
- `topic`: explicit or clearly substantive discussion topic.
- `technical_mechanism`: model behavior, architecture, evals, scaling, data, inference, infrastructure, or engineering mechanism.
- `product_release_signal`: product names, releases, roadmaps, feature shifts, competitive responses, or launch reactions.
- `market_investment_narrative`: demand, budgets, capex, adoption economics, competition, valuation narratives, or investment-relevant framing.
- `safety_risk`: alignment, misuse, evals, governance, deployment safeguards, reliability, security, or social risk.
- `adoption_pattern`: enterprise deployment, workflow change, procurement, user behavior, production readiness, or adoption barriers.
- `terminology_drift`: explicit change in wording or framing, such as "chatbots" to "agents" or "RAG" to "memory".
- `actor_org_position`: a person or organization taking a position, changing stance, or being linked to a viewpoint.
- `relationship_edge`: collaboration, employer, investor, competitor, ecosystem, guest-host, or org-product relationships.
- `forecast`: prediction about capabilities, adoption, market behavior, timelines, or regulation.
- `causal_claim`: a stated mechanism where one thing is said to cause, enable, constrain, or explain another.
- `uncertainty_marker`: caveat, disagreement, hedging, missing evidence, or unknowns.
- `counterclaim`: contradiction, rebuttal, skepticism, or alternative explanation.

High-value code ids:
- Use `agi`, `singularity`, `agents`, `ai_coding`, `test_time_compute`, `inference_scaling`, `open_weights`, `safety_alignment`, `evals`, `enterprise_ai`, `frontier_labs`, `model_capability`, `scaling_law`, `data_bottleneck`, `compute_bottleneck`, `product_launch`, `roadmap`, `platform_shift`, `market_demand`, `competitive_positioning`, `capex_infrastructure`, `regulation_policy`, `adoption_barrier`, `deployment_pattern`, `terminology_shift`, `person_org_position`, `collaboration_relationship`, `acquisition_investment`, `forecast_capability`, `forecast_market`, `causal_mechanism`, `uncertainty_caveat`, `counterclaim_rebuttal`, or `other`.

Speaker and entity rules:
- Use `speaker` only when the segment text or context supports it. Otherwise use `null` and `speaker_role: "unknown"`.
- Do not include entities that appear only in URLs, ad text, generic show boilerplate, or unrelated title cards.
- Relationship edges require a clear relationship in the segment, not just co-mention.

Quality rules:
- Mixed-page/show-note artifacts can be coded when they contain substantive evidence, but must set `needs_review` true and include `mixed_page`.
- If any substantive output has unclear speaker or unclear entity, include the relevant quality flag.
- If using this output before human review, include `llm_adjudicated_not_human_validated`.
- For empty labels, use an explicit `not_present_reason`, keep `observations` empty, and set confidence according to text quality.
