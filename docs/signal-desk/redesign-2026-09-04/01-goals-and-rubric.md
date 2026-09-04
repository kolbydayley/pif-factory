# Signal Desk: goals, evidence, and selection rubric

Research checkpoint: 2026-09-04. This is the design specification, not a claim that the new corpus is approved or deployed.

## Primary evidence and priority

1. Kolby's answers in `Own the Signal Desk` (thread 01a04040-77ee-77f2-9d26-a5ca1ae56986), user message at source rollout line 430: discover important shifts, understand the major issues, compare what trustworthy people actually say, read specific quotes and original sources; support both two-minute mobile scans and deeper research; equally rich people pages; public access is acceptable.
2. Same thread, line 365: progressively drill into charts, issues, people, and evidence, with the click hierarchy governed by the data model. Think from the goals, not literal requests alone.
3. Same thread, line 3285: explicitly accepted strategic redesign: change, implications, disagreement, evidence/coverage; Briefing, Issues, Voices, Ask; secondary Coverage & Trust; cited synthesis and original-source access.
4. Same thread, lines 4073 and 4945: weak excerpts and unattributed voices defeat the product's purpose. Source-context validation is essential; attractive presentation cannot compensate for poor evidence.
5. Same thread, lines 1447, 1910, 2002, 2198: honest share-of-discourse charts, shows/episodes/segments at each applicable funnel stage, dynamic counts, precision-first topic canonicalization.
6. Current redesign thread, September 4: preserve worthwhile elements; evaluate rather than automatically discard. Research and score a broad option space before website implementation. No new discovery questions needed. Retain Railway compatibility.

The full earlier question behind answer number six was not recovered; no product exclusion is inferred from the isolated 'no' in that answer. Public access does not authorize publishing raw transcripts or benchmark material.

## Value propositions and observable success

| ID | Value proposition | User success test | Product consequence |
|---|---|---|---|
| V1 | Spot consequential change without reading everything | In two minutes identify three supported changes, their dates, and the main caveat | Briefing prioritizes change magnitude and evidence eligibility; modest information density, no detector dump |
| V2 | Understand the argument and its implications | Explain the substantive claim, why it matters, and what would change the interpretation | Claim-first briefs; distinguish observed evidence from analyst interpretation; observable watchpoints |
| V3 | Compare credible disagreement | Find at least two attributable positions on the same proposition and understand how they differ | Proposition-level contrast, not generic positive/negative sentiment; no assumed false balance |
| V4 | Know whose views are worth reading | Inspect a voice's consequential claims, expertise basis, direct statements, and supported changes over time | Voice profiles with provenance; reach/frequency separated from credibility |
| V5 | Zoom from overview to proof and back | Open a chart month, claim, source context, voice, original source; return with filters intact | Stable entity IDs, explicit origin, URL state, bounded evidence context |
| V6 | Know what the corpus cannot establish | See dates, denominators, sampled-source concentration, missing coverage, approval state | Trust beside the claim; no implication that podcast sample equals industry consensus |
| V7 | Keep the experience useful as data changes | New accepted data updates all dependent views coherently; old links still work | Versioned publication manifest, consistent split payloads, aliases and retirement states |

## What is worth keeping

The current live briefing screenshot supports preserving the restrained off-white/ink palette, Fraunces headings, IBM Plex text, thin editorial rules, visible primary navigation, full-card detail affordances, and small attention previews. The repository supplies working split payload loading, stable hash routes, alias handling, month-level chart buttons, separated direct/mentioned sections, and the coverage page. Those mechanics are candidates to refine, not presumed replacements.

Reduce oversized introductions where they delay evidence. Keep two columns when they aid desktop comparison and a single ordered reading column on mobile. Preserve identity and chart conventions while repairing semantics, scope and research continuity.

## Scoring before implementation

Scores are explicit design judgments, not user-study measurements. Evaluate both target utility and current deliverability. No high average can override a failed trust gate.

Each criterion uses 0–5: 0 contradicts the goal, 1 severe mismatch, 2 conditional/weak, 3 adequate, 4 strong, 5 directly fulfills the task with clear evidence access. A one-point uncertainty is normal. Differences under three points out of 100 are a tie, resolved by preserving existing strengths and reducing dependencies.

| Criterion | Weight | Evidence anchor |
|---|---:|---|
| Consequential understanding (U) | 22 | V1–V2; actual implications rather than counts |
| Evidence and trust (T) | 22 | V3–V6; attribution, context, coverage, no unsupported conclusion |
| Two-minute scanning (S) | 14 | V1; useful material early on mobile |
| Research depth and navigation (D) | 14 | V4–V5; meaningful drill-down and return |
| Comparison and disagreement (C) | 10 | V3; matched propositions with competing evidence |
| Accessibility and mobile fit (M) | 10 | V1/V5; usable without color, hover, precision pointing |
| Preservation and operational fit (P) | 8 | V7/current correction; Railway, split data, familiar visual identity |

Utility = sum(weight × rating / 5). Report feasibility separately: ready from approved public data; derivable with deterministic aggregation; requires new adjudicated semantic artifact; blocked pending clean-corpus approval. Avoid multiplying utility by a guessed likelihood: that hides strategically valuable deferred choices.

Hard gates: no invented attribution; no unsupported synthesis; no benchmark/holdout leakage; no misleading temporal denominator; no connection equated with credibility; no fake enrollment/notification completion; no loss of legacy routes; no production promotion without the pipeline's release authority.

Sensitivity: re-score with scan-heavy weights (18/22/24/10/8/12/6) and research-heavy weights (22/24/8/20/14/6/6). Compare rank changes and preserve Pareto alternatives. Score options within their category; a chart is not a substitute for an architecture. Evaluate assembled systems for duplication, scroll burden, and shared data dependencies after category scoring.

## Exploration boundary

Consider 100 alternatives in each of seven categories: whole-site architecture, page composition, component interaction, visualization, summary format, alert behavior, and aggregation. Use explicit combinations of distinct mechanisms and treatments; label these as systematic variants, not 700 unrelated original inventions. Every row must state the mechanism, data dependency, limitation, criterion ratings and resulting score. Shortlist winners for actual composition review, not automatic top-score concatenation.

## Current data boundary

`Own the Signal Desk` currently owns the gold audit/scorer/publication-gate work. Its September 4 messages report scorer v6, canonical topic identity as a separate publication gate, semantic disagreement adjudication pending, and A1/GLM still blocked. This design lane owns documents and presentation; it does not change gold, scorer, benchmark, dispatch, or gate files.

The extraction contract in `research_factory/signal_desk_rebuild_contracts.py` yields event ID, claim text, speech act, exact evidence/offsets, speaker, quoted person, mentioned people, attribution type/confidence, issue label/aliases, stance and publishability. It does NOT itself provide validated implication summaries, paired disagreements, expert authority, consensus change, causal links, prediction outcomes, or notification subscriptions. Those require additional, explicitly versioned derivations or approval artifacts.
