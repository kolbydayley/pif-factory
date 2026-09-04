# Signal Desk design and architecture review

**Decision:** evolve the existing editorial interface, preserving its strongest visual and navigational elements. Reorganize around consequential claims, comparable change, credible disagreement, and inspectable evidence. Ship presentation improvements without pretending the unfinished corpus is approved.

## Read in order

1. [Goals and scoring rubric](01-goals-and-rubric.md): recovered owner priorities, source traceability, acceptance tasks, weights and hard gates.
2. [Scoring results](03-scoring-results.md): shortlist across seven categories, with scan-heavy and research-heavy sensitivity views.
3. [Full 700-option catalog](02-option-catalog.csv): 100 systematic alternatives each for architecture, page composition, component interaction, visualization, summary, alert and aggregation. [JSON](02-option-catalog.json) retains all criterion ratings, tradeoffs, data dependencies and readiness.
4. [Architecture and data contract](04-architecture-and-data.md): complete-system alternatives, chosen page/component hierarchy, source-field mapping, missing semantic artifacts, aggregation rules and Railway rollout.
5. [Live audit with screenshots](05-live-audit.md): keep/refine/replace findings grounded in actual public journeys and current source code.
6. [Release verification](06-release-verification.md): implemented changes, checks, deployment evidence and remaining data-dependent work.

The option catalog contains 70 base mechanisms × ten explicit treatments, not a claim to 700 unrelated inventions or 700 user-tested designs. Scores are informed design judgments. Utility, data readiness, and publication authorization remain separate.

## Boundary with the active data work

The `Own the Signal Desk` thread owns extraction, gold reliability, scorer qualification, canonicalization approval and the clean-corpus release. This lane owns presentation and its integration specification. No frozen benchmark, holdout, gold, model-budget, queue, or production data gate was changed by this redesign.
