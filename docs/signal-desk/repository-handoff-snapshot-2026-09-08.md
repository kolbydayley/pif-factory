# Repository handoff snapshot — September 8, 2026

Kolby explicitly requested committing and pushing all local repository changes
so this work can be handed off elsewhere. This snapshot preserves the existing
shared-worktree edits; it is not an independent approval of their architecture,
gold exclusions, budget assumptions, or production readiness.

Start with `comprehensive-chat-handoff-2026-09-08.md` in this directory.

## Included work

- Full-episode speaker turn assignments, confidence filtering, and labeling guidance.
- Attribution-lab append mode, unknown-speaker handling, and confidence reporting.
- Subscription window/reservation changes and headless reasoning configuration.
- Catalog handling of undated episodes.
- Gold audit/exclusion tools and their A1/A2 consumer integrations.
- Browser-caption localhost receiver.
- Associated tests and runbook updates, plus the existing empty `nohup.out`.

The exclusion integrations filter events in memory. Their presence in this
snapshot must not be interpreted as authority to shrink a frozen benchmark or
claim that A1/A2 or corpus reliability now passes. Review the exclusion ledger,
denominators, provenance, and governing campaign rules before executing them.

## Verification for this snapshot

Seventy focused tests passed across attribution lab, daily extraction, prompt
delivery, catalog backfill, subscription budget, gold exclusions, and the gold
audit script. `git diff --check` passed. A credential-pattern scan across source,
tests, docs, and config found no matching credential patterns; this is a bounded
check, not a guarantee that the entire repository history contains no secrets.

No paid extraction, gold audit, exclusion export, or production deployment was
launched to create this snapshot.

## Important portability limitation

The private `work/signal-desk-rebuild/gold-authoring-v2/` artifacts are not tracked
by git. Pushing this snapshot does NOT upload benchmark transcripts, sealed gold,
SQLite databases, completion receipts, provider outputs, or local credentials.
The comprehensive Markdown handoff summarizes those local results and points to
their paths, but those paths will not materialize in a fresh clone.

A remote successor can inspect the code, documents, tests, and committed history.
Reproducing or resuming the campaign requires a separately authorized secure
transfer or access to the original machine. Do not force-add private/ignored
artifacts or expose sealed answers merely to make the clone self-contained.
