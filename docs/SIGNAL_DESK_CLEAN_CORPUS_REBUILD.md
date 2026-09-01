# Signal Desk Clean-Corpus Rebuild

Status: implementation foundation. Public intelligence surfaces remain
fail-closed until the clean-release receipt exists. Coverage & Trust may remain
available because it exposes operational aggregates rather than extracted
claims.

Campaign: `signal-desk-clean-corpus-2026-08-31`

## Model authority

- GLM is the extraction workhorse: direct GLM-5.2 concurrency 12, OpenCode Go
  GLM-5.2 concurrency 3, and separately qualified GLM-5.3 Flash concurrency 8.
- GPT-5.6-sol medium independently authors Gold A and Gold B, adjudicates Gold
  C, qualifies the deterministic scorer, and supplies the measured single-pass
  frontier ceiling using the exact 6,000-character GLM contract.
- GPT-5.5 is the final publication authority. Its decisions are `accept`,
  `correct`, `split`, `reject`, `require_audio`, `request_wider_context`, and
  `fail_closed`. One wider-context retry is permitted per semantic sample.

## Blocking order

Round 1 cannot start until all four receipts are frozen:

1. `scorer-qualified.json`: scorer specification/code/fixture hashes and a
   one-sided 95% Wilson LCB of at least 0.97 against GPT-5.6-sol adjudication.
   Start with 100 stratified decisions and expand by 50 through 500 as needed.
2. `frontier-ceiling.json`: one GPT-5.6-sol medium pass over development using
   the identical input and output contract.
3. `frozen-gates.json`: absolute gates derived from that measured ceiling.
   Every rate gate uses a one-sided 95% confidence bound. Per-show validation
   gates require at least 50 consequential gold events.
4. `dispatch-qualified.json`: crash, lease expiry, duplicate enqueue, terminal
   semantic failure, and explicit resurrection tests pass.

Any scorer change after round 1 creates a new scorer version and invalidates
affected aggregate comparisons. Development, validation, and holdout membership
is content-hashed before gold authoring. Holdout is aggregate-only and opened
once.

## Benchmark

The target benchmark has 804 windows: 684 in-domain windows sampled evenly
across canonical feeds and 120 OOD windows. Each in-domain show contributes one
development episode, two validation episodes, and one sealed episode, with
three windows per episode. Four complete OOD shows remain source-disjoint and
sealed.

Gold audit begins at 120 windows and expands in 40-window blocks until it
contains at least 1,000 consequential events or exhausts the benchmark. The
critical-error denominator is events; its one-sided 95% Wilson UCB must be below
1%. Fabricated evidence, reversed meaning, wrong source, or unusable context is
also a catastrophic window failure and must occur zero times.

OOD evaluation is shape-aware. Freakonomics, Fresh Air, This American Life,
Hidden Brain, and On Being are claim-dense. The Moth, Ear Hustle, and 99%
Invisible are narrative contamination tests. Gastropod and The Allusionist are
format stress tests. Recall is not used as a hard gate on near-zero narrative
denominators.

## Tournament

Every experiment registry row binds campaign, family, parent, hypothesis,
model/provider, prompt hash, representation hash, scorer version, split, seed,
and artifact hashes. A child changes exactly one variable.

Prompt rounds run for at least 10 and at most 15 rounds. Development provides
item-level diagnostics; validation provides predeclared aggregates. In parallel
with rounds 3-9, the representation family evaluates 8,000-character windows,
turn-aligned overlap, and a speaker-map header as independent one-change
children before testing combinations.

The 130,000 legacy labels are a disagreement oracle, never gold. Shadow audits
sample 50% disagreement, 25% new-only, 15% legacy-only, and 10% agreement
controls.

## Budget and failure recovery

The owner-authorized rebuild grant is
`config/signal_desk_rebuild_budget_grant.json`. It authorizes 20M GPT-5.5
subscription tokens/day only for this campaign until the earlier of 45 days or
the first clean release. It does not change the ordinary 5M global default.

Before corpus rebuild, useful campaign work must demonstrate five consecutive
days (seven preferred) between 18M and 20M tokens without provider quota
failure. A campaign KILL is persistent. It means all ledgers must be reconciled
and the bypass diagnosed. Only Kolby, or an operator explicitly delegated by
Kolby in the current turn, may clear it.

Leased work separates semantic task lineages from append-only attempts. Lease
expiry re-leases the same attempt. Semantic failure is terminal for that
attempt. Explicit resurrection creates a new attempt under the same lineage;
ordinary enqueue never silently deduplicates into a dead attempt.

## Release gates

- Every public candidate is processed through GPT-5.5.
- One thousand shadow windows must be processed. Approval (`accept`, `correct`,
  or `split`) must be between 60% and 95%; either boundary breach blocks release
  for investigation.
- No third-party mention may appear as a person's own statement.
- No public claim lacks accepted evidence, transcript location, and original
  source provenance.
- Funnel, quarantine, evidence, and roster totals reconcile from server data.
- All stable entity/evidence routes crawl without a broken internal link.
- Public payload, rendered DOM, 390px/430px mobile views, and Railway production
  URL pass independent smoke tests.

The clean-release receipt expires the rebuild grant. A failed sealed or release
gate preserves the withdrawn state; it never lowers a threshold or tunes on the
opened holdout.

## Current pre-tournament coverage result

Read-only validation against `data/factory.sqlite` on 2026-08-31 found 57
canonical enabled feeds after adjudicating the duplicate Decoder source, but
only 52 currently have four eligible local transcripts under the frozen 1/2/1
split contract. The five coverage blockers are How I Built This, Marketplace
Tech, Search Engine, Tech Brew Ride Home, and The Ben and Marc Show. The builder
fails closed at 52/57; it does not silently reduce the benchmark or begin the
tournament. Transcript coverage for those sources, or a new explicitly
approved benchmark contract, is required first.
