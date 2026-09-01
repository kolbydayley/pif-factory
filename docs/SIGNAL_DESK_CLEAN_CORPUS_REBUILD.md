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

### Qualifying coverage, incremental authoring, and complete-freeze boundary

Coverage is not a count of transcript rows. A show qualifies only when four
episode-disjoint transcripts can each produce three distinct early, middle,
and late windows under the frozen character contract; pass the shared ingest
plausibility rule (`words >= duration_seconds / 6` when duration is known);
and collectively span at least three publication months and 60 days. Episodes
are selected across publication periods rather than from one convenient
cluster. A show whose four candidates all require flattened sentence fallback
is reported but does not pass the qualifying-show gate.

Each qualified show may be frozen and sent through Gold A/B/C independently.
Its artifact binds episode IDs, transcript and window hashes, split membership,
contract versions, and gold outputs. This safely lets authoring proceed for the
covered current and OOD shows while acquisition continues. The tournament,
frontier-ceiling measurement, development-error reading, and deterministic
gold-reliability audit remain blocked until all 57 in-domain and 10 OOD show
artifacts assemble to exactly 804 frozen windows.

OOD fixtures live in a separate benchmark database and filesystem namespace,
never in the canonical `episodes` or `segments` tables. Public aggregation is
tested with a sentinel fixture that must remain invisible. Official archives
take precedence over feed transcript tags; tracking-wrapped feed URLs are
unwrapped before matching; and an official page matches only when at least 85%
of the episode-side title tokens are contained in the page title.

## Bounded transcript acquisition

The missing-show lane is benchmark-only: four qualifying episodes per show,
not a full-catalog transcription project.

- How I Built This: try rendered NPR pages from the indexed story map and
  accept only bodies that pass the duration plausibility rule, then fall back
  to ASR. The index is discovery evidence, not proof that transcript text is
  present. Live checks on 2026-08-31 found two indexed pages marked
  `no-transcript`, so those pages remain rejected.
- Marketplace Tech: try a real rendered first-party session, then ASR.
- Search Engine and Tech Brew Ride Home: ASR only.
- The Ben and Marc Show: try full-episode YouTube captions only after at least
  25% of a 40-episode catalog sample title-matches and each chosen upload is at
  least 1,500 seconds; otherwise use ASR.

ASR uses Groq `whisper-large-v3-turbo` only after the owner-provided
`GROQ_API_KEY` is present. The authorized benchmark scope is approximately 20
audio-hours (about $1); full-rump ASR is a separate decision and is not
authorized by this campaign.

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

The original row-count preflight reported 52/57, but the amended qualifying
episode contract disproved that number. Read-only validation on 2026-08-31
found only 31 canonical current shows with four publication-spread,
episode-disjoint, structurally usable transcripts. The rendered Marketplace
lane then acquired and hash-froze four qualifying episodes without inserting
them into canonical production tables, bringing the authorable current set to
32 shows / 384 windows.

Twenty-five current shows remain blocked: 20 contain only flattened selected
transcripts (Security Now also lacks temporal breadth), The Gradient has fewer
than four qualifying dated transcripts, The Ben and Marc Show's nine local
records fail their frozen hash checks, and How I Built This, Search Engine, and
Tech Brew Ride Home have no ready local transcript. The same audit found stale
transcript hashes in several other blocked shows; those files are rejected,
never silently re-hashed.

The 32 per-show artifacts and private Gold A/B inputs are frozen locally, but
no tournament, development-error inspection, or full reliability audit is
authorized. GPT-5.6-sol execution is additionally blocked until the verified
Codex app-server protocol pin (0.144.1) is reconciled with the installed CLI
(0.147.0); model calls do not bypass that attestation.
