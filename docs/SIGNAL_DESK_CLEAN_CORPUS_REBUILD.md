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
cluster. The benchmark mirrors production's best-available transcript policy:
`speaker_turn`, `paragraph`, `flattened`, and `asr_diarized` are all eligible
and reported as separate strata. On flattened or ASR text, gold attribution is
indeterminable unless the text itself names the speaker; the hard gate is at
least 99% supported attribution rather than fabricated speaker recovery.

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

### Frozen xAI ASR contract

The 16 acquired ASR fixtures are bound to
`config/signal_desk_rebuild_xai_asr_contract.json` (contract SHA-256
`92893c6a67d487a57b6fa2d322be377d94f4993f391f4a40920914aaec8a10f3`).
The provider exposes no versioned STT model selector, so the recorded model
identifier is `xai_speech_to_text_endpoint_unversioned`. Requests use
`POST https://api.x.ai/v1/stt` with URL input, English, diarization on,
formatting on, filler words off, VAD threshold 0.5, no keyterms, and word-level
timestamps. Post-processing only strips response-edge whitespace, appends one
newline, and performs no case, punctuation, or name normalization. The
resulting benchmark stratum is `asr_diarized`. Production's best-available
policy selects 14 of those fixtures (42 windows); two Gradient episodes retain
better canonical transcripts. The unused ASR fixtures remain contract-frozen
for reproducibility but do not displace higher-quality production text.

Before gold authoring, every ASR episode must pass the deterministic receipt
gate: 80-220 words/minute, monotonic word timestamps, integer speaker labels,
no text gap over 30 seconds, no repeated identical-word run over ten, exact
response/text hash binding, and a metadata-name spelling report. Missing or
phonetic proper nouns are retained as production-shaped ASR, but gold is told
not to split identities solely on an ASR spelling variant. The current frozen
set passes 16/16. A future production rump must use this exact contract; any
provider-contract drift stops the lane and requires re-freezing the four ASR
benchmark shows.

The production rump is not authorized by this benchmark campaign. Its planning
ledger records approximately 3,000 episodes at 0.67 audio-hour/episode and
$0.10/audio-hour, or roughly $200 on xAI. It also preserves the earlier Groq
estimate of roughly $45 at $0.04/audio-hour as an unproven legacy estimate for
the owner's later decision.

### OOD acquisition and isolation

The complete OOD cohort is four to six official first-party episodes per show,
with four publication-stratified episodes frozen into 12 windows per show.
Freakonomics, Hidden Brain, On Being, 99% Invisible, The Allusionist, and
Gastropod are visible development/validation fixtures. Fresh Air, This American
Life, Ear Hustle, and The Moth are whole-show sealed fixtures. Sealed acquisition
prints only aggregate counts and hashes; transcript text never appears in logs
or receipts, and sealed gold packets are written to a distinct private sealed
input tree. CMS migration dates are ignored when the publisher prints an
original air date.

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

The benchmark now mirrors production's best-available transcript selection.
Flattened caption text is an explicit benchmark stratum, not a disqualifier.
Every window records `speaker_turn`, `paragraph`, or `flattened`. On flattened
text, gold attribution is indeterminable unless the text itself names a
speaker; invented names are attribution errors. The full attribution gate
applies to structured text, while flattened text must achieve at least 99%
supported attribution.

That correction first changed current-show qualification from 32/57 to 53/57.
After bounded private acquisition, the final current-show accounting is 57
qualified and zero blocked for flattened text, insufficient episodes, stale
hashes, or missing transcripts. Equity, Me Myself and AI, Security Now, and
The Ben and Marc Show were re-selected and frozen against current transcript
bytes before gold authoring. Each window records both the selected transcript
revision and the frozen hash. The in-domain side is now frozen at 684 windows.

A real-browser sweep checked 261 of the indexed NPR-era How I Built This pages
and found only three genuine long transcripts; the rest were shell pages and
were rejected by the plausibility guard. Because that is fewer than the four
episode-disjoint benchmark inputs required, the remaining exact private ASR
queue was How I Built This, Search Engine, Tech Brew Ride Home, and The
Gradient. Groq was removed from the critical path because its developer-tier
upgrade is unavailable. The bounded xAI REST speech-to-text run processed
exactly four duration-qualified, publication-spread episodes per queued show:
10.66 audio hours at an estimated $1.10. Two HIBT catalog-duration mismatches
were accepted only after a bounded inserted-audio check and multiple
episode-title-token matches. The lane may not expand to a catalog run and did
not write canonical production tables.

Codex CLI 0.147.0 is no longer a blocker. A ten-window structure-stratified
canary passed schema validation, deterministic envelope shape, complete token
accounting, and compatible error handling on all ten windows. The protocol
schema and sanitized receipt are frozen in the repository. No tournament,
development-error inspection, or complete gold-reliability audit is authorized
until all 57 current and 10 OOD shows are frozen into the complete 804-window
benchmark.
