# Presentation release verification

## Implemented

- Preserved the existing Fraunces/IBM Plex identity, off-white/ink palette, muted chart colors, four primary destinations, source roster and stable hash routes.
- Reorganized issue and voice pages around source statements with compact counts, section navigation and expandable depth.
- Made the legacy dataset, dates, attribution gaps and absent source context explicit. Removed unsupported authority/decision-grade assertions and generic 'why it matters' filler from research presentation.
- Classified zero-recent-activity records as historical evidence. Recent-count dates use the global corpus window, not the last month present in an individual issue's sparse series.
- Added mobile research-view selection, an accessible month selector and a monthly data table while retaining the chart.
- Aligned month-filtered counts, excerpts and stance examples; preserved origin/filters/scroll when returning from evidence.
- Replaced fake coverage stage drill-downs with honest stage summaries, applicability-aware segment conversions and canonical/raw source explanations.
- Kept source-request drafts intact during filtering and stopped timed refresh from erasing forms. Intake explicitly opens a draft or copies a request; it does not claim enrollment.
- Kept Ask as an honest issue lookup, preserving a path for future approved synthesis.
- Prevented stale asynchronous responses from replacing newer navigation; handled malformed route escapes without a crash.
- Exposed the full briefing list via progressive disclosure rather than silently stopping at sixteen.

## Verified locally

- Nine Node frontend tests passed, including all 28 published issue views, six voice views and 540 reachable evidence views rendered against the packaged public snapshot. These are renderer/data-contract checks, not 574 browser sessions.
- Twelve Python builder/publisher contract tests passed.
- JavaScript syntax and `git diff --check` passed.
- Browser-verified selected month counts (one August excerpt, one source, zero named excerpts), evidence context-missing state, original source links and return to the exact selected month.
- Browser-verified voice-origin evidence return and actual context display when supplied.
- Browser-verified source-request draft survival across roster filter changes, without submitting any request.
- Mobile issue at 390px: document width 390; content width 358 equals client width 358.
- Mobile coverage at 430px: document width 430; content width 398 equals client width 398.
- Mobile briefing at 430px inspected visually; compact selector replaces wrapped chips, original chart metaphor retained.

## Important limits

This is a presentation release, not a clean-corpus release. Public payload JSON is preserved byte-for-byte; no gold, holdout, extraction, scorer, budget, queue or publication gate was changed. The active data lane remains the authority for approving the new corpus.

Substantive cited strategic briefs, adjudicated proposition-level disagreements, verified expertise, meaningful changes of mind, source-scoped loss memberships, forecasts and private notifications remain data/capability dependent. Their page/component/input contracts are specified in `04-architecture-and-data.md`; they are not represented as completed features.

The present corpus still contains questionable fragments and uncertain attribution. The redesign exposes those limitations and source paths; it does not certify the data. No screen-reader compliance or universal accessibility claim is made. Browser verification covers representative flows; the renderer test covers all published generated research entities.

## Deployment receipt

Pending production upload and public verification. The target is the existing Railway `signal-desk` service, production environment, canonical URL. Only the site artifact is to be deployed.
