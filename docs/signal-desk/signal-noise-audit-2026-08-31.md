# Signal Desk signal-noise audit — 2026-08-31

## Outcome

Signal Desk now fails closed on thin records instead of dressing them up as decision briefs. The public surface contains 20 decision-grade issues, 8 explicitly limited watchlist items, and 6 voice profiles with directly attributed evidence. The remaining 239 issue records and 90 voice records remain addressable for alias history, but render as compact withheld candidates.

## Walkthrough

1. Opened the production US–China AI competition issue at a 390 × 844 mobile viewport. It showed four zero metrics, no publishable excerpt, generic watchpoints, and a near-duplicate related issue despite looking like a finished brief.
2. Audited all 267 generated issues and found 261 with no accepted evidence. The shared failure was an incorrect person-speaker gate applied to issue-level source excerpts.
3. Added accepted source-level evidence with source URLs, episode identity, conservative claim-speaker recovery, quality scoring, and deduplication. Person profiles continue to require verified direct speech.
4. Applied the actual decision-grade publication threshold: at least 3 accepted excerpts, 3 episodes, 2 shows, and 2 named voices. Thin records are excluded from Issues and Ask; only 8 near-threshold records appear in Watchlist with their limitation.
5. Collapsed adjudicated identity duplicates, including US–China race/competition, AGI timeline(s), open-source-model variants, AI existential risk/p(doom), broad AI employment variants, broad AI regulation variants, hallucination variants, nuclear energy/power, and several other exact issue synonyms. Old aliases resolve to the surviving stable issue.
6. Reduced mobile evidence overload: named attributable excerpts rank first, six excerpts appear before an explicit expansion control, generic watchpoints were removed, and the two-minute section now distinguishes observed change, qualification, and evidence mix.
7. Removed low-evidence people from public discovery and co-appearance links. A public voice now requires at least 2 accepted direct excerpts from 2 episodes; third-party mentions remain separate.
8. Crawled every published route: Briefing, Issues, Voices, Ask, Coverage & Trust, 20 issue pages, and 6 voice pages. All 31 rendered without unavailable, load-error, or empty-evidence shells.
9. Audited all 338 accepted issue excerpts and 84 public direct-voice excerpts. There were zero missing source URLs, citation failures, webpage-chrome matches, unsafe direct-attribution records, or dangling aliases.

## Before and after

Before: `/tmp/signal-desk-noise-audit/01-us-china-top.png`

After, corrected issue: `/tmp/signal-desk-noise-audit/02-us-china-corrected.png`

After, researchable issue directory: `/tmp/signal-desk-noise-audit/03-researchable-issues.png`

After, strongest evidence first: `/tmp/signal-desk-noise-audit/04-us-china-evidence.png`

## Remaining limitation

The current brief text is deterministic and citation-backed, but it is not yet a genuinely issue-specific AI synthesis of implications and competing arguments. The UI no longer pretends otherwise. A future synthesis step should be allowed to publish only when every factual sentence cites accepted evidence and should fail closed if grounding or retrieval confidence is weak.
