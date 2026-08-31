# Signal Desk V5 design QA

## Comparison sources

- Reference: `/tmp/signal-desk-shifts-audit-2026-08-31/07-mobile-shifts-home.png`
- Implementation: `/tmp/signal-desk-v5-mobile-focused.png`
- Combined comparison input: `/tmp/signal-desk-design-qa-comparison-final.png`
- Desktop implementation: `/tmp/signal-desk-v5-final-desktop.png`

## Viewports and states

- Mobile: 390 × 844, decision-grade `vibe coding` issue detail.
- Mobile reflow: 430 × 932, Ask and issue-detail states.
- Desktop: 1440 × 900, Briefing with full document capture.
- Focused regions: header/secondary Coverage affordance, issue summary,
  confidence badge, evidence metrics, two-minute brief, sentence citations,
  and fixed mobile navigation.
- Full-view regions: desktop ranked signal grid and footer; mobile top-to-nav
  viewport with the internal research scroller.

## Iteration history

1. Replaced the detector dump with ranked, navigable intelligence cards while
   preserving the Fraunces/IBM Plex editorial visual language.
2. Moved the mobile navigation into its own safe-area-aware app-shell region
   after the first browser pass found content overlap.
3. Collapsed the mobile Ask toolbar to one column after the first 390px pass
   found the input clipped.
4. Corrected the funnel field mapping and separated shows, episodes, and
   segments so stage loss is visible without a misleading shared conversion.
5. Rebuilt person attribution against episode-context speaker maps after the
   click-path audit found third-party Dario Amodei references under direct
   speech. The final person view shows zero accepted direct excerpts for that
   profile and keeps all six ambiguous/mentioned records separate.
6. Rebuilt person claims and changed-view summaries from accepted direct
   evidence only, preventing unsafe aggregate metadata from reintroducing
   third-party statements.

## Required surfaces

- Briefing navigation, signal categories, cards, and Ask entry: verified.
- Issues search/filter/empty state and decision-grade detail: verified.
- Month chart keyboard buttons, low-coverage labeling, citations, and
  evidence drill-down: verified.
- Voices search/views, direct-versus-mentioned separation, authority versus
  Network reach, and co-appearance labeling: verified.
- Ask canonical resolution, cited answer, alternatives, and limitation path:
  verified.
- Coverage funnel, clickable stages, needs-attention roster, duplicate feed,
  source search, and enrollment validation: verified.
- Legacy route redirects and origin-aware unavailable state: verified.
- 390px and 430px horizontal overflow: 0px.
- Mobile navigation/content overlap: none; app viewport ends above nav.
- Browser console warnings/errors across audited paths: none.

## Final result

passed
