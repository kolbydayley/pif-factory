# Signal Desk design QA

**Source visual truth path**

- Desktop: `work/pif-ops/design-audit-signal-desk/01-current-desktop.png`
- Mobile: `work/pif-ops/design-audit-signal-desk/02-current-mobile.jpg`

**Implementation screenshots**

- Desktop home: `work/pif-ops/design-audit-signal-desk/08-redesign-desktop-home.jpg`
- Mobile home: `work/pif-ops/design-audit-signal-desk/04-redesign-mobile-home.jpg`
- Mobile topic: `work/pif-ops/design-audit-signal-desk/05-redesign-mobile-topic.jpg`
- Mobile evidence: `work/pif-ops/design-audit-signal-desk/06-redesign-mobile-evidence.jpg`
- Mobile person: `work/pif-ops/design-audit-signal-desk/07-redesign-mobile-person.jpg`

**Viewport and normalization**

- Desktop source and implementation: 1280 x 720 CSS px, device scale 1,
  1280 x 720 captured pixels.
- Mobile source and implementation: 390 x 844 CSS px, device scale 1,
  390 x 844 captured pixels.
- No density normalization was needed. Both pairs use the same viewport,
  crop, theme, and route state.

**State**

Public, unauthenticated home page at `#home/shifts`, plus the core drill-down
states for `frontier ai end state`, negative stance evidence, one evidence
record, and the Grant Sanderson person page.

**Full-view comparison evidence**

- Desktop paired comparison:
  `work/pif-ops/design-audit-signal-desk/09-desktop-comparison.jpg`
- Mobile paired comparison:
  `work/pif-ops/design-audit-signal-desk/10-mobile-comparison.jpg`

The implementation preserves the source's editorial paper, Fraunces display
type, IBM Plex utility type, semantic signal colors, compact sparklines, and
card language. The hierarchy intentionally changes from a one-page report to
a research navigator: clearer task framing, explicit exploration affordances,
separate Shifts/People/Topics destinations, and real detail routes.

**Focused region comparison evidence**

The equal-size mobile comparison makes the header, first two signal cards,
typography, spacing, tap targets, and persistent navigation readable at 1:1.
Separate full-viewport topic, evidence, and person captures were inspected for
the newly introduced states; no tighter crop was needed.

**Required fidelity surfaces**

- Fonts and typography: Fraunces and IBM Plex remain consistent with the
  source. Display sizes wrap cleanly at 390px; utility copy remains readable.
- Spacing and layout rhythm: 16px mobile page gutters, consistent 12-16px
  card rhythm, and 44px-or-larger controls. No page-level horizontal overflow.
- Colors and visual tokens: warm paper and semantic green/red/blue language
  match the source. Focus, status, stance, and trust states maintain contrast.
- Image quality and asset fidelity: neither source nor implementation uses
  imagery, logos, or decorative image assets. Data marks remain crisp native
  bars at both densities.
- Copy and content: labels describe user goals (what changed, major issues,
  trusted voices, evidence and sources) and trust gaps are stated honestly.
- Accessibility and behavior: semantic buttons/links, visible keyboard focus,
  reduced-motion support, practical mobile tap targets, real browser history,
  and original-source links were exercised.

**Primary interactions tested**

- Shift card to topic detail.
- Negative stance filter to a data-governed topic slice.
- Evidence card to its own route.
- Original source link presence and valid HTTPS target.
- Person route with recurring claims and evidence.
- Mobile primary navigation and desktop primary navigation.
- Inline JavaScript parsed successfully. The in-app browser did not expose a
  console-log API; all tested routes rendered and transitioned without an
  uncaught-error or unavailable-state interruption.

**Comparison history**

1. First mobile topic pass: P1 page-level horizontal clipping caused by the
   chart's minimum width; P2 programmatic focus ring surrounded the main view.
   Fix: constrained research-grid children, kept overflow inside the chart
   scroller, hid focus only on the programmatically focused main container,
   and recaptured the topic at 390 x 844.
2. First mobile person pass: P1 hierarchy opened on position changes instead
   of biggest claims; same-episode stance differences also appeared as false
   changes. Fix: limited mobile aside-first ordering to topic pages and
   required position changes to cross episode boundaries. The person page was
   rebuilt and recaptured at 390 x 844.
3. Post-fix comparison: no actionable P0/P1/P2 findings remain. The source is
   denser above the fold, while the implementation's slightly larger cards and
   persistent navigation are an intentional mobile research tradeoff.

**Findings**

- No remaining P0, P1, or P2 issues.

**Open questions**

- None blocking. Excerpt quality still depends on upstream position extraction;
  short, source-linked evidence and explicit trust gaps prevent overclaiming.

**Implementation checklist**

- [x] Preserve editorial visual language.
- [x] Make mobile scan and deep-research paths usable.
- [x] Add topic, stance, week, person, and evidence routes.
- [x] Verify key interactions and source links in the in-app browser.
- [x] Re-run aggregate regression tests and JavaScript syntax validation.

**Follow-up polish**

- P3: show compact topic aliases after a clustering model is available; this
  would reduce long open-vocabulary labels without hiding the source concept.

final result: passed
