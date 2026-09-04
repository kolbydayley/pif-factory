# Selected architecture and data contract

## Decision

Preserve the editorial shell and four primary destinations: **Briefing, Issues, Voices, Ask**. Keep **Coverage & Trust** secondary. Refine the hierarchy and meaning within these surfaces rather than replacing the product with a dashboard builder, feed reader, graph explorer, or chat-only interface.

The entity backbone is: **issue/question → proposition → claim → evidence → source episode**, with **voice** intersecting claims through explicit attribution roles. A **change** is a versioned comparison between compatible observations; a **brief** is a cited interpretation of accepted claims; a **watchpoint** is an observable condition derived from a brief. These are different objects. A topic's volume is neither a claim nor a conclusion.

The 700-row exploration uses 70 base mechanisms with ten explicit treatments each. Its utility scores are heuristic screening, not measured preference or proof of superiority. The strongest complete architecture remains the editorial briefing with progressive entity research. The extra permanent 'trust rail' variant scores highly but should become compact inline scope/caveats on mobile, not a competing sidebar on every page. A high component score is not permission to cram all components into one screen.

## Complete-system alternatives, with interaction costs included

| Architecture | Goal fit /100 | Main benefit | Main sacrifice | Decision |
|---|---:|---|---|---|
| Preserve shell, improve claim-first briefing and dossiers | 92 | Familiar, quick entry, rich evidence paths | Semantic derivations still required | Select |
| Preserve all current layouts, fix labels only | 76 | Lowest transition cost | Generic summaries and excess scroll remain | Interim safety work only |
| Issue atlas home plus secondary briefing | 85 | Excellent intentional research | Weaker discovery of unknown changes | Keep issue directory as secondary entry |
| Debate-first home | 81 | Makes competing arguments prominent | Overweights disagreement; unsupported until matching exists | Add issue-level contrast when approved |
| Timeline home | 78 | Temporal comprehension | Chronology crowds relevance | Secondary history section |
| Chat-first workspace | 72 | Flexible intentional questions | Cannot surface unknown questions; expensive grounding contract | Keep Ask but honest about capability |
| Voice directory home | 71 | Person-led exploration | Personality bias, fragmented industry picture | Keep rich voice pages |
| Evidence ledger home | 69 | Provenance is immediate | Reader performs all synthesis | Evidence drill-down and fallback |
| Personalized multi-panel terminal | 65 | Flexible saved workflows | Setup, clutter, state/backend dependencies | Defer |
| Network/map home | 54 | Relationship discovery | Dense, weak on mobile; semantic edges unavailable | Optional deep view after adjudication |

These are separate holistic judgment scores, not averages of category scores. The selected design sacrifices some graph/comparison power on the first screen to improve the repeated two-minute → ten-minute research journey. Scan-heavy and research-heavy weights preserve the editorial family in the shortlist; within-family differences are ties. Exact scores are not statistically meaningful.

## Page and component blueprint

| Page / route | Core user job | Ordered components | Data inputs | Unavailable/limited state |
|---|---|---|---|---|
| Briefing `#briefing` | What is worth understanding now? | Compact date/scope header; 3–5 priority changes; short cited claim per change; strongest caveat; attention preview; grouped further reading; Watchlist separate | published-through, approved briefs, comparable change records, source breadth, evidence IDs | Explicit legacy/limited coverage notice; do not manufacture implications or classify zero activity as acceleration |
| Issue directory `#issues` | Find an important question | Alias-aware search; result count; honest sort choices; compact rows with titled charts; filters retained in URL; full result access | canonical issues, display title, aliases, normalized history, period bounds, breadth | No invented growth rank; no silently unreachable results after row 80 |
| Issue dossier `#issue/<id>` | Understand this issue and decide what to read | Question/title; scope; compact briefing; substantive approved claim; best attributable evidence; argument contrast; attention history; voices; evidence ledger; related research leads | issue/proposition IDs, approved claims, brief sentences, paired disagreements, monthly aggregates, source/voice joins | Evidence-only research mode when synthesis unavailable; indicate missing attribution/context; no 'decision-grade' from counts alone |
| Change detail `#change/<id>` (future contract) | Understand a specific development | Change statement; before/current periods; substantive claim pair; implications; counterevidence; watchpoint; source list | stable change ID, method version, prior/current stats and source cohorts, cited semantic interpretation | Do not add route until a real change entity exists; use issue/month route meanwhile |
| Voice directory `#voices` | Find whose statements to inspect | Search by person/issue/show; direct-evidence count; domain/role when substantiated; recent supported claim; honest sort | canonical person ID/name, claim IDs, domain expertise sources, dates | 'Most direct evidence' instead of unsupported influential/rising labels |
| Voice dossier `#voice/<id>` | What does this person actually claim? | Name, supported expertise basis; 3 consequential claims; direct evidence; issue-specific position history; verified changes; mentions separate; source appearances | direct/quoted/reported/mentioned roles, claims, issue map, dates, expertise artifact | Reported paraphrase labeled distinctly; no universal authority score; no made-up position from topic names |
| Comparison `#compare/<issue>/<voices>` (future contract) | Compare positions on one question | Shared proposition; dated claim rows; side-by-side evidence; assumptions; unknown cells; scope and source access | adjudicated same-proposition links, voice IDs, dates, evidence | Not a generic sentiment matrix; no relation means 'not established' |
| Evidence `#evidence/<id>/<origin>/<origin-id>` | Can I verify the claim? | Claim and attribution; exact excerpt; bounded before/after context; original link and available timestamp; extraction/approval provenance; return to original slice | stable evidence ID, text hash/offsets, bounded context, source URL, claim/event ID, approval version | Say context unavailable when absent; never call repeated quote 'context'; no fabricated media timestamp from character offset |
| Ask `#ask/<question>` | Find or answer a research question | Question; candidate issue resolution; explicit ambiguity; cited answer only if intent handled; competing evidence; follow-up issue/voice paths | intent, canonical retrieval matches, approved answer/citations and scope | Existing deterministic matcher is issue lookup, not question answering; label accordingly until answer layer exists |
| Coverage `#coverage` and scoped variants | Understand limitations affecting this research | Snapshot freshness; scope label; applicability-aware shows/episodes/segments funnel; source roster and gaps; affected claims; duplicate-feed view; intake last | actual stage memberships, publish/build time, canonical feeds, source IDs, affected issue/voice mappings | Snapshot != live queue; scope filters must work or disclose global view; n/a before segmentation |
| Source detail `#show/<id>` (future contract) | Understand what this source contributes or lacks | Identity and aliases; episodes by period/stage; gaps; accepted claim contributions; original RSS | canonical feed/show/episode IDs and memberships | A roster card is not a detailed loss drill-down; defer route until memberships exist |

## Component contracts

1. **Research scope**: issue/voice/source IDs; publication start/end; comparison start/end; generated timestamp; data-through timestamp; coverage cohort/method. A selected month filters evidence, opposing claims and counted sample consistently. Whole-issue summaries remain explicitly labeled if not recomputed.
2. **Claim card**: claim text as paraphrase, separate exact quote, verified speaker or explicit attribution gap, speech act, date, show, original URL, evidence route. Quality reasons are understandable text; no unexplained percentage suggesting truth probability.
3. **Brief sentence**: text, kind (observation/interpretation/watchpoint), evidence IDs, entailment review, source-set hash, approved version. Aggregate claims cite an aggregate record with dates/denominator and source members, not two arbitrary excerpts.
4. **Change card**: baseline/current comparable windows; normalized delta; coverage test; change class and method; cited substantive meaning. Newly processed historical evidence is backfill, not a new event.
5. **Disagreement pair**: proposition ID; two or more claim IDs; scope/assumptions; relationship type; adjudication decision; confidence basis. Different sentiment on broad AI is insufficient.
6. **Credibility block**: relevant domain, role/experience basis, provenance, observed date, evaluated track record if available. Show network reach separately; never normalize popularity into authority.
7. **Chart**: labeled unit, period, common-vs-local scale explanation, raw and normalized values, explicit missingness, selected slice route, text/table alternative. Low coverage is not zero discourse. Keep existing restrained chart style and improve meaning.
8. **Research trail**: origin route + entity + scope + query/filter state + return scroll. Deep links work independently; Back restores previous research state. A voice's evidence returns to that voice, not a generic issue.
9. **Coverage badge**: shows/episodes/voices for precisely the represented evidence set; label excerpt display cap separately from corpus total. Open scoped diagnostics only when a membership mapping exists.
10. **Revision notice**: superseded/withdrawn IDs, reason, new version, affected briefs; inline first. No notifications sent without a configured, authorized channel.

## Canonical data mapping and ownership

| Object | Present extraction / public fields | Additional required fields | Owner / readiness |
|---|---|---|---|
| Extraction event | event_id, claim_text, speech_act, evidence_text/start/end, speaker_id, quoted_person_id, mentioned_person_ids, attribution_type/confidence, issue_label/aliases, stance, publishability_state | source/window hash binding from envelope; final approval lineage | Existing data lane; do not change frozen event schema for UI convenience |
| Canonical issue | registry stable ID and aliases; public id/name | accepted assignment version, proposition/question ID, merge/split/retirement history | Topic canonicalization + publication gate; separate from raw issue-label agreement |
| Public claim | public evidence currently omits distinct claim_text and transcript context in inspected sample | event_id, canonical issue/proposition, claim_text, attribution roles, source IDs, publication date, approval receipt reference | Versioned public projection after approval |
| Evidence | public id/evidence/date/show/episode/source_url, quality and publication fields | exact source identity + text hash, bounded context, known timestamp, role-safe voice join | Projection from approved event and source; never publish full transcript |
| Brief | what_changed, why_it_matters, implications, coverage | substantive grounded interpretation; entailment-tested citations; method/version | New approved semantic output; current templated breadth sentences are insufficient |
| Temporal aggregate | series, pulse_vol/rate, base_rate, month totals | equal periods/cohort, approved denominator, low coverage, change method | Deterministic aggregation; careful exclusion of backfill artifacts |
| Argument edge | none established by event schema | proposition/claim IDs, relation, scope and adjudication | New semantic artifact; hidden until supported |
| Person expertise | legacy authority field and network reach | domain basis, source, freshness, method; separate accuracy evidence | New sourced artifact; omit unexplained scalar |
| Forecast outcome | speech_act can indicate forecast, legacy outcome attestations | target, horizon, probability if stated, preregistered resolution criteria, actual outcome source | Deferred; no fabricated track record |
| Coverage | public stage totals and show roster | scope membership, stage-specific episode IDs/loss reasons, current snapshot age | Operational producer; keep aggregate-only public data |
| Publication manifest | schema_version/generated_at currently | corpus generation, source snapshot, approval receipt hash, build ID, compatible payload list/hashes, clean/legacy state | Publication adapter; missing approval never means approved |

## Aggregation rules

Use episode publication time for discourse, ingestion time for operations, approval time for publication, and build time for freshness. Never swap these. Deduplicate at event/evidence family before breadth counts; canonicalize resolved feeds before counting shows. Report raw and canonical source counts separately.

Attention share is accepted issue evidence divided by the defined accepted-corpus denominator in the same period. Explain whether a claim can belong to multiple issues. Cross-period change uses equal-length periods and coverage checks; a matched-source cohort is a secondary robustness view. Do not compare a historical monthly peak to a latest three-month total. Neutral-to-supportive distribution movement is a descriptive sample change, not proof that specific people changed their minds.

Authority, coverage, model confidence, semantic approval, and truth probability remain different fields. Claim-level acceptance is necessary but not sufficient for a strategic conclusion. Source breadth is useful support metadata, not the answer to 'why it matters'.

## Alerts and summaries

Default to a pull-based briefing and inline material-change/retraction notices. Materiality combines consequential content, comparable movement, source diversity and novelty; require all hard gates before ranking. Deduplicate multiple alerts from the same evidence lineage, use hysteresis for threshold changes, distinguish new/corrected/backfilled events, and suppress repetitive healthy-state chatter.

Email/push, cross-device watchlists, prediction reminders and automatic follow subscriptions remain capability-dependent proposals. This redesign does not send messages, create notification subscriptions, or start paid synthesis calls.

## Incremental delivery on existing Railway

**Presentation release:** preserve brand, routes, payload schema and hosting. Improve evidence-first hierarchy, date/scope visibility, search/results completeness, accurately named sorts, context-unavailable state, original-source access, return paths, and coverage-stage semantics. Label legacy data honestly. Do not promote the unfinished gold corpus.

**Approved-data release:** introduce a backwards-compatible public projection of approved events; explicit manifest; claim/context/attribution joins; trustworthy representative claims and aggregate-source provenance. Stable aliases bridge old IDs.

**Semantic intelligence release:** cited substantive briefs, proposition-matched disagreements, observable watchpoints, voice comparisons and genuine position changes. Only expose components for which the payload demonstrates required evidence and approval.

Railway target remains the existing `signal-desk` service and `site/` root on `codex/pif-working-system-rebuild-20260720`. The current Caddy image serves split files. Never replace the whole project with a new framework or a Sites starter. Keep the nightly builder's source assets authoritative so the next refresh does not erase UI changes. Avoid the legacy dashboard-host mutation path: it crosses into a separate shared service and is unnecessary for an unchanged canonical URL.

Before deploying: inspect linked project/service/domain; isolate this lane's files; build from known payloads; test route/escaping/state logic; verify all companion paths, HTML/JS/CSS and payload hashes. After deploy: verify actual public bytes and rendered journeys, then report presentation release and corpus approval separately. Roll back by redeploying the previous site artifact, preserving its matching payload generation.
