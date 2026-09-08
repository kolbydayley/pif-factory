# Signal Desk: comprehensive project and conversation handoff

**Prepared:** September 8, 2026. **Latest runtime check:** 2026-09-08 15:25:40 UTC.

**Owner task:** `01a04040-77ee-77f2-9d26-a5ca1ae56986`.

**Authoritative repository:** `/Users/kolbydayley/pif-factory`.

**Working branch:** `codex/pif-working-system-rebuild-20260720`.

**Historical checkout / task working directory:** `/Users/kolbydayley/Documents/Codex/podcast-intelligence-factory`.

## 1. Executive summary: what actually happened

This conversation began as ownership, deployment, and usability work on Signal Desk, a podcast-industry research interface. Kolby wanted a mobile-friendly way to discover important industry shifts, understand the arguments, compare credible people, and inspect the exact evidence behind a claim.

The project progressively exposed a more fundamental problem: the extracted corpus was not sufficiently trustworthy to support that experience. Unattributed excerpts, third-party mentions presented as personal statements, irrelevant material, misleading chart counts, weak source context, fragmented entities, and unreliable claim semantics were not merely presentation defects. Making the interface attractive would not repair them.

The scope therefore expanded into a clean-corpus rebuild. GLM was designated the extraction workhorse, GPT-5.6-sol at medium reasoning the benchmark gold author, and GPT-5.5 the independent final approval authority. A frozen 804-window benchmark, development/validation/holdout separation, non-tech transfer data, calibrated gates, a qualified scorer, and a prompt-optimization program were specified.

Initial gold authoring made substantial progress, but independent checks did not establish acceptable gold quality. The work then expanded again into repeated small development-only experiments on the labeling contract itself: attribution ownership, narrator continuity, stance, publication relevance, atomicity, evidence/context separation, adjudication lineage, questions, and source-recovery needs.

**The blunt current conclusion:** we have not moved successfully beyond gold qualification. We have built and investigated a substantial amount of the product and pipeline, but the full gold benchmark is still not accepted. The recent activity is mostly diagnosis and contract experimentation on 16 development windows—not completion of the 804-window dataset, not a qualified GLM tournament, and not a production clean-corpus release.

Kolby's frustration that this has become an excessively long process is justified. The process has produced real findings, but repeated reports of a worker being active were not an adequate account of actual accepted-data progress. Too much operational motion was allowed to substitute for a bounded plan to reach a decision.

### Status in one table

| Workstream | What exists | What must not be claimed |
|---|---|---|
| Signal Desk product | Public interface, research navigation, split payloads, coverage surface, documented presentation release | That its underlying corpus has passed the rebuild's reliability gates |
| Benchmark acquisition | Recorded freeze of 57 current + 10 OOD shows, 804 windows | That acquired or frozen text is automatically clean or correctly attributed |
| Initial gold | Substantial authored output and several independent audit/review stages | That all 804 are accepted gold |
| Gold reliability | A documented failed development audit and subsequent diagnostic investigations | That lower disagreement in a tuned example proves the full gate passes |
| Contract experiments | Multiple versioned prototypes and 16-window comparisons | That unit tests or valid JSON establish semantic quality |
| Latest comparison | 14 returned role outputs, 4 structurally valid, 10 held, 1 complete four-role window out of 16 | That the one complete negative-control window establishes useful extraction recall |
| Runtime | Last observed worker exited with code 2; exact worker and wrapper absent at 15:25:40 UTC | That it is still running because a receipt says dispatch was accepted |
| A1 / A2 / tournament | Implementation and historical work exist; acceptance prerequisites remain unresolved | That a prerequisite-valid final calibration, scorer qualification, or tournament is complete |
| Production rebuild | Not established as complete | That presentation deployment equals new-corpus deployment |

## 2. Scope, provenance, and how to read this document

This is a comprehensive operational synthesis, not a verbatim transcript. It combines the supplied conversation history, retained continuation context, committed repository documents, the current git log, source-code spot checks, and fresh runtime observations.

Historical user instructions are distinguished from verified implementation. A requirement listed below does not imply that its implementation has passed acceptance. Some older work was performed or audited by Claude or another Codex task; this document does not attribute every commit to this task.

Evidence categories used here:

- **Current local verification:** checked directly while preparing this document.
- **Recorded result:** supported by an existing project report or prior retained receipt; not rerun here.
- **Requested contract:** a user-approved requirement whose complete acceptance is not asserted here.
- **Diagnosis:** a source-based interpretation, not a corpus-wide measured prevalence.
- **Proposal:** a recommendation that has not automatically become an authorized new architecture or paid campaign.

No validation/holdout item-level answers were opened to prepare this handoff. No raw transcript corpus, API keys, or other credentials are copied into it. Counts from different experiment families are not interchangeable and must not be combined into one apparent accuracy score.

## 3. Original product goal and requirements

Kolby's intended outcome was not a podcast catalog or a detector dashboard. It was useful strategic intelligence:

1. Discover important changes in an industry.
2. Understand the major issues and why they matter.
3. See how credible people's opinions differ.
4. Read representative direct quotations and inspect their original sources.
5. Move naturally from a two-minute mobile scan to a deeper research session.

The desired research path was progressively deeper: overview → issue → claim or argument → person → excerpt → transcript context → original source. People were to have equally rich pages, leading with consequential current claims rather than network statistics. Public access was acceptable. Kolby was not attached to the original design.

### Early implementation and operational requests

The conversation included requests to:

- Read `docs/signal-desk/README.md` and own Signal Desk.
- Find Claude's V2/V3 updates and reconcile what should reach production.
- Publish where the podcast-factory data was already hosted and provide the URL.
- Improve mobile layouts, chart drill-downs, navigation, and evidence exploration.
- Fix broken Shifts and unavailable detail routes.
- Run the dashboard publisher and extend the refresh workflow to publish after building.
- Report processed shows/episodes and add a Podcast Funnel with enrollment visibility.
- Make every funnel stage report shows, episodes, and applicable segments, including zero segments before segmentation.
- Fetch server-derived funnel data dynamically rather than hardcode counts.
- Investigate and improve topic canonicalization to avoid subtle duplicates.
- Preserve Claude's simultaneous repository edits.

These requests led into the larger redesign and then the corpus-quality investigation. Exact present-day funnel totals were not re-queried for this handoff; an old count would be misleading.

## 4. Product audit and redesign

### Findings that changed the direction

The multi-section audit described several decision-safety failures:

- Detector output was presented as intelligence without sufficient supporting evidence.
- Topic pages emphasized counts/co-occurrence instead of arguments and disagreement.
- Third-party mentions sometimes appeared as statements made by the named person.
- Excerpts could contain webpage navigation, episode listings, or other non-transcript material.
- Some apparently strong signals had inadequate underlying evidence.
- Duplicate identities and resolved RSS feeds inflated apparent breadth.
- Generic broken-link states lost research context.
- Mobile navigation and desktop-shaped charts obscured useful content.
- Operational throughput did not explain losses, source bias, or findings affected by missing coverage.

Kolby specifically flagged the US–China AI competition page and AI employment page. The latter's many “unattributed voice” excerpts became an important trigger for the full data audit.

### Strategic redesign contract

Primary navigation became **Briefing, Issues, Voices, Ask**. Podcast Funnel moved into secondary **Coverage & Trust**.

The intended public intelligence model includes accepted evidence, cited briefs, arguments, implications, watchpoints, confidence and coverage warnings, credible voices, and typed relationships. Co-occurrence is not proof of causation, disagreement, influence, or conceptual dependency.

A decision-worthy signal requires at least three publishable excerpts from three episodes, two shows, and two distinct speakers. Weaker material belongs in Watchlist with its limitations. “Network reach” must not be mislabeled as trust or domain expertise.

The intended evidence gate separates speaker, quoted person, mentioned person, organization, and show. Candidate/uncertain/quarantined material must not silently become public intelligence. Factual synthesis requires working citations to accepted evidence and original material.

### What the repository now documents

The README identifies these principal components:

| Component | Repository path |
|---|---|
| Canonical issue identities and aliases | `research_factory/topic_canonicalizer.py` |
| Corpus aggregation and episode context attribution | `research_factory/pif_discourse_aggregates.py` |
| Publishability, briefs, identities, evidence quality | `research_factory/signal_desk_intelligence.py` |
| HTML/application build | `scripts/pif_dashboard_build.py` |
| Frontend views | `scripts/signal_desk_assets/` |
| Publisher | `scripts/pif_dashboard_publish.py` |
| Railway static image | `site/Dockerfile` |

Documented split payloads include the index, issues, voices, network, and funnel. Coverage is documented as cache-disabled and refreshed every 60 seconds. This is an architectural description, not a fresh end-to-end dynamic-data smoke test in this turn.

Legacy hash routes remain compatibility routes. The public address recorded throughout this work is:

[Signal Desk production](https://signal-desk-production-edf4.up.railway.app/pif-signal-desk.html)

### Presentation release versus data acceptance

The September 4 presentation release report is especially important because it explicitly limits its claims:

- Preserved the existing visual identity while making research pages evidence-first.
- Exposed legacy data, attribution gaps, missing context, and historical evidence.
- Removed unsupported authority/decision-grade language and generic implications filler.
- Added accessible month selection and a monthly table.
- Preserved evidence-return origin, selected month, filters, and scroll.
- Prevented stale asynchronous navigation responses from replacing newer pages.
- Kept source-intake drafts intact; intake was honestly described as a draft/copy action, not completed enrollment.
- Kept Ask as issue lookup rather than pretending approved synthesis existed.
- Replaced fake coverage drill-downs with honest summaries.

Recorded checks: nine Node frontend tests covered 28 issue views, six voice views, and 540 evidence views; twelve Python builder/publisher tests passed. These were not 574 separate browser sessions. Representative browser flows and 390px/430px layouts were checked. Universal screen-reader/accessibility compliance was not asserted.

Recorded Railway release: deployment `1751f45f-79cb-4819-b5f3-ba1e1f8daf99`, commits `c6ff4a0` and `a59b19c`; the release report records successful HTTP and rendered-flow verification. Public JSON was preserved byte-for-byte.

**That release did not replace or approve the corpus.** It also belongs to a recorded separate presentation task; this is project continuity, not a claim that this gold worker deployed it. Production was not re-smoke-tested while writing this document.

## 5. Why chart and canonicalization work mattered

One cited data-layer change, commit `cb15268`, addressed raw weekly topic counts that looked artificially spiky. The reported labeled coverage varied from 81 to 923 mentions/week. Evergreen topics inherited that corpus-coverage sawtooth, and niche topics with 0–4 mentions/week were below a reliable sampling floor.

The user-supplied report described adding attention share, smoothing, and low-sample flags. This was a correction to the denominator, not evidence that the underlying statements were reliable. Later product documentation describes monthly smoothed share. These are different temporal representations; future comparisons must inspect the active schema rather than assume weekly and monthly fields are interchangeable.

Canonicalization likewise has several distinct dimensions:

- Topic aliases and stable issue IDs.
- Person identity and ASR surface variants.
- Show identity and resolved RSS feed duplicates.
- Semantic duplicate claims inside and across extraction outputs.

Solving one does not solve the others. A canonical person ID is not evidence that the person actually uttered a passage. Two claims sharing a span may be distinct propositions—or redundant splitting. Two topics appearing in an episode are not automatically related in a decision-relevant way.

## 6. Clean-corpus rebuild architecture and benchmark

### Model responsibilities

- **GLM:** intended high-throughput workhorse, using parallel API calls and measured prompt optimization.
- **GPT-5.6-sol, medium reasoning:** gold authoring and calibration reference.
- **GPT-5.5:** final independent approval, fail closed on unresolved material.

Gold A and B are independent authoring passes. Gold C adjudicates A/B. The blind AUDIT role independently extracts from source without seeing the author answers. Final review must inspect the source and the actual proposed records/dispositions—not merely trust the earlier models' agreement.

The fact that the final reviewer is GPT-5.5 does not make every correction true. Several source inspections found unsupported or inconsistent reviewer proposals. Those must be challenged explicitly, not automatically applied or resolved by repeated voting until approval appears.

### Frozen benchmark

The recorded benchmark freeze is **804 windows**:

- 57 current shows × 12 windows = 684.
- 10 non-tech/OOD shows × 12 windows = 120.
- Four episode-disjoint episodes per show, with pre-label-property sampling.

Recorded split work inventories include 189 development, 378 validation, and 189 holdout windows, plus the source-disjoint sealed OOD allocation; use the frozen manifest for exact membership rather than reconstructing membership from arithmetic. The full benchmark and sealed-source boundaries must not be shrunk to improve apparent progress.

Development data may inform tuning. Validation and holdout item-level answers must remain sealed from prompt authors. All splits need gold, but their answers and audit details have different access rules. A convenient small dev diagnostic is not a replacement benchmark or an untouched test set.

### Acquisition and representation corrections

Early qualification wrongly treated flattened transcripts as disqualifying. Kolby corrected this: production uses captions and flattened text, so the benchmark must represent them. Select the same best-available transcript production would select, and record structure as a stratum.

Relevant strata include speaker-turn, paragraph, flattened, and ASR variants. Metrics must be reported by stratum. Missing speaker information in genuinely unstructured text is not permission to guess. On flattened text, unsupported attribution is an error; abstention can be correct.

Acquisition constraints included:

- Browser extraction for client-rendered NPR transcript shells and bot-walled sources.
- Rejecting navigation-only shells with a duration-relative plausibility guard.
- Episode-side title-token containment of at least 0.85 rather than Jaccard for archive matching.
- Unwrapping podcast feed tracking redirects.
- Preferring official archives over recent-only transcript feed tags.
- OOD fixtures physically isolated from canonical public aggregation.
- No sealed-show transcript previews in prompt-author-readable logs.
- Re-freezing stale text hashes before gold authoring touches changed bytes.

The blocked-show paths included How I Built This, Marketplace Tech, Search Engine, Tech Brew Ride Home, and The Ben and Marc Show. Benchmark acquisition was bounded to a few episodes, not full catalogs.

Groq was considered but its upgrade path was unavailable. xAI STT became the recorded benchmark ASR lane. Exact production-reusable ASR settings belong in `config/signal_desk_rebuild_xai_asr_contract.json`, with sanity checks in the companion receipt. Do not invent the endpoint/model/settings from this summary or silently use a new contract later.

The reported 16 benchmark ASR episodes and four ASR shows were to carry a distinct stratum. WPM, duration gaps, and host/guest proper-name checks were required before authoring. A later finding showed that `asr_diarized` acquisition metadata did not necessarily mean the model-visible text contained speaker turns.

The full approximately 3,000-episode ASR remainder was explicitly a separate spending decision. The user supplied a planning estimate of roughly $200 at measured xAI pricing versus an earlier unproven Groq estimate around $45. These are historical planning figures, not current quotes or authorization to run the full remainder.

Credentials were pasted into the conversation. They are intentionally omitted here. Their exposure is a credential-hygiene concern; this document does not assert that they were rotated or revoked.

## 7. Gate amendments and why they were necessary

### A1: measure the reference ceiling before setting gates

The earlier aspirational coverage gate was reportedly 0.95 while the reference model itself achieved about 0.855 on the same harness. A gate that exceeds the measured achievable baseline can lock the campaign indefinitely.

Required correction: single-pass SOL over development with the same 6,000-character contract; measure per-gate ceilings, derive relative thresholds, then freeze absolute gates. The later amendment requires bootstrap lower bounds rather than point estimates. This is still subject to accepted gold and a qualified scorer.

### A2: qualify the scorer, not just the extraction prompt

The deterministic matcher is a critical artifact. It needs an explicit spec, hash/version, registry entry, tests, and 100 stratified model-adjudicated match/no-match decisions with at least 97% agreement. Include split/merge cases and ASR surface aliases. A scorer change invalidates affected cross-round comparisons and requires reaggregation.

The user requested v5 in the deep-audit amendment. **Current source inspection during this handoff finds `SCORER_VERSION = signal-desk-rebuild-scorer-v6`.** Do not blindly resume a v5 qualification command or report v5 as current. Determine which version each receipt actually binds to before interpreting scores. The existence of v6 is not proof it passed A2.

### Statistical power and direction of confidence bounds

- Per-show recall gates require a numerical power floor, recommended at least 50 gold consequential events.
- Per-show gates belong on validation, not tiny holdout slices.
- Aggregate holdout questions include show-macro composite, contamination, and OOD behavior.
- Success-rate gates use lower confidence bounds; harmful-error-rate gates use upper bounds.
- Claim-dense OOD shows can support recall gates. Narrative shows with few consequential claims require precision/contamination treatment rather than unstable recall denominators.
- Gold audit error denominators are events, not casually interchangeable window counts.
- Correct-empty windows must count correctly rather than appear as missing recall or pathological density.

### D1: distinguish catastrophic failures from critical disagreement

The original audit path reportedly counted any window with a critical error as catastrophic, while catastrophe had zero tolerance. With many events per window this made the gate structurally misleading.

Required and now visibly represented in the audited code path: separate source fabrication/wrong-window/reversed-meaning/unusable-context failures from ordinary unpaired-event or attribution/stance disagreements; apply a one-sided event error bound; preserve the 19-window dev seed and expand in 40-window blocks to reach at least 1,000 consequential events; flag four or more same-span claims for re-adjudication.

The code spot check shows rate-gate calls, catastrophe reasons, expansion logic, and same-span review. This document does not assert every branch was rerun or all audit tests passed today.

### D2 / D3: do not inflate field accuracy or hide weak shows

Match on claim identity first—evidence overlap and claim text—then score attribution, role, issue, and stance on all matched pairs. Requiring field agreement before matching falsely inflates those accuracies.

Atomicity is not F1 under a new name: measure one-to-one counterparts, shared-span multiplicity, and density. Show-macro scores must be unweighted averages over shows, with micro reported separately. Selection uses the macro lower confidence bound, and promotion requires improvement over the parent on at least 80% of sufficiently powered shows. Persist per-show rows.

### Other required safeguards

- GPT-5.5 may request wider context, retrying with ±3 turns/full segment before failing closed; this is not a new independent semantic sample.
- Legacy labels remain candidate history and a disagreement oracle for shadow audits, not unquestioned truth.
- A representation experiment family should run alongside prompt rounds 3–9 rather than only after many failures.
- Terminal failed tasks must be explicitly resurrectable with lineage; expired leases and semantic failures are different states.
- “100% processed through approval” does not mean “100% approved”; extreme approval rates need investigation.
- Approval packets are bounded to 25 candidates and 12,000 input tokens.
- A real multi-day burn-rate probe is required before relying on the approval budget.
- Named regression fixtures must cover source context, speaker attribution, chrome, and conceptual distinctions through built-site release checks.

Not every requirement above has a verified passing receipt in this handoff. They remain an acceptance checklist, not a blanket completion statement.

## 8. Budget, capacity, concurrency, and stop/resume history

### Budget authorization evolved

The approval lane received a scoped GPT-5.5 20M/day rebuild grant, with explicit expiry and first-clean-release termination. Gold initially had a normal 5M/day policy, then a 15M/day grant, then a burst amendment removing its daily ceiling and making the provider weekly window binding.

The gold burst grant was scoped to gold authoring, with completion/30-day expiry, mandatory per-call reservations, weekly-headroom monitoring, an 85% notification, checkpointing on actual exhaustion, and automatic resumption when calls become available. It did not authorize unrelated models or purposes, indefinite spending, or silently consuming a reset.

Grant files and live governor state must be checked before future paid calls. This handoff does not extend any grant or prove current entitlement from an old authorization.

### Capacity incident: distinguish causes

Kolby repeatedly encountered “Selected model is at capacity. Please try a different model.” Claude's supplied audit attributed the September 2 incident to the SOL provider model pool, not weekly account quota or local process concurrency alone. Its evidence included failures with two in-flight calls, successes with more, successes near failures, concentration in one evening period, and weekly quota still around 27–33%.

That is a recorded incident diagnosis, not a guarantee that concurrency never matters. The correct response is observable per-lane capacity state, actual backend messages, admission control, backoff, and explicit priority—not promising the provider will never saturate again.

### Incorrect latency guard

Gold medium-reasoning calls had reported A/B p95 near 476–478 seconds and C p95 around 368 seconds. A 90-second ceiling inherited from fast GLM extraction therefore labeled healthy gold calls degraded.

The user directed a lane-specific gold degradation threshold around 900 seconds, a 900-second deadline, an 1,800-second lease, and unchanged GLM limits. Other guards: 429 >2%, timeout >5%, parse/schema >2%, no success for 15 minutes, rolling 100-call/10-minute windows, reduce by two on trip, increase by one after three healthy windows and cooldown.

Other throughput directives included pipelining B/C per window rather than per-split barriers, compacting C's input, durable in-place concurrency changes, and reporting throughput/in-flight state. Some were implementation requirements rather than independently reverified facts in this handoff.

### Stop markers and recovery hazards

The conversation included explicit operator stops, later authorized clearances, and a stop/relaunch race that left work idle. Operational lessons:

- Preserve completed output and queue lineage.
- A concurrency adjustment should not require engaging a kill.
- An old stop must not accidentally invalidate an explicitly authorized later run.
- Never broadly ignore stop files merely to restore throughput.
- Failed terminal rows must not silently deduplicate every future enqueue into permanent failure.
- Never restart Codex/ChatGPT hosts without explicit permission.

## 9. Hooks and monitoring: what they prove and what they do not

Kolby repeatedly had to ask whether a process was still running or finished. A claimed completion hook initially did not resume the task, and Kolby correctly challenged the claim.

The system subsequently used a standalone process-exit wrapper that writes a receipt and sends a continuation message through the running app's local IPC. A separate acknowledgement is required. Important distinctions:

1. Child exit is not task continuation.
2. IPC dispatch acceptance is not proof of task acknowledgement.
3. Acknowledgement during an already-active turn is not proof that an idle task woke autonomously.
4. A heartbeat recovery must be called heartbeat recovery, not retroactively attributed to the hook.
5. A worker must be verified absent before any replacement launch.

Many recent hook acknowledgements explicitly recorded `hook_received_during_active_goal_turn` and `autonomous_wake_verified: false`. That honesty must be preserved.

Scheduling policy changed over time. Earlier Kolby explicitly prohibited scheduled tasks in favor of hooks. On September 7 he reauthorized one backstop:

- `signal-desk-completion-monitor`: sole authorized active backstop, documented as a quiet 15-minute native heartbeat.
- `signal-desk-gold-rebuild-supervisor`: older supervisor remains paused.

The active task goal, exit wrapper, and one backstop are intended to provide continuity. They cannot guarantee uninterrupted progress while the Mac sleeps, the host is unavailable, the provider exhausts capacity, a quality gate fails, or authority expires.

The latest documented monitor state was checked earlier in the ongoing work; it was not reconfigured by this documentation task. Do not launch another canary, reset a host, or add another monitor simply because there are many exit-code-2 events.

## 10. Gold quality: actual recorded results

### Initial strong-looking signals were insufficient

A supplied independent audit of 134 authored dev C windows reported 4,153 exactly grounded events and 252/252 flattened direct-speech events with names in text. Those findings were encouraging, but exact grounding and name presence did not establish complete recall, correct ownership, meaningful stance, strategic usefulness, or gold reliability.

Historical status later described 803 authored windows plus one quarantine. This is a recorded authoring status, not a freshly reconciled current accepted total. “Authored,” “quarantined,” and “accepted” must remain separate.

### Failed development reliability gate

The retained development checkpoint records:

| Measure | Recorded result |
|---|---:|
| Audited windows | 59: 19 seed + 40 expansion |
| Consequential-event denominator | 1,909 |
| Critical errors | 151 |
| Critical point estimate | 7.9099% |
| One-sided Wilson error upper bound | Approximately 8.9865% |
| Required critical bound | Below 1% |
| Event agreement | Approximately 92.09% |
| Agreement lower bound | Approximately 91.01% |
| Required agreement | At least 95%, under the specified gate |
| Confirmed catastrophes at that checkpoint | 0 |

This gate failed materially. Zero confirmed catastrophes did not rescue ordinary critical quality. These figures are the recorded checkpoint, not recalculated from sealed data during this turn.

The retained disagreement ledger also described 860 differences: 675 gold-supported, 123 audit-supported, 34 both, 28 neither. Differences are not automatically errors; adjudication and denominator definitions matter.

### Repair and re-audit did not resolve the underlying contract

The September 7 report describes a repaired dev candidate with 189 windows and 6,156 events, preserving the 1,909-event audit denominator. Its independent re-audit covered 64 windows and generated 831 disagreements.

A purposive 40-case diagnostic over seven disagreement-history cohorts returned 24 gold-supported, 11 audit-supported, two both, and three neither. This was not a random prevalence estimate.

The key discovery was contradictory instructions: one author addendum treated explicit assertions as supportive stance, while repair/review instructions distinguished assertion from endorsement. Other inconsistencies involved inferred speakers, ASR aliases, incidental facts treated as consequential, and reporting versus personal speech.

This is why merely running another author/auditor round could not safely fix the dataset.

## 11. Development experiment history

### Shared-rubric qualification

A shared hashed rubric was introduced for author, auditor, and approver roles, isolated to a 16-window development sample. Recorded output:

- 48 A/B/C outputs and 16 independent audit outputs.
- 432 audit events exact-source/schema-valid after preserving one failed-offset attempt and a bounded retry.
- 477 C events.
- 27 GPT-5.5 review packets; 19 first-pass responses valid, eight required explicit format-contract retry.
- Combined proposals: 434 supported, 43 needing correction.

These were proposals, not acceptance. A dry check found 11 schema conflicts among the 43 patches: seven lacked the required actual speaker and four lacked a quoted-person ID. No automatic patch application was justified.

### Attribution representation

The old schema overloaded a speaker field with both actual transcript voice and owner of a quotation/report. This caused two opposite errors:

- Guessing a narrator to preserve an explicit quote owner.
- Dropping an explicit quote owner because the narrator could not be named.

The new experimental distinction keeps transcript voice, proposition owner, and mentioned entities separate. Main-proposition ownership matters: a speaker describing their own decision after hearing others' doubts is not merely reporting the doubters' view.

ASR acquisition metadata also proved insufficient: a diarized acquisition could supply a one-line flattened window to the model. A closing host signoff does not establish every preceding utterance's boundary, especially around embedded quotes and multi-speaker material.

### Full-event v3

Recorded 16-window result: all 64 SOL A/B/C/AUDIT calls complete; 16 GPT-5.5 review packets; 198 C candidates, 178 supported, 20 corrected, plus a supported empty window. Three original offset failures remained in provenance.

Source inspection exposed continuing defects:

- Community invitations and weekly show schedules remained candidates despite being promotional/operational.
- A neutral correction discarded an explicit negative attitude.
- Some “corrections” were harmless paraphrase edits, not critical semantic errors.
- A luxury-business correction introduced an antecedent not clearly established by the inspected passage.
- A reduction from 477 to 198 candidates was not a recall score.

### Source reconciliation and independent-review challenges

A later diagnostic covered 1,027 overlapping candidate/correction cases in 53 packets across the 16 development windows. These are overlapping diagnostic cases, not 1,027 independent benchmark events. All packets validated after explicit recovery; failed originals were retained.

Even independent contract review made factual mistakes: two reviews alleged missing schema-field nesting that direct inspection disproved. This prevented an unnecessary schema “fix.” Agreement between models is not a substitute for checking actual source/code.

### Voice continuity and separate context evidence

One prompt invited a turn-boundary label as continuity evidence while its validator required evidence inside the current voice corridor. Authors and reviewer repeated the same incompatible recommendation.

A short interview assent created another conflict: including the interviewer's question in the attributed excerpt made it multi-speaker; removing it lost the answer's meaning. The experimental v5 contract separates `context_evidence` from spoken `evidence_text`. Context must not inherit speaker ownership, create additional votes, or inflate source counts.

### Adjudication lineage

Marketplace C retained all 11 A and nine B records as 20 records, including equivalent propositions reclassified as context. Exact spans and low same-span multiplicity did not establish uniqueness.

The diagnosed instruction collision was between merging redundant claims and preserving every supplied candidate ID. The proposed/implemented experimental envelope separates a canonical event set from dispositions for every `(author, input_event_id)`. Several inputs can map to one output; splits, merges, rejection, correction, and unresolved states remain explicit.

Six GPT-5.5 calls reviewed eleven source-based contrasts and supported trying that envelope. That bounded result did not qualify all 16 windows or the full 804. A subtle source-support-versus-publishability disagreement remained unresolved.

### Original lineage/full-event diagnostic before questions

The question qualification document records an earlier diagnostic inventory of eight complete windows and eight held, 42 returned outputs containing 515 records, eight first-pass structurally valid outputs, 34 first-pass failures, 26 explicit repairs, eight still held, and 22 unfulfilled slots out of 64.

The repaired outputs must not hide the poor first-pass rate. None of these counts establishes an accepted corpus.

### Question-v1

Some inquiries were mislabeled as assertions or forecasts because the speech-act representation lacked a question value. An isolated question family added questions as context with their own speaker, questioned proposition status, and exact evidence.

Its first-pass observation finished with:

- All 16 A outputs returned.
- 15 held, one valid negative-control window.
- 19 total A/B/C/AUDIT returns, four valid roles, 15 held, 45 unfulfilled of 64.
- First-error families: six recovery-need conflicts, six exact-span failures, two question-role conflicts, one status conflict.

This was a failed qualification. The frozen validator itself was too restrictive for housekeeping questions and clipped questions legitimately represented as research limitations.

A separate question-boundary v2 module allows those non-substantive roles. It remains isolated and undispatched in the recorded state. It is not included in the current prompt comparison.

### Current recovery-rule comparison

Family: `question-v1-recovery-rule-clarification-v1`.

The only intended change is an appended clarification separating unknown speaker identity from missing/unreadable proposition content. It preserves the same 16 sources, 64 role slots, SOL medium, concurrency two, schemas, source bytes, and validators. It does not include the separate question-boundary amendment.

Existing held raw responses are preserved without retry; later resumes only admit previously unattempted independent work. C requires verified A/B parents. One failed A therefore blocks dependent roles rather than disappearing from the denominator.

## 12. Current comparison: concrete source findings

These are development-source observations, not externally verified world facts or representative corpus error rates.

| Source/window | Returned A records | Main observed issue or finding |
|---|---:|---|
| Marketplace DEI, `sdw_777d46db3fa4c592b71e` | 14 | Held for exact span; one excerpt reused in three mismatching fields. Additional status and attitude-target concerns remain. |
| JSParty, `sdw_c38394ac01937fde7f33` | 13 | Housekeeping question conflicts with frozen role rule. Source-specific show commitments are not automatically strategic intelligence. |
| Hidden Brain, `sdw_99a1771e94fa2b923f9e` | 11 | Held for span. Preserve association versus causation, estimates, anecdote ownership, and host interpretation versus guest response. |
| Coding/legal AI, `sdw_4117ea30ae4ec3f116ef` | 27 | Held for span. “Quadrupled from” approximately 30M overstates an ambiguous baseline; preserve withdrawn thesis and damaged names. |
| DRA, `sdw_8bc85d983495f3780cb2` | 12 | Readable opening claims now retain unknown voice without unnecessary source recovery; still held for span. |
| TSMC, `sdw_4acaaec65ea71658126b` | 19 | Same targeted unknown-voice improvement; still held for span. Historical offers, forecasts, sponsors, and narrator interpretation need separation. |
| AI governance, `sdw_3e13b01692fa8508d0cd` | 20 | Held for exact source span. Distinguish actual expressed forecasts from imagined opposition and preserve clipped context. |
| Stoica, `sdw_ff2331e598d948e07cc9` | 12 | Third targeted recovery-rule improvement; still held for evidence offsets. Compound claims and interviewer recap remain risks. |
| EconTalk chrome, `sdw_fe1a756b868166848a44` | A: 0 | A empty; B/C retain reading-list metadata. Four structurally valid roles, but metadata-retention agreement still needs independent semantic review. |

The EconTalk source is webpage material, not substantive transcript. Its success shows a useful negative-control behavior, not that the extraction system captures substantive claims accurately.

Two more held role returns appeared before the final runtime snapshot, increasing held outputs from eight to ten. Their item-level contents were not inspected during this documentation turn, so no source-specific verdict is invented here. Use `--calls` and the fresh artifacts to identify them before the next continuation.

### Latest measured state, 15:25:40 UTC

| State | Count |
|---|---:|
| Planned windows | 16 |
| Planned role outputs | 64 |
| Raw returns | 14 |
| Structurally valid / authored-not-accepted roles | 4 |
| Held roles | 10 |
| Not started | 35 |
| Waiting for verified parents | 15 |
| Complete four-role windows | 1 |
| Qualified | false |
| Gold accepted | false |

The role-state counts reconcile to 64. A complete window is not necessarily approved; “structurally valid” and “semantic approval” are different gates.

## 13. Runtime at this handoff

The most recent worker was:

- Wrapper PID: `61810`.
- Child PID: `61818`.
- Command: `scripts/pif_signal_desk_question_recovery_qualification.py --execute`.
- Hook ID: `6b408522-3d38-468a-8c38-196e53c4c19a`.
- Started: `2026-09-08T14:59:39.215427+00:00`.
- Exited: `2026-09-08T15:09:51.618434+00:00`.
- Child exit code: `2`.

At 15:25:40 UTC, `ps` found neither exact PID. The receipt says `dispatch_accepted`, `start_turn_sent: true`, `response_type: success`, and `autonomous_wake_verified: false`.

**Therefore the last observed worker is finished, not running.** Dispatch success does not establish an autonomous wake, and exit code 2 does not by itself distinguish provider failure from a deliberate quality hold. Inspect the run outputs before deciding the next action.

This document-writing turn did not relaunch a paid worker, retry held outputs, change a prompt, bypass a gate, alter grants, reset a host, or publish a new corpus. It also did not manufacture a hook acknowledgement claiming autonomous wake. The new documentation request is the active task for this turn.

The continuation handoff's top 15:00 UTC entry still described this worker as active when read. That entry is now stale. This section supersedes it for the timestamp above; future readers must still check fresh process and receipt state.

## 14. Main unresolved problems, ranked

### 1. Gold reliability has not passed

This is the blocking product fact. No amount of operational continuity or test coverage can substitute for an accepted reference dataset. The independent development error rate was materially above the requested bound.

### 2. The labeling contract is too complex and internally fragile

The schema accumulated narrator identity, quote ownership, attribution type, stance, stance target, actuality, epistemic status, speech act, publication role, exact evidence, context evidence, continuity corridors, recovery needs, and full adjudication lineage. Several additions repair real problems, but together they create many mechanical and cross-field failure modes.

### 3. Exact offsets are a persistent first-pass bottleneck

Models often produce substantively plausible evidence text with inexact character positions. One mismatch can hold a complete role output and prevent downstream adjudication. Many recent failures are mechanical, but some records also contain real semantic errors. Do not equate “offset bug” with “otherwise correct.”

### 4. Speaker recovery is not merely filling nulls

Unknown voices can arise from missing source turns, lost diarization, clipped windows, overstrict continuity rules, or extraction mistakes. Those require different remedies. A named person anywhere in text is not an utterer map; a quoted owner may be known while the narrator is not. Guessing speakers would satisfy a superficial completeness metric while poisoning person pages.

### 5. Assertion, stance, and strategic relevance remain distinct

An assertion is not necessarily endorsement. Negative wording is not necessarily a negative attitude toward the same target. Source-supported claims can still be housekeeping or irrelevant biography. A report about a company is not the company's expressed position. These distinctions are essential to usable issue maps and disagreements.

### 6. Reviewers are imperfect and sometimes share the author's defect

Review corrections must remain proposals until checked. Same-model/rubric agreement can reinforce an instruction error. Gold and auditor can both be wrong, both reasonable, or disagree over a harmless paraphrase rather than a consequential error.

### 7. Atomicity and omission cannot be inferred from counts

Fewer records may mean cleaner consolidation or missing claims. More records may mean greater coverage or redundant splitting. Exact same-span count is only a flag. Full-source alignment and merge/split lineage are necessary.

### 8. Experiment governance has become too open-ended

The campaign preserved versions and failed outputs, but repeatedly pursued the next small defect without an adequate stopping rule. The 16 development windows are now heavily inspected and used for tuning. They are useful regressions, not evidence of generalization. A bounded decision is overdue before another large campaign.

### 9. Operational reporting emphasized activity over outcome

“Still running,” hook IDs, and individual return counts did not answer Kolby's real question: how much closer are we to accepted useful data? Future status must lead with accepted scope, gate results, remaining blockers, and whether the current run is a diagnostic or completion work.

## 15. What should happen next

### Immediate, safe continuation

1. Read the latest hook receipt and current run results; verify no duplicate worker exists.
2. Handle acknowledgement honestly, distinguishing the actual hook event from this documentation turn and from any heartbeat recovery.
3. Inspect the two newly held outputs and complete the current comparison's failure inventory.
4. Preserve all source/prompt/output hashes and failed raw responses.
5. Keep the frozen 804 scope and sealed data untouched.
6. Do not interpret the lone completed chrome control as positive extraction qualification.

### Explicit scope correction

In the most recent discussion, the assistant acknowledged that the effort had drifted into rebuilding labeling rather than finishing gold. It recommended finishing the bounded comparison and then making an explicit architecture decision, not launching another indefinite series of tiny repairs.

One proposed simplification is to let models select exact verbatim evidence while deterministic code locates that text and rejects ambiguity. Models would focus on meaning, ownership, and claims rather than manually reproducing many offsets. This proposal requires careful qualification:

- Preserve raw model output and transformation provenance.
- Reject ambiguous repeated matches; do not silently select the first occurrence.
- Preserve context/voice boundaries and source revisions.
- Test repeated fragments, Unicode, whitespace, ASR text, and multi-speaker context.
- Do not silently fuzzy-match a fabricated quote into something plausible.
- Independently compare semantic and mechanical error rates against the frozen baseline.

**This was a recommendation, not an already approved replacement architecture or a completed fix.** Do not infer permission for a broad new paid campaign from the request to write this document.

### Completion plan after a contract is qualified

1. Freeze the selected schema, prompts, source representation, scorer, and audit rubric.
2. Specify exactly which existing gold outputs can be reused by verified provenance and which require reauthoring.
3. Repair/reauthor a bounded development population and run an independent reliability audit with the unchanged denominators and confidence rules.
4. If it fails, report the dominant error families and use a predeclared iteration/time/cost budget; do not continue indefinitely.
5. If development passes, author/migrate validation and holdout through sealed tooling and complete their independent audit slices.
6. Run A1 against accepted dev gold, derive frozen gates from the measured reference bounds.
7. Complete A2 against the actual scorer version, including difficult split/merge and alias cases.
8. Start the GLM tournament only when the prerequisites are genuinely satisfied. Keep prompt and representation experiment families separate.
9. Complete shadow runs, disagreement-focused audits, GPT-5.5 approval, regression fixtures, and coverage reconciliation.
10. Rebuild/publish production only after accepted-data and rendered-site checks; then independently verify Railway payloads, routes, and public DOM.

There is no defensible total completion ETA until the contract/quality blocker is resolved. Provider calls per hour alone cannot estimate acceptance time.

## 16. Artifact and code map

All relative paths in this section are relative to `/Users/kolbydayley/pif-factory`.

### Product and audit documents

- `docs/signal-desk/README.md` — architecture and public contract; reconcile older aspirational wording with later release limitations.
- `docs/signal-desk/quality-audit-2026-08-26.md`.
- `docs/signal-desk/signal-noise-audit-2026-08-31.md`.
- `docs/signal-desk/redesign-2026-09-04/` — goals/rubric, option catalog, scoring, architecture, live audit, release verification, production receipt, screenshots.

### Gold diagnostic and control documents

- `docs/signal-desk/gold-completion-control.md` — goal/hooks/one-backstop policy.
- `docs/signal-desk/gold-reaudit-contract-findings-2026-09-07.md` — failed re-audit, shared-rubric contradictions, attribution constraints.
- `docs/signal-desk/full-event-v3-review-findings-2026-09-08.md` — 20 correction checks and reviewer challenges.
- `docs/signal-desk/source-reconciliation-2026-09-08.md` — cross-contract/source reconciliation.
- `docs/signal-desk/adjudication-lineage-2026-09-08.md` — merge/identity preservation defect and contrasts.
- `docs/signal-desk/diagnostic-holds-2026-09-08.md`.
- `docs/signal-desk/lineage-held-source-review-2026-09-08.md`.
- `docs/signal-desk/lineage-new-holds-2026-09-08.md`.
- `docs/signal-desk/question-contract-qualification.md` — baseline, isolated boundary amendment, current comparison.
- `docs/signal-desk/question-diagnostic-findings-2026-09-08.md` — all-source first-pass diagnostic.
- `docs/signal-desk/source-need-prompt-gap.md`.
- `docs/signal-desk/recovery-rule-diagnostic-findings-2026-09-08.md` — current source-specific findings.

### Operational artifact root

`work/signal-desk-rebuild/gold-authoring-v2/`

Important descendants:

- `artifacts/continuation-handoff.md` — long chronological operational ledger; read newest entries first and verify their freshness.
- `artifacts/completion-hooks/` — per-process exit receipts and separate acknowledgements.
- `development-source-reauthor-v1/shared-rubric-qualification-v1/` — experimental lineage root.
- `development-source-reauthor-v1/shared-rubric-qualification-v1/question-all-role-qualification-v1/` — failed question baseline.
- `development-source-reauthor-v1/shared-rubric-qualification-v1/question-v1-recovery-rule-clarification-v1/` — current comparison.

### Core pipeline modules

- `research_factory/signal_desk_gold_runner.py`.
- `research_factory/signal_desk_gold_budget.py` and `signal_desk_rebuild_budget.py`.
- `research_factory/signal_desk_gold_capacity.py`, `signal_desk_model_capacity.py`, and `signal_desk_rebuild_fleet.py`.
- `research_factory/signal_desk_gold_audit.py` and `signal_desk_rebuild_gates.py`.
- `research_factory/signal_desk_rebuild_scorer.py`, `signal_desk_scorer_qualification.py`, and `signal_desk_scorer_runner.py`.
- `research_factory/signal_desk_rebuild_evaluation.py`.
- `research_factory/signal_desk_rebuild_approval.py`.
- `research_factory/signal_desk_gold_shared_rubric.py`.
- `research_factory/signal_desk_question_contract.py`, `signal_desk_question_prompts.py`, and `signal_desk_question_lineage.py`.
- `research_factory/signal_desk_question_boundary_contract.py` — isolated boundary candidate, not the active family.
- `research_factory/signal_desk_source_need_prompts.py` — recovery clarification.

### Current runners and read-only status

- `scripts/pif_signal_desk_question_recovery_qualification.py` — current family execution.
- `scripts/pif_signal_desk_question_status.py` — explicit family counts; does not prove liveness.
- `scripts/pif_signal_desk_question_qualification.py` — exhausted baseline; do not accidentally resume it as the new family.
- `scripts/pif_signal_desk_question_final_review.py` — baseline reviewer contract; do not assume it automatically accepts another family's artifacts.
- `scripts/pif_signal_desk_completion_hook.py` — one-shot process wrapper.

Read-only comparison status:

```sh
cd /Users/kolbydayley/pif-factory
python3 -B scripts/pif_signal_desk_question_status.py --family recovery
python3 -B scripts/pif_signal_desk_question_status.py --family recovery --calls
```

The historical authorized launch form is recorded for handoff, **not as an instruction to launch without checking the current state and quality holds**:

```sh
python3 -B scripts/pif_signal_desk_completion_hook.py --launch -- \
  /usr/bin/python3 -B scripts/pif_signal_desk_question_recovery_qualification.py --execute
```

### Selected recent commits

| Commit | Recorded purpose |
|---|---|
| `0e73df2` | Isolated fresh recovery-rule comparison with full denominators |
| `ab98829` | Family-aware read-only status |
| `f4e1b61` | Held outputs never retried; denominator/lineage preserved |
| `44542fd` | Marketplace recovery diagnostic |
| `e12bf7b` | JSParty question-boundary recurrence |
| `bcff71f` | Hidden Brain source diagnostic |
| `59b9f8b` | Coding/legal AI grounding defects |
| `1da3479` | DRA targeted improvement and remaining span failure |
| `70ae311` | TSMC targeted improvement and evidence failure |
| `7bd0969` | Governance source/ownership boundaries |
| `c40a2a0` | Stoica targeted improvement and atomicity risk |

Recorded recent tests include five status tests and five recovery-runner tests, plus earlier question/lineage/boundary contract tests. These are historical test results, not a test suite rerun for this documentation-only change and not a semantic approval score.

## 17. Shared-worktree safety

Claude and other work have edited the same repository. The local git status during this handoff contained unrelated modified and untracked paths. Preserve them; stage only the specific document or owned code being changed.

Observed modified paths included `docs/RUNBOOK.md`, headless execution/frontier/scorer/budget/worker modules, attribution and catalog scripts, and their tests. Untracked paths included `nohup.out`, gold-exclusion code/tests, a caption receiver, and a gold-fable audit script/tests.

Do not use `git add -A`, `git commit -a`, destructive checkout/reset, broad process kills, or a dirty-tree production release. A script's presence in the tree may reflect another agent's work; inspect ownership and receipts before modifying it or claiming its result.

No OpenClaw or Finance production lane is in scope. Public pages must not expose full transcripts, long copyrighted passages, credentials, or private fixtures.

## 18. Acceptance ledger and non-negotiable boundaries

The project is not finished until there is an explicit reconciliation of:

- All 804 frozen windows and their source revisions.
- Authored, structurally valid, independently reviewed, accepted, quarantined, and unresolved counts.
- Per-split audit coverage, event denominators, critical-error bounds, agreement bounds, and catastrophe counts.
- Speaker-turn, flattened, paragraph, ASR, current-show, and OOD behavior.
- Correct-empty windows and narrative-source contamination.
- Current scorer/spec version and qualification receipt.
- A1 measured ceilings beside frozen gates.
- Source/claim/voice identities and every relevant citation/route.
- Shadow-run throughput, losses, quarantines, approval processing and actual approval rates.
- Public build, payload, DOM, routes, mobile, and Railway verification.

Never collapse these distinctions:

| Observation | Does not establish |
|---|---|
| Model call returned | Valid schema |
| Valid schema / exact offsets | Correct meaning or attribution |
| Source-supported assertion | Strategic importance |
| Person named in text | That person's utterance |
| Quote owner known | Narrator identity known |
| Independent reviewer agrees | Infallibility or corpus-wide reliability |
| Fewer claims | Better atomicity or recall |
| More claims | Better coverage |
| Retry succeeded | Good first-pass behavior |
| Child exited zero | Gold acceptance |
| IPC dispatch accepted | Idle task woke and continued |
| Worker running | Accepted-data progress |
| Presentation deployed | Clean corpus released |

## 19. Candid retrospective

The valuable work here is real: a better research interface, clearer publication boundaries, a representative acquisition design, preserved provenance, disciplined sealed-data handling, explicit statistical gates, recoverable execution, and detailed source-grounded findings about why labels were wrong.

The failure is also real: the campaign became an extended contract-development project without giving Kolby a clear enough answer about the remaining distance to usable accepted data. Mechanisms to continue work were repeatedly improved, but continuation itself was not the objective. The user wanted the dataset finished and the research product trustworthy.

The next owner should not celebrate another batch of valid envelopes or another successful callback as completion. The next milestone needs to be a bounded, understandable decision about the labeling architecture, followed by a genuinely passing independent quality gate—or an explicit, evidence-backed explanation that the current approach cannot meet the requested standard within the authorized budget.

**Bottom line:** there is substantial implementation and diagnostic progress, but the original data-quality objective remains open. The last checked worker is stopped after a failed/held run, the full gold benchmark is not accepted, and the project must now prioritize a controlled path to acceptance over further open-ended experimental motion.
