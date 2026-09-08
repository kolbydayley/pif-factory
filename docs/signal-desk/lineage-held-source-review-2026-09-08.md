# Development lineage holds: source reconciliation

These are diagnostic findings, not approved gold or model-wide prevalence estimates.
All original records and failed attempts remain in the qualification denominator.

## `sdw_4117ea30ae4ec3f116ef`, role A

Inspected the complete supplied development window and all 15 proposed claims.
Source digest `71ca0f8e13f9e570826e9f5a3ec81f1306a8ec1789b6075121d31ad85bbdca1c`;
packet `818ced84576324345a2661dbff5108d4fce97a8d5f7cd70963e778d2758f89f5`;
raw output `fccf910e61e76ba4ccc6fe49ec1650e12dce5641866502322ce8efeccdb60021`.

The immediate validation error is not the entire defect. There are 19 inexact
nested spans and seven inconsistent recovery-need assignments. No correction has
been applied. Span quotes exist in the source, but repeated words need explicit
context selection: event 10's Anthropic target is at 3123, not the later mention
at 3248; event 13's obviously is at 4340, not the Replit passage at 2327.
Do not use a generic first-occurrence repair.

### Proposed distinctions requiring independent approval

- Events 01, 12, 14, 15: the surrounding supplied source makes the assessments,
  reported figures and forecasts intelligible. Propose `needs: none`, retaining
  uncertain publication state and every qualifier. External verification of
  financial figures remains separate from source-context recovery.
- Event 02: the product-positioning contrast is understandable, but its supplied
  ASR noun is damaged. Independent reviewer should determine whether retaining
  the literal surface with uncertainty is adequate or whether this particular
  claim must become a quarantined research limitation. Do not silently replace
  that noun with IDE using outside knowledge.
- Event 09: the proposed acquirer is unresolved in ASR. Keep the candidate and
  audio/source recovery need, but propose research-limitation role plus quarantine.
  Do not infer OpenAI from context or general knowledge.
- Event 10: the customer relationship phrase is damaged, and the proposed claim
  adds temporal linkage between becoming a customer and encountering limits.
  Full-source review must examine that linkage, not merely make the role schema
  valid. Propose holding this as a research limitation pending audio/source;
  do not treat quarantine alone as semantic correction or accepted gold.

### Additional review scope

Review all 15 records, not only the seven recovery flags. Event 03 strengthens
an expressed uncertainty about strategy into a question about viability. Event
07 bundles product progress, market attention and switching economics; inspect
atomicity without blindly splitting each clause. Event 08 interprets a damaged
ASR word using the competition context; its fidelity needs explicit review.
The supplied speaker labels Shawn and Swyx must not be externally collapsed or
reassigned; use source-bound voice corridors, while retaining source uncertainty.
Preserve names such as Lex Friedman as transcript surfaces, not external spelling
corrections. The final independent review must address these fidelity questions
even if a numeric projection passes every deterministic validator.

## `sdw_99a1771e94fa2b923f9e`, role B

The complete source supports understanding the speaker's hedged prevalence
estimate. Its unspecified underlying methods limit verification, not interpretation.
The prepared one-field proposal changes only evt_002's recovery need to none;
all nine records, attribution, claim wording and uncertainty remain unchanged.
`scripts/pif_signal_desk_hidden_brain_b_repair_review.py` binds the actual raw,
source, request and provider evidence. Two tests verify preservation and provenance.
An independent GPT-5.5 decision is still required; the earlier role-A decision
does not authorize this new role-B correction.

## Two additional opening-voice holds

Full supplied development sources and all candidate claim texts were inspected.
These expose an important distinction to test independently before further prompt
changes: unavailable speaker identity is not automatically unintelligible content.
Do not backfill a clipped opening voice from a later named turn.

### Kubernetes DRA: `sdw_8bc85d983495f3780cb2`, A, 12 records

Raw `20292551810b36fb607612139b10571532dbd542c5b533be4075a4abe216b3e4`;
packet `a41014004e553cb84aa28e8c043c96bd8ca7c9e514b5afadeb8b8d4afc719cee`.
The opening explanation has no speaker label. Later Kaslin Fields and John
Belamaric labels are explicit, but do not prove who spoke before the first label.
Events 01–07 retain understandable device metadata, resource-claim flexibility,
DeviceClass administration, allocation workflow and a qualified versioned feature
forecast. They carry recovery needs solely for the opening voice, while retaining
substantive/supporting roles. This trips the role/needs equivalence validator.
The correct next diagnosis must separate semantic usefulness, identity recovery,
publication eligibility and evidence-role classification. Never assign John to the
opening paragraphs merely because he gives the later answers. Event 08 is a host
question linked as context, not evidence that the guest endorses its premises.
Events 09–11 retain conditional scalability and architectural caveats from the
explicit guest turn. Event 12 is the host's conclusion, not the guest's statement.

### TSMC history: `sdw_4acaaec65ea71658126b`, A, 14 records

Raw `77695c587211dd21da57e3f1c3f824277fef37920c8bbe6dca9e5dfb04a84dc9`;
packet `d0f4f279d15b66c882a262b078f8b8bc39d355978c9d485146d8a1d913f02516`.
Opening solar/LED/new-business explanations are unlabeled; preserve unresolved
identity rather than inferring Morris from later labels. Event 04's executive
identity is unresolved even though its current speaker is named. Events 05–12
come from explicitly labeled turns in the NVIDIA settlement discussion; preserve
the distinction between Morris's account and Ben's retrospective interpretation.
The final two records correctly isolate an explicit presenting-partner pitch as
quarantined promotion. Its coherent financial mechanisms do not turn sponsor copy
into independent evidence. Nested offset errors need separate exact-source review.

## Contract-level diagnosis to carry into qualification

`signal_desk_evidence_role_experiment.RULES` says usefulness is independent of
attribution. `signal_desk_full_event_v4_prompts.COMMON` explicitly permits a
null-voice binding for unresolved speakers. But the inherited v2 role validator
requires `(role == research_limitation) == (needs != none)`, while the prompt only
explicitly instructs research limitations to specify recovery needs. The reverse
restriction is not stated as clearly. Authors repeatedly use `needs` to capture
identity or verification concerns on otherwise substantive content.

This is a measured recurring contract interpretation problem, not proof that all
those claims are unusable or that the scorer should loosen its gates. Do not edit
the frozen contract or reclassify these outputs silently. Independently adjudicate
the distinction using these sources; if a prompt/schema revision is warranted,
version it as a new measured family and keep first-pass failures and repairs visible.
No extra speaker names, benchmark shrinkage, or publication authorization follows.
