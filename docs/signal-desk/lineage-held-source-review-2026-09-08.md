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
