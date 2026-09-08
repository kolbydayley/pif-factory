# Adjudication identity defect

Source inspection of development window `sdw_777d46db3fa4c592b71e` found
that v5 C retained all 11 A and 9 B records as 20 output records. Several
near-identical propositions were merely reclassified as supporting context.
Exact span validation and a maximum same-span count of two do not establish
atomicity or uniqueness.

The likely mechanism is an instruction collision: Gold C must merge redundant
claims, while embedded single-family classification rules require preserving
every supplied candidate ID. Those rules apply to the output record's family
assignments, not to retaining every A/B input as a separate semantic record.

## Required next experiment

- Preserve frozen v5 inputs, raw outputs, failures and correction receipts.
- Introduce a separately versioned adjudication envelope. Keep a canonical
  event set and explicit dispositions for every `(author, input_event_id)`.
- Allow multiple input IDs to map to one retained output. Record corrections,
  splits, rejections and unresolved cases explicitly; never lose the input
  denominator or invent supporting-context events to satisfy ID coverage.
- Validate total input coverage, unique lineage keys, valid output references,
  and nonempty reasons. Structural validation is not semantic approval.
- Test identical paraphrases, distinct propositions sharing evidence, compound
  claims requiring splits, duplicated metadata, conflicting attribution, and
  legitimate question context separately. Independently review actual-source
  dispositions before proceeding to full-population qualification.

This is an unqualified development diagnosis, not accepted gold or a measured
corpus-wide duplication rate. No sealed answers were inspected.

## Independent source review completed

On 2026-09-08, six metered GPT-5.5 high calls reviewed eleven contrasts against
the full Marketplace source. Actual model/request hashes, original outputs,
assigned-ID coverage and exact source quotes all verified. Hook
`5ea5b080-29e8-4a90-975a-c444931316d7` exited zero and was separately acknowledged.

- Cases 01–08: merge equivalent author records, retaining qualifications and
  correcting fields rather than multiplying semantic records.
- Case 09: B's compound culture/political-climate claim maps to two narrower
  A propositions; split-and-merge rather than three retained records.
- Case 10: political-climate explanation and firsthand affirmation of effects
  on the speaker's work remain distinct. Preserve the question as context.
- Case 11: industry resource withdrawal and the downstream nonprofit viability
  consequence remain distinct despite their causal connection.

All eleven contract notes said the lineage contract was sufficient. This
supports trying the envelope; it does not establish semantic reliability across
the full sixteen-source diagnostic or the 804-window benchmark.

Case 07 introduces a classification nuance requiring explicit final review:
the reviewer calls the vague affirmative-action statement source-supported and
warns against treating it as unsupported. The prior independent review supported
quarantining the same preserved record as an underspecified research limitation.
These are not grounds to silently promote it: source support and publishability
are separate, and quarantine must retain its stated wider-context need. The
next C and final review must preserve this distinction.

The next qualification family changes adjudication lineage only. A/B/AUDIT
semantic rules and source windows remain unchanged. Reuse only outputs with
verified identical source/prompt/schema/provider provenance; never reuse old C
as adjudicated output or erase first-pass failures. Qualify over all original
sixteen sources before any full-benchmark acceptance claim.

## Operational control

The qualification runner now checks its own `ADMISSION-HOLD.json` before each
new role. It drains in-flight calls and records a quality hold separately from
provider errors and budget kills. Existing processes launched before this code
change do not hot-reload it. A v5-only hold prevents later relaunches; it does
not assert that the currently running older process has stopped.
