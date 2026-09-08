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

## Operational control

The qualification runner now checks its own `ADMISSION-HOLD.json` before each
new role. It drains in-flight calls and records a quality hold separately from
provider errors and budget kills. Existing processes launched before this code
change do not hot-reload it. A v5-only hold prevents later relaunches; it does
not assert that the currently running older process has stopped.
