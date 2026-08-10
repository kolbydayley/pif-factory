# Direction-only metric quarantine disclosure

The 791 `ai_discourse_v3_1` labels produced during the temporary direction-only
contract window contain sparse direction-only metrics by construction. A
seeded audit found that only 6 of 15 sampled retained directions were supported
by an explicit change-or-state assertion. The contract was therefore replaced
with deterministic `direction_evidence` grounding.

Remediation preserved the rest of each label and non-destructively quarantined
1,823 unsupported legacy direction-only metric objects. Each quarantine record
keeps the original metric payload, evidence, label, segment, event index,
failure rule, and timestamp for targeted recovery or later relabeling. Numeric
metrics and non-metric projections were outside this remediation: all 13,325
event-context metrics matched and all non-metric projections were byte-equivalent
before and after the scoped operation.

Consumers must treat direction-only coverage in these 791 labels as deliberately
sparse and must not interpret absence as evidence that no direction was stated.
The quarantine-and-disclose decision avoids rewriting otherwise-valid labels at
an estimated cost of 791 provider calls and roughly 186 million billed tokens.
