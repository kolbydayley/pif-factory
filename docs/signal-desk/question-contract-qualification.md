# Question representation: qualification boundary

The development-source audit found genuine inquiries mislabeled as assertions or
forecasts in the DRA and coding-tools windows. The frozen v5 speech-act enum has no
question value. Fixing a speaker label or accepting that incorrect enum would not
fix this data defect.

The isolated `signal_desk_question_contract`, `signal_desk_question_prompts`, and
`signal_desk_question_lineage` modules add the missing representation. Questions
remain supporting context, never a substantive assertion. They retain their own
voice, questioned proposition status, and exact source evidence. The adjudication
ledger still accounts for every A/B input and every output, including rejection,
merging, splitting, and source-discovered additions.

## Required execution and evidence

1. Finish inspecting the active original-family diagnostic without overwriting its
   requests, responses, failures, or repairs. Its held records remain in totals.
2. Freeze the question-family schemas and all role prompts against the same full
   original sixteen development windows and exact source bytes. Do not select only
   the two failures. Preserve the original 804-window benchmark and sealed splits.
3. Qualify all four roles under the new version with actual model/request receipts;
   old responses cannot be relabeled as new-family calls. Independent A, B, and
   AUDIT do not receive one another's answers. C receives A/B and no blind audit.
4. Independently review the complete output population against the complete source,
   including every question and its answer linkage, rhetorical assertions, mixed
   inquiry/assertion turns, unresolved voices, and all adjudication dispositions.
   Judge attribution and consequential claim coverage separately from question
   coverage. A question is not a factual vote or independent supporting claim.
5. Report first-pass validity separately from explicit repairs. Any change to a
   reviewed record invalidates its approval; unchanged approvals require exact
   source, record, and voice-pool equivalence. Never shrink failed populations.
6. Only after passing qualification should the full gold migration and independent
   per-split reliability audits proceed. Recalibrate A1 and qualify A2 against the
   final frozen representation before tournament comparisons. Existing acceptance
   thresholds and uncertainty bounds do not change because the enum changed.

The isolated runner is now implemented in
`scripts/pif_signal_desk_question_qualification.py`: it requires all sixteen
windows and 64 fresh A/B/C/AUDIT outputs, verifies source and provider hashes,
preserves held failures, and uses the existing metering and shared runner locks.
It was frozen and launched on 2026-09-08 after the original diagnostic was
accounted for: 8 complete windows, 8 held windows, 42 returned outputs containing
515 records. Only 8 returned outputs were first-pass structurally valid; 34 failed,
26 were explicitly repaired, and 8 remain held. All 64 original role slots remain
in the denominator, including 22 unfulfilled slots. This is diagnostic observation,
not qualification. Thirty question-family code tests passed before launch.

The new run preserves the question-only change and does not incorporate the
separate source-need clarification. That known failure risk remains measurable;
it must not be silently repaired away or hidden by a successful exit code.
The independent full-population reviewer
is `scripts/pif_signal_desk_question_final_review.py`; it requires every role
output before preparing record and adjudication-ledger reviews. Actual review
receipts are verified separately from dispatch success.

All receipts still say `qualified: false`. Unit tests demonstrate contract,
lineage, and provenance safeguards only. They are not evidence of extraction
quality, speaker accuracy, or accepted gold. The original diagnostic's repaired
outputs must not be used to disguise its first-pass failure rate or to claim that
the new question-family prompt has been tested.

Read-only progress is available through
`python3 -B scripts/pif_signal_desk_question_status.py` (`--calls` for role rows).
It retains every planned slot, distinguishes structurally valid raw responses from
provider-verified authored outputs, and never claims acceptance or process liveness.
Check the exact child PID independently before deciding whether to resume a run.

## Isolated boundary amendment — not dispatched

The frozen question-v1 diagnostic exposed two non-substantive role conflicts:
JSParty housekeeping and a clipped luxury-interview inquiry marked as a research
limitation. `signal_desk_question_boundary_contract.py` is a separate, unqualified
v2 candidate allowing those two roles in addition to supporting context. It still
rejects substantive/metadata question roles and asserted question premises, and
delegates unchanged source, attribution, recovery-need and publication validation.
The v1 runner, frozen schemas and all recorded responses remain untouched.

Fifteen unit tests passed across v1 and this isolated boundary module. These tests
verify rule delegation and immutability, not semantic correctness or model quality.
There is no v2 dispatch or automatic migration. Before any use, finish the original
diagnostic inventory, freeze fresh role prompts and lineage for the same sixteen
sources, and obtain full-population independent review. Keep the separate source-need
clarification out of this one-change family; neither defect may be hidden by repairs
or reused answers. The complete 804-window acceptance scope remains unchanged.

## Next measured comparison: recovery-rule prompt only

Question-v1 returned A for all sixteen sources: fifteen held and only the chrome
negative control valid. Its full four roles are valid, giving 19 returned outputs,
four valid roles, fifteen held and45unfulfilled out of64. The first-error inventory
is six recovery-rule, six exact-span, two question-boundary and one status conflict.
This is a failed qualification, not an accepted or repaired baseline.

`scripts/pif_signal_desk_question_recovery_qualification.py` defines
`question-v1-recovery-rule-clarification-v1`. It appends only the already specified
recovery-need clarification to each question-v1 role prompt. Schemas, validators,
source bytes, source population, model, effort and concurrency stay unchanged;
question-boundary v2 is NOT included. Packets carry new family/prompt hashes and
cannot reuse old outputs. The same shared locks, per-call metering, stop controls,
source checks and provider proofs apply. C still requires verified independent A/B;
held responses are preserved without silent retries. Complete qualification still
requires all64fresh roles and independent full-source review; no runner exit code
constitutes acceptance. Nine baseline/comparison execution tests passed in4.30s,
including64-role fresh execution, idempotence, holds and stale-prompt rejection.

The comparison tests a specific observed cause, not a claim that prompt wording
alone fixes all defects. Report first-pass failures and missing roles even if the
recovery error decreases. Exact-span failures and question-boundary conflicts are
separate dimensions for later isolated changes. Full804audits/A1/A2 remain closed.
