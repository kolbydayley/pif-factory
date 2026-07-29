# True-North Relational-Merge Certification Verifier

Date: 2026-07-28

Module: `research_factory/true_north_relational_merge.py`
Tests: `tests/test_true_north_relational_merge.py`

## Why this exists

Plan Task 4d option 2 proposes rebinding the Phase C disposition gate to
*intrinsic* junk only (chrome, bare mention, fragment) and moving *relational*
junk downstream. Relational junk is the `non_useful_repetition*` and
`nonasserted_question_frame` families: a candidate that is a perfectly good
sentence in isolation and is junk only because the same proposition is already
carried by another retained candidate.

That reclassification is safe only if the downstream canonicalization stage
actually folds each relational escape into its duplicate's canonical group
(ledger category `merged_duplicate_retained`). Without a check, option 2 would
weaken the gate on a promise nobody measured. This module is that check.

It is buildable and useful regardless of which 4d option Kolby chooses: option 2
requires it, and option 1 still benefits from having the contamination number.

Nothing here is wired into the pipeline. It is a clean function surface a later
task can call from certification.

## Design

Two layers, deliberately separated.

**Pure verifier** — `verify_relational_merges(atomics, canonical_groups, escapes)`.
Deterministic; no model calls, no network, no disk. Inputs:

- `atomics`: `[{"candidate_id", "atomic_claim_ids" | "atomic_claim_id", "ledger_category"?}]`
- `canonical_groups`: `[{"canonical_group_id", "atomic_claim_ids", "identifying"?}]`
- `escapes`: `[{"candidate_id", "junk_reason", "duplicate_of"?}]` — a
  non-relational `junk_reason` raises `RelationalMergeError`, so the disposition
  gate's intrinsic classes can never be laundered through this path.

Merge rule: an escape is **merged** when one of its atomic claims sits in an
*identifying* canonical group that also contains an atomic claim owned by a
different candidate. That co-membership is the observable form of
`merged_duplicate_retained`. If the escape declares a `duplicate_of`, that
specific candidate must be in the group.

Output `RelationalMergeReport` carries per-escape
`{candidate_id, merged, canonical_group_id, duplicate_of, duplicate_candidate_ids,
atomic_claim_ids, ledger_category, junk_reason, reason}` and the aggregate
`{relational_escapes, merged_count, unmerged, contamination_zero,
non_identifying_groups}`. An empty escape list is vacuously
`contamination_zero: true` with `relational_escapes: 0`.

Non-merge outcomes are named rather than collapsed: `no_atomic_claims`,
`no_canonical_group`, `non_identifying_canonical_group`,
`singleton_canonical_group`, `declared_duplicate_not_in_canonical_group`.

**Identifying keys.** A canonical group whose subject or proposition key is a
placeholder (`unmapped`, `unassigned`, `unknown`, `none`, `other`, … — see
`NON_IDENTIFYING_CANONICAL_KEYS`) is *not* evidence of a merge. A producer that
emits `unmapped` has declined to reconcile the variant. Counting those keys as
groups would certify a merge that never happened. This is the single most
load-bearing decision in the module; see the dry-run below.

**Loader** — `load_relational_merge_inputs(...)` / `verify_run(...)` /
`discover_canonicalized_runs(...)`. The only disk-aware surface. It opens a
stored run's shadow database read-only (`PRAGMA query_only`) and reads the
development gold file, exactly as the existing certification scorer does.

- Canonical groups are built from `true_north_variant_canonical_map` joined to
  `current_accepted_position_observations`; the group id is
  `<canonical_subject_key>::<canonical_proposition_key>`.
- Atomics and ledger categories come from `true_north_stage_ledger` (stage
  `atomic`).
- Escapes default to ledger-derived: a gold `reject` candidate whose ledger
  category is in `VALUE_LEDGER_CATEGORIES`. Pass `escape_candidate_ids` to score
  a different stage's escape set instead (e.g. the frozen Phase C composed
  disposition); ids without a relational gold reason are filtered out and
  reported as diagnostics rather than raising.
- `partition="holdout"` is refused, and any gold or shadow path containing
  `sealed-holdout`/`holdout` is refused. **Sealed holdouts stay sealed.**

No candidate identifier or answer key is embedded in the module. Reason codes are
read from the gold file supplied at call time — the same certification-time gold
access the existing scorer already performs. Test fixtures are synthetic.

CLI for offline inspection (reads only, writes nothing):

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -B -m research_factory.true_north_relational_merge --list-runs
PYTHONDONTWRITEBYTECODE=1 python3 -B -m research_factory.true_north_relational_merge \
  --run-id tnrun_62691fcee1b451600460cf53
```

Exit code: `0` zero contamination, `2` contamination found, `1` input error.

## Offline dry-run against stored artifacts

No paid calls, no network, no writes. Development partition only.

### Artifact availability

Of the fourteen stored run databases under the suite, **exactly one has a
canonical map**:
`tnrun_62691fcee1b451600460cf53` (development, `succeeded`, 208 candidates, 76
subject-map rows, 187 variant-map rows). Every other run either has an empty
ledger or never reached canonicalization. The Phase C Task 4/4b runs are stored
as composed JSON results and never produced a ledger-bearing canonicalized run,
so the measurement below necessarily uses this earlier multipass run's
canonicalization as the only observed instance of the downstream stage.

### Gold reason codes for the five plan-named junk candidates

| Candidate | Gold reason code | Class |
|---|---|---|
| `dev_c094b91406c9222943a29eba` | `non_useful_repetition` | relational |
| `dev_ab9794e907ab6d420d4a9bea` | `non_useful_repetition_of_model_definition` | relational |
| `dev_fcec890304c9c2b40332af53` | `nonasserted_question_frame` | relational |
| `dev_d7f6bd87ab720be875111f97` | `truncated_fragment_with_unresolved_watching_object` | **intrinsic** |
| `dev_6184abb2048d10e9496e6ee4` | `bare_policy_artifact_mention` | **intrinsic** |

This is itself a finding for the 4d decision: of the three residual Task 4b
escapes (`dev_6184…`, `dev_ab97…`, `dev_fcec…`), **only two are relational**.
`dev_6184abb2048d10e9496e6ee4` is a bare mention, an intrinsic class that option 2
explicitly keeps inside the gate. Option 2 therefore cannot by itself take the
Task 4b result to 0/9 — it reduces three escapes to one intrinsic escape.

### Result — ledger-derived escape set (`tnrun_62691fcee1b451600460cf53`)

```
relational_escapes: 2   merged_count: 0   contamination_zero: FALSE
unmerged: dev_ab9794e907ab6d420d4a9bea, dev_c094b91406c9222943a29eba
```

| Candidate | Ledger category | Canonical group | Merged | Reason |
|---|---|---|---|---|
| `dev_ab9794e907ab6d420d4a9bea` | `retained_canonical_member` | `fable_as_guardrailed_mythos_tiering_and_access_levels::unmapped` | no | `non_identifying_canonical_group` |
| `dev_c094b91406c9222943a29eba` | `retained_canonical_member` | `fable5_jailbreak_attempts_and_nonuniqueness_claims::unmapped` | no | `non_identifying_canonical_group` |

Scoring the frozen Phase C escape set instead (`--escape-id dev_6184… --escape-id
dev_ab97… --escape-id dev_fcec…`) gives the same verdict:
`relational_escapes: 2, merged_count: 0, contamination_zero: FALSE`, with
`dev_fcec890304c9c2b40332af53` unmerged for a different reason — this run held it
(`held_needs_review`, zero atomic claims), so it has nothing to merge.

### The measurement that decides the answer

**90 of 187 variant-map rows carry `canonical_proposition_key = "unmapped"`**, i.e.
41 of 107 canonical groups are placeholder groups. Both relational escapes landed
in one.

If placeholder keys were treated as real groups, the verdict flips to
`contamination_zero: TRUE` — `dev_ab97…` would "merge" with
`dev_0b86b7c34e7b77f3de338e8d` and `dev_4a68998ca7df637c16aef843`, and
`dev_c094…` with `dev_e6e83cf89be496581ee63173`. But co-membership under
`subject::unmapped` only means the two claims share a *subject*, not that the
canonicalizer judged their truth conditions equivalent. Certifying on that basis
would let subject-level topicality masquerade as duplicate suppression, which is
precisely the contamination the check exists to prevent. The module therefore
excludes placeholder keys, and reports every one it excluded.

### Ledger category `merged_duplicate_retained` is never written

`LEDGER_CATEGORIES` in `research_factory/true_north.py` defines
`merged_duplicate_retained`, but no code path assigns it. `_insert_atomic_ledger`
writes only `retained_supported_singleton`, `revised_to_valid`,
`held_needs_review`, and `rejected_junk`; `_finalize_ledger` upgrades some rows to
`retained_canonical_member` and nothing else. Observed count in the canonicalized
run: **0 rows**. Plan Task 4d option 2's phrase "the ledger already defines
`merged_duplicate_retained`" is true of the constant only — the disposition it
names is not produced today. That is why this verifier derives merge status from
canonical-group co-membership rather than trusting the ledger category, and it is
work option 2 would have to fund before it could rely on that category.

### Headline

Both relational escapes are currently **UNMERGED**; measured corpus contamination
is **2**, not zero. On today's stored evidence, Task 4d option 2's premise does
not hold: canonicalization did not fold the relational duplicates into their
originals — it left them in placeholder groups, and the ledger disposition the
option cites is never emitted. Option 2 is not free; it requires canonicalization
work first, and even then it addresses only two of the three residual Task 4b
escapes.

## Verification

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -B -m pytest -p no:cacheprovider \
  tests/test_true_north.py tests/test_true_north_relational_merge.py
```

68 passed (46 pre-existing true-north tests untouched, 22 new).
