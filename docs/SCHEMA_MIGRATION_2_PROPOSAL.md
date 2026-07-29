# Schema Migration 2 Proposal

Status: proposal only. Nothing in this document has been applied.

Date: 2026-07-29

## Executive decision

Production migration 1 cannot be treated as equivalent to the current
`INTELLIGENCE_SCHEMA_V1`. All 90 object names exist, and all 18 tables, 22
indexes, and 34 triggers are structurally equivalent, but 12 of the 16 views
have different authorization semantics.

The live views accept any succeeded `pipeline_runs` row referenced by an
accepted record. The currently declared views additionally require that the
producing run belong to the same corpus release:

```sql
AND producing_run.corpus_release_id = release.id
```

For `current_accepted_corpus_releases`, the equivalent condition is:

```sql
AND pipeline_runs.corpus_release_id = corpus_releases.id
```

This is a correctness fix, not neutral formatting. Without it, a record can be
surfaced through an accepted release while its producing run is authoritative
for a different release.

## Version-number warning

The requested name “migration 2” describes the next production transition,
because production has applied only version 1. It is **not currently safe to
register this proposal as numeric version 2**: the source registry already
reserves versions 2 through 4:

1. `versioned_intelligence_v1`
2. `archive_orphan_queue_envelopes_v2`
3. `semantic_scope_and_coverage_v3`
4. `accepted_pipeline_run_authority_v4`

Migration 4 already drops and recreates the accepted-state view family with an
even stronger, stage-specific pipeline-run authority model. The recommended
implementation sequence is therefore:

1. record the explicit version-1 baseline adoption described below;
2. test and apply the existing migrations 2, 3, and 4 in order on a restored
   backup;
3. compare the post-v4 schema with the intended schema;
4. use numeric version 5 only if a residual reconciliation remains.

The narrow 12-view statements in this proposal are included as requested and
are the correct minimal reconciliation if review determines that the views
must be repaired before the broader v4 authority migration. They must not be
registered under a colliding version number.

## Evidence record

| Evidence | Value |
|---|---|
| Applied migration | `1 / versioned_intelligence_v1` |
| Applied checksum | `0ea8dd4afc02b1a668dc872a0a0f9f445c75da53833ebc7bca5144229b94e0fa` |
| Applied at | `2026-07-20T15:05:10+00:00` |
| Current-source checksum | `d973def89537b8e2749306eb578a96adecd2397c1513cae99b5a0b9a720799ca` |
| Current-source statements | 90 |
| Declared objects present | 90 of 90 |
| Equivalent tables/indexes/triggers | 18 / 22 / 34 |
| Equivalent views | 4 of 16 |
| Divergent views | 12 of 16 |
| Live `sqlite_master` fingerprint | `35d7b21177a83f8cd348a583341f029a28d33f2aa23e0b87c9748ee4591f6a97` |
| Verified backup | `/Users/kolbydayley/pif-backups/factory-20260729T1527ET-before-schema-repair.sqlite` |
| Backup SHA-256 | `424a15b02b2dbbc4f9f34665a63b2888229f3b71c536d35114203f1f812d70ed` |

## Difference and correctness matrix

| View | Difference | Semantic effect | Assessment |
|---|---|---|---|
| `current_accepted_corpus_releases` | Current adds `pipeline_runs.corpus_release_id = corpus_releases.id`. | Prevents a promotion from borrowing a succeeded pipeline run belonging to another release. | **Current is a fix.** |
| `current_accepted_identity_resolutions` | Current binds `producing_run.corpus_release_id` to `release.id`. | Excludes accepted identity judgments produced under a different release. | **Current is a fix.** |
| `current_accepted_atomic_claims` | Same release binding. | Excludes claims whose producing run belongs to another release. | **Current is a fix.** |
| `current_accepted_claim_subjects` | Same release binding. | Excludes subjects whose producing run belongs to another release. | **Current is a fix.** |
| `current_accepted_proposition_variants` | Same release binding. | Excludes proposition variants whose producing run belongs to another release. | **Current is a fix.** |
| `current_accepted_position_observations` | Same release binding. | Excludes positions whose producing run belongs to another release. | **Current is a fix.** |
| `current_accepted_source_affiliations` | Same release binding. | Excludes affiliations whose producing run belongs to another release. | **Current is a fix.** |
| `current_accepted_person_appearances` | Same release binding. | Excludes appearances whose producing run belongs to another release. | **Current is a fix.** |
| `current_accepted_claim_relations` | Same release binding. | Excludes relation judgments whose producing run belongs to another release. | **Current is a fix.** |
| `current_accepted_outcome_resolutions` | Same release binding. | Excludes outcome resolutions whose producing run belongs to another release. | **Current is a fix.** |
| `current_accepted_consensus_snapshots` | Same release binding. | Excludes consensus snapshots whose producing run belongs to another release. | **Current is a fix.** |
| `current_accepted_contrarian_snapshots` | Same release binding. | Excludes contrarian snapshots whose producing run belongs to another release. | **Current is a fix.** |

No output columns, ranking rules, review filters, or lineage partitions differ
in these 12 pairs. The only substantive difference is release/run authority.
The direction is corroborated by pending migration 4, which strengthens this
same invariant with explicit per-stage run-authority decisions.

## Exact DDL comparison

### `current_accepted_corpus_releases`

Live:

```sql
CREATE VIEW current_accepted_corpus_releases AS
    SELECT
      corpus_releases.*,
      corpus_release_promotions.id AS promotion_id,
      corpus_release_promotions.promotion_revision,
      corpus_release_promotions.pipeline_run_id AS promotion_pipeline_run_id,
      corpus_release_promotions.created_at AS promoted_at
    FROM corpus_release_promotions
    JOIN corpus_releases
      ON corpus_releases.id = corpus_release_promotions.corpus_release_id
     AND corpus_releases.status = 'accepted'
    JOIN pipeline_runs
      ON pipeline_runs.id = corpus_release_promotions.pipeline_run_id
     AND pipeline_runs.status = 'succeeded'
    WHERE corpus_release_promotions.action = 'promote'
      AND corpus_release_promotions.promotion_revision = (
        SELECT MAX(promotion_revision) FROM corpus_release_promotions
    )
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_corpus_releases AS
    SELECT
      corpus_releases.*,
      corpus_release_promotions.id AS promotion_id,
      corpus_release_promotions.promotion_revision,
      corpus_release_promotions.pipeline_run_id AS promotion_pipeline_run_id,
      corpus_release_promotions.created_at AS promoted_at
    FROM corpus_release_promotions
    JOIN corpus_releases
      ON corpus_releases.id = corpus_release_promotions.corpus_release_id
     AND corpus_releases.status = 'accepted'
    JOIN pipeline_runs
      ON pipeline_runs.id = corpus_release_promotions.pipeline_run_id
     AND pipeline_runs.status = 'succeeded'
     AND pipeline_runs.corpus_release_id = corpus_releases.id
    WHERE corpus_release_promotions.action = 'promote'
      AND corpus_release_promotions.promotion_revision = (
        SELECT MAX(promotion_revision) FROM corpus_release_promotions
    )
```

### `current_accepted_identity_resolutions`

Live:

```sql
CREATE VIEW current_accepted_identity_resolutions AS
    SELECT ranked.*
    FROM (
      SELECT judgments.*,
             ROW_NUMBER() OVER (
               PARTITION BY judgments.identity_lineage_id
               ORDER BY judgments.revision DESC, judgments.decided_at DESC, judgments.created_at DESC, judgments.id DESC
             ) AS _current_rank
      FROM identity_resolution_judgments AS judgments
      JOIN current_accepted_corpus_releases AS release
        ON release.id = judgments.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = judgments.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    JOIN canonical_people AS person
      ON person.id = ranked.canonical_person_id
     AND person.status = 'accepted'
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND ranked.decision IN ('accepted', 'merged')
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_identity_resolutions AS
    SELECT ranked.*
    FROM (
      SELECT judgments.*,
             ROW_NUMBER() OVER (
               PARTITION BY judgments.identity_lineage_id
               ORDER BY judgments.revision DESC, judgments.decided_at DESC, judgments.created_at DESC, judgments.id DESC
             ) AS _current_rank
      FROM identity_resolution_judgments AS judgments
      JOIN current_accepted_corpus_releases AS release
        ON release.id = judgments.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = judgments.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN canonical_people AS person
      ON person.id = ranked.canonical_person_id
     AND person.status = 'accepted'
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND ranked.decision IN ('accepted', 'merged')
```

### `current_accepted_atomic_claims`

Live:

```sql
CREATE VIEW current_accepted_atomic_claims AS
    SELECT ranked.*
    FROM (
      SELECT claims.*,
             ROW_NUMBER() OVER (
               PARTITION BY claims.claim_lineage_id
               ORDER BY claims.revision DESC, claims.created_at DESC, claims.id DESC
             ) AS _current_rank
      FROM atomic_claims AS claims
      JOIN current_accepted_corpus_releases AS release
        ON release.id = claims.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = claims.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    LEFT JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND (ranked.canonical_person_id IS NULL OR person.id IS NOT NULL)
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_atomic_claims AS
    SELECT ranked.*
    FROM (
      SELECT claims.*,
             ROW_NUMBER() OVER (
               PARTITION BY claims.claim_lineage_id
               ORDER BY claims.revision DESC, claims.created_at DESC, claims.id DESC
             ) AS _current_rank
      FROM atomic_claims AS claims
      JOIN current_accepted_corpus_releases AS release
        ON release.id = claims.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = claims.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    LEFT JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
      AND (ranked.canonical_person_id IS NULL OR person.id IS NOT NULL)
```

### `current_accepted_claim_subjects`

Live:

```sql
CREATE VIEW current_accepted_claim_subjects AS
    SELECT ranked.*
    FROM (
      SELECT subjects.*,
             ROW_NUMBER() OVER (
               PARTITION BY subjects.subject_lineage_id
               ORDER BY subjects.revision DESC, subjects.decided_at DESC, subjects.created_at DESC, subjects.id DESC
             ) AS _current_rank
      FROM accepted_claim_subjects AS subjects
      JOIN current_accepted_corpus_releases AS release
        ON release.id = subjects.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = subjects.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_claim_subjects AS
    SELECT ranked.*
    FROM (
      SELECT subjects.*,
             ROW_NUMBER() OVER (
               PARTITION BY subjects.subject_lineage_id
               ORDER BY subjects.revision DESC, subjects.decided_at DESC, subjects.created_at DESC, subjects.id DESC
             ) AS _current_rank
      FROM accepted_claim_subjects AS subjects
      JOIN current_accepted_corpus_releases AS release
        ON release.id = subjects.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = subjects.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

### `current_accepted_proposition_variants`

Live:

```sql
CREATE VIEW current_accepted_proposition_variants AS
    SELECT ranked.*
    FROM (
      SELECT variants.*,
             ROW_NUMBER() OVER (
               PARTITION BY variants.variant_lineage_id
               ORDER BY variants.revision DESC, variants.decided_at DESC, variants.created_at DESC, variants.id DESC
             ) AS _current_rank
      FROM accepted_proposition_variants AS variants
      JOIN current_accepted_corpus_releases AS release
        ON release.id = variants.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = variants.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    JOIN current_accepted_claim_subjects AS subject
      ON subject.id = ranked.subject_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_proposition_variants AS
    SELECT ranked.*
    FROM (
      SELECT variants.*,
             ROW_NUMBER() OVER (
               PARTITION BY variants.variant_lineage_id
               ORDER BY variants.revision DESC, variants.decided_at DESC, variants.created_at DESC, variants.id DESC
             ) AS _current_rank
      FROM accepted_proposition_variants AS variants
      JOIN current_accepted_corpus_releases AS release
        ON release.id = variants.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = variants.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_claim_subjects AS subject
      ON subject.id = ranked.subject_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

### `current_accepted_position_observations`

Live:

```sql
CREATE VIEW current_accepted_position_observations AS
    SELECT ranked.*
    FROM (
      SELECT positions.*,
             ROW_NUMBER() OVER (
               PARTITION BY positions.position_lineage_id
               ORDER BY positions.revision DESC, positions.decided_at DESC, positions.created_at DESC, positions.id DESC
             ) AS _current_rank
      FROM accepted_position_observations AS positions
      JOIN current_accepted_corpus_releases AS release
        ON release.id = positions.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = positions.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    JOIN current_accepted_claim_subjects AS subject
      ON subject.id = ranked.subject_id
    JOIN current_accepted_proposition_variants AS variant
      ON variant.id = ranked.variant_id
     AND variant.subject_id = ranked.subject_id
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.atomic_claim_id
    JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_position_observations AS
    SELECT ranked.*
    FROM (
      SELECT positions.*,
             ROW_NUMBER() OVER (
               PARTITION BY positions.position_lineage_id
               ORDER BY positions.revision DESC, positions.decided_at DESC, positions.created_at DESC, positions.id DESC
             ) AS _current_rank
      FROM accepted_position_observations AS positions
      JOIN current_accepted_corpus_releases AS release
        ON release.id = positions.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = positions.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_claim_subjects AS subject
      ON subject.id = ranked.subject_id
    JOIN current_accepted_proposition_variants AS variant
      ON variant.id = ranked.variant_id
     AND variant.subject_id = ranked.subject_id
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.atomic_claim_id
    JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

### `current_accepted_source_affiliations`

Live:

```sql
CREATE VIEW current_accepted_source_affiliations AS
    SELECT ranked.*
    FROM (
      SELECT affiliations.*,
             ROW_NUMBER() OVER (
               PARTITION BY affiliations.affiliation_lineage_id
               ORDER BY affiliations.revision DESC, affiliations.created_at DESC, affiliations.id DESC
             ) AS _current_rank
      FROM source_affiliations AS affiliations
      JOIN current_accepted_corpus_releases AS release
        ON release.id = affiliations.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = affiliations.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_source_affiliations AS
    SELECT ranked.*
    FROM (
      SELECT affiliations.*,
             ROW_NUMBER() OVER (
               PARTITION BY affiliations.affiliation_lineage_id
               ORDER BY affiliations.revision DESC, affiliations.created_at DESC, affiliations.id DESC
             ) AS _current_rank
      FROM source_affiliations AS affiliations
      JOIN current_accepted_corpus_releases AS release
        ON release.id = affiliations.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = affiliations.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

### `current_accepted_person_appearances`

Live:

```sql
CREATE VIEW current_accepted_person_appearances AS
    SELECT ranked.*
    FROM (
      SELECT appearances.*,
             ROW_NUMBER() OVER (
               PARTITION BY appearances.appearance_lineage_id
               ORDER BY appearances.revision DESC, appearances.created_at DESC, appearances.id DESC
             ) AS _current_rank
      FROM person_appearances AS appearances
      JOIN current_accepted_corpus_releases AS release
        ON release.id = appearances.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = appearances.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_person_appearances AS
    SELECT ranked.*
    FROM (
      SELECT appearances.*,
             ROW_NUMBER() OVER (
               PARTITION BY appearances.appearance_lineage_id
               ORDER BY appearances.revision DESC, appearances.created_at DESC, appearances.id DESC
             ) AS _current_rank
      FROM person_appearances AS appearances
      JOIN current_accepted_corpus_releases AS release
        ON release.id = appearances.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = appearances.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_people AS person
      ON person.id = ranked.canonical_person_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

### `current_accepted_claim_relations`

Live:

```sql
CREATE VIEW current_accepted_claim_relations AS
    SELECT ranked.*
    FROM (
      SELECT judgments.*,
             ROW_NUMBER() OVER (
               PARTITION BY judgments.source_claim_id, judgments.target_claim_id
               ORDER BY judgments.revision DESC, judgments.decided_at DESC, judgments.created_at DESC, judgments.id DESC
             ) AS _current_rank
      FROM claim_relation_judgments AS judgments
      JOIN current_accepted_corpus_releases AS release
        ON release.id = judgments.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = judgments.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    JOIN current_accepted_atomic_claims AS source_claim
      ON source_claim.id = ranked.source_claim_id
    JOIN current_accepted_atomic_claims AS target_claim
      ON target_claim.id = ranked.target_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_claim_relations AS
    SELECT ranked.*
    FROM (
      SELECT judgments.*,
             ROW_NUMBER() OVER (
               PARTITION BY judgments.source_claim_id, judgments.target_claim_id
               ORDER BY judgments.revision DESC, judgments.decided_at DESC, judgments.created_at DESC, judgments.id DESC
             ) AS _current_rank
      FROM claim_relation_judgments AS judgments
      JOIN current_accepted_corpus_releases AS release
        ON release.id = judgments.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = judgments.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS source_claim
      ON source_claim.id = ranked.source_claim_id
    JOIN current_accepted_atomic_claims AS target_claim
      ON target_claim.id = ranked.target_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

### `current_accepted_outcome_resolutions`

Live:

```sql
CREATE VIEW current_accepted_outcome_resolutions AS
    SELECT ranked.*
    FROM (
      SELECT resolutions.*,
             ROW_NUMBER() OVER (
               PARTITION BY resolutions.claim_id
               ORDER BY resolutions.revision DESC, resolutions.resolved_at DESC, resolutions.created_at DESC, resolutions.id DESC
             ) AS _current_rank
      FROM outcome_resolution_revisions AS resolutions
      JOIN current_accepted_corpus_releases AS release
        ON release.id = resolutions.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = resolutions.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_outcome_resolutions AS
    SELECT ranked.*
    FROM (
      SELECT resolutions.*,
             ROW_NUMBER() OVER (
               PARTITION BY resolutions.claim_id
               ORDER BY resolutions.revision DESC, resolutions.resolved_at DESC, resolutions.created_at DESC, resolutions.id DESC
             ) AS _current_rank
      FROM outcome_resolution_revisions AS resolutions
      JOIN current_accepted_corpus_releases AS release
        ON release.id = resolutions.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = resolutions.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

### `current_accepted_consensus_snapshots`

Live:

```sql
CREATE VIEW current_accepted_consensus_snapshots AS
    SELECT ranked.*
    FROM (
      SELECT snapshots.*,
             ROW_NUMBER() OVER (
               PARTITION BY snapshots.focal_claim_id, snapshots.calculation_version
               ORDER BY snapshots.as_of DESC, snapshots.created_at DESC, snapshots.id DESC
             ) AS _current_rank
      FROM consensus_snapshots AS snapshots
      JOIN current_accepted_corpus_releases AS release
        ON release.id = snapshots.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = snapshots.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.focal_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_consensus_snapshots AS
    SELECT ranked.*
    FROM (
      SELECT snapshots.*,
             ROW_NUMBER() OVER (
               PARTITION BY snapshots.focal_claim_id, snapshots.calculation_version
               ORDER BY snapshots.as_of DESC, snapshots.created_at DESC, snapshots.id DESC
             ) AS _current_rank
      FROM consensus_snapshots AS snapshots
      JOIN current_accepted_corpus_releases AS release
        ON release.id = snapshots.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = snapshots.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.focal_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

### `current_accepted_contrarian_snapshots`

Live:

```sql
CREATE VIEW current_accepted_contrarian_snapshots AS
    SELECT ranked.*
    FROM (
      SELECT snapshots.*,
             ROW_NUMBER() OVER (
               PARTITION BY snapshots.target_claim_id, snapshots.calculation_version
               ORDER BY snapshots.as_of DESC, snapshots.created_at DESC, snapshots.id DESC
             ) AS _current_rank
      FROM contrarian_snapshots AS snapshots
      JOIN current_accepted_corpus_releases AS release
        ON release.id = snapshots.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = snapshots.pipeline_run_id
       AND producing_run.status = 'succeeded'
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.target_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

Currently declared:

```sql
CREATE VIEW IF NOT EXISTS current_accepted_contrarian_snapshots AS
    SELECT ranked.*
    FROM (
      SELECT snapshots.*,
             ROW_NUMBER() OVER (
               PARTITION BY snapshots.target_claim_id, snapshots.calculation_version
               ORDER BY snapshots.as_of DESC, snapshots.created_at DESC, snapshots.id DESC
             ) AS _current_rank
      FROM contrarian_snapshots AS snapshots
      JOIN current_accepted_corpus_releases AS release
        ON release.id = snapshots.corpus_release_id
      JOIN pipeline_runs AS producing_run
        ON producing_run.id = snapshots.pipeline_run_id
       AND producing_run.status = 'succeeded'
       AND producing_run.corpus_release_id = release.id
    ) AS ranked
    JOIN current_accepted_atomic_claims AS claim
      ON claim.id = ranked.target_claim_id
    WHERE ranked._current_rank = 1
      AND ranked.review_status = 'accepted'
```

## Proposed narrow reconciliation statements

If a narrow view-only reconciliation is authorized instead of proceeding
directly through the existing v2–v4 chain, it must execute transactionally.
Drop dependent views first:

```sql
DROP VIEW IF EXISTS current_accepted_contrarian_snapshots;
DROP VIEW IF EXISTS current_accepted_consensus_snapshots;
DROP VIEW IF EXISTS current_accepted_outcome_resolutions;
DROP VIEW IF EXISTS current_accepted_claim_relations;
DROP VIEW IF EXISTS current_accepted_person_appearances;
DROP VIEW IF EXISTS current_accepted_position_observations;
DROP VIEW IF EXISTS current_accepted_proposition_variants;
DROP VIEW IF EXISTS current_accepted_source_affiliations;
DROP VIEW IF EXISTS current_accepted_claim_subjects;
DROP VIEW IF EXISTS current_accepted_atomic_claims;
DROP VIEW IF EXISTS current_accepted_identity_resolutions;
DROP VIEW IF EXISTS current_accepted_corpus_releases;
```

Then create them base-first using exactly the “Currently declared” definitions
above, replacing `CREATE VIEW IF NOT EXISTS` with `CREATE VIEW` so an
unexpected collision fails rather than silently passing:

```text
current_accepted_corpus_releases
current_accepted_identity_resolutions
current_accepted_atomic_claims
current_accepted_claim_subjects
current_accepted_proposition_variants
current_accepted_position_observations
current_accepted_source_affiliations
current_accepted_person_appearances
current_accepted_claim_relations
current_accepted_outcome_resolutions
current_accepted_consensus_snapshots
current_accepted_contrarian_snapshots
```

Only these 12 views are in scope. The four equivalent views, all tables,
indexes, triggers, and data remain untouched.

## Explicit version-1 baseline adoption

The existing `schema_migrations` version-1 row must remain byte-for-byte
unchanged. An implementation should introduce a separate append-only adoption
record, protected against update and deletion, containing at minimum:

```text
migration_version: 1
migration_name: versioned_intelligence_v1
recorded_checksum_sha256:
  0ea8dd4afc02b1a668dc872a0a0f9f445c75da53833ebc7bca5144229b94e0fa
adopted_source_checksum_sha256:
  d973def89537b8e2749306eb578a96adecd2397c1513cae99b5a0b9a720799ca
live_sqlite_master_sha256:
  35d7b21177a83f8cd348a583341f029a28d33f2aa23e0b87c9748ee4591f6a97
declared_object_count: 90
present_object_count: 90
equivalent_table_count: 18
equivalent_index_count: 22
equivalent_trigger_count: 34
equivalent_view_count: 4
divergent_view_count: 12
divergent_view_names: [the 12 names in this document]
evidence_document: docs/SCHEMA_MIGRATION_2_PROPOSAL.md
backup_sha256:
  424a15b02b2dbbc4f9f34665a63b2888229f3b71c536d35114203f1f812d70ed
justification:
  Migration 1 source evolved after application; all declared objects exist,
  while 12 derived views require an explicit forward reconciliation.
authorized_by: explicit owner/review authorization required
adopted_at: recorded at implementation time
```

The migration validator may recognize a historical checksum mismatch only
when all of the following match an immutable adoption entry exactly:

1. version, name, recorded checksum, and adopted checksum;
2. the live `sqlite_master` fingerprint;
3. the enumerated object counts and divergence set;
4. the explicit authorization provenance.

Every other mismatch must continue to raise. This is not an environment flag,
temporary bypass, or weakened checksum check; it is an exact, auditable
transition from one recorded schema identity to another while preserving the
original history.

## Rollback procedure

The verified pre-change backup is:

```text
/Users/kolbydayley/pif-backups/factory-20260729T1527ET-before-schema-repair.sqlite
SHA-256: 424a15b02b2dbbc4f9f34665a63b2888229f3b71c536d35114203f1f812d70ed
Size: 4,052,508,672 bytes
Journal mode: DELETE
PRAGMA quick_check: ok
```

Rollback must be rehearsed on an isolated path before production authorization:

1. Stop all PIF writers only after explicit owner authorization.
2. Preserve the failed database and any `-wal`/`-shm` files as incident
   evidence; do not overwrite them.
3. Recompute the backup SHA-256 and require the exact value above.
4. Open the backup with `mode=ro`, enable `PRAGMA query_only=ON`, and require:
   `quick_check=ok`, matching schema fingerprint, and the expected critical
   row counts.
5. Restore through SQLite’s backup API from the verified backup into a new
   database file in the production directory. Do not use a naive `cp` over a
   potentially active WAL database.
6. Verify the restored candidate read-only: integrity, foreign-key result,
   schema fingerprint, object counts, and critical row counts.
7. Atomically exchange paths only while all writers remain stopped, retaining
   the failed file for recovery.
8. Reopen read-only and repeat all checks before allowing writers to resume.

Expected pre-change row counts:

| Table | Rows |
|---|---:|
| `episodes` | 28,297 |
| `transcripts` | 7,315 |
| `labels` | 7,539 |
| `jobs` | 109,355 |
| `atomic_claims` | 0 |

The backup faithfully carries the production database’s existing 44
`queue_envelopes → jobs` foreign-key violations. Their complete result sets
have the same SHA-256 in production and backup:
`e0166c46ff0b46cca98f42098282526645f6a7f3566387641fb972134b1b2419`.
They are inherited production state, not backup corruption, and must not be
silently repaired as part of this schema change.

## Required implementation gates

No production work is authorized by this proposal. Before implementation:

- verify a restore drill from the backup;
- test baseline adoption and migrations on the restored copy;
- prove an unknown checksum mismatch still hard-fails;
- prove the original version-1 row is unchanged;
- compare view columns and row results before and after;
- run application smoke tests against the migrated copy;
- obtain explicit production authorization.
