# Canonical Claim Subject Full Migration

## Inventory

Live baseline captured before this migration:

| Surface | Current count / state | Migration decision |
| --- | ---: | --- |
| `claims` | 27,601 rows | Primary raw proposition source. |
| Claim Subject assigned claims | 26,348 rows | Keep as the primary canonical layer. |
| Assigned `ai_discourse_v1` claims | 2,459 rows | Migrated through conservative legacy adapter. |
| Assigned `ai_discourse_v3_1` claims | 23,889 rows | Current production claim source. |
| Quarantined `ai_discourse_v1` | 263 rows | Generic/template rows; do not surface. |
| Quarantined `ai_discourse_v2` | 117 rows | Fragment format; re-extract only. |
| Quarantined `ai_discourse_v3` | 838 rows | Template format; re-extract only. |
| Quarantined `ai_discourse_v3_1` | 35 rows | Current-pack templates; keep quarantined. |
| `discourse_events` | 34,749 rows | Useful non-claim events attach to Claim Subjects as event observations. |
| v3.1 event-only claim text rows | 9,781 rows | Only frame/term/uncertainty events migrate; actor/entity mentions stay out. |
| `actor_positions` | 34,749 rows | Old actor/concept stance layer; replace primary exports/views with Claim Subject expert positions. |
| `raw_speaker_mentions` | 33,642 rows | Identity evidence for expert positions. |
| Resolved speaker mentions | 25,972 rows | Prefer `canonical_person_id` in expert rollups. |
| Unresolved speaker mentions | 7,670 rows | Use sanitized speaker surface fallback. |
| `claim_clusters` / `claim_cluster_members` | Compatibility layer | Keep for debug only; not the primary trend model. |
| Old observer keys | `canonical_claim_coverage`, `canonical_claim_samples`, `claim_timeline`, `stance_mix` | Move under compatibility/debug or remove from primary UI. |
| Old export | `actor-stance-report` | Alias to Claim Subject expert stance report. |

## Migration Decisions

- Claim Subjects are the primary canonical object. Concepts remain navigation/filter metadata.
- Proposition variants are created only from real `claims` rows, never from template-like term/frame event text.
- Useful event-only rows are subject-level evidence, not claims. Current accepted event-only types are `frame_usage`, `term_usage`, and `uncertainty`.
- `actor_mention` and `entity_reference` event-only rows are not subject observations; they remain identity/entity graph evidence.
- `ai_discourse_v2` and `ai_discourse_v3` are re-extract-only legacy formats.
- Railway remains observer and broker only. All migration, judging, and backfill work stays local.

## Implementation Checklist

- Add durable `claim_subject_event_observations` and `claim_subject_expert_positions` tables.
- Extend Claim Subject normalization so each run rebuilds claim memberships, proposition variants, claim observations, event observations, and expert-position rollups idempotently.
- Update observer metrics so Claim Subject coverage includes event observations, expert positions, canonical-person coverage, unresolved speaker fallback counts, and quarantined legacy counts.
- Remove old canonical-claim cluster widgets from the primary Railway UI. If retained, expose them only as compatibility/debug metrics.
- Replace actor/concept stance exports with Claim Subject expert stance exports while preserving CLI compatibility.
- Run identity grooming and Claim Subject normalization before observer snapshot publishing.

## Verification Commands

```bash
python3 -m research_factory railway-cost-guard --allowed-service mcp-broker --allowed-service Postgres
PYTHONDONTWRITEBYTECODE=1 python3 -B -m research_factory canonicalize-claims --scope all --mode semantic
PYTHONDONTWRITEBYTECODE=1 /tmp/pif-pytest-venv/bin/python -B -m pytest -p no:cacheprovider tests/test_factory.py
PYTHONDONTWRITEBYTECODE=1 python3 -B -m research_factory snapshot --output exports/observer-snapshot.json
PYTHONDONTWRITEBYTECODE=1 python3 -B -m research_factory privacy-scan --path exports/observer-snapshot.json
PYTHONDONTWRITEBYTECODE=1 python3 -B -m research_factory publish-snapshot --snapshot exports/observer-snapshot.json --url https://observer-ui-production.up.railway.app --token-file .railway-ingest-token.local
curl -fsS https://observer-ui-production.up.railway.app/state.json
```

## Post-Migration Acceptance Snapshot

- Full all-scope semantic backfill: 26,348 assigned claims, 25,692 Claim Subjects, 26,070 proposition variants, 6,283 event observations, and 30,752 expert positions.
- Event observations accepted: 3,853 `frame_usage`, 1,666 `term_usage`, and 764 `uncertainty`.
- Event-only rows quarantined: 2,107 `actor_mention` and 1,391 `entity_reference`.
- Expert position resolution: 21,847 canonical-person rows and 8,905 sanitized speaker-fallback rows in the published observer snapshot.
- Remaining unmigrated claim rows are expected quarantine/re-extract categories: `ai_discourse_v1=263`, `ai_discourse_v2=117`, `ai_discourse_v3=838`, `ai_discourse_v3_1=35`.
- Orphan checks must report zero for subject members, variants, claim observations, event observations, expert positions, and unreferenced subjects.

## Done Means

- Full tests pass.
- Live backfill can be run twice without changing counts unexpectedly or creating duplicates.
- No orphan Claim Subjects, proposition variants, claim observations, event observations, or expert positions remain.
- `claim_subject_coverage` is the primary observer coverage object and reports assigned claims, event observations, expert positions, canonical-person coverage, unresolved speaker fallbacks, and quarantined/re-extract-only counts.
- Old canonical claim cluster metrics are absent from the primary Trends/Claims UI or clearly demoted to compatibility/debug.
- `actor-stance-report` produces a Claim Subject expert stance report.
- Observer time series use `episodes.published_at`, not parse or label creation time.
- Published observer snapshot and generated public reports contain no raw transcripts, local paths, prompt text, long excerpts, secrets, or output JSON.
- Railway cost guard returns `ok: true` with only `observer-ui`, `mcp-broker`, and managed `Postgres`.
- Browser smoke confirms Railway Trends/Claims are Claim Subject first.
