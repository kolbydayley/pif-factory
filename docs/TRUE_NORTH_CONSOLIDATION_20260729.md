# True-North Phase A Consolidation — 2026-07-29

## Certification anchor

- Annotated tag: `v7-certified`
- Target commit: `f3344bb7258e5c97ee74158d4b37b860b137f606`
- Certification document:
  `docs/TRUE_NORTH_CERTIFICATION_20260729.md`
- Certification document SHA-256:
  `c9196e80308090339f0c7c179f61b3bdd2186aac1c5e1aceac1100224f65999d`

The tag was created before residue consolidation, exactly as directed. The
post-tag commits preserve historical source, tests, and experiment contracts;
they do not move the certified baseline.

## Worktree triage

The initial inventory contained 1,423 dirty entries.

| Class | Count | Treatment |
|---|---:|---|
| Belongs in reproducible history | 1,405 | Committed in three coherent groups |
| Generated/private local state | 4 | Preserved locally and added to `.gitignore` |
| Dead duplicate residue | 14 | Deleted after canonical-file comparison |

The 1,405 history entries were committed as:

| Commit | Files | Scope |
|---|---:|---|
| `6b5f59f` | 5 | True-North benchmark and input-optimization support |
| `54523bd` | 135 | Factory core, local operations, docs, and tests |
| `0d3f5f8` | 1,265 | Codex app-server protocol and experiment history |

The four local-only entries are:

- `automation/pif-native-goal-supervisor-runtime`
- `automation/pif-watchdog-runtime`
- `docs/TRUE_NORTH_ACTOR_SHADOW_20260728.md`
- `docs/TRUE_NORTH_OPUS_SHADOW_20260728.md`

The first two are reproducible build products. The diagnostic drafts contain
development-gold IDs and evidence excerpts, so they remain local under the
repository privacy contract. The 14 deleted entries were Finder-style
`* 2.*` label-pack backups; canonical label-pack files were newer, and the two
byte-identical copies added no history.

No private keys, bearer credentials, API secrets, OpenAI-style keys, raw
transcripts, or development-gold candidate IDs were found in the committed
diffs. The two local diagnostic drafts were deliberately excluded rather than
sanitized into a misleading public artifact.

## Reproducibility evidence

A detached temporary worktree was created from `v7-certified`, independent of
all consolidation commits:

- checked-out commit:
  `f3344bb7258e5c97ee74158d4b37b860b137f606`
- certification document hash: exact match
- `python3 -m pytest -q tests/test_true_north*.py`:
  **236 passed**

The consolidated current tree also:

- compiles all `research_factory` Python modules;
- collects **2,988 tests** without collection error;
- passes a representative 70-test app-server runtime, lock, provenance,
  evaluation, and unattended-supervisor slice;
- passes all 11 tests for the newly preserved input-variant history.

The broader core/operations slice is not represented as green: **373 passed
and 17 failed**. The failures are pre-existing cross-version drift in four
areas: transcript snapshot metrics, schema-migration version expectations,
production release/current-view fixtures, and semantic-reconciliation
release fixtures. They do not affect the detached certified-tag result, but
they remain technical debt and must not be described as a passing full suite.

## Reproduction

```bash
git checkout v7-certified
shasum -a 256 docs/TRUE_NORTH_CERTIFICATION_20260729.md
python3 -m pytest -q tests/test_true_north*.py
```

Expected document hash:

```text
c9196e80308090339f0c7c179f61b3bdd2186aac1c5e1aceac1100224f65999d
```

The private `runs/`, corpus, SQLite stores, and gold artifacts remain outside
Git. Certification is reproduced from the hash-bound report, manifest,
contract, scoring code, and regression surface without publishing those
private inputs.
