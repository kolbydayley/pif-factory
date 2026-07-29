# T2 Segment-Overlap Dedup — REFUTED as a suppression rule

Date: 2026-07-29
Zero model calls. Development partition only. No gate, gold, scorer, or scored
run modified. Reported to the Codex review loop through this document.

## Verdict

**Do not implement T2 as a junk-suppression rule.** The segment-overlap signal
is uncorrelated with junk. Implementing the recommendation as written
(`docs/TRUE_NORTH_OPUS_SHADOW_20260728.md`, T2) would have cost substantial
recall to remove a handful of junk candidates.

The module (`research_factory/true_north_segment_overlap.py`) is retained as a
**corpus diagnostic** and carries this refutation in its docstring.

## What T2 claimed

> Both episodes are chunked into *overlapping* segments, so the same utterance
> yields a complete copy and a boundary-truncated copy. This catches
> `dev_fcec8903…` and `dev_d7f6bd87…` — the two both-run escapes that stopped
> Task 4b — with zero model involvement.

The catch claim is **true**. The implied safety was never measured.

## What the measurement shows

Detector: pairs in *different* segments with adjacent `segment_index`, matched
by case/whitespace-normalized evidence containment, complete copy kept.

| Episode | Candidates | Duplicates found |
|---|---:|---:|
| ep_7ec9f808… | 208 | 20 |
| ep_8a0c6919… | 358 | 21 |
| ep_903cc763… | 173 | 9 |
| ep_90c3b5c9… | 48 | 0 |
| ep_e7630540… | 353 | 23 |
| **Total** | **1,140** | **73** |

Both named escapes are caught: `dev_d7f6bd87…` as `boundary_truncated` against
`dev_35f502f3…`, and `dev_fcec8903…` as `exact_duplicate` against
`dev_e500a0af…`.

### Precision against repaired gold

| Flagged relation | gold retain | gold revise | gold hold | gold reject |
|---|---:|---:|---:|---:|
| `boundary_truncated` | 29 | 9 | 1 | 5 |
| `exact_duplicate` | 7 | 6 | 1 | 1 |
| `contained` | 9 | 5 | 0 | 0 |

**Precision: 6/73 = 8.2%.** Excluding the weak `contained` relation: 6/59 =
10.2%. Suppression would discard **65 gold-value candidates** to remove 6 junk
ones.

### The obvious tightening makes it strictly worse

Requiring the duplicate and its keeper to also share claim text:

| Claim-text Jaccard ≥ | Flagged | Gold-reject | Precision |
|---:|---:|---:|---:|
| 0.0 | 73 | 6 | 8.2% |
| 0.5 | 23 | 0 | **0.0%** |
| 0.7 | 11 | 0 | 0.0% |
| 0.9 | 3 | 0 | 0.0% |

Every gold-reject has claim text that **differs** from its keeper (Jaccard
< 0.5). The three near-identical-claim pairs are all gold-value (2 revise,
1 retain).

## Why the signal fails

Overlapping segments legitimately produce **different propositions** from the
same evidence span — that is the extractor working correctly, not duplication.
Shared evidence is not shared meaning, so evidence containment cannot decide
junk. And where the claim genuinely *is* near-identical, gold still keeps it,
which is consistent with duplicate suppression belonging downstream in
canonicalization rather than at disposition.

This is the same lesson Task 4c learned semantically (relational junk needs
neighbour context), reached here structurally: the two Task 4b escapes are
*relational*, and their relationality is not recoverable from segment geometry.

## What survives

- **A corpus statistic**: 73/1,140 candidates (6.4%) are boundary duplicates.
  Useful for reasoning about canonicalization load; not a junk signal.
- **A refuted hypothesis, cheaply**: 12 tests and one afternoon of deterministic
  measurement, no paid quota, before anyone built a suppression stage on it.
- T2's secondary suggestion — "fix the chunker upstream, it costs recall" —
  is also weakened: the overlap is generating distinct, gold-valued claims, so
  removing it may cost recall rather than restore it. Not investigated further.

## Reproducing

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -B -m pytest tests/test_true_north_segment_overlap.py -q
```

12 tests. Precision figures come from
`gold/development/final/gold.private.json` (`gold_sha256 02c06a87…7e6221`)
joined to `analyze_segment_overlap` output over
`bundles/development/*.json`.
