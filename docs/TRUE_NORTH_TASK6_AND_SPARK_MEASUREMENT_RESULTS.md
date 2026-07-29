# True North Task 6 and Spark Conjunction Measurement

Date: 2026-07-29  
Partition: Search development fold only  
Sealed holdouts opened: no  
Production mutation: no  
Stage-B prompt changed: no

## Outcome

Neither bounded measurement is acceptance-eligible.

- The Spark conjunction run validated 84 of the required 96 candidates before
  exhausting its 12-call ceiling. On those 84 candidates, Spark's acceptable
  atomic-count rate was **63.6364%**, exactly the same as frozen GLM on those
  candidates. The explicitly labeled fallback composition left overall
  atomic-count accuracy unchanged at **78.0172%**.
- The actor-value run validated 212 of 243 atomic decisions before the total
  campaign reached 160 calls. On the 211 candidates whose actor decisions were
  fully measured, emission accuracy was **53.2872%**, value accuracy
  conditional on a gold non-null actor was **36.3636%**, and composite
  reported-actor exactness was **48.0969%** against the **90.3182%** gate.

The two invalid batches were not normalized or silently accepted. In both
workstreams the provider returned output that violated a frozen harness
invariant:

- Spark used `edit_reason=none` for a non-verbatim claim.
- GLM emitted a reported actor that was not an exact literal substring of the
  atomic claim's evidence.

Identical-prompt replacements were charged. The final invalid attempts were
preserved, charged, and left outside semantic scoring.

## Workstream B: Spark on conjunction-dense candidates

Run: `task5-spark-conjunction-20260729-v1`

| Measurement | Result |
|---|---:|
| Required selector | claim text contains literal ` and ` or ` or ` |
| Required candidates | 96 |
| Decomposition-eligible candidates | 87 |
| Validated candidates | 84 |
| Validated eligible candidates | 77 |
| Coverage | 87.5000% |
| Charged Spark calls | 12 / 12 |
| Spark tokens | 169,236 |
| Validated-subset Spark atomic-count accuracy | **63.6364%** |
| Same-candidate frozen GLM accuracy | **63.6364%** |

The requested full-96 measurement does not exist because 12 candidates are in
the invalid terminal batch. The following composition is diagnostic only: it
uses validated Spark output where available and frozen single-pass GLM output
for the missing batch and every unselected candidate.

| Metric | Frozen GLM | Partial Spark + GLM fallback | Change |
|---|---:|---:|---:|
| Atomic-count accuracy | 78.0172% | 78.0172% | 0.0000 points |
| Speaker exactness | 68.2493% | 68.4524% | +0.2031 points |
| Claim-text faithfulness | 54.7319% | 54.7205% | -0.0114 points |

This measured subset provides no evidence that routing conjunction-dense
candidates to Spark improves atomic decomposition. It directly contradicts the
92.58% perfect-Spark projection; that projection remains a hypothetical, not a
result.

### Cost

| Route | Calls | Tokens |
|---|---:|---:|
| Frozen all-GLM decomposition baseline | 21 | 242,878 |
| Added Spark measurement | 12 | 169,236 |
| Measured mixed total | 33 | 412,114 |

- Mixed versus all-GLM: **1.571429× calls**, **1.696794× tokens**.
- Spark calls versus an all-Spark 21-call packet baseline: **0.571429×**.
- Total mixed calls versus that 21-call baseline: **1.571429×**.

No all-Spark token or dollar ratio is claimed because there is no complete
all-Spark run or common metered price basis.

## Workstream A: actor value

Run: `task6-actor-value-20260729-v1`

The model was required to return either `null` or an exact literal substring
of the atomic claim's `evidence_text`. The candidate prior was shown as
non-binding context and was never adopted automatically.

| Measurement | Result |
|---|---:|
| Required atomic decisions | 243 |
| Validated atomic decisions | 212 |
| Fully measured candidates | 211 |
| Coverage | 87.2428% |
| Charged GLM calls | 9 / 9 effective remaining campaign budget |
| GLM tokens | 192,274 |
| Emission accuracy, fully measured subset | **53.2872%** |
| Actor-value accuracy given gold non-null | **36.3636%** |
| Composite reported-actor exactness, measured subset | **48.0969%** |
| Gate | **90.3182%** |
| Hallucination proxy, measured subset | **15.6863%** |

For transparency, a second diagnostic uses validated actor decisions where
available and the frozen source prior only for the missing batch:

| Measurement | Result |
|---|---:|
| Composite reported-actor exactness | 43.9169% |
| Gate pass | no |
| Hallucination proxy before | 59.2742% |
| Hallucination proxy after | 19.3548% |
| Hallucination change | -39.9194 points |

The large hallucination reduction does not rescue the stage: its strong null
bias also misses actors and chooses the wrong actor value often enough that
composite exactness remains less than half the required gate. The corrected
value-selection contract is implemented, but this GLM route does not meet it.

## Exact 160-call campaign audit

| Campaign component | Calls |
|---|---:|
| Phase B reported-actor gold repair | 45 |
| Phase C disposition and junk work | 51 |
| Frozen Task 5 single-pass decomposition | 21 |
| Retired dual-pass decomposition measurement | 22 |
| Spark conjunction measurement | 12 |
| Task 6 actor-value measurement | 9 |
| **Total** | **160 / 160** |

The audit counts every preserved invalid provider attempt. No further paid
calls are authorized inside this campaign.

## Decision

The proposed conjunction selector should not be adopted: at 87.5% measured
coverage Spark tied GLM on the selected candidates and did not move aggregate
atomicity. The present actor-value GLM stage should not be adopted either: the
fully measured subset is far below its gate.

These are development results only. The sealed transfer episodes remain
unopened, and neither partial run authorizes a production promotion.
