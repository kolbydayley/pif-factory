# True-North Dual-Pass Decomposition Result

## Outcome

The authorized dual-pass experiment completed but did not improve atomic
decomposition. The two GLM passes produced the same atomic count for every
decomposition-eligible candidate, including the candidates on which that
shared count was wrong. Count disagreement therefore provided no escalation
signal, Spark received zero calls, and the deterministic composition preserved
the frozen Task 5 output.

The frozen Stage-B prompt and packets were unchanged. No sealed holdout or
production database was opened.

## Atomicity first

| Metric | Frozen single pass | Dual pass | Gate |
|---|---:|---:|---:|
| Acceptable atomic-count accuracy | 78.0172% | **78.0172%** | 90% |

The gate remains fair and unchanged. The result demonstrates correlated,
systematic GLM counting errors rather than stochastic uncertainty detectable
through repeated count disagreement.

## Alignment-bound and actor diagnostics

| Metric | Frozen single pass | Dual pass |
|---|---:|---:|
| Speaker exactness | 68.2493% | 68.2493% |
| Claim-text faithfulness | 54.7319% | 54.7319% |
| Reported-actor exactness | 18.6944% | 18.6944% |
| Hallucination proxy | 59.2742% | 59.2742% |

Speaker and faithfulness cannot recover without better atomic alignment.
Reported actor and the actor-driven hallucination proxy remain a separately
measured problem; they are not attributed to decomposition here.

## Agreement and escalation

- Search candidates: 256
- Decomposition-eligible candidates: 241
- Same-count candidates: 241
- Count-disagreement candidates: 0
- Realized escalation rate: 0/256 = 0.00%
- Eligible-candidate escalation rate: 0/241 = 0.00%
- Escalation cap: 64 candidates = 25% of the Search fold
- Spark calls: 0
- Overflow: none

On count agreement, composition used the frozen first-pass decomposition. On
count disagreement, Spark would have decided using the same candidate
projection and Stage-B contract; that branch was never entered.

## Usage and cost

| Measurement | Result |
|---|---:|
| Successful second-pass GLM packets | 21 |
| Charged GLM attempts | 22 |
| Invalid/empty GLM attempts | 1 |
| Spark escalation calls | 0 |
| Total new calls | 22 |
| Total new tokens | 257,604 |
| Provider wall time | 527.323 seconds |
| Declared ceiling | 30 calls / 400,000 tokens |

One GLM attempt returned an empty/non-JSON answer. It was preserved and charged
before an identical-prompt replacement call. There were no prompt changes or
semantic retries hidden as provider fallback.

An all-Spark execution would expose 21 packets to Spark. This experiment used
0/21 frontier calls, an effective frontier-call ratio of **0%**, and avoided
21 Spark calls. Counting all subsidized GLM attempts, including the failed
attempt, total invocation count was 22/21 = 104.76% of the all-Spark packet
count. No dollar equivalence is asserted between subscription GLM calls and
Spark calls.

## Reproducibility

- Run ID: `task5-dual-decomposition-20260729-v1`
- Configuration SHA-256:
  `3cc6780ae089e37f39b07f60e822ed2878f86fd5d819ba62cafee2437adfef73`
- Result SHA-256:
  `4fde116c5036fbdb9e5cc52ea5f161371191e22efb4e5dc1ab19bd4c157c4197`
- Score SHA-256:
  `7b6a2b1a040392041674939e5cc93ee7dec370d3eb40f699e8d6ab9ff03a6a41`
- Prompt SHA-256:
  `263f45b6e05aaad03f5d6e14bc91affbc82d2b229b63e59f1863b536870a12f2`
- Holdout opened: false
- Production mutation: false
