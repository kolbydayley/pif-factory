# True-North Task 5: Split/No-Split Minimal-Edit Decomposition

## Outcome

Task 5 **failed** the frozen Search-fold acceptance contract. The run was
mechanically clean, but its decomposition quality was below every primary
quality threshold and seven of the nine Task 1 floor checks regressed.

No sealed holdout was opened and no production database was mutated.

## Frozen run

- Run ID: `task5-adjudication-20260729-v1`
- Model: `zai-coding-plan/glm-5.2`
- Stage: adjudication only
- Search episodes: 2
- Candidates: 256
- Packets/calls: 21/21
- Tokens: 242,878
- Wall time: 529.854 seconds
- Configuration SHA-256:
  `467de9546e5c9d14a7e03a5b1084b89c7c1daace27fdbda44c58669307c980b6`
- Score SHA-256:
  `263d8510fcc6417cc3b8e5dfae1bb5d29e602e76bdd450298360dd04f3db6763`
- Holdout access allowed: false
- Production database access allowed: false

The runner consumed the certified Phase C development dispositions and used
the plan's verbatim Stage-B system prompt and closed output schema. It did not
run attribution or any other semantic model stage. Frozen candidate speaker,
actor, and enum priors were carried forward so that the measured change was
attributable to decomposition.

## Primary acceptance

| Gate | Observed | Required | Result |
|---|---:|---:|---|
| Atomic-count accuracy | 78.02% | at least 90% | FAIL |
| Claim-text faithfulness | 54.73% | at least 75% | FAIL |
| Hallucination proxy | 59.27% | at most 2% | FAIL |
| No gate below Task 1 floor | 7 of 9 regressed | all protected | FAIL |

The hallucination measure is the benchmark's existing unsupported-candidate
proxy. It penalizes unmatched or insufficiently faithful atomics as well as
unsupported additions; it must not be read as a manual finding that 59.27% of
sentences invented external facts.

## Exact Task 1 comparison cohort

The comparison uses the exact frozen 60-candidate cohort and the approved
Task 1 rescore, whose report SHA-256 is
`9e75212bff7d5074f571ee2d2c97c3368ed604da1a02273b968270bcae8aba3d`.

| Protected metric | Task 5 | Task 1 floor | Result |
|---|---:|---:|---|
| Candidate-state macro F1 | 0.929682 | 0.976561 | FAIL |
| Retained-value recall | 0.976744 | 1.000000 | FAIL |
| Junk escape rate | 0.222222 | 0.111111 | FAIL |
| Atomic-count accuracy | 0.571429 | 0.581395 | FAIL |
| Claim-text faithfulness | 0.380567 | 0.396761 | FAIL |
| Speaker exactness | 0.482353 | 0.500000 | FAIL |
| Reported-actor exactness | 0.141176 | 0.142857 | FAIL |
| Hallucination proxy | 0.519231 | 0.519231 | PASS, unchanged |
| Schema parse success | 1.000000 | 1.000000 | PASS, unchanged |

## Interpretation

The failed result is not a transport or schema incident: all 21 packets
validated, there were no retries, and schema success was 100%. Under the
frozen contract, GLM 5.2 did not reliably preserve the candidate hypothesis
while making only the necessary split or minimal repair. The experiment is
therefore a quality failure, not an operational failure.

This result does not authorize prompt tuning, a second Search-fold attempt, or
opening the sealed transfer episodes. Any next design must be separately
approved and must preserve this run as the frozen Task 5 result.
