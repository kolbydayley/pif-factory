# True North Zero-Call Prior-Adoption Floor

Date: 2026-07-28

Suite: `ai-safety-v1`

Source variant: `stack-03-all-compatible`

Comparison cohort: 60 candidates from both Search episodes

Provider calls: **0**

## Result

The deterministic candidate-prior adapter is **not a gate-closing solution by
itself**. It passes 3 of 9 gates on the exact 60-candidate comparison cohort.
It preserves the stored workhorse's excellent candidate-value decisions, but
the frozen candidate priors are not sufficient to reconstruct the adjudicated
atomic propositions or reported actors.

The result does establish a useful floor:

- Candidate-value state is nearly solved before a new semantic call:
  consensus-state macro F1 is 0.9766 and retained-value recall is 1.0000.
- One adopted prior per retained candidate is insufficient for decomposition:
  acceptable atomic-count rate is 0.5814.
- Candidate claim wording is more grounded than free generation but is not
  equivalent to the independently adjudicated atomic wording:
  faithfulness proxy is 0.3968.
- Candidate speaker priors are usually right for the atom they describe, but
  the current exactness metric also charges missing gold atomics to attribution:
  aggregate speaker exactness is 0.5000.
- The specified `reported_actor.name or actor_name` fallback is not safe:
  reported-actor exactness is 0.2500.
- One stored junk miss remains, producing an 11.11% junk escape rate.
- The adapter is structurally valid and deterministic: schema success is
  1.0000, prediction artifacts are immutable and hash-bound, and there were no
  provider calls or tokens.

## Exact gate table

| Gate | Target | Prior floor | Stored GLM stack | Prior pass |
|---|---:|---:|---:|:---:|
| Consensus candidate-state macro F1 | ≥0.90 | 0.9766 | 0.9766 | Yes |
| Retained-value recall | ≥0.90 | 1.0000 | 1.0000 | Yes |
| Consensus junk escape | ≤0.02 | 0.1111 | 0.1111 | No |
| Acceptable atomic count | ≥0.90 | 0.5814 | 0.6512 | No |
| Claim-text faithfulness proxy | ≥0.90 | 0.3968 | 0.5380 | No |
| Speaker exactness | ≥0.97 | 0.5000 | 0.6471 | No |
| Reported-actor exactness | ≥0.95 | 0.2500 | 0.3882 | No |
| Hallucination rate proxy | ≤0.02 | 0.2115 | 0.0385 | No |
| Schema parse success | ≥0.99 | 1.0000 | 1.0000 | Yes |

The stored GLM stack column is the same 60 candidates re-scored with instrument
v2. Its semantic generation improves atomic count, wording, speaker, actor, and
hallucination metrics over raw prior adoption, although none of those gates
closes.

## Per-episode diagnostic

The source campaign contains a stratified 60-candidate cohort, not all 256
Search candidates. “Both Search episodes” is therefore reported as the two
episode slices of that exact frozen cohort; no missing disposition was inferred
from gold.

| Search episode | Cohort candidates | Strictly scoreable | Gates passed | State F1 | Value recall | Atomic count | Faithfulness | Speaker | Actor | Hallucination |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Decoder (`ep_7ec…`) | 44 | 36 | 3/9 | 0.9743 | 1.0000 | 0.4444 | 0.3254 | 0.4333 | 0.2167 | 0.2222 |
| Big Technology (`ep_90c…`) | 16 | 16 | 4/9 | 1.0000 | 1.0000 | 0.8125 | 0.6071 | 0.6667 | 0.3333 | 0.1875 |
| Combined exact cohort | 60 | 52 | 3/9 | 0.9766 | 1.0000 | 0.5814 | 0.3968 | 0.5000 | 0.2500 | 0.2115 |

## Interpretation for the v2 architecture

The result supports the plan's narrowed-model premise, with one qualification.
New model work should focus on the decisions the priors cannot supply:
split/no-split decomposition, minimal evidence-faithful atomic wording, and
actor repair. However, disposition cannot be removed entirely yet because the
one gold-junk escape is above the hard safety target.

The result also confirms that a candidate-prior adapter should not be evaluated
as though every gold atomic already existed. Missing atomics suppress speaker
and actor exactness even where the adopted prior's single speaker is correct.
That is a measurement-calibration issue to quantify in Task 2, not permission
to loosen the live gate in this task.

## Reproducibility

- Prediction artifact:
  `~/Library/Application Support/Podcast Intelligence Factory/true-north/ai-safety-v1/prior-adoption/pif_true_north_prior_adoption_v1/tnpa-20260728-stack03-v1/predictions.private.json`
- Private score:
  `~/Library/Application Support/Podcast Intelligence Factory/true-north/ai-safety-v1/prior-adoption/pif_true_north_prior_adoption_v1/tnpa-20260728-stack03-v1/score.private.json`
- Prediction SHA-256:
  `b07ccd76fd22f4e1aa49887625d4cdd707e45c6f47c13207be87a6aeae8894b0`
- Report SHA-256:
  `2933826c5c4920c463934f57fc8ed0fa21723cc85e02d0042364ffcfb50a9498`
- Focused regression suite: 99 passed.

Gold was loaded only after the prediction artifact was written immutably and
hashed. The composer has no gold input.
