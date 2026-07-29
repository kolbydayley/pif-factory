# True North Two-Stage Actor Result

Date: 2026-07-29  
Run: `task6-actor-two-stage-20260729-v1`  
Source atomics: frozen Task-5/dual-pass output  
Sealed holdouts opened: no  
Production mutation: no

## Null floor

Always-null was measured before any model calls with the exact current Search
scorer:

| Measurement | Result |
|---|---:|
| Repaired-gold global null prevalence | 69.8217% |
| Exact Search scorer always-null composite | **43.3234%** |
| Always-null hallucination proxy | 2.4194% |

The expected 69.82% is a property of the repaired gold population. It is not
the executable composite floor on the current Search predictions because the
exact scorer also charges atomic-count and alignment misses to
`reported_actor`. The honest design floor is therefore 43.3234%.

## Experiment result

Stage 1 completed all 243 binary emit/null decisions. It marked 76 claims for
value selection.

Stage 2 validated exact evidence-substring values for 60 claims. Two of eight
value batches, containing 16 emitted claims, violated the literal-span
invariant on three consecutive identical-prompt attempts. Rather than spend
the remaining seven calls on a reproducible failure, the run stopped and
accounted every paid attempt.

| Measurement | Result |
|---|---:|
| Stage-1 emission accuracy | **50.4451%** |
| Stage-1 correct / denominator | 170 / 337 |
| Value accuracy across all gold non-null actors | **27.2727%** |
| Value accuracy where a validated value aligned to gold non-null | **89.1892%** |
| Validated value correct / aligned denominator | 33 / 37 |
| Partial null-fallback composite | **48.0712%** |
| Exact always-null floor | 43.3234% |
| Reported-actor gate | 90.3182% |
| Partial hallucination proxy | 9.2742% |

The split diagnosis is now clear:

- **Emission is the limiting half.** At 50.45%, the binary stage is much too
  weak for a null-dominant field.
- **Value selection is not the primary blocker when it actually runs.** The
  validated conditional value result is 89.19%, well above the stale prior's
  64.32%, although it covers only 37 aligned gold-positive cases.
- The partial composition beats the exact null floor by 4.75 points but remains
  42.25 points below the gate and is not acceptance-eligible.

The missing Stage-2 batches use null only in the explicitly labeled diagnostic;
they are not silently treated as successful value decisions.

## Spend

- Experiment ceiling: 30 calls / 400,000 tokens.
- Actual: **23 calls / 261,654 tokens**.
- Unused ceiling: 7 calls, deliberately not spent on unchanged retries.
- Cumulative campaign calls: **208**.
- Known cumulative tokens: **1,688,964**, excluding the earlier 45-call actor
  gold-repair stage whose ledger did not record tokens.

No further actor iteration is authorized by this result itself. A subsequent
design should target emission classification specifically and requires review
before new calls.
