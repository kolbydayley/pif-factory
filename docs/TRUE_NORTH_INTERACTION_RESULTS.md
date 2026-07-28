# True-North Combined-Stack Results

Campaign: `tnio-20260728-combined-stack-v1`

This development-only interaction experiment tested whether the strongest
isolated GLM 5.2 input improvements compound when combined. Production and
sealed-transfer scoring remained disabled.

## Result

No combined stack passed the hard gates or qualified as a Pareto survivor.
The strongest compatible stack improved several decision metrics, but its gains
did not compose across atomicity and identity.

| Stack | Candidate F1 | Retained recall | Junk escape | Atomic count | Faithfulness | Speaker | Reported actor | Unsupported |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Baseline | 0.930 | 0.977 | 0.222 | 0.690 | 0.519 | 0.616 | 0.419 | 0.846 |
| System + schema | 0.951 | 1.000 | 0.222 | 0.674 | 0.538 | 0.632 | 0.425 | 0.865 |
| + speaker context | 0.955 | 0.977 | 0.111 | 0.643 | 0.442 | 0.548 | 0.381 | 0.827 |
| + candidate-prior order | **0.977** | **1.000** | **0.111** | 0.651 | **0.538** | **0.647** | 0.388 | 0.846 |
| Literal maximal union | 0.763 | **1.000** | 0.778 | 0.651 | 0.466 | 0.571 | **0.440** | 0.962 |

Lower is better for junk escape and unsupported rate. All other columns are
higher-is-better.

## Best compatible stack versus baseline

- Candidate-state macro F1: +4.7 percentage points.
- Retained-value recall: +2.3 points.
- Junk escape: -11.1 points.
- Claim faithfulness: +1.9 points.
- Speaker exactness: +3.1 points.
- Atomic-count accuracy: -3.9 points.
- Reported-actor exactness: -3.0 points.
- Unsupported-candidate rate: unchanged.

The literal maximal union demonstrates why isolated best deltas cannot simply
be added: the task-rule rewrite drove junk escape from 22.2% to 77.8% and the
unsupported rate from 84.6% to 96.2%.

## Operational accounting

- Ten development packets completed.
- Twelve calls were conservatively accounted.
- One response truncated and succeeded on one bounded retry.
- One computer-restart-interrupted call was charged at the full 64,000-token
  and 360-second reservation ceiling.
- Total accounted usage: 660,777 tokens and 2,484.153 seconds.
- Focused regression suite: 101 tests passed.

## Conclusion

The broad gains are real but non-additive. Further single-pass prompt stacking
is not the likely path to the gates. The next controlled experiment should
separate candidate disposition, atomic decomposition, and identity resolution
into bounded passes so each task has a smaller semantic and schema surface.
