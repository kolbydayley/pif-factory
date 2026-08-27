"""Statistical core for the Signal Desk discourse detectors. Pure stdlib.

Every detector in pif_discourse_aggregates.py routes through these tests so
that a firing carries a p-value, an effect size with uncertainty, and a
confidence tier instead of a bare threshold crossing. The direct target is
the failure mode Kolby flagged 2026-08-27: raw-count spikes caused by weekly
corpus-coverage swings (not real trends) firing detectors. Rate tests here
condition on exposure (labeled-mention week totals), which normalizes those
swings away.

Subject-blind by construction: nothing in this module knows what a topic is.

Effect floors below are provisional engineering values pending the Phase-2
golden-dataset backtest (`scripts/pif_backtest.py --sweep`), which owns
tuning them; each constant records its provenance.
"""
from __future__ import annotations

import math
import random
from typing import Dict, List, Sequence, Tuple

# ---- detector policy constants (provisional until backtest --sweep) -------
# Chosen 2026-08-27 from inspection of the live corpus (week totals 544-4634,
# per-topic pulse volumes 0-60); to be replaced by calibrated values.
EMERGING_RATE_RATIO_FLOOR = 3.0   # pulse rate must be >= 3x baseline rate
FADING_RATE_RATIO_CEIL = 0.35     # pulse rate must be <= 0.35x baseline rate
SHIFT_TV_FLOOR = 0.25             # minimum stance total-variation distance
CONTESTED_BALANCE_FLOOR = 0.5     # min(pos,neg)/max(pos,neg)
ALPHA_STRONG = 0.01
ALPHA_MODERATE = 0.05
FDR_ALPHA = 0.10                  # Benjamini-Hochberg gate for "strong"
PERMUTATION_SEED = 20260827       # fixed: nightly builds must be reproducible


def wilson_interval(k: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    """Wilson score interval for a binomial proportion. (0,1) when n == 0."""
    if n <= 0:
        return (0.0, 1.0)
    p = k / n
    denom = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))


def _log_binom_pmf(x: int, n: int, p: float) -> float:
    return (math.lgamma(n + 1) - math.lgamma(x + 1) - math.lgamma(n - x + 1)
            + x * math.log(p) + (n - x) * math.log1p(-p))


def _binom_sf(k: int, n: int, p: float) -> float:
    """P(X >= k) for X ~ Binomial(n, p), exact via log-space summation."""
    if k <= 0:
        return 1.0
    if k > n:
        return 0.0
    if p <= 0.0:
        return 0.0
    if p >= 1.0:
        return 1.0
    # Sum the smaller tail for numerical thrift.
    if k > n * p:
        total = sum(math.exp(_log_binom_pmf(x, n, p)) for x in range(k, n + 1))
        return min(1.0, total)
    total = sum(math.exp(_log_binom_pmf(x, n, p)) for x in range(0, k))
    return min(1.0, max(0.0, 1.0 - total))


def _binom_cdf(k: int, n: int, p: float) -> float:
    """P(X <= k)."""
    return 1.0 - _binom_sf(k + 1, n, p)


def _rate_ratio_with_ci(pulse_count: int, pulse_exposure: float,
                        base_count: int, base_exposure: float,
                        z: float = 1.96) -> Tuple[float, Tuple[float, float]]:
    # 0.5 continuity adjustment only where a zero would blow up the ratio.
    kp = pulse_count if pulse_count > 0 else 0.5
    kb = base_count if base_count > 0 else 0.5
    ratio = (kp / max(pulse_exposure, 1e-9)) / (kb / max(base_exposure, 1e-9))
    se = math.sqrt(1.0 / kp + 1.0 / kb)
    lo = ratio * math.exp(-z * se)
    hi = ratio * math.exp(z * se)
    return ratio, (lo, hi)


def poisson_rate_test(pulse_count: int, pulse_exposure: float,
                      base_count: int, base_exposure: float) -> Dict:
    """One-sided test that the pulse rate EXCEEDS the baseline rate.

    Exposure is labeled-mention volume (week totals), so a week where the
    corpus simply covered twice as much ground does not read as a surge.
    Exact conditional binomial: under H0 (equal rates),
    pulse_count | total ~ Binomial(total, pulse_exposure / total_exposure).
    """
    total = pulse_count + base_count
    exposure = pulse_exposure + base_exposure
    ratio, ci = _rate_ratio_with_ci(pulse_count, pulse_exposure,
                                    base_count, base_exposure)
    if total == 0 or exposure <= 0:
        return {"p_value": 1.0, "rate_ratio": ratio, "rate_ratio_ci": ci}
    pi = pulse_exposure / exposure
    p_value = _binom_sf(pulse_count, total, pi)
    return {"p_value": p_value, "rate_ratio": ratio, "rate_ratio_ci": ci}


def fading_test(pulse_count: int, pulse_exposure: float,
                base_count: int, base_exposure: float) -> Dict:
    """One-sided test that the pulse rate has FALLEN below the baseline."""
    total = pulse_count + base_count
    exposure = pulse_exposure + base_exposure
    ratio, ci = _rate_ratio_with_ci(pulse_count, pulse_exposure,
                                    base_count, base_exposure)
    if total == 0 or exposure <= 0:
        return {"p_value": 1.0, "rate_ratio": ratio, "rate_ratio_ci": ci}
    pi = pulse_exposure / exposure
    p_value = _binom_cdf(pulse_count, total, pi)
    return {"p_value": p_value, "rate_ratio": ratio, "rate_ratio_ci": ci}


def _tv_distance(a: Sequence[float], b: Sequence[float]) -> float:
    na, nb = sum(a), sum(b)
    if na == 0 or nb == 0:
        return 0.0
    return 0.5 * sum(abs(x / na - y / nb) for x, y in zip(a, b))


def _chi2_sf(x: float, df: int) -> float:
    """Chi-square survival function for df in {1, 2} (all we need for a
    2xK stance table after dropping empty categories, K <= 3)."""
    if x <= 0:
        return 1.0
    if df == 1:
        return math.erfc(math.sqrt(x / 2.0))
    if df == 2:
        return math.exp(-x / 2.0)
    raise ValueError(f"unsupported df={df}")


def stance_shift_test(pulse_counts: Sequence[int],
                      base_counts: Sequence[int],
                      n_permutations: int = 2000,
                      seed: int = PERMUTATION_SEED) -> Dict:
    """Test whether the pulse stance distribution differs from baseline.

    Statistic is total-variation distance (matches the legacy detector's
    `div`). Small samples get a seeded permutation test; ample samples get
    a chi-square homogeneity test. tv_ci is a seeded bootstrap interval.
    """
    pulse = list(pulse_counts)
    base = list(base_counts)
    n_p, n_b = sum(pulse), sum(base)
    tv = _tv_distance(pulse, base)
    if n_p == 0 or n_b == 0:
        return {"p_value": 1.0, "tv_distance": tv, "tv_ci": (0.0, 1.0)}

    rng = random.Random(seed)

    # Drop categories empty in the pooled data (they carry no information
    # and would zero chi-square expected cells).
    pooled = [p + b for p, b in zip(pulse, base)]
    keep = [i for i, c in enumerate(pooled) if c > 0]
    kp = [pulse[i] for i in keep]
    kb = [base[i] for i in keep]
    pooled = [pooled[i] for i in keep]
    total = n_p + n_b

    expected_ok = all(c * n_p / total >= 5 and c * n_b / total >= 5
                      for c in pooled)
    if expected_ok and min(n_p, n_b) >= 12 and len(keep) >= 2:
        chi2 = 0.0
        for i, c in enumerate(pooled):
            for obs, n_g in ((kp[i], n_p), (kb[i], n_b)):
                exp = c * n_g / total
                chi2 += (obs - exp) ** 2 / exp
        p_value = _chi2_sf(chi2, df=len(keep) - 1)
    else:
        # Permutation: pool the observations, reshuffle group membership.
        labels: List[int] = []
        for i, c in enumerate(pooled):
            labels.extend([i] * c)
        at_least = 0
        for _ in range(n_permutations):
            rng.shuffle(labels)
            perm_pulse = [0] * len(pooled)
            for lab in labels[:n_p]:
                perm_pulse[lab] += 1
            perm_base = [c - p for c, p in zip(pooled, perm_pulse)]
            if _tv_distance(perm_pulse, perm_base) >= tv - 1e-12:
                at_least += 1
        p_value = (1 + at_least) / (n_permutations + 1)

    # Bootstrap CI on the observed TV distance (multinomial resamples).
    resamples = []
    p_hat = [x / n_p for x in kp]
    q_hat = [x / n_b for x in kb]
    for _ in range(500):
        rp = _multinomial(rng, n_p, p_hat)
        rb = _multinomial(rng, n_b, q_hat)
        resamples.append(_tv_distance(rp, rb))
    resamples.sort()
    tv_ci = (round(resamples[int(0.025 * len(resamples))], 4),
             round(resamples[min(len(resamples) - 1,
                                 int(0.975 * len(resamples)))], 4))
    return {"p_value": p_value, "tv_distance": tv, "tv_ci": tv_ci}


def _multinomial(rng: random.Random, n: int,
                 probs: Sequence[float]) -> List[int]:
    counts = [0] * len(probs)
    cum = []
    acc = 0.0
    for p in probs:
        acc += p
        cum.append(acc)
    for _ in range(n):
        u = rng.random() * acc
        for i, c in enumerate(cum):
            if u <= c:
                counts[i] += 1
                break
        else:
            counts[-1] += 1
    return counts


def contested_test(pos: int, neg: int, neu: int) -> Dict:
    """How surprising is this balance if the field were genuinely lopsided?

    Null: the majority side holds 75% of directional positions. p_value is
    the probability such a lopsided field produces a split at least this
    balanced — small p means confidently contested.
    """
    n = pos + neg
    if n == 0 or min(pos, neg) == 0:
        return {"p_value": 1.0, "balance": 0.0,
                "pos_share_ci": wilson_interval(pos, n)}
    m = min(pos, neg)
    balance = m / max(pos, neg)
    # P(m <= X <= n - m) for X ~ Binomial(n, 0.75)
    p_value = max(0.0, _binom_cdf(n - m, n, 0.75) - _binom_cdf(m - 1, n, 0.75))
    return {"p_value": min(1.0, p_value), "balance": balance,
            "pos_share_ci": wilson_interval(pos, n)}


def confidence_tier(p_value: float, effect_ok: bool, shows: int) -> str:
    """Single tier policy for every detector family."""
    if not effect_ok:
        return "weak"
    if p_value < ALPHA_STRONG and shows >= 3:
        return "strong"
    if p_value < ALPHA_MODERATE and shows >= 2:
        return "moderate"
    return "weak"


def benjamini_hochberg(pvalues: Sequence[float],
                       alpha: float = FDR_ALPHA) -> List[bool]:
    """BH step-up FDR gate. Returns pass/fail per input position."""
    m = len(pvalues)
    if m == 0:
        return []
    order = sorted(range(m), key=lambda i: pvalues[i])
    cutoff_rank = -1
    for rank, idx in enumerate(order, start=1):
        if pvalues[idx] <= rank * alpha / m:
            cutoff_rank = rank
    flags = [False] * m
    for rank, idx in enumerate(order, start=1):
        if rank <= cutoff_rank:
            flags[idx] = True
    return flags
