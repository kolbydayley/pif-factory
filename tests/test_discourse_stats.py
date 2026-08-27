"""Tests for research_factory.discourse_stats — the Signal Desk stats core.

These pin the statistical behaviour the detectors rely on: Wilson intervals
against known values, rate tests that ignore corpus-coverage swings,
deterministic permutation tests, and Benjamini-Hochberg gating.
"""
import unittest

from research_factory.discourse_stats import (
    benjamini_hochberg,
    confidence_tier,
    contested_test,
    fading_test,
    pagerank,
    poisson_rate_test,
    stance_shift_test,
    wilson_interval,
)


class WilsonIntervalTest(unittest.TestCase):
    def test_known_value_8_of_10(self):
        lo, hi = wilson_interval(8, 10)
        self.assertAlmostEqual(lo, 0.4902, places=3)
        self.assertAlmostEqual(hi, 0.9433, places=3)

    def test_zero_n_is_vacuous(self):
        self.assertEqual(wilson_interval(0, 0), (0.0, 1.0))

    def test_bounds_stay_in_unit_interval(self):
        for k, n in [(0, 5), (5, 5), (1, 400)]:
            lo, hi = wilson_interval(k, n)
            self.assertGreaterEqual(lo, 0.0)
            self.assertLessEqual(hi, 1.0)
            self.assertLessEqual(lo, hi)


class PoissonRateTest(unittest.TestCase):
    def test_equal_rates_are_not_significant(self):
        r = poisson_rate_test(10, 100, 40, 400)
        self.assertGreater(r["p_value"], 0.2)
        self.assertAlmostEqual(r["rate_ratio"], 1.0, places=2)

    def test_genuine_surge_is_significant(self):
        # 0.30/unit vs 0.025/unit — a 12x rate jump on ample exposure.
        r = poisson_rate_test(30, 100, 10, 400)
        self.assertLess(r["p_value"], 0.001)
        self.assertGreater(r["rate_ratio"], 5.0)

    def test_coverage_doubling_alone_is_not_a_surge(self):
        # Same underlying rate; the pulse window simply has 2x the corpus
        # coverage (exposure). Raw counts double but the test must not fire.
        r = poisson_rate_test(20, 200, 10, 100)
        self.assertGreater(r["p_value"], 0.2)
        self.assertAlmostEqual(r["rate_ratio"], 1.0, places=2)

    def test_zero_baseline_does_not_crash(self):
        r = poisson_rate_test(9, 100, 0, 300)
        self.assertLess(r["p_value"], 0.05)
        self.assertGreater(r["rate_ratio"], 1.0)
        lo, hi = r["rate_ratio_ci"]
        self.assertLessEqual(lo, r["rate_ratio"])
        self.assertGreaterEqual(hi, r["rate_ratio"])

    def test_large_counts_use_stable_path(self):
        r = poisson_rate_test(3000, 10000, 2000, 10000)
        self.assertLess(r["p_value"], 0.001)
        self.assertTrue(0.0 <= r["p_value"] <= 1.0)


class StanceShiftTest(unittest.TestCase):
    def test_tiny_sample_flip_is_not_significant(self):
        # 8 vs 10 observations with TV distance ~0.33 — the exact case the
        # legacy 0.3 threshold used to fire on.
        r = stance_shift_test([5, 3, 0], [3, 7, 0])
        self.assertGreaterEqual(r["tv_distance"], 0.3)
        self.assertGreater(r["p_value"], 0.05)

    def test_same_flip_at_scale_is_significant(self):
        r = stance_shift_test([40, 20, 0], [20, 40, 0])
        self.assertLess(r["p_value"], 0.05)

    def test_permutation_is_deterministic(self):
        a = stance_shift_test([5, 3, 1], [3, 7, 2])
        b = stance_shift_test([5, 3, 1], [3, 7, 2])
        self.assertEqual(a["p_value"], b["p_value"])
        self.assertEqual(a["tv_ci"], b["tv_ci"])

    def test_identical_distributions_high_p(self):
        r = stance_shift_test([30, 30, 30], [30, 30, 30])
        self.assertGreater(r["p_value"], 0.5)
        self.assertAlmostEqual(r["tv_distance"], 0.0, places=6)

    def test_empty_group_is_vacuous(self):
        r = stance_shift_test([0, 0, 0], [3, 7, 2])
        self.assertEqual(r["p_value"], 1.0)


class ContestedTest(unittest.TestCase):
    def test_balanced_field_is_confidently_contested(self):
        r = contested_test(22, 18, 5)
        self.assertLess(r["p_value"], 0.05)
        self.assertGreaterEqual(r["balance"], 0.5)

    def test_lopsided_field_is_not_contested(self):
        r = contested_test(20, 2, 0)
        self.assertGreater(r["p_value"], 0.2)

    def test_tiny_balanced_field_is_uncertain(self):
        # 5 vs 4 looks balanced but proves nothing.
        r = contested_test(5, 4, 0)
        self.assertGreater(r["p_value"], 0.05)


class FadingTest(unittest.TestCase):
    def test_collapse_is_significant(self):
        r = fading_test(1, 100, 60, 500)
        self.assertLess(r["p_value"], 0.01)
        self.assertLess(r["rate_ratio"], 0.5)

    def test_steady_topic_is_not_fading(self):
        r = fading_test(12, 100, 60, 500)
        self.assertGreater(r["p_value"], 0.2)


class ConfidenceTierTest(unittest.TestCase):
    def test_tiers(self):
        self.assertEqual(confidence_tier(0.005, True, 3), "strong")
        self.assertEqual(confidence_tier(0.03, True, 2), "moderate")
        self.assertEqual(confidence_tier(0.005, True, 2), "moderate")
        self.assertEqual(confidence_tier(0.005, False, 3), "weak")
        self.assertEqual(confidence_tier(0.2, True, 5), "weak")


class PageRankTest(unittest.TestCase):
    def test_star_hub_outranks_leaves(self):
        edges = {"hub": {"a": 1, "b": 1, "c": 1, "d": 1},
                 "a": {"hub": 1}, "b": {"hub": 1},
                 "c": {"hub": 1}, "d": {"hub": 1}}
        pr = pagerank(edges)
        self.assertGreater(pr["hub"], pr["a"])
        self.assertAlmostEqual(pr["a"], pr["d"], places=9)

    def test_strength_of_neighbors_matters(self):
        # x and y each have ONE relationship, to endorsers with identical
        # out-degree. x's endorser is itself endorsed by four returning
        # fans; y's endorser points at dead ends. x must rank higher —
        # the strength of the node related to you carries through.
        edges = {"e1": {"f1": 1, "f2": 1, "f3": 1, "f4": 1, "x": 1},
                 "f1": {"e1": 1}, "f2": {"e1": 1},
                 "f3": {"e1": 1}, "f4": {"e1": 1}, "x": {"e1": 1},
                 "e2": {"d1": 1, "d2": 1, "d3": 1, "d4": 1, "y": 1},
                 "d1": {}, "d2": {}, "d3": {}, "d4": {},
                 "y": {"e2": 1}}
        pr = pagerank(edges)
        self.assertGreater(pr["e1"], pr["e2"])
        self.assertGreater(pr["x"], 2 * pr["y"])

    def test_edge_weight_matters(self):
        edges = {"a": {"b": 3, "c": 1},
                 "b": {"a": 3}, "c": {"a": 1}}
        pr = pagerank(edges)
        self.assertGreater(pr["b"], pr["c"])

    def test_scores_sum_to_one_and_deterministic(self):
        edges = {"a": {"b": 1}, "b": {"a": 1, "c": 2}, "c": {"b": 2}}
        pr1, pr2 = pagerank(edges), pagerank(edges)
        self.assertAlmostEqual(sum(pr1.values()), 1.0, places=6)
        self.assertEqual(pr1, pr2)

    def test_empty_graph(self):
        self.assertEqual(pagerank({}), {})

    def test_isolated_node_gets_teleport_floor(self):
        pr = pagerank({"a": {"b": 1}, "b": {"a": 1}, "loner": {}})
        self.assertGreater(pr["loner"], 0.0)
        self.assertLess(pr["loner"], pr["a"])


class BenjaminiHochbergTest(unittest.TestCase):
    def test_known_gate(self):
        flags = benjamini_hochberg([0.001, 0.02, 0.5, 0.04], alpha=0.10)
        self.assertEqual(flags, [True, True, False, True])

    def test_empty(self):
        self.assertEqual(benjamini_hochberg([]), [])

    def test_all_null(self):
        self.assertEqual(benjamini_hochberg([0.6, 0.9, 0.7]),
                         [False, False, False])


if __name__ == "__main__":
    unittest.main()
