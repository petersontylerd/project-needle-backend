"""Tests for independent statistical calculators."""

import numpy as np

from tests.validation.helpers.independent_calculators import (
    IndependentAnomalyTierClassifier,
    IndependentClassificationCalculator,
    IndependentContributionCalculator,
    IndependentPercentileCalculator,
    IndependentRobustZScoreCalculator,
    IndependentSimpleZScoreCalculator,
)


class TestIndependentSimpleZScoreCalculator:
    """Tests for simple z-score calculator."""

    def test_peer_mean_calculation(self) -> None:
        """peer_mean returns arithmetic mean."""
        calc = IndependentSimpleZScoreCalculator()
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        assert calc.peer_mean(values) == 3.0

    def test_peer_std_uses_population_ddof_zero(self) -> None:
        """peer_std uses population std (ddof=0), not sample std."""
        calc = IndependentSimpleZScoreCalculator()
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        # Population std: sqrt(sum((x-mean)^2)/n) = sqrt(2.0) ≈ 1.4142
        expected = np.sqrt(2.0)
        assert abs(calc.peer_std(values) - expected) < 1e-10

    def test_simple_zscore_calculation(self) -> None:
        """simple_zscore returns (value - mean) / std."""
        calc = IndependentSimpleZScoreCalculator()
        # mean=3.0, std=sqrt(2)≈1.4142
        # z = (5.0 - 3.0) / 1.4142 ≈ 1.4142
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        mean = calc.peer_mean(values)
        std = calc.peer_std(values)
        zscore = calc.simple_zscore(5.0, mean, std)
        expected = 2.0 / np.sqrt(2.0)
        assert abs(zscore - expected) < 1e-10

    def test_std_floor_prevents_division_by_zero(self) -> None:
        """std_floor is applied when std is below threshold."""
        calc = IndependentSimpleZScoreCalculator()
        # All same values -> std = 0
        values = np.array([1.0, 1.0, 1.0, 1.0, 1.0])
        std = calc.peer_std(values)
        assert std == 0.0
        # With floor, should not raise ZeroDivisionError
        zscore = calc.simple_zscore(2.0, 1.0, std, std_floor=0.01)
        # (2.0 - 1.0) / 0.01 = 100.0
        assert zscore == 100.0


class TestIndependentRobustZScoreCalculator:
    """Tests for robust z-score calculator."""

    def test_peer_median_calculation(self) -> None:
        """peer_median returns median value."""
        calc = IndependentRobustZScoreCalculator()
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        assert calc.peer_median(values) == 3.0

    def test_mad_calculation(self) -> None:
        """mad returns median absolute deviation."""
        calc = IndependentRobustZScoreCalculator()
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        # median = 3.0
        # deviations = |1-3|, |2-3|, |3-3|, |4-3|, |5-3| = 2, 1, 0, 1, 2
        # median of deviations = 1.0
        assert calc.mad(values) == 1.0

    def test_mad_scaling_constant(self) -> None:
        """MAD_SCALE is 1.4826 for normal distribution equivalence."""
        calc = IndependentRobustZScoreCalculator()
        assert calc.MAD_SCALE == 1.4826

    def test_robust_zscore_calculation(self) -> None:
        """robust_zscore returns (value - median) / (MAD * scale)."""
        calc = IndependentRobustZScoreCalculator()
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        median = calc.peer_median(values)  # 3.0
        mad = calc.mad(values)  # 1.0
        # z = (5.0 - 3.0) / (1.0 * 1.4826) = 2.0 / 1.4826 ≈ 1.349
        zscore = calc.robust_zscore(5.0, median, mad)
        expected = 2.0 / 1.4826
        assert abs(zscore - expected) < 1e-10

    def test_mad_floor_prevents_division_by_zero(self) -> None:
        """mad_floor is applied when scaled MAD is below threshold."""
        calc = IndependentRobustZScoreCalculator()
        # All same values -> MAD = 0
        values = np.array([1.0, 1.0, 1.0, 1.0, 1.0])
        mad = calc.mad(values)
        assert mad == 0.0
        # With floor, should not raise ZeroDivisionError
        zscore = calc.robust_zscore(2.0, 1.0, mad, mad_floor=0.01)
        # (2.0 - 1.0) / 0.01 = 100.0
        assert zscore == 100.0


class TestIndependentPercentileCalculator:
    """Tests for percentile rank calculator."""

    def test_percentile_rank_midpoint_method(self) -> None:
        """percentile_rank uses midpoint method for ties."""
        calc = IndependentPercentileCalculator()
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        # For value=3.0: below=2, equal=1, n=5
        # percentile = 100 * (2 + 0.5*1) / 5 = 100 * 2.5 / 5 = 50.0
        assert calc.percentile_rank(3.0, values) == 50.0

    def test_percentile_rank_minimum_value(self) -> None:
        """Minimum value has percentile based on tie count."""
        calc = IndependentPercentileCalculator()
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        # For value=1.0: below=0, equal=1, n=5
        # percentile = 100 * (0 + 0.5*1) / 5 = 10.0
        assert calc.percentile_rank(1.0, values) == 10.0

    def test_percentile_rank_maximum_value(self) -> None:
        """Maximum value has percentile based on tie count."""
        calc = IndependentPercentileCalculator()
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        # For value=5.0: below=4, equal=1, n=5
        # percentile = 100 * (4 + 0.5*1) / 5 = 90.0
        assert calc.percentile_rank(5.0, values) == 90.0

    def test_percentile_rank_with_ties(self) -> None:
        """Tied values share the same percentile rank."""
        calc = IndependentPercentileCalculator()
        values = np.array([1.0, 2.0, 2.0, 2.0, 5.0])
        # For value=2.0: below=1, equal=3, n=5
        # percentile = 100 * (1 + 0.5*3) / 5 = 100 * 2.5 / 5 = 50.0
        assert calc.percentile_rank(2.0, values) == 50.0

    def test_percentile_rank_clamped_to_0_100(self) -> None:
        """Percentile rank is always in [0, 100] range."""
        calc = IndependentPercentileCalculator()
        values = np.array([1.0, 2.0, 3.0])
        # All valid percentiles should be in range
        assert 0.0 <= calc.percentile_rank(1.0, values) <= 100.0
        assert 0.0 <= calc.percentile_rank(3.0, values) <= 100.0


class TestIndependentAnomalyTierClassifier:
    """Tests for 9-tier anomaly classification."""

    def test_boundaries_match_taxonomy(self) -> None:
        """Boundaries match taxonomy/anomaly_method_profiles.yaml."""
        calc = IndependentAnomalyTierClassifier()
        assert calc.BOUNDARIES == [-3.0, -2.0, -1.0, -0.5, 0.5, 1.0, 2.0, 3.0]

    def test_labels_are_9_tiers(self) -> None:
        """Labels are the 9-tier system."""
        calc = IndependentAnomalyTierClassifier()
        expected = [
            "extremely_low",
            "very_low",
            "moderately_low",
            "slightly_low",
            "normal",
            "slightly_high",
            "moderately_high",
            "very_high",
            "extremely_high",
        ]
        assert expected == calc.LABELS

    def test_classify_normal_range(self) -> None:
        """Z-scores in normal range (-0.5, 0.5] classified as normal."""
        calc = IndependentAnomalyTierClassifier()
        assert calc.classify(0.0) == "normal"
        assert calc.classify(0.3) == "normal"
        assert calc.classify(0.5) == "normal"  # Inclusive upper

    def test_classify_extreme_high(self) -> None:
        """Z-scores > 3.0 classified as extremely_high."""
        calc = IndependentAnomalyTierClassifier()
        assert calc.classify(3.1) == "extremely_high"
        assert calc.classify(5.0) == "extremely_high"

    def test_classify_extreme_low(self) -> None:
        """Z-scores <= -3.0 classified as extremely_low."""
        calc = IndependentAnomalyTierClassifier()
        assert calc.classify(-3.0) == "extremely_low"
        assert calc.classify(-5.0) == "extremely_low"

    def test_classify_none_returns_no_score(self) -> None:
        """None z-score returns no_score."""
        calc = IndependentAnomalyTierClassifier()
        assert calc.classify(None) == "no_score"

    def test_classify_all_boundaries(self) -> None:
        """Test all boundary transitions."""
        calc = IndependentAnomalyTierClassifier()
        # Test each tier
        assert calc.classify(-3.5) == "extremely_low"
        assert calc.classify(-2.5) == "very_low"
        assert calc.classify(-1.5) == "moderately_low"
        assert calc.classify(-0.7) == "slightly_low"
        assert calc.classify(0.0) == "normal"
        assert calc.classify(0.7) == "slightly_high"
        assert calc.classify(1.5) == "moderately_high"
        assert calc.classify(2.5) == "very_high"
        assert calc.classify(3.5) == "extremely_high"


class TestIndependentContributionCalculator:
    """Tests for contribution analysis calculator."""

    def test_weight_share_calculation(self) -> None:
        """weight_share returns weight / total_weight."""
        calc = IndependentContributionCalculator()
        # 30 / 100 = 0.30
        assert calc.weight_share(30.0, 100.0) == 0.30

    def test_weight_share_zero_total_returns_zero(self) -> None:
        """weight_share returns 0 when total is 0 (avoid division by zero)."""
        calc = IndependentContributionCalculator()
        assert calc.weight_share(10.0, 0.0) == 0.0

    def test_raw_component_calculation(self) -> None:
        """raw_component returns weight_share * child_value."""
        calc = IndependentContributionCalculator()
        # 0.30 * 1.5 = 0.45
        result = calc.raw_component(0.30, 1.5)
        assert abs(result - 0.45) < 1e-10

    def test_excess_over_parent_positive(self) -> None:
        """excess_over_parent positive when child > parent."""
        calc = IndependentContributionCalculator()
        # weight_share * (child - parent) = 0.30 * (1.5 - 1.0) = 0.15
        result = calc.excess_over_parent(0.30, 1.5, 1.0)
        assert abs(result - 0.15) < 1e-10

    def test_excess_over_parent_negative(self) -> None:
        """excess_over_parent negative when child < parent."""
        calc = IndependentContributionCalculator()
        # weight_share * (child - parent) = 0.30 * (0.8 - 1.0) = -0.06
        result = calc.excess_over_parent(0.30, 0.8, 1.0)
        assert abs(result - (-0.06)) < 1e-10

    def test_excess_over_parent_verified_example(self) -> None:
        """Verify with actual production data example."""
        calc = IndependentContributionCalculator()
        # From design doc worked example:
        # child_value = 1.02465861284225
        # parent_value = 1.0504409280199398
        # weight_share = 0.29257535909366783
        # excess = 0.29257535909366783 * (1.02465861284225 - 1.0504409280199398)
        #        = -0.007543270121378754
        result = calc.excess_over_parent(
            weight_share=0.29257535909366783,
            child_value=1.02465861284225,
            parent_value=1.0504409280199398,
        )
        expected = -0.007543270121378754
        assert abs(result - expected) < 1e-10


class TestIndependentClassificationCalculator:
    """Tests for signal classification calculator."""

    def test_magnitude_tier_critical(self) -> None:
        """Percentile >= 99 is critical."""
        calc = IndependentClassificationCalculator()
        assert calc.magnitude_tier(99.0) == "critical"
        assert calc.magnitude_tier(99.5) == "critical"
        assert calc.magnitude_tier(100.0) == "critical"

    def test_magnitude_tier_severe(self) -> None:
        """Percentile 95-98.9 is severe."""
        calc = IndependentClassificationCalculator()
        assert calc.magnitude_tier(95.0) == "severe"
        assert calc.magnitude_tier(98.9) == "severe"

    def test_magnitude_tier_elevated(self) -> None:
        """Percentile 85-94.9 is elevated."""
        calc = IndependentClassificationCalculator()
        assert calc.magnitude_tier(85.0) == "elevated"
        assert calc.magnitude_tier(94.9) == "elevated"

    def test_magnitude_tier_marginal(self) -> None:
        """Percentile 75-84.9 is marginal."""
        calc = IndependentClassificationCalculator()
        assert calc.magnitude_tier(75.0) == "marginal"
        assert calc.magnitude_tier(84.9) == "marginal"

    def test_magnitude_tier_expected(self) -> None:
        """Percentile 25-74.9 is expected."""
        calc = IndependentClassificationCalculator()
        assert calc.magnitude_tier(25.0) == "expected"
        assert calc.magnitude_tier(50.0) == "expected"
        assert calc.magnitude_tier(74.9) == "expected"

    def test_magnitude_tier_favorable(self) -> None:
        """Percentile 10-24.9 is favorable."""
        calc = IndependentClassificationCalculator()
        assert calc.magnitude_tier(10.0) == "favorable"
        assert calc.magnitude_tier(24.9) == "favorable"

    def test_magnitude_tier_excellent(self) -> None:
        """Percentile < 10 is excellent."""
        calc = IndependentClassificationCalculator()
        assert calc.magnitude_tier(0.0) == "excellent"
        assert calc.magnitude_tier(9.9) == "excellent"

    def test_trajectory_tier_rapidly_deteriorating(self) -> None:
        """Slope percentile >= 90 is rapidly_deteriorating."""
        calc = IndependentClassificationCalculator()
        assert calc.trajectory_tier(90.0) == "rapidly_deteriorating"
        assert calc.trajectory_tier(100.0) == "rapidly_deteriorating"

    def test_trajectory_tier_deteriorating(self) -> None:
        """Slope percentile 70-89.9 is deteriorating."""
        calc = IndependentClassificationCalculator()
        assert calc.trajectory_tier(70.0) == "deteriorating"
        assert calc.trajectory_tier(89.9) == "deteriorating"

    def test_trajectory_tier_stable(self) -> None:
        """Slope percentile 30-69.9 is stable."""
        calc = IndependentClassificationCalculator()
        assert calc.trajectory_tier(30.0) == "stable"
        assert calc.trajectory_tier(50.0) == "stable"
        assert calc.trajectory_tier(69.9) == "stable"

    def test_trajectory_tier_improving(self) -> None:
        """Slope percentile 10-29.9 is improving."""
        calc = IndependentClassificationCalculator()
        assert calc.trajectory_tier(10.0) == "improving"
        assert calc.trajectory_tier(29.9) == "improving"

    def test_trajectory_tier_rapidly_improving(self) -> None:
        """Slope percentile < 10 is rapidly_improving."""
        calc = IndependentClassificationCalculator()
        assert calc.trajectory_tier(0.0) == "rapidly_improving"
        assert calc.trajectory_tier(9.9) == "rapidly_improving"

    def test_consistency_tier_transient_few_periods(self) -> None:
        """Fewer than 3 periods is transient."""
        calc = IndependentClassificationCalculator()
        assert calc.consistency_tier(0.5, 2) == "transient"
        assert calc.consistency_tier(0.5, 1) == "transient"

    def test_consistency_tier_transient_high_std(self) -> None:
        """Std > 1.10 is transient."""
        calc = IndependentClassificationCalculator()
        assert calc.consistency_tier(1.2, 10) == "transient"

    def test_consistency_tier_persistent(self) -> None:
        """Std < 0.90 with >= 6 periods is persistent."""
        calc = IndependentClassificationCalculator()
        assert calc.consistency_tier(0.5, 6) == "persistent"
        assert calc.consistency_tier(0.89, 10) == "persistent"

    def test_consistency_tier_variable(self) -> None:
        """Std 0.90-1.10 with >= 3 periods is variable."""
        calc = IndependentClassificationCalculator()
        assert calc.consistency_tier(0.95, 5) == "variable"
        assert calc.consistency_tier(1.05, 4) == "variable"
        # Also variable: low std but not enough periods for persistent
        assert calc.consistency_tier(0.5, 4) == "variable"

    def test_priority_score_worked_example(self) -> None:
        """Verify priority score with design doc worked example.

        Entity: medicareId=010033 (gradual_erosion)
        magnitude_tier = marginal (weight 0.50)
        trajectory_tier = deteriorating (weight 0.75)
        consistency_tier = persistent (weight 0.80)
        encounters = 49430
        Expected score = 68
        """
        calc = IndependentClassificationCalculator()
        score = calc.priority_score(
            magnitude_tier="marginal",
            trajectory_tier="deteriorating",
            consistency_tier="persistent",
            encounters=49430,
            sub_classification="gradual_erosion",
        )
        assert score == 68

    def test_priority_score_clamped_to_1_100(self) -> None:
        """Priority score is clamped to [1, 100] range."""
        calc = IndependentClassificationCalculator()
        # Very low score scenario: excellent + rapidly_improving + transient + low volume
        score_low = calc.priority_score(
            magnitude_tier="excellent",
            trajectory_tier="rapidly_improving",
            consistency_tier="transient",
            encounters=1,
            sub_classification="top_performer",  # -15 adjustment
        )
        assert score_low >= 1

        # Very high score scenario: critical + rapidly_deteriorating + persistent + high volume
        score_high = calc.priority_score(
            magnitude_tier="critical",
            trajectory_tier="rapidly_deteriorating",
            consistency_tier="persistent",
            encounters=100000,
            sub_classification="crisis_escalating",  # +15 adjustment
        )
        assert score_high <= 100

    def test_priority_score_sub_class_adjustments(self) -> None:
        """Sub-classification adjustments are applied correctly."""
        calc = IndependentClassificationCalculator()

        # Compare with and without adjustment
        score_crisis = calc.priority_score(
            magnitude_tier="elevated",
            trajectory_tier="stable",
            consistency_tier="variable",
            encounters=1000,
            sub_classification="crisis_escalating",
        )
        score_stable = calc.priority_score(
            magnitude_tier="elevated",
            trajectory_tier="stable",
            consistency_tier="variable",
            encounters=1000,
            sub_classification="stable_performer",
        )

        # crisis_escalating (+15) should be higher than stable_performer (-10)
        assert score_crisis > score_stable
        assert score_crisis - score_stable == 25  # 15 - (-10) = 25
