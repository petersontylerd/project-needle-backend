"""First-principles statistical calculators for validation.

CRITICAL: NO imports from production code. These calculators implement
formulas from scratch to validate the production implementations.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray


class IndependentSimpleZScoreCalculator:
    """Recalculate simple z-scores from first principles.

    Formula:
        z = (value - peer_mean) / peer_std

    Uses population standard deviation (ddof=0), NOT sample std.
    """

    def peer_mean(self, values: NDArray[np.float64]) -> float:
        """Calculate arithmetic mean of peer values.

        Args:
            values: Array of peer metric values.

        Returns:
            Mean value.
        """
        return float(np.sum(values) / len(values))

    def peer_std(self, values: NDArray[np.float64]) -> float:
        """Calculate population standard deviation (ddof=0).

        Args:
            values: Array of peer metric values.

        Returns:
            Population standard deviation.
        """
        mean = self.peer_mean(values)
        variance = float(np.sum((values - mean) ** 2) / len(values))
        return float(np.sqrt(variance))

    def simple_zscore(
        self,
        value: float,
        mean: float,
        std: float,
        std_floor: float = 0.01,
    ) -> float:
        """Calculate simple z-score with floor protection.

        Args:
            value: Entity's metric value.
            mean: Peer mean.
            std: Peer standard deviation.
            std_floor: Minimum std to prevent division by near-zero.

        Returns:
            Z-score.
        """
        effective_std = max(std, std_floor)
        return (value - mean) / effective_std


class IndependentRobustZScoreCalculator:
    """Recalculate robust z-scores from first principles.

    Formula:
        z = (value - peer_median) / (MAD * 1.4826)

    The scaling constant 1.4826 = 1/Φ^(-1)(0.75) makes scaled MAD
    equivalent to standard deviation for normal distributions.
    """

    MAD_SCALE: float = 1.4826

    def peer_median(self, values: NDArray[np.float64]) -> float:
        """Calculate median of peer values.

        Args:
            values: Array of peer metric values.

        Returns:
            Median value.
        """
        return float(np.median(values))

    def mad(self, values: NDArray[np.float64]) -> float:
        """Calculate Median Absolute Deviation.

        MAD = median(|x - median(x)|)

        Args:
            values: Array of peer metric values.

        Returns:
            Median absolute deviation (unscaled).
        """
        median = self.peer_median(values)
        return float(np.median(np.abs(values - median)))

    def robust_zscore(
        self,
        value: float,
        median: float,
        mad: float,
        mad_floor: float = 0.01,
    ) -> float:
        """Calculate robust z-score with floor protection.

        Args:
            value: Entity's metric value.
            median: Peer median.
            mad: Median absolute deviation (unscaled).
            mad_floor: Minimum scaled MAD to prevent division by near-zero.

        Returns:
            Robust z-score.
        """
        scaled_mad = mad * self.MAD_SCALE
        effective_mad = max(scaled_mad, mad_floor)
        return (value - median) / effective_mad


class IndependentPercentileCalculator:
    """Recalculate percentile ranks using midpoint method.

    Formula:
        percentile = 100 * (below + 0.5 * equal) / n

    Where:
        below = count of values strictly less than target
        equal = count of values equal to target (including target itself)
        n = total count of values
    """

    def percentile_rank(self, value: float, all_values: NDArray[np.float64]) -> float:
        """Calculate percentile rank using midpoint method.

        Args:
            value: Entity's metric value.
            all_values: Array of all peer metric values.

        Returns:
            Percentile rank in [0, 100].
        """
        n = len(all_values)
        if n == 0:
            return 50.0  # Default for empty distribution

        below = float(np.sum(all_values < value))
        equal = float(np.sum(all_values == value))
        percentile = 100.0 * (below + 0.5 * equal) / n

        # Clamp to [0, 100] range
        return max(0.0, min(100.0, percentile))


class IndependentAnomalyTierClassifier:
    """Assign 9-tier anomaly labels from z-scores.

    Boundaries are inclusive on upper bound.
    Source of truth: taxonomy/anomaly_method_profiles.yaml
    """

    BOUNDARIES: list[float] = [-3.0, -2.0, -1.0, -0.5, 0.5, 1.0, 2.0, 3.0]
    LABELS: list[str] = [
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

    def classify(self, zscore: float | None) -> str:
        """Classify z-score into 9-tier anomaly label.

        Args:
            zscore: Z-score value, or None for suppressed entities.

        Returns:
            Tier label string.
        """
        if zscore is None:
            return "no_score"

        for i, boundary in enumerate(self.BOUNDARIES):
            if zscore <= boundary:
                return self.LABELS[i]

        return self.LABELS[-1]  # Above highest boundary


class IndependentContributionCalculator:
    """Recalculate contribution metrics from first principles.

    Formulas:
        weight_share = child_weight / total_weight
        raw_component = weight_share * child_value
        excess_over_parent = weight_share * (child_value - parent_value)
    """

    def weight_share(self, weight: float, total_weight: float) -> float:
        """Calculate weight share for a child entity.

        Args:
            weight: Child's weight value.
            total_weight: Sum of all children's weights.

        Returns:
            Weight share in [0, 1].
        """
        if total_weight == 0:
            return 0.0
        return weight / total_weight

    def raw_component(self, weight_share: float, child_value: float) -> float:
        """Calculate raw component contribution.

        Args:
            weight_share: Child's weight share.
            child_value: Child's metric value.

        Returns:
            Raw component value.
        """
        return weight_share * child_value

    def excess_over_parent(self, weight_share: float, child_value: float, parent_value: float) -> float:
        """Calculate excess contribution over parent.

        Args:
            weight_share: Child's weight share.
            child_value: Child's metric value.
            parent_value: Parent's metric value.

        Returns:
            Excess over parent (positive = child above parent).
        """
        return weight_share * (child_value - parent_value)


class IndependentClassificationCalculator:
    """Recalculate signal classifications from first principles.

    Source of truth: src/analytics/signal_classification/thresholds.py
    """

    # Magnitude tier thresholds (based on percentile_rank)
    MAGNITUDE_THRESHOLDS: dict[str, tuple[float, float]] = {
        "critical": (99.0, 100.0),
        "severe": (95.0, 99.0),
        "elevated": (85.0, 95.0),
        "marginal": (75.0, 85.0),
        "expected": (25.0, 75.0),
        "favorable": (10.0, 25.0),
        "excellent": (0.0, 10.0),
    }

    # Trajectory tier thresholds (based on slope_percentile)
    TRAJECTORY_THRESHOLDS: dict[str, tuple[float, float]] = {
        "rapidly_deteriorating": (90.0, 100.0),
        "deteriorating": (70.0, 90.0),
        "stable": (30.0, 70.0),
        "improving": (10.0, 30.0),
        "rapidly_improving": (0.0, 10.0),
    }

    # Consistency tier thresholds
    CONSISTENCY_PERSISTENT_MAX_STD: float = 0.90
    CONSISTENCY_VARIABLE_MAX_STD: float = 1.10
    CONSISTENCY_MIN_PERIODS_PERSISTENT: int = 6
    CONSISTENCY_MIN_PERIODS_VARIABLE: int = 3

    # Priority score weights
    MAGNITUDE_WEIGHT: float = 30.0
    TRAJECTORY_WEIGHT: float = 25.0
    CONSISTENCY_WEIGHT: float = 15.0
    VOLUME_WEIGHT: float = 15.0
    ACTIONABILITY_WEIGHT: float = 10.0

    # Magnitude tier weights for priority calculation
    MAGNITUDE_TIER_WEIGHTS: dict[str, float] = {
        "critical": 1.0,
        "severe": 0.85,
        "elevated": 0.70,
        "marginal": 0.50,
        "expected": 0.25,
        "favorable": 0.10,
        "excellent": 0.0,
    }

    # Trajectory tier weights
    TRAJECTORY_TIER_WEIGHTS: dict[str, float] = {
        "rapidly_deteriorating": 1.0,
        "deteriorating": 0.75,
        "stable": 0.40,
        "improving": 0.20,
        "rapidly_improving": 0.0,
    }

    # Consistency tier weights
    CONSISTENCY_TIER_WEIGHTS: dict[str, float] = {
        "persistent": 0.80,
        "variable": 0.50,
        "transient": 0.20,
    }

    # Sub-classification adjustments
    SUB_CLASS_ADJUSTMENTS: dict[str, float] = {
        "crisis_escalating": 15.0,
        "crisis_acute": 12.0,
        "systemic_failure": 10.0,
        "concentrated_driver": 8.0,
        "accelerating_decline": 6.0,
        "broad_underperformance": 4.0,
        "gradual_erosion": 2.0,
        "approaching_threshold": 2.0,
        "early_warning": 1.0,
        "volatile_pattern": 0.0,
        "recovering": -5.0,
        "maintaining_position": -8.0,
        "stable_performer": -10.0,
        "emerging_leader": -10.0,
        "sustained_excellence": -12.0,
        "top_performer": -15.0,
        "data_quality_suspect": -15.0,
        "transient_anomaly": -12.0,
        "insufficient_history": -15.0,
    }

    def magnitude_tier(self, percentile: float) -> str:
        """Classify percentile into magnitude tier.

        Args:
            percentile: Percentile rank in [0, 100].

        Returns:
            Magnitude tier label.
        """
        if percentile >= 99.0:
            return "critical"
        if percentile >= 95.0:
            return "severe"
        if percentile >= 85.0:
            return "elevated"
        if percentile >= 75.0:
            return "marginal"
        if percentile < 10.0:
            return "excellent"
        if percentile < 25.0:
            return "favorable"
        return "expected"

    def trajectory_tier(self, slope_percentile: float) -> str:
        """Classify slope percentile into trajectory tier.

        Args:
            slope_percentile: Slope percentile in [0, 100].

        Returns:
            Trajectory tier label.
        """
        if slope_percentile >= 90.0:
            return "rapidly_deteriorating"
        if slope_percentile >= 70.0:
            return "deteriorating"
        if slope_percentile < 10.0:
            return "rapidly_improving"
        if slope_percentile < 30.0:
            return "improving"
        return "stable"

    def consistency_tier(self, std_dev: float, periods: int) -> str:
        """Classify into consistency tier.

        Args:
            std_dev: Standard deviation of z-scores over time.
            periods: Number of temporal periods.

        Returns:
            Consistency tier label.
        """
        if periods < self.CONSISTENCY_MIN_PERIODS_VARIABLE:
            return "transient"
        if std_dev > self.CONSISTENCY_VARIABLE_MAX_STD:
            return "transient"
        if std_dev < self.CONSISTENCY_PERSISTENT_MAX_STD and periods >= self.CONSISTENCY_MIN_PERIODS_PERSISTENT:
            return "persistent"
        return "variable"

    def priority_score(
        self,
        magnitude_tier: str,
        trajectory_tier: str,
        consistency_tier: str,
        encounters: int,
        sub_classification: str,
        actionability_weight: float = 0.5,
    ) -> int:
        """Calculate priority score.

        Args:
            magnitude_tier: Magnitude tier label.
            trajectory_tier: Trajectory tier label.
            consistency_tier: Consistency tier label.
            encounters: Number of encounters.
            sub_classification: Sub-classification label.
            actionability_weight: Actionability weight (default 0.5).

        Returns:
            Priority score in [1, 100].
        """
        import math

        mag_weight = self.MAGNITUDE_TIER_WEIGHTS.get(magnitude_tier, 0.25)
        traj_weight = self.TRAJECTORY_TIER_WEIGHTS.get(trajectory_tier, 0.40)
        cons_weight = self.CONSISTENCY_TIER_WEIGHTS.get(consistency_tier, 0.50)

        # Volume weight: min(1.0, log10(encounters) / 4.0)
        vol_weight = min(1.0, math.log10(max(1, encounters)) / 4.0)

        # Calculate base score
        base_score = (
            mag_weight * self.MAGNITUDE_WEIGHT
            + traj_weight * self.TRAJECTORY_WEIGHT
            + cons_weight * self.CONSISTENCY_WEIGHT
            + vol_weight * self.VOLUME_WEIGHT
            + actionability_weight * self.ACTIONABILITY_WEIGHT
        )

        # Apply sub-classification adjustment
        adjustment = self.SUB_CLASS_ADJUSTMENTS.get(sub_classification, 0.0)
        final_score = base_score + adjustment

        # Clamp to [1, 100]
        return int(max(1, min(100, round(final_score))))
