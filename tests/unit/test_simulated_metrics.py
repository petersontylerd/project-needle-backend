"""Tests for the SimulatedMetricsService.

This module tests the simulated clinical metrics service including
financial impact calculations, trend data generation, and intervention
effectiveness lookups.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from src.services.simulated_metrics import (
    FinancialImpact,
    MetricConfig,
    SimulatedMetricsService,
    TrendDataPoint,
)

pytestmark = pytest.mark.tier1


class TestMetricConfig:
    """Tests for MetricConfig retrieval."""

    def test_get_known_metric_config(self) -> None:
        """Test retrieving configuration for a known metric."""
        service = SimulatedMetricsService()
        config = service.get_metric_config("losIndex")

        assert config.metric_id == "losIndex"
        assert config.display_name == "Length of Stay Index"
        assert config.domain == "Efficiency"
        assert config.higher_is_worse is True

    def test_get_unknown_metric_config(self) -> None:
        """Test retrieving configuration for an unknown metric returns default."""
        service = SimulatedMetricsService()
        config = service.get_metric_config("unknownMetric")

        assert config.metric_id == "unknown"
        assert config.display_name == "Unknown Metric"
        assert config.domain == "Unknown"

    def test_safety_metrics_exist(self) -> None:
        """Test that safety metrics are configured."""
        service = SimulatedMetricsService()

        clabsi = service.get_metric_config("clabsiRate")
        assert clabsi.domain == "Safety"
        assert clabsi.cost_per_unit == Decimal("45000.00")

        fall = service.get_metric_config("fallRate")
        assert fall.domain == "Safety"

    def test_effectiveness_metrics_exist(self) -> None:
        """Test that effectiveness metrics are configured."""
        service = SimulatedMetricsService()

        readmission = service.get_metric_config("readmissionRate")
        assert readmission.domain == "Effectiveness"

        mortality = service.get_metric_config("mortalityRate")
        assert mortality.domain == "Effectiveness"
        # Mortality has no dollar cost (priceless)
        assert mortality.cost_per_unit == Decimal("0.00")

    def test_medication_reconciliation_lower_is_worse(self) -> None:
        """Test that medication reconciliation has inverted direction."""
        service = SimulatedMetricsService()
        config = service.get_metric_config("medicationReconciliation")

        # Lower is worse for compliance metrics
        assert config.higher_is_worse is False


class TestFinancialImpact:
    """Tests for financial impact calculations."""

    def test_calculate_basic_impact(self) -> None:
        """Test basic financial impact calculation."""
        service = SimulatedMetricsService()

        impact = service.calculate_financial_impact(
            metric_id="losIndex",
            metric_value=Decimal("1.20"),
            benchmark_value=Decimal("1.00"),
            encounters=100,
        )

        # 0.20 deviation * $2500/unit * 100 encounters = $50,000/day
        assert impact.daily_impact == Decimal("50000.00")
        assert impact.unit_deviation == Decimal("0.2000")
        assert impact.cost_per_unit == Decimal("2500.00")
        assert impact.volume == 100

    def test_calculate_impact_with_days_open(self) -> None:
        """Test financial impact includes days open in total."""
        service = SimulatedMetricsService()
        detected_5_days_ago = datetime.now(tz=UTC) - timedelta(days=5)

        impact = service.calculate_financial_impact(
            metric_id="losIndex",
            metric_value=Decimal("1.10"),
            benchmark_value=Decimal("1.00"),
            encounters=50,
            detected_at=detected_5_days_ago,
        )

        # 0.10 * $2500 * 50 = $12,500/day
        assert impact.daily_impact == Decimal("12500.00")
        assert impact.days_open == 5
        # Total = $12,500 * 5 = $62,500
        assert impact.total_impact == Decimal("62500.00")

    def test_no_impact_when_below_benchmark(self) -> None:
        """Test no financial impact when metric is better than benchmark."""
        service = SimulatedMetricsService()

        impact = service.calculate_financial_impact(
            metric_id="losIndex",  # higher_is_worse=True
            metric_value=Decimal("0.90"),
            benchmark_value=Decimal("1.00"),
            encounters=100,
        )

        # No deviation when performing better than benchmark
        assert impact.daily_impact == Decimal("0.00")
        assert impact.unit_deviation == Decimal("0.0000")

    def test_impact_with_none_benchmark_uses_midpoint(self) -> None:
        """Test that None benchmark uses typical range midpoint."""
        service = SimulatedMetricsService()

        # losIndex typical range: 0.80 to 1.40, midpoint = 1.10
        impact = service.calculate_financial_impact(
            metric_id="losIndex",
            metric_value=Decimal("1.30"),
            benchmark_value=None,
            encounters=100,
        )

        # 0.20 deviation from 1.10 midpoint * $2500 * 100 = $50,000
        assert impact.daily_impact == Decimal("50000.00")
        assert impact.unit_deviation == Decimal("0.2000")

    def test_inverted_metric_impact(self) -> None:
        """Test impact calculation for lower-is-worse metrics."""
        service = SimulatedMetricsService()

        impact = service.calculate_financial_impact(
            metric_id="medicationReconciliation",  # higher_is_worse=False
            metric_value=Decimal("80.0"),  # Below benchmark
            benchmark_value=Decimal("90.0"),
            encounters=50,
        )

        # 10% below benchmark = 10 units * $500 * 50 = $250,000
        assert impact.unit_deviation == Decimal("10.0000")
        assert impact.daily_impact == Decimal("250000.00")

    def test_minimum_days_open_is_one(self) -> None:
        """Test that days_open is at least 1 to avoid zero total."""
        service = SimulatedMetricsService()

        impact = service.calculate_financial_impact(
            metric_id="losIndex",
            metric_value=Decimal("1.20"),
            benchmark_value=Decimal("1.00"),
            encounters=100,
            detected_at=datetime.now(tz=UTC),  # Just now
        )

        # Even if detected today, days_open should be 1
        assert impact.days_open >= 1


class TestTrendData:
    """Tests for trend data generation."""

    def test_generate_trend_data_length(self) -> None:
        """Test that trend data has correct number of points."""
        service = SimulatedMetricsService()

        trend = service.generate_trend_data(
            metric_id="losIndex",
            current_value=Decimal("1.15"),
            benchmark_value=Decimal("1.00"),
            days=7,
        )

        assert len(trend) == 7

    def test_trend_data_last_point_matches_current(self) -> None:
        """Test that the last trend point matches current value."""
        service = SimulatedMetricsService()

        trend = service.generate_trend_data(
            metric_id="losIndex",
            current_value=Decimal("1.25"),
            benchmark_value=Decimal("1.00"),
            days=14,
        )

        # Last point should be current value
        assert trend[-1].value == Decimal("1.25")

    def test_trend_data_has_benchmark(self) -> None:
        """Test that trend data includes benchmark values."""
        service = SimulatedMetricsService()

        trend = service.generate_trend_data(
            metric_id="losIndex",
            current_value=Decimal("1.15"),
            benchmark_value=Decimal("1.05"),
            days=7,
        )

        # All points should have the benchmark
        for point in trend:
            assert point.benchmark == Decimal("1.05")

    def test_trend_data_dates_are_chronological(self) -> None:
        """Test that trend dates are in chronological order."""
        service = SimulatedMetricsService()

        trend = service.generate_trend_data(
            metric_id="losIndex",
            current_value=Decimal("1.15"),
            benchmark_value=Decimal("1.00"),
            days=10,
        )

        for i in range(1, len(trend)):
            assert trend[i].date > trend[i - 1].date

    def test_trend_data_values_in_range(self) -> None:
        """Test that trend values stay within typical range."""
        service = SimulatedMetricsService()
        config = service.get_metric_config("losIndex")

        trend = service.generate_trend_data(
            metric_id="losIndex",
            current_value=Decimal("1.15"),
            benchmark_value=Decimal("1.00"),
            days=30,
        )

        for point in trend:
            assert point.value >= config.typical_range_min
            assert point.value <= config.typical_range_max

    def test_trend_data_with_none_benchmark(self) -> None:
        """Test trend generation uses midpoint when benchmark is None."""
        service = SimulatedMetricsService()

        trend = service.generate_trend_data(
            metric_id="losIndex",
            current_value=Decimal("1.20"),
            benchmark_value=None,
            days=7,
        )

        # Should use midpoint (0.80 + 1.40) / 2 = 1.10
        assert trend[0].benchmark == Decimal("1.10")

    def test_trend_data_volume_varies(self) -> None:
        """Test that volume varies across data points."""
        service = SimulatedMetricsService()

        trend = service.generate_trend_data(
            metric_id="losIndex",
            current_value=Decimal("1.15"),
            benchmark_value=Decimal("1.00"),
            days=7,
            base_volume=100,
        )

        volumes = [point.volume for point in trend]
        # Volume should vary (not all the same)
        assert len(set(volumes)) > 1


class TestInterventionEffectiveness:
    """Tests for intervention effectiveness lookups."""

    def test_known_metric_intervention(self) -> None:
        """Test effectiveness for known metric/intervention combination."""
        service = SimulatedMetricsService()

        effectiveness = service.get_intervention_effectiveness(
            metric_id="losIndex",
            intervention_type="ai_agent",
        )

        assert effectiveness == Decimal("0.12")  # 12% expected improvement

    def test_clabsi_playbook_high_effectiveness(self) -> None:
        """Test that CLABSI bundles (playbooks) have high effectiveness."""
        service = SimulatedMetricsService()

        effectiveness = service.get_intervention_effectiveness(
            metric_id="clabsiRate",
            intervention_type="playbook",
        )

        # CLABSI bundles are very effective
        assert effectiveness == Decimal("0.20")

    def test_unknown_metric_uses_default(self) -> None:
        """Test that unknown metric uses default effectiveness."""
        service = SimulatedMetricsService()

        effectiveness = service.get_intervention_effectiveness(
            metric_id="unknownMetric",
            intervention_type="ai_agent",
        )

        # Should use default ai_agent effectiveness
        assert effectiveness == Decimal("0.10")

    def test_unknown_intervention_type(self) -> None:
        """Test that unknown intervention type returns minimal effectiveness."""
        service = SimulatedMetricsService()

        effectiveness = service.get_intervention_effectiveness(
            metric_id="losIndex",
            intervention_type="unknown_intervention",
        )

        # Should return default fallback
        assert effectiveness == Decimal("0.05")


class TestCurrencyFormatting:
    """Tests for currency formatting utilities."""

    def test_format_currency_basic(self) -> None:
        """Test basic currency formatting."""
        service = SimulatedMetricsService()

        assert service.format_currency(Decimal("1234.56")) == "$1,234.56"
        assert service.format_currency(Decimal("0.99")) == "$0.99"
        assert service.format_currency(Decimal("1000000")) == "$1,000,000.00"

    def test_format_large_currency_millions(self) -> None:
        """Test large currency formatting with millions."""
        service = SimulatedMetricsService()

        assert service.format_large_currency(Decimal("1234567")) == "$1.2M"
        assert service.format_large_currency(Decimal("5500000")) == "$5.5M"

    def test_format_large_currency_thousands(self) -> None:
        """Test large currency formatting with thousands."""
        service = SimulatedMetricsService()

        assert service.format_large_currency(Decimal("45000")) == "$45K"
        assert service.format_large_currency(Decimal("1500")) == "$2K"  # Rounds

    def test_format_large_currency_small(self) -> None:
        """Test large currency formatting with small amounts."""
        service = SimulatedMetricsService()

        assert service.format_large_currency(Decimal("500")) == "$500"
        assert service.format_large_currency(Decimal("99")) == "$99"

    def test_format_large_currency_negative(self) -> None:
        """Test large currency formatting with negative amounts."""
        service = SimulatedMetricsService()

        assert service.format_large_currency(Decimal("-1500000")) == "-$1.5M"
        assert service.format_large_currency(Decimal("-50000")) == "-$50K"


class TestDataclassIntegrity:
    """Tests for dataclass structure and integrity."""

    def test_metric_config_is_frozen(self) -> None:
        """Test that MetricConfig is immutable."""
        config = MetricConfig(
            metric_id="test",
            display_name="Test Metric",
            unit="units",
            typical_range_min=Decimal("0"),
            typical_range_max=Decimal("100"),
            cost_per_unit=Decimal("1000"),
            higher_is_worse=True,
            domain="Test",
        )

        with pytest.raises(AttributeError):
            config.metric_id = "changed"

    def test_financial_impact_is_frozen(self) -> None:
        """Test that FinancialImpact is immutable."""
        impact = FinancialImpact(
            daily_impact=Decimal("1000"),
            total_impact=Decimal("5000"),
            days_open=5,
            cost_per_unit=Decimal("100"),
            unit_deviation=Decimal("10"),
            volume=100,
        )

        with pytest.raises(AttributeError):
            impact.daily_impact = Decimal("2000")

    def test_trend_data_point_is_frozen(self) -> None:
        """Test that TrendDataPoint is immutable."""
        point = TrendDataPoint(
            date=datetime.now(tz=UTC),
            value=Decimal("1.15"),
            benchmark=Decimal("1.00"),
            volume=100,
        )

        with pytest.raises(AttributeError):
            point.value = Decimal("1.20")
