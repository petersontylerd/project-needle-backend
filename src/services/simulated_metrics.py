"""Simulated clinical metrics service for demo mode.

This module provides realistic metric value generation for demonstration
purposes when real clinical data is not available. It generates coherent
metric values, financial impact calculations, and trend data.

Usage:
    from src.services.simulated_metrics import SimulatedMetricsService

    service = SimulatedMetricsService()
    impact = service.calculate_financial_impact(
        metric_id="losIndex",
        metric_value=Decimal("1.15"),
        benchmark_value=Decimal("1.00"),
        encounters=150
    )
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import ClassVar

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MetricConfig:
    """Configuration for a clinical metric.

    Attributes:
        metric_id: Unique identifier for the metric.
        display_name: Human-readable metric name.
        unit: Unit of measurement (e.g., "days", "rate", "%").
        typical_range_min: Lower bound of typical values.
        typical_range_max: Upper bound of typical values.
        cost_per_unit: Cost impact per unit deviation (in USD).
        higher_is_worse: Whether higher values indicate worse performance.
        domain: Quality domain (Efficiency, Safety, Effectiveness).
    """

    metric_id: str
    display_name: str
    unit: str
    typical_range_min: Decimal
    typical_range_max: Decimal
    cost_per_unit: Decimal
    higher_is_worse: bool
    domain: str


@dataclass(frozen=True)
class FinancialImpact:
    """Calculated financial impact of a metric deviation.

    Attributes:
        daily_impact: Estimated daily cost impact in USD.
        total_impact: Total cost impact since detection in USD.
        days_open: Number of days since signal was detected.
        cost_per_unit: Cost per unit of deviation.
        unit_deviation: Amount of deviation from benchmark.
        volume: Number of encounters/patients affected.
    """

    daily_impact: Decimal
    total_impact: Decimal
    days_open: int
    cost_per_unit: Decimal
    unit_deviation: Decimal
    volume: int


@dataclass(frozen=True)
class TrendDataPoint:
    """A single data point in a metric trend.

    Attributes:
        date: Date of the data point.
        value: Metric value at this point.
        benchmark: Benchmark value at this point.
        volume: Encounter volume at this point.
    """

    date: datetime
    value: Decimal
    benchmark: Decimal
    volume: int


class SimulatedMetricsService:
    """Service for generating simulated clinical metrics for demo mode.

    This class provides realistic metric generation for demonstration purposes,
    including financial impact calculations and trend data generation.

    Attributes:
        METRIC_CONFIGS: Mapping of metric IDs to their configurations.

    Example:
        >>> service = SimulatedMetricsService()
        >>> impact = service.calculate_financial_impact(
        ...     metric_id="losIndex",
        ...     metric_value=Decimal("1.15"),
        ...     benchmark_value=Decimal("1.00"),
        ...     encounters=150
        ... )
        >>> impact.daily_impact > 0
        True
    """

    METRIC_CONFIGS: ClassVar[dict[str, MetricConfig]] = {
        # Efficiency metrics
        "losIndex": MetricConfig(
            metric_id="losIndex",
            display_name="Length of Stay Index",
            unit="days",
            typical_range_min=Decimal("0.80"),
            typical_range_max=Decimal("1.40"),
            cost_per_unit=Decimal("2500.00"),  # $2,500 per day above expected
            higher_is_worse=True,
            domain="Efficiency",
        ),
        "averageLos": MetricConfig(
            metric_id="averageLos",
            display_name="Average Length of Stay",
            unit="days",
            typical_range_min=Decimal("3.0"),
            typical_range_max=Decimal("8.0"),
            cost_per_unit=Decimal("2000.00"),
            higher_is_worse=True,
            domain="Efficiency",
        ),
        "edThroughput": MetricConfig(
            metric_id="edThroughput",
            display_name="ED Throughput",
            unit="hours",
            typical_range_min=Decimal("2.0"),
            typical_range_max=Decimal("6.0"),
            cost_per_unit=Decimal("500.00"),
            higher_is_worse=True,
            domain="Efficiency",
        ),
        "icuUtilization": MetricConfig(
            metric_id="icuUtilization",
            display_name="ICU Utilization",
            unit="%",
            typical_range_min=Decimal("70.0"),
            typical_range_max=Decimal("95.0"),
            cost_per_unit=Decimal("1500.00"),
            higher_is_worse=True,  # Over-utilization is concerning
            domain="Efficiency",
        ),
        "dischargeTime": MetricConfig(
            metric_id="dischargeTime",
            display_name="Discharge Time",
            unit="hours",
            typical_range_min=Decimal("10.0"),
            typical_range_max=Decimal("14.0"),
            cost_per_unit=Decimal("200.00"),
            higher_is_worse=True,
            domain="Efficiency",
        ),
        # Safety metrics
        "clabsiRate": MetricConfig(
            metric_id="clabsiRate",
            display_name="CLABSI Rate",
            unit="per 1000 line days",
            typical_range_min=Decimal("0.0"),
            typical_range_max=Decimal("2.0"),
            cost_per_unit=Decimal("45000.00"),  # ~$45K per CLABSI event
            higher_is_worse=True,
            domain="Safety",
        ),
        "fallRate": MetricConfig(
            metric_id="fallRate",
            display_name="Fall Rate",
            unit="per 1000 patient days",
            typical_range_min=Decimal("0.0"),
            typical_range_max=Decimal("4.0"),
            cost_per_unit=Decimal("15000.00"),  # ~$15K per fall with injury
            higher_is_worse=True,
            domain="Safety",
        ),
        "ssiRate": MetricConfig(
            metric_id="ssiRate",
            display_name="Surgical Site Infection Rate",
            unit="%",
            typical_range_min=Decimal("0.0"),
            typical_range_max=Decimal("3.0"),
            cost_per_unit=Decimal("25000.00"),
            higher_is_worse=True,
            domain="Safety",
        ),
        # Effectiveness metrics
        "readmissionRate": MetricConfig(
            metric_id="readmissionRate",
            display_name="30-Day Readmission Rate",
            unit="%",
            typical_range_min=Decimal("8.0"),
            typical_range_max=Decimal("18.0"),
            cost_per_unit=Decimal("15000.00"),  # ~$15K per readmission
            higher_is_worse=True,
            domain="Effectiveness",
        ),
        "mortalityRate": MetricConfig(
            metric_id="mortalityRate",
            display_name="Risk-Adjusted Mortality Rate",
            unit="%",
            typical_range_min=Decimal("0.5"),
            typical_range_max=Decimal("4.0"),
            cost_per_unit=Decimal("0.00"),  # Priceless - no dollar value
            higher_is_worse=True,
            domain="Effectiveness",
        ),
        "sepsisMortality": MetricConfig(
            metric_id="sepsisMortality",
            display_name="Sepsis Mortality Rate",
            unit="%",
            typical_range_min=Decimal("10.0"),
            typical_range_max=Decimal("25.0"),
            cost_per_unit=Decimal("0.00"),
            higher_is_worse=True,
            domain="Effectiveness",
        ),
        "medicationReconciliation": MetricConfig(
            metric_id="medicationReconciliation",
            display_name="Medication Reconciliation Rate",
            unit="%",
            typical_range_min=Decimal("85.0"),
            typical_range_max=Decimal("99.0"),
            cost_per_unit=Decimal("500.00"),  # Cost of adverse drug events
            higher_is_worse=False,  # Higher is better for compliance
            domain="Effectiveness",
        ),
    }

    DEFAULT_CONFIG: ClassVar[MetricConfig] = MetricConfig(
        metric_id="unknown",
        display_name="Unknown Metric",
        unit="units",
        typical_range_min=Decimal("0.0"),
        typical_range_max=Decimal("100.0"),
        cost_per_unit=Decimal("1000.00"),
        higher_is_worse=True,
        domain="Unknown",
    )

    def get_metric_config(self, metric_id: str) -> MetricConfig:
        """Get configuration for a metric.

        Args:
            metric_id: The metric identifier.

        Returns:
            MetricConfig: Configuration for the metric, or default config
                if metric is not recognized.

        Example:
            >>> service = SimulatedMetricsService()
            >>> config = service.get_metric_config("losIndex")
            >>> config.display_name
            'Length of Stay Index'
        """
        return self.METRIC_CONFIGS.get(metric_id, self.DEFAULT_CONFIG)

    def calculate_financial_impact(
        self,
        metric_id: str,
        metric_value: Decimal,
        benchmark_value: Decimal | None,
        encounters: int,
        detected_at: datetime | None = None,
    ) -> FinancialImpact:
        """Calculate the financial impact of a metric deviation.

        Computes estimated daily and total financial impact based on the
        difference between the current metric value and the benchmark,
        multiplied by the cost per unit and encounter volume.

        Args:
            metric_id: Identifier for the metric being analyzed.
            metric_value: Current observed metric value.
            benchmark_value: Expected/benchmark metric value. If None,
                uses the midpoint of the typical range.
            encounters: Number of encounters/patients affected.
            detected_at: When the signal was first detected. Defaults to now
                if not provided.

        Returns:
            FinancialImpact: Calculated financial impact data.

        Example:
            >>> service = SimulatedMetricsService()
            >>> impact = service.calculate_financial_impact(
            ...     metric_id="losIndex",
            ...     metric_value=Decimal("1.20"),
            ...     benchmark_value=Decimal("1.00"),
            ...     encounters=100
            ... )
            >>> impact.daily_impact
            Decimal('50000.00')
        """
        config = self.get_metric_config(metric_id)

        # Use benchmark or midpoint of typical range
        if benchmark_value is None:
            benchmark_value = (config.typical_range_min + config.typical_range_max) / 2

        # Calculate deviation (absolute difference in expected direction)
        unit_deviation = max(Decimal("0"), metric_value - benchmark_value) if config.higher_is_worse else max(Decimal("0"), benchmark_value - metric_value)

        # Calculate days open
        if detected_at is None:
            detected_at = datetime.now(tz=UTC)
        now = datetime.now(tz=UTC)
        days_open = max(1, (now - detected_at).days)

        # Calculate financial impact
        daily_impact = unit_deviation * config.cost_per_unit * encounters
        total_impact = daily_impact * days_open

        logger.debug(
            "Calculated financial impact for %s: daily=$%s, total=$%s",
            metric_id,
            daily_impact,
            total_impact,
        )

        return FinancialImpact(
            daily_impact=daily_impact.quantize(Decimal("0.01")),
            total_impact=total_impact.quantize(Decimal("0.01")),
            days_open=days_open,
            cost_per_unit=config.cost_per_unit,
            unit_deviation=unit_deviation.quantize(Decimal("0.0001")),
            volume=encounters,
        )

    def generate_trend_data(
        self,
        metric_id: str,
        current_value: Decimal,
        benchmark_value: Decimal | None,
        days: int = 30,
        volatility: Decimal = Decimal("0.05"),
        base_volume: int = 100,
    ) -> list[TrendDataPoint]:
        """Generate simulated trend data for a metric.

        Creates realistic-looking historical trend data for display in
        charts and analysis. Uses a simple random walk with mean reversion
        toward the benchmark.

        Args:
            metric_id: Identifier for the metric.
            current_value: The current (most recent) metric value.
            benchmark_value: Expected/benchmark value. If None, uses the
                midpoint of the typical range.
            days: Number of days of historical data to generate.
            volatility: Daily volatility factor (0.05 = 5% daily variation).
            base_volume: Base encounter volume per day.

        Returns:
            list[TrendDataPoint]: List of trend data points, oldest first.

        Example:
            >>> service = SimulatedMetricsService()
            >>> trend = service.generate_trend_data(
            ...     metric_id="losIndex",
            ...     current_value=Decimal("1.15"),
            ...     benchmark_value=Decimal("1.00"),
            ...     days=7
            ... )
            >>> len(trend)
            7
        """
        config = self.get_metric_config(metric_id)

        if benchmark_value is None:
            benchmark_value = (config.typical_range_min + config.typical_range_max) / 2

        trend_data: list[TrendDataPoint] = []
        now = datetime.now(tz=UTC)

        # Work backwards from current value
        # Use a simple linear interpolation + noise for demo purposes
        for i in range(days - 1, -1, -1):
            date = now - timedelta(days=i)

            # Linear interpolation from benchmark toward current value
            # with position factor (0 at oldest, 1 at newest)
            position = (days - 1 - i) / max(1, days - 1)

            # Interpolate between benchmark and current
            base_value = benchmark_value + (current_value - benchmark_value) * Decimal(str(position))

            # Add deterministic "noise" based on day offset
            # Use modulo to create repeatable but varied pattern
            noise_factor = ((i * 7 + 3) % 11 - 5) / 100  # -0.05 to +0.05
            noise = base_value * Decimal(str(noise_factor)) * volatility * 10

            value = base_value + noise

            # Clamp to typical range
            value = max(config.typical_range_min, min(config.typical_range_max, value))

            # Volume varies by day of week (weekends lower)
            weekday = date.weekday()
            volume_factor = 0.6 if weekday >= 5 else 1.0  # noqa: PLR2004 (weekend check)
            volume = int(base_volume * volume_factor * (0.9 + (i % 3) * 0.1))

            trend_data.append(
                TrendDataPoint(
                    date=date,
                    value=value.quantize(Decimal("0.01")),
                    benchmark=benchmark_value.quantize(Decimal("0.01")),
                    volume=volume,
                )
            )

        # Ensure the last point matches current value exactly
        if trend_data:
            trend_data[-1] = TrendDataPoint(
                date=trend_data[-1].date,
                value=current_value,
                benchmark=benchmark_value.quantize(Decimal("0.01")),
                volume=trend_data[-1].volume,
            )

        logger.debug(
            "Generated %d trend data points for %s",
            len(trend_data),
            metric_id,
        )

        return trend_data

    def get_intervention_effectiveness(
        self,
        metric_id: str,
        intervention_type: str,
    ) -> Decimal:
        """Get expected effectiveness of an intervention type for a metric.

        Returns a simulated expected improvement percentage based on
        the metric and intervention type combination.

        Args:
            metric_id: The metric being targeted.
            intervention_type: Type of intervention ("ai_agent", "playbook",
                "protocol_change", "staffing", "education").

        Returns:
            Decimal: Expected percentage improvement (e.g., 0.15 for 15%).

        Example:
            >>> service = SimulatedMetricsService()
            >>> effectiveness = service.get_intervention_effectiveness(
            ...     metric_id="losIndex",
            ...     intervention_type="ai_agent"
            ... )
            >>> effectiveness > 0
            True
        """
        # Simulated effectiveness matrix
        # In a real system, this would come from historical outcome data
        effectiveness_matrix: dict[str, dict[str, Decimal]] = {
            "losIndex": {
                "ai_agent": Decimal("0.12"),
                "playbook": Decimal("0.08"),
                "protocol_change": Decimal("0.10"),
                "staffing": Decimal("0.05"),
                "education": Decimal("0.04"),
            },
            "clabsiRate": {
                "ai_agent": Decimal("0.15"),
                "playbook": Decimal("0.20"),  # Bundles are very effective
                "protocol_change": Decimal("0.18"),
                "staffing": Decimal("0.08"),
                "education": Decimal("0.12"),
            },
            "readmissionRate": {
                "ai_agent": Decimal("0.10"),
                "playbook": Decimal("0.12"),
                "protocol_change": Decimal("0.08"),
                "staffing": Decimal("0.06"),
                "education": Decimal("0.05"),
            },
            "fallRate": {
                "ai_agent": Decimal("0.08"),
                "playbook": Decimal("0.15"),
                "protocol_change": Decimal("0.12"),
                "staffing": Decimal("0.10"),
                "education": Decimal("0.08"),
            },
        }

        # Default effectiveness if metric/intervention not in matrix
        default_effectiveness: dict[str, Decimal] = {
            "ai_agent": Decimal("0.10"),
            "playbook": Decimal("0.08"),
            "protocol_change": Decimal("0.07"),
            "staffing": Decimal("0.05"),
            "education": Decimal("0.04"),
        }

        metric_matrix = effectiveness_matrix.get(metric_id, default_effectiveness)
        return metric_matrix.get(intervention_type, Decimal("0.05"))

    def format_currency(self, amount: Decimal) -> str:
        """Format a decimal amount as USD currency.

        Args:
            amount: Amount in USD to format.

        Returns:
            str: Formatted currency string (e.g., "$1,234.56").

        Example:
            >>> service = SimulatedMetricsService()
            >>> service.format_currency(Decimal("1234.56"))
            '$1,234.56'
        """
        return f"${amount:,.2f}"

    def format_large_currency(self, amount: Decimal) -> str:
        """Format a large decimal amount as abbreviated USD.

        Args:
            amount: Amount in USD to format.

        Returns:
            str: Abbreviated currency string (e.g., "$1.2M", "$45K").

        Example:
            >>> service = SimulatedMetricsService()
            >>> service.format_large_currency(Decimal("1234567"))
            '$1.2M'
        """
        abs_amount = abs(amount)
        sign = "-" if amount < 0 else ""

        if abs_amount >= 1_000_000:
            return f"{sign}${abs_amount / 1_000_000:.1f}M"
        elif abs_amount >= 1_000:
            return f"{sign}${abs_amount / 1_000:.0f}K"
        else:
            return f"{sign}${abs_amount:.0f}"
