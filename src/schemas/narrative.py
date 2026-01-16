"""Pydantic schemas for narrative insights API responses.

These schemas define the structure for narrative data returned by the
narrative API endpoints. They mirror the dataclasses in narrative_service.py
but are optimized for API serialization.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ContributorSummaryResponse(BaseModel):
    """API response schema for contributor summary.

    Attributes:
        dimension: Dimension type (Payer, Discharge, Service Line, etc.).
        segment: Segment name.
        excess: Contribution excess value (+/-).
        trend: Trend indicator with percentage (e.g., "- -79%").
        flags: Optional flags (Double, Triple, etc.).
    """

    model_config = ConfigDict(from_attributes=True)

    dimension: str = Field(description="Dimension type")
    segment: str = Field(description="Segment name")
    excess: float = Field(description="Contribution excess value")
    trend: str | None = Field(default=None, description="Trend indicator")
    flags: list[str] = Field(default_factory=list, description="Optional flags")


class ExecutiveSummaryResponse(BaseModel):
    """API response schema for executive summary.

    Attributes:
        metric_value: Primary metric value.
        total_segments: Total segments analyzed.
        facility_segments: Facility-level segments count.
        pareto_insight: Top N segments account for X% insight.
        top_contributors_higher: Top contributors to higher LOS.
        top_contributors_lower: Top contributors to lower LOS.
    """

    model_config = ConfigDict(from_attributes=True)

    metric_value: float = Field(description="Primary metric value")
    total_segments: int = Field(description="Total segments analyzed")
    facility_segments: int = Field(description="Facility-level segments")
    pareto_insight: str = Field(description="Pareto insight text")
    top_contributors_higher: list[ContributorSummaryResponse] = Field(
        default_factory=list,
        description="Top contributors to higher LOS",
    )
    top_contributors_lower: list[ContributorSummaryResponse] = Field(
        default_factory=list,
        description="Top contributors to lower LOS",
    )


class MetricComparisonResponse(BaseModel):
    """API response schema for metric comparison.

    Attributes:
        metric_name: Display name of the metric.
        value: Metric value.
        z_score: Z-score vs peers.
        peer_status: Peer status label (e.g., "moderately high").
    """

    model_config = ConfigDict(from_attributes=True)

    metric_name: str = Field(description="Metric display name")
    value: float = Field(description="Metric value")
    z_score: float = Field(description="Z-score vs peers")
    peer_status: str = Field(description="Peer status label")


class ParetoSegmentResponse(BaseModel):
    """API response schema for Pareto segment.

    Attributes:
        segment_name: Segment name.
        excess: Contribution excess value.
        cumulative_pct: Cumulative contribution percentage.
    """

    model_config = ConfigDict(from_attributes=True)

    segment_name: str = Field(description="Segment name")
    excess: float = Field(description="Contribution excess")
    cumulative_pct: float = Field(description="Cumulative percentage")


class ParetoAnalysisResponse(BaseModel):
    """API response schema for Pareto analysis.

    Attributes:
        positive_excess: Segments adding to metric (worse).
        negative_excess: Segments reducing metric (better).
        top_n_positive_pct: Top N positive account for X%.
        top_n_negative_pct: Top N negative account for X%.
    """

    model_config = ConfigDict(from_attributes=True)

    positive_excess: list[ParetoSegmentResponse] = Field(
        default_factory=list,
        description="Positive excess segments",
    )
    negative_excess: list[ParetoSegmentResponse] = Field(
        default_factory=list,
        description="Negative excess segments",
    )
    top_n_positive_pct: float = Field(description="Top N positive percentage")
    top_n_negative_pct: float = Field(description="Top N negative percentage")


class DriverResponse(BaseModel):
    """API response schema for driver.

    Attributes:
        rank: Driver rank.
        dimension: Dimension type.
        segment: Segment name.
        value: Metric value.
        weight: Segment weight percentage.
        excess: Contribution excess.
        trend: Trend indicator.
        z_score: Z-score vs peers.
        peer_status: Peer status label.
        interpretation: Human-readable interpretation.
    """

    model_config = ConfigDict(from_attributes=True)

    rank: int = Field(description="Driver rank")
    dimension: str = Field(description="Dimension type")
    segment: str = Field(description="Segment name")
    value: float = Field(description="Metric value")
    weight: float = Field(description="Weight percentage")
    excess: float = Field(description="Contribution excess")
    trend: str | None = Field(default=None, description="Trend indicator")
    z_score: float = Field(description="Z-score vs peers")
    peer_status: str = Field(description="Peer status label")
    interpretation: str = Field(description="Human-readable interpretation")


class TopDriversResponse(BaseModel):
    """API response schema for top drivers.

    Attributes:
        higher_los: Drivers contributing to higher LOS.
        lower_los: Drivers contributing to lower LOS.
    """

    model_config = ConfigDict(from_attributes=True)

    higher_los: list[DriverResponse] = Field(
        default_factory=list,
        description="Higher LOS drivers",
    )
    lower_los: list[DriverResponse] = Field(
        default_factory=list,
        description="Lower LOS drivers",
    )


class InsightItemResponse(BaseModel):
    """API response schema for insight item.

    Attributes:
        segment: Segment name.
        excess: Contribution excess.
        z_score: Z-score vs peers.
        peer_status: Peer status label.
    """

    model_config = ConfigDict(from_attributes=True)

    segment: str = Field(description="Segment name")
    excess: float = Field(description="Contribution excess")
    z_score: float = Field(description="Z-score vs peers")
    peer_status: str = Field(description="Peer status label")


class InsightCategoriesResponse(BaseModel):
    """API response schema for insight categories.

    Attributes:
        double_trouble: High excess AND unusual vs peers.
        internal_issue: High excess but normal vs peers.
    """

    model_config = ConfigDict(from_attributes=True)

    double_trouble: list[InsightItemResponse] = Field(
        default_factory=list,
        description="Double trouble insights",
    )
    internal_issue: list[InsightItemResponse] = Field(
        default_factory=list,
        description="Internal issue insights",
    )


class HierarchyNodeResponse(BaseModel):
    """API response schema for hierarchy node.

    Attributes:
        level: Level type (SL=Service Line, SSL=Sub-Service Line).
        segment: Segment name.
        value: Metric value.
        weight: Segment weight percentage.
        excess: Contribution excess.
        z_score: Z-score vs peers.
        peer_status: Peer status label.
        interpretation: Human-readable interpretation.
        children: Child nodes.
    """

    model_config = ConfigDict(from_attributes=True)

    level: str = Field(description="Level type (SL or SSL)")
    segment: str = Field(description="Segment name")
    value: float = Field(description="Metric value")
    weight: float = Field(description="Weight percentage")
    excess: float = Field(description="Contribution excess")
    z_score: float = Field(description="Z-score vs peers")
    peer_status: str = Field(description="Peer status label")
    interpretation: str = Field(description="Human-readable interpretation")
    children: list["HierarchyNodeResponse"] = Field(
        default_factory=list,
        description="Child nodes",
    )


class NarrativeInsightsResponse(BaseModel):
    """API response schema for full narrative insights.

    Attributes:
        facility_id: Medicare facility identifier.
        metric_value: Primary metric value (e.g., LOS Index).
        generated_at: Timestamp when narrative was generated.
        executive_summary: Extracted executive summary.
        cross_metric_comparison: Peer comparison data.
        pareto_analysis: Pareto cumulative impact data.
        top_drivers: Ranked driver tables.
        insights: Categorized insight lists.
        hierarchical_breakdown: Service line hierarchy.
    """

    model_config = ConfigDict(from_attributes=True)

    facility_id: str = Field(description="Medicare facility identifier")
    metric_value: float = Field(description="Primary metric value")
    generated_at: datetime = Field(description="Generation timestamp")
    executive_summary: ExecutiveSummaryResponse = Field(
        description="Executive summary",
    )
    cross_metric_comparison: list[MetricComparisonResponse] = Field(
        default_factory=list,
        description="Cross-metric comparisons",
    )
    pareto_analysis: ParetoAnalysisResponse = Field(
        description="Pareto analysis",
    )
    top_drivers: TopDriversResponse = Field(
        description="Top drivers",
    )
    insights: InsightCategoriesResponse = Field(
        description="Categorized insights",
    )
    hierarchical_breakdown: list[HierarchyNodeResponse] = Field(
        default_factory=list,
        description="Hierarchy breakdown",
    )


class NarrativeSummaryResponse(BaseModel):
    """API response schema for narrative summary (lightweight).

    Attributes:
        facility_id: Medicare facility identifier.
        metric_value: Primary metric value.
        pareto_insight: Pareto insight text.
        top_contributors_higher: Top contributors to higher LOS.
        top_contributors_lower: Top contributors to lower LOS.
    """

    model_config = ConfigDict(from_attributes=True)

    facility_id: str = Field(description="Medicare facility identifier")
    metric_value: float = Field(description="Primary metric value")
    pareto_insight: str = Field(description="Pareto insight text")
    top_contributors_higher: list[ContributorSummaryResponse] = Field(
        default_factory=list,
        description="Top contributors to higher LOS",
    )
    top_contributors_lower: list[ContributorSummaryResponse] = Field(
        default_factory=list,
        description="Top contributors to lower LOS",
    )


class NarrativeListResponse(BaseModel):
    """API response schema for listing available narratives.

    Attributes:
        facilities: List of facility IDs with narratives.
        count: Number of available narratives.
    """

    model_config = ConfigDict(from_attributes=True)

    facilities: list[str] = Field(description="Facility IDs with narratives")
    count: int = Field(description="Count of available narratives")
