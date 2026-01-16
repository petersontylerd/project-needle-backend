"""Pydantic schemas for signal creation and validation.

These schemas define the structure for creating signals from parsed node results
and for API request/response models.
"""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

from src.db.models import (
    SignalDomain,
)

# Custom type that serializes Decimal as float for JSON responses
SerializedDecimal = Annotated[
    Decimal,
    PlainSerializer(lambda x: float(x) if x is not None else None, return_type=float | None),
]


class MetricTrendPeriod(BaseModel):
    """Single period in a metric trend timeline.

    Attributes:
        period: Period identifier (e.g., "202407").
        value: Metric value for the period.
        encounters: Number of encounters in the period.
    """

    period: str
    value: float | None = None
    encounters: int | None = None


class MetadataPerPeriodValue(BaseModel):
    """A single value in a per-period metadata array.

    Attributes:
        period: Time period identifier (e.g., "202401").
        value: Value for this period.

    Example:
        >>> value = MetadataPerPeriodValue(period="202401", value=150)
    """

    period: str = Field(description="Time period identifier (e.g., '202401')")
    value: int | float = Field(description="Value for this period")


class MetadataPerPeriod(BaseModel):
    """Per-period breakdown of metadata values for temporal nodes.

    Contains arrays of values broken down by time period, enabling
    temporal analysis of metadata like encounter counts.

    Attributes:
        encounters: Encounters per period for temporal analysis.

    Example:
        >>> per_period = MetadataPerPeriod(
        ...     encounters=[
        ...         MetadataPerPeriodValue(period="202401", value=100),
        ...         MetadataPerPeriodValue(period="202402", value=120),
        ...     ]
        ... )
    """

    encounters: list[MetadataPerPeriodValue] | None = Field(
        default=None,
        description="Encounters per period for temporal analysis",
    )


class SignalMetadata(BaseModel):
    """Flexible metadata for signal enrichment with typed per_period support.

    Extends the base metadata dict with structured per_period data.
    Uses extra="allow" to support additional dynamic fields beyond
    the typed fields.

    Attributes:
        encounters: Total encounter count.
        system_name: Health system name (aliased as systemName in JSON).
        per_period: Per-period breakdown of metadata values.

    Example:
        >>> metadata = SignalMetadata(
        ...     encounters=220,
        ...     system_name="Alpha Health System",
        ...     per_period=MetadataPerPeriod(
        ...         encounters=[MetadataPerPeriodValue(period="202401", value=100)]
        ...     ),
        ... )
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    encounters: int | None = Field(default=None, description="Total encounter count")
    system_name: str | None = Field(
        default=None,
        alias="systemName",
        description="Health system name",
    )
    per_period: MetadataPerPeriod | None = Field(
        default=None,
        description="Per-period breakdown of metadata values",
    )


class PercentileTrendsSchema(BaseModel):
    """Peer distribution percentile trends over time.

    Contains parallel arrays of percentile values for each period, enabling
    visualization of reference bands on trend charts.

    Attributes:
        periods: Ordered list of period strings (e.g., ["202407", "202408", ...]).
        p10: 10th percentile values per period (None if suppressed).
        p25: 25th percentile values per period (None if suppressed).
        p50: 50th percentile (median) values per period (None if suppressed).
        p75: 75th percentile values per period (None if suppressed).
        p90: 90th percentile values per period (None if suppressed).
        sample_sizes: Number of entities contributing to each period.
        suppression_applied: Dict mapping percentile name to suppression flags.
    """

    model_config = ConfigDict(from_attributes=True)

    periods: list[str]
    p10: list[float | None]
    p25: list[float | None]
    p50: list[float | None]
    p75: list[float | None]
    p90: list[float | None]
    sample_sizes: list[int]
    suppression_applied: dict[str, list[bool]] | None = None


class SignalCreate(BaseModel):
    """Schema for creating a new consolidated signal from parsed node results.

    Signals are consolidated per entity/metric pair, combining aggregate and
    temporal statistical analysis into a single record.

    Note: Technical details (z-scores, anomaly labels, trend metrics, classification
    tiers) are served via SignalTechnicalDetails schema on-demand.

    Attributes:
        canonical_node_id: Reference to source node in insight graph.
        metric_id: Metric identifier (e.g., "losIndex").
        domain: Quality domain classification.
        facility: Facility/Site of Care name (medicareId value).
        facility_id: Medicare ID or facility code.
        service_line: Primary service line.
        sub_service_line: Sub-service line if present.
        description: Human-readable signal description from interpretation.
        metric_value: Current metric value from aggregate node.
        peer_mean: Peer cohort mean (benchmark value).
        peer_std: Peer cohort standard deviation.
        percentile_rank: Position in peer distribution.
        encounters: Number of patient encounters.
        detected_at: When anomaly was detected.
        temporal_node_id: Reference to temporal node via trends_to edge.
        entity_dimensions: Entity dimension key-value pairs (excluding medicareId).
        entity_dimensions_hash: MD5 hash of entity_dimensions for unique constraint.
        groupby_label: Human-readable label for groupby dimension(s).
        group_value: Entity dimension value(s) for the group.
        metric_trend_timeline: Timeline of metric values for sparkline visualization.
        trend_direction: Direction of trend (increasing, decreasing, stable).

    Example:
        >>> signal = SignalCreate(
        ...     canonical_node_id="losIndex__medicareId__aggregate_time_period",
        ...     metric_id="losIndex",
        ...     domain=SignalDomain.EFFICIENCY,
        ...     facility="AFP658",
        ...     service_line="Cardiology",
        ...     description="Medicare Id AFP658 shows elevated z-score...",
        ...     metric_value=Decimal("1.25"),
        ...     groupby_label="Facility-wide",
        ...     group_value="Facility-wide",
        ...     detected_at=datetime.now(),
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    canonical_node_id: str
    metric_id: str
    domain: SignalDomain
    facility: str
    facility_id: str | None = None
    system_name: str | None = None
    service_line: str
    sub_service_line: str | None = None
    description: str
    metric_value: Decimal
    peer_mean: Decimal | None = None
    peer_std: Decimal | None = None
    percentile_rank: Decimal | None = None
    encounters: int | None = None
    detected_at: datetime
    temporal_node_id: str | None = None

    # Entity identification and grouping
    entity_dimensions: dict[str, str] | None = None
    entity_dimensions_hash: str | None = None
    groupby_label: str | None = None
    group_value: str | None = None

    # Metric trend timeline for sparkline visualization
    metric_trend_timeline: list[MetricTrendPeriod] | None = None

    # Trend direction
    trend_direction: str | None = None

    # Peer percentile reference trends
    peer_percentile_trends: dict[str, Any] | None = None

    # Technical details for temporal analysis
    simple_zscore: Decimal | None = None
    slope_percentile: Decimal | None = None
    monthly_z_scores: list[float] | None = None

    # Business impact metrics
    annual_excess_cost: Decimal | None = None
    excess_los_days: Decimal | None = None
    capacity_impact_bed_days: Decimal | None = None
    expected_metric_value: Decimal | None = None
    why_matters_narrative: str | None = None


class SignalResponse(BaseModel):
    """Schema for consolidated signal API response.

    Includes all signal fields plus computed fields like days_open.
    Signals are consolidated per entity/metric pair with both aggregate
    and temporal statistical analysis.

    Note: Technical details (z-scores, anomaly labels, trend metrics, classification
    tiers) are served via SignalTechnicalDetails schema on-demand.

    Attributes:
        id: Signal UUID.
        canonical_node_id: Reference to source node.
        metric_id: Metric identifier.
        domain: Quality domain.
        facility: Facility name.
        facility_id: Facility identifier.
        service_line: Service line.
        sub_service_line: Sub-service line.
        description: Signal description.
        metric_value: Current metric value from aggregate node.
        peer_mean: Peer cohort mean (benchmark value).
        peer_std: Peer cohort standard deviation.
        percentile_rank: Percentile rank.
        encounters: Encounter count.
        detected_at: Detection timestamp.
        created_at: Creation timestamp.
        days_open: Days since detection (computed).
        simplified_signal_type: One of 9 signal types for classification.
        simplified_severity: Severity score 0-100 within signal type's range.
        temporal_node_id: Reference to temporal node via trends_to edge.
        has_children: Whether signal has child signals via drills_to edges.
        has_parent: Whether signal has a parent signal via drills_to edges.
        edge_types: Available edge types from this signal.
        related_signal_count: Count of related signals via relates_to edges.
        entity_dimensions: Entity dimension key-value pairs.
        groupby_label: Human-readable label for groupby dimension(s).
        group_value: Entity dimension value(s) for the group.
        metric_trend_timeline: Timeline of metric values for sparkline.
        trend_direction: Direction of trend.

    Example:
        >>> response = SignalResponse(
        ...     id=uuid4(),
        ...     canonical_node_id="losIndex__medicareId__aggregate_time_period",
        ...     metric_id="losIndex",
        ...     domain=SignalDomain.EFFICIENCY,
        ...     groupby_label="Facility-wide",
        ...     group_value="Facility-wide",
        ...     ...
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    canonical_node_id: str
    metric_id: str
    domain: SignalDomain
    facility: str
    facility_id: str | None = None
    system_name: str | None = None
    service_line: str
    sub_service_line: str | None = None
    description: str
    metric_value: SerializedDecimal
    peer_mean: SerializedDecimal | None = None
    peer_std: SerializedDecimal | None = None
    percentile_rank: SerializedDecimal | None = None
    encounters: int | None = None
    detected_at: datetime
    created_at: datetime
    days_open: int = Field(default=0, description="Days since detection")

    # 9 Signal Type classification
    simplified_signal_type: str | None = Field(
        default=None,
        description="One of 9 signal types: suspect_data, sustained_excellence, improving_leader, baseline, emerging_risk, volatility_alert, recovering, chronic_underperformer, critical_trajectory",
    )
    simplified_severity: int | None = Field(default=None, description="Severity score 0-100 within the signal type's range")

    temporal_node_id: str | None = None

    # Navigation fields for hierarchical drill-down
    has_children: bool = Field(default=False, description="Whether signal has child signals via drills_to edges")
    has_parent: bool = Field(default=False, description="Whether signal has a parent signal via drills_to edges")
    edge_types: list[str] = Field(default_factory=list, description="Available edge types from this signal")
    related_signal_count: int = Field(default=0, description="Count of related signals via relates_to edges")

    # Entity identification and grouping
    entity_dimensions: dict[str, str] | None = Field(default=None, description="Entity dimension key-value pairs")
    groupby_label: str | None = Field(default=None, description="Human-readable label for groupby dimension(s)")
    group_value: str | None = Field(default=None, description="Entity dimension value(s) for the group")

    # Metric trend timeline for sparkline visualization
    metric_trend_timeline: list[MetricTrendPeriod] | None = Field(default=None, description="Timeline of metric values for sparkline")

    # Trend direction
    trend_direction: str | None = Field(default=None, description="Direction of trend")

    # Business impact metrics
    annual_excess_cost: SerializedDecimal | None = Field(default=None, description="Annual excess cost in dollars")
    excess_los_days: SerializedDecimal | None = Field(default=None, description="Total excess length of stay days")
    capacity_impact_bed_days: SerializedDecimal | None = Field(default=None, description="Bed-days per day capacity impact")
    expected_metric_value: SerializedDecimal | None = Field(default=None, description="Expected/benchmark metric value")
    why_matters_narrative: str | None = Field(default=None, description="User-editable business impact narrative")

    # Workflow status from assignment
    workflow_status: str = Field(default="new", description="Current workflow status from assignment")

    # Extensible metadata for flexible key-value storage
    metadata: dict[str, Any] | None = Field(default=None, description="Flexible JSON metadata for signal enrichment")

    # Per-period metadata for temporal analysis
    metadata_per_period: dict[str, Any] | None = Field(
        default=None,
        description="Per-period breakdown of metadata values (encounters per month, etc.)",
    )

    # Peer percentile reference trends for visualization
    peer_percentile_trends: PercentileTrendsSchema | None = Field(
        default=None,
        description="Peer distribution percentile trends (p10-p90) for reference band visualization",
    )


class SignalUpdate(BaseModel):
    """Schema for partial signal updates via PATCH endpoint.

    All fields are optional - only provided fields will be updated.
    Used for inline editing of signal description and narrative.
    """

    description: str | None = Field(default=None, description="Updated signal description")
    why_matters_narrative: str | None = Field(default=None, description="Updated business impact narrative")


class SignalTechnicalDetails(BaseModel):
    """Technical details for signal drill-down view.

    These fields are fetched on-demand when a user clicks to see
    technical details, not included in list views.
    """

    # Z-score metrics
    simple_zscore: SerializedDecimal | None = Field(default=None, description="Simple z-score from aggregate node")
    robust_zscore: SerializedDecimal | None = Field(default=None, description="Robust z-score (MAD-based)")
    latest_simple_zscore: SerializedDecimal | None = Field(default=None, description="Most recent period simple z-score")
    mean_simple_zscore: SerializedDecimal | None = Field(default=None, description="Mean of temporal simple z-scores")
    latest_robust_zscore: SerializedDecimal | None = Field(default=None, description="Most recent period robust z-score")
    mean_robust_zscore: SerializedDecimal | None = Field(default=None, description="Mean of temporal robust z-scores")

    # Anomaly labels
    simple_zscore_anomaly: str | None = Field(default=None, description="Anomaly tier from simple z-score")
    robust_zscore_anomaly: str | None = Field(default=None, description="Anomaly tier from robust z-score")
    latest_simple_zscore_anomaly: str | None = Field(default=None, description="Anomaly tier from latest simple z-score")
    mean_simple_zscore_anomaly: str | None = Field(default=None, description="Anomaly tier from mean simple z-score")
    latest_robust_zscore_anomaly: str | None = Field(default=None, description="Anomaly tier from latest robust z-score")
    mean_robust_zscore_anomaly: str | None = Field(default=None, description="Anomaly tier from mean robust z-score")

    # Trend metrics
    slope: SerializedDecimal | None = Field(default=None, description="Linear regression slope")
    slope_percentile: SerializedDecimal | None = Field(default=None, description="Slope percentile vs peers")
    monthly_z_scores: list[float] | None = Field(default=None, description="Per-period z-score values")
    acceleration: SerializedDecimal | None = Field(default=None, description="Second derivative of trend")
    momentum: str | None = Field(default=None, description="Momentum indicator (accelerating/slowing/stable)")

    # Classification tiers
    magnitude_tier: str | None = Field(default=None, description="Magnitude tier (CRITICAL/ELEVATED/MODERATE/NORMAL)")
    trajectory_tier: str | None = Field(default=None, description="Trajectory tier (DETERIORATING/STABLE/IMPROVING)")
    consistency_tier: str | None = Field(default=None, description="Consistency tier (PERSISTENT/VARIABLE/TRANSIENT)")

    # Simplified 9 Signal Type classification (glass box details)
    simplified_signal_type: str | None = Field(default=None, description="One of 9 signal types")
    simplified_severity: int | None = Field(default=None, description="Severity score 0-100")
    simplified_severity_range: list[int] | None = Field(default=None, description="[min, max] valid severity range for this signal type")
    simplified_inputs: dict[str, Any] | None = Field(
        default=None, description="5 input values: aggregate_zscore, slope_percentile, acceleration, zscore_std, max_abs_deviation"
    )
    simplified_indicators: dict[str, Any] | None = Field(
        default=None, description="Categorical interpretations: magnitude_level, trajectory_direction, volatility_level, deviation_level, trend_stability"
    )
    simplified_reasoning: str | None = Field(default=None, description="Human-readable explanation of classification decision")
    simplified_severity_calculation: dict[str, Any] | None = Field(
        default=None, description="Severity calculation breakdown: base_severity, refinements, final_severity"
    )

    # Data quality indicators (nullable - may not be available)
    data_quality_fallback_rate: SerializedDecimal | None = Field(default=None, description="Proportion of calculations using fallback methods (0.0-1.0)")
    data_quality_missing_rate: SerializedDecimal | None = Field(default=None, description="Proportion of missing input data (0.0-1.0)")
    data_quality_suppressed: bool | None = Field(default=None, description="Whether results were suppressed due to data quality issues")

    model_config = ConfigDict(from_attributes=True)


class SignalTemporalResponse(BaseModel):
    """Schema for signal temporal data API response.

    Returns temporal trend data for a signal including monthly z-scores
    and trend metrics. Used by the GET /signals/{id}/temporal endpoint.

    Attributes:
        signal_id: UUID of the signal.
        temporal_node_id: Reference to temporal node via trends_to edge.
        slope_percentile: Slope percentile relative to population (0-100).
        monthly_z_scores: Array of monthly z-scores for trend analysis.
        monthly_values: Array of monthly metric values (if available).
        has_temporal_data: Whether temporal data is available for this signal.

    Example:
        >>> response = SignalTemporalResponse(
        ...     signal_id="550e8400-e29b-41d4-a716-446655440000",
        ...     temporal_node_id="losIndex__medicareId__dischargeMonth",
        ...     slope_percentile=25.5,
        ...     monthly_z_scores=[-0.2, -0.4, -0.5, ...],
        ...     monthly_values=[1.02, 1.04, 1.05, ...],
        ...     has_temporal_data=True,
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    signal_id: str
    temporal_node_id: str | None = None
    slope_percentile: SerializedDecimal | None = None
    monthly_z_scores: list[float] | None = None
    monthly_values: list[float] | None = None
    has_temporal_data: bool = False


# =============================================================================
# Node Results JSON Parsing Schemas
# =============================================================================


class NodeEntityField(BaseModel):
    """Entity field from node results JSON.

    Attributes:
        dataset_field: Original dataset field name.
        id: Entity identifier (e.g., "medicareId", "vizientServiceLine").
        value: Entity value.

    Example:
        >>> field = NodeEntityField(
        ...     dataset_field="medicareId",
        ...     id="medicareId",
        ...     value="AFP658",
        ... )
    """

    dataset_field: str
    id: str
    value: str


class NodeMetricMetadata(BaseModel):
    """Metric metadata from node results JSON.

    Attributes:
        metric_id: Metric identifier (e.g., "losIndex").

    Example:
        >>> metadata = NodeMetricMetadata(metric_id="losIndex")
    """

    metric_id: str


class TemporalPeriod(BaseModel):
    """Single period in a temporal timeline.

    Attributes:
        period: Period identifier (e.g., "202407").
        value: Metric value for the period (can be None for missing data).
        encounters: Number of encounters in the period.

    Example:
        >>> period = TemporalPeriod(period="202407", value=1.25, encounters=2800)
    """

    period: str
    value: float | None = None
    encounters: int | None = None


class TemporalTimeline(BaseModel):
    """Timeline of values for temporal nodes.

    Attributes:
        timeline: List of period values.

    Example:
        >>> timeline = TemporalTimeline(timeline=[TemporalPeriod(...)])
    """

    timeline: list[TemporalPeriod]


class TemporalMetricRuntime(BaseModel):
    """Runtime metadata for temporal metrics.

    Attributes:
        coverage_ratio: Ratio of periods with data.
        missing_count: Number of missing periods.
        observed_count: Number of observed periods.
        fill_strategy: Strategy used for filling missing values.

    Example:
        >>> runtime = TemporalMetricRuntime(coverage_ratio=1.0, observed_count=12)
    """

    coverage_ratio: float | None = None
    missing_count: int | None = None
    observed_count: int | None = None
    fill_strategy: str | None = None


class NodeMetric(BaseModel):
    """Metric data from node results JSON.

    Attributes:
        metadata: Metric metadata containing metric_id.
        values: Metric value (scalar for aggregate, timeline for temporal).
        temporal_runtime: Runtime metadata for temporal metrics.

    Example:
        >>> metric = NodeMetric(
        ...     metadata=NodeMetricMetadata(metric_id="losIndex"),
        ...     values=1.25,
        ... )
    """

    metadata: NodeMetricMetadata
    values: float | TemporalTimeline | None = None
    temporal_runtime: TemporalMetricRuntime | None = None

    def get_timeline_values(self) -> list[float] | None:
        """Extract timeline values if this is a temporal metric.

        Returns:
            list[float]: List of metric values for each period (None values filtered),
                or None if this is an aggregate metric or all values are None.
        """
        if isinstance(self.values, TemporalTimeline):
            values = [p.value for p in self.values.timeline if p.value is not None]
            return values if values else None
        return None


class NodeInterpretation(BaseModel):
    """Interpretation text from anomaly method.

    Attributes:
        rendered: Human-readable interpretation text.
        template_id: Template identifier for the interpretation.

    Example:
        >>> interp = NodeInterpretation(
        ...     rendered="Medicare Id AFP658 shows elevated z-score...",
        ...     template_id="interpretation_template__simple_zscore__...",
        ... )
    """

    rendered: str
    template_id: str | None = None


class NodeAnomalyMethod(BaseModel):
    """Anomaly method result from node results JSON.

    Attributes:
        anomaly: Anomaly classification (e.g., "severe_high", "moderately_high", "normal").
        anomaly_method: Method identifier.
        applies_to: Statistic this method applies to.
        interpretation: Human-readable interpretation.
        statistic_value: The statistic value used for classification (can be None).

    Example:
        >>> method = NodeAnomalyMethod(
        ...     anomaly="moderately_high",
        ...     anomaly_method="anomaly_method__simple_zscore__...",
        ...     applies_to="simple_zscore",
        ...     interpretation=NodeInterpretation(rendered="..."),
        ...     statistic_value=1.10,
        ... )
    """

    anomaly: str
    anomaly_method: str | None = None
    applies_to: str | None = None
    interpretation: NodeInterpretation
    statistic_value: float | None


class NodeAnomaly(BaseModel):
    """Anomaly detection results from node results JSON.

    Attributes:
        anomaly_profile: Profile identifier for anomaly detection.
        methods: List of anomaly methods applied.

    Example:
        >>> anomaly = NodeAnomaly(
        ...     anomaly_profile="anomaly_profiles__simple_zscore__aggregate",
        ...     methods=[NodeAnomalyMethod(...)],
        ... )
    """

    anomaly_profile: str
    methods: list[NodeAnomalyMethod]


class NodeStatistics(BaseModel):
    """Statistical values from node results JSON.

    Supports both aggregate statistics (peer_mean, simple_zscore) and
    temporal statistics (latest_simple_zscore, mean_simple_zscore).

    Attributes:
        peer_mean: Peer cohort mean value (aggregate nodes).
        peer_std: Peer cohort standard deviation (aggregate nodes).
        percentile_rank: Position in peer distribution (0-100).
        simple_zscore: Simple z-score (aggregate nodes).
        robust_zscore: Robust z-score (MAD-based).
        suppressed: Whether statistics are suppressed.
        latest_period: Most recent period identifier (temporal nodes).
        latest_simple_zscore: Z-score for latest period (temporal nodes).
        mean_simple_zscore: Mean z-score across periods (temporal nodes).
        observations: Number of data points (temporal nodes).
        periods: Number of time periods (temporal nodes).

    Example:
        >>> stats = NodeStatistics(
        ...     peer_mean=1.08,
        ...     peer_std=0.16,
        ...     percentile_rank=85.17,
        ...     simple_zscore=1.10,
        ... )
    """

    # Aggregate node statistics
    peer_mean: float | None = None
    peer_std: float | None = None
    percentile_rank: float | None = None
    simple_zscore: float | None = None
    robust_zscore: float | None = None
    suppressed: bool = False

    # Temporal node statistics
    latest_period: str | None = None
    latest_simple_zscore: float | None = None
    mean_simple_zscore: float | None = None
    latest_robust_zscore: float | None = None
    mean_robust_zscore: float | None = None
    observations: int | None = None
    periods: int | None = None


class NodeStatisticalMethod(BaseModel):
    """Statistical method results from node results JSON.

    Attributes:
        statistical_method: Method identifier.
        anomalies: List of anomaly detection results.
        statistics: Statistical values computed by this method.
        runtime: Runtime metadata (ignored).

    Example:
        >>> method = NodeStatisticalMethod(
        ...     statistical_method="statistical_method__simple_zscore__aggregate",
        ...     anomalies=[NodeAnomaly(...)],
        ...     statistics=NodeStatistics(...),
        ... )
    """

    statistical_method: str
    anomalies: list[NodeAnomaly]
    statistics: NodeStatistics
    runtime: dict[str, Any] | None = None


class NodeEdgeReference(BaseModel):
    """Edge reference to child or parent node.

    Attributes:
        canonical_child_node_id: ID of child node (for child edges).
        canonical_parent_node_id: ID of parent node (for parent edges).
        edge_type: Type of edge (trends_to, drills_to, relates_to, derives_to).

    Example:
        >>> edge = NodeEdgeReference(
        ...     canonical_child_node_id="losIndex__medicareId__dischargeMonth",
        ...     edge_type="trends_to",
        ... )
    """

    canonical_child_node_id: str | None = None
    canonical_parent_node_id: str | None = None
    edge_type: str


class NodeEntityResult(BaseModel):
    """Entity result from node results JSON.

    Represents a single entity's metric and statistical analysis results.

    Attributes:
        encounters: Number of patient encounters.
        entity: List of entity fields (medicareId, vizientServiceLine, etc.).
        metric: List of metric values.
        statistical_methods: List of statistical analysis results.

    Example:
        >>> result = NodeEntityResult(
        ...     encounters=47927,
        ...     entity=[NodeEntityField(...)],
        ...     metric=[NodeMetric(...)],
        ...     statistical_methods=[NodeStatisticalMethod(...)],
        ... )
    """

    encounters: int
    entity: list[NodeEntityField]
    metric: list[NodeMetric]
    statistical_methods: list[NodeStatisticalMethod]


class NodeResults(BaseModel):
    """Root schema for node results JSON file.

    Attributes:
        canonical_node_id: Unique identifier for this node.
        canonical_child_node_ids: List of child node references.
        canonical_parent_node_ids: List of parent node references.
        dataset_path: Path to source dataset.
        entity_results: List of entity analysis results.

    Example:
        >>> results = NodeResults.model_validate_json(json_content)
        >>> for entity in results.entity_results:
        ...     print(entity.metric[0].metadata.metric_id)
    """

    canonical_node_id: str
    canonical_child_node_ids: list[NodeEdgeReference] = Field(default_factory=list)
    canonical_parent_node_ids: list[NodeEdgeReference] = Field(default_factory=list)
    dataset_path: str | None = None
    entity_results: list[NodeEntityResult]

    def get_temporal_node_id(self) -> str | None:
        """Get the temporal node ID linked via trends_to edge.

        Returns:
            str: The canonical_child_node_id of the trends_to edge,
                or None if no temporal node is linked.
        """
        for edge in self.canonical_child_node_ids:
            if edge.edge_type == "trends_to" and edge.canonical_child_node_id:
                return edge.canonical_child_node_id
        return None


class SignalChildSummary(BaseModel):
    """Summary of a child signal for the children endpoint.

    Attributes:
        signal_id: UUID of the child signal.
        canonical_node_id: Reference to source node in insight graph.
        description: Human-readable signal description.
        domain: Quality domain classification.
        facility: Facility name.
        service_line: Primary service line.
        metric_value: Current metric value.
        groupby_label: Human-readable label for groupby dimension(s).
        group_value: Entity dimension value(s) for the group.
        simplified_signal_type: One of 9 signal types for classification.

    Example:
        >>> child = SignalChildSummary(
        ...     signal_id="550e8400-e29b-41d4-a716-446655440001",
        ...     canonical_node_id="losIndex__serviceLine__aggregate",
        ...     description="Cardiology shows elevated LOS",
        ...     domain=SignalDomain.EFFICIENCY,
        ...     facility="General Hospital",
        ...     service_line="Cardiology",
        ...     groupby_label="Vizient Service Line",
        ...     group_value="Cardiology",
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    signal_id: str
    canonical_node_id: str
    description: str
    domain: SignalDomain
    facility: str
    service_line: str
    sub_service_line: str | None = None
    metric_value: SerializedDecimal | None = None
    groupby_label: str | None = None
    group_value: str | None = None
    simplified_signal_type: str | None = None


class SignalChildrenResponse(BaseModel):
    """Response schema for GET /signals/{id}/children endpoint.

    Attributes:
        signal_id: UUID of the parent signal.
        canonical_node_id: Canonical node ID of the parent signal.
        edge_type: Type of edge used to find children (e.g., "drills_to").
        children: List of child signals found via the edge.
        total_count: Total number of child signals.
        has_children: Whether the parent signal has any children.

    Example:
        >>> response = SignalChildrenResponse(
        ...     signal_id="550e8400-e29b-41d4-a716-446655440000",
        ...     canonical_node_id="losIndex__facilityId__aggregate",
        ...     edge_type="drills_to",
        ...     children=[...],
        ...     total_count=3,
        ...     has_children=True,
        ... )
    """

    signal_id: str
    canonical_node_id: str
    edge_type: str = "drills_to"
    children: list[SignalChildSummary]
    total_count: int
    has_children: bool


class SignalParentResponse(BaseModel):
    """Response schema for GET /signals/{id}/parent endpoint.

    Attributes:
        signal_id: UUID of the child signal requesting its parent.
        canonical_node_id: Canonical node ID of the child signal.
        edge_type: Type of edge used to find parent (e.g., "drills_to").
        parent: Parent signal summary if found, None for root signals.
        has_parent: Whether the signal has a parent via the specified edge.

    Example:
        >>> response = SignalParentResponse(
        ...     signal_id="550e8400-e29b-41d4-a716-446655440000",
        ...     canonical_node_id="losIndex__facilityId__serviceLineId",
        ...     edge_type="drills_to",
        ...     parent=SignalChildSummary(...),
        ...     has_parent=True,
        ... )
    """

    signal_id: str
    canonical_node_id: str
    edge_type: str = "drills_to"
    parent: SignalChildSummary | None = None
    has_parent: bool


class SignalListResponse(BaseModel):
    """Paginated response for GET /signals endpoint.

    Attributes:
        total_count: Total number of signals matching filters (before pagination).
        offset: Current offset in the result set.
        limit: Maximum results returned per page.
        signals: List of signals for the current page.

    Example:
        >>> response = SignalListResponse(
        ...     total_count=150,
        ...     offset=0,
        ...     limit=25,
        ...     signals=[...],
        ... )
    """

    total_count: int
    offset: int
    limit: int
    signals: list[SignalResponse]


class FilterOptionsResponse(BaseModel):
    """Response schema for GET /signals/filter-options endpoint.

    Returns distinct values for column filter dropdowns,
    scoped to optional facility filter.

    Attributes:
        metric_id: Distinct metric IDs with display names.
        domain: Distinct quality domains.
        simplified_signal_type: Distinct signal types (9 types).
        system_name: Distinct health system names.
        service_line: Distinct service lines (for group_value filter).

    Example:
        >>> response = FilterOptionsResponse(
        ...     metric_id=["losIndex", "meanIcuDaysIcuFile"],
        ...     domain=["Efficiency", "Safety"],
        ...     simplified_signal_type=["baseline", "emerging_risk", "critical_trajectory"],
        ...     system_name=["ALPHA_HEALTH", "DELTA_CARE"],
        ...     service_line=["Cardiology", "Orthopedics"],
        ... )
    """

    metric_id: list[str]
    domain: list[str]
    simplified_signal_type: list[str]
    system_name: list[str]
    service_line: list[str]
