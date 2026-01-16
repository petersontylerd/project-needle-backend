"""Pydantic schemas for contribution record parsing.

These schemas define the structure for parsing contribution JSONL files
from Project Needle's parent-child decomposition analysis.
"""

from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

# Custom type that serializes Decimal as float for JSON responses
SerializedDecimal = Annotated[
    Decimal,
    PlainSerializer(lambda x: float(x) if x is not None else None, return_type=float | None),
]


class ContributionRecord(BaseModel):
    """Schema for a single contribution record from JSONL file.

    Represents a parent-child metric decomposition showing how child entities
    contribute to the parent aggregate value.

    Attributes:
        method: Contribution calculation method (e.g., "weighted_mean").
        child_value: Child entity's metric value.
        parent_value: Parent entity's metric value.
        weight_field: Field used for weighting (e.g., "encounters").
        weight_value: Raw weight value for this child.
        weight_share: Proportion of total weight (0.0 to 1.0).
        weighted_child_value: Child value multiplied by weight share.
        contribution_value: Final contribution value.
        raw_component: Raw component value before weighting.
        excess_over_parent: How much child exceeds parent (can be negative).
        parent_node_file: Filename of parent node definition.
        parent_node_id: Canonical ID of parent node.
        parent_entity: Entity fields for parent (e.g., {"medicareId": "AAA001"}).
        child_entity: Entity fields for child (e.g., {"medicareId": "AAA001", "vizientServiceLine": "Cardiology"}).

    Example:
        >>> record = ContributionRecord.model_validate_json(jsonl_line)
        >>> print(f"{record.child_entity}: {record.excess_over_parent}")
    """

    model_config = ConfigDict(from_attributes=True)

    method: str = Field(description="Contribution calculation method")
    child_value: float | None = Field(default=None, description="Child entity metric value")
    parent_value: float | None = Field(default=None, description="Parent entity metric value")
    weight_field: str = Field(description="Field used for weighting")
    weight_value: float = Field(description="Raw weight value")
    weight_share: float = Field(description="Proportion of total weight (0-1)")
    weighted_child_value: float | None = Field(default=None, description="Weighted child value")
    contribution_value: float | None = Field(default=None, description="Final contribution value")
    raw_component: float | None = Field(default=None, description="Raw component before weighting")
    excess_over_parent: float | None = Field(default=None, description="Excess over parent value")
    parent_node_file: str = Field(description="Parent node filename")
    parent_node_id: str = Field(description="Canonical parent node ID")
    parent_entity: dict[str, str] = Field(description="Parent entity fields")
    child_entity: dict[str, str] | None = Field(default=None, description="Child entity fields")


class ContributionResponse(BaseModel):
    """Schema for contribution API response.

    Provides a structured representation of contribution data for the frontend,
    including derived fields for display.

    Attributes:
        method: Calculation method used.
        child_value: Child metric value as Decimal.
        parent_value: Parent metric value as Decimal.
        weight_field: Weighting field name.
        weight_value: Raw weight value as Decimal.
        weight_share_percent: Weight share as percentage (0-100).
        contribution_value: Contribution value as Decimal.
        excess_over_parent_percent: Excess as percentage.
        parent_node_id: Parent node canonical ID.
        parent_entity: Parent entity fields.
        child_entity: Child entity fields.
        child_entity_label: Human-readable child entity label.
        description: Generated description text.

    Example:
        >>> response = ContributionResponse(
        ...     method="weighted_mean",
        ...     child_value=Decimal("1.10"),
        ...     parent_value=Decimal("1.00"),
        ...     weight_field="encounters",
        ...     weight_value=Decimal("50"),
        ...     weight_share_percent=Decimal("50.0"),
        ...     contribution_value=Decimal("0.55"),
        ...     excess_over_parent_percent=Decimal("5.0"),
        ...     parent_node_id="losIndex__medicareId",
        ...     parent_entity={"medicareId": "AAA001"},
        ...     child_entity={"medicareId": "AAA001", "vizientServiceLine": "Cardiology"},
        ...     child_entity_label="Cardiology",
        ...     description="Cardiology contributes 5.0% above parent...",
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    method: str
    child_value: SerializedDecimal | None = None
    parent_value: SerializedDecimal | None = None
    weight_field: str
    weight_value: SerializedDecimal
    weight_share_percent: SerializedDecimal
    contribution_value: SerializedDecimal | None = None
    excess_over_parent_percent: SerializedDecimal | None = None
    parent_node_id: str
    parent_entity: dict[str, str]
    child_entity: dict[str, str] | None = None
    child_entity_label: str
    description: str


class HierarchicalContributionsResponse(BaseModel):
    """Schema for hierarchical contributions API response.

    Provides both upward (how this signal contributes to parent) and
    downward (how children contribute to this signal) sections for
    proper drill-down analysis in the Quality Compass dashboard.

    Attributes:
        upward_contribution: How this signal contributes to its parent aggregate.
            None for facility-wide signals that have no parent.
        downward_contributions: How child entities contribute to this signal.
            Empty list if signal has no children.
        signal_hierarchy_level: Hierarchy level of the signal.
            One of: "facility", "service_line", or "sub_service_line".
        has_children: Whether this signal has child contributions.
        has_parent: Whether this signal has a parent contribution.

    Example:
        >>> response = HierarchicalContributionsResponse(
        ...     upward_contribution=ContributionResponse(...),
        ...     downward_contributions=[ContributionResponse(...), ...],
        ...     signal_hierarchy_level="service_line",
        ...     has_children=True,
        ...     has_parent=True,
        ... )
    """

    model_config = ConfigDict(from_attributes=True)

    upward_contribution: ContributionResponse | None = Field(default=None, description="How this signal contributes to its parent aggregate")
    downward_contributions: list[ContributionResponse] = Field(default_factory=list, description="How child entities contribute to this signal")
    signal_hierarchy_level: str = Field(description="Hierarchy level: 'facility', 'service_line', or 'sub_service_line'")
    has_children: bool = Field(description="Whether this signal has child contributions")
    has_parent: bool = Field(description="Whether this signal has a parent contribution")
