"""Healthcare ontology graph schema definitions.

This module defines the vertex and edge labels for the healthcare_ontology
graph stored in Apache AGE. These definitions are used for:
- Syncing relational data to graph vertices/edges
- Building Cypher queries for traversal
- API response schemas
"""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

# Graph name constant
GRAPH_NAME = "healthcare_ontology"


@dataclass(frozen=True)
class VertexLabel:
    """Base class for vertex label definitions."""

    label: ClassVar[str]


@dataclass(frozen=True)
class Facility(VertexLabel):
    """Healthcare facility vertex."""

    label: ClassVar[str] = "Facility"

    id: str
    name: str
    facility_type: str | None = None


@dataclass(frozen=True)
class Metric(VertexLabel):
    """Quality metric vertex."""

    label: ClassVar[str] = "Metric"

    id: str
    name: str
    domain: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class Signal(VertexLabel):
    """Anomaly signal vertex."""

    label: ClassVar[str] = "Signal"

    id: str
    severity: str | None = None
    trajectory: str | None = None
    consistency: str | None = None
    created_at: datetime | None = None


@dataclass(frozen=True)
class ServiceLine(VertexLabel):
    """Clinical service line vertex."""

    label: ClassVar[str] = "ServiceLine"

    id: str
    name: str


@dataclass(frozen=True)
class Classification(VertexLabel):
    """Signal classification tier vertex."""

    label: ClassVar[str] = "Classification"

    id: str
    tier_name: str
    tier_value: int


@dataclass(frozen=True)
class StatisticalMethod(VertexLabel):
    """Statistical analysis method vertex."""

    label: ClassVar[str] = "StatisticalMethod"

    id: str
    name: str
    description: str | None = None


@dataclass(frozen=True)
class Domain(VertexLabel):
    """Quality domain vertex."""

    label: ClassVar[str] = "Domain"

    id: str
    name: str


@dataclass(frozen=True)
class EdgeLabel:
    """Base class for edge label definitions."""

    label: ClassVar[str]
    from_label: ClassVar[str]
    to_label: ClassVar[str]


@dataclass(frozen=True)
class HasSignal(EdgeLabel):
    """Facility -> Signal relationship."""

    label: ClassVar[str] = "has_signal"
    from_label: ClassVar[str] = "Facility"
    to_label: ClassVar[str] = "Signal"


@dataclass(frozen=True)
class Measures(EdgeLabel):
    """Metric -> Signal relationship."""

    label: ClassVar[str] = "measures"
    from_label: ClassVar[str] = "Metric"
    to_label: ClassVar[str] = "Signal"


@dataclass(frozen=True)
class BelongsTo(EdgeLabel):
    """Metric -> Domain relationship."""

    label: ClassVar[str] = "belongs_to"
    from_label: ClassVar[str] = "Metric"
    to_label: ClassVar[str] = "Domain"


@dataclass(frozen=True)
class LocatedIn(EdgeLabel):
    """Signal -> ServiceLine relationship."""

    label: ClassVar[str] = "located_in"
    from_label: ClassVar[str] = "Signal"
    to_label: ClassVar[str] = "ServiceLine"


@dataclass(frozen=True)
class ClassifiedAs(EdgeLabel):
    """Signal -> Classification relationship."""

    label: ClassVar[str] = "classified_as"
    from_label: ClassVar[str] = "Signal"
    to_label: ClassVar[str] = "Classification"


@dataclass(frozen=True)
class ComputedBy(EdgeLabel):
    """Signal -> StatisticalMethod relationship."""

    label: ClassVar[str] = "computed_by"
    from_label: ClassVar[str] = "Signal"
    to_label: ClassVar[str] = "StatisticalMethod"


@dataclass(frozen=True)
class PotentiallyDrivenBy(EdgeLabel):
    """Edge for potential driver relationships between signals."""

    label: ClassVar[str] = "potentially_driven_by"
    from_label: ClassVar[str] = "Signal"
    to_label: ClassVar[str] = "Signal"


# All vertex labels for iteration
VERTEX_LABELS: list[type[VertexLabel]] = [
    Facility,
    Metric,
    Signal,
    ServiceLine,
    Classification,
    StatisticalMethod,
    Domain,
]

# All edge labels for iteration
EDGE_LABELS: list[type[EdgeLabel]] = [
    HasSignal,
    Measures,
    BelongsTo,
    LocatedIn,
    ClassifiedAs,
    ComputedBy,
    PotentiallyDrivenBy,
]

# Valid edge label names for validation
VALID_EDGE_LABELS: set[str] = {cls.label for cls in EDGE_LABELS}
