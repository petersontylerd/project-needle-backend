"""Tests for ontology schema definitions."""

import pytest

from src.ontology.schema import (
    EDGE_LABELS,
    GRAPH_NAME,
    VALID_EDGE_LABELS,
    VERTEX_LABELS,
    BelongsTo,
    Classification,
    ClassifiedAs,
    ComputedBy,
    Domain,
    Facility,
    HasSignal,
    LocatedIn,
    Measures,
    Metric,
    PotentiallyDrivenBy,
    ServiceLine,
    Signal,
    StatisticalMethod,
)


class TestGraphConstants:
    """Tests for graph constants."""

    def test_graph_name_is_healthcare_ontology(self) -> None:
        """Graph name should be healthcare_ontology."""
        assert GRAPH_NAME == "healthcare_ontology"


class TestVertexLabels:
    """Tests for vertex label definitions."""

    def test_vertex_labels_count(self) -> None:
        """Should have exactly 7 vertex labels."""
        assert len(VERTEX_LABELS) == 7

    def test_all_vertex_labels_have_label_attribute(self) -> None:
        """All vertex labels should have a label class variable."""
        for vertex_class in VERTEX_LABELS:
            assert hasattr(vertex_class, "label")
            assert isinstance(vertex_class.label, str)

    def test_facility_label(self) -> None:
        """Facility should have correct label."""
        assert Facility.label == "Facility"

    def test_metric_label(self) -> None:
        """Metric should have correct label."""
        assert Metric.label == "Metric"

    def test_signal_label(self) -> None:
        """Signal should have correct label."""
        assert Signal.label == "Signal"

    def test_service_line_label(self) -> None:
        """ServiceLine should have correct label."""
        assert ServiceLine.label == "ServiceLine"

    def test_classification_label(self) -> None:
        """Classification should have correct label."""
        assert Classification.label == "Classification"

    def test_statistical_method_label(self) -> None:
        """StatisticalMethod should have correct label."""
        assert StatisticalMethod.label == "StatisticalMethod"

    def test_domain_label(self) -> None:
        """Domain should have correct label."""
        assert Domain.label == "Domain"

    def test_vertex_is_frozen_dataclass(self) -> None:
        """Vertex instances should be immutable."""
        facility = Facility(id="F1", name="Test Hospital")
        with pytest.raises(AttributeError):
            facility.name = "Changed"


class TestEdgeLabels:
    """Tests for edge label definitions."""

    def test_edge_labels_count(self) -> None:
        """Should have exactly 7 edge labels."""
        assert len(EDGE_LABELS) == 7

    def test_all_edge_labels_have_required_attributes(self) -> None:
        """All edge labels should have label, from_label, to_label."""
        for edge_class in EDGE_LABELS:
            assert hasattr(edge_class, "label")
            assert hasattr(edge_class, "from_label")
            assert hasattr(edge_class, "to_label")

    def test_has_signal_edge(self) -> None:
        """HasSignal should connect Facility to Signal."""
        assert HasSignal.label == "has_signal"
        assert HasSignal.from_label == "Facility"
        assert HasSignal.to_label == "Signal"

    def test_measures_edge(self) -> None:
        """Measures should connect Metric to Signal."""
        assert Measures.label == "measures"
        assert Measures.from_label == "Metric"
        assert Measures.to_label == "Signal"

    def test_belongs_to_edge(self) -> None:
        """BelongsTo should connect Metric to Domain."""
        assert BelongsTo.label == "belongs_to"
        assert BelongsTo.from_label == "Metric"
        assert BelongsTo.to_label == "Domain"

    def test_located_in_edge(self) -> None:
        """LocatedIn should connect Signal to ServiceLine."""
        assert LocatedIn.label == "located_in"
        assert LocatedIn.from_label == "Signal"
        assert LocatedIn.to_label == "ServiceLine"

    def test_classified_as_edge(self) -> None:
        """ClassifiedAs should connect Signal to Classification."""
        assert ClassifiedAs.label == "classified_as"
        assert ClassifiedAs.from_label == "Signal"
        assert ClassifiedAs.to_label == "Classification"

    def test_computed_by_edge(self) -> None:
        """ComputedBy should connect Signal to StatisticalMethod."""
        assert ComputedBy.label == "computed_by"
        assert ComputedBy.from_label == "Signal"
        assert ComputedBy.to_label == "StatisticalMethod"

    def test_potentially_driven_by_edge(self) -> None:
        """PotentiallyDrivenBy should connect Signal to Signal."""
        assert PotentiallyDrivenBy.label == "potentially_driven_by"
        assert PotentiallyDrivenBy.from_label == "Signal"
        assert PotentiallyDrivenBy.to_label == "Signal"


def test_valid_edge_labels_defined() -> None:
    """VALID_EDGE_LABELS should contain all edge label names."""
    assert isinstance(VALID_EDGE_LABELS, set)
    assert len(VALID_EDGE_LABELS) == len(EDGE_LABELS)
    for edge_class in EDGE_LABELS:
        assert edge_class.label in VALID_EDGE_LABELS
