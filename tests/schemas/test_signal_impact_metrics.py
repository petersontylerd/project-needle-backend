"""Test impact metrics fields in signal schemas."""

from datetime import datetime
from decimal import Decimal

from src.schemas.signal import SignalCreate, SignalDomain, SignalResponse


def test_signal_create_has_impact_fields():
    """Verify SignalCreate accepts impact metric fields."""
    signal = SignalCreate(
        canonical_node_id="test__node",
        metric_id="losIndex",
        domain=SignalDomain.EFFICIENCY,
        facility="Test Hospital",
        service_line="Cardiology",
        description="Test signal",
        metric_value=Decimal("1.25"),
        detected_at=datetime.now(),
        annual_excess_cost=Decimal("125000.00"),
        excess_los_days=Decimal("50.5"),
        capacity_impact_bed_days=Decimal("0.1384"),
        expected_metric_value=Decimal("1.00"),
        why_matters_narrative="This impacts patient flow.",
    )
    assert signal.annual_excess_cost == Decimal("125000.00")
    assert signal.excess_los_days == Decimal("50.5")
    assert signal.capacity_impact_bed_days == Decimal("0.1384")
    assert signal.expected_metric_value == Decimal("1.00")
    assert signal.why_matters_narrative == "This impacts patient flow."


def test_signal_create_impact_fields_are_optional():
    """Verify SignalCreate impact metric fields are optional."""
    signal = SignalCreate(
        canonical_node_id="test__node",
        metric_id="losIndex",
        domain=SignalDomain.EFFICIENCY,
        facility="Test Hospital",
        service_line="Cardiology",
        description="Test signal",
        metric_value=Decimal("1.25"),
        detected_at=datetime.now(),
    )
    assert signal.annual_excess_cost is None
    assert signal.excess_los_days is None
    assert signal.capacity_impact_bed_days is None
    assert signal.expected_metric_value is None
    assert signal.why_matters_narrative is None


def test_signal_response_has_impact_fields():
    """Verify SignalResponse includes impact metric fields."""
    response = SignalResponse(
        id="test-uuid",
        canonical_node_id="test__node",
        metric_id="losIndex",
        domain=SignalDomain.EFFICIENCY,
        facility="Test Hospital",
        service_line="Cardiology",
        description="Test signal",
        metric_value=Decimal("1.25"),
        detected_at=datetime.now(),
        created_at=datetime.now(),
        annual_excess_cost=Decimal("125000.00"),
        excess_los_days=Decimal("50.5"),
        capacity_impact_bed_days=Decimal("0.1384"),
        expected_metric_value=Decimal("1.00"),
        why_matters_narrative="This impacts patient flow.",
    )
    # Fields store Decimal internally
    assert response.annual_excess_cost == Decimal("125000.00")
    assert response.excess_los_days == Decimal("50.5")
    assert response.capacity_impact_bed_days == Decimal("0.1384")
    assert response.expected_metric_value == Decimal("1.00")
    assert response.why_matters_narrative == "This impacts patient flow."

    # SerializedDecimal converts to float during JSON serialization
    json_data = response.model_dump()
    assert json_data["annual_excess_cost"] == 125000.00
    assert json_data["excess_los_days"] == 50.5
    assert json_data["capacity_impact_bed_days"] == 0.1384
    assert json_data["expected_metric_value"] == 1.00


def test_signal_response_impact_fields_are_optional():
    """Verify SignalResponse impact metric fields are optional."""
    response = SignalResponse(
        id="test-uuid",
        canonical_node_id="test__node",
        metric_id="losIndex",
        domain=SignalDomain.EFFICIENCY,
        facility="Test Hospital",
        service_line="Cardiology",
        description="Test signal",
        metric_value=Decimal("1.25"),
        detected_at=datetime.now(),
        created_at=datetime.now(),
    )
    assert response.annual_excess_cost is None
    assert response.excess_los_days is None
    assert response.capacity_impact_bed_days is None
    assert response.expected_metric_value is None
    assert response.why_matters_narrative is None
