"""Test SignalUpdate schema for PATCH endpoint."""

from src.schemas.signal import SignalUpdate


def test_signal_update_partial():
    """Verify SignalUpdate allows partial updates."""
    # Only description
    update1 = SignalUpdate(description="New description")
    assert update1.description == "New description"
    assert update1.why_matters_narrative is None

    # Only narrative
    update2 = SignalUpdate(why_matters_narrative="Business impact text")
    assert update2.description is None
    assert update2.why_matters_narrative == "Business impact text"

    # Both
    update3 = SignalUpdate(description="Updated", why_matters_narrative="Impact")
    assert update3.description == "Updated"
    assert update3.why_matters_narrative == "Impact"


def test_signal_update_empty_valid():
    """Verify SignalUpdate accepts empty payload."""
    update = SignalUpdate()
    assert update.description is None
    assert update.why_matters_narrative is None
