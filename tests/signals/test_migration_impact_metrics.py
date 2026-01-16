"""Test that impact metrics columns exist in signals table."""

from sqlalchemy import inspect

from src.db.models import Signal


def test_signal_has_impact_metric_columns():
    """Verify Signal model has new impact metric columns."""
    mapper = inspect(Signal)
    column_names = [col.key for col in mapper.columns]

    assert "annual_excess_cost" in column_names
    assert "excess_los_days" in column_names
    assert "capacity_impact_bed_days" in column_names
    assert "expected_metric_value" in column_names
    assert "why_matters_narrative" in column_names
