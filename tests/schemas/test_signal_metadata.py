"""Tests for metadata field in signal schemas."""

from typing import Any


class TestMetadataPerPeriodValue:
    """Tests for MetadataPerPeriodValue schema."""

    def test_metadata_per_period_value_exists(self) -> None:
        """MetadataPerPeriodValue schema should exist."""
        from src.schemas.signal import MetadataPerPeriodValue

        assert MetadataPerPeriodValue is not None

    def test_metadata_per_period_value_fields(self) -> None:
        """MetadataPerPeriodValue should have period and value fields."""
        from src.schemas.signal import MetadataPerPeriodValue

        assert "period" in MetadataPerPeriodValue.model_fields
        assert "value" in MetadataPerPeriodValue.model_fields

    def test_metadata_per_period_value_instantiation(self) -> None:
        """MetadataPerPeriodValue should instantiate with valid data."""
        from src.schemas.signal import MetadataPerPeriodValue

        value = MetadataPerPeriodValue(period="202401", value=150)
        assert value.period == "202401"
        assert value.value == 150

    def test_metadata_per_period_value_accepts_float(self) -> None:
        """MetadataPerPeriodValue should accept float values."""
        from src.schemas.signal import MetadataPerPeriodValue

        value = MetadataPerPeriodValue(period="202401", value=150.5)
        assert value.value == 150.5


class TestMetadataPerPeriod:
    """Tests for MetadataPerPeriod schema."""

    def test_metadata_per_period_exists(self) -> None:
        """MetadataPerPeriod schema should exist."""
        from src.schemas.signal import MetadataPerPeriod

        assert MetadataPerPeriod is not None

    def test_metadata_per_period_has_encounters_field(self) -> None:
        """MetadataPerPeriod should have encounters field."""
        from src.schemas.signal import MetadataPerPeriod

        assert "encounters" in MetadataPerPeriod.model_fields

    def test_metadata_per_period_encounters_defaults_to_none(self) -> None:
        """MetadataPerPeriod encounters field defaults to None."""
        from src.schemas.signal import MetadataPerPeriod

        per_period = MetadataPerPeriod()
        assert per_period.encounters is None

    def test_metadata_per_period_with_encounters(self) -> None:
        """MetadataPerPeriod should accept list of MetadataPerPeriodValue."""
        from src.schemas.signal import MetadataPerPeriod, MetadataPerPeriodValue

        encounters = [
            MetadataPerPeriodValue(period="202401", value=100),
            MetadataPerPeriodValue(period="202402", value=120),
        ]
        per_period = MetadataPerPeriod(encounters=encounters)
        assert per_period.encounters is not None
        assert len(per_period.encounters) == 2
        assert per_period.encounters[0].period == "202401"


class TestSignalMetadata:
    """Tests for SignalMetadata schema."""

    def test_signal_metadata_exists(self) -> None:
        """SignalMetadata schema should exist."""
        from src.schemas.signal import SignalMetadata

        assert SignalMetadata is not None

    def test_signal_metadata_has_expected_fields(self) -> None:
        """SignalMetadata should have encounters, system_name, and per_period fields."""
        from src.schemas.signal import SignalMetadata

        assert "encounters" in SignalMetadata.model_fields
        assert "system_name" in SignalMetadata.model_fields
        assert "per_period" in SignalMetadata.model_fields

    def test_signal_metadata_all_fields_default_to_none(self) -> None:
        """SignalMetadata fields should default to None."""
        from src.schemas.signal import SignalMetadata

        metadata = SignalMetadata()
        assert metadata.encounters is None
        assert metadata.system_name is None
        assert metadata.per_period is None

    def test_signal_metadata_with_per_period(self) -> None:
        """SignalMetadata should accept per_period with nested structure."""
        from src.schemas.signal import MetadataPerPeriod, MetadataPerPeriodValue, SignalMetadata

        per_period = MetadataPerPeriod(
            encounters=[
                MetadataPerPeriodValue(period="202401", value=100),
                MetadataPerPeriodValue(period="202402", value=120),
            ]
        )
        metadata = SignalMetadata(
            encounters=220,
            system_name="Test Health System",
            per_period=per_period,
        )
        assert metadata.encounters == 220
        assert metadata.system_name == "Test Health System"
        assert metadata.per_period is not None
        assert metadata.per_period.encounters is not None
        assert len(metadata.per_period.encounters) == 2

    def test_signal_metadata_accepts_camel_case_alias(self) -> None:
        """SignalMetadata should accept systemName via alias from JSON."""
        from src.schemas.signal import SignalMetadata

        # Simulate parsing JSON with camelCase key
        metadata = SignalMetadata.model_validate({"systemName": "Alpha Health"})
        assert metadata.system_name == "Alpha Health"

    def test_signal_metadata_allows_extra_fields(self) -> None:
        """SignalMetadata should allow extra dynamic fields (extra='allow')."""
        from src.schemas.signal import SignalMetadata

        # Create with extra field
        metadata = SignalMetadata.model_validate({"encounters": 100, "custom_field": "custom_value"})
        assert metadata.encounters == 100
        # Access via model_extra for Pydantic v2
        assert metadata.model_extra.get("custom_field") == "custom_value"


def test_signal_response_includes_metadata() -> None:
    """SignalResponse schema includes metadata field."""
    from src.schemas.signal import SignalResponse

    # Check field exists
    assert "metadata" in SignalResponse.model_fields


def test_signal_response_metadata_field_type() -> None:
    """SignalResponse metadata field has correct type annotation."""
    from src.schemas.signal import SignalResponse

    metadata_field = SignalResponse.model_fields["metadata"]
    # Field should accept dict[str, Any] | None
    assert metadata_field.annotation == dict[str, Any] | None


def test_signal_response_metadata_defaults_to_none() -> None:
    """SignalResponse metadata field defaults to None."""
    from src.schemas.signal import SignalResponse

    metadata_field = SignalResponse.model_fields["metadata"]
    assert metadata_field.default is None
