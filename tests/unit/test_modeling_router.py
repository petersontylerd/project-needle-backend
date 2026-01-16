"""Unit tests for modeling router.

Tests for response schemas, helper functions, and SHAP CSV parsing logic.
Does not test database interactions - those are integration tests.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from src.modeling.router import (
    ExperimentsListResponse,
    FeatureDriver,
    ModelDriversResponse,
    ModelingExperiment,
    ModelingRunSummary,
    ModelingSummaryResponse,
    load_shap_drivers_from_csv,
)

pytestmark = pytest.mark.tier1

# =============================================================================
# Response Model Schema Tests
# =============================================================================


class TestModelingRunSummarySchema:
    """Tests for ModelingRunSummary Pydantic schema."""

    def test_schema_with_required_fields(self) -> None:
        """Test schema with only required fields."""
        summary = ModelingRunSummary(
            run_id="test-run-001",
            status="completed",
        )

        assert summary.run_id == "test-run-001"
        assert summary.status == "completed"
        assert summary.config_path is None
        assert summary.groups == []
        assert summary.duration_seconds is None

    def test_schema_with_all_fields(self) -> None:
        """Test schema with all fields populated."""
        summary = ModelingRunSummary(
            run_id="test-run-002",
            status="completed",
            config_path="config/modeling.yaml",
            groups=["region", "facility_type"],
            duration_seconds=125.5,
        )

        assert summary.run_id == "test-run-002"
        assert summary.config_path == "config/modeling.yaml"
        assert summary.groups == ["region", "facility_type"]
        assert summary.duration_seconds == 125.5

    def test_schema_serialization(self) -> None:
        """Test schema serializes to dict correctly."""
        summary = ModelingRunSummary(
            run_id="test-run-003",
            status="running",
        )

        data = summary.model_dump()
        assert "run_id" in data
        assert "status" in data
        assert "config_path" in data
        assert "groups" in data
        assert "duration_seconds" in data


class TestModelingExperimentSchema:
    """Tests for ModelingExperiment Pydantic schema."""

    def test_schema_with_required_fields(self) -> None:
        """Test schema with only required fields."""
        experiment = ModelingExperiment(
            run_id="test-run-001",
            estimator="ridge",
        )

        assert experiment.run_id == "test-run-001"
        assert experiment.estimator == "ridge"
        assert experiment.groupby_label is None
        assert experiment.r2 is None

    def test_schema_with_all_metrics(self) -> None:
        """Test schema with all metrics populated."""
        experiment = ModelingExperiment(
            run_id="test-run-001",
            estimator="xgboost",
            groupby_label="region",
            group_value="midwest",
            row_count=10000,
            duration_seconds=45.2,
            mae=0.15,
            rmse=0.22,
            r2=0.85,
            mae_original=0.18,
            rmse_original=0.25,
            r2_original=0.82,
            global_shap_path="modeling/run/global_shap.csv",
            facility_shap_path="modeling/run/facility_shap.csv",
            encounter_shap_path="modeling/run/encounter_shap.parquet",
            experiment_rank=1,
        )

        assert experiment.r2 == 0.85
        assert experiment.r2_original == 0.82
        assert experiment.experiment_rank == 1
        assert experiment.global_shap_path == "modeling/run/global_shap.csv"

    def test_schema_serialization(self) -> None:
        """Test schema serializes to dict correctly."""
        experiment = ModelingExperiment(
            run_id="test-run-001",
            estimator="lightgbm",
        )

        data = experiment.model_dump()
        assert "run_id" in data
        assert "estimator" in data
        assert "mae" in data
        assert "rmse" in data
        assert "r2" in data


class TestFeatureDriverSchema:
    """Tests for FeatureDriver Pydantic schema."""

    def test_schema_creation(self) -> None:
        """Test schema creation with all fields."""
        driver = FeatureDriver(
            feature_name="numeric__age",
            shap_value=0.0523,
            direction="positive",
            rank=1,
        )

        assert driver.feature_name == "numeric__age"
        assert driver.shap_value == 0.0523
        assert driver.direction == "positive"
        assert driver.rank == 1

    def test_schema_negative_direction(self) -> None:
        """Test schema with negative direction."""
        driver = FeatureDriver(
            feature_name="categorical__region_west",
            shap_value=0.0312,
            direction="negative",
            rank=5,
        )

        assert driver.direction == "negative"

    def test_schema_serialization(self) -> None:
        """Test schema serializes to dict correctly."""
        driver = FeatureDriver(
            feature_name="test_feature",
            shap_value=0.01,
            direction="positive",
            rank=1,
        )

        data = driver.model_dump()
        assert "feature_name" in data
        assert "shap_value" in data
        assert "direction" in data
        assert "rank" in data


class TestModelDriversResponseSchema:
    """Tests for ModelDriversResponse Pydantic schema."""

    def test_empty_drivers_response(self) -> None:
        """Test response with no drivers."""
        response = ModelDriversResponse(
            run_id="test-run-001",
            estimator="ridge",
            drivers=[],
            top_n=10,
        )

        assert response.drivers == []
        assert response.top_n == 10

    def test_response_with_drivers(self) -> None:
        """Test response with populated drivers."""
        drivers = [
            FeatureDriver(feature_name="feat1", shap_value=0.05, direction="positive", rank=1),
            FeatureDriver(feature_name="feat2", shap_value=0.03, direction="negative", rank=2),
        ]

        response = ModelDriversResponse(
            run_id="test-run-001",
            estimator="xgboost",
            drivers=drivers,
            top_n=2,
        )

        assert len(response.drivers) == 2
        assert response.drivers[0].feature_name == "feat1"


class TestModelingSummaryResponseSchema:
    """Tests for ModelingSummaryResponse Pydantic schema."""

    def test_empty_summary_response(self) -> None:
        """Test response with no data."""
        response = ModelingSummaryResponse()

        assert response.run is None
        assert response.experiments == []
        assert response.experiment_count == 0
        assert response.best_experiment is None

    def test_response_with_run_and_experiments(self) -> None:
        """Test response with full data."""
        run = ModelingRunSummary(run_id="test-run", status="completed")
        experiment = ModelingExperiment(run_id="test-run", estimator="ridge", r2=0.85)

        response = ModelingSummaryResponse(
            run=run,
            experiments=[experiment],
            experiment_count=1,
            best_experiment=experiment,
        )

        assert response.run is not None
        assert response.experiment_count == 1
        assert response.best_experiment is not None


class TestExperimentsListResponseSchema:
    """Tests for ExperimentsListResponse Pydantic schema."""

    def test_empty_list_response(self) -> None:
        """Test response with no experiments."""
        response = ExperimentsListResponse(experiments=[], count=0)

        assert response.experiments == []
        assert response.count == 0

    def test_response_with_experiments(self) -> None:
        """Test response with experiments."""
        experiments = [
            ModelingExperiment(run_id="run1", estimator="ridge"),
            ModelingExperiment(run_id="run1", estimator="xgboost"),
        ]

        response = ExperimentsListResponse(experiments=experiments, count=2)

        assert len(response.experiments) == 2
        assert response.count == 2


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestLoadShapDriversFromCsv:
    """Tests for load_shap_drivers_from_csv helper function."""

    def test_load_valid_csv(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test loading a valid SHAP CSV file."""
        csv_content = """feature,mean_abs_shap,mean_shap,count
numeric__age,0.05,0.02,1000
numeric__income,0.04,-0.01,1000
categorical__region_west,0.03,0.015,1000
"""
        with TemporaryDirectory() as tmpdir:
            # Monkeypatch settings.RUNS_ROOT
            from src import config

            monkeypatch.setattr(config.settings, "RUNS_ROOT", tmpdir)

            # Create test CSV
            csv_path = Path(tmpdir) / "shap_test.csv"
            csv_path.write_text(csv_content)

            # Load drivers
            drivers = load_shap_drivers_from_csv("shap_test.csv", top_n=3)

            assert len(drivers) == 3
            # Should be sorted by mean_abs_shap descending
            assert drivers[0].feature_name == "numeric__age"
            assert drivers[0].shap_value == 0.05
            assert drivers[0].direction == "positive"  # mean_shap > 0
            assert drivers[0].rank == 1

            assert drivers[1].feature_name == "numeric__income"
            assert drivers[1].direction == "negative"  # mean_shap < 0
            assert drivers[1].rank == 2

    def test_load_top_n_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that top_n limits the number of returned drivers."""
        csv_content = """feature,mean_abs_shap,mean_shap,count
feat1,0.10,0.05,1000
feat2,0.09,0.04,1000
feat3,0.08,0.03,1000
feat4,0.07,0.02,1000
feat5,0.06,0.01,1000
"""
        with TemporaryDirectory() as tmpdir:
            from src import config

            monkeypatch.setattr(config.settings, "RUNS_ROOT", tmpdir)

            csv_path = Path(tmpdir) / "shap_limit.csv"
            csv_path.write_text(csv_content)

            drivers = load_shap_drivers_from_csv("shap_limit.csv", top_n=2)

            assert len(drivers) == 2
            assert drivers[0].feature_name == "feat1"
            assert drivers[1].feature_name == "feat2"

    def test_file_not_found_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that missing file returns empty list."""
        with TemporaryDirectory() as tmpdir:
            from src import config

            monkeypatch.setattr(config.settings, "RUNS_ROOT", tmpdir)

            drivers = load_shap_drivers_from_csv("nonexistent.csv", top_n=10)

            assert drivers == []

    def test_path_outside_runs_root_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that paths escaping RUNS_ROOT return empty list."""
        csv_content = """feature,mean_abs_shap,mean_shap,count\nfeat,0.1,0.05,10\n"""
        with TemporaryDirectory() as tmpdir:
            from src import config

            monkeypatch.setattr(config.settings, "RUNS_ROOT", tmpdir)

            outside_path = Path(tmpdir).parent / "outside_shap.csv"
            outside_path.write_text(csv_content)

            drivers = load_shap_drivers_from_csv(f"../{outside_path.name}", top_n=1)

            assert drivers == []

    def test_malformed_csv_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that malformed CSV returns empty list."""
        csv_content = """not,valid,headers
this,is,garbage
"""
        with TemporaryDirectory() as tmpdir:
            from src import config

            monkeypatch.setattr(config.settings, "RUNS_ROOT", tmpdir)

            csv_path = Path(tmpdir) / "bad_shap.csv"
            csv_path.write_text(csv_content)

            # Should return empty due to missing mean_abs_shap column
            drivers = load_shap_drivers_from_csv("bad_shap.csv", top_n=10)

            assert drivers == []

    def test_direction_inference_from_mean_shap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test direction is correctly inferred from mean_shap sign."""
        csv_content = """feature,mean_abs_shap,mean_shap,count
positive_feature,0.05,0.03,100
negative_feature,0.04,-0.02,100
zero_feature,0.03,0.0,100
"""
        with TemporaryDirectory() as tmpdir:
            from src import config

            monkeypatch.setattr(config.settings, "RUNS_ROOT", tmpdir)

            csv_path = Path(tmpdir) / "direction_test.csv"
            csv_path.write_text(csv_content)

            drivers = load_shap_drivers_from_csv("direction_test.csv", top_n=3)

            assert drivers[0].direction == "positive"  # mean_shap > 0
            assert drivers[1].direction == "negative"  # mean_shap < 0
            assert drivers[2].direction == "positive"  # mean_shap == 0 treated as positive

    def test_ranks_are_sequential(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that ranks are assigned sequentially starting from 1."""
        csv_content = """feature,mean_abs_shap,mean_shap,count
feat_a,0.10,0.05,100
feat_b,0.08,0.04,100
feat_c,0.06,0.03,100
"""
        with TemporaryDirectory() as tmpdir:
            from src import config

            monkeypatch.setattr(config.settings, "RUNS_ROOT", tmpdir)

            csv_path = Path(tmpdir) / "rank_test.csv"
            csv_path.write_text(csv_content)

            drivers = load_shap_drivers_from_csv("rank_test.csv", top_n=3)

            assert drivers[0].rank == 1
            assert drivers[1].rank == 2
            assert drivers[2].rank == 3
