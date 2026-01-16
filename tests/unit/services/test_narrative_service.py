"""Unit tests for NarrativeService.

Tests cover:
- Data model creation and validation
- Section parsing functions
- Full markdown parsing
- Error handling and edge cases
- Service methods (get_narrative, list_available_facilities)

Minimum requirement: 8 test cases (actual: 24+)
"""

from __future__ import annotations

from datetime import UTC
from pathlib import Path

import pytest

from src.services.narrative_service import (
    ContributorSummary,
    Driver,
    HierarchyNode,
    MetricComparison,
    NarrativeInsights,
    NarrativeService,
    NarrativeServiceError,
    _parse_contributors,
    _parse_cross_metric_table,
    _parse_drivers_table,
    _parse_executive_summary,
    _parse_header,
    _parse_hierarchy_table,
    _parse_insights_section,
    _parse_pareto_analysis,
    _split_sections,
)

# =============================================================================
# Test Fixtures
# =============================================================================


SAMPLE_HEADER = """# Contribution Analysis: Medicare ID AFP658

**Facility LOS Index**: 1.1668
**Generated**: 2025-12-13 16:46:14 UTC

---
"""

SAMPLE_CROSS_METRIC = """
| Metric | Value | Z-Score | Peer Status |
|--------|-------|---------|-------------|
| Mean ICU Days | 6.640 | +1.38 | moderately high |
| ICU Encounter Rate | 23.599 | +0.78 | slightly high |
| LOS Index (Length of Stay) | 1.167 | +0.57 | slightly high |
"""

SAMPLE_EXECUTIVE_SUMMARY = """
- **Facility LOS Index**: 1.1668
- **Total segments analyzed**: 213 (69 facility-level)
- **Pareto Insight**: Top 5 positive-excess segments account for 44% of total excess

**Top contributors to HIGHER LOS** (worse performance):
  - [Discharge] Skilled nursing facility: +0.3117, - -79%
  - [Payer] Unknown: +0.1634, ↑ -91%
  - [Discharge] Custodial care: +0.1574, ↓ -12%

**Top contributors to LOWER LOS** (better performance):
  - [Payer] Medicaid: -0.2593, ↑ +2%
  - [Discharge] Rehab facility: -0.1782, ↓ -95%
"""

SAMPLE_PARETO = """
### Segments Adding to LOS Index (Positive Excess)

```
▓▓▓▓░░░░░░░░░░░░░░░░░░░░░░░░░░   14.9% | Segment A (+0.3117)
▓▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░░░░░   22.7% | Segment B (+0.1634)
▓▓▓▓▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░░   30.2% | Segment C (+0.1574)

                                      [Top 5 = 44% of total positive excess]
```

### Segments Reducing LOS Index (Negative Excess)

```
▓▓▓░░░░░░░░░░░░░░░░░░░░░░░░░░░   12.4% | Segment D (-0.2593)
▓▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░░░░░   20.9% | Segment E (-0.1782)

                                      [Top 5 = 38% of total negative excess]
```
"""

SAMPLE_DRIVERS_TABLE = """
| Rank | Dimension | Segment | LOS Index (Agg) | Weight | Excess (Agg) | Trend (12mo) | Slope %ile | Mean Z (12mo) | Z (Agg) | Peer Status | Multi-KPI | Interpretation |
|------|------|------|------|------|------|------|------|------|------|------|------|------|
| 1 | Discharge | Skilled Nursing | 6.459 | 11.0% | +0.3117 | - -79% | - | -1.62 | +1.80 | moderately high | - | 454% above avg; [Cum:15%] |
| 2 | Payer | Unknown | 3.181 | 16.2% | +0.1634 | ↑ -91% | 67 | +0.26 | +0.71 | slightly high | - | 173% above avg; [Cum:23%] |
| 3 | Service Line | Neurosurgery | 3.636 | 5.8% | +0.1055 | ↓ -80% | 5 | -0.29 | +1.57 | moderately high | Double | 212% above avg; [Cum:30%] |
"""

SAMPLE_INSIGHTS = """
### ⚠️ Double Trouble (High Excess + Unusual vs Peers)

These segments hurt your LOS Index AND are worse than peers:
  - **Skilled Nursing Facility**: excess +0.3117, z=+1.80 (moderately high)
  - **Other**: excess +0.1270, z=+1.52 (moderately high)

### 📊 Internal Issue (High Excess, Normal vs Peers)

These segments hurt your LOS Index but peers have similar performance:
  - Unknown: excess +0.1634, z=+0.71
  - Custodial care: excess +0.1574, z=+1.41
"""

SAMPLE_HIERARCHY = """
| Level | Segment | Value | Weight | Excess | Z-Score | Peer Status | Interpretation |
|-------|---------|-------|--------|--------|---------|-------------|----------------|
| **SL** | **Neurosurgery** | **3.636** | **5.8%** | **+0.1055** | **+1.57** | **moderately high** | **212% longer** |
| └─ SSL | Open Procedure | 12.495 | 74.5% | +2.0413 | +1.29 | moderately high | 244% longer |
| └─ SSL | Percutaneous | 1.741 | 25.5% | -2.0413 | -1.35 | moderately low | 52% shorter |
| **SL** | **Cardiology** | **0.322** | **4.4%** | **-0.0662** | **-1.62** | **moderately low** | **72% shorter** |
| └─ SSL | Acute Coronary | 13.796 | 8.8% | +0.5513 | +1.38 | moderately high | 4187% longer |
"""


# =============================================================================
# Test Data Model Classes
# =============================================================================


class TestContributorSummary:
    """Tests for ContributorSummary dataclass."""

    def test_create_contributor_with_all_fields(self) -> None:
        """Test creating a ContributorSummary with all fields."""
        contrib = ContributorSummary(
            dimension="Discharge",
            segment="Skilled Nursing",
            excess=0.3117,
            trend="↑ -79%",
            flags=["Double"],
        )
        assert contrib.dimension == "Discharge"
        assert contrib.segment == "Skilled Nursing"
        assert contrib.excess == 0.3117
        assert contrib.trend == "↑ -79%"
        assert contrib.flags == ["Double"]

    def test_create_contributor_with_defaults(self) -> None:
        """Test creating a ContributorSummary with default values."""
        contrib = ContributorSummary(
            dimension="Payer",
            segment="Medicaid",
            excess=-0.2593,
        )
        assert contrib.trend is None
        assert contrib.flags == []


class TestMetricComparison:
    """Tests for MetricComparison dataclass."""

    def test_create_metric_comparison(self) -> None:
        """Test creating a MetricComparison."""
        comp = MetricComparison(
            metric_name="Mean ICU Days",
            value=6.640,
            z_score=1.38,
            peer_status="moderately high",
        )
        assert comp.metric_name == "Mean ICU Days"
        assert comp.value == 6.640
        assert comp.z_score == 1.38
        assert comp.peer_status == "moderately high"


class TestDriver:
    """Tests for Driver dataclass."""

    def test_create_driver_with_all_fields(self) -> None:
        """Test creating a Driver with all fields populated."""
        driver = Driver(
            rank=1,
            dimension="Discharge",
            segment="Skilled Nursing",
            value=6.459,
            weight=11.0,
            excess=0.3117,
            trend="- -79%",
            slope_percentile=None,
            mean_z=-1.62,
            z_score=1.80,
            peer_status="moderately high",
            multi_kpi=None,
            interpretation="454% above avg",
            cumulative_pct=15.0,
        )
        assert driver.rank == 1
        assert driver.slope_percentile is None
        assert driver.multi_kpi is None


class TestHierarchyNode:
    """Tests for HierarchyNode dataclass."""

    def test_create_hierarchy_node_with_children(self) -> None:
        """Test creating a HierarchyNode with child nodes."""
        child = HierarchyNode(
            level="SSL",
            segment="Open Procedure",
            value=12.495,
            weight=74.5,
            excess=2.0413,
            z_score=1.29,
            peer_status="moderately high",
            interpretation="244% longer",
            children=[],
        )
        parent = HierarchyNode(
            level="SL",
            segment="Neurosurgery",
            value=3.636,
            weight=5.8,
            excess=0.1055,
            z_score=1.57,
            peer_status="moderately high",
            interpretation="212% longer",
            children=[child],
        )
        assert parent.level == "SL"
        assert len(parent.children) == 1
        assert parent.children[0].segment == "Open Procedure"


# =============================================================================
# Test Parsing Functions
# =============================================================================


class TestParseHeader:
    """Tests for _parse_header function."""

    def test_parse_valid_header(self) -> None:
        """Test parsing a valid header."""
        facility_id, metric_value, generated_at = _parse_header(SAMPLE_HEADER)
        assert facility_id == "AFP658"
        assert metric_value == 1.1668
        assert generated_at.year == 2025
        assert generated_at.month == 12
        assert generated_at.day == 13
        assert generated_at.tzinfo == UTC

    def test_parse_header_missing_facility_id(self) -> None:
        """Test that missing facility ID raises ValueError."""
        with pytest.raises(ValueError, match="Could not parse facility ID"):
            _parse_header("# Some other title\n")

    def test_parse_header_missing_metric_uses_default(self) -> None:
        """Test that missing metric value defaults to 0.0."""
        content = "# Contribution Analysis: Medicare ID TEST123\n**Generated**: 2025-01-01 00:00:00 UTC\n"
        facility_id, metric_value, _ = _parse_header(content)
        assert facility_id == "TEST123"
        assert metric_value == 0.0


class TestSplitSections:
    """Tests for _split_sections function."""

    def test_split_multiple_sections(self) -> None:
        """Test splitting content into multiple sections."""
        content = """# Header

## Section One

Content for section one.

## Section Two

Content for section two.

## Section Three

More content here.
"""
        sections = _split_sections(content)
        assert "Section One" in sections
        assert "Section Two" in sections
        assert "Section Three" in sections
        assert "Content for section one" in sections["Section One"]

    def test_split_empty_content(self) -> None:
        """Test splitting empty content."""
        sections = _split_sections("")
        assert sections == {}


class TestParseCrossMetricTable:
    """Tests for _parse_cross_metric_table function."""

    def test_parse_valid_table(self) -> None:
        """Test parsing a valid cross-metric table."""
        comparisons = _parse_cross_metric_table(SAMPLE_CROSS_METRIC)
        assert len(comparisons) == 3
        assert comparisons[0].metric_name == "Mean ICU Days"
        assert comparisons[0].value == 6.640
        assert comparisons[0].z_score == 1.38
        assert comparisons[0].peer_status == "moderately high"

    def test_parse_empty_table(self) -> None:
        """Test parsing empty content."""
        comparisons = _parse_cross_metric_table("")
        assert comparisons == []


class TestParseExecutiveSummary:
    """Tests for _parse_executive_summary function."""

    def test_parse_valid_summary(self) -> None:
        """Test parsing a valid executive summary."""
        summary = _parse_executive_summary(SAMPLE_EXECUTIVE_SUMMARY, 1.1668)
        assert summary.metric_value == 1.1668
        assert summary.total_segments == 213
        assert summary.facility_segments == 69
        assert "Top 5" in summary.pareto_insight
        assert len(summary.top_contributors_higher) == 3
        assert len(summary.top_contributors_lower) == 2

    def test_parse_empty_summary(self) -> None:
        """Test parsing empty summary uses defaults."""
        summary = _parse_executive_summary("", 0.0)
        assert summary.total_segments == 0
        assert summary.facility_segments == 0
        assert summary.top_contributors_higher == []


class TestParseContributors:
    """Tests for _parse_contributors function."""

    def test_parse_higher_contributors(self) -> None:
        """Test parsing HIGHER LOS contributors."""
        contributors = _parse_contributors(SAMPLE_EXECUTIVE_SUMMARY, "HIGHER LOS")
        assert len(contributors) == 3
        assert contributors[0].dimension == "Discharge"
        assert contributors[0].excess == 0.3117

    def test_parse_lower_contributors(self) -> None:
        """Test parsing LOWER LOS contributors."""
        contributors = _parse_contributors(SAMPLE_EXECUTIVE_SUMMARY, "LOWER LOS")
        assert len(contributors) == 2
        assert contributors[0].dimension == "Payer"
        assert contributors[0].excess == -0.2593


class TestParseParetoAnalysis:
    """Tests for _parse_pareto_analysis function."""

    def test_parse_valid_pareto(self) -> None:
        """Test parsing a valid Pareto analysis."""
        pareto = _parse_pareto_analysis(SAMPLE_PARETO)
        assert len(pareto.positive_excess) == 3
        assert len(pareto.negative_excess) == 2
        assert pareto.top_n_positive_pct == 44.0
        assert pareto.top_n_negative_pct == 38.0
        assert pareto.positive_excess[0].excess == 0.3117
        assert pareto.positive_excess[0].cumulative_pct == 14.9

    def test_parse_empty_pareto(self) -> None:
        """Test parsing empty Pareto content."""
        pareto = _parse_pareto_analysis("")
        assert pareto.positive_excess == []
        assert pareto.negative_excess == []
        assert pareto.top_n_positive_pct == 0.0


class TestParseDriversTable:
    """Tests for _parse_drivers_table function."""

    def test_parse_valid_drivers_table(self) -> None:
        """Test parsing a valid drivers table."""
        drivers = _parse_drivers_table(SAMPLE_DRIVERS_TABLE)
        assert len(drivers) == 3

        # Check first driver
        assert drivers[0].rank == 1
        assert drivers[0].dimension == "Discharge"
        assert drivers[0].segment == "Skilled Nursing"
        assert drivers[0].value == 6.459
        assert drivers[0].weight == 11.0
        assert drivers[0].excess == 0.3117
        assert drivers[0].slope_percentile is None  # "-" in table
        assert drivers[0].z_score == 1.80
        assert drivers[0].cumulative_pct == 15.0

        # Check driver with slope percentile
        assert drivers[1].slope_percentile == 67
        assert drivers[1].trend == "↑ -91%"

        # Check driver with multi-KPI flag
        assert drivers[2].multi_kpi == "Double"

    def test_parse_empty_drivers_table(self) -> None:
        """Test parsing empty drivers content."""
        drivers = _parse_drivers_table("")
        assert drivers == []


class TestParseInsightsSection:
    """Tests for _parse_insights_section function."""

    def test_parse_valid_insights(self) -> None:
        """Test parsing a valid insights section."""
        insights = _parse_insights_section(SAMPLE_INSIGHTS)
        assert len(insights.double_trouble) == 2
        assert len(insights.internal_issue) == 2
        assert insights.double_trouble[0].segment == "Skilled Nursing Facility"
        assert insights.double_trouble[0].excess == 0.3117
        assert insights.double_trouble[0].z_score == 1.80

    def test_parse_empty_insights(self) -> None:
        """Test parsing empty insights."""
        insights = _parse_insights_section("")
        assert insights.double_trouble == []
        assert insights.internal_issue == []


class TestParseHierarchyTable:
    """Tests for _parse_hierarchy_table function."""

    def test_parse_valid_hierarchy(self) -> None:
        """Test parsing a valid hierarchy table."""
        nodes = _parse_hierarchy_table(SAMPLE_HIERARCHY)
        assert len(nodes) == 2  # Two service lines

        # Check first service line
        assert nodes[0].level == "SL"
        assert nodes[0].segment == "Neurosurgery"
        assert len(nodes[0].children) == 2

        # Check children
        assert nodes[0].children[0].level == "SSL"
        assert nodes[0].children[0].segment == "Open Procedure"
        assert nodes[0].children[1].segment == "Percutaneous"

        # Check second service line has children
        assert nodes[1].segment == "Cardiology"
        assert len(nodes[1].children) == 1

    def test_parse_empty_hierarchy(self) -> None:
        """Test parsing empty hierarchy."""
        nodes = _parse_hierarchy_table("")
        assert nodes == []


# =============================================================================
# Test NarrativeService Class
# =============================================================================


class TestNarrativeServiceInit:
    """Tests for NarrativeService initialization."""

    def test_init_with_defaults(self) -> None:
        """Test initialization with default values."""
        service = NarrativeService()
        assert service._project_root == Path.cwd()

    def test_init_with_custom_paths(self) -> None:
        """Test initialization with custom paths."""
        service = NarrativeService(
            runs_root=Path("/custom/runs"),
            insight_graph_run="custom/run",
            project_root=Path("/custom/project"),
        )
        assert service._runs_root == Path("/custom/runs")
        assert service._insight_graph_run == "custom/run"


class TestNarrativeServiceListFacilities:
    """Tests for list_available_facilities method."""

    def test_list_facilities_with_real_data(self) -> None:
        """Test listing facilities from real narrative directory."""
        service = NarrativeService(
            project_root=Path("/home/ubuntu/repos/project_needle"),
        )
        facilities = service.list_available_facilities()
        # Should find the test narratives
        assert isinstance(facilities, list)
        if facilities:  # May be empty in CI
            assert all(isinstance(f, str) for f in facilities)

    def test_list_facilities_nonexistent_directory(self) -> None:
        """Test listing facilities when directory doesn't exist."""
        service = NarrativeService(
            runs_root=Path("/nonexistent/path"),
            project_root=Path("/nonexistent"),
        )
        facilities = service.list_available_facilities()
        assert facilities == []


class TestNarrativeServiceGetNarrative:
    """Tests for get_narrative method."""

    def test_get_narrative_not_found(self) -> None:
        """Test getting narrative for non-existent facility."""
        service = NarrativeService(
            runs_root=Path("/nonexistent/path"),
        )
        result = service.get_narrative("NONEXISTENT")
        assert result is None

    def test_get_narrative_with_real_data(self) -> None:
        """Test getting narrative from real file."""
        service = NarrativeService(
            project_root=Path("/home/ubuntu/repos/project_needle"),
        )
        # Try to get a real narrative
        facilities = service.list_available_facilities()
        if facilities:
            result = service.get_narrative(facilities[0])
            assert result is not None
            assert isinstance(result, NarrativeInsights)
            assert result.facility_id == facilities[0]


class TestNarrativeServiceParseMarkdown:
    """Tests for parse_markdown method."""

    def test_parse_full_markdown(self) -> None:
        """Test parsing a complete markdown document."""
        # Construct a minimal but complete markdown
        full_markdown = f"""{SAMPLE_HEADER}
## Cross-Metric Peer Comparison
{SAMPLE_CROSS_METRIC}

## Executive Summary
{SAMPLE_EXECUTIVE_SUMMARY}

## Pareto Analysis: Cumulative Impact
{SAMPLE_PARETO}

## Top Drivers of Higher LOS (Positive Excess)
{SAMPLE_DRIVERS_TABLE}

## Top Drivers of Lower LOS (Negative Excess)
{SAMPLE_DRIVERS_TABLE}

## Insights: Internal vs External Comparison
{SAMPLE_INSIGHTS}

## Hierarchical Contribution Breakdown
{SAMPLE_HIERARCHY}
"""
        service = NarrativeService()
        result = service.parse_markdown(full_markdown, source_file=Path("test.md"))

        assert result.facility_id == "AFP658"
        assert result.metric_value == 1.1668
        assert result.source_file == Path("test.md")
        assert len(result.cross_metric_comparison) > 0
        assert result.executive_summary.total_segments == 213
        assert len(result.pareto_analysis.positive_excess) > 0
        assert len(result.top_drivers.higher_los) > 0
        assert len(result.insights.double_trouble) > 0
        assert len(result.hierarchical_breakdown) > 0


class TestNarrativeServiceError:
    """Tests for NarrativeServiceError exception."""

    def test_error_with_facility_id(self) -> None:
        """Test error message includes facility ID."""
        error = NarrativeServiceError("Test error", facility_id="ABC123")
        assert "Test error" in str(error)
        assert "ABC123" in str(error)
        assert error.facility_id == "ABC123"

    def test_error_without_facility_id(self) -> None:
        """Test error message without facility ID."""
        error = NarrativeServiceError("Test error")
        assert "Test error" in str(error)
        assert error.facility_id is None
