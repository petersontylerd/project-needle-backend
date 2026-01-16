# Semantic Layer

This directory contains MetricFlow semantic model and metric definitions for the Quality Compass analytics platform.

## Overview

The semantic layer provides:
- **Declarative entity/dimension/measure definitions** mapped to fact and dimension tables
- **Business metric definitions** with filtering, aggregation, and metadata
- **Rich documentation** visible in dbt docs with lineage integration

## Files

| File | Purpose |
|------|---------|
| `_semantic_models.yml` | Defines semantic models mapping to marts tables with entities, dimensions, and measures |
| `_metrics.yml` | Defines declarative business metrics computed from semantic model measures |

## Semantic Models

| Model | Source Table | Description |
|-------|-------------|-------------|
| `signals` | `fct_signals` | Quality signals with anomaly classifications |
| `contributions` | `fct_contributions` | Parent-child contribution analysis |
| `facilities` | `dim_facilities` | Facility dimension |
| `metrics_dim` | `dim_metrics` | Metric dimension |

## Metric Categories

| Category | Metrics | Description |
|----------|---------|-------------|
| **Volume** | `total_signals`, `critical_signals`, `high_priority_signals` | Signal counts |
| **Domain** | `efficiency_signals`, `safety_signals`, `effectiveness_signals` | Domain breakdown |
| **Statistical** | `avg_z_score_magnitude`, `avg_robust_zscore_magnitude` | Statistical measures |
| **Coverage** | `unique_facilities`, `total_encounters_covered` | Analytical coverage |
| **Contribution** | `total_contributions`, `avg_impact_score` | Root cause analysis |
| **Ratio** | `critical_signal_rate`, `signals_per_facility` | Derived ratios |
| **Trend** | `cumulative_signals`, `cumulative_encounters` | Time-series |

## Usage

### Generating the Semantic Manifest

```bash
cd backend/dbt
dbt parse  # Generates target/semantic_manifest.json
```

### Viewing in dbt Docs

```bash
dbt docs generate
dbt docs serve
# Navigate to http://localhost:8080
```

The semantic models and metrics appear in the dbt docs with:
- Entity relationships and foreign keys
- Dimension and measure documentation
- Metric definitions with filters and formulas

## Future: MetricFlow Query Runner (Phase 2)

In a future phase, these semantic definitions will power a MetricFlow query runner service
that enables programmatic metric queries via the Quality Compass API:

```python
# Example future usage
result = metricflow.query(
    metric="critical_signal_rate",
    group_by=["signal__domain", "signal__facility_id"],
    where=["{{ Dimension('signal__severity') }} IN ('Critical', 'High')"]
)
```

This is deferred pending dbt Core MetricFlow SDK maturity.
