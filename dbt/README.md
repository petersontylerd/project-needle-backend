# Quality Compass dbt Project

This dbt project transforms raw Project Needle data into analytics-ready tables for the Quality Compass dashboard.

## Project Structure

```
dbt/
├── dbt_project.yml     # Project configuration
├── profiles.yml        # Database connection profiles
├── packages.yml        # dbt package dependencies
├── models/
│   ├── staging/        # Raw data extraction and cleaning
│   │   ├── sources.yml # Source definitions with freshness SLAs
│   │   ├── schema.yml  # Model tests and documentation
│   │   ├── stg_nodes.sql
│   │   ├── stg_contributions.sql
│   │   └── stg_classifications.sql
│   ├── intermediate/   # Pivots and aggregations
│   │   └── int_anomaly_labels.sql
│   └── marts/          # Aggregate/presentation layer
│       ├── schema.yml  # Model tests and documentation
│       ├── exposures.yml # Downstream consumer documentation
│       ├── fct_*.sql   # Fact tables (fct_signals: enriched signal data)
│       └── dim_*.sql   # Dimension tables
├── tests/              # Data quality tests
├── seeds/              # Static reference data (CSV)
│   └── ref_facilities.csv # Facility master data
├── macros/             # Reusable SQL functions
│   ├── extract_feature_type.sql
│   ├── classify_metric_domain.sql
│   ├── map_anomaly_to_severity.sql
│   ├── compute_groupby_label.sql
│   ├── compute_group_value.sql
│   └── postgres_maintenance.sql
├── snapshots/          # Historical tracking (currently empty)
├── analyses/           # Ad-hoc analysis queries
└── design/             # Schema design documents
```

## Setup

### Prerequisites

- Python 3.12+
- PostgreSQL 16+ with Quality Compass database
- dbt-core and dbt-postgres

### Installation

```bash
cd backend
UV_CACHE_DIR=../.uv-cache uv add dbt-core dbt-postgres

# Install dbt packages
cd dbt
UV_CACHE_DIR=../.uv-cache uv run dbt deps
```

### Environment Variables

Set these before running dbt:

```bash
export DBT_POSTGRES_HOST=localhost
export DBT_POSTGRES_USER=postgres
export DBT_POSTGRES_PASSWORD=postgres
export DBT_POSTGRES_PORT=5432
export DBT_POSTGRES_DATABASE=quality_compass

# Production only: SSL mode
export DBT_POSTGRES_SSLMODE=require
```

## Commands

> **Important**: All dbt commands must be run from the `backend/dbt/` directory. The `profiles.yml` and `dbt_project.yml` files are located here.

```bash
# Navigate to the dbt directory first
cd backend/dbt

# Verify configuration
UV_CACHE_DIR=../.uv-cache uv run dbt debug

# Install packages
UV_CACHE_DIR=../.uv-cache uv run dbt deps

# Run all models
UV_CACHE_DIR=../.uv-cache uv run dbt run

# Run full build (models + tests)
UV_CACHE_DIR=../.uv-cache uv run dbt build

# Run specific model
UV_CACHE_DIR=../.uv-cache uv run dbt run --select stg_nodes

# Run tests
UV_CACHE_DIR=../.uv-cache uv run dbt test

# Check source freshness
UV_CACHE_DIR=../.uv-cache uv run dbt source freshness

# Load seed data
UV_CACHE_DIR=../.uv-cache uv run dbt seed

# Generate documentation
UV_CACHE_DIR=../.uv-cache uv run dbt docs generate
UV_CACHE_DIR=../.uv-cache uv run dbt docs serve
```

## Models

### Staging Layer (`models/staging/`)

Raw data extraction with minimal transformation:

| Model | Description | Grain |
|-------|-------------|-------|
| `stg_nodes` | Node result metadata | One row per node per run |
| `stg_node_edges` | Graph edge relationships | One row per edge |
| `stg_entity_results` | Entity-level results | One row per entity per node |
| `stg_statistical_methods` | Statistical measures | One row per method per entity |
| `stg_anomalies` | Anomaly classifications | One row per anomaly |
| `stg_contributions` | Contribution records | One row per parent-child pair |
| `stg_classifications` | Signal classifications | One row per entity-metric classification |

### Intermediate Layer (`models/intermediate/`)

Complex transformations before the mart layer:

| Model | Description | Grain |
|-------|-------------|-------|
| `int_anomaly_labels` | Pivoted anomaly classifications | One row per statistical_method_id with 7 label columns |

### Marts Layer (`models/marts/`)

Business-ready tables for API consumption:

| Model | Description | Consumers |
|-------|-------------|-----------|
| `fct_signals` | Enriched quality signals (classification, temporal, anomaly data) | Signal API, Dashboard |
| `fct_contributions` | Contribution analysis | Contribution API |
| `dim_facilities` | Facility dimension | All APIs |
| `dim_metrics` | Metric dimension | All APIs |

### Semantic Layer (`models/semantic/`)

MetricFlow semantic model definitions for consistent metric calculations:

| File | Description |
|------|-------------|
| `_metrics.yml` | Metric definitions (aggregations, calculations) |
| `_semantic_models.yml` | Entity and dimension definitions |
| `_time_spine.yml` | Time dimension configuration |
| `metricflow_time_spine.sql` | Time spine table for date-based queries |

The semantic layer enables:
- Consistent metric definitions across the dashboard
- Dimension-aware queries via the metrics API
- Time-series aggregation with proper time spine support

## Operational Runbook

### Daily Operations

1. **Source Data Loading**
   - Python ETL loads raw data from insight graph output
   - Verify raw tables have new records: `SELECT max(loaded_at) FROM raw_node_results;`

2. **dbt Build**
   ```bash
   UV_CACHE_DIR=../.uv-cache uv run dbt build
   ```

3. **Check Source Freshness**
   ```bash
   UV_CACHE_DIR=../.uv-cache uv run dbt source freshness
   ```
   - Warn threshold: 12-48 hours depending on source
   - Error threshold: 48-168 hours

### Weekly Maintenance

1. **VACUUM ANALYZE on large tables**
   ```bash
   UV_CACHE_DIR=../.uv-cache uv run dbt run-operation vacuum_analyze_table \
     --args '{"table_name": "fct_signals"}'
   ```

2. **Generate fresh documentation**
   ```bash
   UV_CACHE_DIR=../.uv-cache uv run dbt docs generate
   ```

3. **Review test results**
   ```bash
   UV_CACHE_DIR=../.uv-cache uv run dbt test --store-failures
   ```

### Emergency Procedures

#### Rollback Model Changes

If a model produces incorrect results:

1. Identify the problematic commit
2. Revert model changes: `git checkout <commit>^ -- models/marts/fct_signals.sql`
3. Rebuild: `UV_CACHE_DIR=../.uv-cache uv run dbt run --select fct_signals+`

### Production Deployment Checklist

Before deploying to production:

- [ ] Run full test suite: `dbt build --target prod --full-refresh`
- [ ] Verify all tests pass
- [ ] Check source freshness thresholds
- [ ] Verify SSL is enabled: `DBT_POSTGRES_SSLMODE=require`
- [ ] Load seed data: `dbt seed --target prod`
- [ ] Generate documentation: `dbt docs generate --target prod`

## Testing

### Test Categories

1. **Schema Tests** - Defined in schema.yml files
   - `unique`, `not_null`, `accepted_values`
   - `relationships` (FK enforcement)
   - `dbt_expectations.*` (range validation)
   - `dbt_utils.unique_combination_of_columns`

2. **Freshness Tests** - Defined in sources.yml
   - Warn/error thresholds for data staleness

3. **Custom Tests** - In tests/ directory
   - Complex validation logic

### Running Tests

```bash
# All tests
UV_CACHE_DIR=../.uv-cache uv run dbt test

# Specific model tests
UV_CACHE_DIR=../.uv-cache uv run dbt test --select fct_signals

# Store failures for debugging
UV_CACHE_DIR=../.uv-cache uv run dbt test --store-failures

# Source freshness only
UV_CACHE_DIR=../.uv-cache uv run dbt source freshness
```

## Custom Macros

| Macro | Purpose | Usage |
|-------|---------|-------|
| `map_anomaly_to_severity` | 9-tier to 4-tier mapping | fct_signals |
| `classify_metric_domain` | Metric to domain mapping | dim_metrics, fct_signals |
| `compute_groupby_label` | Human-readable groupby labels | fct_signals |
| `compute_group_value` | Concatenated dimension values | fct_signals |
| `analyze_table` | Post-hook for ANALYZE | Mart tables |
| `vacuum_analyze_table` | Operation for maintenance | Manual |

## Design Documents

Schema designs are in the `design/` directory:

- `schema_design_node_results.md` - Node result table schemas
- `schema_design_contributions.md` - Contribution table schema

## Rollback Procedure

If dbt migration fails, see `docs/architecture/dbt_rollback.md` for the rollback procedure.
