# dbt Pipeline

This document describes the dbt transformation layer that processes analytics data for the Quality Compass backend.

## Overview

The dbt project (`dbt/`) transforms raw data loaded from insight graph artifacts into analytics-ready tables. The primary output is `fct_signals`, which feeds the backend's `SignalHydrator` service.

## Project Structure

```
dbt/
├── dbt_project.yml         # Project configuration
├── profiles.yml            # Connection profiles (not committed)
├── models/
│   ├── staging/            # Data cleaning (stg_* models)
│   ├── intermediate/       # Aggregations and joins (int_* models)
│   ├── marts/              # Fact and dimension tables
│   │   ├── fct_signals.sql
│   │   ├── fct_contributions.sql
│   │   ├── fct_model_drivers.sql
│   │   ├── dim_facilities.sql
│   │   ├── dim_metrics.sql
│   │   └── schema.yml
│   └── semantic/           # Metric and entity definitions
├── macros/                 # Reusable SQL functions
├── seeds/                  # Static reference data
├── snapshots/             # Slowly changing dimension tracking
└── tests/                  # Data quality tests
```

## Data Layers

### Raw Layer (Source)

Raw tables are loaded by `scripts/load_insight_graph_to_dbt.py` before dbt runs:

| Table | Description |
|-------|-------------|
| `raw_node_results` | Insight graph node data (JSON parsed) |
| `raw_entity_results` | Entity-level results extracted from nodes |
| `raw_statistical_methods` | Z-scores and anomaly labels per method |
| `raw_global_statistics` | Global metric statistics |
| `raw_classifications` | 9-type signal classifications |
| `raw_modeling_runs` | ML experiment metadata |
| `raw_modeling_experiments` | Individual experiment results |

### Staging Layer

Staging models clean and normalize raw data:

| Model | Source | Purpose |
|-------|--------|---------|
| `stg_node_results` | raw_node_results | Parse JSON, extract fields |
| `stg_entity_results` | raw_entity_results | Normalize entity dimensions |
| `stg_statistical_methods` | raw_statistical_methods | Standardize method names |
| `stg_global_statistics` | raw_global_statistics | Parse facet values |
| `stg_classifications` | raw_classifications | Extract classification fields |
| `stg_node_edges` | raw_node_results | Parse edge arrays |

**Configuration:**
```yaml
staging:
  +materialized: table
  +schema: staging
  +tags: ['staging']
```

### Intermediate Layer

Intermediate models aggregate and join staging data:

| Model | Purpose |
|-------|---------|
| `int_statistical_methods_agg` | Aggregate methods per entity (one row per entity) |
| `int_temporal_entity_stats` | Extract temporal statistics from temporal nodes |

**Configuration:**
```yaml
intermediate:
  +materialized: table
  +schema: intermediate
  +tags: ['intermediate']
```

### Marts Layer

Marts are the final analytics-ready tables:

| Model | Grain | Purpose |
|-------|-------|---------|
| `fct_signals` | One row per entity/metric | Primary signal table for API |
| `fct_contributions` | One row per contribution | Hierarchical contribution analysis |
| `fct_model_drivers` | One row per experiment | ML experiment results |
| `dim_facilities` | One row per facility | Facility dimension |
| `dim_metrics` | One row per metric | Metric dimension |

**Configuration:**
```yaml
marts:
  +materialized: table
  +schema: marts
  +tags: ['marts']
  +post-hook:
    - "{{ analyze_table() }}"  # Run ANALYZE for query optimization
```

Output schema: `public_marts`

## fct_signals Model

The primary output table that feeds the backend. Grain: one row per signal (entity + metric combination).

### Pipeline Flow

```
stg_entity_results
    │
    ▼
┌─────────────────────────────────────────┐
│            entity_base CTE              │
│  - Base entity results                  │
│  - No statistical method fan-out        │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│           with_methods CTE              │
│  - Join aggregated statistical methods  │
│  - Filter suppressed rows               │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│        with_global_benchmark CTE        │
│  - Join global metric statistics        │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│            with_edges CTE               │
│  - Add temporal node reference          │
│  - Via trends_to edges                  │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│        with_temporal_stats CTE          │
│  - Enrich with temporal node data       │
│  - Slope, monthly z-scores, timeline    │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│       with_classifications CTE          │
│  - Add 9-type signal classification     │
│  - Magnitude, trajectory, consistency   │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│             enriched CTE                │
│  - Compute groupby_label, group_value   │
│  - Prepare metric_trend_timeline        │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│           with_domain CTE               │
│  - Add quality domain classification    │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│           signals_only CTE              │
│  - Filter to actual signals             │
│  - Require non-null classification      │
└─────────────────────────────────────────┘
    │
    ▼
           fct_signals table
```

### Key Columns

**Identification:**
- `signal_id` - Surrogate key
- `run_id` - Analytics run identifier
- `canonical_node_id` - Reference to insight graph node
- `temporal_node_id` - Reference to linked temporal node

**Entity Context:**
- `system_name` - Health system
- `facility_id` - Facility code
- `service_line` / `sub_service_line`
- `entity_dimensions` - JSONB of dimension key-value pairs
- `entity_dimensions_hash` - MD5 hash for joining

**Metric Data:**
- `metric_id` - Metric identifier
- `metric_value` - Current value
- `benchmark_value` - Global or peer mean
- `percentile_rank` - Position in peer distribution

**Statistical Methods:**
- `statistical_methods` - JSONB array of all method details
- `simple_zscore` / `robust_zscore` - Primary z-scores
- `slope` / `slope_percentile` - Trend metrics

**Classification:**
- `simplified_signal_type` - 9-type classification
- `simplified_severity` - Severity score 0-100
- `magnitude_tier` / `trajectory_tier` / `consistency_tier`
- `domain` - Quality domain (Efficiency, Safety, Effectiveness)

## Running dbt

### Prerequisites

```bash
# Install dependencies
cd dbt
dbt deps
```

### Commands

**Full run:**
```bash
dbt run
```

**Run specific layer:**
```bash
dbt run --select staging.*
dbt run --select marts.*
```

**Run specific model:**
```bash
dbt run --select fct_signals
```

**Run with dependencies:**
```bash
dbt run --select +fct_signals  # Include upstream
dbt run --select fct_signals+  # Include downstream
```

**Test models:**
```bash
dbt test
dbt test --select fct_signals
```

**Generate documentation:**
```bash
dbt docs generate
dbt docs serve  # Start docs server on port 8080
```

### Variables

Override via command line:

```bash
dbt run --vars '{"run_id": "20251215100000"}'
```

Variables defined in `dbt_project.yml`:
- `run_id` - Default run ID for testing
- `min_encounters` - Minimum encounters threshold (30)

## Custom Macros

Located in `dbt/macros/`:

**`compute_groupby_label(entity_dimensions)`**
- Generates human-readable label from dimension keys
- Example: "Service Line" or "Facility > Service Line"

**`compute_group_value(entity_dimensions)`**
- Extracts values from entity dimensions
- Example: "Cardiology" or "General Hospital > Cardiology"

**`classify_metric_domain(metric_id)`**
- Maps metric IDs to quality domains
- Returns: Efficiency, Safety, or Effectiveness

**`analyze_table()`**
- Post-hook that runs PostgreSQL ANALYZE
- Improves query plan optimization

## Data Quality Tests

Schema tests in `schema.yml`:

```yaml
models:
  - name: fct_signals
    columns:
      - name: signal_id
        tests:
          - unique
          - not_null
      - name: metric_value
        tests:
          - not_null
      - name: simplified_signal_type
        tests:
          - not_null
          - accepted_values:
              values: ['chronic_underperformer', 'critical_trajectory', ...]
```

## Integration with Backend

After dbt runs:

1. Tables are created in `public_marts` schema
2. Backend's `SignalHydrator` queries `public_marts.fct_signals`
3. Records are upserted into application's `signals` table
4. API endpoints read from application tables

This separation allows:
- dbt to handle transformation logic
- Application tables to maintain workflow state
- Independent schema evolution
