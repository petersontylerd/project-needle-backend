# dbt Schema Design: Node Results Tables

**Design Document for Checklist Task 5.A**
**Date:** 2025-12-14
**Status:** Draft

---

## Overview

This document defines the PostgreSQL table schemas for node result data currently stored in JSON files. The design follows a normalized approach with clear separation of concerns while maintaining query efficiency for common access patterns.

---

## Entity-Relationship Diagram

```
┌─────────────────────────┐
│      stg_nodes          │
│ (canonical_node_id PK)  │
└─────────────┬───────────┘
              │
   ┌──────────┼──────────┐
   │          │          │
   ▼          ▼          ▼
┌───────┐ ┌────────┐ ┌──────────────┐
│ edges │ │entities│ │entity_results│
└───────┘ └────────┘ └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
       ┌──────────┐ ┌───────────┐ ┌────────────────────┐
       │ metrics  │ │statistics │ │statistical_methods │
       └──────────┘ └───────────┘ └─────────┬──────────┘
                                            │
                                            ▼
                                     ┌───────────┐
                                     │ anomalies │
                                     └───────────┘
```

---

## Table Definitions

### 1. stg_nodes (Staging: Node Core)

Primary table for node metadata.

```sql
CREATE TABLE stg_nodes (
    -- Primary Key
    canonical_node_id VARCHAR(255) PRIMARY KEY,

    -- Metadata
    dataset_path TEXT,
    run_id VARCHAR(100) NOT NULL,  -- Links to run manifest
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Indexes
    INDEX idx_nodes_run_id (run_id)
);
```

**Rationale:**
- `canonical_node_id` is globally unique across runs
- `run_id` enables filtering by analysis run
- Simple flat structure for fast lookups

---

### 2. stg_node_edges (Staging: Parent/Child Edges)

Represents parent-child relationships between nodes.

```sql
CREATE TABLE stg_node_edges (
    -- Primary Key (composite)
    id SERIAL PRIMARY KEY,

    -- Foreign Keys
    canonical_node_id VARCHAR(255) NOT NULL REFERENCES stg_nodes(canonical_node_id),
    related_node_id VARCHAR(255) NOT NULL,  -- May reference future node

    -- Edge Attributes
    edge_type VARCHAR(50) NOT NULL,  -- 'trends_to', 'drills_to', 'relates_to', 'derives_to'
    edge_direction VARCHAR(10) NOT NULL,  -- 'parent' or 'child'

    -- Constraints
    UNIQUE (canonical_node_id, related_node_id, edge_direction),
    CHECK (edge_type IN ('trends_to', 'drills_to', 'relates_to', 'derives_to')),
    CHECK (edge_direction IN ('parent', 'child')),

    -- Indexes
    INDEX idx_edges_node (canonical_node_id),
    INDEX idx_edges_related (related_node_id),
    INDEX idx_edges_type (edge_type)
);
```

**Rationale:**
- Separate table for edges allows many-to-many relationships
- `edge_direction` distinguishes parent vs child references
- Constraint on `edge_type` enforces taxonomy

---

### 3. stg_entity_results (Staging: Entity-Level Results)

One row per entity within a node.

```sql
CREATE TABLE stg_entity_results (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key
    canonical_node_id VARCHAR(255) NOT NULL REFERENCES stg_nodes(canonical_node_id),

    -- Entity Identification (JSONB for flexibility)
    entity_fields JSONB NOT NULL,  -- [{"id": "medicareId", "value": "AFP658", "dataset_field": "medicareId"}]

    -- Denormalized Entity Keys (for efficient querying)
    facility_id VARCHAR(50),  -- Extracted from entity_fields where id='medicareId'
    service_line VARCHAR(100),  -- Extracted from entity_fields where id='vizientServiceLine'
    sub_service_line VARCHAR(100),  -- Extracted from entity_fields where id='vizientSubServiceLine'

    -- Aggregate Data
    encounters INTEGER NOT NULL,

    -- Indexes
    INDEX idx_entity_results_node (canonical_node_id),
    INDEX idx_entity_results_facility (facility_id),
    INDEX idx_entity_results_service_line (service_line)
);
```

**Rationale:**
- JSONB `entity_fields` preserves full entity data
- Denormalized keys (`facility_id`, `service_line`) enable fast filtering
- Single row per entity simplifies joins

---

### 4. stg_entity_metrics (Staging: Metric Values)

Metric values for each entity (supports both aggregate and temporal).

```sql
CREATE TABLE stg_entity_metrics (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key
    entity_result_id INTEGER NOT NULL REFERENCES stg_entity_results(id),

    -- Metric Identification
    metric_id VARCHAR(100) NOT NULL,  -- 'losIndex', 'readmissionRate', etc.

    -- Aggregate Value (for aggregate nodes)
    aggregate_value NUMERIC(12, 6),

    -- Temporal Runtime (for temporal nodes)
    coverage_ratio NUMERIC(5, 4),
    missing_count INTEGER,
    observed_count INTEGER,
    fill_strategy VARCHAR(50),

    -- Indexes
    INDEX idx_entity_metrics_result (entity_result_id),
    INDEX idx_entity_metrics_metric (metric_id)
);
```

---

### 5. stg_metric_timeline (Staging: Temporal Timeline Values)

For temporal nodes, stores period-by-period values.

```sql
CREATE TABLE stg_metric_timeline (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key
    entity_metric_id INTEGER NOT NULL REFERENCES stg_entity_metrics(id),

    -- Period Data
    period VARCHAR(10) NOT NULL,  -- '202407', '202408', etc.
    period_value NUMERIC(12, 6) NOT NULL,
    period_encounters INTEGER,

    -- Constraints
    UNIQUE (entity_metric_id, period),

    -- Indexes
    INDEX idx_timeline_metric (entity_metric_id),
    INDEX idx_timeline_period (period)
);
```

**Rationale:**
- Normalized timeline table allows efficient period queries
- Unique constraint prevents duplicate periods
- Period format supports both YYYYMM and YYYY-MM

---

### 6. stg_statistical_methods (Staging: Statistical Method Results)

Statistical method metadata and computed statistics.

```sql
CREATE TABLE stg_statistical_methods (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key
    entity_result_id INTEGER NOT NULL REFERENCES stg_entity_results(id),

    -- Method Identification
    statistical_method VARCHAR(255) NOT NULL,  -- Full method identifier

    -- Runtime Status
    runtime_status VARCHAR(20) DEFAULT 'ok',  -- 'ok', 'warning', 'error'
    has_issues BOOLEAN DEFAULT FALSE,

    -- Aggregate Statistics
    peer_mean NUMERIC(12, 6),
    peer_std NUMERIC(12, 6),
    simple_zscore NUMERIC(12, 6),
    robust_zscore NUMERIC(12, 6),
    percentile_rank NUMERIC(7, 4),  -- 0.0000 to 100.0000
    suppressed BOOLEAN DEFAULT FALSE,

    -- Temporal Statistics
    latest_period VARCHAR(10),
    latest_simple_zscore NUMERIC(12, 6),
    mean_simple_zscore NUMERIC(12, 6),
    latest_robust_zscore NUMERIC(12, 6),
    mean_robust_zscore NUMERIC(12, 6),
    observations INTEGER,
    periods INTEGER,
    peer_mad NUMERIC(12, 6),
    peer_median NUMERIC(12, 6),
    winsor_applied BOOLEAN,

    -- Indexes
    INDEX idx_stat_methods_entity (entity_result_id),
    INDEX idx_stat_methods_method (statistical_method),
    INDEX idx_stat_methods_zscore (simple_zscore)
);
```

**Rationale:**
- Single table handles both aggregate and temporal statistics
- Null columns for inapplicable statistics (aggregate vs temporal)
- Index on `simple_zscore` supports severity filtering

---

### 7. stg_anomalies (Staging: Anomaly Detection Results)

Individual anomaly classification results.

```sql
CREATE TABLE stg_anomalies (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Foreign Key
    statistical_method_id INTEGER NOT NULL REFERENCES stg_statistical_methods(id),

    -- Anomaly Profile
    anomaly_profile VARCHAR(255) NOT NULL,

    -- Method Results (denormalized from methods array)
    anomaly_method VARCHAR(255),
    anomaly_classification VARCHAR(50) NOT NULL,  -- 'severe_high', 'moderately_high', 'normal', etc.
    applies_to VARCHAR(100),  -- 'simple_zscore', 'latest_simple_zscore', etc.
    statistic_value NUMERIC(12, 6),

    -- Interpretation
    interpretation_rendered TEXT,
    interpretation_template_id VARCHAR(255),

    -- Indexes
    INDEX idx_anomalies_method (statistical_method_id),
    INDEX idx_anomalies_classification (anomaly_classification),
    INDEX idx_anomalies_profile (anomaly_profile)
);
```

**Rationale:**
- Flattened from nested JSON structure for efficient querying
- `anomaly_classification` enables severity filtering
- Full interpretation text stored for UI display

---

## Normalization Decisions

### Normalized Tables (Chosen)
1. **stg_node_edges** - Many-to-many relationships require separate table
2. **stg_metric_timeline** - Variable-length arrays require separate table
3. **stg_anomalies** - Nested array flattened for query efficiency

### Denormalized Fields (Chosen)
1. **stg_entity_results.facility_id/service_line** - Frequently filtered, extracted from JSONB
2. **stg_statistical_methods** - All statistics in one row (no need for method-specific tables)

### JSONB Storage (Chosen)
1. **stg_entity_results.entity_fields** - Preserves original structure, rarely queried directly

---

## Index Strategy

### Primary Access Patterns
1. **Signal hydration**: `canonical_node_id` → entity_results → statistics
2. **Severity filtering**: Filter by `anomaly_classification` or `simple_zscore`
3. **Facility filtering**: Filter by `facility_id` in entity_results
4. **Service line filtering**: Filter by `service_line` in entity_results

### Recommended Indexes
```sql
-- Primary lookups
CREATE INDEX idx_nodes_run ON stg_nodes(run_id);
CREATE INDEX idx_entity_results_node ON stg_entity_results(canonical_node_id);
CREATE INDEX idx_stat_methods_entity ON stg_statistical_methods(entity_result_id);

-- Filtering
CREATE INDEX idx_entity_results_facility ON stg_entity_results(facility_id);
CREATE INDEX idx_entity_results_service_line ON stg_entity_results(service_line);
CREATE INDEX idx_anomalies_classification ON stg_anomalies(anomaly_classification);

-- Z-score sorting
CREATE INDEX idx_stat_methods_zscore ON stg_statistical_methods(simple_zscore);
```

---

## Migration Path

### Phase 1: Parallel Loading
1. Load JSON files into staging tables
2. Keep existing JSON loading code functional
3. Validate row counts and data integrity

### Phase 2: Service Integration
1. Update SignalHydrator to read from staging tables
2. A/B test: compare results from JSON vs dbt
3. Measure query performance improvements

### Phase 3: JSON Deprecation
1. Remove JSON loading code paths
2. Archive JSON files
3. dbt becomes single source of truth

---

## Performance Considerations

### Estimated Row Counts (per run)
- `stg_nodes`: ~500-2000 nodes
- `stg_entity_results`: ~5000-20000 entities
- `stg_statistical_methods`: ~10000-40000 methods
- `stg_anomalies`: ~20000-80000 anomalies
- `stg_metric_timeline`: ~100000-500000 periods

### Query Optimization
- Use covering indexes for common queries
- Partition `stg_metric_timeline` by period range if needed
- Consider materialized views for complex aggregations

---

## Open Questions

1. **Multi-run support**: Should tables support multiple runs simultaneously, or one run at a time?
   - **Recommendation**: Support multiple runs via `run_id` column

2. **Historical retention**: How long to retain historical data?
   - **Recommendation**: Define retention policy (e.g., 90 days)

3. **Incremental loading**: Full refresh or incremental updates?
   - **Recommendation**: Start with full refresh, add incremental later

---

## Next Steps

1. ✅ Design schema (this document)
2. [ ] Create dbt project structure (5.D)
3. [ ] Implement staging models (5.E)
4. [ ] Add schema.yml documentation (5.I)
5. [ ] Validate with test data (5.N.1)
