# dbt Schema Design: Contributions Table

**Design Document for Checklist Task 5.B**
**Date:** 2025-12-14
**Status:** Draft

---

## Overview

This document defines the PostgreSQL table schema for contribution data currently stored in JSONL files. Contribution records represent parent-child metric decompositions showing how child entities contribute to the parent aggregate value.

---

## Source Data Analysis

### JSONL Structure

Each line in the contribution JSONL file contains:
```json
{
  "run_id": "20251210170210",
  "parent_node_id": "losIndex__medicareId_vizientServiceLine__aggregate_time_period",
  "parent_node_file": "/path/to/parent_node.json",
  "parent_entity": {"medicareId": "AFP658", "vizientServiceLine": "Behavioral Health"},
  "child_node_id": "losIndex__medicareId_vizientServiceLine_vizientSubServiceLine__aggregate_time_period",
  "child_node_file": "/path/to/child_node.json",
  "child_entity": {"medicareId": "AFP658", "vizientServiceLine": "Behavioral Health", "vizientSubServiceLine": "..."},
  "metric_id": "losIndex",
  "method": "weighted_mean",
  "child_value": 12.26,
  "parent_value": 8.35,
  "weight_field": "encounters",
  "weight_value": 10412.0,
  "weight_share": 0.205,
  "weighted_child_value": 2.51,
  "contribution_value": 0.205,
  "raw_component": 2.51,
  "excess_over_parent": 0.801,
  "parent_statistics": null,
  "assumptions": ["child_metric=losIndex", "weighted_mean_parent"],
  "fallbacks": [],
  "missing_inputs": [],
  "dataset_row_locator": {"key": {"medicareId": "...", ...}},
  "timestamp": "2025-12-11T13:37:25.419853Z"
}
```

### Key Fields for Signal Prioritization

- `excess_over_parent`: Used to calculate `contribution_weight` (absolute value × weight_share)
- `weight_share`: Proportion of total weight (0.0 to 1.0)
- `parent_node_id` / `child_node_id`: Links to node_results tables

---

## Table Definition

### stg_contributions (Staging: Contribution Records)

```sql
CREATE TABLE stg_contributions (
    -- Primary Key
    id SERIAL PRIMARY KEY,

    -- Run Context
    run_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP,

    -- Parent-Child Relationship
    parent_node_id VARCHAR(255) NOT NULL,
    parent_node_file TEXT,
    child_node_id VARCHAR(255) NOT NULL,
    child_node_file TEXT,

    -- Entity Context (JSONB for flexibility)
    parent_entity JSONB NOT NULL,  -- {"medicareId": "AFP658", "vizientServiceLine": "..."}
    child_entity JSONB,            -- May be null or empty

    -- Denormalized Entity Keys (for efficient filtering)
    parent_facility_id VARCHAR(50),   -- Extracted from parent_entity.medicareId
    parent_service_line VARCHAR(100), -- Extracted from parent_entity.vizientServiceLine
    child_facility_id VARCHAR(50),    -- Extracted from child_entity.medicareId
    child_service_line VARCHAR(100),  -- Extracted from child_entity.vizientServiceLine
    child_sub_service_line VARCHAR(100), -- Extracted from child_entity.vizientSubServiceLine

    -- Metric Context
    metric_id VARCHAR(100) NOT NULL,
    method VARCHAR(50) NOT NULL,  -- 'weighted_mean', etc.

    -- Contribution Values
    child_value NUMERIC(18, 8),
    parent_value NUMERIC(18, 8),
    weight_field VARCHAR(50),
    weight_value NUMERIC(18, 4),
    weight_share NUMERIC(10, 8) NOT NULL,  -- 0.0 to 1.0
    weighted_child_value NUMERIC(18, 8),
    contribution_value NUMERIC(18, 8),
    raw_component NUMERIC(18, 8),
    excess_over_parent NUMERIC(18, 8),  -- Critical for impact calculation

    -- Calculated Fields (for signal prioritization)
    contribution_weight NUMERIC(18, 8) GENERATED ALWAYS AS (
        ABS(COALESCE(excess_over_parent, 0)) * weight_share
    ) STORED,
    contribution_direction VARCHAR(10) GENERATED ALWAYS AS (
        CASE
            WHEN excess_over_parent > 0 THEN 'positive'
            WHEN excess_over_parent < 0 THEN 'negative'
            ELSE 'neutral'
        END
    ) STORED,

    -- Parent Statistics (nullable JSONB)
    parent_statistics JSONB,

    -- Audit Fields (JSONB for arrays)
    assumptions JSONB,  -- ["child_metric=losIndex", "weighted_mean_parent"]
    fallbacks JSONB,
    missing_inputs JSONB,
    dataset_row_locator JSONB,

    -- Constraints
    CHECK (weight_share >= 0 AND weight_share <= 1),
    CHECK (contribution_direction IN ('positive', 'negative', 'neutral')),

    -- Indexes
    INDEX idx_contributions_run (run_id),
    INDEX idx_contributions_parent_node (parent_node_id),
    INDEX idx_contributions_child_node (child_node_id),
    INDEX idx_contributions_metric (metric_id),
    INDEX idx_contributions_weight (contribution_weight DESC),
    INDEX idx_contributions_parent_facility (parent_facility_id),
    INDEX idx_contributions_child_facility (child_facility_id)
);
```

---

## Relationship to Node Results Tables

```
┌─────────────────────────┐           ┌─────────────────────────┐
│      stg_nodes          │           │    stg_contributions    │
│ (canonical_node_id PK)  │◄─────────┤  (parent_node_id FK)    │
└─────────────────────────┘           │  (child_node_id FK)     │
                                      └─────────────────────────┘
```

### Join Strategy

**Primary Access Pattern: Get contribution data for a signal**

```sql
-- Find contributions where this node is the parent
SELECT c.*
FROM stg_contributions c
WHERE c.parent_node_id = :signal_canonical_node_id
  AND c.run_id = :run_id
ORDER BY c.contribution_weight DESC
LIMIT 10;  -- Top 10 contributors
```

**Alternative: Find parent contributions for a child node**

```sql
-- Find what parents this child contributes to
SELECT c.*
FROM stg_contributions c
WHERE c.child_node_id = :signal_canonical_node_id
  AND c.run_id = :run_id;
```

### Foreign Key Considerations

Foreign key constraints to `stg_nodes` are **optional** because:
1. Contribution files may reference nodes not yet loaded
2. Contribution analysis may run before full node results are available
3. Performance impact of FK checks during bulk loading

**Recommendation:** Use indexes for join performance, defer FK enforcement to application layer or post-load validation.

---

## Index Strategy

### Primary Access Patterns

1. **Signal detail panel**: Fetch contributions for `parent_node_id`
2. **Impact sorting**: Sort by `contribution_weight DESC`
3. **Facility filtering**: Filter by `parent_facility_id` or `child_facility_id`
4. **Metric filtering**: Filter by `metric_id`

### Recommended Indexes

```sql
-- Node lookups (most common)
CREATE INDEX idx_contributions_parent_node ON stg_contributions(parent_node_id, run_id);
CREATE INDEX idx_contributions_child_node ON stg_contributions(child_node_id, run_id);

-- Impact sorting
CREATE INDEX idx_contributions_weight ON stg_contributions(contribution_weight DESC)
    WHERE contribution_weight IS NOT NULL;

-- Filtering
CREATE INDEX idx_contributions_metric ON stg_contributions(metric_id);
CREATE INDEX idx_contributions_parent_facility ON stg_contributions(parent_facility_id);
CREATE INDEX idx_contributions_child_facility ON stg_contributions(child_facility_id);
CREATE INDEX idx_contributions_direction ON stg_contributions(contribution_direction);
```

---

## Generated Columns

### contribution_weight

```sql
contribution_weight = ABS(COALESCE(excess_over_parent, 0)) * weight_share
```

This matches the algorithm in `backend/docs/contribution_weighting.md`:
- Magnitude of deviation × proportion of total weight
- Always positive (absolute value)
- Null excess treated as 0

### contribution_direction

```sql
CASE
    WHEN excess_over_parent > 0 THEN 'positive'  -- Child pulls parent UP
    WHEN excess_over_parent < 0 THEN 'negative'  -- Child pulls parent DOWN
    ELSE 'neutral'
END
```

Pre-computed for efficient filtering and display.

---

## Normalization Decisions

### Denormalized (Chosen)

1. **Entity keys** (`parent_facility_id`, `child_service_line`, etc.): Extracted from JSONB for efficient filtering
2. **Generated columns** (`contribution_weight`, `contribution_direction`): Computed once, queried many times

### Kept as JSONB (Chosen)

1. **parent_entity / child_entity**: Full entity context preserved
2. **assumptions / fallbacks / missing_inputs**: Audit arrays, rarely queried
3. **parent_statistics**: Complex nested object, optional

---

## Data Volume Estimates

| Metric | Estimate |
|--------|----------|
| Records per JSONL file | 50-500 |
| JSONL files per run | 50-200 |
| Total records per run | 2,500-100,000 |
| Typical record size | ~1 KB |
| Table size per run | 2.5-100 MB |

---

## Loading Strategy

### Full Refresh (Initial)

```sql
-- Truncate and reload
TRUNCATE stg_contributions;

-- Bulk insert from staging
INSERT INTO stg_contributions (...)
SELECT ... FROM staging_jsonl_import;
```

### Incremental (Future)

```sql
-- Upsert by (run_id, parent_node_id, child_node_id, metric_id)
INSERT INTO stg_contributions (...)
ON CONFLICT (run_id, parent_node_id, child_node_id, metric_id)
DO UPDATE SET ...;
```

**Note:** Requires adding unique constraint:
```sql
ALTER TABLE stg_contributions
ADD CONSTRAINT uk_contributions_identity
UNIQUE (run_id, parent_node_id, child_node_id, metric_id);
```

---

## Validation Queries

### Row Count Check

```sql
SELECT run_id, COUNT(*) as record_count
FROM stg_contributions
GROUP BY run_id;
```

### Null Check

```sql
SELECT
    COUNT(*) as total,
    COUNT(excess_over_parent) as has_excess,
    COUNT(contribution_weight) as has_weight
FROM stg_contributions
WHERE run_id = :run_id;
```

### Weight Share Validation

```sql
-- Weight shares should sum to ~1.0 per parent
SELECT
    parent_node_id,
    parent_entity,
    SUM(weight_share) as total_weight_share
FROM stg_contributions
WHERE run_id = :run_id
GROUP BY parent_node_id, parent_entity
HAVING ABS(SUM(weight_share) - 1.0) > 0.01;  -- Flag if >1% off
```

---

## Next Steps

1. ✅ Design schema (this document)
2. [ ] Create dbt staging model (5.F)
3. [ ] Add schema.yml documentation (5.I)
4. [ ] Validate with test data (5.N.1)
