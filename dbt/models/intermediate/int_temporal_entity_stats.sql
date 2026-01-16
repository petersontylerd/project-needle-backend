-- int_temporal_entity_stats.sql
-- Intermediate model that extracts temporal statistics from temporal nodes,
-- keyed by (aggregate_node_id, facility_id, entity_dimensions_hash).
--
-- This allows fct_signals to join aggregate entity results with their
-- corresponding temporal entity statistics.
--
-- Grain: One row per entity per aggregate-temporal node pair per run
--
-- Join Strategy:
-- 1. Use stg_node_edges to find aggregate → temporal node mappings (trends_to edge)
-- 2. Use stg_entity_results to get temporal node entity results
-- 3. Use int_statistical_methods_agg to get aggregated temporal stats per entity
-- 4. Key by aggregate node ID + entity key hash for downstream join

{{
    config(
        materialized='table',
        tags=['intermediate', 'temporal'],
        indexes=[
            {'columns': ['aggregate_node_id', 'run_id', 'facility_id', 'entity_dimensions_hash']}
        ]
    )
}}

with edges as (
    -- Get aggregate → temporal node mappings via trends_to edges
    select
        canonical_node_id as aggregate_node_id,
        related_node_id as temporal_node_id,
        run_id
    from {{ ref('stg_node_edges') }}
    where edge_type = 'trends_to'
      and edge_direction = 'child'
),

percentile_trends as (
    -- Get percentile trends from temporal nodes (node-level data)
    -- Use DISTINCT ON to get one row per node/run (in case of multiple stat methods)
    select distinct on (canonical_node_id, run_id)
        canonical_node_id as temporal_node_id,
        run_id,
        percentile_trends
    from {{ ref('stg_percentile_trends') }}
    order by canonical_node_id, run_id, loaded_at desc
),

temporal_entity_results as (
    -- Get entity results from temporal nodes only
    -- (those that have a parent aggregate node via trends_to edge)
    select
        er.entity_result_id,
        er.canonical_node_id as temporal_node_id,
        er.run_id,
        er.facility_id,
        er.entity_dimensions,
        md5(coalesce(er.entity_dimensions::text, '{}')) as entity_dimensions_hash,
        er.metric_timeline,
        er.metadata_per_period
    from {{ ref('stg_entity_results') }} er
    inner join edges e
        on er.canonical_node_id = e.temporal_node_id
        and er.run_id = e.run_id
),

temporal_methods as (
    -- Get aggregated statistical methods for temporal entities
    select
        sm.entity_result_id,
        sm.slope,
        sm.slope_percentile,
        sm.acceleration,
        sm.trend_direction,
        sm.momentum,
        sm.latest_simple_zscore,
        sm.mean_simple_zscore,
        sm.latest_robust_zscore,
        sm.mean_robust_zscore,
        sm.monthly_z_scores
    from {{ ref('int_statistical_methods_agg') }} sm
),

joined as (
    -- Join temporal entity results with their statistical methods
    select
        ter.entity_result_id,
        ter.temporal_node_id,
        ter.run_id,
        ter.facility_id,
        ter.entity_dimensions,
        ter.entity_dimensions_hash,
        ter.metric_timeline,
        ter.metadata_per_period,
        tm.slope,
        tm.slope_percentile,
        tm.acceleration,
        tm.trend_direction,
        tm.momentum,
        tm.latest_simple_zscore,
        tm.mean_simple_zscore,
        tm.latest_robust_zscore,
        tm.mean_robust_zscore,
        tm.monthly_z_scores
    from temporal_entity_results ter
    left join temporal_methods tm
        on ter.entity_result_id = tm.entity_result_id
),

-- Attach aggregate node ID and percentile trends for downstream join in fct_signals
with_aggregate_node as (
    select
        e.aggregate_node_id,
        j.*,
        pt.percentile_trends as peer_percentile_trends
    from joined j
    inner join edges e
        on j.temporal_node_id = e.temporal_node_id
        and j.run_id = e.run_id
    left join percentile_trends pt
        on j.temporal_node_id = pt.temporal_node_id
        and j.run_id = pt.run_id
)

select
    {{ dbt_utils.generate_surrogate_key([
        'aggregate_node_id',
        'run_id',
        'facility_id',
        'entity_dimensions_hash'
    ]) }} as temporal_stats_id,
    aggregate_node_id,
    temporal_node_id,
    run_id,
    facility_id,
    entity_dimensions,
    entity_dimensions_hash,
    -- Temporal z-scores (prefixed to avoid confusion with aggregate values)
    latest_simple_zscore as temporal_latest_simple_zscore,
    mean_simple_zscore as temporal_mean_simple_zscore,
    latest_robust_zscore as temporal_latest_robust_zscore,
    mean_robust_zscore as temporal_mean_robust_zscore,
    -- Slope statistics
    slope as temporal_slope,
    slope_percentile as temporal_slope_percentile,
    acceleration as temporal_acceleration,
    trend_direction as temporal_trend_direction,
    momentum as temporal_momentum,
    -- Monthly z-scores for consistency tier calculation
    monthly_z_scores as temporal_monthly_z_scores,
    -- Metric timeline for sparkline visualization
    metric_timeline as temporal_metric_timeline,
    -- Peer distribution percentile trends (node-level data)
    peer_percentile_trends,
    -- Per-period metadata (e.g., encounter counts per month)
    metadata_per_period,
    current_timestamp as dbt_updated_at
from with_aggregate_node
