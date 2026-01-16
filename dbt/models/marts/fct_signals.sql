-- fct_signals.sql
-- Fact table for quality signals ready for API consumption.
-- Joins staging tables to produce denormalized signal records matching
-- the backend Signal model structure.
--
-- Grain: One row per signal (entity + metric combination)
--
-- This model replaces JSON file loading in SignalHydrator by providing
-- all signal data in a single queryable table.

{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'signal']
    )
}}

with entity_base as (
    -- Base entity results without statistical method fan-out
    select
        e.entity_result_id,
        e.canonical_node_id,
        e.run_id,
        e.system_name,
        e.facility_id,
        e.service_line,
        e.sub_service_line,
        e.encounters,
        e.metric_id,
        e.metric_value,
        e.entity_fields,
        e.entity_dimensions,
        -- Use pre-computed hash from staging (avoids MD5 computation per row)
        e.entity_dimensions_hash as entity_key_hash,
        e.metric_timeline,
        e.metadata
    from {{ ref('stg_entity_results') }} e
),

with_methods as (
    -- Join aggregated statistical methods (one row per entity)
    select
        eb.*,
        sm.statistical_methods,
        sm.primary_simple_zscore as simple_zscore,
        sm.primary_robust_zscore as robust_zscore,
        sm.primary_percentile_rank as percentile_rank,
        sm.primary_peer_mean as peer_mean,
        sm.primary_peer_std as peer_std,
        sm.primary_peer_count as peer_count,
        sm.latest_simple_zscore,
        sm.mean_simple_zscore,
        sm.latest_robust_zscore,
        sm.mean_robust_zscore,
        sm.slope,
        sm.slope_percentile,
        sm.acceleration,
        sm.trend_direction,
        sm.momentum,
        sm.monthly_z_scores,
        sm.simple_zscore_anomaly,
        sm.robust_zscore_anomaly,
        sm.latest_simple_zscore_anomaly,
        sm.mean_simple_zscore_anomaly,
        sm.latest_robust_zscore_anomaly,
        sm.mean_robust_zscore_anomaly,
        sm.slope_anomaly,
        sm.any_suppressed
    from entity_base eb
    left join {{ ref('int_statistical_methods_agg') }} sm
        on eb.entity_result_id = sm.entity_result_id
    -- Filter suppressed rows early to reduce downstream processing
    where not coalesce(sm.any_suppressed, false)
),

-- Join with global statistics to get global benchmark values.
-- Filter to robust_zscore method directly instead of using DISTINCT ON.
-- This avoids expensive sort operation by selecting the method explicitly.
with_global_benchmark as (
    select
        wm.*,
        gs.global_metric_mean,
        gs.global_metric_std
    from with_methods wm
    left join {{ ref('stg_global_statistics') }} gs
        on wm.canonical_node_id = gs.canonical_node_id
        and wm.run_id = gs.run_id
        and wm.entity_dimensions = gs.facet_values
        and gs.statistical_method_full = 'robust_zscore'
),

with_edges as (
    -- Add temporal node reference via trends_to edges
    select
        gb.*,
        te.related_node_id as temporal_node_id
    from with_global_benchmark gb
    left join {{ ref('stg_node_edges') }} te
        on gb.canonical_node_id = te.canonical_node_id
        and te.edge_type = 'trends_to'
        and te.edge_direction = 'child'
),

-- Add temporal entity statistics from the linked temporal node
-- This enriches aggregate signals with trend data (slope, monthly z-scores, etc.)
with_temporal_stats as (
    select
        we.*,
        -- Override temporal fields with actual temporal node data
        -- Use COALESCE to prefer temporal values over aggregate (which are NULL)
        coalesce(ts.temporal_slope, we.slope) as slope_enriched,
        coalesce(ts.temporal_slope_percentile, we.slope_percentile) as slope_percentile_enriched,
        coalesce(ts.temporal_acceleration, we.acceleration) as acceleration_enriched,
        coalesce(ts.temporal_trend_direction, we.trend_direction) as trend_direction_enriched,
        coalesce(ts.temporal_momentum, we.momentum) as momentum_enriched,
        coalesce(ts.temporal_latest_simple_zscore, we.latest_simple_zscore) as latest_simple_zscore_enriched,
        coalesce(ts.temporal_mean_simple_zscore, we.mean_simple_zscore) as mean_simple_zscore_enriched,
        coalesce(ts.temporal_latest_robust_zscore, we.latest_robust_zscore) as latest_robust_zscore_enriched,
        coalesce(ts.temporal_mean_robust_zscore, we.mean_robust_zscore) as mean_robust_zscore_enriched,
        coalesce(ts.temporal_monthly_z_scores, we.monthly_z_scores) as monthly_z_scores_enriched,
        -- Metric timeline from temporal node for sparkline visualization
        coalesce(ts.temporal_metric_timeline, we.metric_timeline) as metric_timeline_enriched,
        -- Peer percentile trends for reference band visualization
        ts.peer_percentile_trends,
        -- Period-specific metadata for temporal hydration
        ts.metadata_per_period
    from with_edges we
    left join {{ ref('int_temporal_entity_stats') }} ts
        on we.canonical_node_id = ts.aggregate_node_id
        and we.run_id = ts.run_id
        and we.facility_id = ts.facility_id
        and we.entity_key_hash = ts.entity_dimensions_hash
),

-- Add classification data from stg_classifications (9 Signal Type system)
with_classifications as (
    select
        wts.entity_result_id,
        wts.canonical_node_id,
        wts.run_id,
        wts.system_name,
        wts.facility_id,
        wts.service_line,
        wts.sub_service_line,
        wts.encounters,
        wts.metric_id,
        wts.metric_value,
        wts.entity_fields,
        wts.entity_dimensions,
        wts.entity_key_hash,
        wts.metric_timeline,
        wts.metadata,
        wts.metadata_per_period,
        wts.statistical_methods,
        wts.simple_zscore,
        wts.robust_zscore,
        wts.percentile_rank,
        wts.peer_mean,
        wts.peer_std,
        wts.peer_count,
        wts.latest_simple_zscore,
        wts.mean_simple_zscore,
        wts.latest_robust_zscore,
        wts.mean_robust_zscore,
        wts.slope,
        wts.slope_percentile,
        wts.acceleration,
        wts.trend_direction,
        wts.momentum,
        wts.monthly_z_scores,
        wts.simple_zscore_anomaly,
        wts.robust_zscore_anomaly,
        wts.latest_simple_zscore_anomaly,
        wts.mean_simple_zscore_anomaly,
        wts.latest_robust_zscore_anomaly,
        wts.mean_robust_zscore_anomaly,
        wts.slope_anomaly,
        wts.any_suppressed,
        wts.global_metric_mean,
        wts.global_metric_std,
        wts.temporal_node_id,
        wts.slope_enriched,
        wts.slope_percentile_enriched,
        wts.acceleration_enriched,
        wts.trend_direction_enriched,
        wts.momentum_enriched,
        wts.latest_simple_zscore_enriched,
        wts.mean_simple_zscore_enriched,
        wts.latest_robust_zscore_enriched,
        wts.mean_robust_zscore_enriched,
        wts.monthly_z_scores_enriched,
        wts.metric_timeline_enriched,
        wts.peer_percentile_trends,
        -- 9 Signal Type classification
        c.signal_type as simplified_signal_type,
        c.severity as simplified_severity,
        c.severity_range as simplified_severity_range,
        c.inputs as simplified_inputs,
        c.derived_indicators as simplified_indicators,
        c.classification_reasoning as simplified_reasoning,
        c.severity_calculation as simplified_severity_calculation,
        -- 3D Matrix dimensional tiers (kept for technical display)
        c.magnitude_tier,
        c.trajectory_tier,
        c.consistency_tier,
        c.coefficient_of_variation
    from with_temporal_stats wts
    left join {{ ref('stg_classifications') }} c
        on wts.canonical_node_id = c.canonical_node_id
        and wts.run_id = c.run_id
        and wts.metric_id = c.metric_id
        and wts.facility_id = c.facility_id
        -- Must also match entity dimensions to avoid fan-out on faceted nodes
        and wts.entity_key_hash = c.entity_key_hash
),

-- Add entity dimensions and computed groupby fields
enriched as (
    select
        wc.*,
        -- entity_key_hash already computed in entity_base, rename for output
        wc.entity_key_hash as entity_dimensions_hash,
        {{ compute_groupby_label('wc.entity_dimensions') }} as groupby_label,
        {{ compute_group_value('wc.entity_dimensions') }} as group_value,
        -- Metric trend timeline enriched from temporal node (for aggregate signals)
        wc.metric_timeline_enriched as metric_trend_timeline
    from with_classifications wc
),

-- Add metric domain classification
with_domain as (
    select
        *,
        {{ classify_metric_domain('metric_id') }} as domain
    from enriched
),

-- Filter to actual signals (non-null classification)
-- Note: Suppressed rows already filtered in with_methods CTE
signals_only as (
    select *
    from with_domain
    where simplified_signal_type is not null
)

select
    -- Surrogate key (entity-level grain, no method/anomaly)
    {{ dbt_utils.generate_surrogate_key([
        'run_id',
        'canonical_node_id',
        'entity_result_id'
    ]) }} as signal_id,

    -- Run context
    run_id,

    -- Node reference
    canonical_node_id,
    temporal_node_id,

    -- Entity identification
    system_name,
    facility_id,
    service_line,
    sub_service_line,

    -- Metric context
    metric_id,
    metric_value,
    -- Use global_metric_mean as benchmark (falls back to peer_mean if not available)
    coalesce(global_metric_mean, peer_mean) as benchmark_value,

    -- Computed variance using global benchmark
    case
        when coalesce(global_metric_mean, peer_mean) is not null
             and coalesce(global_metric_mean, peer_mean) != 0
        then round(((metric_value - coalesce(global_metric_mean, peer_mean)) / coalesce(global_metric_mean, peer_mean) * 100)::numeric, 2)
        else null
    end as variance_percent,

    -- Statistical methods as JSONB array (replaces individual method columns)
    statistical_methods,

    -- Primary statistical measures (from simple_zscore method for display)
    simple_zscore,
    robust_zscore,
    -- Temporal node z-score statistics (enriched from temporal node)
    latest_simple_zscore_enriched as latest_simple_zscore,
    mean_simple_zscore_enriched as mean_simple_zscore,
    latest_robust_zscore_enriched as latest_robust_zscore,
    mean_robust_zscore_enriched as mean_robust_zscore,
    percentile_rank,
    peer_std,
    peer_count,
    encounters,

    -- Global benchmark context
    global_metric_mean,
    global_metric_std,

    -- Signal type based description
    case
        when simplified_signal_type is not null then
            simplified_signal_type || ' (severity: ' || simplified_severity || ')'
        else null
    end as description,

    -- Quality domain classification
    domain,

    -- =========================================================================
    -- Entity grouping (4 fields)
    -- =========================================================================
    entity_dimensions,
    entity_dimensions_hash,
    groupby_label,
    group_value,

    -- =========================================================================
    -- Temporal/slope statistics (8 fields) - enriched from temporal node
    -- =========================================================================
    metric_trend_timeline,
    monthly_z_scores_enriched as monthly_z_scores,
    slope_enriched as slope,
    slope_percentile_enriched as slope_percentile,
    acceleration_enriched as acceleration,
    trend_direction_enriched as trend_direction,
    momentum_enriched as momentum,
    -- Peer percentile trends for reference band visualization
    peer_percentile_trends,

    -- =========================================================================
    -- Anomaly labels (7 fields)
    -- =========================================================================
    simple_zscore_anomaly,
    robust_zscore_anomaly,
    latest_simple_zscore_anomaly,
    mean_simple_zscore_anomaly,
    latest_robust_zscore_anomaly,
    mean_robust_zscore_anomaly,
    slope_anomaly,

    -- =========================================================================
    -- 9 Signal Type Classification (10 fields)
    -- =========================================================================
    simplified_signal_type,
    simplified_severity,
    simplified_severity_range,
    simplified_inputs,
    simplified_indicators,
    simplified_reasoning,
    simplified_severity_calculation,
    -- 3D Matrix dimensional tiers (kept for technical display)
    magnitude_tier,
    trajectory_tier,
    consistency_tier,
    coefficient_of_variation,

    -- Foreign keys for dimension joins
    {{ dbt_utils.generate_surrogate_key(['facility_id']) }} as facility_sk,
    {{ dbt_utils.generate_surrogate_key(['metric_id']) }} as metric_sk,

    -- Metadata
    metadata,
    metadata_per_period,
    current_timestamp as detected_at,
    current_timestamp as dbt_updated_at

from signals_only
