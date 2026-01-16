-- stg_statistical_methods.sql
-- Staging model for statistical method results per entity.
-- Extracts z-scores, percentile ranks, and peer statistics.
--
-- Source: stg_entity_results (parsed entity data)
-- Grain: One row per statistical_method per entity per run
--
-- Statistical Methods:
--   - statistical_method__simple_zscore__aggregate_time_period
--   - statistical_method__robust_zscore__aggregate_time_period
--   - statistical_method__simple_zscore__temporal (for temporal nodes)

{{
    config(
        materialized='table',
        tags=['staging', 'nodes', 'statistics']
    )
}}

with entity_results as (
    select * from {{ ref('stg_entity_results') }}
),

-- Unnest statistical_methods array
methods_unnested as (
    select
        er.entity_result_id,
        er.canonical_node_id,
        er.run_id,
        er.facility_id,
        er.service_line,
        stat_method
    from entity_results er,
    lateral jsonb_array_elements(
        coalesce(er.statistical_methods_json, '[]'::jsonb)
    ) as stat_method
),

-- Parse statistical method details
parsed_methods as (
    select
        entity_result_id,
        canonical_node_id,
        run_id,
        facility_id,
        service_line,

        -- Method identification
        stat_method->>'statistical_method' as statistical_method,

        -- Short method name for easier filtering/grouping
        -- e.g., "statistical_method__simple_zscore__aggregate_time_period" -> "simple_zscore"
        case
            when stat_method->>'statistical_method' like '%simple_zscore%' then 'simple_zscore'
            when stat_method->>'statistical_method' like '%robust_zscore%' then 'robust_zscore'
            when stat_method->>'statistical_method' like '%slope_percentile%' then 'slope_percentile'
            else split_part(stat_method->>'statistical_method', '__', 2)
        end as statistical_method_short_name,

        -- Runtime status
        stat_method->'runtime'->>'status' as runtime_status,
        (stat_method->'runtime'->'summary'->>'has_issues')::boolean as has_issues,

        -- Aggregate statistics (simple_zscore method)
        (stat_method->'statistics'->>'simple_zscore')::numeric as simple_zscore,
        (stat_method->'statistics'->>'robust_zscore')::numeric as robust_zscore,
        (stat_method->'statistics'->>'percentile_rank')::numeric as percentile_rank,
        (stat_method->'statistics'->>'peer_mean')::numeric as peer_mean,
        (stat_method->'statistics'->>'peer_std')::numeric as peer_std,
        (stat_method->'statistics'->>'suppressed')::boolean as suppressed,

        -- Peer count from runtime
        (stat_method->'runtime'->'anomaly_statistics'->>'peer_count')::integer as peer_count,

        -- Temporal statistics (for temporal nodes)
        (stat_method->'statistics'->>'latest_simple_zscore')::numeric as latest_simple_zscore,
        (stat_method->'statistics'->>'mean_simple_zscore')::numeric as mean_simple_zscore,
        (stat_method->'statistics'->>'latest_robust_zscore')::numeric as latest_robust_zscore,
        (stat_method->'statistics'->>'mean_robust_zscore')::numeric as mean_robust_zscore,
        stat_method->'statistics'->'timeline' as timeline_json,

        -- Slope statistics (from slope_percentile method)
        (stat_method->'statistics'->>'slope')::numeric as slope,
        (stat_method->'statistics'->>'slope_percentile')::numeric as slope_percentile,
        (stat_method->'statistics'->>'acceleration')::numeric as acceleration,
        stat_method->'statistics'->>'trend_direction' as trend_direction,
        stat_method->'statistics'->>'momentum' as momentum,

        -- Monthly z-scores (from per_period arrays in trending methods)
        -- trending_simple_zscore has per_period_simple_zscores
        -- trending_robust_zscore has per_period_robust_zscores
        case
            when stat_method->'statistics'->'per_period_simple_zscores' is not null
                 and jsonb_typeof(stat_method->'statistics'->'per_period_simple_zscores') = 'array'
            then stat_method->'statistics'->'per_period_simple_zscores'
            when stat_method->'statistics'->'per_period_robust_zscores' is not null
                 and jsonb_typeof(stat_method->'statistics'->'per_period_robust_zscores') = 'array'
            then stat_method->'statistics'->'per_period_robust_zscores'
            else null
        end as monthly_z_scores,

        -- Anomalies array (for downstream processing)
        stat_method->'anomalies' as anomalies_json,

        -- Percentile trends (peer distribution at each time period, from temporal methods)
        stat_method->'summary'->'global_statistics'->'percentile_trends' as percentile_trends

    from methods_unnested
)

select
    {{ dbt_utils.generate_surrogate_key(['entity_result_id', 'statistical_method']) }} as statistical_method_id,
    entity_result_id,
    canonical_node_id,
    run_id,
    facility_id,
    service_line,
    statistical_method,
    statistical_method_short_name,
    runtime_status,
    has_issues,
    simple_zscore,
    robust_zscore,
    percentile_rank,
    peer_mean,
    peer_std,
    peer_count,
    suppressed,
    latest_simple_zscore,
    mean_simple_zscore,
    latest_robust_zscore,
    mean_robust_zscore,
    timeline_json,
    -- Slope statistics
    slope,
    slope_percentile,
    acceleration,
    trend_direction,
    momentum,
    -- Monthly z-scores for classification
    monthly_z_scores,
    anomalies_json,
    -- Percentile trends for temporal methods
    percentile_trends,
    current_timestamp as dbt_updated_at
from parsed_methods
where statistical_method is not null
