-- int_statistical_methods_agg.sql
-- Aggregates all statistical methods for each entity into a JSONB array.
-- Grain: One row per entity_result_id
--
-- Purpose: Consolidates the 1:N relationship between entities and statistical
-- methods into a single row with a JSONB array, enabling fct_signals to have
-- entity-level grain instead of method-level grain.
--
-- Output columns:
--   - entity_result_id (PK)
--   - statistical_methods (JSONB array of all methods)
--   - primary_* columns (scalar values from simple_zscore method for display)
--   - *_anomaly columns (pivoted anomaly labels)
--   - slope stats (from slope_percentile method)
--   - any_suppressed (boolean)

{{
    config(
        materialized='table',
        tags=['intermediate', 'statistics'],
        indexes=[
            {'columns': ['entity_result_id']}
        ]
    )
}}

with statistical_methods as (
    select * from {{ ref('stg_statistical_methods') }}
),

anomaly_labels as (
    select * from {{ ref('int_anomaly_labels') }}
),

-- Join anomaly labels to get per-method anomaly classifications
methods_with_anomalies as (
    select
        sm.*,
        al.simple_zscore_anomaly,
        al.robust_zscore_anomaly,
        al.latest_simple_zscore_anomaly,
        al.mean_simple_zscore_anomaly,
        al.latest_robust_zscore_anomaly,
        al.mean_robust_zscore_anomaly,
        al.slope_anomaly
    from statistical_methods sm
    left join anomaly_labels al
        on sm.statistical_method_id = al.statistical_method_id
),

-- Aggregate methods into JSONB array per entity
aggregated as (
    select
        entity_result_id,

        -- Aggregate all methods into a JSONB array
        jsonb_agg(
            jsonb_build_object(
                'method_id', statistical_method_id,
                'method_name', statistical_method_short_name,
                'method_full', statistical_method,
                'simple_zscore', simple_zscore,
                'robust_zscore', robust_zscore,
                'percentile_rank', percentile_rank,
                'peer_mean', peer_mean,
                'peer_std', peer_std,
                'peer_count', peer_count,
                'suppressed', suppressed,
                'latest_simple_zscore', latest_simple_zscore,
                'mean_simple_zscore', mean_simple_zscore,
                'latest_robust_zscore', latest_robust_zscore,
                'mean_robust_zscore', mean_robust_zscore,
                'slope', slope,
                'slope_percentile', slope_percentile,
                'acceleration', acceleration,
                'trend_direction', trend_direction,
                'momentum', momentum,
                'monthly_z_scores', monthly_z_scores,
                'simple_zscore_anomaly', simple_zscore_anomaly,
                'robust_zscore_anomaly', robust_zscore_anomaly,
                'latest_simple_zscore_anomaly', latest_simple_zscore_anomaly,
                'mean_simple_zscore_anomaly', mean_simple_zscore_anomaly,
                'latest_robust_zscore_anomaly', latest_robust_zscore_anomaly,
                'mean_robust_zscore_anomaly', mean_robust_zscore_anomaly,
                'slope_anomaly', slope_anomaly
            )
            order by statistical_method_short_name
        ) as statistical_methods,

        -- Primary values from simple_zscore method for backward compatibility
        max(simple_zscore) filter (where statistical_method_short_name = 'simple_zscore') as primary_simple_zscore,
        max(robust_zscore) filter (where statistical_method_short_name = 'robust_zscore') as primary_robust_zscore,
        max(percentile_rank) filter (where statistical_method_short_name = 'simple_zscore') as primary_percentile_rank,
        max(peer_mean) filter (where statistical_method_short_name = 'simple_zscore') as primary_peer_mean,
        max(peer_std) filter (where statistical_method_short_name = 'simple_zscore') as primary_peer_std,
        max(peer_count) filter (where statistical_method_short_name = 'simple_zscore') as primary_peer_count,

        -- Temporal z-scores (from any method that has them)
        max(latest_simple_zscore) as latest_simple_zscore,
        max(mean_simple_zscore) as mean_simple_zscore,
        max(latest_robust_zscore) as latest_robust_zscore,
        max(mean_robust_zscore) as mean_robust_zscore,

        -- Slope stats from slope_percentile method
        max(slope) filter (where statistical_method_short_name = 'slope_percentile') as slope,
        max(slope_percentile) filter (where statistical_method_short_name = 'slope_percentile') as slope_percentile,
        max(acceleration) filter (where statistical_method_short_name = 'slope_percentile') as acceleration,
        max(trend_direction) filter (where statistical_method_short_name = 'slope_percentile') as trend_direction,
        max(momentum) filter (where statistical_method_short_name = 'slope_percentile') as momentum,
        -- Use array_agg and take first non-null for JSONB (can't use max on JSONB)
        (array_agg(monthly_z_scores) filter (where monthly_z_scores is not null))[1] as monthly_z_scores,

        -- Anomaly labels (pivoted, take max across methods)
        max(simple_zscore_anomaly) as simple_zscore_anomaly,
        max(robust_zscore_anomaly) as robust_zscore_anomaly,
        max(latest_simple_zscore_anomaly) as latest_simple_zscore_anomaly,
        max(mean_simple_zscore_anomaly) as mean_simple_zscore_anomaly,
        max(latest_robust_zscore_anomaly) as latest_robust_zscore_anomaly,
        max(mean_robust_zscore_anomaly) as mean_robust_zscore_anomaly,
        max(slope_anomaly) as slope_anomaly,

        -- Suppression: any method suppressed = entity suppressed
        bool_or(suppressed) as any_suppressed,

        current_timestamp as dbt_updated_at

    from methods_with_anomalies
    group by entity_result_id
)

select * from aggregated
