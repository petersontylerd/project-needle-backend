-- stg_global_statistics.sql
-- Staging model for global benchmark statistics per facet per statistical method.
-- Extracts global_metric_mean from NODE-level statistical_methods[].summary.global_statistics.facets[].
--
-- Source: stg_nodes (raw node data)
-- Grain: One row per facet per statistical_method per node per run
--
-- Key Fields:
--   - global_metric_mean: Global benchmark for variance calculation
--   - global_metric_std: Global standard deviation
--   - facet_values: Entity dimension values (excluding medicareId) for matching entities
--
-- Note: Global statistics are at the NODE level (statistical_methods[]), NOT within entity_results.
-- Each statistical method has its own set of faceted global statistics.

{{
    config(
        materialized='table',
        tags=['staging', 'nodes', 'global_statistics'],
        indexes=[
            {'columns': ['canonical_node_id', 'run_id', 'statistical_method_full']}
        ]
    )
}}

with nodes as (
    select * from {{ ref('stg_nodes') }}
),

-- Extract NODE-level statistical_methods (NOT from entity_results)
-- Global statistics live at: raw_json->'statistical_methods'[]->'summary'->'global_statistics'->'facets'[]
node_stat_methods as (
    select
        n.canonical_node_id,
        n.run_id,
        n.loaded_at,
        stat_method,
        stat_method->>'statistical_method' as statistical_method_full,
        -- Short method name for easier filtering/grouping
        case
            when stat_method->>'statistical_method' like '%simple_zscore%' then 'simple_zscore'
            when stat_method->>'statistical_method' like '%robust_zscore%' then 'robust_zscore'
            when stat_method->>'statistical_method' like '%slope_percentile%' then 'slope_percentile'
            else split_part(stat_method->>'statistical_method', '__', 2)
        end as statistical_method_short_name
    from nodes n,
    lateral jsonb_array_elements(
        coalesce(n.raw_json->'statistical_methods', '[]'::jsonb)
    ) as stat_method
),

-- Unnest facets from global_statistics
facets_unnested as (
    select
        nsm.canonical_node_id,
        nsm.run_id,
        nsm.loaded_at,
        nsm.statistical_method_full,
        nsm.statistical_method_short_name,
        facet
    from node_stat_methods nsm,
    lateral jsonb_array_elements(
        coalesce(nsm.stat_method->'summary'->'global_statistics'->'facets', '[]'::jsonb)
    ) as facet
),

-- Parse facet details including method_statistics (benchmarks)
parsed_facets as (
    select
        canonical_node_id,
        run_id,
        loaded_at,
        statistical_method_full,
        statistical_method_short_name,

        -- Facet dimension values (JSONB for flexible matching)
        -- For root aggregates this is {} (empty), for multi-dimension nodes it contains the facet values
        -- e.g., {"vizientServiceLine": "Cardiology"} or {"admissionSource": "Emergency Room"}
        facet->'facet_values' as facet_values,

        -- Global benchmark statistics from method_statistics
        (facet->'method_statistics'->>'global_metric_mean')::numeric as global_metric_mean,
        (facet->'method_statistics'->>'global_metric_std')::numeric as global_metric_std,
        (facet->'method_statistics'->>'global_metric_median')::numeric as global_metric_median,
        (facet->'method_statistics'->>'global_mad')::numeric as global_mad,
        (facet->'method_statistics'->>'min_peer_count')::integer as min_peer_count,
        (facet->'method_statistics'->>'std_floor')::numeric as std_floor,
        (facet->'method_statistics'->>'std_floor_applied')::boolean as std_floor_applied,

        -- Facet counts for context
        (facet->'facet_counts'->>'record_count')::integer as facet_record_count,
        (facet->'facet_counts'->>'entity_count')::integer as facet_entity_count,
        (facet->'facet_counts'->>'entity_with_statistics_count')::integer as facet_entity_with_stats_count

    from facets_unnested
)

select
    -- Surrogate key: unique per facet per method per node per run
    {{ dbt_utils.generate_surrogate_key([
        'canonical_node_id',
        'statistical_method_full',
        'facet_values',
        'run_id'
    ]) }} as global_stat_id,
    canonical_node_id,
    run_id,
    statistical_method_full,
    statistical_method_short_name,
    facet_values,
    global_metric_mean,
    global_metric_std,
    global_metric_median,
    global_mad,
    min_peer_count,
    std_floor,
    std_floor_applied,
    facet_record_count,
    facet_entity_count,
    facet_entity_with_stats_count,
    loaded_at,
    current_timestamp as dbt_updated_at
from parsed_facets
where global_metric_mean is not null
