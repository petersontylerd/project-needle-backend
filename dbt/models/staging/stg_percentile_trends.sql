-- stg_percentile_trends.sql
-- Staging model for peer distribution percentile trends at the node level.
-- Extracts percentile_trends from temporal node statistical_methods.
--
-- Source: stg_nodes (raw node data)
-- Grain: One row per temporal node per run
--
-- Note: percentile_trends is node-level data (same for all entities in a node).
-- It lives at: statistical_methods[].summary.global_statistics.percentile_trends

{{
    config(
        materialized='table',
        tags=['staging', 'nodes', 'percentile_trends'],
        indexes=[
            {'columns': ['canonical_node_id', 'run_id']}
        ]
    )
}}

with nodes as (
    select * from {{ ref('stg_nodes') }}
),

-- Extract NODE-level statistical_methods containing percentile_trends
-- Only temporal nodes have percentile_trends (trending methods)
node_stat_methods as (
    select
        n.canonical_node_id,
        n.run_id,
        n.loaded_at,
        stat_method,
        stat_method->>'statistical_method' as statistical_method_full
    from nodes n,
    lateral jsonb_array_elements(
        coalesce(n.raw_json->'statistical_methods', '[]'::jsonb)
    ) as stat_method
    -- Filter to trending methods which have percentile_trends
    where stat_method->>'statistical_method' like '%trending%'
),

-- Extract percentile_trends from global_statistics
percentile_trends_extracted as (
    select
        nsm.canonical_node_id,
        nsm.run_id,
        nsm.loaded_at,
        nsm.statistical_method_full,
        nsm.stat_method->'summary'->'global_statistics'->'percentile_trends' as percentile_trends
    from node_stat_methods nsm
    where nsm.stat_method->'summary'->'global_statistics'->'percentile_trends' is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['canonical_node_id', 'run_id', 'statistical_method_full']) }} as percentile_trends_id,
    canonical_node_id,
    run_id,
    loaded_at,
    statistical_method_full,
    percentile_trends
from percentile_trends_extracted
