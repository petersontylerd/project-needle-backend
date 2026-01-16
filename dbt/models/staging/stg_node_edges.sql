-- stg_node_edges.sql
-- Staging model for node edge relationships.
-- Extracts parent and child edges from node result JSON.
--
-- Source: stg_nodes (parsed node data)
-- Grain: One row per edge (node → related_node)
--
-- Edge Types:
--   - drills_to: Hierarchical (facility → service_line → sub_service_line)
--   - trends_to: Temporal (aggregate → dischargeMonth)
--   - relates_to: Correlation between metrics
--   - derives_to: Derived metric relationships

{{
    config(
        materialized='table',
        tags=['staging', 'nodes', 'edges']
    )
}}

with nodes as (
    select * from {{ ref('stg_nodes') }}
),

-- Extract child edges from canonical_child_node_ids array
child_edges as (
    select
        n.canonical_node_id,
        n.run_id,
        child_edge->>'canonical_child_node_id' as related_node_id,
        child_edge->>'edge_type' as edge_type,
        'child' as edge_direction
    from nodes n,
    lateral jsonb_array_elements(
        coalesce(n.raw_json->'canonical_child_node_ids', '[]'::jsonb)
    ) as child_edge
),

-- Extract parent edges from canonical_parent_node_ids array
parent_edges as (
    select
        n.canonical_node_id,
        n.run_id,
        parent_edge->>'canonical_parent_node_id' as related_node_id,
        parent_edge->>'edge_type' as edge_type,
        'parent' as edge_direction
    from nodes n,
    lateral jsonb_array_elements(
        coalesce(n.raw_json->'canonical_parent_node_ids', '[]'::jsonb)
    ) as parent_edge
),

-- Combine all edges
all_edges as (
    select * from child_edges
    union all
    select * from parent_edges
)

select
    {{ dbt_utils.generate_surrogate_key(['canonical_node_id', 'related_node_id', 'edge_direction', 'run_id']) }} as edge_id,
    canonical_node_id,
    related_node_id,
    edge_type,
    edge_direction,
    run_id,
    current_timestamp as dbt_updated_at
from all_edges
where related_node_id is not null
