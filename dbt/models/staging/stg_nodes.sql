-- stg_nodes.sql
-- Staging model for node result metadata.
-- Extracts core node information from the raw node results source table.
--
-- Source: raw_node_results (JSON files loaded via Python ETL)
-- Grain: One row per canonical_node_id per run
--
-- Dependencies: None (base staging model)

{{
    config(
        materialized='table',
        tags=['staging', 'nodes']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_node_results') }}
),

parsed as (
    select
        -- Primary key
        (json_data::jsonb)->>'canonical_node_id' as canonical_node_id,

        -- Run context
        run_id,

        -- Metadata
        (json_data::jsonb)->>'dataset_path' as dataset_path,

        -- Edge counts (for quick filtering)
        jsonb_array_length(
            coalesce((json_data::jsonb)->'canonical_child_node_ids', '[]'::jsonb)
        ) as child_count,
        jsonb_array_length(
            coalesce((json_data::jsonb)->'canonical_parent_node_ids', '[]'::jsonb)
        ) as parent_count,

        -- Entity results metadata (actual entities stored in raw_entity_results)
        (json_data::jsonb)->>'entity_results_path' as entity_results_path,
        ((json_data::jsonb)->>'entity_results_count')::integer as entity_results_count,

        -- Raw JSON for downstream processing (entity_results is now empty array)
        json_data::jsonb as raw_json,

        -- File metadata
        file_path,
        loaded_at

    from source
    where (json_data::jsonb)->>'canonical_node_id' is not null
)

select
    canonical_node_id,
    run_id,
    dataset_path,
    child_count,
    parent_count,
    entity_results_path,
    entity_results_count,
    raw_json,
    file_path,
    loaded_at,
    current_timestamp as dbt_updated_at
from parsed
