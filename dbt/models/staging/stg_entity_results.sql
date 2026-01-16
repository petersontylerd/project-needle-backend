-- stg_entity_results.sql
-- Staging model for entity-level results within nodes.
-- Extracts facility/service_line combinations with their metrics.
--
-- Source: raw_entity_results (JSONL files loaded via Python ETL)
-- Grain: One row per entity per node per run
--
-- Entity Examples:
--   - Facility only: {"medicareId": "AFP658"}
--   - Facility + Service Line: {"medicareId": "AFP658", "vizientServiceLine": "Cardiology"}
--   - Full hierarchy: {..., "vizientSubServiceLine": "Cardiac ICU"}
--
-- Performance: Reads from pre-split JSONL files loaded into raw_entity_results,
-- avoiding massive JSON parsing that previously caused PostgreSQL OOM.

{{
    config(
        materialized='table',
        tags=['staging', 'nodes', 'entities'],
        indexes=[
            {'columns': ['entity_result_id']},
            {'columns': ['canonical_node_id', 'run_id']},
            {'columns': ['entity_dimensions_hash']}
        ]
    )
}}

with source as (
    select * from {{ source('raw', 'raw_entity_results') }}
),

-- Parse JSON once per row (each row is a small entity JSON, ~1-10KB)
parsed as (
    select
        canonical_node_id,
        run_id,
        loaded_at,
        json_data::jsonb as entity_result
    from source
    where json_data is not null
),

-- Single-pass entity field extraction using LATERAL + conditional aggregation
-- Replaces 4 correlated subqueries with one array scan
entity_fields_extracted as (
    select
        p.canonical_node_id,
        p.run_id,
        p.loaded_at,
        p.entity_result,
        -- Scalar extractions via conditional aggregation
        -- system_name is stored in metadata.systemName (as of metadata refactor)
        coalesce(
            p.entity_result->'metadata'->>'systemName',
            max(case when ef->>'id' = 'systemName' then ef->>'value' end)
        ) as system_name,
        max(case when ef->>'id' = 'medicareId' then ef->>'value' end) as facility_id,
        max(case when ef->>'id' = 'vizientServiceLine' then ef->>'value' end) as service_line,
        max(case when ef->>'id' = 'vizientSubServiceLine' then ef->>'value' end) as sub_service_line,
        -- Dimension aggregation with filter (excluding base entity keys)
        coalesce(
            jsonb_object_agg(ef->>'id', ef->>'value')
            filter (where ef->>'id' not in ('medicareId', 'systemName')),
            '{}'::jsonb
        ) as entity_dimensions
    from parsed p
    left join lateral jsonb_array_elements(p.entity_result->'entity') as ef on true
    group by p.canonical_node_id, p.run_id, p.loaded_at, p.entity_result
),

-- Extract remaining fields (metric value, timeline, etc.)
parsed_entities as (
    select
        efe.canonical_node_id,
        efe.run_id,
        efe.loaded_at,

        -- Entity identification (full JSONB for flexibility)
        efe.entity_result->'entity' as entity_fields,

        -- Pre-extracted entity keys for efficient filtering
        efe.system_name,
        efe.facility_id,
        efe.service_line,
        efe.sub_service_line,

        -- Entity dimensions (excluding base entity keys) for grouping
        efe.entity_dimensions,

        -- Encounters count (stored in metadata.encounters as of metadata refactor)
        -- Cast through numeric first to handle float values like "15166.0"
        (efe.entity_result->'metadata'->>'encounters')::numeric::integer as encounters,

        -- Per-period metadata (e.g., encounters per period for temporal nodes)
        efe.entity_result->'metadata'->'per_period' as metadata_per_period,

        -- Extract metric value - handles both aggregate (scalar) and temporal (timeline) nodes
        -- Aggregate nodes: metric[0].values = scalar numeric (e.g., 1.166847)
        -- Temporal nodes: metric[0].values.timeline[].value = array of period values
        case
            -- Aggregate nodes: values is a scalar number
            when jsonb_typeof(efe.entity_result->'metric'->0->'values') = 'number'
            then (efe.entity_result->'metric'->0->'values')::numeric
            -- Temporal nodes: values.timeline is an array - get most recent non-null value
            when jsonb_typeof(efe.entity_result->'metric'->0->'values'->'timeline') = 'array'
            then (
                select (t->>'value')::numeric
                from jsonb_array_elements(
                    efe.entity_result->'metric'->0->'values'->'timeline'
                ) as t
                where t->>'value' is not null
                  and t->>'value' != 'null'
                order by t->>'period' desc
                limit 1
            )
            else null
        end as metric_value,
        efe.entity_result->'metric'->0->'metadata'->>'metric_id' as metric_id,

        -- Full timeline for temporal analysis
        efe.entity_result->'metric'->0->'values'->'timeline' as metric_timeline,

        -- Statistical methods (array for downstream processing)
        efe.entity_result->'statistical_methods' as statistical_methods_json,

        -- Metadata JSONB passthrough (contains entity-level metadata from upstream)
        efe.entity_result->'metadata' as metadata,

        -- Raw entity result for complex queries
        efe.entity_result as raw_entity_result

    from entity_fields_extracted efe
)

select
    -- Use surrogate key with full entity identification for uniqueness
    -- Grain: One row per entity (all dimensions) per metric per node per run
    -- entity_fields is JSONB containing all entity dimensions (medicareId, vizientServiceLine, admissionSource, etc.)
    {{ dbt_utils.generate_surrogate_key(['canonical_node_id', 'entity_fields', 'metric_id', 'run_id']) }} as entity_result_id,
    canonical_node_id,
    run_id,
    entity_fields,
    system_name,
    facility_id,
    service_line,
    sub_service_line,
    entity_dimensions,
    -- Pre-computed hash for efficient joins (excludes base entity keys, matches stg_classifications logic)
    md5(coalesce(entity_dimensions::text, '{}')) as entity_dimensions_hash,
    encounters,
    metadata_per_period,
    metric_id,
    metric_value,
    metric_timeline,
    statistical_methods_json,
    metadata,
    raw_entity_result,
    loaded_at,
    current_timestamp as dbt_updated_at
from parsed_entities
-- Note: encounters filter removed - encounters may be null in new metadata structure
-- and downstream models should handle null encounters appropriately
