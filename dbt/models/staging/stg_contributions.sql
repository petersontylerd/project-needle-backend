-- stg_contributions.sql
-- Staging model for contribution records from JSONL files.
-- Shows how child entities contribute to parent aggregate values.
--
-- Source: raw_contributions (JSONL lines loaded via Python ETL)
-- Grain: One row per parent-child entity pair per metric per run
--
-- Key Fields for Signal Prioritization:
--   - contribution_weight: |excess_over_parent| × weight_share
--   - contribution_direction: positive, negative, or neutral
--   - weight_share: Proportion of total weight (0.0 to 1.0)

{{
    config(
        materialized='view',
        tags=['staging', 'contributions']
    )
}}

with source as (
    select * from {{ source('raw', 'raw_contributions') }}
),

parsed as (
    select
        -- Run context (cast json_data to jsonb first)
        (json_data::jsonb)->>'run_id' as run_id,
        ((json_data::jsonb)->>'timestamp')::timestamp as record_timestamp,

        -- Parent-child relationship
        (json_data::jsonb)->>'parent_node_id' as parent_node_id,
        (json_data::jsonb)->>'parent_node_file' as parent_node_file,
        (json_data::jsonb)->>'child_node_id' as child_node_id,
        (json_data::jsonb)->>'child_node_file' as child_node_file,

        -- Entity context (JSONB for flexibility)
        (json_data::jsonb)->'parent_entity' as parent_entity,
        (json_data::jsonb)->'child_entity' as child_entity,

        -- Denormalized parent entity keys
        (json_data::jsonb)->'parent_entity'->>'medicareId' as parent_facility_id,
        (json_data::jsonb)->'parent_entity'->>'vizientServiceLine' as parent_service_line,

        -- Denormalized child entity keys (service lines)
        (json_data::jsonb)->'child_entity'->>'medicareId' as child_facility_id,
        (json_data::jsonb)->'child_entity'->>'vizientServiceLine' as child_service_line,
        (json_data::jsonb)->'child_entity'->>'vizientSubServiceLine' as child_sub_service_line,

        -- Denormalized child entity keys (other dimensions)
        -- These enable proper label display for non-service-line breakdowns
        (json_data::jsonb)->'child_entity'->>'admissionStatus' as child_admission_status,
        (json_data::jsonb)->'child_entity'->>'dischargeStatus' as child_discharge_status,
        (json_data::jsonb)->'child_entity'->>'payerSegment' as child_payer_segment,
        (json_data::jsonb)->'child_entity'->>'admissionSource' as child_admission_source,

        -- Metric context
        (json_data::jsonb)->>'metric_id' as metric_id,
        (json_data::jsonb)->>'method' as contribution_method,

        -- Contribution values
        ((json_data::jsonb)->>'child_value')::numeric as child_value,
        ((json_data::jsonb)->>'parent_value')::numeric as parent_value,
        (json_data::jsonb)->>'weight_field' as weight_field,
        ((json_data::jsonb)->>'weight_value')::numeric as weight_value,
        ((json_data::jsonb)->>'weight_share')::numeric as weight_share,
        ((json_data::jsonb)->>'weighted_child_value')::numeric as weighted_child_value,
        ((json_data::jsonb)->>'contribution_value')::numeric as contribution_value,
        ((json_data::jsonb)->>'raw_component')::numeric as raw_component,
        ((json_data::jsonb)->>'excess_over_parent')::numeric as excess_over_parent,

        -- Parent statistics (nullable JSONB)
        (json_data::jsonb)->'parent_statistics' as parent_statistics,

        -- Audit fields (JSONB arrays)
        (json_data::jsonb)->'assumptions' as assumptions,
        (json_data::jsonb)->'fallbacks' as fallbacks,
        (json_data::jsonb)->'missing_inputs' as missing_inputs,
        (json_data::jsonb)->'dataset_row_locator' as dataset_row_locator,

        -- Source file metadata
        file_path,
        loaded_at

    from source
    where (json_data::jsonb)->>'parent_node_id' is not null
      and (json_data::jsonb)->>'child_node_id' is not null
)

select
    -- Surrogate key
    -- Uses full entity JSONB to capture all entity dimensions (medicareId, dischargeStatus, etc.)
    {{ dbt_utils.generate_surrogate_key([
        'run_id',
        'parent_node_id',
        'child_node_id',
        'parent_entity',
        'child_entity',
        'metric_id'
    ]) }} as contribution_id,

    -- Run context
    run_id,
    record_timestamp,

    -- Parent-child relationship
    parent_node_id,
    parent_node_file,
    child_node_id,
    child_node_file,

    -- Entity context
    parent_entity,
    child_entity,

    -- Denormalized entity keys for filtering
    parent_facility_id,
    parent_service_line,
    child_facility_id,
    child_service_line,
    child_sub_service_line,

    -- Denormalized child entity keys (other dimensions)
    child_admission_status,
    child_discharge_status,
    child_payer_segment,
    child_admission_source,

    -- Metric context
    metric_id,
    contribution_method,

    -- Contribution values
    child_value,
    parent_value,
    weight_field,
    weight_value,
    weight_share,
    weighted_child_value,
    contribution_value,
    raw_component,
    excess_over_parent,

    -- Calculated fields for signal prioritization
    -- contribution_weight = |excess_over_parent| × weight_share
    abs(coalesce(excess_over_parent, 0)) * coalesce(weight_share, 0) as contribution_weight,

    -- contribution_direction: positive pulls parent UP, negative pulls DOWN
    case
        when excess_over_parent > 0 then 'positive'
        when excess_over_parent < 0 then 'negative'
        else 'neutral'
    end as contribution_direction,

    -- Parent statistics
    parent_statistics,

    -- Audit fields
    assumptions,
    fallbacks,
    missing_inputs,
    dataset_row_locator,

    -- Source metadata
    file_path,
    loaded_at,
    current_timestamp as dbt_updated_at

from parsed
