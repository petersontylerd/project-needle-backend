-- fct_contributions.sql
-- Fact table for contribution analysis ready for API consumption.
-- Shows how child entities contribute to parent aggregate values.
--
-- Grain: One row per parent-child contribution relationship
--
-- Key Use Cases:
-- 1. Signal detail panel: "Which sub-service lines drive this anomaly?"
-- 2. Impact sorting: Rank signals by contribution_weight
-- 3. Drill-down navigation: Navigate from parent to child signals

{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'contribution']
    )
}}

with contributions as (
    select
        contribution_id,
        run_id,
        record_timestamp,

        -- Parent-child relationship
        parent_node_id,
        child_node_id,

        -- Entity context (denormalized for easy filtering)
        parent_facility_id,
        parent_service_line,
        child_facility_id,
        child_service_line,
        child_sub_service_line,

        -- Additional child dimensions (for proper label display)
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
        excess_over_parent,

        -- Pre-computed impact metrics
        contribution_weight,
        contribution_direction,

        -- Source metadata
        loaded_at

    from {{ ref('stg_contributions') }}
    where contribution_weight is not null
),

-- Add ranking within parent node
ranked as (
    select
        *,
        -- Rank by impact within parent (for "top contributors" queries)
        row_number() over (
            partition by run_id, parent_node_id, parent_facility_id, parent_service_line
            order by contribution_weight desc
        ) as contribution_rank,

        -- Total weight share check (should sum to ~1.0)
        sum(weight_share) over (
            partition by run_id, parent_node_id, parent_facility_id, parent_service_line
        ) as total_weight_share

    from contributions
)

select
    -- Primary key
    contribution_id,

    -- Run context
    run_id,
    record_timestamp,

    -- Parent-child relationship
    parent_node_id,
    child_node_id,

    -- Entity context
    parent_facility_id,
    parent_service_line,
    child_facility_id,
    child_service_line,
    child_sub_service_line,

    -- Additional child dimensions (for proper label display)
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
    excess_over_parent,

    -- Impact metrics for prioritization
    contribution_weight,
    contribution_direction,
    contribution_rank,

    -- Data quality indicator
    total_weight_share,
    case
        when abs(total_weight_share - 1.0) <= 0.01 then true
        else false
    end as weight_share_valid,

    -- Relative contribution percentage
    case
        when total_weight_share > 0
        then round((weight_share / total_weight_share * 100)::numeric, 2)
        else null
    end as contribution_pct,

    -- Foreign keys for dimension joins
    {{ dbt_utils.generate_surrogate_key(['parent_facility_id']) }} as parent_facility_sk,
    {{ dbt_utils.generate_surrogate_key(['child_facility_id']) }} as child_facility_sk,
    {{ dbt_utils.generate_surrogate_key(['metric_id']) }} as metric_sk,

    -- Metadata
    loaded_at,
    current_timestamp as dbt_updated_at

from ranked
