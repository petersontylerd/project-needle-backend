-- stg_classifications.sql
-- Staging model for signal classification records from JSONL files.
-- Contains the 9 Signal Type classification with glass box explainability:
-- signal_type, severity, inputs, derived_indicators, reasoning, severity_calculation.
-- Also includes 3D matrix dimensional tiers for technical display.
--
-- Source: raw_classifications (JSONL lines loaded via Python ETL)
-- Grain: One row per node-entity-metric classification
--
-- Key Fields for Signal Enrichment:
--   - signal_type: One of 9 signal types (suspect_data, sustained_excellence, etc.)
--   - severity: 0-100 severity score within type's range
--   - severity_range: [min, max] tuple for this signal type
--   - inputs: 5 input values (aggregate_zscore, slope_percentile, acceleration, zscore_std, max_abs_deviation)
--   - derived_indicators: Categorical interpretations (magnitude_level, trajectory_direction, etc.)
--   - classification_reasoning: Human-readable explanation of type selection
--   - severity_calculation: Full breakdown (base_severity, refinements, final_severity)
--   - magnitude_tier, trajectory_tier, consistency_tier: 3D matrix dimensional tiers
--   - coefficient_of_variation: CV of z-scores for consistency calculation

{{
    config(
        materialized='table',
        tags=['staging', 'classification'],
        indexes=[
            {'columns': ['canonical_node_id', 'run_id', 'metric_id', 'facility_id']},
            {'columns': ['entity_key_hash']},
            {'columns': ['signal_type']},
            {'columns': ['severity']}
        ]
    )
}}

with source as (
    select * from {{ source('raw', 'raw_classifications') }}
),

parsed as (
    select
        -- Run context
        run_id,

        -- Parse JSON data
        (json_data::jsonb)->>'node_id' as canonical_node_id,
        (json_data::jsonb)->'entity_key' as entity_key,
        (json_data::jsonb)->>'metric_id' as metric_id,

        -- 9 Signal Type classification fields
        (json_data::jsonb)->>'signal_type' as signal_type,
        ((json_data::jsonb)->>'severity')::integer as severity,
        (json_data::jsonb)->'severity_range' as severity_range,

        -- Glass box explainability (stored as JSONB for flexibility)
        (json_data::jsonb)->'inputs' as inputs,
        (json_data::jsonb)->'derived_indicators' as derived_indicators,
        (json_data::jsonb)->>'classification_reasoning' as classification_reasoning,
        (json_data::jsonb)->'severity_calculation' as severity_calculation,

        -- Suppression flag
        ((json_data::jsonb)->>'suppressed')::boolean as suppressed,
        ((json_data::jsonb)->>'period_count')::integer as period_count,

        -- 3D Matrix dimensional tiers (kept for technical display)
        (json_data::jsonb)->>'magnitude_tier' as magnitude_tier,
        (json_data::jsonb)->>'trajectory_tier' as trajectory_tier,
        (json_data::jsonb)->>'consistency_tier' as consistency_tier,
        ((json_data::jsonb)->>'coefficient_of_variation')::numeric(10,4) as coefficient_of_variation,

        -- Extract base entity keys from entity_key for joining
        (json_data::jsonb)->'entity_key'->>'systemName' as system_name,
        (json_data::jsonb)->'entity_key'->>'medicareId' as facility_id,

        -- Compute entity key hash for join (same logic as other staging models)
        -- Excludes base entity keys (medicareId, systemName) since facility_id is used for joining
        md5(coalesce((
            select jsonb_object_agg(key, value order by key)::text
            from jsonb_each_text((json_data::jsonb)->'entity_key')
            where key not in ('medicareId', 'systemName')
        ), '{}')) as entity_key_hash,

        -- Source metadata
        file_path,
        loaded_at

    from source
    where (json_data::jsonb)->>'node_id' is not null
)

select
    -- Surrogate key using full entity_key JSONB to avoid collisions
    {{ dbt_utils.generate_surrogate_key([
        'run_id',
        'canonical_node_id',
        'entity_key',
        'metric_id'
    ]) }} as classification_id,

    -- All parsed fields
    run_id,
    canonical_node_id,
    entity_key,
    metric_id,

    -- 9 Signal Type classification
    signal_type,
    severity,
    severity_range,
    inputs,
    derived_indicators,
    classification_reasoning,
    severity_calculation,

    -- Data quality
    suppressed,
    period_count,

    -- 3D Matrix dimensional tiers (kept for technical display)
    magnitude_tier,
    trajectory_tier,
    consistency_tier,
    coefficient_of_variation,

    system_name,
    facility_id,
    entity_key_hash,

    -- Source metadata
    file_path,
    loaded_at,
    current_timestamp as dbt_updated_at

from parsed
-- Filter out suppressed signals at staging level
where not coalesce(suppressed, false)
