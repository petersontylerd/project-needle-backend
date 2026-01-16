-- stg_anomalies.sql
-- Staging model for anomaly classification results.
-- Extracts severity classifications and interpretations.
--
-- Source: stg_statistical_methods (parsed statistical data)
-- Grain: One row per anomaly method per statistical method per entity
--
-- Anomaly Classifications (9-tier + no_score):
--   extremely_low, very_low, moderately_low, slightly_low,
--   normal,
--   slightly_high, moderately_high, very_high, extremely_high
--   plus: no_score (when suppressed, null statistic, or insufficient peers)
-- Source of truth: taxonomy/anomaly_method_profiles.yaml

{{
    config(
        materialized='table',
        tags=['staging', 'nodes', 'anomalies']
    )
}}

with statistical_methods as (
    select * from {{ ref('stg_statistical_methods') }}
),

-- Unnest anomalies array
anomalies_unnested as (
    select
        sm.statistical_method_id,
        sm.entity_result_id,
        sm.canonical_node_id,
        sm.run_id,
        sm.facility_id,
        sm.service_line,
        anomaly
    from statistical_methods sm,
    lateral jsonb_array_elements(
        coalesce(sm.anomalies_json, '[]'::jsonb)
    ) as anomaly
),

-- Unnest methods within each anomaly
methods_unnested as (
    select
        au.statistical_method_id,
        au.entity_result_id,
        au.canonical_node_id,
        au.run_id,
        au.facility_id,
        au.service_line,
        au.anomaly->>'anomaly_profile' as anomaly_profile,
        method
    from anomalies_unnested au,
    lateral jsonb_array_elements(
        coalesce(au.anomaly->'methods', '[]'::jsonb)
    ) as method
),

-- Parse anomaly method details
parsed_anomalies as (
    select
        statistical_method_id,
        entity_result_id,
        canonical_node_id,
        run_id,
        facility_id,
        service_line,
        anomaly_profile,

        -- Anomaly classification
        method->>'anomaly' as anomaly_classification,
        method->>'anomaly_method' as anomaly_method,
        method->>'applies_to' as applies_to,
        (method->>'statistic_value')::numeric as statistic_value,

        -- Interpretation
        method->'interpretation'->>'rendered' as interpretation_rendered,
        method->'interpretation'->>'template_id' as interpretation_template_id

    from methods_unnested
)

select
    {{ dbt_utils.generate_surrogate_key(['statistical_method_id', 'anomaly_profile', 'anomaly_method', 'applies_to']) }} as anomaly_id,
    statistical_method_id,
    entity_result_id,
    canonical_node_id,
    run_id,
    facility_id,
    service_line,
    anomaly_profile,
    anomaly_classification,
    anomaly_method,
    applies_to,
    statistic_value,
    interpretation_rendered,
    interpretation_template_id,
    current_timestamp as dbt_updated_at
from parsed_anomalies
where anomaly_classification is not null
