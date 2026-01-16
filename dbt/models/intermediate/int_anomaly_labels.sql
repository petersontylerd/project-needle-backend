-- int_anomaly_labels.sql
-- Intermediate model that pivots anomaly classifications by applies_to.
-- Creates one row per statistical_method_id with anomaly labels as columns.
--
-- Source: stg_anomalies (parsed anomaly data)
-- Grain: One row per statistical_method_id
--
-- Pivoted Anomaly Labels:
--   - simple_zscore_anomaly: Anomaly classification for simple z-score
--   - robust_zscore_anomaly: Anomaly classification for robust z-score
--   - latest_simple_zscore_anomaly: For temporal nodes
--   - mean_simple_zscore_anomaly: For temporal nodes
--   - latest_robust_zscore_anomaly: For temporal nodes
--   - mean_robust_zscore_anomaly: For temporal nodes
--   - slope_anomaly: For slope percentile method

{{
    config(
        materialized='table',
        tags=['intermediate', 'anomalies']
    )
}}

with anomalies as (
    select
        statistical_method_id,
        anomaly_classification,
        applies_to
    from {{ ref('stg_anomalies') }}
)

select
    statistical_method_id,
    max(case when applies_to = 'simple_zscore' then anomaly_classification end) as simple_zscore_anomaly,
    max(case when applies_to = 'robust_zscore' then anomaly_classification end) as robust_zscore_anomaly,
    max(case when applies_to = 'latest_simple_zscore' then anomaly_classification end) as latest_simple_zscore_anomaly,
    max(case when applies_to = 'mean_simple_zscore' then anomaly_classification end) as mean_simple_zscore_anomaly,
    max(case when applies_to = 'latest_robust_zscore' then anomaly_classification end) as latest_robust_zscore_anomaly,
    max(case when applies_to = 'mean_robust_zscore' then anomaly_classification end) as mean_robust_zscore_anomaly,
    max(case when applies_to = 'slope_percentile' then anomaly_classification end) as slope_anomaly,
    current_timestamp as dbt_updated_at
from anomalies
group by statistical_method_id
