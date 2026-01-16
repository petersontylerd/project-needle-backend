-- ABOUTME: Summarizes modeling experiments with key metrics and feature drivers.
-- ABOUTME: Provides a high-level view of model performance for API consumption.
{{
    config(
        materialized='table'
    )
}}

with experiments as (
    select
        run_id,
        estimator,
        groupby_label,
        group_value,
        row_count,
        duration_seconds,

        -- Primary metrics (on transformed scale)
        mae,
        rmse,
        r2,

        -- Original scale metrics
        mae_original,
        rmse_original,
        r2_original,

        -- Interpretability paths for driver extraction
        global_shap_path,
        facility_shap_path,
        encounter_shap_path,

        loaded_at

    from {{ ref('stg_modeling_experiments') }}
),

ranked as (
    select
        *,
        -- Rank experiments by R² (higher is better)
        row_number() over (
            partition by run_id, estimator
            order by r2 desc nulls last
        ) as experiment_rank
    from experiments
)

select
    run_id,
    estimator,
    groupby_label,
    group_value,
    row_count,
    duration_seconds,
    mae,
    rmse,
    r2,
    mae_original,
    rmse_original,
    r2_original,
    global_shap_path,
    facility_shap_path,
    encounter_shap_path,
    experiment_rank,
    loaded_at
from ranked
