-- ABOUTME: Parses raw modeling experiment JSON into structured columns.
-- ABOUTME: Extracts metrics, hyperparameters, and artifact paths for analytics.
{{
    config(
        materialized='view'
    )
}}

with source as (
    select
        *,
        json_data::jsonb as data
    from {{ source('raw', 'raw_modeling_experiments') }}
),

parsed as (
    select
        id,
        run_id,

        -- Experiment configuration
        data->>'config_path' as config_path,
        data->>'groupby_label' as groupby_label,
        data->>'group_value' as group_value,
        data->>'estimator' as estimator,
        data->>'scoring' as scoring,
        (data->>'n_iter')::integer as n_iter,
        (data->>'random_state')::integer as random_state,
        (data->>'tuning_random_state')::integer as tuning_random_state,

        -- Dataset info
        data->>'dataset_path' as dataset_path,
        (data->>'rows')::integer as row_count,
        (data->>'duration_seconds')::float as duration_seconds,

        -- Model metrics
        (data->'metrics'->>'mae')::float as mae,
        (data->'metrics'->>'rmse')::float as rmse,
        (data->'metrics'->>'r2')::float as r2,
        (data->'metrics'->>'mae_original')::float as mae_original,
        (data->'metrics'->>'rmse_original')::float as rmse_original,
        (data->'metrics'->>'r2_original')::float as r2_original,
        (data->'metrics'->>'inverse_cap')::float as inverse_cap,
        (data->'metrics'->>'capped_pred_original_count')::float as capped_pred_count,
        (data->'metrics'->>'capped_pred_original_fraction')::float as capped_pred_fraction,

        -- Artifact paths
        data->'artifacts'->>'metrics' as metrics_path,
        data->'artifacts'->>'predictions' as predictions_path,
        data->'artifacts'->>'eda_summary' as eda_summary_path,
        data->'artifacts'->>'group_summary' as group_summary_path,
        data->'artifacts'->>'feature_descriptor' as feature_descriptor_path,
        data->'artifacts'->>'hparam_trials' as hparam_trials_path,

        -- Interpretability artifacts
        data->'artifacts'->'interpretability'->>'encounter' as encounter_shap_path,
        data->'artifacts'->'interpretability'->>'facility' as facility_shap_path,
        data->'artifacts'->'interpretability'->>'global_summary' as global_shap_path,
        data->'artifacts'->'interpretability'->>'transparency' as transparency_path,

        -- Full JSON for complex queries
        data as raw_json,
        loaded_at

    from source
)

select * from parsed
