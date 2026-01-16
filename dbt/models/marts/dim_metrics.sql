-- dim_metrics.sql
-- Dimension table of unique metrics with semantic descriptions.
-- Provides plain-language context for metric identifiers.
--
-- Grain: One row per metric (metric_id)
--
-- Usage: Join with fact tables on metric_id for metric descriptions.
--
-- Semantic data is sourced from the metric_catalog seed, which is generated
-- from taxonomy/metrics.yaml. See scripts/semantic/generate_metric_catalog_seed.py.
--
-- Fallback logic: If a metric_id is not in the catalog, heuristic defaults are applied.

{{
    config(
        materialized='table',
        tags=['marts', 'dimension', 'metric', 'semantic']
    )
}}

with metrics_from_entities as (
    -- Extract unique metrics from entity results
    select distinct metric_id
    from {{ ref('stg_entity_results') }}
    where metric_id is not null
),

metrics_from_contributions as (
    -- Extract unique metrics from contributions
    select distinct metric_id
    from {{ ref('stg_contributions') }}
    where metric_id is not null
),

all_metrics as (
    select metric_id from metrics_from_entities
    union
    select metric_id from metrics_from_contributions
),

-- Load authoritative metric catalog from seed
metric_catalog as (
    select
        metric_id,
        metric_name,
        description,
        domain,
        direction_preference,
        unit_type,
        display_format
    from {{ ref('metric_catalog') }}
),

-- Enrich metrics: prefer catalog, fall back to heuristics
enriched as (
    select
        am.metric_id,

        -- Use catalog values if available, else apply heuristics
        coalesce(
            mc.domain,
            {{ classify_metric_domain('am.metric_id') }}
        ) as domain,

        coalesce(
            mc.metric_name,
            initcap(
                replace(
                    replace(
                        replace(am.metric_id, '_', ' '),
                        'Index', ' Index'
                    ),
                    'Rate', ' Rate'
                )
            )
        ) as metric_name,

        coalesce(
            mc.description,
            'Quality metric tracked by Project Needle analytics.'
        ) as description,

        coalesce(
            mc.direction_preference,
            case
                when am.metric_id ilike '%los%' then 'lower_is_better'
                when am.metric_id ilike '%readmission%' then 'lower_is_better'
                when am.metric_id ilike '%mortality%' then 'lower_is_better'
                when am.metric_id ilike '%cost%' then 'lower_is_better'
                when am.metric_id ilike '%fall%' then 'lower_is_better'
                when am.metric_id ilike '%infection%' then 'lower_is_better'
                when am.metric_id ilike '%satisfaction%' then 'higher_is_better'
                when am.metric_id ilike '%compliance%' then 'higher_is_better'
                else 'context_dependent'
            end
        ) as direction_preference,

        coalesce(
            mc.unit_type,
            case
                when am.metric_id ilike '%index%' then 'index (ratio)'
                when am.metric_id ilike '%rate%' then 'percentage'
                when am.metric_id ilike '%cost%' then 'currency (USD)'
                when am.metric_id ilike '%days%' or am.metric_id ilike '%los%' then 'days'
                when am.metric_id ilike '%count%' then 'count'
                else 'numeric'
            end
        ) as unit_type,

        mc.display_format

    from all_metrics am
    left join metric_catalog mc on am.metric_id = mc.metric_id
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['metric_id']) }} as metric_sk,

    -- Natural key
    metric_id,

    -- Semantic attributes
    metric_name,
    description,
    domain,
    direction_preference,
    unit_type,
    display_format,

    -- Metadata
    current_timestamp as dbt_updated_at

from enriched
