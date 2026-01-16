{{
    config(
        materialized = 'table',
        schema = 'semantic'
    )
}}

/*
MetricFlow Time Spine

This model provides a date dimension table required by MetricFlow for time-based 
metric aggregations. It generates a complete date range for analytics.

Documentation: https://docs.getdbt.com/docs/build/metricflow-time-spine
*/

with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2020-01-01' as date)",
            end_date="cast('2030-12-31' as date)"
        )
    }}
)

select
    cast(date_day as date) as date_day
from date_spine
