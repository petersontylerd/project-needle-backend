-- int_statistical_methods_agg_grain_unique.sql
-- Fails if any entity_result_id has more than one row
-- This ensures the intermediate model maintains entity-level grain

select
    entity_result_id,
    count(*) as row_count
from {{ ref('int_statistical_methods_agg') }}
group by 1
having count(*) > 1
