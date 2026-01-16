-- fct_signals_grain_unique.sql
-- Fails if any signal_id appears more than once
-- This ensures the fact table maintains entity-level grain (no method fan-out)
-- signal_id is the surrogate key derived from (run_id, canonical_node_id, entity_result_id)

select
    signal_id,
    count(*) as row_count
from {{ ref('fct_signals') }}
group by 1
having count(*) > 1
