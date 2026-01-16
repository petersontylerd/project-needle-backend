-- dim_facilities.sql
-- Dimension table of unique facilities (Sites of Care).
-- Deduplicates facility information from entity results for clean lookups.
-- Enriches with reference data from ref_facilities seed when available.
--
-- Grain: One row per facility (facility_id)
--
-- Usage: Join with fact tables on facility_id for facility attributes.

{{
    config(
        materialized='table',
        tags=['marts', 'dimension', 'facility']
    )
}}

with facility_entities as (
    -- Extract unique facility info from entity results
    select distinct
        facility_id,
        -- Take first non-null system_name for each facility
        first_value(system_name) over (
            partition by facility_id
            order by case when system_name is not null then 0 else 1 end, loaded_at desc
        ) as system_name,
        -- Take first non-null value for each facility
        first_value(service_line) over (
            partition by facility_id
            order by case when service_line is not null then 0 else 1 end, loaded_at desc
        ) as primary_service_line,
        max(encounters) over (partition by facility_id) as total_encounters,
        min(loaded_at) over (partition by facility_id) as first_seen_at,
        max(loaded_at) over (partition by facility_id) as last_seen_at
    from {{ ref('stg_entity_results') }}
    where facility_id is not null
),

-- Deduplicate to single row per facility
deduplicated as (
    select distinct
        facility_id,
        system_name,
        primary_service_line,
        total_encounters,
        first_seen_at,
        last_seen_at
    from facility_entities
),

-- Join with reference data for facility names and attributes
enriched as (
    select
        d.facility_id,
        d.system_name,
        d.primary_service_line,
        d.total_encounters,
        d.first_seen_at,
        d.last_seen_at,
        -- Use reference data when available, otherwise fallback to facility_id
        coalesce(r.facility_name, d.facility_id) as facility_name,
        r.facility_type,
        -- Prefer system_name from entity results, fallback to health_system from ref data
        coalesce(d.system_name, r.health_system) as health_system,
        r.region,
        r.state,
        r.bed_count
    from deduplicated d
    left join {{ ref('ref_facilities') }} r
        on d.facility_id = r.facility_id
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['facility_id']) }} as facility_sk,

    -- Natural key
    facility_id,

    -- System name (from entity results, matches runtime group_by)
    system_name,

    -- Facility attributes from reference data
    facility_name,
    facility_type,
    health_system,
    region,
    state,
    bed_count,

    -- Primary service line (derived from entity results)
    primary_service_line,

    -- Volume indicators
    total_encounters,

    -- Metadata
    first_seen_at,
    last_seen_at,
    current_timestamp as dbt_updated_at

from enriched
