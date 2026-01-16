{% macro analyze_table() %}
{#
    Runs PostgreSQL ANALYZE on the current model after materialization.
    ANALYZE updates table statistics used by the query planner for optimal
    query execution plans.
    
    Usage in dbt_project.yml:
        models:
          quality_compass:
            marts:
              +post-hook: "{{ analyze_table() }}"
    
    Or in model config:
        {{ config(post_hook="{{ analyze_table() }}") }}
#}
    analyze {{ this }}
{% endmacro %}


{% macro create_index_if_not_exists(index_name, columns, unique=false) %}
{#
    Creates a PostgreSQL index if it doesn't already exist.
    Useful for optimizing common query patterns on mart tables.
    
    Args:
        index_name: Name for the index (should follow naming convention: idx_{table}_{columns})
        columns: List of column names to index
        unique: Boolean, whether to create a unique index (default: false)
    
    Usage in model post-hook:
        {{ config(
            post_hook=[
                "{{ create_index_if_not_exists('idx_fct_signals_facility', ['facility_id']) }}",
                "{{ create_index_if_not_exists('idx_fct_signals_severity', ['severity']) }}"
            ]
        ) }}
#}
    {% set unique_clause = 'unique' if unique else '' %}
    {% set column_list = columns | join(', ') %}
    
    create {{ unique_clause }} index if not exists {{ index_name }}
    on {{ this }} ({{ column_list }})
{% endmacro %}


{% macro vacuum_analyze_table(table_name, schema_name=none) %}
{#
    Runs VACUUM ANALYZE on a specified table.
    VACUUM reclaims storage from dead tuples.
    ANALYZE updates statistics.
    
    Args:
        table_name: Name of the table to maintain
        schema_name: Optional schema name (defaults to target schema)
    
    Usage:
        Run as an operation:
        dbt run-operation vacuum_analyze_table --args '{"table_name": "fct_signals"}'
    
    Note: Requires appropriate database privileges.
#}
    {% set schema = schema_name if schema_name else target.schema %}
    
    {% set sql %}
        vacuum analyze {{ schema }}.{{ table_name }}
    {% endset %}
    
    {% if execute %}
        {% set results = run_query(sql) %}
        {{ log("Completed VACUUM ANALYZE on " ~ schema ~ "." ~ table_name, info=true) }}
    {% endif %}
{% endmacro %}
