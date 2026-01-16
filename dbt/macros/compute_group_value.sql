{% macro compute_group_value(entity_dimensions_column) %}
{#
    Computes the concatenated values of entity dimensions for display.
    
    Args:
        entity_dimensions_column: Column reference containing JSONB entity dimensions
                                  (e.g., {"vizientServiceLine": "Cardiology"})
    
    Returns:
        Concatenated values like:
        - "Facility-wide" for empty dimensions
        - "Cardiology" for {"vizientServiceLine": "Cardiology"}
        - "Cardiology / Cardiac ICU" for multiple dimensions
    
    Example Usage:
        SELECT {{ compute_group_value('entity_dimensions') }} AS group_value
        FROM my_table
#}
    case
        when {{ entity_dimensions_column }} is null or {{ entity_dimensions_column }} = '{}'::jsonb
        then 'Facility-wide'
        else (
            select coalesce(
                string_agg(value::text, ' / ' order by key),
                'Facility-wide'
            )
            from jsonb_each_text({{ entity_dimensions_column }})
        )
    end
{% endmacro %}
