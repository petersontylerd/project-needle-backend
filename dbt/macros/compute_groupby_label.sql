{% macro compute_groupby_label(entity_dimensions_column) %}
{#
    Computes a human-readable label for the groupby dimensions.
    
    Args:
        entity_dimensions_column: Column reference containing JSONB entity dimensions
                                  (e.g., {"vizientServiceLine": "Cardiology"})
    
    Returns:
        Human-readable label like:
        - "Facility-wide" for empty dimensions
        - "Service Line" for {"vizientServiceLine": "..."}
        - "Service Line / Sub-Service Line" for multiple dimensions
    
    Example Usage:
        SELECT {{ compute_groupby_label('entity_dimensions') }} AS groupby_label
        FROM my_table
#}
    case
        when {{ entity_dimensions_column }} is null or {{ entity_dimensions_column }} = '{}'::jsonb
        then 'Facility-wide'
        else (
            select string_agg(
                case key
                    when 'vizientServiceLine' then 'Service Line'
                    when 'vizientSubServiceLine' then 'Sub-Service Line'
                    when 'admissionSource' then 'Admission Source'
                    when 'dischargeStatus' then 'Discharge Status'
                    when 'primaryPayer' then 'Primary Payer'
                    when 'patientAge' then 'Patient Age'
                    when 'patientGender' then 'Patient Gender'
                    else initcap(replace(key, '_', ' '))
                end,
                ' / ' order by key
            )
            from jsonb_object_keys({{ entity_dimensions_column }}) as key
        )
    end
{% endmacro %}
