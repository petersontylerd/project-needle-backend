{% macro classify_metric_domain(metric_id_column) %}
{#
    Classifies a metric into a domain based on naming conventions.
    
    Domain mappings:
        - Efficiency: LOS, length of stay, cost, charge, wait, throughput
        - Effectiveness: readmission, readmit
        - Safety: mortality, death, infection, sepsis, fall
        - Other: anything not matching above patterns
    
    Args:
        metric_id_column: The column containing the metric identifier
    
    Returns:
        SQL CASE expression that evaluates to the domain name
    
    Example:
        {{ classify_metric_domain('metric_id') }} as domain
#}
    case
        when {{ metric_id_column }} ilike '%los%' or {{ metric_id_column }} ilike '%length%stay%' then 'Efficiency'
        when {{ metric_id_column }} ilike '%readmission%' or {{ metric_id_column }} ilike '%readmit%' then 'Effectiveness'
        when {{ metric_id_column }} ilike '%mortality%' or {{ metric_id_column }} ilike '%death%' then 'Safety'
        when {{ metric_id_column }} ilike '%infection%' or {{ metric_id_column }} ilike '%sepsis%' then 'Safety'
        when {{ metric_id_column }} ilike '%fall%' then 'Safety'
        when {{ metric_id_column }} ilike '%cost%' or {{ metric_id_column }} ilike '%charge%' then 'Efficiency'
        when {{ metric_id_column }} ilike '%wait%' or {{ metric_id_column }} ilike '%throughput%' then 'Efficiency'
        else 'Other'
    end
{% endmacro %}
