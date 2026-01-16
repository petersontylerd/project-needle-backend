{% macro map_anomaly_to_severity(anomaly_classification_column) %}
{#
    Maps the 9-tier anomaly classification to a 4-tier severity level.
    
    9-tier classifications:
        - extremely_high, extremely_low -> Critical
        - very_high, very_low -> High
        - moderately_high, moderately_low, high, low -> Moderate
        - slightly_high, slightly_low -> Watch
        - normal, no_score -> NULL (no action needed)
    
    Args:
        anomaly_classification_column: The column containing the anomaly classification
    
    Returns:
        SQL CASE expression that evaluates to 'Critical', 'High', 'Moderate', 'Watch', or NULL
    
    Example:
        {{ map_anomaly_to_severity('anomaly_classification') }} as severity
#}
    case
        when {{ anomaly_classification_column }} in ('extremely_high', 'extremely_low') then 'Critical'
        when {{ anomaly_classification_column }} in ('very_high', 'very_low') then 'High'
        when {{ anomaly_classification_column }} in ('moderately_high', 'moderately_low', 'high', 'low') then 'Moderate'
        when {{ anomaly_classification_column }} in ('slightly_high', 'slightly_low') then 'Watch'
        else null
    end
{% endmacro %}
