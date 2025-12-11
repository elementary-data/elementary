{% macro normalized_source_freshness_status(status_column='status') %}
    case
        when {{ status_column }} = 'error' then 'fail'
        when {{ status_column }} = 'runtime error' then 'error'
        else {{ status_column }}
    end as normalized_status
{% endmacro %}
