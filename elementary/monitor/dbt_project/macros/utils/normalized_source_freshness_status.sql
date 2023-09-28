{% macro normalized_source_freshness_status() %}
    case
        when status = 'error' then 'fail'
        when status = 'runtime error' then 'error'
        else status
    end as normalized_status
{% endmacro %}
