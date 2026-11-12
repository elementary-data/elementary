{% macro normalized_source_freshness_status(status_column='status') %}
    case
        when lower({{ status_column }}) = 'error' then 'fail'
        when lower({{ status_column }}) = 'runtime error' then 'error'
        else lower({{ status_column }})
    end as normalized_status
{% endmacro %}
