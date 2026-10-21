{#
    dbt-core writes source freshness statuses in lowercase ("pass", "warn", "error",
    "runtime error"), while dbt Fusion writes them capitalized ("Pass", "Warn", "Error").
    Compare and return them lowercased, so rows from either engine get the same status.
#}
{% macro normalized_source_freshness_status(status_column='status') %}
    case
        when lower({{ status_column }}) = 'error' then 'fail'
        when lower({{ status_column }}) = 'runtime error' then 'error'
        else lower({{ status_column }})
    end as normalized_status
{% endmacro %}
