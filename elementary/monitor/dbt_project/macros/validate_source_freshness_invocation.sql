{% macro validate_source_freshness_invocation(invocation_id, days_back=14) %}
    {% set query %}
        with invocations as (
            select invocation_id
            from {{ ref("elementary", "dbt_source_freshness_results") }}
            where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('generated_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
        )
        select count(*) as count
        from invocations
        where invocation_id = {{ "'" ~ invocation_id ~ "'" }}
    {% endset %}

    {% set result = elementary.run_query(query) %}

    {% if result[0][0] == 0 %}
        {% do return(true) %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}
