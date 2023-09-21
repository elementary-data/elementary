{% macro can_upload_source_freshness(invocation_id, days_back=14) %}
    {% set counter_query %}
        with invocations as (
            select invocation_id
            from {{ ref("elementary", "dbt_source_freshness_results") }}
            where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('generated_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
        )
        select count(*) as count
        from invocations
        where invocation_id = {{ elementary.edr_quote(invocation_id) }}
    {% endset %}

    {% set records_count = elementary.result_value(counter_query) %}

    {% if records_count == 0 %}
        {% do return(true) %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}
