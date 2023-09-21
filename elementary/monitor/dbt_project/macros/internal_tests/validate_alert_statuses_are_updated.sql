{% macro validate_alert_statuses_are_updated() %}
    {% set alerts_with_no_updated_status_query %}
        with alerts as (
            select *
            from {{ ref('alerts') }}
            -- When using --group-by table, singular test alerts are not sent.
            where sub_type != 'singular'
        ),

        alerts_models as (
            select *
            from {{ ref('alerts_models') }}
        ),

        alerts_source_freshness as (
            select *
            from {{ ref('alerts_source_freshness') }}
        ),

        all_alerts as (
            select alert_id, suppression_status
            from alerts
            union all
            select alert_id, suppression_status
            from alerts_models
            union all
            select alert_id, suppression_status
            from alerts_source_freshness
        )

        select alert_id
        from all_alerts
        where suppression_status not in ('sent', 'skipped')
    {% endset %}
    {% set alerts_agate = run_query(alerts_with_no_updated_status_query) %}
    {% set alerts_with_no_updated_status = elementary.agate_to_dicts(alerts_agate) %}
    {% if alerts_with_no_updated_status %}
        {% do exceptions.raise_compiler_error("Elementary couldn't update all of the alerts status") %}
    {% endif %}
{% endmacro %}
