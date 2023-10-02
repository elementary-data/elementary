{% macro get_pending_source_freshness_alerts(days_back) %}
    -- depends_on: {{ ref('alerts_source_freshness') }}
    {% set alerts_source_freshness_relation = ref('alerts_source_freshness') %}
    {% set error_after_column_exists = elementary.column_exists_in_relation(alerts_source_freshness_relation, 'error_after') %}

    {% set sources_relation = ref('elementary', 'dbt_sources') %}
    {% set freshness_description_column_exists = elementary.column_exists_in_relation(sources_relation, 'freshness_description') %}

    {% set select_pending_alerts_query %}
        with alerts_in_time_limit as (
            select *, {{ elementary_cli.normalized_source_freshness_status()}} from {{ ref('alerts_source_freshness') }}
            where {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit(days_back) }}
        ),

        sources as (
            select * from {{ ref('elementary', 'dbt_sources') }}
        ),

        extended_alerts as (
            select
                alerts_in_time_limit.alert_id,
                alerts_in_time_limit.max_loaded_at,
                alerts_in_time_limit.snapshotted_at,
                alerts_in_time_limit.detected_at,
                alerts_in_time_limit.max_loaded_at_time_ago_in_s,
                alerts_in_time_limit.status,
                alerts_in_time_limit.normalized_status,
                alerts_in_time_limit.error,
                alerts_in_time_limit.unique_id,
                {# Currently alert_class_id equals to unique_id - might change in the future so we return both #}
                alerts_in_time_limit.unique_id as alert_class_id,
                alerts_in_time_limit.database_name,
                alerts_in_time_limit.schema_name,
                alerts_in_time_limit.source_name,
                alerts_in_time_limit.identifier,
                {# backwards compatibility - these fields were added together #}
                {% if error_after_column_exists %}
                    alerts_in_time_limit.warn_after,
                    alerts_in_time_limit.error_after,
                    alerts_in_time_limit.filter,
                {% endif %}
                alerts_in_time_limit.tags,
                alerts_in_time_limit.meta,
                alerts_in_time_limit.owner,
                alerts_in_time_limit.package_name,
                alerts_in_time_limit.path,
                {# backwards compatibility #}
                case
                    when alerts_in_time_limit.suppression_status is NULL and alerts_in_time_limit.alert_sent = TRUE then 'sent'
                    when alerts_in_time_limit.suppression_status is NULL and alerts_in_time_limit.alert_sent = FALSE then 'pending'
                    else suppression_status
                end as suppression_status,
                alerts_in_time_limit.sent_at,
                sources.meta as model_meta,
                sources.freshness_error_after,
                sources.freshness_warn_after,
                sources.freshness_filter,
                {# backwards compatibility #}
                {% if freshness_description_column_exists %}
                    sources.freshness_description
                {% else %}
                    'dbt source freshness validates if the data in a table is not updated by calculating if the time elapsed between the test execution to the latest record is above an acceptable SLA threshold.' as freshness_description
                {% endif %}
            from alerts_in_time_limit
            left join sources on alerts_in_time_limit.unique_id = sources.unique_id
        )

        select *
        from extended_alerts
        where suppression_status = 'pending'
    {% endset %}

    {% set alerts_agate = run_query(select_pending_alerts_query) %}
    {% set alerts_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set pending_alerts = [] %}
    {% for alert_dict in alerts_dicts %}
        {% set error_after = alert_dict.get('error_after') %}
        {% set warn_after = alert_dict.get('warn_after') %}
        {% set filter = alert_dict.get('filter') %}

        {% set pending_alert_dict = {'id': alert_dict.get('alert_id'),
                                 'model_unique_id': alert_dict.get('unique_id'),
                                 'alert_class_id': alert_dict.get('alert_class_id'),
                                 'detected_at': alert_dict.get('detected_at'),
                                 'snapshotted_at': alert_dict.get('snapshotted_at'),
                                 'max_loaded_at': alert_dict.get('max_loaded_at'),
                                 'max_loaded_at_time_ago_in_s': alert_dict.get('max_loaded_at_time_ago_in_s'),
                                 'database_name': alert_dict.get('database_name'),
                                 'schema_name': alert_dict.get('schema_name'),
                                 'source_name': alert_dict.get('source_name'),
                                 'identifier': alert_dict.get('identifier'),
                                 'error_after': error_after if error_after is not none else alert_dict.get('freshness_error_after'),
                                 'warn_after': warn_after if warn_after is not none else alert_dict.get('freshness_warn_after'),
                                 'filter': filter if filter is not none else alert_dict.get('freshness_filter'),
                                 'status': alert_dict.get('status'),
                                 'normalized_status': alert_dict.get('normalized_status'),
                                 'owners': alert_dict.get('owner'),
                                 'path': alert_dict.get('path'),
                                 'error': alert_dict.get('error'),
                                 'tags': alert_dict.get('tags'),
                                 'model_meta': alert_dict.get('model_meta'),
                                 'suppression_status': alert_dict.get('suppression_status'),
                                 'sent_at': alert_dict.get('sent_at'),
                                 'freshness_description': alert_dict.get('freshness_description')
                                } %}
        {% do pending_alerts.append(pending_alert_dict) %}
    {% endfor %}
    {% do return(pending_alerts) %}
{% endmacro %}


{% macro get_last_source_freshness_alert_sent_times(days_back) %}
    -- depends_on: {{ ref('alerts_source_freshness') }}
    {% set select_last_alert_sent_times_query %}
        with alerts_in_time_limit as (
            select
                unique_id as alert_class_id,
                case
                    when suppression_status is NULL and alert_sent = TRUE then 'sent'
                    when suppression_status is NULL and alert_sent = FALSE then 'pending'
                    else suppression_status
                end as suppression_status,
                sent_at
            from {{ ref('alerts_source_freshness') }}
            where {{ elementary.edr_cast_as_timestamp('detected_at') }} >= {{ elementary_cli.get_alerts_time_limit(days_back) }}
        )

        select 
            alert_class_id,
            max(sent_at) as last_sent_at
        from alerts_in_time_limit
        where suppression_status = 'sent'
        group by alert_class_id
    {% endset %}

    {% set alerts_agate = run_query(select_last_alert_sent_times_query) %}
    {% set last_alert_sent_time_result_dicts = elementary.agate_to_dicts(alerts_agate) %}
    {% set last_alert_times = {} %}
    {% for last_alert_sent_time_result_dict in last_alert_sent_time_result_dicts %}
        {% do last_alert_times.update({
            last_alert_sent_time_result_dict.get('alert_class_id'): last_alert_sent_time_result_dict.get('last_sent_at')
        }) %}
    {% endfor %}
    {% do return(last_alert_times) %}
{% endmacro %}
