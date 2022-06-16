{%- macro get_test_results(results_sample_limit = 5) -%}
    --{# TODO: how do we show only last (if we had the same test in the last x days) #}
    {% set select_test_results %}
        with elemetary_test_results as (
            select * from {{ ref('elementary', 'elementary_test_results') }}
        ),

        tests_in_last_30_days as (
            select *
                from elemetary_test_results
                where {{ elementary.cast_as_timestamp('detected_at') }} >= {{ get_alerts_time_limit(30) }}
        )

        select *,
               {{ dbt_utils.datediff(elementary.cast_as_timestamp('detected_at'), dbt_utils.current_timestamp(), 'day') }} as days_diff
               from tests_in_last_30_days
    {%- endset -%}
    {% set query_results_agate = run_query(select_test_results) %}
    {% set query_results = elementary.agate_to_dicts(query_results_agate) %}
    {% set test_results = [] %}
    {%- for result_dict in query_results -%}
        {% set test_results_query = elementary.insensitive_get_dict_value(result_dict, 'test_results_query') %}
        {% set test_type = elementary.insensitive_get_dict_value(result_dict, 'test_type') %}
        {% set status = elementary.insensitive_get_dict_value(result_dict, 'status') | lower %}

        --{# TODO: maybe move to handle_run_result #}
        {% set test_rows_sample = none %}
        {% if test_results_query and status != 'error' %}
            {% set test_query = test_results_query %}
            {% if test_type == 'dbt_test' %}
                {% set test_query = test_results_query ~ ' limit ' ~ results_sample_limit %}
            {% endif %}

            {% set test_rows_agate = run_query(test_query) %}
            {% set test_rows_sample = elementary.agate_to_dicts(test_rows_agate) %}
        {% endif %}

        {% set new_alert_dict = {'alert_id': elementary.insensitive_get_dict_value(result_dict, 'id'),
                                 'model_unique_id': elementary.insensitive_get_dict_value(result_dict, 'model_unique_id'),
                                 'test_unique_id': elementary.insensitive_get_dict_value(result_dict, 'test_unique_id'),
                                 'detected_at': elementary.insensitive_get_dict_value(result_dict, 'detected_at'),
                                 'database_name': elementary.insensitive_get_dict_value(result_dict, 'database_name'),
                                 'schema_name': elementary.insensitive_get_dict_value(result_dict, 'schema_name'),
                                 'table_name': elementary.insensitive_get_dict_value(result_dict, 'table_name'),
                                 'column_name': elementary.insensitive_get_dict_value(result_dict, 'column_name'),
                                 'alert_type': test_type,
                                 'sub_type': elementary.insensitive_get_dict_value(result_dict, 'test_sub_type'),
                                 'alert_description': elementary.insensitive_get_dict_value(result_dict, 'test_results_description'),
                                 'owners': elementary.insensitive_get_dict_value(result_dict, 'owners'),
                                 'tags': elementary.insensitive_get_dict_value(result_dict, 'tags'),
                                 'alert_results_query': test_results_query,
                                 'alert_results': test_rows_sample,
                                 'other': elementary.insensitive_get_dict_value(result_dict, 'other'),
                                 'test_name': elementary.insensitive_get_dict_value(result_dict, 'test_name'),
                                 'test_params': elementary.insensitive_get_dict_value(result_dict, 'test_params'),
                                 'severity': elementary.insensitive_get_dict_value(result_dict, 'severity'),
                                 'status': status,
                                 'days_diff': elementary.insensitive_get_dict_value(result_dict, 'days_diff')} %}
        {% do test_results.append(new_alert_dict) %}
    {%- endfor -%}
    {% do elementary.edr_log(tojson(test_results)) %}
{%- endmacro -%}

