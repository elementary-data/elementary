{% macro validate_schema_changes() %}
    {% set expected_changes = {('schema_changes', 'red_cards'): 'column_added',
                               ('schema_changes', 'group_a'):   'column_removed',
                               ('schema_changes', 'goals'):   'type_changed',
                               ('schema_changes', 'key_crosses'): 'column_added',
                               ('schema_changes', 'offsides'): 'column_removed',
                               ('schema_changes_from_baseline', 'group_b'): 'type_changed',
                               ('schema_changes_from_baseline', 'group_d'): 'column_added',
                               ('schema_changes_from_baseline', 'goals'): 'type_changed',
                               ('schema_changes_from_baseline', 'coffee_cups_consumed'): 'column_removed'
                               } %}
    {% set alerts_relation = ref('alerts_schema_changes') %}
    {% set failed_schema_changes_alerts %}
        select test_short_name, column_name, sub_type
        from {{ alerts_relation }}
        where status in ('fail', 'warn')
        group by 1,2,3
    {% endset %}
    {% set error_schema_changes_alerts %}
        select test_short_name, column_name, sub_type
        from {{ alerts_relation }}
        where status = 'error'
        group by 1,2,3
    {% endset %}
    {% set error_alert_rows = run_query(error_schema_changes_alerts) %}
    {# We should have one error test from schema_changes_from_baseline with enforce_types true #}
    {% if error_alert_rows | length != 1 %}
        {% do elementary.edr_log("FAILED: for schema_changes_from_baseline with enforce_types true - no error eccured") %}
        {{ return(1) }}
    {% endif %}
    {% set failure_alert_rows = run_query(failed_schema_changes_alerts) %}
    {% set found_schema_changes = {} %}
    {% for row in failure_alert_rows %}
        {% set test_short_name = row[0] | lower %}
        {% set column_name = row[1] | lower %}
        {% set alert = row[2] | lower %}
        {% if (test_short_name, column_name) not in expected_changes %}
            {% do elementary.edr_log("FAILED: " ~ test_short_name ~ " - could not find expected alert for " ~ column_name ~ ", " ~ alert) %}
        {% endif %}
        {% if expected_changes[(test_short_name, column_name)] != alert %}
            {% do elementary.edr_log("FAILED: " ~ test_short_name ~ " - for column " ~ column_name ~ " expected alert type " ~ expected_changes[(test_short_name, column_name)] ~ " but got " ~ alert) %}
            {{ return(1) }}
        {% endif %}
        {% do found_schema_changes.update({(test_short_name, column_name): alert}) %}
    {% endfor %}
    {% if found_schema_changes %}
        {%- set missing_changes = [] %}
        {%- for expected_change in expected_changes %}
            {%- if expected_change not in found_schema_changes %}
                {% do elementary.edr_log("FAILED: for column " ~ expected_change ~ " expected alert " ~ expected_changes[expected_change] ~ " but alert is missing") %}
                {%- do missing_changes.append(expected_change) -%}
            {%- endif %}
        {%- endfor %}
        {%- if missing_changes | length == 0 %}
            {% do elementary.edr_log("SUCCESS: all expected schema changes were found - " ~ found_schema_changes) %}
            {{ return(0) }}
        {%- endif %}
    {% endif %}
    {{ return(0) }}
{% endmacro %}

