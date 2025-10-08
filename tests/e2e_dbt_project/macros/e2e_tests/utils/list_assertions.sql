{% macro assert_value_in_list(value, list, context='') %}
    {% set upper_value = value | upper %}
    {% set lower_value = value | lower %}
    {% if upper_value in list or lower_value in list %}
        {% do elementary.edr_log(context ~ " SUCCESS: " ~ upper_value  ~ " in list " ~ list) %}
        {{ return(0) }}
    {% else %}
        {% do elementary.edr_log(context ~ " FAILED: " ~ upper_value ~ " not in list " ~ list) %}
        {{ return(1) }}
    {% endif %}
{% endmacro %}

{% macro assert_value_not_in_list(value, list) %}
    {% set upper_value = value | upper %}
    {% if upper_value not in list %}
        {% do elementary.edr_log("SUCCESS: " ~ upper_value  ~ " not in list " ~ list) %}
        {{ return(0) }}
    {% else %}
        {% do elementary.edr_log("FAILED: " ~ upper_value ~ " in list " ~ list) %}
        {{ return(1) }}
    {% endif %}
{% endmacro %}

{% macro assert_lists_contain_same_items(list1, list2, context='') %}
    {% if list1 | length != list2 | length %}
        {% do elementary.edr_log(context ~ " FAILED: " ~ list1 ~ " has different length than " ~ list2) %}
        {{ return(1) }}
    {% endif %}
    {% for item1 in list1 %}
        {% if item1 is string %}
            {% set item1 = item1 | lower %}
        {% endif %}
        {% if item1 not in list2 %}
            {% do elementary.edr_log(context ~ " FAILED: " ~ item1 ~ " not in list " ~ list2) %}
            {{ return(1) }}
        {% endif %}
    {% endfor %}
    {% do elementary.edr_log(context ~ " SUCCESS: " ~ list1  ~ " in list " ~ list2) %}
    {{ return(0) }}
{% endmacro %}

{% macro assert_list1_in_list2(list1, list2, context = '') %}
    {% set lower_list2 = list2 | lower %}
    {% if not list1 or not list2 %}
        {% do elementary.edr_log(context ~ " FAILED: list1 is empty or list2 is empty") %}
        {{ return(1) }}
    {% endif %}
    {% for item1 in list1 %}
        {% if item1 | lower not in lower_list2 %}
            {% do elementary.edr_log(context ~ " FAILED: " ~ item1 ~ " not in list " ~ list2) %}
            {{ return(1) }}
        {% endif %}
    {% endfor %}
    {% do elementary.edr_log(context ~ " SUCCESS: " ~ list1  ~ " in list " ~ list2) %}
    {{ return(0) }}
{% endmacro %}

{% macro assert_list_has_expected_length(list, expected_length) %}
    {% if list | length != expected_length %}
        {% do elementary.edr_log("FAILED: " ~ list ~ " has different length than expected " ~ expected_length) %}
        {{ return(1) }}
    {% endif %}
    {% do elementary.edr_log("SUCCESS: " ~ list  ~ " has length " ~ expected_length) %}
    {{ return(0) }}
{% endmacro %}
