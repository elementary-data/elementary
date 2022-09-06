{% macro assert_equal_values(actual_object, expected_object) %}
{% if not execute %}

    {# pass #}

{% elif actual_object != expected_object %}

    {% set msg %}
    Expected did not match actual

    -----------
    Actual:
    -----------
    --->{{ actual_object }}<---

    -----------
    Expected:
    -----------
    --->{{ expected_object }}<---

    {% endset %}

    {{ log(msg, info=True) }}

    select 'fail'

{% else %}

    select 'ok' {{ limit_zero() }}

{% endif %}
{% endmacro %}