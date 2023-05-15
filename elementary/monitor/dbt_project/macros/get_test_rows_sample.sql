{#
  Sources of parameters:
  legacy_result_rows: elementary_test_results.result_rows
  result_rows_agate: test_result_rows
#}
{%- macro get_test_rows_sample(legacy_result_rows, result_rows_agate) -%}

    {% set result_rows = [] %}
    {% if legacy_result_rows is defined and legacy_result_rows is not none %}
        {% set result_rows = fromjson(legacy_result_rows) %}
    {% elif result_rows_agate %}
        {% set result_row_column = result_rows_agate.columns["result_row"] %}
        {% for result_row in result_row_column %}
            {% do result_rows.append(fromjson(result_row)) %}
        {% endfor %}
    {% endif %}

    {% do return(result_rows) %}
{%- endmacro -%}
