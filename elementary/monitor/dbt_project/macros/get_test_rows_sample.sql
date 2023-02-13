{%- macro get_test_rows_sample(result_rows, result_rows_agate, test_type, test_query, results_sample_limit=5) -%}
    {% set should_limit_sample = test_type == 'dbt_test' %}

    {% if result_rows is defined and result_rows is not none %}
        {% set result_rows = fromjson(result_rows) %}
        {% if should_limit_sample %}
            {% do return(result_rows[:results_sample_limit]) %}
        {% endif %}
        {% do return(result_rows) %}
    {% endif %}

    {% if result_rows_agate %}
        {% set test_rows_sample = [] %}
        {% set result_row_column = result_rows_agate.columns["result_row"] %}
        {% if should_limit_sample %}
            {% set result_row_column = result_row_column[:results_sample_limit] %}
        {% endif %}
        {% for result_row in result_row_column %}
            {% do test_rows_sample.append(fromjson(result_row)) %}
        {% endfor %}
        {{ return(test_rows_sample) }}
    {% endif %}

  {% set query %}
    with test_results as (
      {{ test_query }}
    )
    select * from test_results {% if should_limit_sample %} limit {{ results_sample_limit }} {% endif %}
  {% endset %}
  {% do return(elementary.agate_to_dicts(elementary.run_query(query))) %}
{%- endmacro -%}
