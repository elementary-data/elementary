{% macro get_singular_tests() %}
    {% set get_tests_query %}
        select
          unique_id,
          name,
          original_path,
          package_name,
          tags
        from {{ ref('elementary', 'dbt_tests') }}
        where type = 'singular'
    {% endset %}
    {% set tests_agate = elementary.run_query(get_tests_query) %}
    {% do return(elementary.agate_to_dicts(tests_agate)) %}
{% endmacro %}
