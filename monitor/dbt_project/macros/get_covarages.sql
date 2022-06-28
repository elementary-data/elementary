{% macro get_covarages() %}
    {% set dbt_tests_relation = ref('elementary', 'dbt_tests') %}
    {%- if elementary.relation_exists(dbt_tests_relation) -%}
        {% set get_coverages_query %}
            with dbt_tests as (
                select
                    parent_model_unique_id,
                    test_column_name
                from {{ dbt_tests_relation }}
            )

            select 
                parent_model_unique_id as model_unique_id,
                SUM(
                    case 
                        when test_column_name is not null then 1
                        else 0
                        end
                ) as column_tests,
                 SUM(
                    case
                        when test_column_name is null then 1
                        else 0
                        end 
                )as table_tests
            from dbt_tests
            group by parent_model_unique_id
        {% endset %}
        {% set covarage_agate = run_query(get_coverages_query) %}
        {% set covarage_json = elementary.agate_to_json(covarage_agate) %}
        {% do elementary.edr_log(covarage_json) %}
    {%- endif -%}
{% endmacro %}
