{%- macro get_tests() -%}
    {% set dbt_tests_relation = ref('elementary', 'dbt_tests') %}
    {% set dbt_models_relation = ref('elementary', 'dbt_models') %}
    {% set dbt_sources_relation = ref('elementary', 'dbt_sources') %}
    {% set test_results_relation = ref('elementary', 'elementary_test_results') %}

    {%- if elementary.relation_exists(dbt_tests_relation) -%}
        {% set get_tests_query %}
            with tests as (
                select * from {{ dbt_tests_relation }}
            ),
            
            test_results as (
                select * from {{ test_results_relation }}
            ),

            models as (
                select * from {{ dbt_models_relation }}
            ),
            
            sources as (
                select * from {{ dbt_sources_relation }}
            ),

            nodes as (
                select
                    models.unique_id as unique_id,
                    models.name as name,
                    models.meta,
                    models.tags,
                    'model' as type
                from models
                union all
                select
                    sources.unique_id as unique_id,
                    sources.name as name,
                    sources.meta,
                    sources.tags,
                    'source' as type
                from sources
            ),

            test_results_times as (
                select
                    test_unique_id,
                    max(detected_at) as last_detected_at,
                    min(detected_at) as first_detected_at
                from test_results
                group by test_unique_id
            ),

            last_test_results as (
                select
                    test_results.test_unique_id,
                    test_results.detected_at,
                    test_results.status,
                    test_results.test_type,
                    test_results.test_sub_type
                from test_results
                join test_results_times 
                    on test_results_times.test_unique_id = test_results.test_unique_id
                    and test_results_times.last_detected_at = test_results.detected_at
            )

            select
                tests.unique_id as unique_id,
                tests.parent_model_unique_id as model_unique_id,
                tests.database_name,
                tests.schema_name,
                nodes.name as model_name,
                tests.test_column_name as column_name,
                tests.name as name,
                tests.description,
                tests.package_name,
                tests.original_path,
                tests.test_params,
                tests.meta as meta,
                nodes.meta as model_meta,
                tests.tags,
                nodes.tags as model_tags,
                tests.type as type,
                last_test_results.test_type,
                last_test_results.test_sub_type,
                test_results_times.first_detected_at as created_at,
                last_test_results.detected_at as latest_run_time,
                last_test_results.status as latest_run_status
            from tests
            left join test_results_times on tests.unique_id = test_results_times.test_unique_id
            left join last_test_results on tests.unique_id = last_test_results.test_unique_id
            left join nodes on tests.parent_model_unique_id = nodes.unique_id
        {% endset %}
        {% set tests_agate = run_query(get_tests_query) %}
        {% do return(elementary.agate_to_dicts(tests_agate)) %}
    {%- endif -%}
{%- endmacro -%}
