{% macro get_source_freshness_results(days_back = 7, invocations_per_test = 720) %}
    {% set source_freshness_results_relation = ref('elementary', 'dbt_source_freshness_results') %}
    {% set error_after_column_exists = elementary.column_exists_in_relation(source_freshness_results_relation, 'error_after') %}

    {% set sources_relation = ref('elementary', 'dbt_sources') %}
    {% set freshness_description_column_exists = elementary.column_exists_in_relation(sources_relation, 'freshness_description') %}


    {% set select_source_freshness_results %}
        with dbt_source_freshness_results as (
            select
                *,
                {{ elementary_cli.normalized_source_freshness_status()}},
                rank() over (partition by unique_id order by generated_at desc) as invocations_rank_index
            from {{ ref('elementary', 'dbt_source_freshness_results') }}
            {% if days_back %}
                where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('generated_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
            {% endif %}
        ),

        dbt_sources as (
            select * from {{ ref('elementary', 'dbt_sources') }}
        )

        select
            freshness.source_freshness_execution_id,
            freshness.unique_id,
            freshness.max_loaded_at,
            freshness.generated_at,
            freshness.status as original_status,
            freshness.normalized_status,
            {# backwards compatibility - these fields were added together #}
            {% if error_after_column_exists %}
                freshness.error_after,
                freshness.warn_after,
                freshness.filter,
            {% endif %}
            freshness.max_loaded_at_time_ago_in_s,
            freshness.snapshotted_at,
            freshness.invocation_id,
            freshness.error,
            freshness.invocations_rank_index,
            sources.database_name,
            sources.schema_name,
            sources.source_name,
            sources.name as table_name,
            sources.loaded_at_field,
            sources.meta,
            sources.tags,
            sources.owner as owners,
            sources.freshness_error_after as source_freshness_error_after,
            sources.freshness_warn_after as source_freshness_warn_after,
            sources.freshness_filter as source_freshness_filter,
            sources.relation_name,
            {# backwards compatibility #}
            {% if freshness_description_column_exists %}
                sources.freshness_description
            {% else %}
                'dbt source freshness validates if the data in a table is not updated by calculating if the time elapsed between the test execution to the latest record is above an acceptable SLA threshold.' as freshness_description
            {% endif %}
        from dbt_source_freshness_results freshness
        join dbt_sources sources on freshness.unique_id = sources.unique_id
        where invocations_rank_index <= {{ invocations_per_test }}
        order by freshness.unique_id, invocations_rank_index desc
    {% endset %}

    {% set results_agate = run_query(select_source_freshness_results) %}
    {% set source_freshness_results_dicts = elementary.agate_to_dicts(results_agate) %}
    {% set source_freshness_results = [] %}
    {% for source_freshness_result_dict in source_freshness_results_dicts %}
        {% set error_after = source_freshness_result_dict.get('error_after') %}
        {% set warn_after = source_freshness_result_dict.get('warn_after') %}
        {% set filter = source_freshness_result_dict.get('filter') %}

        {# we want to use the normalized status for all usages #}
        {% set status = source_freshness_result_dict.get('normalized_status') %}

        {% set result_dict = {'source_freshness_execution_id': source_freshness_result_dict.get('source_freshness_execution_id'),
                                 'unique_id': source_freshness_result_dict.get('unique_id'),
                                 'max_loaded_at': source_freshness_result_dict.get('max_loaded_at'),
                                 'generated_at': source_freshness_result_dict.get('generated_at'),
                                 'execute_started_at': source_freshness_result_dict.get('execute_started_at'),
                                 'status': status,
                                 'original_status': source_freshness_result_dict.get('original_status'),
                                 'error': source_freshness_result_dict.get('error'),
                                 'invocation_id': source_freshness_result_dict.get('invocation_id'),
                                 'database_name': source_freshness_result_dict.get('database_name'),
                                 'schema_name': source_freshness_result_dict.get('schema_name'),
                                 'source_name': source_freshness_result_dict.get('source_name'),
                                 'table_name': source_freshness_result_dict.get('table_name'),
                                 'test_type': 'source_freshness',
                                 'test_sub_type': 'freshness',
                                 'loaded_at_field': source_freshness_result_dict.get('loaded_at_field'),
                                 'meta': source_freshness_result_dict.get('meta'),
                                 'owners': source_freshness_result_dict.get('owner'),
                                 'tags': source_freshness_result_dict.get('tags'),
                                 'error_after': error_after if error_after is not none else source_freshness_result_dict.get('source_freshness_error_after'),
                                 'warn_after': warn_after if warn_after is not none else source_freshness_result_dict.get('source_freshness_warn_after'),
                                 'filter': filter if filter is not none else source_freshness_result_dict.get('freshness_filter'),
                                 'relation_name': source_freshness_result_dict.get('relation_name'),
                                 'invocations_rank_index': source_freshness_result_dict.get('invocations_rank_index'),
                                 'max_loaded_at_time_ago_in_s': source_freshness_result_dict.get('max_loaded_at_time_ago_in_s'),
                                 'snapshotted_at': source_freshness_result_dict.get('snapshotted_at'),
                                 'freshness_description': source_freshness_result_dict.get('freshness_description')
                                } %}

        {% do source_freshness_results.append(result_dict) %}
    {% endfor %}
    {% do return(source_freshness_results) %}
{% endmacro %}
