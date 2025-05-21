{%- macro get_test_results(days_back = 7, invocations_per_test = 720, disable_passed_test_metrics = false) -%}
    {{ return(adapter.dispatch('get_test_results', 'elementary_cli')(days_back, invocations_per_test, disable_passed_test_metrics)) }}
{%- endmacro -%}

{%- macro default__get_test_results(days_back = 7, invocations_per_test = 720, disable_passed_test_metrics = false) -%}
    {% set elementary_tests_allowlist_status = ['fail', 'warn'] if disable_passed_test_metrics else ['fail', 'warn', 'pass']  %}
    
    {% set select_test_results %}
        with test_results as (
            {{ elementary_cli.current_tests_run_results_query(days_back=days_back) }}
        ),

        ordered_test_results as (
            select
                *,
                {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('detected_at'), elementary.edr_current_timestamp(), 'day') }} as days_diff,
                {# When we split test into multiple test results, we want to have the same invocation order for the test results from the same run so we use rank. #}
                rank() over (partition by elementary_unique_id order by {{elementary.edr_cast_as_timestamp('detected_at')}} desc) as invocations_rank_index
            from test_results
        )

        select 
            test_results.id,
            test_results.invocation_id,
            test_results.test_execution_id,
            test_results.model_unique_id,
            test_results.test_unique_id,
            test_results.elementary_unique_id,
            {{elementary.edr_cast_as_timestamp('test_results.detected_at')}} as detected_at,
            test_results.database_name,
            test_results.schema_name,
            test_results.table_name,
            test_results.column_name,
            test_results.test_type,
            test_results.test_sub_type,
            test_results.test_results_description,
            test_results.test_description,
            test_results.original_path,
            test_results.package_name,
            test_results.owners,
            test_results.model_owner,
            test_results.tags,
            test_results.test_tags,
            test_results.model_tags,
            test_results.meta,
            test_results.model_meta,
            case when test_results.invocations_rank_index = 1 then test_results.test_results_query else NULL end as test_results_query,
            test_results.other,
            test_results.test_name,
            test_results.test_params,
            test_results.severity,
            test_results.status,
            test_results.execution_time,
            test_results.days_diff,
            test_results.invocations_rank_index,
            test_results.failures,
            test_results.result_rows
        from ordered_test_results as test_results
        where test_results.invocations_rank_index <= {{ invocations_per_test }}
        order by test_results.elementary_unique_id, test_results.invocations_rank_index desc
    {%- endset -%}

    {% set test_results = [] %}

    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% set ordered_test_results_relation = elementary.create_temp_table(elementary_database, elementary_schema, 'ordered_test_results', select_test_results) %}

    {% set test_results_agate_sql %}
        select * from {{ ordered_test_results_relation }}
    {% endset %}

    {% set valid_ids_query %}
        select distinct id 
        from {{ ordered_test_results_relation }}
        where invocations_rank_index = 1
    {% endset %}

    {% set test_results_agate = elementary.run_query(test_results_agate_sql) %}
    {% set test_result_rows_agate = elementary_cli.get_result_rows_agate(days_back, valid_ids_query) %}
    {% if not elementary.has_temp_table_support() %}
        {% do elementary.fully_drop_relation(ordered_test_results_relation) %}
    {% endif %}
    {% set tests = elementary.agate_to_dicts(test_results_agate) %}

    {% set filtered_tests = [] %}
    {% for test in tests %}
        {% set test_meta = fromjson(test.meta) %}
        {% if test_meta.get("elementary", {}).get("include", true) %}
            {% do filtered_tests.append(test) %}
        {% endif %}
    {% endfor %}

    {% for test in filtered_tests %}
        {% set test_rows_sample = none %}
        {% if test.invocations_rank_index == 1 %}
            {% set test_type = test.test_type %}
            {% set test_params = fromjson(test.test_params) %}
            {% set status = test.status | lower %}

            {%- if (test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status in elementary_tests_allowlist_status) -%}
                {% set test_rows_sample = elementary_cli.get_test_rows_sample(test.result_rows, test_result_rows_agate.get(test.id)) %}
                {# Dimension anomalies return multiple dimensions for the test rows sample, and needs to be handle differently. #}
                {# Currently we show only the anomalous for all of the dimensions. #}
                {% if test.test_sub_type == 'dimension' or test_params.dimensions %}
                    {% if test.test_sub_type == 'dimension' %}
                      {% set metric_name = 'row_count' %}
                    {% elif test_params.dimensions %}
                      {% set metric_name = test.test_sub_type %}
                    {% endif %}
                    {% set anomalous_rows = [] %}
                    {% set headers = [{'id': 'anomalous_value_timestamp', 'display_name': 'timestamp', 'type': 'date'}] %}
                    {% for row in test_rows_sample %}
                        {% set anomalous_row = {
                        'anomalous_value_timestamp': row['end_time'],
                        'anomalous_value_' ~ metric_name: row['value'],
                        'anomalous_value_average_' ~ metric_name: row['average'] | round(1)
                        } %}
                        {% set dimensions = row['dimension'].split('; ') %}
                        {% set diemsions_values = row['dimension_value'].split('; ') %}
                        {% for index in range(dimensions | length) %}
                            {% do anomalous_row.update({dimensions[index]: diemsions_values[index]}) %}
                        {% endfor %}
                        {% if loop.last %}
                            {# Adding dimensions to the headers #}
                            {% for index in range(dimensions | length) %}
                                {% do headers.append({'id': dimensions[index], 'display_name': dimensions[index], 'type': 'str'},) %}
                            {% endfor %}
                        {% endif %}
                        {% if row['is_anomalous'] %}
                          {% do anomalous_rows.append(anomalous_row) %}
                        {% endif %}
                    {% endfor %}
                    {# Adding the rest of the static headers (metrics headers) #}
                    {% do headers.extend([
                        {'id': 'anomalous_value_' ~ metric_name, 'display_name': ' '.join(metric_name.split('_')), 'type': 'int'},
                        {'id': 'anomalous_value_average_' ~ metric_name, 'display_name': 'average ' ~ ' '.join(metric_name.split('_')), 'type': 'int'}
                    ]) %}
                    {% set test_rows_sample = {
                        'headers': headers,
                        'test_rows_sample': anomalous_rows
                    } %}
                {% endif %}
            {%- endif -%}
        {% endif %}
        {# Adding sample data to test results #}
        {% do test.update({"sample_data": test_rows_sample}) %}
        {% do test_results.append(test) %}
    {%- endfor -%}

    {% do return(test_results) %}
{%- endmacro -%}

{%- macro clickhouse__get_test_results(days_back = 7, invocations_per_test = 720, disable_passed_test_metrics = false) -%}
    {% do elementary.run_query('drop table if exists ordered_test_results') %}
    {% set create_table_query %}
    CREATE TABLE ordered_test_results (
        id String,
        invocation_id String,
        test_execution_id String,
        model_unique_id String,
        test_unique_id String,
        elementary_unique_id String,
        detected_at DateTime,
        database_name String,
        schema_name String,
        table_name String,
        column_name String,
        test_type String,
        test_sub_type String,
        test_results_description String,
        test_description String,
        original_path String,
        package_name String,
        owners String,
        model_owner String,
        tags String,
        test_tags String,
        model_tags String,
        meta String,
        model_meta String,
        test_results_query String,
        other String,
        test_name String,
        test_params String,
        severity String,
        status String,
        execution_time Float64,
        days_diff Int32,
        invocations_rank_index UInt32,
        failures Int64,
        result_rows String
    )
    ENGINE = MergeTree()
    ORDER BY (elementary_unique_id, invocations_rank_index);
    {% endset %}
    {% do elementary.run_query(create_table_query) %}
    {% set insert_query %}
    INSERT INTO ordered_test_results
    SELECT
        etr.id,
        etr.invocation_id,
        etr.test_execution_id,
        etr.model_unique_id,
        etr.test_unique_id,
        CASE
            WHEN etr.test_type = 'schema_change' THEN etr.test_unique_id
            WHEN dt.short_name = 'dimension_anomalies' THEN etr.test_unique_id
            ELSE coalesce(etr.test_unique_id, 'None') || '.' || coalesce(nullif(etr.column_name, ''), 'None') || '.' || coalesce(etr.test_sub_type, 'None')
        END AS elementary_unique_id,
        etr.detected_at,
        etr.database_name,
        etr.schema_name,
        etr.table_name,
        etr.column_name,
        etr.test_type,
        etr.test_sub_type,
        etr.test_results_description,
        dt.description AS test_description,
        dt.original_path,
        dt.package_name,
        etr.owners,
        da.owner AS model_owner,
        etr.tags,
        dt.tags AS test_tags,
        da.tags AS model_tags,
        dt.meta,
        da.meta AS model_meta,
        etr.test_results_query,
        etr.other,
        CASE
            WHEN dt.short_name IS NOT NULL THEN dt.short_name
            ELSE etr.test_name
        END AS test_name,
        etr.test_params,
        etr.severity,
        etr.status,
        drr.execution_time,
        {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('etr.detected_at'), elementary.edr_current_timestamp(), 'day') }} AS days_diff,
        RANK() OVER (PARTITION BY elementary_unique_id ORDER BY etr.detected_at DESC) AS invocations_rank_index,
        etr.failures,
        etr.result_rows
    FROM {{ ref('elementary', 'elementary_test_results') }} etr
    JOIN {{ ref('elementary', 'dbt_tests') }} dt ON etr.test_unique_id = dt.unique_id
    LEFT JOIN (
        SELECT 
            min(detected_at) AS first_time_occurred,
            test_unique_id
        FROM {{ ref('elementary', 'elementary_test_results') }}
        GROUP BY test_unique_id
    ) first_occurred ON etr.test_unique_id = first_occurred.test_unique_id
    LEFT JOIN (
        SELECT unique_id, meta, tags, owner FROM {{ ref('elementary', 'dbt_models') }}
        UNION ALL
        SELECT unique_id, meta, tags, owner FROM {{ ref('elementary', 'dbt_sources') }}
    ) da ON etr.model_unique_id = da.unique_id
    LEFT JOIN {{ ref('elementary', 'dbt_run_results') }} drr ON etr.test_execution_id = drr.model_execution_id
    WHERE {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('etr.detected_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
    {% endset %}
    {% do elementary.run_query(insert_query) %}
    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% set ordered_test_results_relation = api.Relation.create(
        database=elementary_database,
        schema=elementary_schema,
        identifier='ordered_test_results',
        type='table'
    ) %}

    {% set test_results = [] %}

    {% set test_results_agate_sql %}
        select * from {{ ordered_test_results_relation }}
    {% endset %}

    {% set valid_ids_query %}
        select distinct id 
        from {{ ordered_test_results_relation }}
        where invocations_rank_index = 1
    {% endset %}

    {% set test_results_agate = elementary.run_query(test_results_agate_sql) %}
    {% set test_result_rows_agate = elementary_cli.get_result_rows_agate(days_back, valid_ids_query) %}
    {% if not elementary.has_temp_table_support() %}
        {% do elementary.fully_drop_relation(ordered_test_results_relation) %}
    {% endif %}
    {% set tests = elementary.agate_to_dicts(test_results_agate) %}

    {% set filtered_tests = [] %}
    {% for test in tests %}
        {% set test_meta = fromjson(test.meta) %}
        {% if test_meta.get("elementary", {}).get("include", true) %}
            {% do filtered_tests.append(test) %}
        {% endif %}
    {% endfor %}

    {% for test in filtered_tests %}
        {% set test_rows_sample = none %}
        {% if test.invocations_rank_index == 1 %}
            {% set test_type = test.test_type %}
            {% set test_params = fromjson(test.test_params) %}
            {% set status = test.status | lower %}

            {%- if (test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status in elementary_tests_allowlist_status) -%}
                {% set test_rows_sample = elementary_cli.get_test_rows_sample(test.result_rows, test_result_rows_agate.get(test.id)) %}
                {# Dimension anomalies return multiple dimensions for the test rows sample, and needs to be handle differently. #}
                {# Currently we show only the anomalous for all of the dimensions. #}
                {% if test.test_sub_type == 'dimension' or test_params.dimensions %}
                    {% if test.test_sub_type == 'dimension' %}
                      {% set metric_name = 'row_count' %}
                    {% elif test_params.dimensions %}
                      {% set metric_name = test.test_sub_type %}
                    {% endif %}
                    {% set anomalous_rows = [] %}
                    {% set headers = [{'id': 'anomalous_value_timestamp', 'display_name': 'timestamp', 'type': 'date'}] %}
                    {% for row in test_rows_sample %}
                        {% set anomalous_row = {
                        'anomalous_value_timestamp': row['end_time'],
                        'anomalous_value_' ~ metric_name: row['value'],
                        'anomalous_value_average_' ~ metric_name: row['average'] | round(1)
                        } %}
                        {% set dimensions = row['dimension'].split('; ') %}
                        {% set diemsions_values = row['dimension_value'].split('; ') %}
                        {% for index in range(dimensions | length) %}
                            {% do anomalous_row.update({dimensions[index]: diemsions_values[index]}) %}
                        {% endfor %}
                        {% if loop.last %}
                            {# Adding dimensions to the headers #}
                            {% for index in range(dimensions | length) %}
                                {% do headers.append({'id': dimensions[index], 'display_name': dimensions[index], 'type': 'str'},) %}
                            {% endfor %}
                        {% endif %}
                        {% if row['is_anomalous'] %}
                          {% do anomalous_rows.append(anomalous_row) %}
                        {% endif %}
                    {% endfor %}
                    {# Adding the rest of the static headers (metrics headers) #}
                    {% do headers.extend([
                        {'id': 'anomalous_value_' ~ metric_name, 'display_name': ' '.join(metric_name.split('_')), 'type': 'int'},
                        {'id': 'anomalous_value_average_' ~ metric_name, 'display_name': 'average ' ~ ' '.join(metric_name.split('_')), 'type': 'int'}
                    ]) %}
                    {% set test_rows_sample = {
                        'headers': headers,
                        'test_rows_sample': anomalous_rows
                    } %}
                {% endif %}
            {%- endif -%}
        {% endif %}
        {# Adding sample data to test results #}
        {% do test.update({"sample_data": test_rows_sample}) %}
        {% do test_results.append(test) %}
    {%- endfor -%}

    {% do return(test_results) %}
{%- endmacro -%}