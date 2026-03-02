{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id'],
    on_schema_change = 'sync_all_columns',
    table_type=elementary.get_default_table_type(),
    incremental_strategy=elementary.get_default_incremental_strategy(),
    post_hook = "{{ elementary_cli.populate_alerts_table(days_back=var('days_back', 1)) }}"
  )
}}

-- depends_on: {{ ref('dbt_tests') }}
-- depends_on: {{ ref('elementary_test_results') }}
-- depends_on: {{ ref('test_result_rows', package='elementary') }}

-- depends_on: {{ ref('dbt_models') }}
-- depends_on: {{ ref('dbt_snapshots', package='elementary') }}
-- depends_on: {{ ref('model_run_results') }}
-- depends_on: {{ ref('snapshot_run_results') }}

-- depends_on: {{ ref('dbt_sources') }}
-- depends_on: {{ ref('dbt_source_freshness_results', package='elementary') }}

-- depends_on: {{ ref('dbt_seeds') }}

-- depends_on: {{ ref('dbt_invocations', package='elementary') }}
-- depends_on: {{ ref('dbt_run_results', package='elementary') }}

-- backwards compatibility
-- depends_on: {{ ref('elementary_cli', 'alerts') }}
-- depends_on: {{ ref('elementary_cli', 'alerts_models') }}
-- depends_on: {{ ref('elementary_cli', 'alerts_source_freshness') }}

{{ elementary_cli.empty_alerts_v2() }}
