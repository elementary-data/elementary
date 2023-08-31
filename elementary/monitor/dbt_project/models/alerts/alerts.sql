{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id'],
    on_schema_change = 'append_new_columns'
  )
}}

with all_tests as (
    select id as alert_id,
        data_issue_id,
        test_execution_id,
        test_unique_id,
        model_unique_id,
        detected_at,
        database_name,
        schema_name,
        table_name,
        column_name,
        test_type as alert_type,
        test_sub_type as sub_type,
        test_results_description as alert_description,
        owners,
        tags,
        test_results_query as alert_results_query,
        other,
        test_name,
        test_short_name,
        test_params,
        severity,
        case 
            when lower(status) = 'pass' then 'resolved'
            else status
        end as status,
        lower(lag(status) over (
            partition by coalesce(test_unique_id, 'None') || '.' || coalesce(column_name, 'None') || '.' || coalesce(test_sub_type, 'None') 
            order by detected_at
        )) as previous_status,
        result_rows
    from {{ ref("elementary_test_results") }}
    {#wouldnt it be more performant to put the incremental block here?#}
)

, failed_tests as (
    select * 
    from all_tests
    where status != 'resoved'
)

, resolved_tests as (
    select *
    from all_tests
    where status = 'resolved'
      and previous_status in ('error', 'fail', 'warn')
)

select 
    *,
    false as alert_sent,  {# backwards compatibility #}
    'pending' as suppression_status,
    {{ elementary.edr_cast_as_string('NULL') }} as sent_at
from (
    select * from failed_tests
    union all
    select * from resolved_tests
) as all_test_alerts
{%- if is_incremental() %}
    {{ get_new_alerts_where_clause(this) }}
{%- endif %}
