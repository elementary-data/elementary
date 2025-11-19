with dbt as (
    select * from {{ ref('alerts_dbt_tests') }}
),
{%- if target.type != 'databricks' %}
schema_changes as (
    select * from {{ ref('alerts_schema_changes') }}
),
{%- endif %}
anomalies as (
    select * from {{ ref('alerts_anomaly_detection') }}
)
select * from dbt
union all
select * from anomalies
{%- if target.type != 'databricks' %}
union all
select * from schema_changes
{%- endif %}
