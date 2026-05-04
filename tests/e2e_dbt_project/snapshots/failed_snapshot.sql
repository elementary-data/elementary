{% snapshot failed_snapshot() %}

{# target_schema is required by dbt; reuse target.schema so this lands in the per-run CI schema. #}
{{
    config(
      target_schema=target.schema,
      unique_key='unique_id',
      strategy='timestamp',
      updated_at='generated_at',
    )
}}
    SELECT FAILED_SNAPSHOT
{% endsnapshot %}
