{% snapshot failed_snapshot() %}

{{
    config(
      target_schema='snapshots',
      unique_key='unique_id',
      strategy='timestamp',
      updated_at='generated_at',
    )
}}
    SELECT FAILED_SNAPSHOT
{% endsnapshot %}
