with
    metrics as (
        select * from {{ ref("data_monitoring_metrics") }}
        -- where ...
    )
    latest_metrics as (
        select
            id,
            full_table_name,
            column_name,
            metric_name,
            metric_value,
            source_value,
            bucket_start,
            bucket_end,
            bucket_duration_hours,
            updated_at,
            dimension,
            dimension_value,
            row_number() over (partition by id order by updated_at desc) as row_number
        from union_metrics
    )
