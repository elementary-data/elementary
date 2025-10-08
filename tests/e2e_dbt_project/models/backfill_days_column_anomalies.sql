with training as (
    select * from {{ ref('backfill_days_column_anomalies_training') }}
),

validation as (
    select * from {{ ref('backfill_days_column_anomalies_validation') }}
),

source as (
    select * from training
    union all
    select * from validation
),

final as (
    select
        updated_at,
        occurred_at,
        min_length
    from source
)

select * from final
