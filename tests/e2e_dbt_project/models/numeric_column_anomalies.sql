with training as (
    select * from {{ ref('numeric_column_anomalies_training') }}
),

validation as (
    select * from {{ ref('numeric_column_anomalies_validation') }}
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
         min_val,
         max_val,
         zero_count,
         zero_percent,
         average,
         standard_deviation,
         variance,
         sum_val
     from source
 )

select * from final
