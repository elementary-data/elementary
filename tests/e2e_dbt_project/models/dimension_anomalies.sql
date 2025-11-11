with training as (
    select * from {{ ref('dimension_anomalies_training') }}
),

validation as (
    select * from {{ ref('dimension_anomalies_validation') }}
),

source as (
    select * from training
    union all
    select * from validation
),

 final as (
     select
         updated_at,
         platform,
         version,
         user_id
     from source
 )

select * from final
