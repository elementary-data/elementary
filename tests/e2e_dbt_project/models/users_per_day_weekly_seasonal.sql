with training as (
    select * from {{ source('training', 'users_per_day_weekly_seasonal_training') }}
),

validation as (
    select * from {{ source('validation', 'users_per_day_weekly_seasonal_validation') }}
),

source as (
    select * from training
    union all
    select * from validation
),

 final as (
     select
         updated_at,
         user_id
     from source
 )

select * from final
