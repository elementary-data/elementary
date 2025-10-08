with training as (
    select * from {{ ref('dimension_anomalies_training') }}
),

{% if var("stage") == "validation" %}
 validation as (
     select * from {{ ref('dimension_anomalies_validation') }}
 ),

 source as (
     select * from training
     union all
     select * from validation
 ),
{% else %}
 source as (
     select * from training
 ),
{% endif %}

 final as (
     select
         updated_at,
         platform,
         version,
         user_id
     from source
 )

select * from final
