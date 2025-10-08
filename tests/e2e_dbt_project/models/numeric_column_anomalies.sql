with training as (
    select * from {{ ref('numeric_column_anomalies_training') }}
),

{% if var("stage") == "validation" %}
 validation as (
     select * from {{ ref('numeric_column_anomalies_validation') }}
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
         occurred_at,
         min,
         max,
         zero_count,
         zero_percent,
         average,
         standard_deviation,
         variance,
         sum
     from source
 )

select * from final
