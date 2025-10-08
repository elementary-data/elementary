with training as (
    select * from {{ ref('string_column_anomalies_training') }}
),

{% if var("stage") == "validation" %}
validation as (
    select * from {{ ref('string_column_anomalies_validation') }}
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
        min_length,
        max_length,
        average_length,
        missing_count,
        missing_percent
     from source
)

select * from final
