with

source as (
    select *
    from {{ ref('data_deduplicate') }}
    where user_id = 1
),

deduped as (

    {{
        dbt_utils.deduplicate(
            ref('data_deduplicate'),
            group_by='user_id',
            order_by='version desc',
            relation_alias='source',
        ) | indent
    }}

)

select * from deduped
