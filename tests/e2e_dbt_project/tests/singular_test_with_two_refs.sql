with min_len_issues as (
    select null_count_int as min_issue from {{ ref('any_type_column_anomalies') }} where null_count_int < 100
),

min_issues as (
    select min_val as min_issue from {{ ref('numeric_column_anomalies') }} where min_val < 100
),

all_issues as (
        select * from min_len_issues
        union all
        select * from min_issues
)

select * from all_issues
