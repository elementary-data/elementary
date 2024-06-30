{{ config(materialized="view") }}

select * from {{ source("test_data", "metrics_seed1") }}
union all
select * from {{ source("test_data", "metrics_seed2") }}
