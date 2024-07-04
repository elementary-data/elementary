{{ config(materialized="table") }}

select * from {{ source("test_data", "metrics_seed1") }}
