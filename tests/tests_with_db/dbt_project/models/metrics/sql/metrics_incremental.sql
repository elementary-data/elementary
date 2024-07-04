{{ config(materialized="incremental") }}

select * from {{ source("test_data", "metrics_seed2") }}