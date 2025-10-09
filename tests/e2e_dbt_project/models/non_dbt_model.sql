{{ config(materialized='non_dbt') }}
    SELECT 1
-- depends_on: {{ ref('one') }}