{{
    config(
        materialized='table',
        tags=var('one_tags', []),
        meta={'owner': var('one_owner', 'egk')}
    )
}}

SELECT 1/0 AS fail_value
