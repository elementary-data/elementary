{{
  config(
    materialized='incremental',
    transient=False,
    unique_key='unique_id',
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
    on_schema_change='sync_all_columns',
  )
}}

{{ elementary_cli.empty_elementary_exposures() }}
