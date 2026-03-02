{#
  Override dbt-data-reliability's dremio__edr_cast_as_timestamp macro.
  Dremio's Gandiva (Arrow execution engine) cannot parse ISO 8601 timestamps:
  1. The 'Z' UTC timezone suffix is rejected as an unknown zone
  2. The 'T' date-time separator is not recognized (needs space)
  This override normalizes ISO 8601 format to 'YYYY-MM-DD HH:MM:SS.sss'.
#}

{%- macro dremio__edr_cast_as_timestamp(timestamp_field) -%}
    cast(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE({{ timestamp_field }}, 'T', ' '), '(\.\d{3})\d+', '$1'), 'Z$', '') as {{ elementary.edr_type_timestamp() }})
{%- endmacro -%}
