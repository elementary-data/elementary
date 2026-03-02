{#
  Override dbt-data-reliability's dremio__edr_cast_as_timestamp macro.
  Dremio's Gandiva (Arrow execution engine) cannot parse ISO 8601 timestamps
  with the 'Z' UTC timezone suffix (e.g. '2026-03-02T22:50:42.101Z').
  This override strips the 'Z' suffix before casting to TIMESTAMP.
#}

{%- macro dremio__edr_cast_as_timestamp(timestamp_field) -%}
    cast(REGEXP_REPLACE(REGEXP_REPLACE({{ timestamp_field }}, '(\.\d{3})\d+', '$1'), 'Z$', '') as {{ elementary.edr_type_timestamp() }})
{%- endmacro -%}
