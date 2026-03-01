{#
  Override dbt-dremio's seed materialization to support large seeds via batched inserts.
  Dremio's REST API rejects SQL that is too large or complex, so we split the VALUES
  clause into batches of BATCH_SIZE rows and issue separate INSERT INTO statements.
#}

{% macro dremio__select_csv_rows_batch(model, agate_table, start_idx, end_idx) %}
{%- set column_override = model['config'].get('column_types', {}) -%}
{%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
{%- set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) -%}
  select
    {% for col_name in agate_table.column_names -%}
      {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
      {%- set type = column_override.get(col_name, inferred_type) -%}
      {%- set column_name = (col_name | string) -%}
      cast({{ adapter.quote_seed_column(column_name, quote_seed_column) }} as {{ type }})
        as {{ adapter.quote_seed_column(column_name, quote_seed_column) }}{%- if not loop.last -%}, {%- endif -%}
    {% endfor %}
  from
    (values
      {% for idx in range(start_idx, end_idx) %}
        {%- set row = agate_table.rows[idx] -%}
        ({%- for value in row -%}
          {% if value is not none %}
            {{ "'" ~ (value | string | replace("'", "''")) ~ "'" }}
          {% else %}
            cast(null as varchar)
          {% endif %}
          {%- if not loop.last%},{%- endif %}
        {%- endfor -%})
        {%- if not loop.last%},{%- endif %}
      {% endfor %}) temp_table ( {{ cols_sql }} )
{% endmacro %}


{% materialization seed, adapter = 'dremio' %}

  {%- set identifier = model['alias'] -%}
  {%- set format = config.get('format', validator=validation.any[basestring]) or 'iceberg' -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks) }}

  {% if old_relation is not none -%}
    {{ adapter.drop_relation(old_relation) }}
  {%- endif %}

  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}
  {%- set num_rows = (agate_table.rows | length) -%}

  {# Batch size: keep each SQL statement small enough for Dremio's REST API #}
  {%- set batch_size = 500 -%}
  {%- set first_end = [batch_size, num_rows] | min -%}

  {# Create table with first batch #}
  {%- set first_batch_sql = dremio__select_csv_rows_batch(model, agate_table, 0, first_end) -%}
  {% call statement('effective_main') -%}
    {{ create_table_as(False, target_relation, first_batch_sql) }}
  {%- endcall %}

  {# Insert remaining batches #}
  {% for batch_start in range(batch_size, num_rows, batch_size) %}
    {%- set batch_end = [batch_start + batch_size, num_rows] | min -%}
    {%- set batch_sql = dremio__select_csv_rows_batch(model, agate_table, batch_start, batch_end) -%}
    {% call statement('insert_batch_' ~ batch_start) -%}
      INSERT INTO {{ target_relation }}
      {{ batch_sql }}
    {%- endcall %}
  {% endfor %}

  {% call noop_statement('main', 'CREATE ' ~ num_rows, 'CREATE', num_rows) %}
    -- batched seed insert ({{ num_rows }} rows in {{ (num_rows / batch_size) | round(0, 'ceil') | int }} batches)
  {% endcall %}

  {{ refresh_metadata(target_relation, format) }}

  {{ apply_twin_strategy(target_relation) }}

  {% do persist_docs(target_relation, model) %}

  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
