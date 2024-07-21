{% macro empty_elementary_exposures() %}
    {% set columns = [('unique_id', 'string'),
                      ('name', 'string'),
                      ('maturity', 'string'),
                      ('type', 'string'),
                      ('owner_email', 'string'),
                      ('owner_name', 'string'),
                      ('url', 'long_string'),
                      ('depends_on_macros', 'long_string'),
                      ('depends_on_nodes', 'long_string'),
                      ('depends_on_columns', 'long_string'),
                      ('description', 'long_string'),
                      ('tags', 'long_string'),
                      ('meta', 'long_string'),
                      ('package_name', 'string'),
                      ('original_path', 'long_string'),
                      ('path', 'string'),
                      ('generated_at', 'string'),
                      ('created_at', 'string'),
                      ('metadata_hash', 'string'),
                      ('label', 'string'),
                      ('raw_queries', 'long_string'),
                     ] %}
    {{ elementary.empty_table(columns) }}
{% endmacro %}

