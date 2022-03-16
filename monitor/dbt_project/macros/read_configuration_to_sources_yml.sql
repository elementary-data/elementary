{% macro read_configuration_to_sources_yml() %}
    {% if execute %}
        {%- set table_monitors_config = adapter.get_relation(database=database,
                                                             schema=schema,
                                                             identifier='table_monitors_config') -%}
        {%- set column_monitors_config = adapter.get_relation(database=database,
                                                              schema=schema,
                                                              identifier='column_monitors_config') -%}
        {% set table_monitors_config_query %}
            select * from {{ table_monitors_config.include(database=True, schema=True, identifier=True) }} table_config left join
                          {{ column_monitors_config.include(database=True, schema=True, identifier=True) }} column_config on
                          (table_config.database_name = column_config.database_name and table_config.schema_name = column_config.schema_name
                           and table_config.table_name = column_config.table_name)
                where table_monitored = true
        {% endset %}
        {% set table_configs = run_query(table_monitors_config_query) %}
        {% set tables_dict = {} %}
        {% for table_config in table_configs.rows %}
            {% set table_database_name = table_config['DATABASE_NAME'] %}
            {% set table_schema_name = table_config['SCHEMA_NAME'] %}
            {% set table_name = table_config['TABLE_NAME'] %}
            {% set table_monitors_list = table_config['TABLE_MONITORS'] %}
            {% set monitor_schema_changes = table_config['MONITOR_SCHEMA_CHANGES'] %}
            {% set columns_monitored = table_config['COLUMNS_MONITORED'] %}
            {% set column_name = table_config['COLUMN_NAME'] %}
            {% set column_monitors_list = table_config['COLUMN_MONITORS'] %}
            {% set schema_changes_test_enabled = False %}
            {% if table_monitors_list %}
                {% set table_monitors_list = fromjson(table_monitors_list) %}
            {% endif %}
            {% if column_monitors_list %}
                {% set column_monitors_list = fromjson(column_monitors_list) %}
            {% endif %}

            {% set table_key = table_database_name ~ '.' ~ table_schema_name ~ '.' ~ table_name %}
            {% set table_dict = tables_dict.get(table_key) %}
            {% if not table_dict %}
                {% set table_dict = {'name': table_name,
                     'tests': [],
                     'columns': []
                     } %}
                {% for table_monitors in table_monitors_list %}
                    {% if 'schema_changes' in table_monitors %}
                        {% do table_dict['tests'].append({'elementary.schema_changes': {'tags': ['elementary']}}) %}
                    {% endif %}
                    {% do table_dict['tests'].append({'elementary.table_anomalies': {'table_anomalies': table_monitors,
                                                                                     'tags': ['elementary']}}) %}
                {% endfor %}
            {% endif %}
            {% if column_name and columns_monitored %}
                {% if column_name ==  '__ALL_COLUMNS__' %}
                    {% for column_monitors in column_monitors_list %}
                        {% do table_dict['tests'].append({'elementary.all_columns_anomalies':
                                                        {'column_anomalies': column_monitors, 'tags': ['elementary']}}) %}
                    {% endfor %}
                {% else %}
                    {% set column_dict = {'name': column_name, 'tests': []} %}
                    {% for column_monitors in column_monitors_list %}
                        {% do column_dict['tests'].append({'elementary.column_anomalies':
                                                        {'column_anomalies': column_monitors, 'tags': ['elementary']}}) %}
                    {% endfor %}
                {% endif %}
            {% endif %}
            {% do tables_dict.update({table_key: table_dict}) %}
        {% endfor %}
        {% set sources_dict = {} %}
        {% for table_key, table_dict in tables_dict.items() %}
            {% set table_database_name, table_schema_name, table_name = table_key.split('.') %}
            {% set source_key = table_database_name ~ '.' ~ table_schema_name %}
            {% set source_dict = sources_dict.get(source_key) %}
            {% if not source_dict %}
                {% set source_dict = {'name': table_schema_name, 'database': table_database_name, 'tables': []} %}
            {% endif %}
            {% do source_dict['tables'].append(table_dict) %}
            {% do sources_dict.update({source_key: source_dict}) %}
        {% endfor %}
        {% set sources_yml_dict = {'version': 2, 'sources': sources_dict.values() | list} %}
        {% set sources_yml = toyaml(sources_yml_dict) %}
        {% do elementary.edr_log(sources_yml) %}
    {% endif %}
{% endmacro %}
