{% macro get_elementary_database_and_schema() %}
    {{ elementary.edr_log(elementary.get_package_database_and_schema('elementary')) }}
{% endmacro %}
