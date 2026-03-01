{# Override for dremio__target_database – the upstream elementary package
   does not yet provide a Dremio dispatch, so the default falls back to
   target.dbname which is Undefined for the Dremio adapter.
   Dremio profiles use 'database' for the catalog/space name. #}
{% macro dremio__target_database() %}
    {% do return(target.database) %}
{% endmacro %}
