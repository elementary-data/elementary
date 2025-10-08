{# Logging the wanted config var as an elementary log (using elementary.edr_log) #}
{# The dbtRunner catch this log when executed with run_operation #}
{# This is used for accessing the integration tests vars #}
{% macro return_config_var(var_name) %}
    {{ elementary.edr_log(elementary.get_config_var(var_name)) }}
{% endmacro %}
