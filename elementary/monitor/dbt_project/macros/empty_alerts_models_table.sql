{% macro empty_alerts_models() %}
	{{ elementary.empty_table([('alert_id', 'string'), ('unique_id', 'string'), ('detected_at', 'string'), ('database_name', 'string'), ('materialization', 'string'), ('path', 'string'), ('schema_name', 'string'), ('message', 'string'), ('owners', 'string'), ('tags', 'string'), ('alias', 'string'), ('status', 'string'), ('full_refresh', 'boolean'), ('alert_sent', 'boolean'), ('original_path', 'string')]) }}
{% endmacro %}
