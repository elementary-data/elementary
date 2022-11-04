{% macro empty_alerts_models() %}
	{{ elementary.empty_table([
	  ('alert_id', 'string'),
	  ('unique_id', 'string'),
	  ('detected_at', 'string'),
	  ('database_name', 'string'),
	  ('materialization', 'string'),
	  ('path', 'string'),
	  ('schema_name', 'string'),
	  ('message', 'string'),
	  ('owners', 'string'),
	  ('tags', 'string'),
	  ('alias', 'string'),
	  ('status', 'string'),
	  ('full_refresh', 'boolean'),
	  ('alert_sent', 'boolean'),
	  ('original_path', 'string')
	]) }}
{% endmacro %}

{% macro empty_alerts_source_freshness() %}
	{{ elementary.empty_table([
	  ('alert_id', 'string'),
	  ('max_loaded_at', 'string'),
	  ('snapshotted_at', 'string'),
	  ('detected_at', 'string'),
	  ('max_loaded_at_time_ago_in_s', 'float'),
	  ('status', 'string'),
	  ('error', 'string'),
	  ('unique_id', 'string'),
	  ('database_name', 'string'),
	  ('schema_name', 'string'),
	  ('source_name', 'string'),
	  ('identifier', 'string'),
	  ('freshness_error_after', 'string'),
	  ('freshness_warn_after', 'string'),
	  ('freshness_filter', 'string'),
	  ('tags', 'string'),
	  ('meta', 'string'),
	  ('owner', 'string'),
	  ('package_name', 'string'),
	  ('path', 'string'),
	  ('alert_sent', 'boolean')
	]) }}
{% endmacro %}
