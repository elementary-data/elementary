{% macro empty_alerts_models() %}
	{{ elementary.empty_table([
	  ('alert_id', 'string'),
	  ('unique_id', 'string'),
	  ('detected_at', 'timestamp'),
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
	  ('original_path', 'string'),
	  ('suppression_status', 'string'),
	  ('sent_at', 'string')
	]) }}
{% endmacro %}

{% macro empty_alerts_source_freshness() %}
	{{ elementary.empty_table([
	  ('alert_id', 'string'),
	  ('max_loaded_at', 'string'),
	  ('snapshotted_at', 'string'),
	  ('detected_at', 'timestamp'),
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
	  ('alert_sent', 'boolean'),
	  ('suppression_status', 'string'),
	  ('sent_at', 'string')
	]) }}
{% endmacro %}


{% macro empty_alerts_v2() %}
    {{ elementary.empty_table([
		('alert_id', 'long_string'),
		('alert_class_id', 'long_string'),
		('type', 'string'),
		('detected_at','timestamp'),
    	('created_at','timestamp'),
    	('updated_at','timestamp'),
		('status','string'),
		('data', 'long_string'),
		('sent_at', 'timestamp')
    ]) }}
{% endmacro %}