{% macro generate_alert_object(alert_id, alert_class_id, type, detected_at, created_at, data) %}
    {% set alert_object = {
        'alert_id': alert_id,
        'alert_class_id': alert_class_id,
		'type': type,
		'detected_at': detected_at,
    	'created_at': created_at,
    	'updated_at': created_at,
		'status': 'pending',
		'data': tojson(data),
		'sent_at': none
    } %}
    {% do return(alert_object) %}
{% endmacro %}