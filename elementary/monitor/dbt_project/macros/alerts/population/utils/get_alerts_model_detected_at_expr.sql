{% macro get_alerts_model_detected_at_expr(existing_relation) %}
    {# Backwards compatibility - do not change the data type of detected_at if it is string in the
       existing relation #}

    {% set col = elementary.get_column_in_relation(existing_relation, 'detected_at') or
                 elementary.get_column_in_relation(existing_relation, 'DETECTED_AT') %}
    {% set col_type = elementary.normalize_data_type(col.dtype) %}
    {% if col_type == 'string' %}
        {# If the column exists and is currently a string, keep it a string #}
        {% do return(elementary.edr_cast_as_string("detected_at")) %}
    {% else %}
        {% do return(elementary.edr_cast_as_timestamp("detected_at")) %}
    {% endif %}
{% endmacro %}
