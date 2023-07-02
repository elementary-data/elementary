{% test unique_if_exists(model, column_name) %}
    {% if not elementary.relation_exists(model) %}
        {% do exceptions.warn("Relation '{}' does not exist.".format(model)) %}
        {{ elementary.no_results_query() }}
    {% else %}
        {{ default__test_unique(model, column_name) }}
    {% endif %}
{% endtest %}
