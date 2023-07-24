{% test unique_if_exists(model, column_name) %}
    {% if not execute %}
        {% do return(none) %}
    {% endif %}

    {% if not elementary.relation_exists(model) %}
        {% do exceptions.warn("Relation '{}' does not exist.".format(model)) %}
        {% do return(elementary.no_results_query()) %}
    {% else %}
        {% do return(adapter.dispatch("test_unique")(model, column_name)) %}
    {% endif %}
{% endtest %}
