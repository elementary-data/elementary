{% macro get_recommended_tests(
    depends_on_count=none,
    dependant_on_count=none,
    exposure_count=none,
    critical_name_like_patterns=none,
    table_types=none,
    tags=none,
    owners=none
) %}
    {% set query %}
        select resource_name, source_name, test_namespace, test_name, timestamp_column
        from {{ ref("pending_test_recommendations") }}
        where 1=0

        {% if depends_on_count %}
            or depends_on_count >= {{ depends_on_count }}
        {% endif %}

        {% if dependant_on_count %}
            or dependant_on_count >= {{ dependant_on_count }}
        {% endif %}

        {% if exposure_count %}
            or exposure_count >= {{ exposure_count }}
        {% endif %}

        {% if critical_name_like_patterns %}
            {% for name_like_pattern in critical_name_like_patterns %}
                or resource_name like '{{ name_like_pattern }}'
            {% endfor %}
        {% endif %}

        {% if table_types %}
            or table_type in ('{{ table_types | join("','") }}')
        {% endif %}

        {% if tags %}
            or tags ?| array['{{ tags | join("','") }}']
        {% endif %}

        {% if owners %}
            or owner ?| array['{{ owners | join("','") }}']
        {% endif %}
    {% endset %}

    {% set result = elementary.run_query(query) %}
    {% do return(elementary.agate_to_dicts(result)) %}
{% endmacro %}
