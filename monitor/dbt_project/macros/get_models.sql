{% macro get_models() %}
    {% set models_query %}
        select
            name as model_name,
            unique_id as model_unique_id,
            database_name,
            schema_name,
            alias as table_name, -- TODO: not sure about it
            owner,
            tags,
            package_name,
            description,
            original_path as full_path
        from dbt_models -- TODO: should we filter?
             -- TODO: check that dbt_models exists
             -- TODO: artifacts can be uploaded to a different db / schema - how do we get them?
    {% endset %}
    {% set models = run_query(models_query) %}
    {% set models_dict = {} %}
    {% for model in models %}
        {% set model_dict = model.dict() %}
        {% set model_unique_id = elementary.insensitive_get_dict_value(model_dict, 'model_unique_id') %}
        {% set full_path = elementary.insensitive_get_dict_value(model_dict, 'full_path') %}
        {% set file_name = none %}
        {% if full_path %}
            {% set file_name = full_path.split('/')[-1] %}
        {% endif %}
        {% set normalized_model_dict = {'model_name': elementary.insensitive_get_dict_value(model_dict, 'model_name'),
                                        'model_unique_id': model_unique_id,
                                        'database_name': elementary.insensitive_get_dict_value(model_dict, 'database_name'),
                                        'database_name': elementary.insensitive_get_dict_value(model_dict, 'database_name'),
                                        'table_name': elementary.insensitive_get_dict_value(model_dict, 'table_name'),
                                        'owner': elementary.insensitive_get_dict_value(model_dict, 'owner'),
                                        'tags': elementary.insensitive_get_dict_value(model_dict, 'tags'),
                                        'package_name': elementary.insensitive_get_dict_value(model_dict, 'package_name'),
                                        'description': elementary.insensitive_get_dict_value(model_dict, 'description'),
                                        'full_path': full_path,
                                        'file_name': file_name} %}
        {% do models_dict.update({model_unique_id: normalized_model_dict}) %}
    {% endfor %}

    {% do elementary.edr_log(tojson(models_dict)) %}
{% endmacro %}