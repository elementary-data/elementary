{% macro get_models() %}
    {% set models_query %}
        with sources as (
            select
              name as model_name,
              unique_id as model_unique_id,
              database_name,
              schema_name,
              identifier as table_name,
              owner,
              tags,
              package_name,
              description,
              original_path as full_path
            from {{ ref('dbt_sources') }}
          ),

          models as (
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
            from {{ ref('dbt_models') }}
          ),

          models_and_sources as (
            select * from sources
            union all
            select * from models
          )

        select * from models_and_sources -- TODO: should we filter?
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
        {% set owners = elementary.insensitive_get_dict_value(model_dict, 'owner') %}
        {% if owners and owners is string %}
            {% set loaded_owners = fromjson(owners) %}
            {% if loaded_owners %}
                {% set owners = loaded_owners %}
            {% endif %}
        {% endif %}
        {% set tags = elementary.insensitive_get_dict_value(model_dict, 'tags') %}
        {% if tags and tags is string %}
            {% set loaded_tags = fromjson(tags) %}
            {% if loaded_tags %}
                {% set tags = loaded_tags %}
            {% endif %}
        {% endif %}
        {% set normalized_model_dict = {'model_name': elementary.insensitive_get_dict_value(model_dict, 'model_name'),
                                        'model_unique_id': model_unique_id,
                                        'database_name': elementary.insensitive_get_dict_value(model_dict, 'database_name'),
                                        'schema_name': elementary.insensitive_get_dict_value(model_dict, 'schema_name'),
                                        'table_name': elementary.insensitive_get_dict_value(model_dict, 'table_name'),
                                        'owners': owners,
                                        'tags': tags,
                                        'package_name': elementary.insensitive_get_dict_value(model_dict, 'package_name'),
                                        'description': elementary.insensitive_get_dict_value(model_dict, 'description'),
                                        'full_path': full_path,
                                        'file_name': file_name} %}
        {% do models_dict.update({model_unique_id: normalized_model_dict}) %}
    {% endfor %}

    {% do elementary.edr_log(tojson(models_dict)) %}
{% endmacro %}