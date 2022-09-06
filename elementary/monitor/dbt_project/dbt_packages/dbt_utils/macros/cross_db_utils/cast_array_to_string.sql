{% macro cast_array_to_string(array) %}
  {{ adapter.dispatch('cast_array_to_string', 'dbt_utils') (array) }}
{% endmacro %}

{% macro default__cast_array_to_string(array) %}
    cast({{ array }} as {{ dbt_utils.type_string() }})
{% endmacro %}

{# when casting as array to string, postgres uses {} (ex: {1,2,3}) while other dbs use [] (ex: [1,2,3]) #}
{% macro postgres__cast_array_to_string(array) %}
    {%- set array_as_string -%}cast({{ array }} as {{ dbt_utils.type_string() }}){%- endset -%}
    {{ dbt_utils.replace(dbt_utils.replace(array_as_string,"'}'","']'"),"'{'","'['") }}
{% endmacro %}

{# redshift should use default instead of postgres #}
{% macro redshift__cast_array_to_string(array) %}
    cast({{ array }} as {{ dbt_utils.type_string() }})
{% endmacro %}

{% macro bigquery__cast_array_to_string(array) %}
    '['||(select string_agg(cast(element as string), ',') from unnest({{ array }}) element)||']'
{% endmacro %}