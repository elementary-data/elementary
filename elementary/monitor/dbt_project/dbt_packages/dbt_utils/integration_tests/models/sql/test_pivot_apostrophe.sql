
-- TODO: How do we make this work nicely on Snowflake too?

{% if target.type == 'snowflake' %}
    {% set column_values = ['RED', 'BLUE', "BLUE'S"] %}
    {% set cmp = 'ilike' %}
{% else %}
    {% set column_values = ['red', 'blue', "blue's"] %}
    {% set cmp = '=' %}
{% endif %}

select
    size,
    {{ dbt_utils.pivot('color', column_values, cmp=cmp, quote_identifiers=False) }}

from {{ ref('data_pivot') }}
group by size
