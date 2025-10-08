{% if var("stage") == "training" %}
     select * from {{ ref('groups_training') }}
{% elif var("stage") == "validation" %}
     select * from {{ ref('groups_validation') }}
{% endif %}
