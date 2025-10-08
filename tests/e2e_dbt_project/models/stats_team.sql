{% if var("stage") == "training" %}
     select * from {{ ref('stats_team_training') }}
{% elif var("stage") == "validation" %}
     select * from {{ ref('stats_team_validation') }}
{% endif %}
