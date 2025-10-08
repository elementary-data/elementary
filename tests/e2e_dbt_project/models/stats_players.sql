{% if var("stage") == "training" %}
     select * from {{ ref('stats_players_training') }}
{% elif var("stage") == "validation" %}
     select * from {{ ref('stats_players_validation') }}
{% endif %}
