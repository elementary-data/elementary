{{
  config(
    materialized = 'nothing',
    )
}}

-- depends_on: {{ ref('alerts_v2') }}

{% do elementary_cli.update_sent_alerts(var("alert_ids"), var("sent_at")) %}
{{ elementary.no_results_query() }}
