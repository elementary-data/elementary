{{
  config(
    materialized = 'nothing',
    )
}}

-- depends_on: {{ ref('alerts') }}
-- depends_on: {{ ref('alerts_models') }}
-- depends_on: {{ ref('alerts_source_freshness') }}

{% do elementary_cli.update_sent_alerts(var("alert_ids"), var("sent_at"), var("table_name")) %}
{{ elementary.no_results_query() }}
