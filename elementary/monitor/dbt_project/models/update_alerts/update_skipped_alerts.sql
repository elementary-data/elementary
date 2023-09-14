{{
  config(
    materialized = 'nothing',
    )
}}

-- depends_on: {{ ref('alerts') }}
-- depends_on: {{ ref('alerts_models') }}
-- depends_on: {{ ref('alerts_source_freshness') }}

{% do elementary_cli.update_skipped_alerts(var("alert_ids"), var("table_name")) %}
{{ elementary.no_results_query() }}
