{{
  config(
    materialized = 'nothing',
    )
}}

-- depends_on: {{ ref('alerts_v2') }}

{% do elementary_cli.update_skipped_alerts(var("alert_ids")) %}
{{ elementary.no_results_query() }}
