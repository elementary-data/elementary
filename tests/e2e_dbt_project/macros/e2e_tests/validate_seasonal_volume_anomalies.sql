{% macro validate_seasonal_volume_anomalies() %}
    {% set query %}
        select test_alias, status
        from {{ ref('elementary_test_results') }}
        where table_name in ('users_per_day_weekly_seasonal', 'users_per_hour_daily_seasonal')
    {% endset %}
    {% set results = elementary.run_query(query) %}
    {{ assert_lists_contain_same_items(results, [
        ('day_of_week_volume_anomalies_no_seasonality', 'fail'),
        ('day_of_week_volume_anomalies_with_seasonality', 'pass'),
        ('hour_of_day_volume_anomalies_with_seasonality', 'pass'),
        ('hour_of_day_volume_anomalies_no_seasonality', 'fail'),
        ('hour_of_week_volume_anomalies_no_seasonality', 'fail'),
        ('hour_of_week_volume_anomalies_with_seasonality', 'pass')
    ]) }}
{% endmacro %}
