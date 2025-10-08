{% test config_levels(model, expected_config, timestamp_column, time_bucket, where_expression, anomaly_sensitivity, anomaly_direction, days_back, backfill_days, seasonality, min_training_set_size) %}
    {%- if execute and elementary.is_test_command() %}
        {%- set unexpected_config = [] %}
        {%- set model_relation = dbt.load_relation(model) %}

        {% set configuration_dict, metric_properties_dict =
               elementary.get_anomalies_test_configuration(model_relation,
                                                           mandatory_params,
                                                           timestamp_column,
                                                           where_expression,
                                                           anomaly_sensitivity,
                                                           anomaly_direction,
                                                           min_training_set_size,
                                                           time_bucket,
                                                           days_back,
                                                           backfill_days,
                                                           seasonality) %}

        {%- set configs_to_test = [('timestamp_column', metric_properties_dict.timestamp_column),
                                   ('where_expression', metric_properties_dict.where_expression),
                                   ('time_bucket', configuration_dict.time_bucket),
                                   ('anomaly_sensitivity', configuration_dict.anomaly_sensitivity),
                                   ('anomaly_direction', configuration_dict.anomaly_direction),
                                   ('min_training_set_size', configuration_dict.min_training_set_size),
                                   ('days_back', configuration_dict.days_back),
                                   ('backfill_days', configuration_dict.backfill_days),
                                   ('seasonality', configuration_dict.seasonality)
                                   ] %}

        {%- for config in configs_to_test %}
            {%- set config_name, config_value = config %}
            {%- set config_check = compare_configs(config_name, config_value, expected_config) %}
            {%- if config_check %}
                {%- do unexpected_config.append(config_check) -%}
            {%- endif %}
        {%- endfor %}

        {%- if unexpected_config | length > 0 %}
            {%- do exceptions.raise_compiler_error('Failure config_levels: ' ~ unexpected_config) -%}
        {%- else %}
            {#- test must run an sql query -#}
            {{ elementary.no_results_query() }}
        {%- endif %}
    {%- endif %}
{%- endtest %}

{% macro compare_configs(config_name, config, expected_config) %}
    {%- if config != expected_config.get(config_name) %}
        {%- set unexpected_message = ('For {0} - got config: {1}, expected config: {2}').format(config_name, config, expected_config.get(config_name) ) %}
        {{ return(unexpected_message) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}