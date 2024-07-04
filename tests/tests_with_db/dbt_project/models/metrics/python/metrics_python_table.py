def model(dbt, session):
    dbt.config(materialized="table")
    return dbt.source("test_data", "metrics_seed3")
