select min_val from {{ source('training', 'numeric_column_anomalies_training') }} where min_val < 105
