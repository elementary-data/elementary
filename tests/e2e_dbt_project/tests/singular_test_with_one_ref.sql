select min_val from {{ ref('numeric_column_anomalies') }} where min_val < 100
