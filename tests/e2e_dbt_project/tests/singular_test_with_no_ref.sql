{% set relation = api.Relation.create(database=elementary.target_database(), schema=target.schema, identifier='numeric_column_anomalies') %}
select min_val from {{ relation }} where min_val < 100
