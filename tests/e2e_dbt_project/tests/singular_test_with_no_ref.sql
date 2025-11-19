{% set relation = api.Relation.create(database=elementary.target_database(), schema=target.schema, identifier='numeric_column_anomalies') %}
select min from {{ relation }} where min < 100
