{# 
  Those macros are used to generate a table with all of the supported data types for each DWH.
#}

{% macro create_all_types_table() %}
    {% do return(adapter.dispatch('create_all_types_table','elementary')()) %}
{% endmacro %}

{% macro bigquery__create_all_types_table() %}
    {# see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types #}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {% set _, relation = dbt.get_or_create_relation(database=database_name, schema=schema_name, identifier='all_types', type="table") %}
    {% set sql_query %}
      select 
        struct("string" as col1, 42 as col2) as flat_struct_col,
        struct("string" as col1, struct(42 as nestcol1) as col2) as nested_struct_col,
        [1,2,3] as array_col,
        null as null_col,
        true as bool_col,
        cast("str" as STRING) as str_col,
        cast(12345 as INT64) as int64_col,
        cast(12345 as FLOAT64) as float64_col,
        cast(12345 as NUMERIC) as numeric_col,
        cast(1122334455 as BIGNUMERIC) as bignum_col,
        b'1' as bytes_col,
        INTERVAL '10 -12:30' DAY TO MINUTE as interval_col,
        JSON '{"data_type": "json"}' as json_col,
        ST_GEOGPOINT(-122, 47) AS geo_col,
        CURRENT_DATE() as date_col,
        CURRENT_DATETIME() as datetime_col,
        CURRENT_TIME() as time_col,
        CURRENT_TIMESTAMP() as timestamp_col,
    {% endset %}
    {% set create_table_query = dbt.create_table_as(false, relation, sql_query) %}
    {% do elementary.edr_log(create_table_query) %}
    {% do elementary.run_query(create_table_query) %}
{% endmacro %}

{% macro snowflake__create_all_types_table() %}
    {# see https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html #}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {% set _, relation = dbt.get_or_create_relation(database=database_name, schema=schema_name, identifier='all_types', type="table") %}
    {% set sql_query %}
      select 
        'str'::STRING as str_col,
        'str'::TEXT as text_col,
        'a'::VARCHAR as var_col,
        'a'::CHAR as char_col,
        'a'::CHARACTER as character_col,
        'a'::NCHAR as nchar_col,
        'a'::NVARCHAR as nvarchar_col,
        'a'::NVARCHAR2 as nvarchar2_col,
        'a'::CHAR VARYING as char_varying_col,
        'a'::NCHAR VARYING as nchar_varying_col,
        TRY_TO_BINARY('1', 'HEX')::BINARY as binary_col,
        TRY_TO_BINARY('1', 'HEX')::VARBINARY as varbinary_col,
        TRUE::BOOLEAN as boolean_col,
        13::NUMBER as number_col,
        13::DEC as dec_col,
        13::DECIMAL as decimal_col,
        13::INT as int_col,
        13::INTEGER as integer_col,
        13::BIGINT as bigint_col,
        13::SMALLINT as smallint_col,
        13::TINYINT as tinyint_col,
        13::BYTEINT as byteint_col,
        13::FLOAT as float_col,
        13::FLOAT4 as float4_col,
        13::FLOAT8 as float8_col,
        13::DOUBLE as double_col,
        13::DOUBLE PRECISION as double_precision_col,
        13::REAL as real_col,
        '2023-10-23'::DATE as date_col,
        '13:30:00'::TIME as time_col,
        '2023-10-23 12:00:00'::TIMESTAMP_TZ as timestamp_tz_col,
        '2023-10-23 12:00:00'::TIMESTAMP_LTZ as timestamp_ltz_col,
        '2023-10-23 12:00:00'::TIMESTAMP_NTZ as timestamp_ntz_col,
        '2023-10-23 12:00:00'::DATETIME as datetime_col,
        TO_VARIANT(1.23) as variant_col,
        {'data_type': 'object'} as object_col,
        [1,2,3] as array_col,
        TO_GEOGRAPHY('POINT(-122.35 37.55)') as geography_col
    {% endset %}
    {% set create_table_query = dbt.create_table_as(false, relation, sql_query) %}
    {% do elementary.edr_log(create_table_query) %}
    {% do elementary.run_query(create_table_query) %}
{% endmacro %}

{% macro redshift__create_all_types_table() %}
    {# see https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html #}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {% set _, relation = dbt.get_or_create_relation(database=database_name, schema=schema_name, identifier='all_types', type="table") %}
    {% set sql_query %}
      select 
        1::SMALLINT as smallint_col,
        1::INT2 as int2_col,
        1::INTEGER as integer_col,
        1::INT as int_col,
        1::INT4 as int4_col,
        1::BIGINT as bigint_col,
        1::INT8 as int8_col,
        1::DECIMAL as decimal_col,
        1::NUMERIC as numeric_col,
        1::REAL as real_col,
        1::FLOAT4 as float4_col,
        1::FLOAT as float_col,
        1::FLOAT8 as float8_col,
        1::DOUBLE PRECISION as double_precision_col,
        TRUE::BOOLEAN as boolean_col,
        TRUE::bool as bool_col,
        'a'::VARCHAR as var_col,
        'str'::TEXT as text_col,
        'a'::NVARCHAR as nvarchar_col,
        'a'::CHARACTER VARYING as character_varying_col,
        'a'::CHAR as char_col,
        'a'::CHARACTER as character_col,
        'a'::NCHAR as nchar_col,
        'a'::BPCHAR as bpchar_col,
        TO_DATE('20231023', 'YYYYMMDD') as date_col,
        sysdate as timestamp_col,
        TO_TIMESTAMP(sysdate, 'YYYY-MM-DD HH24:MI:SS') as timestampptz_col,
        ST_GeomFromText('POLYGON((0 2,1 1,0 -1,0 2))') as geometry_col,
        ST_GeogFromText('SRID=4324;POLYGON((0 0,0 1,1 1,10 10,1 0,0 0))') as geography_col,
        JSON_PARSE('{"data_type": "super"}') as super_col
    {% endset %}
    {% set create_table_query = dbt.create_table_as(false, relation, sql_query) %}
    {% do elementary.edr_log(create_table_query) %}
    {% do elementary.run_query(create_table_query) %}
  
{% endmacro %}

{% macro postgres__create_all_types_table() %}
    {# see https://www.postgresql.org/docs/current/datatype.html #}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {% set _, relation = dbt.get_or_create_relation(database=database_name, schema=schema_name, identifier='all_types', type="table") %}
    {% set sql_query %}
      select 
        CAST(1 as BIGINT) as bigint_col,
        CAST(1 as INT8) as int8_col,
        CAST(B'00' as BIT) as bit_col,
        CAST(B'00' as BIT VARYING) as bit_varying_col,
        CAST(B'00' as VARBIT) as varbit_col,
        CAST(TRUE as BOOLEAN) as boolean_col,
        CAST(TRUE as BOOL) as bool_col,
        CAST('(1, 1), (2, 2)' as BOX) as box_col,
        '\xDEADBEEF'::bytea as bytea_col,
        'a'::char as char_col,
        'a'::character as character_col,
        'a'::character varying as character_varying_col,
        'a'::varchar as varchar_col,
        '8.8.8.8'::cidr as cidr_col,
        '(1, 1), 1'::circle as circle_col,
        '2023-10-23'::date as date_col,
        CAST(1 as FLOAT8) as float8_col,
        CAST(1 as DOUBLE PRECISION) as double_precision_col,
        '8.8.8.8'::inet as inet_col,
        CAST(1 as INTEGER) as integer_col,
        CAST(1 as INT) as int_col,
        CAST(1 as INT4) as int4_col,
        interval '1 hour' as interval_col,
        '{"a":1,"b":2}'::json as json_col,
        '{"a":1,"b":2}'::jsonb as jsonb_col,
        '[(1,1),(2,2)]'::line as line_col,
        '[(1,1),(2,2)]'::lseg as lseg_col,
        'ff:ff:ff:ff:ff:ff'::macaddr as mac_col,
        'ff:ff:ff:ff:ff:ff'::macaddr8 as mac8_col,
        42::money as money_col,
        42::numeric as numeric_col,
        42::decimal as decimal_col,
        '[(1,1),(2,2)]'::path as path_col,
        '(1,1)'::point as point_col,
        '((1,1),(2,2))'::polygon as polygon_col,
        CAST(1 as REAL) as real_col,
        CAST(1 as FLOAT4) as float4_col,
        CAST(1 as SMALLINT) as smallint_col,
        CAST(1 as INT2) as int2_col,
        'a'::text as text_col,
        '12:00:00'::time as time_col,
        '12:00:00-600'::timetz as timetz_col,
        '2004-10-19 10:23:54'::timestamp as timestamp_col,
        '2004-10-19 10:23:54+02'::timestamptz as timestamptz_col,
        'confidence'::tsquery as tsquery_col,
        'confidence'::tsvector as tsvector_col,
        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid as uuid_col,
        xmlcomment('text') as xml_col
    {% endset %}
    {% set create_table_query = dbt.create_table_as(false, relation, sql_query) %}
    {% do elementary.edr_log(create_table_query) %}
    {% do elementary.run_query(create_table_query) %}
{% endmacro %}

{% macro default__create_all_types_table() %}
  {{ exceptions.raise_compiler_error("This macro is not supported on '{}'.".format(target.type)) }}
{% endmacro %}


{% macro compare_relation_types_and_information_schema_types() %}
    {% do elementary_tests.create_all_types_table() %}

    {% set schema_tuple = elementary.get_package_database_and_schema('elementary') %}
    {% set database_name, schema_name = schema_tuple %}
    {% set _, relation = dbt.get_or_create_relation(database=database_name, schema=schema_name, identifier='all_types', type="table") %}

    {% set relation_column_types = {} %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% for column in columns %}
      {% do relation_column_types.update({column.name.lower(): elementary.get_normalized_data_type(elementary.get_column_data_type(column))}) %}
    {% endfor %}
    {% do elementary.edr_log(relation_column_types) %}

    {% set information_schema_column_types = {} %}
    {% set information_schema_column_types_rows = elementary.agate_to_dicts(elementary.run_query(elementary.get_columns_from_information_schema(schema_tuple, 'all_types'))) %}
    {% for row in information_schema_column_types_rows %}
      {% do information_schema_column_types.update({row.column_name.lower(): elementary.get_normalized_data_type(row.data_type)}) %}
    {% endfor %}
    {% do elementary.edr_log(information_schema_column_types) %}

    {% set unmatched_types = [] %}
    {% for col, relation_value in relation_column_types.items() %}
      {% set info_schema_value = information_schema_column_types[col] %}
      {% if relation_value != info_schema_value %}
        {% do unmatched_types.append('Column "{}" types do not match: {} != {} '.format(col, relation_value, info_schema_value)) %}
      {% endif %}
    {% endfor %}
    {% do elementary.edr_log(unmatched_types) %}
    {% do return(unmatched_types) %}
{% endmacro %}
