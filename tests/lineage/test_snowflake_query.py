import pytest
from lineage.query_context import QueryContext
from lineage.snowflake_query import SnowflakeQuery
from lineage.table_resolver import TableResolver


@pytest.mark.parametrize("query_text, queried_db, queried_sc, expected_source_tables,"
                         "expected_target_tables", [
    ('insert into target_table (a, b) (select c, count(*) from source_table group by c);', 'db1', 'sc1',
     {'db1.sc1.source_table'}, {'db1.sc1.target_table'}),
    ('insert into db1.sc1.target_table (a, b) (select c, count(*) from sc1.source_table group by c);',
     'db1', 'sc1', {'db1.sc1.source_table'}, {'db1.sc1.target_table'}),
    ('insert into db1.sc1.target_table (a, b) (select c, count(*) from db1.sc1.source_table group by c);',
     None, None, {'db1.sc1.source_table'}, {'db1.sc1.target_table'}),
    ('select metadata$filename, metadata$file_row_number, t.$1 t.$2 from @my_gcs_stage (file_format => mycsvformat) t;'
     , 'db1', 'sc1', {'db1.sc1.@my_gcs_stage'}, set()),
    ('CREATE TABLE db1.sc1.B$1 AS ( WITH stuff AS (SELECT * FROM db1.sc1.B$2) select * from stuff)',
     None, None, {'db1.sc1.b$2'}, {'db1.sc1.b$1'}),
    ('CREATE TABLE db1.sc1.B$1 AS ( WITH stuff AS (SELECT * FROM db1.sc1.B$2) select metadata$filename from stuff)',
     None, None, {'db1.sc1.b$2'}, {'db1.sc1.b$1'}),
    ("""
    create or replace  view ELEMENTARY_DB.elementary.my_second_dbt_model  as (
     -- Use the `ref` function to select from other models

    select *
    from ELEMENTARY_DB.elementary.my_first_dbt_model
    where id = 1
     );""",
     None, None, {'elementary_db.elementary.my_first_dbt_model'},
     {'elementary_db.elementary.my_second_dbt_model'}),
])
def test_snowflake_query_parse(query_text, queried_db, queried_sc, expected_source_tables,
                               expected_target_tables):
    empty_context = QueryContext(queried_database=queried_db, queried_schema=queried_sc)

    reference = SnowflakeQuery(query_text, empty_context)
    reference.parse(full_table_names=True)

    assert reference.source_tables == expected_source_tables
    assert reference.target_tables == expected_target_tables


@pytest.mark.parametrize("query_text, profile_db, profile_sc, queried_db, queried_sc, expected_dropped_tables", [
                             ('drop table sc1.t1', 'db1', 'sc1', 'db1', None, {'db1.sc1.t1'}),
                             ('drop table t1', 'db1', 'sc1', 'db1', 'sc1', {'db1.sc1.t1'}),
                             ('alter table t1 rename to t2', 'db1', 'sc1', 'db1', 'sc1', {}),
                             ('insert into db1.sc1.target_table (a, b) (select c, count(*) from db1.sc1.source_table'
                              ' group by c);', 'db1', 'sc1', 'db1', 'sc1', {}),
                         ])
def test_snowflake_query_parse_with_drop_table_statement(query_text, profile_db, profile_sc, queried_db, queried_sc,
                                                         expected_dropped_tables):
    empty_context = QueryContext(queried_database=profile_db, queried_schema=profile_sc)
    full_table_names = True

    reference = SnowflakeQuery(query_text, empty_context)
    reference.parse(full_table_names=full_table_names)

    assert len(reference.dropped_tables) == len(expected_dropped_tables)
    if len(expected_dropped_tables) > 0:
        assert reference.dropped_tables == expected_dropped_tables


@pytest.mark.parametrize("query_text, profile_db, profile_sc, queried_db, queried_sc, expected_renamed_tables", [
    ('alter table sc1.t1 rename to t2', 'db1', 'sc1', 'db1', None, {('db1.sc1.t1', 'db1.sc1.t2')}),
    ('alter table t1 rename to t2', 'db1', 'sc1', 'db1', 'sc1', {('db1.sc1.t1', 'db1.sc1.t2')}),
])
def test_snowflake_query_parse_with_alter_table(query_text, profile_db, profile_sc, queried_db, queried_sc,
                                                expected_renamed_tables):
    empty_context = QueryContext(queried_database=profile_db, queried_schema=profile_sc)
    full_table_names = True

    reference = SnowflakeQuery(query_text, empty_context)
    reference.parse(full_table_names=full_table_names)

    assert reference.renamed_tables == expected_renamed_tables


@pytest.mark.parametrize("query_text, db, sc, expected_source_tables, expected_target_tables", [
                        ("""MERGE INTO db.sc.target_table        AS  dest
                            USING db.sc.source_table             AS  src
                                ON  src.id =  dest.id
                            WHEN NOT matched THEN INSERT
                                (id, a, b)
                            VALUES
                                (id, a, b)
                            """,
                            'db', 'sc', {'db.sc.source_table'}, {'db.sc.target_table'}),
                        ("""MERGE INTO db.sc.target_table        AS  dest
                            USING (SELECT * FROM db.sc.source_table) AS src
                                ON  src.id =  dest.id
                            WHEN NOT matched THEN INSERT
                                (id, a, b)
                            VALUES
                                (id, a, b)
                            """,
                         'db', 'sc', {'db.sc.source_table'}, {'db.sc.target_table'}),
])
def test_snowflake_query_parse_with_merge_query(query_text, db, sc, expected_source_tables, expected_target_tables):

    table_resolver = TableResolver(db, sc, True)
    sources, targets = SnowflakeQuery._parse_merge_query(table_resolver, query_text)
    assert expected_source_tables == sources
    assert expected_target_tables == targets

