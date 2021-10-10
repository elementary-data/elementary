import pytest
from build.lib.lineage.query_context import QueryContext
from lineage.snowflake_query import SnowflakeQuery


@pytest.mark.parametrize("query_text, profile_db, profile_sc, queried_db, queried_sc, expected_source_tables,"
                         "expected_target_tables", [
    ('insert into target_table (a, b) (select c, count(*) from source_table group by c);', 'db1', 'sc1', 'db1', 'sc1',
     {'db1.sc1.source_table'}, {'db1.sc1.target_table'}),
    ('insert into db1.sc1.target_table (a, b) (select c, count(*) from sc1.source_table group by c);', 'db1', None,
     'db1', 'sc1', {'db1.sc1.source_table'}, {'db1.sc1.target_table'}),
    ('insert into db1.sc1.target_table (a, b) (select c, count(*) from db1.sc1.source_table group by c);', 'db1', 'sc1',
     None, None, {'db1.sc1.source_table'}, {'db1.sc1.target_table'})
])
def test_snowflake_query_parse(query_text, profile_db, profile_sc, queried_db, queried_sc, expected_source_tables,
                               expected_target_tables):
    empty_context = QueryContext(queried_database=profile_db, queried_schema=profile_sc)
    full_table_names = True

    reference = SnowflakeQuery(query_text, empty_context, profile_db, profile_sc)
    reference.parse(full_table_names=full_table_names)

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

    reference = SnowflakeQuery(query_text, empty_context, profile_db, profile_sc)
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

    reference = SnowflakeQuery(query_text, empty_context, profile_db, profile_sc)
    reference.parse(full_table_names=full_table_names)

    assert reference.renamed_tables == expected_renamed_tables



