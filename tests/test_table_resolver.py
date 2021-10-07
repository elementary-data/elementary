import pytest
from sqllineage.models import Table, Schema

from lineage.TableResolver import TableResolver


@pytest.mark.parametrize("table_name_in_query_text, db_name_in_history_table, schema_name_in_history_table, "
                         "expected_resolved_schema", [
                             # Always prefer the db and schema from query text over the history db and schema
                             ('elementary_db.elementary.table1', 'history_db', 'history_schema',
                              'elementary_db.elementary'),
                             ('elementary_db.elementary.table1', 'history_db', None, 'elementary_db.elementary'),
                             ('elementary_db.elementary.table1', None, 'history_schema', 'elementary_db.elementary'),
                             ('elementary_db.elementary.table1', None, None, 'elementary_db.elementary'),
                             # Use history_db if query_text contains only schema and table name
                             ('elementary.table1', 'history_db', 'history_schema', 'history_db.elementary'),
                             ('elementary.table1', 'history_db', None, 'history_db.elementary'),
                             ('elementary.table1', None, 'history_schema', Schema.unknown),
                             ('elementary.table1', None, None, Schema.unknown),
                             # Use history_db and history schema if query_text contains only table name
                             ('table1', 'history_db', 'history_schema', 'history_db.history_schema'),
                             ('table1', 'history_db', None, Schema.unknown),
                             ('table1', None, 'history_schema', Schema.unknown),
                             ('table1', None, None, Schema.unknown),
                         ])
def test_lineage_graph_resolve_table_qualification(table_name_in_query_text,
                                                   db_name_in_history_table,
                                                   schema_name_in_history_table,
                                                   expected_resolved_schema):

    reference = TableResolver(profile_database_name='profile_elementary_db',
                              profile_schema_name='profile_elementary_sc',
                              queried_database_name=db_name_in_history_table,
                              queried_schema_name=schema_name_in_history_table)

    resolved_table = reference._resolve_table_qualification(Table(table_name_in_query_text), db_name_in_history_table,
                                                            schema_name_in_history_table)

    assert str(resolved_table.schema) == expected_resolved_schema


@pytest.mark.parametrize("resolved_table_name, profile_db_name, profile_schema_name, should_ignore", [
    ('profile_elementary_db.profile_elementary_sc.table1', 'profile_elementary_db', 'profile_elementary_sc', False),
    ('elementary_db.elementary_sc.table1', 'profile_elementary_db', 'profile_elementary_sc', True),
    ('elementary_sc.table1', 'profile_elementary_db', 'profile_elementary_sc', True),
    ('elementary_db.table1', 'profile_elementary_db', 'profile_elementary_sc', True),
    (str(Table('table1')), 'profile_elementary_db', 'profile_elementary_sc', True),
    ('profile_elementary_db.profile_elementary_sc.table1', 'profile_elementary_db', None, False),
    ('profile_elementary_db.elementary_sc.table1', 'profile_elementary_db', None, False),
    ('profile_elementary_sc.table1', 'profile_elementary_db', None, True),
    (str(Table('table1')), 'profile_elementary_db', None, True),
])
def test_lineage_graph_should_ignore_table(resolved_table_name, profile_db_name, profile_schema_name, should_ignore):
    reference = TableResolver(profile_database_name=profile_db_name, profile_schema_name=profile_schema_name)
    assert reference._should_ignore_table(Table(resolved_table_name)) == should_ignore


@pytest.mark.parametrize("resolved_table_name, show_full_table_names, expected_result", [
    ('elementary_db.elementary_sc.table1', True, 'elementary_db.elementary_sc.table1'),
    ('elementary_db.elementary_sc.table1', False, 'table1')
])
def test_lineage_graph_name_qualification(resolved_table_name, show_full_table_names, expected_result):
    reference = TableResolver(profile_database_name='elementary_db', profile_schema_name='elementary_sc',
                              queried_database_name='', queried_schema_name='',
                              full_table_names=show_full_table_names)

    assert reference.name_qualification(Table(resolved_table_name)) == expected_result
