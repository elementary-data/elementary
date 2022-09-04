import pytest
from elementary.lineage.query_context import QueryContext
from elementary.lineage.bigquery_query import BigQueryQuery
from elementary.lineage.table_resolver import TableResolver


def create_bigquery_table_colum(project_id, dataset_id, table_id):
    return {'project_id': project_id, 'dataset_id': dataset_id, 'table_id': table_id}


@pytest.mark.parametrize("json_table_column, full_table_names, expected_parsed_result", [
    (create_bigquery_table_colum('db1', 'sc1', 't1'), True, 'db1.sc1.t1'),
    (create_bigquery_table_colum('db1', 'sc1', 't1'), False, 't1'),
    ({'project': 'db1', 'dataset_id': 'sc1', 'table_id': 't1'}, True, None),
    ({}, True, None),
    (None, True, None),
    (create_bigquery_table_colum('db1', 'sc1', 'anont1'), True, None), #anon represents a temp cached table and therefore should be ignored
])
def test_bigquery_query_parse_table_json_column(json_table_column, full_table_names, expected_parsed_result):
    table_resolver = TableResolver(full_table_names=full_table_names)
    assert BigQueryQuery._parse_table_json_column(table_resolver, json_table_column) == expected_parsed_result


def test_bigquery_query_parse():
    raw_query_text = ''
    query_context = QueryContext(referenced_tables=[create_bigquery_table_colum('db1', 'sc1', 'source_table')],
                                 destination_table=create_bigquery_table_colum('db1', 'sc1', 'target_table'))
    reference = BigQueryQuery(raw_query_text, query_context)
    reference.parse(full_table_names=False)
    assert reference.source_tables == {'source_table'}
    assert reference.target_tables == {'target_table'}


def test_bigquery_query_parse_with_drop_statement():
    raw_query_text = 'drop table db1.sc1.target_table'
    query_context = QueryContext(referenced_tables=[],
                                 destination_table=create_bigquery_table_colum('db1', 'sc1', 'target_table'),
                                 query_type='DROP_TABLE')
    reference = BigQueryQuery(raw_query_text, query_context)
    reference.parse(full_table_names=False)
    assert reference.dropped_tables == {'target_table'}


def test_bigquery_query_parse_with_alter_statement():
    raw_query_text = 'alter table db1.sc1.target_table rename to target_table_ng'
    query_context = QueryContext(referenced_tables=[],
                                 destination_table=create_bigquery_table_colum('db1', 'sc1', 'target_table'),
                                 query_type='ALTER_TABLE')
    reference = BigQueryQuery(raw_query_text, query_context)
    reference.parse(full_table_names=False)
    assert reference.renamed_tables == {('target_table', 'target_table_ng')}
