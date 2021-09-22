import pytest
from unittest import mock
from sqllineage.core import LineageResult, Table
import networkx as nx
from sqllineage.models import Schema

from lineage.lineage_graph import LineageGraph


def create_lineage_result(read, write):
    result = LineageResult()
    if read is not None:
        result.read = {Table(read)}
    if write is not None:
        result.write = {Table(write)}
    return result


@pytest.mark.parametrize("query,expected_parsed_result", [
    ("insert into target_table (a, b) (select c, count(*) from source_table group by c);",
     create_lineage_result('source_table', 'target_table')),
    ("insert into target_table (select c, count(*) from source_table group by c);",
     create_lineage_result(None, 'target_table'))  # This syntax is not supported currently, therefore source_table
    # is not identified
])
def test_lineage_graph_parse_query(query, expected_parsed_result):
    reference = LineageGraph(profile_database_name='elementary_db')
    parsed_results = reference._parse_query(query)
    assert len(parsed_results) == 1
    assert parsed_results[0].read == expected_parsed_result.read
    assert parsed_results[0].write == expected_parsed_result.write


def test_lineage_graph_rename_node():
    reference = LineageGraph(profile_database_name='elementary_db')
    di_graph_mock = mock.create_autospec(nx.DiGraph)
    di_graph_mock.has_node.return_value = True
    reference._lineage_graph = di_graph_mock

    # Test rename_node
    reference._rename_node(None, 'new_node')  # Should have no impact
    reference._rename_node('old_node', None)  # Should have no impact
    reference._rename_node('old_node', 'new_node')

    # rename_node should check if node exists first
    di_graph_mock.has_node.assert_called_once_with('old_node')
    di_graph_mock.remove_node.assert_called_with('old_node')
    di_graph_mock.add_node.assert_called_with('new_node')

    # Test rename_node



@pytest.mark.parametrize("sources,targets,edges,show_isolated_nodes", [
    ({'s'}, {'t'}, {('s', 't')}, False),
    ({'s', None}, {'t'}, {('s', 't')}, False),
    ({'s'}, {'t'}, {('s', 't')}, True),
    ({'s'}, {'t', None}, {('s', 't')}, True),
    (set(), {'t'}, set(), True),
    (set(), {'t'}, set(), False),
    ({'s'}, set(), set(), True),
    ({'s'}, set(), set(), False),
    ({'s1', 's2'}, {'t'}, {('s1', 't'), ('s2', 't')}, True),
    ({'s1', 's2'}, {'t'}, {('s1', 't'), ('s2', 't')}, False),
    ({'s'}, {'t1', 't2'}, {('s', 't1'), ('s', 't2')}, True),
    ({'s'}, {'t1', 't2'}, {('s', 't1'), ('s', 't2')}, False),
    ({None}, {'t1', 't2'}, set(), True),
    (set(), {'t1', 't2'}, set(), False),
    ({'s1', 's2'}, set(), set(), True),
    ({'s1', 's2'}, set(), set(), False),
])
def test_lineage_graph_add_nodes_and_edges(sources, targets, edges, show_isolated_nodes):
    reference = LineageGraph(profile_database_name='elementary_db', show_isolated_nodes=show_isolated_nodes)
    di_graph_mock = mock.create_autospec(nx.DiGraph)
    reference._lineage_graph = di_graph_mock

    reference._add_nodes_and_edges(sources, targets)

    node_calls = []
    if len(sources) > 0:
        node_calls.append(mock.call(sources))
    if len(targets) > 0:
        node_calls.append(mock.call(targets))

    edge_calls = []
    for edge in edges:
        edge_calls.append(mock.call(edge[0], edge[1]))

    if show_isolated_nodes or len(edges) > 0:
        di_graph_mock.add_nodes_from.assert_has_calls(node_calls, any_order=True)

    di_graph_mock.add_edge.assert_has_calls(edge_calls, any_order=True)


@pytest.mark.parametrize("show_isolated_nodes", [
    (False,),
    (True,),
])
def test_lineage_graph_remove_node(show_isolated_nodes):
    reference = LineageGraph(profile_database_name='elementary_db', show_isolated_nodes=show_isolated_nodes)

    di_graph_mock = mock.create_autospec(nx.DiGraph)
    di_graph_mock.has_node.return_value = True
    node_successors = ['successor1', 'successor2']
    node_predecessors = ['predecessor1', 'predecessor2']
    di_graph_mock.successors.side_effect = node_successors
    di_graph_mock.predecessors.side_effect = node_predecessors
    di_graph_mock.degree.return_value = 0

    reference._lineage_graph = di_graph_mock

    # Test remove_node
    reference._remove_node('node')
    reference._remove_node(None)  # Should have no impact

    # Validate calling to has node before deleting it
    di_graph_mock.has_node.assert_called_once_with('node')

    if show_isolated_nodes:
        di_graph_mock.remove_node.assert_called_once_with('node')
    else:
        adjacent_nodes = node_successors + node_predecessors
        calls = [mock.call(node) for node in adjacent_nodes]
        calls.append(mock.call('node'))
        di_graph_mock.remove_node.assert_has_calls(calls, any_order=True)


@pytest.mark.parametrize("queries, show_isolated_nodes", [
    (['insert into table2 (c1, c2) (select c1, c2 from table1)',
      'insert into loop_table (c1, c2) (select c1, c2 from table1)',
      'insert into table3 (c1, c2) (select c1, c2 from loop_table)',
      'insert into loop_table (c1, c2) (select c1, c2 from loop_table join table3 on table3.id = loop_table.id)',
      'drop table loop_table'],
     False),
    (['insert into table2 (c1, c2) (select c1, c2 from table1)',
      'insert into loop_table (c1, c2) (select c1, c2 from table1)',
      'insert into table3 (c1, c2) (select c1, c2 from loop_table)',
      'insert into loop_table (c1, c2) (select c1, c2 from loop_table join table3 on table3.id = loop_table.id)',
      'drop table loop_table'],
     True),
])
def test_lineage_graph_init_graph_from_query_list_with_loops(queries, show_isolated_nodes):
    reference = LineageGraph(profile_database_name='elementary_db', show_isolated_nodes=show_isolated_nodes)

    query_list = [(query, 'elementary_db', 'elementary_schema') for query in queries]
    try:
        reference.init_graph_from_query_list(query_list)
    except Exception as exc:
        assert False, f"'init_graph_from_query_list' raised an exception {exc}"


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
def test_lineage_graph_resolve_table_qualification(table_name_in_query_text, db_name_in_history_table, schema_name_in_history_table,
                                     expected_resolved_schema):
    reference = LineageGraph(profile_database_name='profile_elementary_db', profile_schema_name='profile_elementary_sc')
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
    reference = LineageGraph(profile_database_name=profile_db_name, profile_schema_name=profile_schema_name)
    assert reference._should_ignore_table(Table(resolved_table_name)) == should_ignore


@pytest.mark.parametrize("resolved_table_name, show_full_table_names, expected_result", [
    ('elementary_db.elementary_sc.table1', True, 'elementary_db.elementary_sc.table1'),
    ('elementary_db.elementary_sc.table1', False, 'table1')
])
def test_lineage_graph_name_qualification(resolved_table_name, show_full_table_names, expected_result):
    reference = LineageGraph(profile_database_name='elementary_db', profile_schema_name='elementary_sc',
                             full_table_names=show_full_table_names)
    assert reference._name_qualification(Table(resolved_table_name), '', '') == expected_result
