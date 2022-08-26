import pytest
from unittest import mock
import networkx as nx
from sqllineage.models import Schema

from elementary.lineage.table_resolver import TableResolver
from elementary.lineage.lineage_graph import LineageGraph
from elementary.lineage.query import Query
from elementary.lineage.query_context import QueryContext


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
    reference = LineageGraph(show_isolated_nodes=show_isolated_nodes)
    di_graph_mock = mock.create_autospec(nx.DiGraph)
    reference._lineage_graph = di_graph_mock

    empty_query_context = QueryContext()
    reference._add_nodes_and_edges(sources, targets, empty_query_context.to_html())

    source_nodes_calls = []
    target_nodes_calls = []
    if len(sources) > 0:
        source_nodes_calls.append(mock.call(sources))
    if len(targets) > 0:
        for target_node in targets:
            target_nodes_calls.append(mock.call(target_node))

    edge_calls = []
    for edge in edges:
        edge_calls.append(mock.call(edge[0], edge[1]))

    if show_isolated_nodes or len(edges) > 0:
        di_graph_mock.add_nodes_from.assert_has_calls(source_nodes_calls, any_order=True)
        di_graph_mock.add_node.assert_has_calls(target_nodes_calls, any_order=True)

    di_graph_mock.add_edge.assert_has_calls(edge_calls, any_order=True)


@pytest.mark.parametrize("show_isolated_nodes", [
    (False,),
    (True,),
])
def test_lineage_graph_remove_node(show_isolated_nodes):
    reference = LineageGraph(show_isolated_nodes=show_isolated_nodes)

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
    reference = LineageGraph(show_isolated_nodes=show_isolated_nodes)

    query_context = QueryContext('elementary_db', 'elementary_schema')
    query_list = [Query(query, query_context) for query in queries]
    try:
        reference.init_graph_from_query_list(query_list)
    except Exception as exc:
        assert False, f"'init_graph_from_query_list' raised an exception {exc}"


def compare_edges(directed_graph: nx.DiGraph, edges: [set]) -> bool:
    for edge in directed_graph.edges:
        if (edge[0], edge[1]) not in edges:
            return False
    return True


def create_directed_graph_from_edge_list(edges: [list]) -> nx.DiGraph:
    G = nx.DiGraph()
    for edge in edges:
        G.add_node(edge[0])
        G.add_node(edge[1])
        G.add_edge(edge[0], edge[1])
    return G


@pytest.mark.parametrize("edges, selected_node, depth, expected_remaining_edges", [
    (nx.path_graph(5).edges, 3, None, {(3, 4)}),
    (nx.path_graph(5).edges, 3, 1, {(3, 4)}),
    (nx.path_graph(7).edges, 3, 1, {(3, 4)}),
    ([(0, 1), (1, 2), (0, 3), (3, 4), (3, 2), (2, 5), (4, 6)], 3, None, {(3, 4), (3, 2), (2, 5), (4, 6)}),
    ([(0, 1), (1, 2), (0, 3), (3, 4), (3, 2), (2, 5), (4, 6)], 3, 1, {(3, 4), (3, 2)})
])
def test_lineage_graph_downstream_graph(edges, selected_node, depth, expected_remaining_edges):
    reference = LineageGraph()
    reference._lineage_graph.add_edges_from(edges)
    reference._lineage_graph = reference._downstream_graph(selected_node, depth)
    assert compare_edges(reference._lineage_graph, expected_remaining_edges)


@pytest.mark.parametrize("edges, selected_node, depth, expected_remaining_edges", [
    (nx.path_graph(5).edges, 3, None, {(0, 1), (1, 2), (2, 3)}),
    (nx.path_graph(5).edges, 3, 1, {(2, 3)}),
    (nx.path_graph(7).edges, 3, 1, {(2, 3)}),
    ([(0, 1), (1, 2), (2, 3), (0, 3), (3, 4), (2, 5), (4, 6)], 3, None, {(0, 3), (2, 3), (1, 2), (0, 1)}),
    ([(0, 1), (1, 2), (2, 3), (0, 3), (3, 4), (2, 5), (4, 6)], 3, 1, {(0, 3), (2, 3)}),
])
def test_lineage_graph_upstream_graph(edges, selected_node, depth, expected_remaining_edges):
    reference = LineageGraph()
    reference._lineage_graph.add_edges_from(edges)
    reference._lineage_graph = reference._upstream_graph(selected_node, depth)
    assert compare_edges(reference._lineage_graph, expected_remaining_edges)


@pytest.mark.parametrize("profile_database_name, profile_schema_name, full_table_names, edges, selected_node, "
                         "direction, depth, expected_remaining_edges", [
                            ('db', 'sc', True, [('db.sc.t1', 'db.sc.t2'), ('db.sc.t1', 'db.sc.t3')],
                             't3', 'upstream', None,
                             {('db.sc.t1', 'db.sc.t3')}),
                            ('db', 'sc', True, [('db.sc.t1', 'db.sc.t2'), ('db.sc.t1', 'db.sc.t3')],
                             'db.sc.t3', 'upstream', None,
                             {('db.sc.t1', 'db.sc.t3')}),
                            ('db', 'sc', True, [('db.sc.t1', 'db.sc.t2'), ('db.sc.t1', 'db.sc.t3'),
                                                ('db.sc.t3', 'db.sc.t4')],
                             'sc.t1', 'downstream', 1,
                             {('db.sc.t1', 'db.sc.t3'), ('db.sc.t1', 'db.sc.t2')}),
                            ('db', 'sc', False, [('db.sc.t1', 'db.sc.t2'), ('db.sc.t1', 'db.sc.t3')],
                             't3', 'upstream', None,
                             {('t1', 't3')}),
                            ('db', 'sc', False, [('db.sc.t1', 'db.sc.t2'), ('db.sc.t1', 'db.sc.t3'),
                                                 ('db.sc.t3', 'db.sc.t4'), ('db.sc.t4', 'db.sc.t5'),
                                                 ('db.sc.t2', 'db.sc.t4'), ('db.sc.t4', 'db.sc.t6'),
                                                 ('db.sc.t6', 'db.sc.t8')],
                             't4', 'both', 1,
                             {('t3', 't4'), ('t4', 't5'), ('t2', 't4'), ('t4', 't6')}),
])
def test_lineage_graph_filter_on_table(profile_database_name, profile_schema_name, full_table_names, edges,
                                       selected_node, direction, depth, expected_remaining_edges):
    reference = LineageGraph()
    table_resolver = TableResolver(database_name=profile_database_name, schema_name=profile_schema_name,
                                   full_table_names=full_table_names)
    reference._lineage_graph = create_directed_graph_from_edge_list(edges)
    reference.filter_on_table(table_resolver.name_qualification(selected_node))
    assert compare_edges(reference._lineage_graph, expected_remaining_edges)

