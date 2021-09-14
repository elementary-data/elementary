import pytest
from unittest import mock
from sqllineage.core import LineageResult, Table
import networkx as nx
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
    reference = LineageGraph()
    parsed_results = reference._parse_query(query)
    assert len(parsed_results) == 1
    assert parsed_results[0].read == expected_parsed_result.read
    assert parsed_results[0].write == expected_parsed_result.write


def test_lineage_graph_rename_node():
    reference = LineageGraph()
    di_graph_mock = mock.create_autospec(nx.DiGraph)
    di_graph_mock.has_node.return_value=True
    reference._lineage_graph = di_graph_mock

    # Test rename_node
    reference._rename_node('old_node', 'new_node')

    # rename_node should check if node exists first
    di_graph_mock.has_node.assert_called_once_with('old_node')
    di_graph_mock.remove_node.assert_called_with('old_node')
    di_graph_mock.add_node.assert_called_with('new_node')


@pytest.mark.parametrize("sources,targets,edges,show_isolated_nodes", [
    ({'s'}, {'t'}, {('s', 't')}, False),
    ({'s'}, {'t'}, {('s', 't')}, True),
    (set(), {'t'}, set(), True),
    (set(), {'t'}, set(), False),
    ({'s'}, set(), set(), True),
    ({'s'}, set(), set(), False),
    ({'s1', 's2'}, {'t'}, {('s1', 't'), ('s2', 't')}, True),
    ({'s1', 's2'}, {'t'}, {('s1', 't'), ('s2', 't')}, False),
    ({'s'}, {'t1', 't2'}, {('s', 't1'), ('s', 't2')}, True),
    ({'s'}, {'t1', 't2'}, {('s', 't1'), ('s', 't2')}, False),
    (set(), {'t1', 't2'}, set(), True),
    (set(), {'t1', 't2'}, set(), False),
    ({'s1', 's2'}, set(), set(), True),
    ({'s1', 's2'}, set(), set(), False),
])
def test_lineage_graph_add_nodes_and_edges(sources, targets, edges, show_isolated_nodes):
    reference = LineageGraph(show_isolated_nodes=show_isolated_nodes)
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

    # Validate calling to has node before deleting it
    di_graph_mock.has_node.assert_called_once_with('node')

    if show_isolated_nodes:
        di_graph_mock.remove_node.assert_called_once_with('node')
    else:
        adjacent_nodes = node_successors + node_predecessors
        calls = [mock.call(node) for node in adjacent_nodes]
        calls.append(mock.call('node'))
        di_graph_mock.remove_node.assert_has_calls(calls, any_order=True)




