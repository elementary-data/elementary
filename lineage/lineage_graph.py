import itertools

import networkx as nx
import sqlparse
from sqllineage.core import LineageAnalyzer, LineageResult
from sqllineage.exceptions import SQLLineageException
from pyvis.network import Network
import webbrowser

# TODO: add logger and debug logs like a pro
# TODO: add relevant requirements
GRAPH_VISUALIZATION_OPTIONS = """{
            "edges": {
                "color": {
                    "inherit": true
                },
                "dashes": true,
                "smooth": {
                    "type": "continuous",
                    "forceDirection": "none"
                }
            },
            "layout": {
                "hierarchical": {
                    "enabled": true,
                    "levelSeparation": 485,
                    "nodeSpacing": 300,
                    "treeSpacing": 300,
                    "blockShifting": false,
                    "edgeMinimization": false,
                    "parentCentralization": false,
                    "direction": "LR",
                    "sortMethod": "directed"
                }
            },
            "interaction": {
                "navigationButtons": true
            },
            "physics": {
                "enabled": false,
                "hierarchicalRepulsion": {
                    "centralGravity": 0
                },
                "minVelocity": 0.75,
                "solver": "hierarchicalRepulsion"
            }
}"""


class LineageGraph(object):
    def __init__(self, show_islands: bool = False) -> None:
        self._lineage_graph = nx.DiGraph()
        self._show_islands = show_islands

    @staticmethod
    def _parse_query(query: str) -> [LineageResult]:
        parsed_query = sqlparse.parse(query.strip())
        analyzed_statements = [LineageAnalyzer().analyze(statement) for statement in parsed_query
                               if statement.token_first(skip_cm=True, skip_ws=True)]
        return analyzed_statements

    def _update_lineage_graph(self, analyzed_statements: [LineageResult]) -> None:
        for analyzed_statement in analyzed_statements:
            # Handle drop tables, if they exist in the statement
            dropped_tables = analyzed_statement.drop
            for dropped_table in dropped_tables:
                self._remove_node(str(dropped_table))

            # Handle rename tables
            renamed_tables = analyzed_statement.rename
            for old_table, new_table in renamed_tables:
                self._rename_node(str(old_table), str(new_table))

            # sqllineage lib marks CTEs as intermediate tables. Remove CTEs (WITH statements) from the source tables.
            sources = {str(source) for source in analyzed_statement.read - analyzed_statement.intermediate}
            targets = {str(target) for target in analyzed_statement.write}

            self._add_nodes_and_edges(sources, targets)

    def _add_nodes_and_edges(self, sources: {str}, targets: {str}) -> None:
        if not sources and not targets:
            return

        if len(sources) > 0 and len(targets) == 0:
            if self._show_islands:
                self._lineage_graph.add_nodes_from(sources)
        elif len(targets) > 0 and len(sources) == 0:
            if self._show_islands:
                self._lineage_graph.add_nodes_from(targets)
        else:
            self._lineage_graph.add_nodes_from(sources)
            self._lineage_graph.add_nodes_from(targets)
            for source, target in itertools.product(sources, targets):
                self._lineage_graph.add_edge(source, target)

    def _rename_node(self, old_node: str, new_node: str) -> None:
        if self._lineage_graph.has_node(old_node):
            # Rename in place instead of copying the entire lineage graph
            nx.relabel_nodes(self._lineage_graph, {old_node: new_node}, copy=False)

    def _remove_node(self, node: str) -> None:
        # First let's check if the node exists in the graph
        if self._lineage_graph.has_node(node):
            node_successors = list(self._lineage_graph.successors(node))
            node_predecessors = list(self._lineage_graph.predecessors(node))

            # networknx's remove_node already takes care of in and out edges
            self._lineage_graph.remove_node(node)

            # Now that we have just deleted the dropped table from the graph, we need to take care of
            # new island nodes.
            if not self._show_islands:
                for successor in node_successors:
                    if self._lineage_graph.degree(successor) == 0:
                        self._lineage_graph.remove_node(successor)
                for predecessor in node_predecessors:
                    if self._lineage_graph.degree(predecessor) == 0:
                        self._lineage_graph.remove_node(predecessor)

    def init_graph_from_query_list(self, queries: [str]) -> None:
        for query in queries:
            try:
                analyzed_statements = self._parse_query(query)
            except SQLLineageException as e:
                #TODO: log exception here
                continue

            self._update_lineage_graph(analyzed_statements)

    def draw_graph(self, should_open_browser: bool = True) -> None:
        # Visualize the graph
        net = Network(height="100%", width="100%", directed=True)
        net.from_nx(self._lineage_graph)
        net.set_options(GRAPH_VISUALIZATION_OPTIONS)

        net.show("elementary_lineage.html")
        net.save_graph("elementary_lineage.html")
        if should_open_browser:
            webbrowser.open_new_tab('./elementary_lineage.html')