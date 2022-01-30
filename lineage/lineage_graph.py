import itertools
from typing import Optional
import networkx as nx
from collections import defaultdict
from exceptions.exceptions import ConfigError
from pyvis.network import Network
import webbrowser
from lineage.query import Query
from utils.log import get_logger
from alive_progress import alive_it
import pkg_resources

logger = get_logger(__name__)

GRAPH_VISUALIZATION_OPTIONS = """{
            "edges": {
                "color": {
                  "color": "rgba(23,107,215,1)",
                  "highlight": "rgba(23,107,215,1)",
                  "hover": "rgba(23,107,215,1)",
                  "inherit": false
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
                    "nodeSpacing": 100,
                    "treeSpacing": 100,
                    "blockShifting": false,
                    "edgeMinimization": true,
                    "parentCentralization": false,
                    "direction": "LR",
                    "sortMethod": "directed"
                }
            },
            "interaction": {
                "hover": true,
                "navigationButtons": true,
                "multiselect": true,
                "keyboard": {
                    "enabled": true
                }
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
    UPSTREAM_DIRECTION = 'upstream'
    DOWNSTREAM_DIRECTION = 'downstream'
    BOTH_DIRECTIONS = 'both'
    SELECTED_NODE_COLOR = '#0925C7'
    SELECTED_NODE_TITLE = 'Selected table<br/><br/>'

    def __init__(self, show_isolated_nodes: bool = False) -> None:
        self._lineage_graph = nx.DiGraph()
        self._show_isolated_nodes = show_isolated_nodes
        self._catalog = defaultdict(lambda: None)

    def _update_lineage_graph(self, query: Query) -> None:
        # Handle drop tables, if they exist in the statement
        for dropped_table_name in query.dropped_tables:
            self._remove_node(dropped_table_name)

        # Handle rename tables
        for old_table_name, new_table_name in query.renamed_tables:
            self._rename_node(old_table_name, new_table_name)

        self._add_nodes_and_edges(query.source_tables, query.target_tables, query.get_context_as_html())

    def _add_nodes_and_edges(self, sources: {str}, targets: {str}, query_context_html: str) -> None:
        if None in sources:
            sources.remove(None)
        if None in targets:
            targets.remove(None)

        if not sources and not targets:
            return

        if len(sources) > 0 and len(targets) == 0:
            if self._show_isolated_nodes:
                self._lineage_graph.add_nodes_from(sources)
        elif len(targets) > 0 and len(sources) == 0:
            if self._show_isolated_nodes:
                for target_node in targets:
                    self._lineage_graph.add_node(target_node)
                    self._catalog[target_node] = query_context_html
        else:
            self._lineage_graph.add_nodes_from(sources)
            for target_node in targets:
                self._lineage_graph.add_node(target_node)
                self._catalog[target_node] = query_context_html
            for source, target in itertools.product(sources, targets):
                self._lineage_graph.add_edge(source, target)

    def _rename_node(self, old_node: str, new_node: str) -> None:
        if old_node is None or new_node is None:
            return

        if self._lineage_graph.has_node(old_node):
            # Rename in place instead of copying the entire lineage graph
            nx.relabel_nodes(self._lineage_graph, {old_node: new_node}, copy=False)
            if old_node in self._catalog:
                old_node_attributes = self._catalog[old_node]
                del self._catalog[old_node]
                self._catalog[new_node] = old_node_attributes

    def _remove_node(self, node: str) -> None:
        # First let's check if the node exists in the graph
        if node is not None and self._lineage_graph.has_node(node):
            node_successors = list(self._lineage_graph.successors(node))
            node_predecessors = list(self._lineage_graph.predecessors(node))

            # networknx's remove_node already takes care of in and out edges
            self._lineage_graph.remove_node(node)
            if node in self._catalog:
                del self._catalog[node]

            # Now that we have just deleted the dropped table from the graph, we need to take care of
            # new island nodes.
            if not self._show_isolated_nodes:
                for successor in node_successors:
                    if self._lineage_graph.has_node(successor) and self._lineage_graph.degree(successor) == 0:
                        self._lineage_graph.remove_node(successor)
                for predecessor in node_predecessors:
                    if self._lineage_graph.has_node(predecessor) and self._lineage_graph.degree(predecessor) == 0:
                        self._lineage_graph.remove_node(predecessor)

    def init_graph_from_query_list(self, queries: [Query]) -> None:
        queries_with_progress_bar = alive_it(queries, title='Updating lineage graph')
        for query in queries_with_progress_bar:
            self._update_lineage_graph(query)

        logger.debug(f'Finished updating lineage graph!')

    def filter_on_table(self, selected_table: str, direction: str = None, depth: int = None) -> None:
        logger.debug(f'Filtering lineage graph on table - {selected_table}')

        if direction == self.DOWNSTREAM_DIRECTION:
            self._lineage_graph = self._downstream_graph(selected_table, depth)
        elif direction == self.UPSTREAM_DIRECTION:
            self._lineage_graph = self._upstream_graph(selected_table, depth)
        elif direction == self.BOTH_DIRECTIONS:
            downstream_graph = self._downstream_graph(selected_table, depth)
            upstream_graph = self._upstream_graph(selected_table, depth)
            self._lineage_graph = nx.compose(upstream_graph, downstream_graph)
        else:
            raise ConfigError(f'Direction must be one of the following - {self.UPSTREAM_DIRECTION}|'
                              f'{self.DOWNSTREAM_DIRECTION}|{self.BOTH_DIRECTIONS}, '
                              f'Got - {direction} instead.')

        self._update_selected_node_attributes(selected_table)
        logger.debug(f'Finished filtering lineage graph on table - {selected_table}')
        pass

    def _downstream_graph(self, source_node: str, depth: Optional[int]) -> nx.DiGraph:
        logger.debug(f'Building a downstream graph for - {source_node}, depth - {depth}')
        return nx.bfs_tree(G=self._lineage_graph, source=source_node, depth_limit=depth)

    def _upstream_graph(self, target_node: str, depth: Optional[int]) -> nx.DiGraph:
        logger.debug(f'Building an upstream graph for - {target_node}, depth - {depth}')
        reversed_lineage_graph = self._lineage_graph.reverse(copy=True)
        return nx.bfs_tree(G=reversed_lineage_graph, source=target_node, depth_limit=depth).reverse(copy=False)

    def _update_selected_node_attributes(self, selected_node: str) -> None:
        if self._lineage_graph.has_node(selected_node):
            node = self._lineage_graph.nodes[selected_node]
            node_title = node.get('title', '')
            node.update({'color': self.SELECTED_NODE_COLOR,
                         'title': self.SELECTED_NODE_TITLE + node_title})

    @staticmethod
    def _load_header() -> str:
        header_content = ""
        header = pkg_resources.resource_filename(__name__, "header.html")

        if header is not None:
            with open(header, 'r') as header_file:
                header_content = header_file.read()

        return header_content

    def properties(self):
        return {'lineage_properties': {'nodes_count': len(self._lineage_graph.nodes),
                                       'edges_count': len(self._lineage_graph.edges)}}

    def _enrich_graph_with_monitoring_data(self):
        for node, attr in self._lineage_graph.nodes(data=True):
            if node in self._catalog:
                self._lineage_graph.nodes[node]['title'] = attr.get('title', '') + self._catalog[node]

    def draw_graph(self, should_open_browser: bool = True) -> bool:
        if len(self._lineage_graph.edges) == 0:
            return False

        self._enrich_graph_with_monitoring_data()
        # Visualize the graph
        net = Network(height="95%", width="100%", directed=True, heading=self._load_header())
        net.from_nx(self._lineage_graph)
        net.set_options(GRAPH_VISUALIZATION_OPTIONS)

        net.save_graph("elementary_lineage.html")
        if should_open_browser:
            webbrowser.open_new_tab('elementary_lineage.html')

        return True

