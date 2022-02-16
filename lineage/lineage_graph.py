import itertools
import os.path
from typing import Optional, Union
import networkx as nx
from collections import defaultdict
from exceptions.exceptions import ConfigError
from pyvis.network import Network
import webbrowser
from lineage.query import Query
from utils.log import get_logger
from alive_progress import alive_it
import json
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

    LINEAGE_GRAPH_FILE_MAME = 'lineage_graph.gpickle'
    LINEAGE_GRAPH_ATTRIBUTES_FILE_NAME = 'graph_attributes.json'

    def __init__(self, show_isolated_nodes: bool = False) -> None:
        self._lineage_graph = nx.DiGraph()
        self._show_isolated_nodes = show_isolated_nodes
        self._graph_attributes = defaultdict(lambda: None)

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
                    self._graph_attributes[target_node] = query_context_html
        else:
            self._lineage_graph.add_nodes_from(sources)
            for target_node in targets:
                self._lineage_graph.add_node(target_node)
                self._graph_attributes[target_node] = query_context_html
            for source, target in itertools.product(sources, targets):
                self._lineage_graph.add_edge(source, target)

    def _rename_node(self, old_node: str, new_node: str) -> None:
        if old_node is None or new_node is None:
            return

        if self._lineage_graph.has_node(old_node):
            # Rename in place instead of copying the entire lineage graph
            nx.relabel_nodes(self._lineage_graph, {old_node: new_node}, copy=False)
            if old_node in self._graph_attributes:
                old_node_attributes = self._graph_attributes[old_node]
                del self._graph_attributes[old_node]
                self._graph_attributes[new_node] = old_node_attributes

    def _remove_node(self, node: str) -> None:
        # First let's check if the node exists in the graph
        if node is not None and self._lineage_graph.has_node(node):
            node_successors = list(self._lineage_graph.successors(node))
            node_predecessors = list(self._lineage_graph.predecessors(node))

            # networknx's remove_node already takes care of in and out edges
            self._lineage_graph.remove_node(node)
            if node in self._graph_attributes:
                del self._graph_attributes[node]

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

    @staticmethod
    def parse_filter(filter_str: str) -> (Union[int, None], str, Union[int, None]):
        split_filter = [split_str.strip() for split_str in filter_str.split('+')]
        upstream_depth = 0
        downstream_depth = 0
        selected_item = None

        if len(split_filter) == 1:
            selected_item = split_filter[0]
        elif len(split_filter) == 2:
            if split_filter[0].isnumeric():
                upstream_depth = int(split_filter[0])
                selected_item = split_filter[1]
            elif split_filter[1].isnumeric():
                selected_item = split_filter[0]
                downstream_depth = int(split_filter[1])
            elif split_filter[0] == '':
                upstream_depth = None
                selected_item = split_filter[1]
            elif split_filter[1] == '':
                selected_item = split_filter[0]
                downstream_depth = None
        elif len(split_filter) == 3:
            if split_filter[0].isnumeric():
                upstream_depth = int(split_filter[0])
            elif split_filter[0] == '':
                upstream_depth = None
            if split_filter[2].isnumeric():
                downstream_depth = int(split_filter[2])
            elif split_filter[2] == '':
                downstream_depth = None
            selected_item = split_filter[1]

        if selected_item is None:
            raise ConfigError('Invalid graph filter')

        return upstream_depth, selected_item, downstream_depth

    def filter(self, database_filter: str, schema_filter: str, table_filter: str) -> None:
        if table_filter is not None:
            self.filter_on_table(table_filter)
            return
        if schema_filter is not None:
            self.filter_on_schema(schema_filter)
            return
        if database_filter is not None:
            self.filter_on_database(database_filter)
            return

    def get_subgraph(self, nodes: set, upstream_depth: Union[int, None], downstream_depth: Union[int, None]) \
            -> nx.DiGraph:
        subgraph = nx.DiGraph()
        subgraph.add_nodes_from((n, self._lineage_graph.nodes[n]) for n in nodes)
        subgraph.add_edges_from((n, nbr, d) for n, nbrs in self._lineage_graph.adj.items() if n in nodes
                                for nbr, d in nbrs.items() if nbr in nodes)

        if upstream_depth is None or upstream_depth > 0:
            source_nodes = [x for x in subgraph.nodes() if subgraph.in_degree(x) == 0]
            for node in source_nodes:
                subgraph = nx.compose(subgraph, self._upstream_graph(node, upstream_depth))
        if downstream_depth is None or downstream_depth > 0:
            last_nodes = [x for x in subgraph.nodes() if subgraph.out_degree(x) == 0]
            for node in last_nodes:
                subgraph = nx.compose(subgraph, self._downstream_graph(node, downstream_depth))

        return subgraph

    def filter_on_database(self, database_filter: str) -> None:
        upstream_depth, database_name, downstream_depth = self.parse_filter(database_filter)
        nodes_in_db = set()
        for node in self._lineage_graph:
            if database_name.lower() == node.split('.')[0].lower():
                nodes_in_db.add(node)

        self._lineage_graph = self.get_subgraph(nodes_in_db, upstream_depth, downstream_depth)

    @staticmethod
    def _split_graph_node_name(node: str) -> (Union[str, None], Union[str, None], Union[str, None]):
        split_node = [part.lower() for part in node.split('.')]
        if len(split_node) != 3:
            return None, None, None
        node_database_name, node_schema_name, node_table_name = split_node
        return node_database_name, node_schema_name, node_table_name

    def filter_on_schema(self, schema_filter: str) -> None:
        upstream_depth, schema_name, downstream_depth = self.parse_filter(schema_filter)
        nodes_in_schema = set()
        for node in self._lineage_graph:
            node_database_name, node_schema_name, _ = self._split_graph_node_name(node)
            if node_schema_name is None:
                continue
            normalized_schema_name = schema_name.lower()
            if normalized_schema_name == node_schema_name or normalized_schema_name == \
                    '.'.join([node_database_name, node_schema_name]):
                nodes_in_schema.add(node)

        self._lineage_graph = self.get_subgraph(nodes_in_schema, upstream_depth, downstream_depth)

    def filter_on_table(self, table_filter: str) -> None:
        upstream_depth, table_name, downstream_depth = self.parse_filter(table_filter)
        matched_nodes = set()
        for node in self._lineage_graph.nodes:
            _, node_schema_name, node_table_name = self._split_graph_node_name(node)
            if node_table_name is None or node_schema_name is None:
                continue
            normalized_table_name = table_name.lower()
            if normalized_table_name == node_table_name or normalized_table_name == '.'.join([node_schema_name,
                                                                                              node_table_name]):
                matched_nodes.add(node)

        self._lineage_graph = self.get_subgraph(matched_nodes, upstream_depth, downstream_depth)
        if len(matched_nodes) == 1:
            self._update_selected_node_attributes(matched_nodes.pop())

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
            if node in self._graph_attributes:
                self._lineage_graph.nodes[node]['title'] = attr.get('title', '') + self._graph_attributes[node]

    def export_graph_to_files(self, target_dir_path: str) -> None:
        lineage_graph_file_path = os.path.join(target_dir_path, self.LINEAGE_GRAPH_FILE_MAME)
        lineage_graph_attributes_file_path = os.path.join(target_dir_path,
                                                          self.LINEAGE_GRAPH_ATTRIBUTES_FILE_NAME)

        nx.write_gpickle(self._lineage_graph, lineage_graph_file_path)
        with open(lineage_graph_attributes_file_path, 'w') as graph_attributes_file:
            json.dump(self._graph_attributes, graph_attributes_file)

    def load_graph_from_files(self, target_dir_path: str) -> bool:
        lineage_graph_file_path = os.path.join(target_dir_path, self.LINEAGE_GRAPH_FILE_MAME)
        lineage_graph_attributes_file_path = os.path.join(target_dir_path,
                                                          self.LINEAGE_GRAPH_ATTRIBUTES_FILE_NAME)

        if not os.path.exists(lineage_graph_file_path) or not os.path.exists(lineage_graph_attributes_file_path):
            return False

        self._lineage_graph = nx.read_gpickle(lineage_graph_file_path)
        with open(lineage_graph_attributes_file_path, 'r') as graph_attributes_file:
            self._graph_attributes = json.load(graph_attributes_file)

        return True

    def draw_graph(self, should_open_browser: bool = True, full_table_names: bool = True) -> bool:
        if len(self._lineage_graph.edges) == 0 and len(self._lineage_graph.nodes) == 0:
            return False

        self._enrich_graph_with_monitoring_data()

        if not full_table_names:
            short_name_mappings = dict()
            for node in self._lineage_graph.nodes:
                short_name_mappings[node] = node.rsplit('.', 1)[-1]
            self._lineage_graph = nx.relabel_nodes(self._lineage_graph, short_name_mappings)

        # Visualize the graph
        net = Network(height="95%", width="100%", directed=True, heading=self._load_header())
        net.from_nx(self._lineage_graph)
        net.set_options(GRAPH_VISUALIZATION_OPTIONS)

        net.save_graph("elementary_lineage.html")
        if should_open_browser:
            webbrowser.open_new_tab('elementary_lineage.html')

        return True

