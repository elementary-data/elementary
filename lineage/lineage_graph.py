import itertools
import os.path
from typing import Optional
import networkx as nx
from collections import defaultdict
from lineage.exceptions import ConfigError
from pyvis.network import Network
import webbrowser
from lineage.query import Query
from lineage.utils import get_logger
from tqdm import tqdm
import pkg_resources
import yaml

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
                    "levelSeparation": 700,
                    "nodeSpacing": 200,
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
    SELECTED_NODE_TITLE = 'Selected node<br/><br/>'

    def __init__(self, show_isolated_nodes: bool = False, show_full_names: bool = False) -> None:
        self._lineage_graph = nx.DiGraph()
        self._show_isolated_nodes = show_isolated_nodes
        self._catalog = defaultdict(lambda: None)
        self._edge_attributes = defaultdict(lambda: None)
        self._show_full_names = show_full_names

    def _update_lineage_graph(self, query: Query) -> None:
        # Handle drop tables, if they exist in the statement
        for dropped_table_name in query.dropped_tables:
            self._remove_node(dropped_table_name)

        # Handle rename tables
        for old_table_name, new_table_name in query.renamed_tables:
            self._rename_node(old_table_name, new_table_name)

        self._add_nodes_and_edges(query.source_tables, query.target_tables, query.get_context_as_html())

    def resolve_node_name(self, node_name: str) -> str:
        if self._show_full_names:
            return node_name

        return node_name.split('.', 2)[-1]

    def _add_nodes_and_edges(self, sources: {str}, targets: {str}, query_context_html: str,
                             edge_color: Optional[str] = None, edge_title: Optional[str] = None) -> None:
        if None in sources:
            sources.remove(None)
        if None in targets:
            targets.remove(None)

        if not sources and not targets:
            return

        if len(sources) > 0 and len(targets) == 0:
            if self._show_isolated_nodes:
                for source_node in sources:
                    source_node = self.resolve_node_name(source_node)
                    self._lineage_graph.add_node(source_node)
        elif len(targets) > 0 and len(sources) == 0:
            if self._show_isolated_nodes:
                for target_node in targets:
                    target_node = self.resolve_node_name(target_node)
                    self._lineage_graph.add_node(target_node)
                    self._catalog[target_node] = query_context_html
        else:
            for source_node in sources:
                source_node = self.resolve_node_name(source_node)
                self._lineage_graph.add_node(source_node)
            for target_node in targets:
                target_node = self.resolve_node_name(target_node)
                self._lineage_graph.add_node(target_node)
                self._catalog[target_node] = query_context_html
            for source, target in itertools.product(sources, targets):
                source = self.resolve_node_name(source)
                target = self.resolve_node_name(target)
                self._lineage_graph.add_edge(source, target, color=edge_color)
                self._edge_attributes[(source, target)] = {'color': edge_color, 'title': edge_title}

    def _rename_node(self, old_node: str, new_node: str) -> None:
        if old_node is None or new_node is None:
            return

        old_node = self.resolve_node_name(old_node)
        new_node = self.resolve_node_name(new_node)
        if self._lineage_graph.has_node(old_node):
            # Rename in place instead of copying the entire lineage graph
            nx.relabel_nodes(self._lineage_graph, {old_node: new_node}, copy=False)
            if old_node in self._catalog:
                old_node_attributes = self._catalog[old_node]
                del self._catalog[old_node]
                self._catalog[new_node] = old_node_attributes

    def _remove_node(self, node: str) -> None:
        node = self.resolve_node_name(node)
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
        for query in tqdm(queries, desc="Updating lineage graph", colour='green'):
            self._update_lineage_graph(query)

        logger.debug(f'Finished updating lineage graph!')

    def init_graph_from_yml_files(self, yml_files: [str]) -> None:
        for yml_file in yml_files:
            with open(yml_file, "r") as stream:
                try:
                    yml_dict = yaml.safe_load(stream)
                    target = yml_dict.get('target')
                    sources = yml_dict.get('sources')
                    alias_to_source = {}
                    alias_to_target = {}
                    for source, alias_dict in sources.items():
                        alias_to_source[alias_dict['alias']] = source
                    for target, alias_dict in target.items():
                        alias_to_target[alias_dict['alias']] = target
                    columns = yml_dict.get('columns')
                    for target_column, source_column in columns.items():

                        target_table_name, target_column_name = target_column.rsplit('.', 1)
                        if target_table_name in alias_to_target:
                            target_table_name = alias_to_target[target_table_name]
                        expanded_target_column = '.'.join([target_table_name, target_column_name])
                        source_table_name, source_column_name = source_column.rsplit('.', 1)
                        if source_table_name in alias_to_source:
                            source_table_name = alias_to_source[source_table_name]
                        expanded_source_column = '.'.join([source_table_name, source_column_name])
                        sql = yml_dict.get('sql', '')
                        sql = sql.replace(target_column_name, f'<b>{target_column_name}</b>')
                        query_context_html = f"""
                           <html>
                        <body>
                            <div style="font-family:arial;color:DarkSlateGrey;font-size:110%;">
                                <strong>
                                    Lineage details</br>
                                </strong>
                                <div style="min-width:74px;display:inline-block">Table:</div> {target_table_name}</br>
                                <div style="min-width:74px;display:inline-block">Column:</div> <b>{target_column_name}</b></br></br>    
                                <strong>
                                    SQL Raw</br>
                                </strong>
                                <span style="white-space: pre-line"> {sql}</span></br>
                            </div>
                        </body>
                    </html>
                        """
                        edge_color = 'red' if not yml_dict.get('validated') else None
                        edge_title = 'link is validated' if yml_dict.get('validated') else 'link is not validated'
                        self._add_nodes_and_edges(sources={expanded_source_column},
                                                  targets={expanded_target_column},
                                                  query_context_html=query_context_html,
                                                  edge_color=edge_color,
                                                  edge_title=edge_title)

                except yaml.YAMLError as exc:
                    raise
                    # TODO: fix

    def filter_on_node(self, selected_node: str, direction: str = None, depth: int = None) -> None:
        logger.debug(f'Filtering lineage graph on node - {selected_node}')

        found_nodes = []
        node_names = list(self._lineage_graph.nodes)
        for node_name in node_names:
            if node_name.endswith(selected_node):
                found_nodes.append(node_name)
        if len(found_nodes) == 1:
            selected_node = found_nodes[0]
        else:
            raise ConfigError(f"Could not find {selected_node} in graph, please make sure to filter on a valid name")

        if direction == self.DOWNSTREAM_DIRECTION:
            self._lineage_graph = self._downstream_graph(selected_node, depth)
        elif direction == self.UPSTREAM_DIRECTION:
            self._lineage_graph = self._upstream_graph(selected_node, depth)
        elif direction == self.BOTH_DIRECTIONS:
            downstream_graph = self._downstream_graph(selected_node, depth)
            upstream_graph = self._upstream_graph(selected_node, depth)
            self._lineage_graph = nx.compose(upstream_graph, downstream_graph)
        else:
            raise ConfigError(f'Direction must be one of the following - {self.UPSTREAM_DIRECTION}|'
                              f'{self.DOWNSTREAM_DIRECTION}|{self.BOTH_DIRECTIONS}, '
                              f'Got - {direction} instead.')

        self._update_selected_node_attributes(selected_node)
        logger.debug(f'Finished filtering lineage graph on table - {selected_node}')
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
        return {'nodes_count': len(self._lineage_graph.nodes),
                'edges_count': len(self._lineage_graph.edges)}

    def _enrich_graph_with_monitoring_data(self):
        for node, attr in self._lineage_graph.nodes(data=True):
            if node in self._catalog:
                self._lineage_graph.nodes[node]['title'] = attr.get('title', '') + self._catalog[node]

    def _enrich_graph_with_edge_attributes(self):
        for edge in self._lineage_graph.edges:
            if edge in self._edge_attributes:
                self._lineage_graph.edges[edge[0], edge[1]].update(self._edge_attributes[edge])

    def draw_graph(self, should_open_browser: bool = True) -> bool:
        if len(self._lineage_graph.edges) == 0:
            return False

        self._enrich_graph_with_monitoring_data()
        self._enrich_graph_with_edge_attributes()
        # Visualize the graph
        net = Network(height="95%", width="100%", directed=True, heading=self._load_header())
        net.from_nx(self._lineage_graph)
        net.set_options(GRAPH_VISUALIZATION_OPTIONS)

        net.save_graph("elementary_lineage.html")
        if should_open_browser:
            webbrowser.open_new_tab('elementary_lineage.html')

        return True

