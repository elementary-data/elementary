import itertools
from collections import defaultdict
from typing import Optional
import networkx as nx
from build.lib.lineage.query_context import QueryContext
from lineage.exceptions import ConfigError
from pyvis.network import Network
import webbrowser
from lineage.query import Query
from lineage.utils import get_logger
from tqdm import tqdm
import pkg_resources
import matplotlib.pyplot as plt
from statistics import median
import base64
from io import BytesIO

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
    SELECTED_NODE_TITLE = 'Selected table<br/>'

    def __init__(self, show_isolated_nodes: bool = False, show_full_table_names: bool = False) -> None:
        self._lineage_graph = nx.DiGraph()
        self._show_isolated_nodes = show_isolated_nodes
        self._show_full_table_names = show_full_table_names
        self._queries_count = None
        self._failed_queries_count = 0
        self.catalog = defaultdict(lambda: {'volume': [], 'update_times': [], 'last_html': None})

    def _add_node_to_catalog(self, node: str, query_context: QueryContext) -> None:
        self.catalog[node]['volume'].append(query_context.query_volume)
        self.catalog[node]['update_times'].append(
            query_context.query_time_to_str(query_context.query_time, fmt='%Y-%m-%d %H:%M:%S'))
        self.catalog[node]['last_html'] = query_context.to_html()

    def _update_lineage_graph(self, query: Query) -> None:
        if not query.parse(self._show_full_table_names):
            self._failed_queries_count += 1
            return

        # Handle drop tables, if they exist in the statement
        for dropped_table_name in query.dropped_tables:
            self._remove_node(dropped_table_name)

        # Handle rename tables
        for old_table_name, new_table_name in query.renamed_tables:
            self._rename_node(old_table_name, new_table_name)

        self._add_nodes_and_edges(query.source_tables, query.target_tables, query.get_context())

    def _add_nodes_and_edges(self, sources: {str}, targets: {str}, query_context: 'QueryContext') -> None:
        if None in sources:
            sources.remove(None)
        if None in targets:
            targets.remove(None)

        if not sources and not targets:
            return

        if len(sources) > 0 and len(targets) == 0:
            if self._show_isolated_nodes:
                self._lineage_graph.add_nodes_from(sources)

            user_name = query_context.user_name.lower()
            bi_node_name = None
            bi_node_url = None
            if user_name == 'tableau':
                bi_node_name = 'Tableau'
                bi_node_url = 'https://cdn.worldvectorlogo.com/logos/tableau-software.svg'
            elif user_name == 'looker':
                bi_node_name = 'Looker'
                bi_node_url = 'https://seeklogo.com/images/G/google-looker-logo-B27BD25E4E-seeklogo.com.png'

            if bi_node_name is not None:
                for source_node in sources:
                    self._lineage_graph.add_node(source_node)
                    self._lineage_graph.add_node(bi_node_name,
                                                 shape='image',
                                                 image=bi_node_url,
                                                 size=18,
                                                 title=query_context.to_html())
                    self._lineage_graph.add_edge(source_node, bi_node_name)
                    self._add_node_to_catalog(bi_node_name, query_context)
        elif len(targets) > 0 and len(sources) == 0:
            if self._show_isolated_nodes:
                self._lineage_graph.add_nodes_from(targets, title=query_context.to_html())

            user_name = query_context.user_name.lower()
            etl_node_name = None
            etl_node_url = None
            if user_name == 'airbyte':
                etl_node_name = 'Airbyte'
                etl_node_url = 'https://img.stackshare.io/service/21342/default_6e06b23ed369812a3f23810d72817bafca9ac5a4.png'
            elif user_name == 'fivetran':
                etl_node_name = 'Fivetran'
                etl_node_url = 'https://store-images.s-microsoft.com/image/apps.13830.d451b47a-6fa6-4741-bfc3-cdd3ce54e65a.5eef8f15-74b2-46d7-a9b8-296e9e69c136.fa0c02b3-78f5-4eb9-b665-948de3615d73'

            if etl_node_name is not None:
                for target_node in targets:
                    self._lineage_graph.add_node(target_node,
                                                 title=query_context.to_html())
                    self._add_node_to_catalog(target_node, query_context)
                    self._lineage_graph.add_node(etl_node_name,
                                                 shape='image',
                                                 image=etl_node_url,
                                                 size=19)
                    self._lineage_graph.add_edge(etl_node_name, target_node)
        else:
            self._lineage_graph.add_nodes_from(sources)
            for target_node in targets:
                self._lineage_graph.add_node(target_node, title=query_context.to_html())
                self._add_node_to_catalog(target_node, query_context)
            for source, target in itertools.product(sources, targets):
                self._lineage_graph.add_edge(source, target)

    def _rename_node(self, old_node: str, new_node: str) -> None:
        if old_node is None or new_node is None:
            return

        if self._lineage_graph.has_node(old_node):
            # Rename in place instead of copying the entire lineage graph
            nx.relabel_nodes(self._lineage_graph, {old_node: new_node}, copy=False)
            if old_node in self.catalog:
                old_node_attributes = self.catalog[old_node]
                del self.catalog[old_node]
                self.catalog[new_node] = old_node_attributes

    def _remove_node(self, node: str) -> None:
        # First let's check if the node exists in the graph
        if node is not None and self._lineage_graph.has_node(node):
            node_successors = list(self._lineage_graph.successors(node))
            node_predecessors = list(self._lineage_graph.predecessors(node))

            # networknx's remove_node already takes care of in and out edges
            self._lineage_graph.remove_node(node)

            # Now that we have just deleted the dropped table from the graph, we need to take care of
            # new island nodes.
            if not self._show_isolated_nodes:
                for successor in node_successors:
                    if self._lineage_graph.has_node(successor) and self._lineage_graph.degree(successor) == 0:
                        self._lineage_graph.remove_node(successor)
                for predecessor in node_predecessors:
                    if self._lineage_graph.has_node(predecessor) and self._lineage_graph.degree(predecessor) == 0:
                        self._lineage_graph.remove_node(predecessor)

            if node in self.catalog:
                del self.catalog[node]

    def init_graph_from_query_list(self, queries: [Query]) -> None:
        self._queries_count = len(queries)
        logger.debug(f'Loading {self._queries_count} queries into the lineage graph')
        for query in tqdm(queries, desc="Updating lineage graph", colour='green'):
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

    def _get_freshness_and_volume_graph_for_node(self, node: str) -> str:
        times = self.catalog[node]['update_times'][-3:]
        volumes = self.catalog[node]['volume'][-3:]
        # plotting a bar chart
        plt.clf()
        plt.bar(times, volumes, width=0.1, color=['blue'])
        plt.xlabel('Time')
        plt.ylabel('Volume')
        plt.title(node)
        fig = plt.gcf()
        tmpfile = BytesIO()
        fig.savefig(tmpfile, format='png')
        encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')
        return f"""
        <br/><div style="font-family:arial;color:DarkSlateGrey;font-size:110%;">
                                <strong>
                                    Freshness & volume graph</br>
                                </strong>
                                <img width="400" height="300" src=\'data:image/png;base64,{encoded}\'>
        </div>
        """

    def _enrich_graph_with_monitoring_context(self) -> None:
        # TODO: get only nodes with tag = target
        for node in self._lineage_graph.nodes:
            if node in self.catalog:
                title_html = f"""
                <html>
                    <body>
                        {self.catalog[node]['last_html'] + self._get_freshness_and_volume_graph_for_node(node)}
                    </body>
                </html>
                """

                node_volumes = self.catalog[node]['volume']
                if node_volumes[-1] < median(node_volumes) / 2:
                    self._lineage_graph.nodes[node]['color'] = 'red'
                    title_html = f"""
                        <html>
                            <body>
                                <div style="font-family:arial;color:tomato;font-size:110%;">
                                    <strong>
                                    Warning - last update volume is too low</br></br>
                                    </strong>
                                </div>
                                {self.catalog[node]['last_html'] + self._get_freshness_and_volume_graph_for_node(node)}
                            </body>
                        </html>
                        """
                self._lineage_graph.nodes[node]['title'] = title_html

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
                'edges_count': len(self._lineage_graph.edges),
                'queries_count': self._queries_count,
                'failed_queries': self._failed_queries_count}

    def draw_graph(self, should_open_browser: bool = True) -> None:
        self._enrich_graph_with_monitoring_context()
        # Visualize the graph
        net = Network(height="95%", width="100%", directed=True, heading=self._load_header())
        net.from_nx(self._lineage_graph)
        net.set_options(GRAPH_VISUALIZATION_OPTIONS)

        net.save_graph("elementary_lineage.html")
        if should_open_browser:
            webbrowser.open_new_tab('elementary_lineage.html')
