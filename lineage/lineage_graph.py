import itertools
from typing import Optional

import networkx as nx
import sqlparse
from lineage.exceptions import ConfigError
from sqllineage.core import LineageAnalyzer, LineageResult
from sqllineage.exceptions import SQLLineageException
from pyvis.network import Network
import webbrowser

from lineage.query_context import QueryContext
from lineage.utils import get_logger
from sqllineage.models import Schema, Table
from tqdm import tqdm

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

    def __init__(self, profile_database_name: str, profile_schema_name: str = None, show_isolated_nodes: bool = False,
                 full_table_names: bool = False) -> None:
        self._lineage_graph = nx.DiGraph()
        self._show_isolated_nodes = show_isolated_nodes
        self._profile_database_name = profile_database_name
        self._profile_schema_name = profile_schema_name
        self._show_full_table_name = full_table_names

    @staticmethod
    def _parse_query(query: str) -> [LineageResult]:
        parsed_query = sqlparse.parse(query.strip())
        analyzed_statements = [LineageAnalyzer().analyze(statement) for statement in parsed_query
                               if statement.token_first(skip_cm=True, skip_ws=True)]
        return analyzed_statements

    @staticmethod
    def _resolve_table_qualification(table: Table, database_name: str, schema_name: str) -> Table:
        if not table.schema:
            if database_name is not None and schema_name is not None:
                table.schema = Schema(f'{database_name}.{schema_name}')
        else:
            parsed_query_schema_name = str(table.schema)
            if '.' not in parsed_query_schema_name:
                # Resolved schema is either empty or fully qualified with db_name.schema_name
                if database_name is not None:
                    table.schema = Schema(f'{database_name}.{parsed_query_schema_name}')
                else:
                    table.schema = Schema()
        return table

    def _should_ignore_table(self, table: Table) -> bool:
        if self._profile_schema_name is not None:
            if str(table.schema) == str(Schema(f'{self._profile_database_name}.{self._profile_schema_name}')):
                return False
        else:
            if str(Schema(self._profile_database_name)) in str(table.schema):
                return False

        return True

    def _name_qualification(self, table: Table, database_name: str, schema_name: str) -> Optional[str]:
        table = self._resolve_table_qualification(table, database_name, schema_name)

        if self._should_ignore_table(table):
            return None

        if self._show_full_table_name:
            return str(table)

        return str(table).rsplit('.', 1)[-1]

    def _update_lineage_graph(self, analyzed_statements: [LineageResult], query_context: QueryContext) -> None:
        database_name = query_context.queried_database
        schema_name = query_context.queried_schema
        for analyzed_statement in analyzed_statements:
            # Handle drop tables, if they exist in the statement
            dropped_tables = analyzed_statement.drop
            for dropped_table in dropped_tables:
                dropped_table_name = self._name_qualification(dropped_table, database_name, schema_name)
                self._remove_node(dropped_table_name)

            # Handle rename tables
            renamed_tables = analyzed_statement.rename
            for old_table, new_table in renamed_tables:
                old_table_name = self._name_qualification(old_table, database_name, schema_name)
                new_table_name = self._name_qualification(new_table, database_name, schema_name)
                self._rename_node(old_table_name, new_table_name)

            # sqllineage lib marks CTEs as intermediate tables. Remove CTEs (WITH statements) from the source tables.
            sources = {self._name_qualification(source, database_name, schema_name)
                       for source in analyzed_statement.read - analyzed_statement.intermediate}
            targets = {self._name_qualification(target, database_name, schema_name)
                       for target in analyzed_statement.write}

            self._add_nodes_and_edges(sources, targets, query_context)

    def _add_nodes_and_edges(self, sources: {str}, targets: {str}, query_context: QueryContext) -> None:
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
                self._lineage_graph.add_nodes_from(targets, title=query_context.to_html())
        else:
            self._lineage_graph.add_nodes_from(sources)
            self._lineage_graph.add_nodes_from(targets, title=query_context.to_html())
            for source, target in itertools.product(sources, targets):
                self._lineage_graph.add_edge(source, target)

    def _rename_node(self, old_node: str, new_node: str) -> None:
        if old_node is None or new_node is None:
            return

        if self._lineage_graph.has_node(old_node):
            # Rename in place instead of copying the entire lineage graph
            nx.relabel_nodes(self._lineage_graph, {old_node: new_node}, copy=False)

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

    def init_graph_from_query_list(self, queries: [tuple]) -> None:
        logger.debug(f'Loading {len(queries)} queries into the lineage graph')
        for query, query_context in tqdm(queries, desc="Updating lineage graph", colour='green'):
            try:
                analyzed_statements = self._parse_query(query)
            except SQLLineageException as exc:
                logger.debug(f'SQLLineageException was raised while parsing this query -\n{query}\n'
                             f'Error was -\n{exc}.')
                continue

            self._update_lineage_graph(analyzed_statements, query_context)

        logger.debug(f'Finished updating lineage graph!')

    def filter_on_table(self, selected_table: str, direction: str = None, depth: int = None) -> None:
        logger.debug(f'Filtering lineage graph on table - {selected_table}')
        resolved_selected_table_name = self._name_qualification(Table(selected_table), self._profile_database_name,
                                                                self._profile_schema_name)
        logger.debug(f'Qualified table name - {resolved_selected_table_name}')
        if resolved_selected_table_name is None:
            raise ConfigError(f'Could not resolve table name - {selected_table}, please make sure to '
                              f'specify a table name that exists in the database configured in your profiles file.')

        if direction == self.DOWNSTREAM_DIRECTION:
            self._lineage_graph = self._downstream_graph(resolved_selected_table_name, depth)
        elif direction == self.UPSTREAM_DIRECTION:
            self._lineage_graph = self._upstream_graph(resolved_selected_table_name, depth)
        elif direction == self.BOTH_DIRECTIONS:
            downstream_graph = self._downstream_graph(resolved_selected_table_name, depth)
            upstream_graph = self._upstream_graph(resolved_selected_table_name, depth)
            self._lineage_graph = nx.compose(upstream_graph, downstream_graph)
        else:
            raise ConfigError(f'Direction must be one of the following - {self.UPSTREAM_DIRECTION}|'
                              f'{self.DOWNSTREAM_DIRECTION}|{self.BOTH_DIRECTIONS}, '
                              f'Got - {direction} instead.')

        self._update_selected_node_attributes(resolved_selected_table_name)
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

    def draw_graph(self, should_open_browser: bool = True) -> None:
        # Visualize the graph
        net = Network(height="100%", width="100%", directed=True)
        net.from_nx(self._lineage_graph)
        net.set_options(GRAPH_VISUALIZATION_OPTIONS)

        net.save_graph("elementary_lineage.html")
        if should_open_browser:
            webbrowser.open_new_tab('elementary_lineage.html')