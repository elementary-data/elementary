import itertools
from collections import defaultdict
from statistics import median
from typing import Optional
import matplotlib.pyplot as plt
import networkx as nx
import sqlparse
from lineage.exceptions import ConfigError
from sqllineage.core import LineageAnalyzer, LineageResult
from sqllineage.exceptions import SQLLineageException
from pyvis.network import Network
import webbrowser
import base64
from io import BytesIO
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
        self.catalog = defaultdict(lambda: {'volume': [], 'update_times': [], 'last_html': None})

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

    def _add_node_to_catalog(self, node: str, query_context: QueryContext) -> None:
        self.catalog[node]['volume'].append(query_context.query_volume)
        self.catalog[node]['update_times'].append(
            query_context.query_time_to_str(query_context.query_time, fmt='%Y-%m-%d %H:%M:%S'))
        self.catalog[node]['last_html'] = query_context.to_html()

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
                for target_node in targets:
                    self._lineage_graph.add_node(target_node, title=query_context.to_html())
                    self._add_node_to_catalog(target_node, query_context)
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

    def draw_graph(self, should_open_browser: bool = True) -> None:
        self._enrich_graph_with_monitoring_context()
        heading = """
<html style="box-sizing:border-box;font-family:sans-serif;-ms-text-size-adjust:100%;-webkit-text-size-adjust:100%;height:100%;overflow-y:auto;overflow-x:hidden;font-size:16px;">
   <body style="box-sizing:border-box;margin:0;height:auto;min-height:100%;position:relative;">
      <header class="u-align-center-sm u-align-center-xs u-clearfix u-header u-header" id="sec-efee" style="box-sizing:border-box;display:block;position:relative;background-image:none;text-align:center;">
         <div class="u-clearfix u-sheet u-sheet-1" style="box-sizing:border-box;position:relative;width:100%;margin:0 auto;">
            <a href="https://elementary-data.com" class="u-image u-logo u-image-1" data-image-width="650" data-image-height="150" style="box-sizing:border-box;background-color:transparent;-webkit-text-decoration-skip:objects;border-top-width:0;border-left-width:0;border-right-width:0;color:#111111;text-decoration:none;font-size:inherit;font-family:inherit;line-height:inherit;letter-spacing:inherit;text-transform:inherit;font-style:inherit;font-weight:inherit;border:0 none transparent;outline-width:0;margin:18px auto 0 11px;position:relative;object-fit:cover;display:table;vertical-align:middle;background-size:cover;background-position:50% 50%;background-repeat:no-repeat;white-space:nowrap;width:200px;height:46px;">
            <img src="/Users/oravidov/downloads/HTML_header/images/045fd385-6817-475d-86c9-c1191eaf7205.png" class="u-logo-image u-logo-image-1" style="box-sizing:border-box;border-style:none;display:block;width:100%;height:100%;"></a>
            <div class="u-social-icons u-spacing-10 u-social-icons-1" style="box-sizing:border-box;position:relative;display:flex;white-space:nowrap;height:26px;min-height:16px;width:134px;min-width:94px;margin:-46px 20px 16px auto;">
               <a class="u-social-url" target="_blank" title="Star" href="https://github.com/elementary-data/elementary-lineage/stargazers" style="box-sizing:border-box;background-color:transparent;-webkit-text-decoration-skip:objects;border-top-width:0;border-left-width:0;border-right-width:0;color:currentColor;text-decoration:none;font-size:inherit;font-family:inherit;line-height:inherit;letter-spacing:inherit;text-transform:inherit;font-style:inherit;font-weight:inherit;border:0 none transparent;outline-width:0;margin:0;margin-top:0 !important;margin-bottom:0 !important;height:100%;display:inline-block;flex:1;">
                  <span class="u-icon u-social-custom u-social-icon u-text-custom-color-1 u-icon-1" style="box-sizing:border-box;display:flex;line-height:0;border-width:0px;color:#f37474 !important;height:100%;">
                     <svg class="u-svg-link" preserveaspectratio="xMidYMin slice" viewbox="0 -10 511.98685 511" style="box-sizing:border-box;width:100%;height:100%;fill:#f37474;">
                        <use xlink:href="#svg-8a45" style="box-sizing: border-box;"></use>
                     </svg>
                     <svg class="u-svg-content" viewbox="0 -10 511.98685 511" id="svg-8a45" style="box-sizing:border-box;width:0;height:0;fill:#f37474;">
                        <path d="m510.652344 185.902344c-3.351563-10.367188-12.546875-17.730469-23.425782-18.710938l-147.773437-13.417968-58.433594-136.769532c-4.308593-10.023437-14.121093-16.511718-25.023437-16.511718s-20.714844 6.488281-25.023438 16.535156l-58.433594 136.746094-147.796874 13.417968c-10.859376 1.003906-20.03125 8.34375-23.402344 18.710938-3.371094 10.367187-.257813 21.738281 7.957031 28.90625l111.699219 97.960937-32.9375 145.089844c-2.410156 10.667969 1.730468 21.695313 10.582031 28.09375 4.757813 3.4375 10.324219 5.1875 15.9375 5.1875 4.839844 0 9.640625-1.304687 13.949219-3.882813l127.46875-76.183593 127.421875 76.183593c9.324219 5.609376 21.078125 5.097657 29.910156-1.304687 8.855469-6.417969 12.992187-17.449219 10.582031-28.09375l-32.9375-145.089844 111.699219-97.941406c8.214844-7.1875 11.351563-18.539063 7.980469-28.925781zm0 0" fill="currentColor" style="box-sizing: border-box;"></path>
                     </svg>
                  </span>
               </a>
               <a class="u-social-url" target="_blank" title="Slack" href="https://bit.ly/slack-elementary" style="box-sizing:border-box;background-color:transparent;-webkit-text-decoration-skip:objects;border-top-width:0;border-left-width:0;border-right-width:0;color:currentColor;text-decoration:none;font-size:inherit;font-family:inherit;line-height:inherit;letter-spacing:inherit;text-transform:inherit;font-style:inherit;font-weight:inherit;border:0 none transparent;outline-width:0;margin:0;margin-top:0 !important;margin-bottom:0 !important;height:100%;display:inline-block;flex:1;margin-left:10px;">
                  <span class="u-icon u-social-custom u-social-icon u-text-custom-color-1 u-icon-2" style="box-sizing:border-box;display:flex;line-height:0;border-width:0px;color:#f37474 !important;height:100%;">
                     <svg class="u-svg-link" preserveaspectratio="xMidYMin slice" viewbox="0 0 512 512" style="box-sizing:border-box;width:100%;height:100%;fill:#f37474;">
                        <use xlink:href="#svg-8896" style="box-sizing: border-box;"></use>
                     </svg>
                     <svg class="u-svg-content" viewbox="0 0 512 512" id="svg-8896" style="box-sizing:border-box;width:0;height:0;fill:#f37474;">
                        <g style="box-sizing: border-box;">
                           <path d="m467 271h-151c-24.813 0-45 20.187-45 45s20.187 45 45 45h151c24.813 0 45-20.187 45-45s-20.187-45-45-45z" style="box-sizing: border-box;"></path>
                           <path d="m196 151h-151c-24.813 0-45 20.187-45 45s20.187 45 45 45h151c24.813 0 45-20.187 45-45s-20.187-45-45-45z" style="box-sizing: border-box;"></path>
                           <path d="m316 241c24.813 0 45-20.187 45-45v-151c0-24.813-20.187-45-45-45s-45 20.187-45 45v151c0 24.813 20.187 45 45 45z" style="box-sizing: border-box;"></path>
                           <path d="m196 271c-24.813 0-45 20.187-45 45v151c0 24.813 20.187 45 45 45s45-20.187 45-45v-151c0-24.813-20.187-45-45-45z" style="box-sizing: border-box;"></path>
                           <path d="m407 241h45c33.084 0 60-26.916 60-60s-26.916-60-60-60-60 26.916-60 60v45c0 8.284 6.716 15 15 15z" style="box-sizing: border-box;"></path>
                           <path d="m105 271h-45c-33.084 0-60 26.916-60 60s26.916 60 60 60 60-26.916 60-60v-45c0-8.284-6.716-15-15-15z" style="box-sizing: border-box;"></path>
                           <path d="m181 0c-33.084 0-60 26.916-60 60s26.916 60 60 60h45c8.284 0 15-6.716 15-15v-45c0-33.084-26.916-60-60-60z" style="box-sizing: border-box;"></path>
                           <path d="m331 392h-45c-8.284 0-15 6.716-15 15v45c0 33.084 26.916 60 60 60s60-26.916 60-60-26.916-60-60-60z" style="box-sizing: border-box;"></path>
                        </g>
                     </svg>
                  </span>
               </a>
               <a class="u-social-url" target="_blank" title="Github" href="https://github.com/elementary-data/elementary-lineage" style="box-sizing:border-box;background-color:transparent;-webkit-text-decoration-skip:objects;border-top-width:0;border-left-width:0;border-right-width:0;color:currentColor;text-decoration:none;font-size:inherit;font-family:inherit;line-height:inherit;letter-spacing:inherit;text-transform:inherit;font-style:inherit;font-weight:inherit;border:0 none transparent;outline-width:0;margin:0;margin-top:0 !important;margin-bottom:0 !important;height:100%;display:inline-block;flex:1;margin-left:10px;">
                  <span class="u-icon u-social-custom u-social-icon u-text-custom-color-1 u-icon-3" style="box-sizing:border-box;display:flex;line-height:0;border-width:0px;color:#f37474 !important;height:100%;">
                     <svg class="u-svg-link" preserveaspectratio="xMidYMin slice" viewbox="0 0 512 512" style="box-sizing:border-box;width:100%;height:100%;fill:#f37474;">
                        <use xlink:href="#svg-d7b6" style="box-sizing: border-box;"></use>
                     </svg>
                     <svg class="u-svg-content" viewbox="0 0 512 512" x="0px" y="0px" id="svg-d7b6" style="enable-background:new 0 0 512 512;box-sizing:border-box;width:0;height:0;fill:#f37474;">
                        <g style="box-sizing: border-box;">
                           <g style="box-sizing: border-box;">
                              <path d="M255.968,5.329C114.624,5.329,0,120.401,0,262.353c0,113.536,73.344,209.856,175.104,243.872    c12.8,2.368,17.472-5.568,17.472-12.384c0-6.112-0.224-22.272-0.352-43.712c-71.2,15.52-86.24-34.464-86.24-34.464    c-11.616-29.696-28.416-37.6-28.416-37.6c-23.264-15.936,1.728-15.616,1.728-15.616c25.696,1.824,39.2,26.496,39.2,26.496    c22.848,39.264,59.936,27.936,74.528,21.344c2.304-16.608,8.928-27.936,16.256-34.368    c-56.832-6.496-116.608-28.544-116.608-127.008c0-28.064,9.984-51.008,26.368-68.992c-2.656-6.496-11.424-32.64,2.496-68    c0,0,21.504-6.912,70.4,26.336c20.416-5.696,42.304-8.544,64.096-8.64c21.728,0.128,43.648,2.944,64.096,8.672    c48.864-33.248,70.336-26.336,70.336-26.336c13.952,35.392,5.184,61.504,2.56,68c16.416,17.984,26.304,40.928,26.304,68.992    c0,98.72-59.84,120.448-116.864,126.816c9.184,7.936,17.376,23.616,17.376,47.584c0,34.368-0.32,62.08-0.32,70.496    c0,6.88,4.608,14.88,17.6,12.352C438.72,472.145,512,375.857,512,262.353C512,120.401,397.376,5.329,255.968,5.329z" style="box-sizing: border-box;"></path>
                           </g>
                        </g>
                     </svg>
                  </span>
               </a>
               <a class="u-social-url" target="_blank" title="Docs" href="https://docs.elementary-data.com/" style="box-sizing:border-box;background-color:transparent;-webkit-text-decoration-skip:objects;border-top-width:0;border-left-width:0;border-right-width:0;color:currentColor;text-decoration:none;font-size:inherit;font-family:inherit;line-height:inherit;letter-spacing:inherit;text-transform:inherit;font-style:inherit;font-weight:inherit;border:0 none transparent;outline-width:0;margin:0;margin-top:0 !important;margin-bottom:0 !important;height:100%;display:inline-block;flex:1;margin-left:10px;">
                  <span class="u-icon u-social-custom u-social-icon u-text-custom-color-1 u-icon-4" style="box-sizing:border-box;display:flex;line-height:0;border-width:0px;color:#f37474 !important;height:100%;">
                     <svg class="u-svg-link" preserveaspectratio="xMidYMin slice" viewbox="0 0 431.855 431.855" style="box-sizing:border-box;width:100%;height:100%;fill:#f37474;">
                        <use xlink:href="#svg-2c74" style="box-sizing: border-box;"></use>
                     </svg>
                     <svg class="u-svg-content" viewbox="0 0 431.855 431.855" x="0px" y="0px" id="svg-2c74" style="enable-background:new 0 0 431.855 431.855;box-sizing:border-box;width:0;height:0;fill:#f37474;">
                        <g style="box-sizing: border-box;">
                           <path style="fill:currentColor;box-sizing:border-box;" d="M215.936,0C96.722,0,0.008,96.592,0.008,215.814c0,119.336,96.714,216.041,215.927,216.041   c119.279,0,215.911-96.706,215.911-216.041C431.847,96.592,335.214,0,215.936,0z M231.323,335.962   c-5.015,4.463-10.827,6.706-17.411,6.706c-6.812,0-12.754-2.203-17.826-6.617c-5.08-4.406-7.625-10.575-7.625-18.501   c0-7.031,2.463-12.949,7.373-17.745c4.91-4.796,10.933-7.194,18.078-7.194c7.031,0,12.949,2.398,17.753,7.194   c4.796,4.796,7.202,10.713,7.202,17.745C238.858,325.362,236.346,331.5,231.323,335.962z M293.856,180.934   c-3.853,7.145-8.429,13.306-13.737,18.501c-5.292,5.194-14.81,13.924-28.548,26.198c-3.788,3.463-6.836,6.503-9.12,9.12   c-2.284,2.626-3.991,5.023-5.105,7.202c-1.122,2.178-1.983,4.357-2.593,6.535c-0.61,2.17-1.528,5.999-2.772,11.469   c-2.113,11.608-8.754,17.411-19.915,17.411c-5.804,0-10.681-1.894-14.656-5.69c-3.959-3.796-5.934-9.429-5.934-16.907   c0-9.372,1.455-17.493,4.357-24.361c2.886-6.869,6.747-12.892,11.543-18.086c4.804-5.194,11.274-11.356,19.427-18.501   c7.145-6.251,12.307-10.965,15.485-14.144c3.186-3.186,5.861-6.73,8.031-10.632c2.187-3.91,3.26-8.145,3.26-12.721   c0-8.933-3.308-16.46-9.957-22.597c-6.641-6.137-15.209-9.21-25.703-9.21c-12.282,0-21.321,3.097-27.125,9.291   c-5.804,6.194-10.705,15.314-14.729,27.369c-3.804,12.616-11.006,18.923-21.598,18.923c-6.251,0-11.526-2.203-15.826-6.609   c-4.292-4.406-6.438-9.177-6.438-14.314c0-10.6,3.406-21.346,10.21-32.23c6.812-10.884,16.745-19.899,29.807-27.036   c13.054-7.145,28.296-10.722,45.699-10.722c16.184,0,30.466,2.991,42.854,8.966c12.388,5.966,21.963,14.087,28.718,24.361   c6.747,10.266,10.128,21.427,10.128,33.482C299.635,165.473,297.709,173.789,293.856,180.934z"></path>
                        </g>
                     </svg>
                  </span>
               </a>
            </div>
         </div>
      </header>
   </body>
</html>
        """
        # Visualize the graph
        net = Network(height="95%", width="100%", directed=True, heading=heading)
        net.from_nx(self._lineage_graph)
        net.set_options(GRAPH_VISUALIZATION_OPTIONS)

        net.save_graph("elementary_lineage.html")
        if should_open_browser:
            webbrowser.open_new_tab('elementary_lineage.html')