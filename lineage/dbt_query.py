import sqlfluff
import networkx as nx
from pandas._libs.internals import defaultdict
from sqlfluff.core.linter import ParsedString
from pyvis.network import Network
from lineage.dbt_utils import get_model_name

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


class DbtQuery(object):
    def __init__(self, target_model: str, model_query_txt: str, dialect: str, column_to_source: dict,
                 source_to_column: dict) -> None:
        self.target_model = target_model
        self.model_query_txt = model_query_txt
        self.dialect = dialect
        #TODO: not sure that we need this across queries (maybe only the sources for this model will suffice)
        self.column_to_source = column_to_source
        self.source_to_column = source_to_column
        self.source_columns = []
        for source, columns in self.source_to_column.items():
            for column in columns:
                column_name = '.'.join([get_model_name(source), column])
                self.source_columns.append(column_name)
        self.query_graph = nx.DiGraph()

    @classmethod
    def extract_column_and_alias_from_select_clause(cls, select_clause_element):
        column_alias = None

        columns = [column_reference.raw.lower() for column_reference in select_clause_element.recursive_crawl('column_reference', 'wildcard_expression')]
        found_alias = [alias_ref.raw.lower().replace('as', '').strip() for alias_ref in select_clause_element.recursive_crawl('alias_expression')]
        if len(found_alias) == 1:
            column_alias = found_alias[0]

        return columns, column_alias

    @classmethod
    def extract_table_and_alias_from_from_expression(cls, from_expression):
        table = None
        table_alias = None

        found_tables = [table_reference.raw.lower() for table_reference in from_expression.recursive_crawl('table_reference')]
        if len(found_tables) == 1:
            table = found_tables[0]
        found_alias = [alias_ref.raw.lower().replace('as', '').strip() for alias_ref in from_expression.recursive_crawl('alias_expression')]
        if len(found_alias) == 1:
            table_alias = found_alias[0]

        return table, table_alias

    @classmethod
    def get_subquery_selected_columns(cls, segments):
        #TODO: can we extract sources and target here instead of sources and alias? this will simplify the code later
        column_list = []

        for seg in segments:
            if seg.is_type('select_clause_element'):
                columns, column_alias = cls.extract_column_and_alias_from_select_clause(seg)
                #TODO: currently we assume that two or more columns can compound one target column if they have the
                # same alias. Is it possible that two or more columns will be used without an alias?
                # Meaning - what do you do if you have a column like this -> select sum(col1, col2) from table1 (do we need to give it a temp name?)
                # It won't work for the CTE / subquery that comes afterwards so maybe not a real problem
                for column in columns:
                    column_list.append({'name': column, 'alias': column_alias})

        for seg in segments:
            column_list.extend(cls.get_subquery_selected_columns(seg.segments))

        return column_list

    #TODO: remove code duplication with get_cte_selected_columns
    @classmethod
    def get_subquery_selected_tables(cls, segments):
        table_list = []

        for seg in segments:
            if seg.is_type('from_expression_element'):
                table, table_alias = cls.extract_table_and_alias_from_from_expression(seg)
                if table is not None:
                    table_list.append({'name': get_model_name(table), 'alias': table_alias})
        for seg in segments:
            table_list.extend(cls.get_subquery_selected_tables(seg.segments))

        return table_list

    def get_subquery_join_columns(self, segments, subquery_sources):
        column_list = []

        for seg in segments:
            if seg.is_type('join_on_condition'):
                extracted_columns, _ = self.extract_column_and_alias_from_select_clause(seg)
                #TODO: do this with a separate expand alias function (see also TODO comment in expand_subquery_column)
                return [[self.expand_subquery_column(column, subquery_sources)[0] for column in extracted_columns]]
        for seg in segments:
            column_list.extend(self.get_subquery_join_columns(seg.segments, subquery_sources))

        return column_list

    @classmethod
    def get_ctes(cls, parsed_query: ParsedString):
        return parsed_query.tree.recursive_crawl('common_table_expression')

    @staticmethod
    def get_cte_name(cte):
        for seg in cte.segments:
            if seg.is_type('raw'):
                return seg.raw

    def expand_wildcard(self, source_name):
        return ['.'.join([source_name, c]) for c in self.source_to_column[source_name]]

    def expand_subquery_column(self, subquery_column_name: str, subquery_sources: list) -> list:
        #TODO: extract to a separate function the ability to expand a table alias - i,e c.id to customers.id
        if '.' in subquery_column_name:
            subquery_column_source, subquery_column_name = subquery_column_name.rsplit('.', 1)
            for subquery_source in subquery_sources:
                subquery_source_name = subquery_source['name']
                subquery_source_alias = subquery_source['alias']
                # Find column source
                if subquery_column_source in {subquery_source_name, subquery_source_alias}:
                    if '*' in subquery_column_name:
                        return self.expand_wildcard(subquery_source_name)
                    else:
                        return ['.'.join([subquery_source_name, subquery_column_name])]
        else:
            expanded_columns = []
            for subquery_source in subquery_sources:
                subquery_source_name = subquery_source['name']
                if '*' in subquery_column_name:
                    expanded_columns.extend(self.expand_wildcard(subquery_source_name))
                else:
                    if subquery_source_name in self.column_to_source[subquery_column_name]:
                        expanded_columns.append('.'.join([subquery_source_name, subquery_column_name]))
            return expanded_columns

    def parse_subquery(self, subquery, subquery_target: str):
        #TODO: we travere the tree multiple times, can we do it more efficently?
        subquery_columns = self.get_subquery_selected_columns(subquery.segments)
        subquery_sources = self.get_subquery_selected_tables(subquery.segments)
        subquery_join_columns = self.get_subquery_join_columns(subquery.segments, subquery_sources)

        subquery_source_to_target_column_list = set()
        for subquery_column in subquery_columns:
            subquery_column_alias = subquery_column['alias']
            subquery_column_name = subquery_column['name']
            expanded_subquery_columns = self.expand_subquery_column(subquery_column_name, subquery_sources)
            for expanded_subquery_source_column in expanded_subquery_columns:
                # TODO: handle differently between wildcard and regular column
                if subquery_column_alias is not None:
                    target_column_name = subquery_column_alias
                else:
                    target_column_name = expanded_subquery_source_column.rsplit('.', 1)[-1]

                subquery_target_column = '.'.join([subquery_target, target_column_name])
                #TODO: add cte prefix to separate cte names from real sources (maybe store them separately)\
                #TODO: maybe update outside of here

                self.column_to_source[target_column_name].add(subquery_target)
                self.source_to_column[subquery_target].add(target_column_name)

                subquery_source_to_target_column_list.add((expanded_subquery_source_column, subquery_target_column))

                for join_columns in subquery_join_columns:
                    if expanded_subquery_source_column in join_columns:
                        for join_column in join_columns:
                            subquery_source_to_target_column_list.add((join_column, subquery_target_column))

        return subquery_source_to_target_column_list

    #TODO: not sure about this implementation, should we handle subquery?
    @classmethod
    def find_last_select_statement(cls, segments):
        for seg in segments:
            if seg.is_type('select_statement'):
                return seg

        for seg in segments:
            if seg.is_type('common_table_expression'):
                continue

            select_segment = cls.find_last_select_statement(seg.segments)
            if select_segment is not None:
                return select_segment

        return None

    def update_query_graph(self, subquery_source_to_target_column_list:[]) -> None:
        for subquery_source_column, subquery_target_column in subquery_source_to_target_column_list:
            self.query_graph.add_node(subquery_source_column)
            self.query_graph.add_node(subquery_target_column)
            self.query_graph.add_edge(subquery_source_column, subquery_target_column)

    def parse(self):
        parsed_query = sqlfluff.parse(self.model_query_txt, dialect=self.dialect)
        query_ctes = self.get_ctes(parsed_query)
        for cte in query_ctes:
            subquery_source_to_target_column_list = self.parse_subquery(cte, self.get_cte_name(cte))
            self.update_query_graph(subquery_source_to_target_column_list)

        last_select = self.find_last_select_statement(parsed_query.tree.segments)
        subquery_source_to_target_column_list = self.parse_subquery(last_select, self.target_model)
        self.update_query_graph(subquery_source_to_target_column_list)

        model_columns = [target_column for _, target_column in subquery_source_to_target_column_list]
        return self.find_model_column_dependencies(model_columns)

    def find_model_column_dependencies(self, model_columns: list) -> dict:
        # source_nodes = [node for node, indegree in
        #                 self.query_graph.in_degree(self.query_graph.nodes()) if indegree == 0]
        model_column_dependencies = defaultdict(lambda: [])
        # TODO: find more efficient way to implement this and also validate that it's no a CTE node
        for model_column in model_columns:
            for source_node in self.source_columns:
                paths = list(nx.all_simple_paths(self.query_graph, source=source_node, target=model_column))
                for path in paths:
                    model_column_dependencies[model_column].append(path[0])
        return model_column_dependencies

    def draw_query_graph(self):
        net = Network(height="100%", width="100%", directed=True)
        net.from_nx(self.query_graph)
        net.set_options(GRAPH_VISUALIZATION_OPTIONS)
        net.show(f"./{self.target_model}.html")

