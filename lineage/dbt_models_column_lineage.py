import os.path
from collections import defaultdict
import glob
import yaml

from lineage.dbt_query import DbtQuery
from lineage.dbt_utils import get_dbt_target_dir, load_dbt_catalog, load_dbt_manifest, get_model_name
from lineage.lineage_graph import LineageGraph

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


class DbtModelsColumnLineage(object):

    def __init__(self, dbt_dir: str) -> None:

        self.dbt_dir = dbt_dir
        self.dbt_target_dir = get_dbt_target_dir(self.dbt_dir)
        self.dbt_catalog = load_dbt_catalog(self.dbt_target_dir)
        self.dbt_manifest = load_dbt_manifest(self.dbt_target_dir)

    @staticmethod
    def get_model_columns(model: str, dbt_catalog: dict) -> set:
        return set(column['name'].lower() for column in dbt_catalog['nodes'][model]['columns'].values())

    def load_columns_from_catalog(self, source_models: list) -> (dict, dict):
        column_to_source = defaultdict(lambda: set())
        source_to_column = defaultdict(lambda: set())
        for source_model in source_models:
            source_columns = self.get_model_columns(source_model, self.dbt_catalog)
            for source_column in source_columns:
                source_model_name = get_model_name(source_model)
                column_to_source[source_column].add(source_model_name)
                source_to_column[source_model_name].add(source_column)
        return column_to_source, source_to_column

    def get_model_schema_yml_file_paths(self, target_model: str) -> list:
        model_root_path = self.dbt_manifest['nodes'][target_model]['root_path']
        model_relative_file_path = self.dbt_manifest['nodes'][target_model]['original_file_path']
        model_full_path = os.path.join(model_root_path, model_relative_file_path)
        model_schema_yml_path = os.path.dirname(model_full_path)
        return glob.glob(os.path.join(model_schema_yml_path, '*.yml'))

    def enrich_schema_files(self):
        models = [node for node in self.dbt_manifest['nodes'] if node.startswith('model.')]
        for target_model in models:
            source_models = self.dbt_manifest['nodes'][target_model]['depends_on']['nodes']
            column_to_source, source_to_column = self.load_columns_from_catalog(source_models)
            model_query_text = self.dbt_manifest['nodes'][target_model]['compiled_sql']
            dialect = self.dbt_manifest['metadata']['adapter_type']
            dbt_query = DbtQuery(target_model, model_query_text, dialect, column_to_source, source_to_column)
            model_column_dependencies = dbt_query.parse()
            model_catalog_columns = self.get_model_columns(target_model, self.dbt_catalog)

            model_schema_yml_file_paths = self.get_model_schema_yml_file_paths(target_model)
            for model_schema_yml_file_path in model_schema_yml_file_paths:
                with open(model_schema_yml_file_path, 'r') as model_schema_yml_file:
                    try:
                        schema_dict = yaml.safe_load(model_schema_yml_file)
                        schema_models = schema_dict['models']
                        for schema_model in schema_models:
                            if schema_model['name'] == get_model_name(target_model):
                                schema_columns = schema_model['columns']
                                for model_column in model_catalog_columns:
                                    found = False
                                    column_dependencies = model_column_dependencies.get('.'.join([target_model,
                                                                                                  model_column]))
                                    if column_dependencies is not None:
                                        for schema_column in schema_columns:
                                            if model_column == schema_column['name']:
                                                found = True
                                                schema_column['meta'] = {'depends_on': column_dependencies}

                                        if not found:
                                            schema_columns.append({'name': model_column,
                                                                   'meta': {'depends_on': column_dependencies}})
                    except yaml.YAMLError as exc:
                        raise

                with open(model_schema_yml_file_path, 'w') as model_schema_yml_file:
                    yaml.safe_dump(schema_dict, model_schema_yml_file, sort_keys=False)
                    #model_schema_yml_file.write(yaml.dump(schema_dict))
                    print(f'Updated {model_schema_yml_file.name} for model {target_model} successfully')

            #print(model_column_dependencies)
            #dbt_query.draw_query_graph()

    def draw_lineage_from_schema_files(self, column: str, direction: str, depth: str):
        import networkx as nx
        from pyvis.network import Network
        from pathlib import Path

        lineage_graph = LineageGraph(show_isolated_nodes=True)

        schema_file_paths = Path(os.path.join(self.dbt_dir, 'models')).rglob('*.yml')
        for schema_file_path in schema_file_paths:
            with open(schema_file_path, 'r') as schema_file:
                schema_dict = yaml.safe_load(schema_file)
                for schema_model in schema_dict['models']:
                    for schema_column in schema_model['columns']:
                        column_dependencies = schema_column.get('meta', {}).get('depends_on')
                        target_column = '.'.join([schema_model['name'], schema_column['name']])
                        taregts = {target_column}
                        sources = set()
                        if column_dependencies is not None:
                            sources = set(column_dependencies)

                        column_html = f"""
                                            <html>
                                                <body>
                                                    <div style="font-family:arial;color:DarkSlateGrey;font-size:110%;">
                                                        <strong>
                                                            Column details</br>
                                                        </strong>
                                                        <div style="min-width:75px;display:inline-block">Description:</div> {schema_column.get('description', 'No description defined')}</br>
                                                        <div style="min-width:75px;display:inline-block">Tests:</div> {schema_column.get('tests', 'No tests defined')}</br>
                                                    </div>
                                                </body>
                                            </html>
                                      """

                        lineage_graph._add_nodes_and_edges(sources=sources, targets=taregts,
                                                           query_context_html=column_html)

        if column is not None:
            lineage_graph.filter_on_table(column, direction, depth)

        lineage_graph.draw_graph()

