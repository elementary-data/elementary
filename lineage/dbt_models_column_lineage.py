import os.path
from collections import defaultdict
import glob
import yaml

from lineage.dbt_query import DbtQuery
from lineage.dbt_utils import get_dbt_target_dir, load_dbt_catalog, load_dbt_manifest, get_model_name


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

            model_schema_yml_file_paths = self.get_model_schema_yml_file_paths(target_model)
            for model_schema_yml_file_path in model_schema_yml_file_paths:
                with open(model_schema_yml_file_path, 'r') as model_schema_yml_file:
                    try:
                        schema_dict = yaml.safe_load(model_schema_yml_file)
                        schema_models = schema_dict['models']
                        for schema_model in schema_models:
                            if schema_model['name'] == get_model_name(target_model):
                                schema_columns = schema_model['columns']

                    except yaml.YAMLError as exc:
                        raise

            print(model_column_dependencies)
            dbt_query.draw_query_graph()


