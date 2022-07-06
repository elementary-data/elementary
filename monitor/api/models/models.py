import json
import os
from typing import Dict, Optional

from clients.api.api import APIClient
from monitor.api.models.schema import ModelCoverageSchema, ModelSchema, NormalizedModelSchema
from utils.json_utils import try_load_json

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class ModelsAPI(APIClient):
    def get_models(self) -> Dict[str, NormalizedModelSchema]:
        models_results = self.dbt_runner.run_operation(macro_name="get_models")
        models = dict()
        if models_results:
            for model_result in json.loads(models_results[0]):
                model_data = ModelSchema(**model_result)
                normalized_model = ModelsAPI._normalize_dbt_model_dict(model_data)
                model_unique_id = normalized_model.unique_id
                models[model_unique_id] = normalized_model
        return models
    
    def get_sources(self) -> Dict[str, NormalizedModelSchema]:
        sources_results = self.dbt_runner.run_operation(macro_name="get_sources")
        sources = dict()
        if sources_results:
            for source_result in json.loads(sources_results[0]):
                source_data = ModelSchema(**source_result)
                normalized_source = self._normalize_dbt_model_dict(source_data, is_source=True)
                source_unique_id = normalized_source.unique_id
                sources[source_unique_id] = normalized_source
        return sources
    
    def get_test_coverages(self) -> Dict[str, ModelCoverageSchema]:
        coverage_results = self.dbt_runner.run_operation(macro_name="get_dbt_models_test_coverage")
        coverages = dict()
        if coverage_results:
            for coverage_result in json.loads(coverage_results[0]):
                coverages[coverage_result["model_unique_id"]] = ModelCoverageSchema(
                    table_tests=coverage_result["table_tests"],
                    column_tests=coverage_result["column_tests"]
                )
        return coverages

    @staticmethod
    def _normalize_dbt_model_dict(model: ModelSchema, is_source: bool = False) -> NormalizedModelSchema:
        model_name = model.name

        owners = model.owners
        if owners:
            loaded_owners = try_load_json(owners)
            if loaded_owners is not None:
                owners = loaded_owners
            else:
                owners = [owners]

        tags = model.tags
        if tags:
            loaded_tags = try_load_json(tags)
            if loaded_tags is not None:
                tags = loaded_tags
            else:
                tags = [tags]
        
        normalized_model = json.loads(model.json())
        normalized_model['owners'] = owners
        normalized_model['tags'] = tags
        normalized_model['model_name'] = model_name
        normalized_model['normalized_full_path'] = ModelsAPI._normalize_model_path(
            model_path=model.full_path,
            model_package_name=model.package_name,
            is_source=is_source
        )
        return NormalizedModelSchema(**normalized_model)
    
    @classmethod
    def _normalize_model_path(cls, model_path: str, model_package_name: Optional[str] = None, is_source: bool = False) -> str:
        splited_model_path = model_path.split(os.path.sep)
        model_file_name = splited_model_path[-1]

        # If source, change models directory into sources and file extension from .yml to .sql
        if is_source:
            if splited_model_path[0] == "models":
                splited_model_path[0] = "sources"
            if model_file_name.endswith(YAML_FILE_EXTENSION):
                head, _sep, tail = model_file_name.rpartition(YAML_FILE_EXTENSION)
                splited_model_path[-1] = head + SQL_FILE_EXTENSION + tail
        
        # Add package name to model path
        if model_package_name:
            splited_model_path.insert(0, model_package_name)
        
        return os.path.sep.join(splited_model_path)
