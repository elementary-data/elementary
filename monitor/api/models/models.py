import json
import os
from typing import Optional

from clients.api.api import APIClient
from utils.json_utils import try_load_json

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class ModelsAPI(APIClient):
    def get_models(self) -> dict:
        models_results = self.dbt_runner.run_operation(macro_name='get_models')
        models = dict()
        if models_results:
            models_data = json.loads(models_results[0])
            for model_data in models_data:
                normalized_model = ModelsAPI._normalize_dbt_model_dict(model_data)
                model_unique_id = normalized_model.get('unique_id')
                models[model_unique_id] = normalized_model
        return models
    
    def get_sources(self) -> dict:
        sources_results = self.dbt_runner.run_operation(macro_name='get_sources')
        sources = dict()
        if sources_results:
            sources_data = json.loads(sources_results[0])
            for source_data in sources_data:
                normalized_source = self._normalize_dbt_model_dict(source_data, is_source=True)
                source_unique_id = normalized_source.get('unique_id')
                sources[source_unique_id] = normalized_source
        return sources

    @staticmethod
    def _normalize_dbt_model_dict(model: dict, is_source: bool = False) -> dict:
        model_name = model.get('name')

        owners = model.get('owners')
        if owners:
            loaded_owners = try_load_json(owners)
            if loaded_owners:
                owners = loaded_owners
            else:
                owners = [owners]

        tags = model.get('tags')
        if tags:
            loaded_tags = try_load_json(tags)
            if loaded_tags:
                tags = loaded_tags
            else:
                tags = [tags]
        
        normalized_model = dict(**model)
        normalized_model['owners'] = owners
        normalized_model['tags'] = tags
        normalized_model['model_name'] = model_name
        normalized_model['normalized_full_path'] = ModelsAPI._normalize_model_path(
            model_path=model.get('full_path'),
            model_package_name=model.get('package_name'),
            is_source=is_source
        )
        return normalized_model
    
    @staticmethod
    def _normalize_model_path(model_path: str, model_package_name: Optional[str] = None, is_source: bool = False) -> str:
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
