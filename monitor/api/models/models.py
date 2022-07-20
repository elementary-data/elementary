import json
import os
from typing import Dict, Literal, Optional, Union

from clients.api.api import APIClient
from monitor.api.models.schema import ExposureSchema, ModelCoverageSchema, ModelSchema, NormalizedExposureSchema, NormalizedModelSchema
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
                normalized_model = ModelsAPI._normalize_dbt_node_dict(model_data, type="model")
                model_unique_id = normalized_model.unique_id
                models[model_unique_id] = normalized_model
        return models
    
    def get_sources(self) -> Dict[str, NormalizedModelSchema]:
        sources_results = self.dbt_runner.run_operation(macro_name="get_sources")
        sources = dict()
        if sources_results:
            for source_result in json.loads(sources_results[0]):
                source_data = ModelSchema(**source_result)
                normalized_source = self._normalize_dbt_node_dict(source_data, type="source")
                source_unique_id = normalized_source.unique_id
                sources[source_unique_id] = normalized_source
        return sources
    
    def get_exposures(self):
        exposures_results = self.dbt_runner.run_operation(macro_name="get_exposures")
        exposures = dict()
        if exposures_results:
            for exposure_result in json.loads(exposures_results[0]):
                exposure_data = ExposureSchema(**exposure_result)
                normalized_exposure = self._normalize_dbt_node_dict(exposure_data, type="exposure")
                exposure_unique_id = normalized_exposure.unique_id
                exposures[exposure_unique_id] = normalized_exposure
        return exposures
    
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
    def _normalize_dbt_node_dict(
        node: Union[ModelSchema, ExposureSchema],
        type: Literal["model", "source", "exposure"]
    ) -> Union[NormalizedExposureSchema, NormalizedModelSchema]:
        node_name = node.name

        owners = node.owners
        if owners:
            loaded_owners = try_load_json(owners)
            if loaded_owners is not None:
                owners = loaded_owners
            else:
                owners = [owners]

        tags = node.tags
        if tags:
            loaded_tags = try_load_json(tags)
            if loaded_tags is not None:
                tags = loaded_tags
            else:
                tags = [tags]
        
        normalized_node = json.loads(node.json())
        normalized_node['owners'] = owners
        normalized_node['tags'] = tags
        normalized_node['model_name'] = node_name
        normalized_node['normalized_full_path'] = ModelsAPI._normalize_node_path(
            node_path=node.full_path,
            node_package_name=node.package_name,
            type=type
        )

        if type == "exposure":
            return NormalizedExposureSchema(**normalized_node)
        else:
            return NormalizedModelSchema(**normalized_node)
    
    @classmethod
    def _normalize_node_path(
        cls,
        node_path: str,
        type: Literal["model", "source", "exposure"],
        node_package_name: Optional[str] = None,
    ) -> str:
        splited_node_path = node_path.split(os.path.sep)
        node_file_name = splited_node_path[-1]

        # If not a model, change models directory into the right directory and file extension from .yml to .sql
        if type != "model":
            if splited_node_path[0] == "models":
                splited_node_path[0] = f"{type}s"
            if node_file_name.endswith(YAML_FILE_EXTENSION):
                head, _sep, tail =node_file_name.rpartition(YAML_FILE_EXTENSION)
                splited_node_path[-1] = head + SQL_FILE_EXTENSION + tail
        
        # Add package name to model path
        if node_package_name:
            splited_node_path.insert(0, node_package_name)
        
        return os.path.sep.join(splited_node_path)
