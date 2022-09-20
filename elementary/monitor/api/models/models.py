from collections import defaultdict
import json
import os
from typing import Dict, List, Optional, Union

from elementary.clients.api.api import APIClient
from elementary.monitor.api.models.schema import (
    AggregatedModelRunsSchema,
    ExposureSchema,
    ModelCoverageSchema,
    ModelRunSchema,
    ModelRunsSchema,
    ModelSchema,
    ModelUniqueIdType,
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSourceSchema,
    SourceSchema,
    TotalsModelRunsSchema
)
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class ModelsAPI(APIClient):
    def get_aggregated_models_runs(self, days_back: Optional[int] = 7) -> List[AggregatedModelRunsSchema]:
        run_operation_response = self.dbt_runner.run_operation(macro_name='get_aggregated_models_runs',
                                                               macro_args=dict(days_back=days_back))
        aggregated_models_runs = json.loads(run_operation_response[0]) if run_operation_response else []
        models_runs = self.get_models_runs(days_back=days_back)
        aggregated_models_runs_with_totals = []
        for aggregated_model_runs in aggregated_models_runs:
            try:
                model_runs = models_runs[aggregated_model_runs['unique_id']]
                aggregated_models_runs_with_totals.append(
                    AggregatedModelRunsSchema(
                        **aggregated_model_runs,
                        totals=model_runs.totals,
                        runs=model_runs.runs
                    )
                )
            
            except Exception:
                logger.error(
                    f"Could not parse model ({aggregated_models_runs_with_totals.get('unique_id')}) - continue to the next model")
                continue
        return aggregated_models_runs_with_totals

    def get_models_runs(self, days_back: Optional[int] = 7) -> Dict[ModelUniqueIdType, ModelRunsSchema]:
        run_operation_response = self.dbt_runner.run_operation(macro_name='get_models_runs',
                                                               macro_args=dict(days_back=days_back))
        model_runs_dicts = json.loads(run_operation_response[0]) if run_operation_response else []
        grouped_models_runs = defaultdict(list)
        for model_run in model_runs_dicts:
            try:
                grouped_models_runs[model_run['unique_id']].append(
                    ModelRunSchema(
                        id=model_run['invocation_id'],
                        time_utc=model_run['generated_at'],
                        status=model_run['status'],
                        full_refresh=model_run['full_refresh'],
                        materialization=model_run['materialization']
                    )
                )
            except Exception:
                logger.error(
                    f"Could not parse model ({model_run.get('unique_id')}) run ({model_run.get('invocation_id')}) - continue to the next model run")
                continue
        
        models_runs = dict()
        for model_unique_id, model_runs in grouped_models_runs.items():
            totals = self._get_model_runs_totals(model_runs)
            models_runs[model_unique_id] = ModelRunsSchema(
                totals=totals,
                runs=model_runs,
            )
        return models_runs

    @staticmethod
    def _get_model_runs_totals(runs: List[ModelRunSchema]) -> TotalsModelRunsSchema:
        error_runs = len([run for run in runs if run.status in ["error", "fail"]])
        seccuss_runs = len([run for run in runs if run.status == "success"])
        return TotalsModelRunsSchema(
            errors=error_runs,
            success=seccuss_runs
        )

    def get_models(self) -> Dict[str, NormalizedModelSchema]:
        models_results = self.dbt_runner.run_operation(macro_name="get_models")
        models = dict()
        if models_results:
            for model_result in json.loads(models_results[0]):
                model_data = ModelSchema(**model_result)
                normalized_model = ModelsAPI._normalize_dbt_artifact_dict(model_data, type="model")
                model_unique_id = normalized_model.unique_id
                models[model_unique_id] = normalized_model
        return models
    
    def get_sources(self) -> Dict[str, NormalizedSourceSchema]:
        sources_results = self.dbt_runner.run_operation(macro_name="get_sources")
        sources = dict()
        if sources_results:
            for source_result in json.loads(sources_results[0]):
                source_data = SourceSchema(**source_result)
                normalized_source = self._normalize_dbt_artifact_dict(source_data, type="source")
                source_unique_id = normalized_source.unique_id
                sources[source_unique_id] = normalized_source
        return sources
    
    def get_exposures(self) -> Dict[str, NormalizedExposureSchema]:
        exposures_results = self.dbt_runner.run_operation(macro_name="get_exposures")
        exposures = dict()
        if exposures_results:
            for exposure_result in json.loads(exposures_results[0]):
                exposure_data = ExposureSchema(**exposure_result)
                normalized_exposure = self._normalize_dbt_artifact_dict(exposure_data, type="exposure")
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
    def _normalize_dbt_artifact_dict(
        artifact: Union[ModelSchema, ExposureSchema, SourceSchema],
        type: str
    ) -> Union[NormalizedExposureSchema, NormalizedModelSchema, NormalizedSourceSchema]:
        artifact_name = artifact.name

        owners = artifact.owners
        if owners:
            loaded_owners = try_load_json(owners)
            if loaded_owners is not None:
                owners = loaded_owners
            else:
                owners = [owners]

        tags = artifact.tags
        if tags:
            loaded_tags = try_load_json(tags)
            if loaded_tags is not None:
                tags = loaded_tags
            else:
                tags = [tags]
        
        normalized_artifact = json.loads(artifact.json())
        normalized_artifact['owners'] = owners
        normalized_artifact['tags'] = tags
        normalized_artifact['model_name'] = artifact_name
        normalized_artifact['normalized_full_path'] = ModelsAPI._normalize_artifact_path(
            artifact_path=artifact.full_path,
            artifact_package_name=artifact.package_name,
            type=type
        )

        if type == "exposure":
            return NormalizedExposureSchema(**normalized_artifact)
        elif type == "model":
            return NormalizedModelSchema(**normalized_artifact)
        elif type == "source":
            return NormalizedSourceSchema(**normalized_artifact)
    
    @classmethod
    def _normalize_artifact_path(
        cls,
        artifact_path: str,
        type: str,
        artifact_package_name: Optional[str] = None,
    ) -> str:
        splited_artifact_path = artifact_path.split(os.path.sep)
        artifact_file_name = splited_artifact_path[-1]

        # If source, change models directory into sources and file extension from .yml to .sql
        if type == "source":
            if splited_artifact_path[0] == "models":
                splited_artifact_path[0] = f"sources"
            if artifact_file_name.endswith(YAML_FILE_EXTENSION):
                head, _sep, tail =artifact_file_name.rpartition(YAML_FILE_EXTENSION)
                splited_artifact_path[-1] = head + SQL_FILE_EXTENSION + tail
        
        # Add package name to model path
        if artifact_package_name:
            splited_artifact_path.insert(0, artifact_package_name)
        
        return os.path.sep.join(splited_artifact_path)
