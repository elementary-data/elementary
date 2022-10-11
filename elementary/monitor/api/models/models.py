import json
import os
import statistics
from collections import defaultdict
from typing import Dict, List, Optional, Union

from elementary.clients.api.api import APIClient
from elementary.monitor.api.models.schema import (
    ExposureSchema,
    ModelCoverageSchema,
    ModelRunSchema,
    ModelRunsSchema,
    ModelSchema,
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSourceSchema,
    SourceSchema,
    TotalsModelRunsSchema,
)
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class ModelsAPI(APIClient):
    def get_models_runs(
        self, days_back: Optional[int] = 7, exclude_elementary_models: bool = False
    ) -> List[ModelRunsSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="get_models_runs",
            macro_args={
                "days_back": days_back,
                "exclude_elementary": exclude_elementary_models,
            },
        )
        model_run_dicts = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )

        models_runs = defaultdict(list)
        for model_run in model_run_dicts:
            models_runs[model_run["unique_id"]].append(model_run)

        aggregated_models_runs = []
        for model_unique_id, model_runs in models_runs.items():
            totals = self._get_model_runs_totals(model_runs)
            runs = [
                ModelRunSchema(
                    id=model_run["invocation_id"],
                    time_utc=model_run["generated_at"],
                    status=model_run["status"],
                    full_refresh=model_run["full_refresh"],
                    materialization=model_run["materialization"],
                    execution_time=model_run["execution_time"],
                )
                for model_run in model_runs
            ]
            # The median should be based only on succesfull model runs.
            successful_execution_times = [
                model_run["execution_time"]
                for model_run in model_runs
                if model_run["status"].lower() == "success"
            ]
            median_execution_time = (
                statistics.median(successful_execution_times)
                if len(successful_execution_times)
                else 0
            )
            last_model_run = sorted(model_runs, key=lambda run: run["generated_at"])[-1]
            execution_time_change_rate = (
                (last_model_run["execution_time"] / median_execution_time - 1) * 100
                if median_execution_time != 0
                else 0
            )
            aggregated_models_runs.append(
                ModelRunsSchema(
                    unique_id=model_unique_id,
                    schema=last_model_run["schema"],
                    name=last_model_run["name"],
                    status=last_model_run["status"],
                    last_exec_time=last_model_run["execution_time"],
                    median_exec_time=median_execution_time,
                    exec_time_change_rate=execution_time_change_rate,
                    totals=totals,
                    runs=runs,
                )
            )
        return aggregated_models_runs

    @staticmethod
    def _get_model_runs_totals(runs: List[dict]) -> TotalsModelRunsSchema:
        error_runs = len([run for run in runs if run["status"] in ["error", "fail"]])
        seccuss_runs = len([run for run in runs if run["status"] == "success"])
        return TotalsModelRunsSchema(errors=error_runs, success=seccuss_runs)

    def get_models(
        self, exclude_elementary_models: bool = False
    ) -> Dict[str, NormalizedModelSchema]:
        models_results = self.dbt_runner.run_operation(
            macro_name="get_models",
            macro_args={"exclude_elementary": exclude_elementary_models},
        )
        models = dict()
        if models_results:
            for model_result in json.loads(models_results[0]):
                model_data = ModelSchema(**model_result)
                normalized_model = self._normalize_dbt_artifact_dict(model_data)
                model_unique_id = normalized_model.unique_id
                models[model_unique_id] = normalized_model
        return models

    def get_sources(self) -> Dict[str, NormalizedSourceSchema]:
        sources_results = self.dbt_runner.run_operation(macro_name="get_sources")
        sources = dict()
        if sources_results:
            for source_result in json.loads(sources_results[0]):
                source_data = SourceSchema(**source_result)
                normalized_source = self._normalize_dbt_artifact_dict(source_data)
                source_unique_id = normalized_source.unique_id
                sources[source_unique_id] = normalized_source
        return sources

    def get_exposures(self) -> Dict[str, NormalizedExposureSchema]:
        exposures_results = self.dbt_runner.run_operation(macro_name="get_exposures")
        exposures = dict()
        if exposures_results:
            for exposure_result in json.loads(exposures_results[0]):
                exposure_data = ExposureSchema(**exposure_result)
                normalized_exposure = self._normalize_dbt_artifact_dict(exposure_data)
                exposure_unique_id = normalized_exposure.unique_id
                exposures[exposure_unique_id] = normalized_exposure
        return exposures

    def get_test_coverages(self) -> Dict[str, ModelCoverageSchema]:
        coverage_results = self.dbt_runner.run_operation(
            macro_name="get_dbt_models_test_coverage"
        )
        coverages = dict()
        if coverage_results:
            for coverage_result in json.loads(coverage_results[0]):
                coverages[coverage_result["model_unique_id"]] = ModelCoverageSchema(
                    table_tests=coverage_result["table_tests"],
                    column_tests=coverage_result["column_tests"],
                )
        return coverages

    def _normalize_dbt_artifact_dict(
        self, artifact: Union[ModelSchema, ExposureSchema, SourceSchema]
    ) -> Union[NormalizedExposureSchema, NormalizedModelSchema, NormalizedSourceSchema]:
        schema_to_normalized_schema_map = {
            ExposureSchema: NormalizedExposureSchema,
            ModelSchema: NormalizedModelSchema,
            SourceSchema: NormalizedSourceSchema,
        }
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
        normalized_artifact["owners"] = owners
        normalized_artifact["tags"] = tags
        normalized_artifact["model_name"] = artifact_name
        normalized_artifact["normalized_full_path"] = self._normalize_artifact_path(
            artifact
        )

        return schema_to_normalized_schema_map[type(artifact)](**normalized_artifact)

    @classmethod
    def _normalize_artifact_path(
        cls,
        artifact: Union[ModelSchema, ExposureSchema, SourceSchema],
    ) -> str:
        splited_artifact_path = artifact.full_path.split(os.path.sep)
        artifact_file_name = splited_artifact_path[-1]

        # If source, change models directory into sources and file extension from .yml to .sql
        if isinstance(artifact, SourceSchema):
            if splited_artifact_path[0] == "models":
                splited_artifact_path[0] = f"sources"
            if artifact_file_name.endswith(YAML_FILE_EXTENSION):
                head, _sep, tail = artifact_file_name.rpartition(YAML_FILE_EXTENSION)
                splited_artifact_path[-1] = head + SQL_FILE_EXTENSION + tail

        # Add package name to model path
        if artifact.package_name:
            splited_artifact_path.insert(0, artifact.package_name)

        return os.path.sep.join(splited_artifact_path)
