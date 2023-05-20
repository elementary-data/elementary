import json
import os
import statistics
from collections import defaultdict
from typing import Dict, List, Optional, Union, overload

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.monitor.api.models.schema import (
    ModelCoverageSchema,
    ModelRunSchema,
    ModelRunsSchema,
    ModelRunsWithTotalsSchema,
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSourceSchema,
    TotalsModelRunsSchema,
    TotalsSchema,
)
from elementary.monitor.fetchers.models.models import ModelsFetcher
from elementary.monitor.fetchers.models.schema import ArtifactSchemaType, ExposureSchema
from elementary.monitor.fetchers.models.schema import (
    ModelRunSchema as FetcherModelRunSchema,
)
from elementary.monitor.fetchers.models.schema import ModelSchema, SourceSchema
from elementary.utils.log import get_logger

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class ModelsAPI(APIClient):
    def __init__(self, dbt_runner: BaseDbtRunner):
        super().__init__(dbt_runner)
        self.models_fetcher = ModelsFetcher(dbt_runner=self.dbt_runner)

    def get_models_runs(
        self, days_back: Optional[int] = 7, exclude_elementary_models: bool = False
    ) -> ModelRunsWithTotalsSchema:
        model_runs_results = self.models_fetcher.get_models_runs(
            days_back=days_back, exclude_elementary_models=exclude_elementary_models
        )

        model_id_to_runs_map = defaultdict(list)
        for model_run in model_runs_results:
            model_id_to_runs_map[model_run.unique_id].append(model_run)

        aggregated_models_runs = []
        for model_unique_id, model_runs in model_id_to_runs_map.items():
            totals = self._get_model_runs_totals(model_runs)
            runs = [
                ModelRunSchema(
                    id=model_run.invocation_id,
                    time_utc=model_run.generated_at,
                    status=model_run.status,
                    full_refresh=model_run.full_refresh,
                    materialization=model_run.materialization,
                    execution_time=model_run.execution_time,
                )
                for model_run in model_runs
            ]
            # The median should be based only on succesfull model runs.
            successful_execution_times = [
                model_run.execution_time
                for model_run in model_runs
                if model_run.status.lower() == "success"
            ]
            median_execution_time = (
                statistics.median(successful_execution_times)
                if len(successful_execution_times)
                else 0
            )
            last_model_run = sorted(model_runs, key=lambda run: run.generated_at)[-1]
            execution_time_change_rate = (
                (last_model_run.execution_time / median_execution_time - 1) * 100
                if median_execution_time != 0
                else 0
            )
            aggregated_models_runs.append(
                ModelRunsSchema(
                    unique_id=model_unique_id,
                    schema=last_model_run.schema_name,
                    name=last_model_run.name,
                    status=last_model_run.status,
                    last_exec_time=last_model_run.execution_time,
                    median_exec_time=median_execution_time,
                    exec_time_change_rate=execution_time_change_rate,
                    totals=totals,
                    runs=runs,
                )
            )

        model_runs_totals = {}
        for aggregated_model_run in aggregated_models_runs:
            model_runs_totals[aggregated_model_run.unique_id] = TotalsSchema(
                errors=aggregated_model_run.totals.errors,
                warnings=0,
                failures=0,
                passed=aggregated_model_run.totals.success,
            )
        return ModelRunsWithTotalsSchema(
            runs=aggregated_models_runs, totals=model_runs_totals
        )

    @staticmethod
    def _get_model_runs_totals(
        runs: List[FetcherModelRunSchema],
    ) -> TotalsModelRunsSchema:
        error_runs = len([run for run in runs if run.status in ["error", "fail"]])
        seccuss_runs = len([run for run in runs if run.status == "success"])
        return TotalsModelRunsSchema(errors=error_runs, success=seccuss_runs)

    def get_models(
        self, exclude_elementary_models: bool = False
    ) -> Dict[str, NormalizedModelSchema]:
        models_results = self.models_fetcher.get_models(
            exclude_elementary_models=exclude_elementary_models
        )
        models = dict()
        if models_results:
            for model_result in models_results:
                normalized_model = self._normalize_dbt_artifact_dict(model_result)

                model_unique_id = normalized_model.unique_id
                if model_unique_id is None:
                    # Shouldn't happen, but handling this case for mypy
                    continue

                models[model_unique_id] = normalized_model
        return models

    def get_sources(self) -> Dict[str, NormalizedSourceSchema]:
        sources_results = self.models_fetcher.get_sources()
        sources = dict()
        if sources_results:
            for source_result in sources_results:
                normalized_source = self._normalize_dbt_artifact_dict(source_result)

                source_unique_id = normalized_source.unique_id
                if source_unique_id is None:
                    # Shouldn't happen, but handling this case for mypy
                    continue

                sources[source_unique_id] = normalized_source
        return sources

    def get_exposures(self) -> Dict[str, NormalizedExposureSchema]:
        exposures_results = self.models_fetcher.get_exposures()
        exposures = dict()
        if exposures_results:
            for exposure_result in exposures_results:
                normalized_exposure = self._normalize_dbt_artifact_dict(exposure_result)

                exposure_unique_id = normalized_exposure.unique_id
                if exposure_unique_id is None:
                    # Shouldn't happen, but handling this case for mypy
                    continue

                exposures[exposure_unique_id] = normalized_exposure
        return exposures

    def get_test_coverages(self) -> Dict[str, ModelCoverageSchema]:
        coverage_results = self.models_fetcher.get_test_coverages()
        coverages = dict()
        if coverage_results:
            for coverage_result in coverage_results:
                if coverage_result.model_unique_id is None:
                    # Shouldn't happen, but handling this case for mypy
                    continue

                coverages[coverage_result.model_unique_id] = ModelCoverageSchema(
                    table_tests=coverage_result.table_tests,
                    column_tests=coverage_result.column_tests,
                )
        return coverages

    @overload
    def _normalize_dbt_artifact_dict(
        self, artifact: ModelSchema
    ) -> NormalizedModelSchema:
        ...

    @overload
    def _normalize_dbt_artifact_dict(
        self, artifact: ExposureSchema
    ) -> NormalizedExposureSchema:
        ...

    @overload
    def _normalize_dbt_artifact_dict(
        self, artifact: SourceSchema
    ) -> NormalizedSourceSchema:
        ...

    def _normalize_dbt_artifact_dict(
        self, artifact: Union[ModelSchema, ExposureSchema, SourceSchema]
    ) -> Union[NormalizedModelSchema, NormalizedExposureSchema, NormalizedSourceSchema]:
        schema_to_normalized_schema_map = {
            ExposureSchema: NormalizedExposureSchema,
            ModelSchema: NormalizedModelSchema,
            SourceSchema: NormalizedSourceSchema,
        }
        artifact_name = artifact.name
        normalized_artifact = json.loads(artifact.json())
        normalized_artifact["model_name"] = artifact_name
        normalized_artifact["normalized_full_path"] = self._normalize_artifact_path(
            artifact
        )

        return schema_to_normalized_schema_map[type(artifact)](**normalized_artifact)

    @classmethod
    def _normalize_artifact_path(
        cls,
        artifact: Union[ArtifactSchemaType],
    ) -> str:
        if artifact.full_path is None:
            raise Exception("Artifact full path can't be null")

        splited_artifact_path = artifact.full_path.split(os.path.sep)
        artifact_file_name = splited_artifact_path[-1]

        # If source, change models directory into sources and file extension from .yml to .sql
        if isinstance(artifact, SourceSchema):
            if splited_artifact_path[0] == "models":
                splited_artifact_path[0] = "sources"
            if artifact_file_name.endswith(YAML_FILE_EXTENSION):
                head, _sep, tail = artifact_file_name.rpartition(YAML_FILE_EXTENSION)
                splited_artifact_path[-1] = head + SQL_FILE_EXTENSION + tail

        # Add package name to model path
        if artifact.package_name:
            splited_artifact_path.insert(0, artifact.package_name)

        return os.path.sep.join(splited_artifact_path)
