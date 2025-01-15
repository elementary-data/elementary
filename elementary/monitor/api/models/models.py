import json
import os
import statistics
from collections import defaultdict
from typing import Dict, List, Optional, Set, Union, cast, overload

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.monitor.api.models.schema import (
    ModelCoverageSchema,
    ModelRunSchema,
    ModelRunsSchema,
    ModelRunsWithTotalsSchema,
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSeedSchema,
    NormalizedSnapshotSchema,
    NormalizedSourceSchema,
    TotalsModelRunsSchema,
)
from elementary.monitor.api.totals_schema import TotalsSchema
from elementary.monitor.fetchers.models.models import ModelsFetcher
from elementary.monitor.fetchers.models.schema import ArtifactSchemaType, ExposureSchema
from elementary.monitor.fetchers.models.schema import (
    ModelRunSchema as FetcherModelRunSchema,
)
from elementary.monitor.fetchers.models.schema import (
    ModelSchema,
    SeedSchema,
    SnapshotSchema,
    SourceSchema,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class ModelsAPI(APIClient):
    _ARTIFACT_TYPE_DIR_MAP = {
        SeedSchema: "seeds",
        SnapshotSchema: "snapshots",
        SourceSchema: "sources",
        ModelSchema: "models",
        ExposureSchema: "exposures",
    }

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
            # The median should be based only on successful model runs.
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
                    last_generated_at=last_model_run.generated_at,
                    compiled_code=last_model_run.compiled_code,
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
        success_runs = len([run for run in runs if run.status == "success"])
        return TotalsModelRunsSchema(errors=error_runs, success=success_runs)

    def get_seeds(self) -> Dict[str, NormalizedSeedSchema]:
        seed_results = self.models_fetcher.get_seeds()
        seeds = dict()
        if seed_results:
            for seed_result in seed_results:
                normalized_seed = self._normalize_dbt_artifact_dict(seed_result)
                seed_unique_id = cast(str, normalized_seed.unique_id)
                seeds[seed_unique_id] = normalized_seed
        return seeds

    def get_snapshots(self) -> Dict[str, NormalizedSnapshotSchema]:
        snapshot_results = self.models_fetcher.get_snapshots()
        snapshots = dict()
        if snapshot_results:
            for snapshot_result in snapshot_results:
                normalized_snapshot = self._normalize_dbt_artifact_dict(snapshot_result)
                snapshot_unique_id = cast(str, normalized_snapshot.unique_id)
                snapshots[snapshot_unique_id] = normalized_snapshot
        return snapshots

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
                model_unique_id = cast(str, normalized_model.unique_id)
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

    def get_exposures(
        self,
        upstream_node_ids: Optional[List[str]] = None,
    ) -> Dict[str, NormalizedExposureSchema]:
        exposures_results = self.models_fetcher.get_exposures()
        exposures: Dict[str, NormalizedExposureSchema] = dict()
        if exposures_results:
            for exposure_result in exposures_results:
                normalized_exposure = self._normalize_dbt_artifact_dict(exposure_result)

                exposure_unique_id = normalized_exposure.unique_id
                if exposure_unique_id is None:
                    # Shouldn't happen, but handling this case for mypy
                    continue

                exposures[exposure_unique_id] = normalized_exposure

        if not upstream_node_ids:
            return exposures

        return {
            exp_id: exp
            for exp_id, exp in exposures.items()
            if self._exposure_has_upstream_node(exp, exposures, upstream_node_ids)
        }

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

    def _exposure_has_upstream_node(
        self,
        exposure: NormalizedExposureSchema,
        exposures: Dict[str, NormalizedExposureSchema],
        upstream_node_ids: List[str],
        visited: Optional[Set[str]] = None,
    ) -> bool:
        if not exposure.depends_on_nodes:
            return False

        if not visited:
            visited = set()

        return any(
            dep not in visited
            and (
                dep in upstream_node_ids
                or (
                    dep in exposures
                    and self._exposure_has_upstream_node(
                        exposures[dep],
                        exposures,
                        upstream_node_ids,
                        visited.union({dep}),
                    )
                )
            )
            for dep in exposure.depends_on_nodes
        )

    @overload
    def _normalize_dbt_artifact_dict(
        self, artifact: SeedSchema
    ) -> NormalizedSeedSchema:
        ...

    @overload
    def _normalize_dbt_artifact_dict(
        self, artifact: SnapshotSchema
    ) -> NormalizedSnapshotSchema:
        ...

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
        self,
        artifact: Union[
            SeedSchema, SnapshotSchema, ModelSchema, ExposureSchema, SourceSchema
        ],
    ) -> Union[
        NormalizedSeedSchema,
        NormalizedSnapshotSchema,
        NormalizedModelSchema,
        NormalizedExposureSchema,
        NormalizedSourceSchema,
    ]:
        schema_to_normalized_schema_map = {
            SeedSchema: NormalizedSeedSchema,
            SnapshotSchema: NormalizedSnapshotSchema,
            ExposureSchema: NormalizedExposureSchema,
            ModelSchema: NormalizedModelSchema,
            SourceSchema: NormalizedSourceSchema,
        }
        artifact_name = artifact.name
        normalized_artifact = json.loads(artifact.json())
        normalized_artifact["model_name"] = artifact_name

        fqn = self._fqn(artifact)
        normalized_artifact["fqn"] = fqn
        normalized_artifact["normalized_full_path"] = self._normalize_artifact_path(
            artifact, fqn
        )

        return schema_to_normalized_schema_map[type(artifact)](**normalized_artifact)

    @classmethod
    def _normalize_artifact_path(cls, artifact: ArtifactSchemaType, fqn: str) -> str:
        if artifact.full_path is None:
            raise Exception("Artifact full path can't be null")

        if (
            isinstance(artifact, ExposureSchema)
            and artifact.meta
            and artifact.meta.get("platform")
        ):
            split_artifact_path = [artifact.meta["platform"], *fqn.split("/")]
        else:
            artifact_dir_name = cls._ARTIFACT_TYPE_DIR_MAP[type(artifact)]
            split_artifact_path = [
                artifact.package_name,
                artifact_dir_name,
                # Remove dbt's 'models-path' directory.
                *artifact.full_path.split(os.path.sep)[1:],
            ]

        return os.path.sep.join(split_artifact_path)

    @classmethod
    def _fqn(
        cls,
        artifact: Union[
            ModelSchema, ExposureSchema, SourceSchema, SeedSchema, SnapshotSchema
        ],
    ) -> str:
        if isinstance(artifact, ExposureSchema):
            path = (artifact.meta or {}).get("path")
            name = artifact.label or artifact.name or "N/A"
            fqn = f"{path}/{name}" if path else name
            return fqn

        fqn = (
            f"{artifact.database_name}.{artifact.schema_name}.{artifact.table_name}"
            if artifact.database_name is not None and artifact.schema_name is not None
            else artifact.table_name
        )

        return fqn.lower()
