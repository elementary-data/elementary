from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple, Union

from elementary.clients.api.api_client import APIClient
from elementary.monitor.api.filters.filters import FiltersAPI
from elementary.monitor.api.groups.groups import GroupsAPI
from elementary.monitor.api.groups.schema import GroupsSchema
from elementary.monitor.api.invocations.invocations import InvocationsAPI
from elementary.monitor.api.lineage.lineage import LineageAPI
from elementary.monitor.api.models.models import ModelsAPI
from elementary.monitor.api.models.schema import (
    ModelCoverageSchema,
    ModelRunsSchema,
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSeedSchema,
    NormalizedSnapshotSchema,
    NormalizedSourceSchema,
)
from elementary.monitor.api.report.schema import ReportDataEnvSchema, ReportDataSchema
from elementary.monitor.api.report.totals_utils import (
    get_total_test_results,
    get_total_test_runs,
)
from elementary.monitor.api.source_freshnesses.schema import (
    SourceFreshnessResultSchema,
    SourceFreshnessRunSchema,
)
from elementary.monitor.api.source_freshnesses.source_freshnesses import (
    SourceFreshnessesAPI,
)
from elementary.monitor.api.tests.schema import (
    TestResultSchema,
    TestRunSchema,
    TestSchema,
)
from elementary.monitor.api.tests.tests import TestsAPI
from elementary.monitor.api.totals_schema import TotalsSchema
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema
from elementary.monitor.fetchers.tests.schema import NormalizedTestSchema
from elementary.utils.time import get_now_utc_iso_format


class ReportAPI(APIClient):
    def _get_groups(
        self,
        models: Iterable[NormalizedModelSchema],
        sources: Iterable[NormalizedSourceSchema],
        exposures: Iterable[NormalizedExposureSchema],
        seeds: Iterable[NormalizedSeedSchema],
        snapshots: Iterable[NormalizedSnapshotSchema],
        singular_tests: Iterable[NormalizedTestSchema],
    ) -> GroupsSchema:
        groups_api = GroupsAPI(self.dbt_runner)
        return groups_api.get_groups(
            artifacts=[
                *models,
                *sources,
                *exposures,
                *seeds,
                *snapshots,
                *singular_tests,
            ]
        )

    def _get_exposures(
        self, models_api: ModelsAPI, upstream_node_ids: Optional[List[str]] = None
    ) -> Dict[str, NormalizedExposureSchema]:
        return models_api.get_exposures(upstream_node_ids=upstream_node_ids)

    def get_report_data(
        self,
        days_back: int = 7,
        test_runs_amount: int = 720,
        disable_passed_test_metrics: bool = False,
        exclude_elementary_models: bool = False,
        project_name: Optional[str] = None,
        disable_samples: bool = False,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
        env: Optional[str] = None,
        warehouse_type: Optional[str] = None,
    ) -> Tuple[ReportDataSchema, Optional[Exception]]:
        try:
            tests_api = TestsAPI(
                dbt_runner=self.dbt_runner,
                days_back=days_back,
                invocations_per_test=test_runs_amount,
                disable_passed_test_metrics=disable_passed_test_metrics,
            )
            source_freshnesses_api = SourceFreshnessesAPI(
                dbt_runner=self.dbt_runner,
                days_back=days_back,
                invocations_per_test=test_runs_amount,
            )
            models_api = ModelsAPI(dbt_runner=self.dbt_runner)
            lineage_api = LineageAPI(dbt_runner=self.dbt_runner)
            filters_api = FiltersAPI(dbt_runner=self.dbt_runner)
            invocations_api = InvocationsAPI(dbt_runner=self.dbt_runner)

            lineage_node_ids: List[str] = []
            seeds = models_api.get_seeds()
            lineage_node_ids.extend(seeds.keys())
            snapshots = models_api.get_snapshots()
            lineage_node_ids.extend(snapshots.keys())
            models = models_api.get_models(exclude_elementary_models)
            lineage_node_ids.extend(models.keys())
            sources = models_api.get_sources()
            lineage_node_ids.extend(sources.keys())
            exposures = self._get_exposures(
                models_api, upstream_node_ids=lineage_node_ids
            )
            lineage_node_ids.extend(exposures.keys())
            singular_tests = tests_api.get_singular_tests()

            groups = self._get_groups(
                models.values(),
                sources.values(),
                exposures.values(),
                seeds.values(),
                snapshots.values(),
                singular_tests,
            )

            models_runs = models_api.get_models_runs(
                days_back=days_back, exclude_elementary_models=exclude_elementary_models
            )
            coverages = models_api.get_test_coverages()

            tests = tests_api.get_tests()
            test_invocation = invocations_api.get_test_invocation_from_filter(filter)

            test_results = tests_api.get_test_results(
                invocation_id=test_invocation.invocation_id,
                disable_samples=disable_samples,
            )
            source_freshness_results = (
                source_freshnesses_api.get_source_freshness_results()
            )

            union_test_results = {
                x: test_results.get(x, []) + source_freshness_results.get(x, [])
                for x in set(test_results).union(source_freshness_results)
            }

            test_results_totals = get_total_test_results(union_test_results)

            test_runs = tests_api.get_test_runs()
            source_freshness_runs = source_freshnesses_api.get_source_freshness_runs()

            union_test_runs = dict()
            for key in set(test_runs).union(source_freshness_runs):
                test_run = test_runs.get(key, [])
                source_freshness_run = (
                    source_freshness_runs.get(key, []) if key is not None else []
                )
                union_test_runs[key] = test_run + source_freshness_run

            test_runs_totals = get_total_test_runs(union_test_runs)

            lineage = lineage_api.get_lineage(
                lineage_node_ids, exclude_elementary_models
            )
            filters = filters_api.get_filters(
                test_results_totals, test_runs_totals, models, sources, models_runs.runs
            )

            serializable_groups = groups.dict()
            serializable_models = self._serialize_models(
                models, sources, exposures, seeds, snapshots
            )
            serializable_model_runs = self._serialize_models_runs(models_runs.runs)
            serializable_model_runs_totals = models_runs.dict(include={"totals"})[
                "totals"
            ]
            serializable_models_coverages = self._serialize_coverages(coverages)
            serializable_tests = self._serialize_tests(tests)
            serializable_test_results = self._serialize_test_results(union_test_results)
            serializable_test_results_totals = self._serialize_totals(
                test_results_totals
            )
            serializable_test_runs = self._serialize_test_runs(union_test_runs)
            serializable_test_runs_totals = self._serialize_totals(test_runs_totals)
            serializable_invocation = test_invocation.dict()
            serializable_filters = filters.dict()
            serializable_lineage = lineage.dict()

            models_latest_invocation = invocations_api.get_models_latest_invocation()
            invocations = invocations_api.get_models_latest_invocations_data()

            invocations_job_identification = defaultdict(list)
            for invocation in invocations:
                invocation_key = invocation.job_name or invocation.job_id
                if invocation_key is not None:
                    invocations_job_identification[invocation_key].append(
                        invocation.invocation_id
                    )

            report_data = ReportDataSchema(
                creation_time=get_now_utc_iso_format(),
                days_back=days_back,
                models=serializable_models,
                groups=serializable_groups,
                tests=serializable_tests,
                invocation=serializable_invocation,
                test_results=serializable_test_results,
                test_results_totals=serializable_test_results_totals,
                test_runs=serializable_test_runs,
                test_runs_totals=serializable_test_runs_totals,
                coverages=serializable_models_coverages,
                model_runs=serializable_model_runs,
                model_runs_totals=serializable_model_runs_totals,
                filters=serializable_filters,
                lineage=serializable_lineage,
                invocations=invocations,
                resources_latest_invocation=models_latest_invocation,
                invocations_job_identification=invocations_job_identification,
                env=ReportDataEnvSchema(
                    project_name=project_name, env=env, warehouse_type=warehouse_type
                ),
            )
            return report_data, None
        except Exception as error:
            return ReportDataSchema(), error

    def _serialize_models(
        self,
        models: Dict[str, NormalizedModelSchema],
        sources: Dict[str, NormalizedSourceSchema],
        exposures: Dict[str, NormalizedExposureSchema],
        seeds: Dict[str, NormalizedSeedSchema],
        snapshots: Dict[str, NormalizedSnapshotSchema],
    ) -> Dict[str, dict]:
        nodes = dict(**models, **sources, **exposures, **seeds, **snapshots)
        serializable_nodes = dict()
        for key in nodes.keys():
            serializable_nodes[key] = dict(nodes[key])
        return serializable_nodes

    def _serialize_coverages(
        self, coverages: Dict[str, ModelCoverageSchema]
    ) -> Dict[str, dict]:
        return {model_id: dict(coverage) for model_id, coverage in coverages.items()}

    def _serialize_models_runs(self, models_runs: List[ModelRunsSchema]) -> List[dict]:
        return [model_runs.dict(by_alias=True) for model_runs in models_runs]

    def _serialize_test_results(
        self,
        test_results: Dict[
            str, List[Union[TestResultSchema, SourceFreshnessResultSchema]]
        ],
    ) -> Dict[str, List[dict]]:
        serializable_test_results = defaultdict(list)
        for key, test_result in test_results.items():
            serializable_test_results[key].extend(
                [result.dict() for result in test_result]
            )
        return serializable_test_results

    def _serialize_tests(self, tests: Dict[str, TestSchema]) -> Dict[str, dict]:
        serializable_tests = dict()
        for key, test in tests.items():
            serializable_tests[key] = test.dict()
        return serializable_tests

    def _serialize_test_runs(
        self,
        test_runs: Dict[str, List[Union[TestRunSchema, SourceFreshnessRunSchema]]],
    ) -> Dict[str, List[dict]]:
        serializable_test_runs = defaultdict(list)
        for key, test_run in test_runs.items():
            serializable_test_runs[key].extend([run.dict() for run in test_run])
        return serializable_test_runs

    def _serialize_totals(self, totals: Dict[str, TotalsSchema]) -> Dict[str, dict]:
        serialized_totals = dict()
        for key, total in totals.items():
            serialized_totals[key] = total.dict()
        return serialized_totals
