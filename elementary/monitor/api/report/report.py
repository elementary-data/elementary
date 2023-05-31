from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from elementary.clients.api.api_client import APIClient
from elementary.monitor.api.filters.filters import FiltersAPI
from elementary.monitor.api.invocations.invocations import InvocationsAPI
from elementary.monitor.api.lineage.lineage import LineageAPI
from elementary.monitor.api.models.models import ModelsAPI
from elementary.monitor.api.models.schema import (
    ModelCoverageSchema,
    ModelRunsSchema,
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSourceSchema,
    TotalsSchema,
)
from elementary.monitor.api.report.schema import ReportDataSchema
from elementary.monitor.api.sidebar.sidebar import SidebarAPI
from elementary.monitor.api.tests.schema import TestResultSchema, TestRunSchema
from elementary.monitor.api.tests.tests import TestsAPI
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema
from elementary.utils.time import get_now_utc_iso_format


class ReportAPI(APIClient):
    def get_report_data(
        self,
        days_back: Optional[int] = None,
        test_runs_amount: Optional[int] = None,
        disable_passed_test_metrics: bool = False,
        exclude_elementary_models: bool = False,
        project_name: Optional[str] = None,
        disable_samples: bool = False,
        filter: SelectorFilterSchema = SelectorFilterSchema(),
        env: Optional[str] = None,
    ) -> Tuple[ReportDataSchema, Optional[Exception]]:
        try:
            tests_api = TestsAPI(
                dbt_runner=self.dbt_runner,
                days_back=days_back,
                invocations_per_test=test_runs_amount,
                disable_passed_test_metrics=disable_passed_test_metrics,
            )
            models_api = ModelsAPI(dbt_runner=self.dbt_runner)
            sidebar_api = SidebarAPI(dbt_runner=self.dbt_runner)
            lineage_api = LineageAPI(dbt_runner=self.dbt_runner)
            filters_api = FiltersAPI(dbt_runner=self.dbt_runner)
            invocations_api = InvocationsAPI(dbt_runner=self.dbt_runner)

            models = models_api.get_models(exclude_elementary_models)
            sources = models_api.get_sources()
            exposures = models_api.get_exposures()

            sidebars = sidebar_api.get_sidebars(
                artifacts=[*models.values(), *sources.values()]
            )

            models_runs = models_api.get_models_runs(
                days_back=days_back, exclude_elementary_models=exclude_elementary_models
            )
            coverages = models_api.get_test_coverages()

            test_results = tests_api.get_test_results(
                filter=filter, disable_samples=disable_samples
            )
            test_runs = tests_api.get_test_runs()

            lineage = lineage_api.get_lineage(exclude_elementary_models)
            filters = filters_api.get_filters(
                test_results.totals, test_runs.totals, models, sources, models_runs.runs
            )

            serializable_sidebars = sidebars.dict()
            serializable_models = self._serilize_models(models, sources, exposures)
            serializable_model_runs = self._serilize_models_runs(models_runs.runs)
            serializable_model_runs_totals = models_runs.dict(include={"totals"})[
                "totals"
            ]
            serializable_models_coverages = self._serilize_coverages(coverages)
            serializable_test_results = self._serilize_test_results(
                test_results.results
            )
            serializable_test_restuls_totals = self._serialize_totals(
                test_results.totals
            )
            serializable_test_runs = self._serilize_test_runs(test_runs.runs)
            serializable_test_runs_totals = self._serialize_totals(test_runs.totals)
            serializable_invocation = test_results.invocation.dict()
            serializable_filters = filters.dict()
            serializable_lineage = lineage.dict()

            resources_latest_invocation = (
                invocations_api.get_resources_latest_invocation()
            )
            invocations = invocations_api.get_invocations_by_ids(
                invocations_ids=list(set(resources_latest_invocation.values()))
            )

            report_data = ReportDataSchema(
                creation_time=get_now_utc_iso_format(),
                days_back=days_back,
                models=serializable_models,
                sidebars=serializable_sidebars,
                invocation=serializable_invocation,
                test_results=serializable_test_results,
                test_results_totals=serializable_test_restuls_totals,
                test_runs=serializable_test_runs,
                test_runs_totals=serializable_test_runs_totals,
                coverages=serializable_models_coverages,
                model_runs=serializable_model_runs,
                model_runs_totals=serializable_model_runs_totals,
                filters=serializable_filters,
                lineage=serializable_lineage,
                invocations=invocations,
                resources_latest_invocation=resources_latest_invocation,
                env=dict(project_name=project_name, env=env),
            )
            return report_data, None
        except Exception as error:
            return ReportDataSchema(), error

    def _serilize_models(
        self,
        models: Dict[str, NormalizedModelSchema],
        sources: Dict[str, NormalizedSourceSchema],
        exposures: Dict[str, NormalizedExposureSchema],
    ) -> Dict[str, dict]:
        nodes = dict(**models, **sources, **exposures)
        serializable_nodes = dict()
        for key in nodes.keys():
            serializable_nodes[key] = dict(nodes[key])
        return serializable_nodes

    def _serilize_coverages(
        self, coverages: Dict[str, ModelCoverageSchema]
    ) -> Dict[str, dict]:
        return {model_id: dict(coverage) for model_id, coverage in coverages.items()}

    def _serilize_models_runs(self, models_runs: List[ModelRunsSchema]) -> List[dict]:
        return [model_runs.dict(by_alias=True) for model_runs in models_runs]

    def _serilize_test_results(
        self, test_results: Dict[Optional[str], List[TestResultSchema]]
    ) -> Dict[str, List[dict]]:
        serializable_test_results = defaultdict(list)
        for model_unique_id, test_result in test_results.items():
            serializable_test_results[model_unique_id].extend(
                [result.dict() for result in test_result]
            )
        return serializable_test_results

    def _serilize_test_runs(
        self, test_runs: Dict[Optional[str], List[TestRunSchema]]
    ) -> Dict[str, List[dict]]:
        serializable_test_runs = defaultdict(list)
        for model_unique_id, test_run in test_runs.items():
            serializable_test_runs[model_unique_id].extend(
                [run.dict() for run in test_run]
            )
        return serializable_test_runs

    def _serialize_totals(self, totals: Dict[str, TotalsSchema]) -> Dict[str, dict]:
        serialized_totals = dict()
        for model_unique_id, total in totals.items():
            serialized_totals[model_unique_id] = total.dict()
        return serialized_totals
