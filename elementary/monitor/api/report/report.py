from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterable, List, Optional, Tuple, Union

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner
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
from elementary.utils.log import get_logger
from elementary.utils.time import get_now_utc_iso_format

logger = get_logger(__name__)


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

    def _create_subprocess_runner(self) -> SubprocessDbtRunner:
        """Create a SubprocessDbtRunner for thread-safe parallel execution.

        dbt's Python API (APIDbtRunner) is not thread-safe due to global
        mutable state (GLOBAL_FLAGS, adapter FACTORY, etc.).
        SubprocessDbtRunner spawns an independent dbt process per call,
        making it safe to use from multiple threads.
        """
        runner = self.dbt_runner
        return SubprocessDbtRunner(
            project_dir=runner.project_dir,
            profiles_dir=runner.profiles_dir,
            target=runner.target,
            raise_on_failure=runner.raise_on_failure,
            env_vars=getattr(runner, "env_vars", None),
            vars=runner.vars,
            secret_vars=runner.secret_vars,
            allow_macros_without_package_prefix=runner.allow_macros_without_package_prefix,
            run_deps_if_needed=False,
        )

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
        threads: int = 1,
    ) -> Tuple[ReportDataSchema, Optional[Exception]]:
        if threads > 1:
            return self._get_report_data_parallel(
                days_back=days_back,
                test_runs_amount=test_runs_amount,
                disable_passed_test_metrics=disable_passed_test_metrics,
                exclude_elementary_models=exclude_elementary_models,
                project_name=project_name,
                disable_samples=disable_samples,
                filter=filter,
                env=env,
                warehouse_type=warehouse_type,
                threads=threads,
            )
        return self._get_report_data_sequential(
            days_back=days_back,
            test_runs_amount=test_runs_amount,
            disable_passed_test_metrics=disable_passed_test_metrics,
            exclude_elementary_models=exclude_elementary_models,
            project_name=project_name,
            disable_samples=disable_samples,
            filter=filter,
            env=env,
            warehouse_type=warehouse_type,
        )

    def _get_report_data_sequential(
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
                test_results_totals,
                test_runs_totals,
                models,
                sources,
                models_runs.runs,
                seeds,
                snapshots,
            )

            return self._build_report_data(
                days_back=days_back,
                project_name=project_name,
                env=env,
                warehouse_type=warehouse_type,
                exclude_elementary_models=exclude_elementary_models,
                seeds=seeds,
                snapshots=snapshots,
                models=models,
                sources=sources,
                exposures=exposures,
                singular_tests=singular_tests,
                groups=groups,
                models_runs=models_runs,
                coverages=coverages,
                tests=tests,
                test_invocation=test_invocation,
                test_results=test_results,
                source_freshness_results=source_freshness_results,
                test_runs=test_runs,
                source_freshness_runs=source_freshness_runs,
                lineage=lineage,
                filters=filters,
                invocations_api=invocations_api,
            )
        except Exception as error:
            return ReportDataSchema(), error

    def _get_report_data_parallel(
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
        threads: int = 4,
    ) -> Tuple[ReportDataSchema, Optional[Exception]]:
        try:
            parallel_runner = self._create_subprocess_runner()
            logger.info(
                "Fetching report data in parallel with %d threads", threads
            )

            # Phase 1: fetch all independent data in parallel
            with ThreadPoolExecutor(max_workers=threads) as pool:
                f_seeds = pool.submit(
                    ModelsAPI(dbt_runner=parallel_runner).get_seeds
                )
                f_snapshots = pool.submit(
                    ModelsAPI(dbt_runner=parallel_runner).get_snapshots
                )
                f_models = pool.submit(
                    ModelsAPI(dbt_runner=parallel_runner).get_models,
                    exclude_elementary_models,
                )
                f_sources = pool.submit(
                    ModelsAPI(dbt_runner=parallel_runner).get_sources
                )
                f_singular_tests = pool.submit(
                    TestsAPI(
                        dbt_runner=parallel_runner,
                        days_back=days_back,
                        invocations_per_test=test_runs_amount,
                        disable_passed_test_metrics=disable_passed_test_metrics,
                    ).get_singular_tests
                )
                f_models_runs = pool.submit(
                    ModelsAPI(dbt_runner=parallel_runner).get_models_runs,
                    days_back,
                    exclude_elementary_models,
                )
                f_coverages = pool.submit(
                    ModelsAPI(dbt_runner=parallel_runner).get_test_coverages
                )
                f_tests = pool.submit(
                    TestsAPI(
                        dbt_runner=parallel_runner,
                        days_back=days_back,
                        invocations_per_test=test_runs_amount,
                        disable_passed_test_metrics=disable_passed_test_metrics,
                    ).get_tests
                )
                f_test_invocation = pool.submit(
                    InvocationsAPI(
                        dbt_runner=parallel_runner
                    ).get_test_invocation_from_filter,
                    filter,
                )
                f_freshness_results = pool.submit(
                    SourceFreshnessesAPI(
                        dbt_runner=parallel_runner,
                        days_back=days_back,
                        invocations_per_test=test_runs_amount,
                    ).get_source_freshness_results
                )
                f_test_runs = pool.submit(
                    TestsAPI(
                        dbt_runner=parallel_runner,
                        days_back=days_back,
                        invocations_per_test=test_runs_amount,
                        disable_passed_test_metrics=disable_passed_test_metrics,
                    ).get_test_runs
                )
                f_freshness_runs = pool.submit(
                    SourceFreshnessesAPI(
                        dbt_runner=parallel_runner,
                        days_back=days_back,
                        invocations_per_test=test_runs_amount,
                    ).get_source_freshness_runs
                )
                f_latest_invocation = pool.submit(
                    InvocationsAPI(
                        dbt_runner=parallel_runner
                    ).get_models_latest_invocation
                )
                f_invocations_data = pool.submit(
                    InvocationsAPI(
                        dbt_runner=parallel_runner
                    ).get_models_latest_invocations_data
                )

            seeds = f_seeds.result()
            snapshots = f_snapshots.result()
            models = f_models.result()
            sources = f_sources.result()
            singular_tests = f_singular_tests.result()
            models_runs = f_models_runs.result()
            coverages = f_coverages.result()
            tests = f_tests.result()
            test_invocation = f_test_invocation.result()
            source_freshness_results = f_freshness_results.result()
            test_runs = f_test_runs.result()
            source_freshness_runs = f_freshness_runs.result()
            models_latest_invocation = f_latest_invocation.result()
            invocations_data = f_invocations_data.result()

            # Phase 2: fetch data that depends on Phase 1 results
            lineage_node_ids: List[str] = list(
                seeds.keys()
            ) + list(snapshots.keys()) + list(models.keys()) + list(sources.keys())

            with ThreadPoolExecutor(max_workers=threads) as pool:
                f_exposures = pool.submit(
                    ModelsAPI(dbt_runner=parallel_runner).get_exposures,
                    upstream_node_ids=lineage_node_ids,
                )
                f_test_results = pool.submit(
                    TestsAPI(
                        dbt_runner=parallel_runner,
                        days_back=days_back,
                        invocations_per_test=test_runs_amount,
                        disable_passed_test_metrics=disable_passed_test_metrics,
                    ).get_test_results,
                    test_invocation.invocation_id,
                    disable_samples,
                )

            exposures = f_exposures.result()
            test_results = f_test_results.result()

            lineage_node_ids.extend(exposures.keys())

            # Phase 3: lineage depends on all node IDs
            lineage = LineageAPI(dbt_runner=parallel_runner).get_lineage(
                lineage_node_ids, exclude_elementary_models
            )

            # Phase 4: pure computation (no dbt calls)
            groups = self._get_groups(
                models.values(),
                sources.values(),
                exposures.values(),
                seeds.values(),
                snapshots.values(),
                singular_tests,
            )

            union_test_results = {
                x: test_results.get(x, []) + source_freshness_results.get(x, [])
                for x in set(test_results).union(source_freshness_results)
            }
            test_results_totals = get_total_test_results(union_test_results)

            union_test_runs = dict()
            for key in set(test_runs).union(source_freshness_runs):
                test_run = test_runs.get(key, [])
                source_freshness_run = (
                    source_freshness_runs.get(key, []) if key is not None else []
                )
                union_test_runs[key] = test_run + source_freshness_run
            test_runs_totals = get_total_test_runs(union_test_runs)

            filters = FiltersAPI(dbt_runner=parallel_runner).get_filters(
                test_results_totals,
                test_runs_totals,
                models,
                sources,
                models_runs.runs,
                seeds,
                snapshots,
            )

            invocations_job_identification = defaultdict(list)
            for invocation in invocations_data:
                invocation_key = invocation.job_name or invocation.job_id
                if invocation_key is not None:
                    invocations_job_identification[invocation_key].append(
                        invocation.invocation_id
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
                invocations=invocations_data,
                resources_latest_invocation=models_latest_invocation,
                invocations_job_identification=invocations_job_identification,
                env=ReportDataEnvSchema(
                    project_name=project_name, env=env, warehouse_type=warehouse_type
                ),
            )
            return report_data, None
        except Exception as error:
            return ReportDataSchema(), error

    def _build_report_data(
        self,
        days_back,
        project_name,
        env,
        warehouse_type,
        exclude_elementary_models,
        seeds,
        snapshots,
        models,
        sources,
        exposures,
        singular_tests,
        groups,
        models_runs,
        coverages,
        tests,
        test_invocation,
        test_results,
        source_freshness_results,
        test_runs,
        source_freshness_runs,
        lineage,
        filters,
        invocations_api,
    ) -> Tuple[ReportDataSchema, Optional[Exception]]:
        union_test_results = {
            x: test_results.get(x, []) + source_freshness_results.get(x, [])
            for x in set(test_results).union(source_freshness_results)
        }
        test_results_totals = get_total_test_results(union_test_results)

        union_test_runs = dict()
        for key in set(test_runs).union(source_freshness_runs):
            test_run = test_runs.get(key, [])
            source_freshness_run = (
                source_freshness_runs.get(key, []) if key is not None else []
            )
            union_test_runs[key] = test_run + source_freshness_run
        test_runs_totals = get_total_test_runs(union_test_runs)

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
