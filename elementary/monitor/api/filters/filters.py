from typing import Dict, List

from elementary.clients.api.api_client import APIClient
from elementary.monitor.api.filters.schema import FilterSchema, FiltersSchema
from elementary.monitor.api.models.schema import (
    ModelRunsSchema,
    NormalizedModelSchema,
    NormalizedSourceSchema,
)
from elementary.monitor.api.totals_schema import TotalsSchema
from elementary.monitor.fetchers.models.schema import ArtifactSchema
from elementary.utils.log import get_logger

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class FiltersAPI(APIClient):
    def get_filters(
        self,
        test_results_totals: Dict[str, TotalsSchema],
        test_runs_totals: Dict[str, TotalsSchema],
        models: Dict[str, NormalizedModelSchema],
        sources: Dict[str, NormalizedSourceSchema],
        models_runs: List[ModelRunsSchema],
    ) -> FiltersSchema:
        test_results_filters = self._get_test_filters(
            test_results_totals, models, sources
        )
        test_runs_filters = self._get_test_filters(test_runs_totals, models, sources)
        model_runs_filters = self._get_model_runs_filters(models_runs)
        return FiltersSchema(
            test_results=test_results_filters,
            test_runs=test_runs_filters,
            model_runs=model_runs_filters,
        )

    @staticmethod
    def _get_test_filters(
        totals: Dict[str, TotalsSchema],
        models: Dict[str, NormalizedModelSchema],
        sources: Dict[str, NormalizedSourceSchema],
    ) -> List[FilterSchema]:
        failures_filter = FilterSchema(name="failures", display_name="Failures")
        warnings_filter = FilterSchema(name="warnings", display_name="Warnings")
        errors_filter = FilterSchema(name="errors", display_name="Errors")
        passed_filter = FilterSchema(name="passed", display_name="Passed")
        no_tests_filter = FilterSchema(name="no_test", display_name="No Tests")

        totals_models_ids = totals.keys()
        artifacts: List[ArtifactSchema] = [*models.values(), *sources.values()]
        for artifact in artifacts:
            if artifact.unique_id and artifact.unique_id not in totals_models_ids:
                no_tests_filter.add_model_unique_id(artifact.unique_id)

        for model_unique_id, total in totals.items():
            if total.failures:
                failures_filter.add_model_unique_id(model_unique_id)
            if total.warnings:
                warnings_filter.add_model_unique_id(model_unique_id)
            if total.errors:
                errors_filter.add_model_unique_id(model_unique_id)
            if total.passed:
                passed_filter.add_model_unique_id(model_unique_id)
            if (
                not total.failures
                and not total.warnings
                and not total.errors
                and not total.passed
            ):
                no_tests_filter.add_model_unique_id(model_unique_id)

        filters = [
            failures_filter,
            warnings_filter,
            errors_filter,
            passed_filter,
            no_tests_filter,
        ]
        return [filter for filter in filters if len(filter.model_unique_ids)]

    @staticmethod
    def _get_model_runs_filters(
        models_runs: List[ModelRunsSchema],
    ) -> List[FilterSchema]:
        successful_runs_filter = FilterSchema(
            name="success", display_name="Successful Runs"
        )
        failed_runs_filter = FilterSchema(name="errors", display_name="Failed Runs")
        no_runs_filter = FilterSchema(name="no_runs", display_name="No Runs")

        for model_runs in models_runs:
            totals = model_runs.totals
            unique_id = model_runs.unique_id
            if totals.success:
                successful_runs_filter.add_model_unique_id(unique_id)
            if totals.errors:
                failed_runs_filter.add_model_unique_id(unique_id)
            if not totals.success and not totals.errors:
                no_runs_filter.add_model_unique_id(unique_id)

        filters = [successful_runs_filter, failed_runs_filter, no_runs_filter]
        return [filter for filter in filters if len(filter.model_unique_ids)]
