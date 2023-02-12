import json
from typing import List, Optional

from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.monitor.fetchers.models.schema import (
    ExposureSchema,
    ModelRunSchema,
    ModelSchema,
    ModelTestCoverage,
    SourceSchema,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)

YAML_FILE_EXTENSION = ".yml"
SQL_FILE_EXTENSION = ".sql"


class ModelsFetcher(FetcherClient):
    def get_models_runs(
        self, days_back: Optional[int] = 7, exclude_elementary_models: bool = False
    ) -> List[ModelRunSchema]:
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
        model_runs = [ModelRunSchema(**model_run) for model_run in model_run_dicts]
        return model_runs

    def get_models(self, exclude_elementary_models: bool = False) -> List[ModelSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="get_models",
            macro_args={"exclude_elementary": exclude_elementary_models},
        )
        models = json.loads(run_operation_response[0]) if run_operation_response else []
        models = [ModelSchema(**model) for model in models]
        return models

    def get_sources(self) -> List[SourceSchema]:
        run_operation_response = self.dbt_runner.run_operation(macro_name="get_sources")
        sources = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        sources = [SourceSchema(**source) for source in sources]
        return sources

    def get_exposures(self) -> List[ExposureSchema]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="get_exposures"
        )
        exposures = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        exposures = [ExposureSchema(**exposure) for exposure in exposures]
        return exposures

    def get_test_coverages(self) -> List[ModelTestCoverage]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="get_dbt_models_test_coverage"
        )
        coverages = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        coverages = [ModelTestCoverage(**coverage) for coverage in coverages]
        return coverages
