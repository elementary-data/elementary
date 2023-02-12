from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.monitor.api.selector.schema import SelectorSchema
from elementary.monitor.fetchers.selector.selector import SelectorFetcher
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class SelectorAPI(APIClient):
    def __init__(self, dbt_runner: DbtRunner):
        super().__init__(dbt_runner)
        self.selector_fetcher = SelectorFetcher(dbt_runner=self.dbt_runner)

    def get_selector_results(self, selector: str) -> SelectorSchema:
        selector_results = self.selector_fetcher.get_selector_results(selector=selector)
        return SelectorSchema(selector=selector, selector_results=selector_results)
