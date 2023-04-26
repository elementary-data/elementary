from typing import List

from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class SelectorFetcher(FetcherClient):
    def get_selector_results(self, selector: str) -> List[str]:
        return self.dbt_runner.ls(select=selector)
