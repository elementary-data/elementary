from typing import List, Optional

from elementary.clients.api.api import APIClient
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class SelectorAPI(APIClient):
    def get_selector_results(self, selector: str) -> List[Optional[str]]:
        return self.dbt_runner.ls(select=selector)
