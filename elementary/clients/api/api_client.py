from collections import defaultdict
from typing import Any, Union

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.clients.dbt.slim_dbt_runner import SlimDbtRunner
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class APIClient:
    def __init__(self, dbt_runner: Union[DbtRunner, SlimDbtRunner]):
        self.dbt_runner = dbt_runner
        self.run_cache = defaultdict(lambda: None)

    def set_run_cache(self, key: str, value: Any):
        self.run_cache[key] = value

    def get_run_cache(self, key: str) -> Any:
        return self.run_cache[key]
