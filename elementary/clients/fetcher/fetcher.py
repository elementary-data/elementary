from collections import defaultdict
from typing import Any

from elementary.clients.dbt.dbt_runner import DbtRunner


class FetcherClient:
    def __init__(self, dbt_runner: DbtRunner):
        self.dbt_runner = dbt_runner
        self.run_cache = defaultdict(lambda: None)

    def set_run_cache(self, key: str, value: Any):
        self.run_cache[key] = value

    def get_run_cache(self, key: str) -> Any:
        return self.run_cache[key]
