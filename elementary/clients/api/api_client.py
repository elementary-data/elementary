from collections import defaultdict
from typing import Any, DefaultDict

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner


class APIClient:
    def __init__(self, dbt_runner: BaseDbtRunner):
        self.dbt_runner = dbt_runner
        self.run_cache: DefaultDict[str, Any] = defaultdict(lambda: None)

    def set_run_cache(self, key: str, value: Any):
        self.run_cache[key] = value

    def get_run_cache(self, key: str) -> Any:
        return self.run_cache[key]
