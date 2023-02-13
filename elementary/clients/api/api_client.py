from collections import defaultdict
from typing import Any, Optional

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class APIClient:
    def __init__(self, dbt_runner: DbtRunner):
        self.dbt_runner = dbt_runner
        self.run_cache = defaultdict(lambda: None)
        self.errors = []

    def set_run_cache(self, key: str, value: Any):
        self.run_cache[key] = value

    def get_run_cache(self, key: str) -> Any:
        return self.run_cache[key]

    def append_error(self, error: Exception, api_method: Optional[str] = None):
        self.errors.append(f'Failed to run api method "{api_method}" - Error: {error}')

    def log_errors(self):
        if self.errors:
            logger.error(f"{len(self.errors)} errors occured:")
            for error in self.errors:
                logger.error(error)
