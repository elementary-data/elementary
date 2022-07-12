from dataclasses import dataclass

from clients.dbt.dbt_runner import DbtRunner


@dataclass
class APIClient:
    dbt_runner: DbtRunner
