from elementary.clients.dbt.dbt_runner import DbtRunner


class BaseFetcher:
    def __init__(self, dbt_runner: DbtRunner):
        self.dbt_runner = dbt_runner
