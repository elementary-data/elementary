from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner


class BaseFetcher:
    def __init__(self, dbt_runner: BaseDbtRunner):
        self.dbt_runner = dbt_runner
