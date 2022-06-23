from clients.dbt.dbt_runner import DbtRunner


class APIClient:
    def __init__(
        self,
        dbt_runner: DbtRunner
    ) -> None:
        self.dbt_runner = dbt_runner
        self.run_sucessfully = True
