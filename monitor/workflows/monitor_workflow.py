from monitor.workflows.base_workflow import BaseWorkflow


class MonitorWorkflow(BaseWorkflow):
    def execute(self) -> None:
        self.data_monitoring.run(
            days_back=self.cli_params.get("days_back"),
            dbt_full_refresh=self.cli_params.get("full_refresh_dbt_package")
        )
