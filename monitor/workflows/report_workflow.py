from monitor.workflows.base_workflow import BaseWorkflow


class ReportWorkflow(BaseWorkflow):
    def execute(self) -> None:
        self.data_monitoring.generate_report()


class SendReportWorkflow(BaseWorkflow):
    def execute(self) -> None:
        success, elementary_html_path = self.data_monitoring.generate_report()
        self.data_monitoring.send_report(elementary_html_path)
