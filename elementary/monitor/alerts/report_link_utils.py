from typing import Optional

from elementary.monitor.alerts.schema.alert import ReportLinkData

LINK_TEXT = "Runs history"


def _get_formatted_report_url(report_url: str) -> str:
    return report_url.strip("/")


def _get_run_history_report_link(
    report_url, path, unique_id
) -> Optional[ReportLinkData]:
    report_link = None

    if unique_id and report_url:
        formatted_report_url = _get_formatted_report_url(report_url)
        url = f"{formatted_report_url}/report/{path}/{unique_id}/"
        report_link = ReportLinkData(url=url, text=LINK_TEXT)

    return report_link


def get_test_runs_report_link(
    report_url: Optional[str], elementary_unique_id: Optional[str]
) -> Optional[ReportLinkData]:
    return _get_run_history_report_link(report_url, "test-runs", elementary_unique_id)


def get_model_runs_report_link(
    report_url: Optional[str], model_unique_id: Optional[str]
) -> Optional[ReportLinkData]:
    return _get_run_history_report_link(report_url, "model-runs", model_unique_id)


def get_model_test_runs_report_link(
    report_url: Optional[str], model_unique_id: Optional[str]
) -> Optional[ReportLinkData]:
    report_link = None

    if model_unique_id and report_url:
        formatted_report_url = _get_formatted_report_url(report_url)
        url = f'{formatted_report_url}/report/test-runs/?treeNode={{"id":"{model_unique_id}"}}'
        report_link = ReportLinkData(url=url, text=LINK_TEXT)

    return report_link
