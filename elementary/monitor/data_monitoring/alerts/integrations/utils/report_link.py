from enum import Enum
from typing import Optional

from elementary.utils.pydantic_shim import BaseModel

TEST_RUNS_LINK_TEXT = "View test runs"
MODEL_RUNS_LINK_TEXT = "View model runs"


class ReportLinkData(BaseModel):
    url: str
    text: str


class ReportPath(Enum):
    TEST_RUNS = "test-runs"
    TEST_RESULTS = "test-results"
    MODEL_RUNS = "model-runs"


def _get_formatted_report_url(report_url: str) -> str:
    return report_url.strip("/")


def _get_run_history_report_link(
    report_url: Optional[str], path: ReportPath, unique_id: Optional[str]
) -> Optional[ReportLinkData]:
    report_link = None

    if unique_id and report_url:
        formatted_report_url = _get_formatted_report_url(report_url)
        url = f"{formatted_report_url}/report/{path.value}/{unique_id}/"
        report_link = ReportLinkData(
            url=url,
            text=TEST_RUNS_LINK_TEXT
            if path == ReportPath.TEST_RUNS
            else MODEL_RUNS_LINK_TEXT,
        )

    return report_link


def get_test_runs_link(
    report_url: Optional[str], elementary_unique_id: Optional[str]
) -> Optional[ReportLinkData]:
    return _get_run_history_report_link(
        report_url, ReportPath.TEST_RUNS, elementary_unique_id
    )


def get_model_runs_link(
    report_url: Optional[str], model_unique_id: Optional[str]
) -> Optional[ReportLinkData]:
    return _get_run_history_report_link(
        report_url, ReportPath.MODEL_RUNS, model_unique_id
    )


def get_model_test_runs_link(
    report_url: Optional[str], model_unique_id: Optional[str]
) -> Optional[ReportLinkData]:
    report_link = None

    if model_unique_id and report_url:
        formatted_report_url = _get_formatted_report_url(report_url)
        url = f'{formatted_report_url}/report/{ReportPath.TEST_RUNS.value}/?treeNode={{"id":"{model_unique_id}"}}'
        report_link = ReportLinkData(url=url, text=TEST_RUNS_LINK_TEXT)

    return report_link


def get_owner_test_runs_link(
    report_url: Optional[str], owner: Optional[str]
) -> Optional[ReportLinkData]:
    report_link = None

    if owner and report_url:
        formatted_report_url = _get_formatted_report_url(report_url)
        url = (
            f"{formatted_report_url}/report/{ReportPath.TEST_RESULTS.value}/?tree_view_by=owners"
            f'&tree_filters=["failures","warnings"]'
            f'&tree_node={{"id":"folderNode_{owner}"}}'
        )
        report_link = ReportLinkData(url=url, text=TEST_RUNS_LINK_TEXT)

    return report_link
