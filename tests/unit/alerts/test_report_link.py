from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportPath,
    get_model_runs_link,
    get_model_test_runs_link,
    get_test_runs_link,
)

REPORT_URL = "https://elementary.example.com/#"
TEST_UNIQUE_ID = "test.my_project.my_test.abc123.row_count"
MODEL_UNIQUE_ID = "model.my_project.my_model"


def test_get_test_runs_link_routes_to_test_results():
    # Regression for #2283: the test-runs/<id> detail route redirects to the
    # parent list on a cold page load, so test alert deep links must target the
    # cold-load-safe test-results/<id> route instead.
    link = get_test_runs_link(REPORT_URL, TEST_UNIQUE_ID)

    assert link is not None
    assert link.url == f"{REPORT_URL}/report/test-results/{TEST_UNIQUE_ID}/"
    assert "/report/test-runs/" not in link.url
    # Only the URL changes — the button label is unaffected.
    assert link.text == "View test runs"


def test_get_test_runs_link_returns_none_without_url_or_id():
    assert get_test_runs_link(None, TEST_UNIQUE_ID) is None
    assert get_test_runs_link(REPORT_URL, None) is None


def test_get_model_runs_link_unchanged():
    link = get_model_runs_link(REPORT_URL, MODEL_UNIQUE_ID)

    assert link is not None
    assert link.url == f"{REPORT_URL}/report/model-runs/{MODEL_UNIQUE_ID}/"
    assert link.text == "View model runs"


def test_get_model_test_runs_link_routes_to_test_results():
    link = get_model_test_runs_link(REPORT_URL, MODEL_UNIQUE_ID)

    assert link is not None
    assert link.url.startswith(f"{REPORT_URL}/report/test-results/?treeNode=")
    assert ReportPath.TEST_RUNS.value == "test-results"
