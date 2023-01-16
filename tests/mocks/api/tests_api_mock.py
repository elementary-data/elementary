from elementary.monitor.api.tests.tests import TestsAPI
from tests.mocks.dbt_runner_mock import MockDbtRunner


class MockTestsAPI(TestsAPI):
    def __init__(self):
        mock_dbt_runner = MockDbtRunner()
        super().__init__(mock_dbt_runner)
