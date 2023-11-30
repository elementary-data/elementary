from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.monitor.fetchers.test_management.schema import (
    ResourcesModel,
    TagsModel,
    TestsModel,
    UserModel,
    UsersModel,
)
from elementary.monitor.fetchers.test_management.test_management import (
    TestManagementFetcher,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class TestManagementAPI(APIClient):
    def __init__(
        self,
        dbt_runner: BaseDbtRunner,
        exclude_elementary: bool = True,
    ):
        super().__init__(dbt_runner)
        self.test_management_fetcher = TestManagementFetcher(dbt_runner=self.dbt_runner)
        self.exclude_elementary = exclude_elementary

    def get_resources(self) -> ResourcesModel:
        return self.test_management_fetcher.get_resources(self.exclude_elementary)

    def get_tests(self) -> TestsModel:
        tests = self.test_management_fetcher.get_tests()
        return TestsModel(tests=tests)

    def get_tags(self) -> TagsModel:
        return self.test_management_fetcher.get_tags()

    def get_project_users(self) -> UsersModel:
        project_user_names = self.test_management_fetcher.get_all_project_users()
        project_users = [
            UserModel(name=project_user, origin="project")
            for project_user in project_user_names
        ]
        return UsersModel(users=project_users)
