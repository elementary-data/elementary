from typing import Dict, List

from elementary.clients.api.api_client import APIClient
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema
from elementary.monitor.fetchers.invocations.invocations import InvocationsFetcher
from elementary.monitor.fetchers.invocations.schema import DbtInvocationSchema
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class InvocationsAPI(APIClient):
    def __init__(self, dbt_runner: BaseDbtRunner):
        super().__init__(dbt_runner)
        self.invocations_fetcher = InvocationsFetcher(dbt_runner=self.dbt_runner)

    def get_last_invocation(self, type: str) -> DbtInvocationSchema:
        if type == "test":
            return self.invocations_fetcher.get_test_last_invocation()
        else:
            raise NotImplementedError

    def get_invocation_by_time(
        self, type: str, invocation_max_time: str
    ) -> DbtInvocationSchema:
        if type == "test":
            return self.invocations_fetcher.get_test_last_invocation(
                macro_args=dict(invocation_max_time=invocation_max_time)
            )
        else:
            raise NotImplementedError

    def get_invocation_by_id(
        self, type: str, invocation_id: str
    ) -> DbtInvocationSchema:
        if type == "test":
            return self.invocations_fetcher.get_test_last_invocation(
                macro_args=dict(invocation_id=invocation_id)
            )
        else:
            raise NotImplementedError

    def get_test_invocation_from_filter(
        self, selector_filter: SelectorFilterSchema
    ) -> DbtInvocationSchema:
        # If none of the following filter options exists, the invocation is empty and there is no filter.
        invocation = DbtInvocationSchema()

        if selector_filter.invocation_id:
            invocation = self.get_invocation_by_id(
                type="test", invocation_id=selector_filter.invocation_id
            )
        elif selector_filter.invocation_time:
            invocation = self.get_invocation_by_time(
                type="test", invocation_max_time=selector_filter.invocation_time
            )
        elif selector_filter.last_invocation:
            invocation = self.get_last_invocation(type="test")

        return invocation

    def get_models_latest_invocations_data(self) -> List[DbtInvocationSchema]:
        return self.invocations_fetcher.get_models_latest_invocations_data()

    def get_models_latest_invocation(self) -> Dict[str, str]:
        return self.invocations_fetcher.get_models_latest_invocation()
