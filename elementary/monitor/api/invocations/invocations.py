import json

from elementary.clients.api.api import APIClient
from elementary.monitor.api.invocations.schema import DbtInvocationSchema
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class InvocationsAPI(APIClient):
    def get_last_invocation(self, type: str) -> DbtInvocationSchema:
        if type == "test":
            invocation_response = self.dbt_runner.run_operation(
                macro_name="get_test_last_invocation",
            )
            invocation = (
                json.loads(invocation_response[0]) if invocation_response else None
            )
            return DbtInvocationSchema(**invocation[0])
        else:
            raise NotImplementedError

    def get_invocation_by_time(
        self, type: str, invocation_max_time: str
    ) -> DbtInvocationSchema:
        if type == "test":
            invocation_response = self.dbt_runner.run_operation(
                macro_name="get_test_last_invocation",
                macro_args=dict(invocation_max_time=invocation_max_time),
            )
            invocation = (
                json.loads(invocation_response[0]) if invocation_response else None
            )
            return DbtInvocationSchema(**invocation[0])
        else:
            raise NotImplementedError

    def get_invocation_by_id(
        self, type: str, invocation_id: str
    ) -> DbtInvocationSchema:
        if type == "test":
            invocation_response = self.dbt_runner.run_operation(
                macro_name="get_test_last_invocation",
                macro_args=dict(invocation_id=invocation_id),
            )
            invocation = (
                json.loads(invocation_response[0]) if invocation_response else None
            )
            return DbtInvocationSchema(**invocation[0])
        else:
            raise NotImplementedError
