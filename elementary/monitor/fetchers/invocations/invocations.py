import json
from typing import Dict, List, Optional

from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.monitor.fetchers.invocations.schema import DbtInvocationSchema
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class InvocationsFetcher(FetcherClient):
    def get_test_last_invocation(
        self, macro_args: Optional[dict] = None
    ) -> DbtInvocationSchema:
        invocation_response = self.dbt_runner.run_operation(
            macro_name="get_test_last_invocation", macro_args=macro_args
        )
        invocation = json.loads(invocation_response[0]) if invocation_response else None
        if invocation:
            return DbtInvocationSchema(**invocation[0])
        else:
            logger.warning(f"Could not find invocation by filter: {macro_args}")
            return DbtInvocationSchema()

    def get_invocations_by_ids(
        self, invocations_ids: List[str]
    ) -> List[DbtInvocationSchema]:
        invocations_response = self.dbt_runner.run_operation(
            macro_name="get_invocations_by_ids",
            macro_args={
                "ids": invocations_ids,
            },
        )
        invocation_results = (
            json.loads(invocations_response[0]) if invocations_response else []
        )
        invocation_results = [
            DbtInvocationSchema(**invocation_result)
            for invocation_result in invocation_results
        ]
        return invocation_results

    def get_resources_latest_invocation(self) -> Dict[str, str]:
        response = self.dbt_runner.run_operation(
            macro_name="get_resources_latest_invocation"
        )
        resources_latest_invocation_results = (
            json.loads(response[0]) if response else []
        )

        resources_latest_invocation_map = {
            result["unique_id"]: result["invocation_id"]
            for result in resources_latest_invocation_results
        }
        return resources_latest_invocation_map
