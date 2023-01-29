import re
from typing import Optional

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.monitor.api.selector.selector import SelectorAPI
from elementary.monitor.data_monitoring.schema import DataMonitoringFilterSchema
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class DataMonitoringFilter:
    def __init__(
        self,
        tracking: AnonymousTracking,
        internal_dbt_runner: DbtRunner,
        user_dbt_runner: Optional[DbtRunner] = None,
        selector: Optional[str] = None,
    ) -> None:
        self.tracking = tracking
        self.internal_dbt_runner = internal_dbt_runner
        self.user_dbt_runner = user_dbt_runner
        self.selector = selector
        self.filter = self._parse_selector(self.selector)

    def _parse_selector(
        self, selector: Optional[str] = None
    ) -> DataMonitoringFilterSchema:
        data_monitoring_filter = DataMonitoringFilterSchema()
        if selector:
            if self.user_dbt_runner:
                self.tracking.set_env("select_method", "dbt selector")
                selector_api = SelectorAPI(self.user_dbt_runner)
                node_names = selector_api.get_selector_results(selector=selector)
                return DataMonitoringFilterSchema(
                    node_names=node_names, selector=selector
                )
            else:

                invocation_id_regex = re.compile(r"invocation_id:.*")
                invocation_time_regex = re.compile(r"invocation_time:.*")
                last_invocation_regex = re.compile(r"last_invocation")
                tag_regex = re.compile(r"tag:.*")
                owner_regex = re.compile(r"config.meta.owner:.*")
                model_regex = re.compile(r"model:.*")

                invocation_id_match = invocation_id_regex.search(selector)
                invocation_time_match = invocation_time_regex.search(selector)
                last_invocation_match = last_invocation_regex.search(selector)
                tag_match = tag_regex.search(selector)
                owner_match = owner_regex.search(selector)
                model_match = model_regex.search(selector)

                if last_invocation_match:
                    data_monitoring_filter = DataMonitoringFilterSchema(
                        last_invocation=True, selector=selector
                    )
                elif invocation_id_match:
                    data_monitoring_filter = DataMonitoringFilterSchema(
                        invocation_id=invocation_id_match.group().split(":", 1)[1],
                        selector=selector,
                    )
                elif invocation_time_match:
                    data_monitoring_filter = DataMonitoringFilterSchema(
                        invocation_time=invocation_time_match.group().split(":", 1)[1],
                        selector=selector,
                    )
                elif tag_match:
                    self.tracking.set_env("select_method", "tag")
                    data_monitoring_filter = DataMonitoringFilterSchema(
                        tag=tag_match.group().split(":", 1)[1], selector=selector
                    )
                elif owner_match:
                    self.tracking.set_env("select_method", "owner")
                    data_monitoring_filter = DataMonitoringFilterSchema(
                        owner=owner_match.group().split(":", 1)[1], selector=selector
                    )
                elif model_match:
                    self.tracking.set_env("select_method", "model")
                    data_monitoring_filter = DataMonitoringFilterSchema(
                        model=model_match.group().split(":", 1)[1], selector=selector
                    )
                else:
                    logger.error(f"Could not parse the given -s/--select: {selector}")
        return data_monitoring_filter

    def get_filter(self) -> DataMonitoringFilterSchema:
        return self.filter

    def get_selector(self) -> Optional[str]:
        return self.selector
