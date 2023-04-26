import re
from typing import Optional

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.monitor.data_monitoring.schema import SelectorFilterSchema
from elementary.monitor.fetchers.selector.selector import SelectorFetcher
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class SelectorFilter:
    def __init__(
        self,
        tracking: Optional[Tracking],
        user_dbt_runner: Optional[DbtRunner] = None,
        selector: Optional[str] = None,
    ) -> None:
        self.tracking = tracking
        self.selector = selector
        self.selector_fetcher = (
            SelectorFetcher(user_dbt_runner) if user_dbt_runner else None
        )
        self.filter = self._parse_selector(self.selector)

    def _parse_selector(self, selector: Optional[str] = None) -> SelectorFilterSchema:
        data_monitoring_filter = SelectorFilterSchema()
        if selector:
            if self.selector_fetcher:
                if self.tracking:
                    self.tracking.set_env("select_method", "dbt selector")
                node_names = self.selector_fetcher.get_selector_results(
                    selector=selector
                )
                return SelectorFilterSchema(node_names=node_names, selector=selector)
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
                    data_monitoring_filter = SelectorFilterSchema(
                        last_invocation=True, selector=selector
                    )
                elif invocation_id_match:
                    data_monitoring_filter = SelectorFilterSchema(
                        invocation_id=invocation_id_match.group().split(":", 1)[1],
                        selector=selector,
                    )
                elif invocation_time_match:
                    data_monitoring_filter = SelectorFilterSchema(
                        invocation_time=invocation_time_match.group().split(":", 1)[1],
                        selector=selector,
                    )
                elif tag_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "tag")
                    data_monitoring_filter = SelectorFilterSchema(
                        tag=tag_match.group().split(":", 1)[1], selector=selector
                    )
                elif owner_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "owner")
                    data_monitoring_filter = SelectorFilterSchema(
                        owner=owner_match.group().split(":", 1)[1], selector=selector
                    )
                elif model_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "model")
                    data_monitoring_filter = SelectorFilterSchema(
                        model=model_match.group().split(":", 1)[1], selector=selector
                    )
                else:
                    logger.error(f"Could not parse the given -s/--select: {selector}")
        return data_monitoring_filter

    def get_filter(self) -> SelectorFilterSchema:
        return self.filter

    def get_selector(self) -> Optional[str]:
        return self.selector

    def is_empty(self) -> bool:
        if self.selector:
            for selector, value in dict(self.filter).items():
                if value and selector != "selector":
                    return False
        return True
