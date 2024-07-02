import re
from typing import Optional

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.dbt.factory import create_dbt_runner
from elementary.config.config import Config
from elementary.monitor.data_monitoring.schema import (
    FilterSchema,
    FiltersSchema,
    ResourceType,
    ResourceTypeFilterSchema,
    Status,
    StatusFilterSchema,
)
from elementary.monitor.fetchers.selector.selector import SelectorFetcher
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class SelectorFilter:
    def __init__(
        self,
        config: Config,
        tracking: Optional[Tracking],
        selector: Optional[str] = None,
    ) -> None:
        self.tracking = tracking
        self.selector = selector
        user_dbt_runner = self._create_user_dbt_runner(config)
        self.selector_fetcher = (
            SelectorFetcher(user_dbt_runner) if user_dbt_runner else None
        )
        self.filter = self._parse_selector(self.selector)

    # Once we will separate the filters to monitor and report filters, we will remove this method.
    def _parse_selector(self, selector: Optional[str] = None) -> FiltersSchema:
        data_monitoring_filter = FiltersSchema()
        if selector:
            if self.selector_fetcher and self._can_use_fetcher(selector):
                if self.tracking:
                    self.tracking.set_env("select_method", "dbt selector")
                node_names = self.selector_fetcher.get_selector_results(
                    selector=selector
                )
                return FiltersSchema(node_names=node_names, selector=selector)
            else:
                invocation_id_regex = re.compile(r"invocation_id:(.*)")
                invocation_time_regex = re.compile(r"invocation_time:(.*)")
                last_invocation_regex = re.compile(r"last_invocation")
                tag_regex = re.compile(r"tag:(.*)")
                owner_regex = re.compile(r"config.meta.owner:(.*)")
                model_regex = re.compile(r"model:(.*)")
                statuses_regex = re.compile(r"statuses:(.*)")
                resource_types_regex = re.compile(r"resource_types:(.*)")

                invocation_id_match = invocation_id_regex.search(selector)
                invocation_time_match = invocation_time_regex.search(selector)
                last_invocation_match = last_invocation_regex.search(selector)
                tag_match = tag_regex.search(selector)
                owner_match = owner_regex.search(selector)
                model_match = model_regex.search(selector)
                statuses_match = statuses_regex.search(selector)
                resource_types_match = resource_types_regex.search(selector)

                if last_invocation_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "last_invocation")
                    data_monitoring_filter = FiltersSchema(
                        last_invocation=True, selector=selector
                    )
                elif invocation_id_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "invocation_id")
                    data_monitoring_filter = FiltersSchema(
                        invocation_id=invocation_id_match.group(1),
                        selector=selector,
                    )
                elif invocation_time_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "invocation_time")
                    data_monitoring_filter = FiltersSchema(
                        invocation_time=invocation_time_match.group(1),
                        selector=selector,
                    )
                elif tag_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "tag")
                    data_monitoring_filter = FiltersSchema(
                        tags=[FilterSchema(values=[tag_match.group(1)])],
                        selector=selector,
                    )
                elif owner_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "owner")
                    data_monitoring_filter = FiltersSchema(
                        owners=[FilterSchema(values=[owner_match.group(1)])],
                        selector=selector,
                    )
                elif model_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "model")
                    data_monitoring_filter = FiltersSchema(
                        models=[FilterSchema(values=[model_match.group(1)])],
                        selector=selector,
                    )
                elif statuses_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "statuses")
                    statuses = [
                        Status(status) for status in statuses_match.group(1).split(",")
                    ]
                    data_monitoring_filter = FiltersSchema(
                        statuses=[StatusFilterSchema(values=statuses)],
                        selector=selector,
                    )
                elif resource_types_match:
                    if self.tracking:
                        self.tracking.set_env("select_method", "resource_types")
                    resource_types = [
                        ResourceType(resource_type)
                        for resource_type in resource_types_match.group(1).split(",")
                    ]
                    data_monitoring_filter = FiltersSchema(
                        resource_types=[
                            ResourceTypeFilterSchema(values=resource_types)
                        ],
                        selector=selector,
                    )
                else:
                    logger.error(f"Could not parse the given -s/--select: {selector}")
                    return FiltersSchema(selector=selector, statuses=[])
        return data_monitoring_filter

    def _create_user_dbt_runner(self, config: Config) -> Optional[BaseDbtRunner]:
        if config.project_dir:
            return create_dbt_runner(
                config.project_dir,
                config.profiles_dir,
                config.project_profile_target,
                env_vars=config.env_vars,
                run_deps_if_needed=config.run_dbt_deps_if_needed,
            )
        else:
            return None

    def get_filter(self) -> FiltersSchema:
        return self.filter

    @staticmethod
    def _can_use_fetcher(selector):
        non_dbt_selectors = [
            "last_invocation",
            "invocation_id",
            "invocation_time",
            "statuses",
            "resource_types",
        ]
        return all(
            [selector_type not in selector for selector_type in non_dbt_selectors]
        )
