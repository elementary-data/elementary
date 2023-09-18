import json
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.monitor.fetchers.test_management.schema import (
    ResourceColumnModel,
    ResourceModel,
    ResourcesModel,
    TagsModel,
    TestModel,
)
from elementary.utils.json_utils import unpack_and_flatten_and_dedup_list_of_strings
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class TestManagementFetcher(FetcherClient):
    def __init__(self, dbt_runner: BaseDbtRunner):
        super().__init__(dbt_runner)

    def get_models(
        self,
        exclude_elementary: bool = True,
        columns: Optional[DefaultDict[str, List[ResourceColumnModel]]] = None,
    ) -> List[ResourceModel]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_model_resources",
            macro_args=dict(exclude_elementary=exclude_elementary),
        )
        models_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        return self._format_resources(models_results, columns)

    def get_sources(
        self,
        exclude_elementary: bool = True,
        columns: Optional[DefaultDict[str, List[ResourceColumnModel]]] = None,
    ) -> List[ResourceModel]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_source_resources",
            macro_args=dict(exclude_elementary=exclude_elementary),
        )
        sources_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        return self._format_resources(sources_results, columns)

    def get_resources_columns(self) -> DefaultDict[str, List[ResourceColumnModel]]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_resources_columns"
        )
        resources_columns_results = (
            json.loads(run_operation_response[0]) if run_operation_response else {}
        )
        resources_columns = defaultdict(list)
        for resource, columns in resources_columns_results.items():
            resources_columns[resource.lower()].extend(
                [
                    ResourceColumnModel(
                        name=column.get("column"),
                        type=column.get("type"),
                    )
                    for column in columns
                ]
            )
        return resources_columns

    def _format_resources(
        self,
        resources: List[Dict[str, Any]],
        columns: Optional[DefaultDict[str, List[ResourceColumnModel]]] = None,
    ) -> List[ResourceModel]:
        if not columns:
            columns = defaultdict(list)

        formatted_resources = []
        for resource in resources:
            owners = (
                unpack_and_flatten_and_dedup_list_of_strings(resource["owners"])
                if resource["owners"]
                else []
            )
            formatted_resources.append(
                ResourceModel(
                    id=resource["unique_id"],
                    name=resource["name"],
                    source_name=resource.get("source_name"),
                    schema=resource["schema"],
                    tags=json.loads(resource["tags"]),
                    owners=owners,
                    columns=columns[
                        f'{resource["database"]}.{resource["schema"]}.{resource["name"]}'.lower()
                    ],
                )
            )
        return formatted_resources

    def get_resources(self, exclude_elementary: bool = True) -> ResourcesModel:
        columns = self.get_resources_columns()
        models = self.get_models(exclude_elementary, columns)
        sources = self.get_sources(exclude_elementary, columns)
        return ResourcesModel(models=models, sources=sources)

    def get_tags(self) -> TagsModel:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_project_tags"
        )
        tags_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        all_tags = []
        for tags_result in tags_results:
            tags = json.loads(tags_result["tags"])
            all_tags.extend(tags)
        return TagsModel(tags=all_tags)

    def get_tests(self) -> List[TestModel]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_tests"
        )
        test_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        tests = []
        for test_result in test_results:
            meta = json.loads(test_result["meta"])
            owners = unpack_and_flatten_and_dedup_list_of_strings(
                meta.get("owner", "[]")
            )
            model_owners = unpack_and_flatten_and_dedup_list_of_strings(
                test_result["model_owners"]
            )
            tags = list(set(json.loads(test_result["tags"])))
            model_tags = list(set(json.loads(test_result["model_tags"])))
            description = meta.get("description")

            tests.append(
                TestModel(
                    id=test_result["id"],
                    schema=test_result["schema"],
                    table=test_result["table"],
                    source_name=test_result["source_name"],
                    column=test_result["column"],
                    package=test_result["test_package"],
                    name=test_result["test_name"],
                    args=json.loads(test_result["test_params"]),
                    severity=test_result["severity"],
                    owners=owners,
                    model_owners=model_owners,
                    tags=tags,
                    model_tags=model_tags,
                    meta=meta,
                    description=description,
                    is_singular=test_result["is_singular"],
                    updated_at=test_result["generated_at"],
                )
            )
        return tests

    def get_project_owners(self) -> List[str]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_project_owners"
        )
        owners_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        all_owners = []
        for owners_result in owners_results:
            owners = owners_result["owner"]
            if owners is None:
                continue
            owners = unpack_and_flatten_and_dedup_list_of_strings(owners)
            all_owners.extend(owners)
        return all_owners

    def get_project_subscribers(self) -> List[str]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="elementary_cli.get_resources_meta"
        )
        resources_meta_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        all_subscribers = []
        for resources_meta_result in resources_meta_results:
            stringfy_meta = resources_meta_result["meta"]
            if stringfy_meta:
                meta = json.loads(stringfy_meta)
                subscribers = meta.get(
                    "subscribers",
                    meta.get("alerts_config", {}).get("subscribers", []),
                )
                if type(subscribers) is str:
                    try:
                        subscribers = json.loads(subscribers)
                    except json.JSONDecodeError:
                        subscribers = subscribers.split(",")
                for subscriber in subscribers:
                    all_subscribers.append(subscriber.strip())
        return all_subscribers

    def get_all_project_users(self) -> List[str]:
        project_users = [
            *self.get_project_owners(),
            *self.get_project_subscribers(),
        ]
        return list(set(project_users))
