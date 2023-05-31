import json
from typing import List

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.fetcher.fetcher import FetcherClient
from elementary.monitor.fetchers.test_management.schema import (
    ResourceModel,
    ResourcesModel,
    TagsModel,
    TestModel,
)
from elementary.utils.json_utils import unpack_and_flatten_str_to_list
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class TestManagementFetcher(FetcherClient):
    def __init__(self, dbt_runner: BaseDbtRunner):
        super().__init__(dbt_runner)

    def get_models(self, exclude_elementary=True) -> List[ResourceModel]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="model_resources",
            macro_args=dict(exclude_elementary=exclude_elementary),
        )
        models_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        models = []
        for model_result in models_results:
            owners = (
                unpack_and_flatten_str_to_list(model_result["owners"])
                if model_result["owners"]
                else []
            )
            models.append(
                ResourceModel(
                    id=model_result["unique_id"],
                    name=model_result["name"],
                    schema=model_result["schema"],
                    tags=json.loads(model_result["tags"]),
                    owners=owners,
                )
            )
        return models

    def get_sources(self, exclude_elementary=True) -> List[ResourceModel]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="source_resources",
            macro_args=dict(exclude_elementary=exclude_elementary),
        )
        sources_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        sources = []
        for source_result in sources_results:
            owners = (
                unpack_and_flatten_str_to_list(source_result["owners"])
                if source_result["owners"]
                else []
            )
            sources.append(
                ResourceModel(
                    id=source_result["unique_id"],
                    name=source_result["name"],
                    source_name=source_result["source_name"],
                    schema=source_result["schema"],
                    tags=json.loads(source_result["tags"]),
                    owners=owners,
                )
            )
        return sources

    def get_resources(self, exclude_elementary=True) -> ResourcesModel:
        models = self.get_models(exclude_elementary)
        sources = self.get_sources(exclude_elementary)
        return ResourcesModel(models=models, sources=sources)

    def get_tags(self) -> TagsModel:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="project_tags"
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
        run_operation_response = self.dbt_runner.run_operation(macro_name="get_tests")
        test_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        tests = []
        for test_result in test_results:
            meta = json.loads(test_result["meta"])
            owners = unpack_and_flatten_str_to_list(meta.get("owner", "[]"))
            model_owners = unpack_and_flatten_str_to_list(test_result["model_owners"])
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
            macro_name="project_owners"
        )
        owners_results = (
            json.loads(run_operation_response[0]) if run_operation_response else []
        )
        all_owners = []
        for owners_result in owners_results:
            owners = owners_result["owner"]
            if owners is None:
                continue
            owners = unpack_and_flatten_str_to_list(owners)
            all_owners.extend(owners)
        return all_owners

    def get_project_subscribers(self) -> List[str]:
        run_operation_response = self.dbt_runner.run_operation(
            macro_name="resources_meta"
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
