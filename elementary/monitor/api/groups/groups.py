import posixpath
from collections import defaultdict
from typing import List, Union

from elementary.clients.api.api_client import APIClient
from elementary.monitor.api.groups.schema import (
    GroupItemSchema,
    GroupsSchema,
    OwnersGroupSchema,
    TagsGroupSchema,
    TreeGroupSchema,
)
from elementary.monitor.api.groups.tree_builder import TreeBuilder
from elementary.monitor.api.models.schema import (
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSourceSchema,
)
from elementary.monitor.fetchers.tests.schema import NormalizedTestSchema

FILES_GROUP_KEYWORD = "__files__"
NO_TAGS_DEFAULT_TREE = "No tags"
NO_OWNERS_DEFAULT_TREE = "No owners"


GROUPABLE_ARTIFACT = Union[
    NormalizedModelSchema,
    NormalizedSourceSchema,
    NormalizedExposureSchema,
    NormalizedTestSchema,
]


class GroupsAPI(APIClient):
    def get_groups(self, artifacts: List[GROUPABLE_ARTIFACT]) -> GroupsSchema:
        dbt_group = self.get_dbt_group(artifacts)
        tags_group = self.get_tags_group(artifacts)
        owners_group = self.get_owners_group(artifacts)
        dwh_group = self.get_dwh_group(artifacts)
        return GroupsSchema(
            dbt=dbt_group, dwh=dwh_group, tags=tags_group, owners=owners_group
        )

    def get_dbt_group(
        self,
        artifacts: List[GROUPABLE_ARTIFACT],
    ) -> TreeGroupSchema:
        tree_builder = TreeBuilder[GroupItemSchema](separator=posixpath.sep)
        for artifact in artifacts:
            if artifact.unique_id is None:
                continue
            tree_builder.add(
                path=artifact.normalized_full_path, data=self._get_group_item(artifact)
            )
        return tree_builder.get_tree()

    def get_dwh_group(self, artifacts: List[GROUPABLE_ARTIFACT]) -> TreeGroupSchema:
        tree_builder = TreeBuilder[GroupItemSchema](separator=".")
        relevant_artifacts = (
            artifact
            for artifact in artifacts
            if artifact.unique_id is not None
            and artifact.fqn is not None
            and isinstance(artifact, (NormalizedSourceSchema, NormalizedModelSchema))
        )
        for artifact in relevant_artifacts:
            tree_builder.add(path=artifact.fqn, data=self._get_group_item(artifact))
        return tree_builder.get_tree()

    def get_tags_group(
        self,
        artifacts: List[GROUPABLE_ARTIFACT],
    ) -> TagsGroupSchema:
        group = defaultdict(list)
        for artifact in artifacts:
            unique_id = artifact.unique_id
            if unique_id is None:
                continue

            if artifact.tags:
                for tag in artifact.tags:
                    group[tag].append(self._get_group_item(artifact))
            else:
                group[NO_TAGS_DEFAULT_TREE].append(self._get_group_item(artifact))
        return dict(group)

    def get_owners_group(
        self, artifacts: List[GROUPABLE_ARTIFACT]
    ) -> OwnersGroupSchema:
        group = defaultdict(list)
        for artifact in artifacts:
            unique_id = artifact.unique_id
            if unique_id is None:
                continue

            if artifact.owners:
                for owner in artifact.owners:
                    group[owner].append(self._get_group_item(artifact))
            else:
                group[NO_OWNERS_DEFAULT_TREE].append(self._get_group_item(artifact))
        return dict(group)

    @staticmethod
    def _get_group_item(artifact: GROUPABLE_ARTIFACT) -> GroupItemSchema:
        return GroupItemSchema(
            node_id=artifact.unique_id, resource_type=artifact.artifact_type
        )
