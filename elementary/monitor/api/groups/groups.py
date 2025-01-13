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
    TreeViewSchema,
)
from elementary.monitor.api.groups.tree_builder import TreeBuilder
from elementary.monitor.api.models.schema import (
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSeedSchema,
    NormalizedSnapshotSchema,
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
    NormalizedSeedSchema,
    NormalizedSnapshotSchema,
]


class GroupsAPI(APIClient):
    def get_groups(self, artifacts: List[GROUPABLE_ARTIFACT]) -> GroupsSchema:
        data_assets_group = self.get_data_assets_group(artifacts)

        # direct views - deprecated
        dbt_group = self.get_normalized_full_path_view(artifacts)
        tags_group = self.get_tags_view(artifacts)
        owners_group = self.get_owners_view(artifacts)

        return GroupsSchema(
            dbt=dbt_group,
            tags=tags_group,
            owners=owners_group,
            data_assets=data_assets_group,
        )

    def get_data_assets_group(
        self, artifacts: List[GROUPABLE_ARTIFACT]
    ) -> List[TreeViewSchema]:
        filtered_artifacts = self.filter_data_assets_artifacts(artifacts)

        dwh_view = self.get_dwh_view(filtered_artifacts)
        dbt_view = self.get_normalized_full_path_view(filtered_artifacts)
        tags_view = self.get_tags_view(filtered_artifacts)
        owners_view = self.get_owners_view(filtered_artifacts)

        return [
            TreeViewSchema(name="dwh", data=dwh_view),
            TreeViewSchema(name="dbt", data=dbt_view),
            TreeViewSchema(name="tags", data=tags_view),
            TreeViewSchema(name="owners", data=owners_view),
        ]

    @staticmethod
    def filter_data_assets_artifacts(
        artifacts: List[GROUPABLE_ARTIFACT],
    ) -> List[GROUPABLE_ARTIFACT]:
        return [
            artifact
            for artifact in artifacts
            if not isinstance(artifact, NormalizedExposureSchema)
            or not artifact.meta
            or not artifact.meta.get("platform")
        ]

    def get_dwh_view(self, artifacts: List[GROUPABLE_ARTIFACT]) -> TreeGroupSchema:
        filtered_artifacts: List[GROUPABLE_ARTIFACT] = [
            artifact
            for artifact in artifacts
            if isinstance(
                artifact,
                (
                    NormalizedSourceSchema,
                    NormalizedModelSchema,
                    NormalizedSnapshotSchema,
                ),
            )
        ]
        return self.get_fqn_view(filtered_artifacts)

    def get_normalized_full_path_view(
        self,
        artifacts: List[GROUPABLE_ARTIFACT],
    ) -> TreeGroupSchema:
        filtered_artifacts = (
            artifact for artifact in artifacts if artifact.unique_id is not None
        )
        tree_builder = TreeBuilder[GroupItemSchema](separator=posixpath.sep)
        for artifact in filtered_artifacts:
            tree_builder.add(
                path=artifact.normalized_full_path, data=self._get_group_item(artifact)
            )
        return tree_builder.get_tree()

    def get_fqn_view(self, artifacts: List[GROUPABLE_ARTIFACT]) -> TreeGroupSchema:
        filtered_artifacts = (
            artifact
            for artifact in artifacts
            if artifact.unique_id is not None
            and artifact.fqn is not None
            and isinstance(
                artifact,
                (
                    NormalizedSourceSchema,
                    NormalizedModelSchema,
                    NormalizedSnapshotSchema,
                ),
            )
        )
        tree_builder = TreeBuilder[GroupItemSchema](separator=".")
        for artifact in filtered_artifacts:
            tree_builder.add(path=artifact.fqn, data=self._get_group_item(artifact))
        return tree_builder.get_tree()

    def get_tags_view(
        self,
        artifacts: List[GROUPABLE_ARTIFACT],
    ) -> TagsGroupSchema:
        filtered_artifacts = (
            artifact for artifact in artifacts if artifact.unique_id is not None
        )
        group = defaultdict(list)
        for artifact in filtered_artifacts:
            if artifact.tags:
                for tag in artifact.tags:
                    group[tag].append(self._get_group_item(artifact))
            else:
                group[NO_TAGS_DEFAULT_TREE].append(self._get_group_item(artifact))
        return dict(group)

    def get_owners_view(self, artifacts: List[GROUPABLE_ARTIFACT]) -> OwnersGroupSchema:
        filtered_artifacts = (
            artifact for artifact in artifacts if artifact.unique_id is not None
        )
        group = defaultdict(list)
        for artifact in filtered_artifacts:
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
