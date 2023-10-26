import posixpath
from collections import defaultdict
from typing import List, Union

from elementary.clients.api.api_client import APIClient
from elementary.monitor.api.groups.schema import (
    DbtGroupSchema,
    GroupItemSchema,
    GroupsSchema,
    OwnersGroupSchema,
    TagsGroupSchema,
)
from elementary.monitor.api.models.schema import (
    NormalizedExposureSchema,
    NormalizedModelSchema,
    NormalizedSourceSchema,
)

FILES_GROUP_KEYWORD = "__files__"
NO_TAGS_DEFAULT_TREE = "No tags"
NO_OWNERS_DEFAULT_TREE = "No owners"


GROUPABLE_ARTIFACT = Union[
    NormalizedModelSchema, NormalizedSourceSchema, NormalizedExposureSchema
]


class GroupsAPI(APIClient):
    def get_groups(self, artifacts: List[GROUPABLE_ARTIFACT]) -> GroupsSchema:
        dbt_group = self.get_dbt_group(artifacts)
        tags_group = self.get_tags_group(artifacts)
        owners_group = self.get_owners_group(artifacts)
        return GroupsSchema(dbt=dbt_group, tags=tags_group, owners=owners_group)

    def get_dbt_group(
        self,
        artifacts: List[GROUPABLE_ARTIFACT],
    ) -> DbtGroupSchema:
        group: DbtGroupSchema = dict()
        for artifact in artifacts:
            if artifact.unique_id is None:
                continue
            self._update_dbt_group(dbt_group=group, artifact=artifact)
        return group

    def _update_dbt_group(
        self,
        dbt_group: dict,
        artifact: GROUPABLE_ARTIFACT,
    ) -> None:
        if artifact.unique_id is None or artifact.normalized_full_path is None:
            return

        artifact_full_path_split = artifact.normalized_full_path.split(posixpath.sep)
        if isinstance(artifact, NormalizedExposureSchema):
            # For exposures, we want the path to be the path in the BI, but to start with <package name>/<artifact type>
            # like other artifacts.
            # NOTE - if there is no path provided in the BI, the FQN will just be the exposure name.
            artifact_full_path_split = []
            if artifact.package_name:
                artifact_full_path_split.append(artifact.package_name)
            artifact_full_path_split.append("exposures")
            artifact_full_path_split.extend(artifact.fqn.split("/"))

        for part in artifact_full_path_split[:-1]:
            if part not in dbt_group:
                dbt_group[part] = {}
            dbt_group = dbt_group[part]

        if FILES_GROUP_KEYWORD in dbt_group:
            if artifact.unique_id not in dbt_group[FILES_GROUP_KEYWORD]:
                dbt_group[FILES_GROUP_KEYWORD].append(self._get_group_item(artifact))
        else:
            dbt_group[FILES_GROUP_KEYWORD] = [self._get_group_item(artifact)]

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
