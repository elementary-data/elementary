import os
from collections import defaultdict
from typing import List, Union

from elementary.clients.api.api import APIClient
from elementary.monitor.api.models.schema import NormalizedModelSchema
from elementary.monitor.api.sidebar.schema import (
    DbtSidebarSchema,
    OwnersSidebarSchema,
    SidebarsSchema,
    TagsSidebarSchema,
)

SIDEBAR_FILES_KEYWORD = "__files__"
NO_TAGS_DEFAULT_TREE = "No tags"
NO_OWNERS_DEFAULT_TREE = "No owners"


class SidebarAPI(APIClient):
    def get_sidebars(
        self, artifacts: List[Union[NormalizedModelSchema, NormalizedModelSchema]]
    ) -> SidebarsSchema:
        dbt_sidebar = self.get_dbt_sidebar(artifacts)
        tags_sidebar = self.get_tags_sidebar(artifacts)
        owners_sidebar = self.get_owners_sidebar(artifacts)
        return SidebarsSchema(dbt=dbt_sidebar, tags=tags_sidebar, owners=owners_sidebar)

    def get_dbt_sidebar(
        self, artifacts: List[Union[NormalizedModelSchema, NormalizedModelSchema]]
    ) -> DbtSidebarSchema:
        sidebar = dict()
        for artifact in artifacts:
            self._update_dbt_sidebar(
                dbt_sidebar=sidebar,
                artifact_unique_id=artifact.unique_id,
                artifact_full_path=artifact.normalized_full_path,
            )
        return sidebar

    @classmethod
    def _update_dbt_sidebar(
        cls, dbt_sidebar: dict, artifact_unique_id: str, artifact_full_path: str
    ) -> None:
        if artifact_unique_id is None or artifact_full_path is None:
            return
        artifact_full_path_split = artifact_full_path.split(os.path.sep)
        for part in artifact_full_path_split:
            if part.endswith(".sql"):
                if SIDEBAR_FILES_KEYWORD in dbt_sidebar:
                    if artifact_unique_id not in dbt_sidebar[SIDEBAR_FILES_KEYWORD]:
                        dbt_sidebar[SIDEBAR_FILES_KEYWORD].append(artifact_unique_id)
                else:
                    dbt_sidebar[SIDEBAR_FILES_KEYWORD] = [artifact_unique_id]
            else:
                if part not in dbt_sidebar:
                    dbt_sidebar[part] = {}
                dbt_sidebar = dbt_sidebar[part]

    def get_tags_sidebar(
        self, artifacts: List[Union[NormalizedModelSchema, NormalizedModelSchema]]
    ) -> TagsSidebarSchema:
        sidebar = defaultdict(list)
        for artifact in artifacts:
            unique_id = artifact.unique_id
            if artifact.tags:
                for tag in artifact.tags:
                    sidebar[tag].append(unique_id)
            else:
                sidebar[NO_TAGS_DEFAULT_TREE].append(unique_id)
        return sidebar

    def get_owners_sidebar(
        self, artifacts: List[Union[NormalizedModelSchema, NormalizedModelSchema]]
    ) -> OwnersSidebarSchema:
        sidebar = defaultdict(list)
        for artifact in artifacts:
            unique_id = artifact.unique_id
            if artifact.owners:
                for owner in artifact.owners:
                    sidebar[owner].append(unique_id)
            else:
                sidebar[NO_OWNERS_DEFAULT_TREE].append(unique_id)
        return sidebar
