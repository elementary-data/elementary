from collections import defaultdict
import os
from typing import Dict

from elementary.clients.api.api import APIClient
from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.monitor.api.models.models import ModelsAPI
from elementary.monitor.api.models.schema import NormalizedModelSchema
from elementary.monitor.api.sidebar.schema import (
    DbtSidebarSchema,
    OwnersSidebarSchema,
    TagsSidebarSchema,
    SidebarsSchema,
)
from elementary.monitor.api.tests.tests import TestsAPI
from elementary.utils.json_utils import try_load_json

SIDEBAR_FILES_KEYWORD = "__files__"
NO_TAGS_DEFAULT_TREE = "No tags"
NO_OWNERS_DEFAULT_TREE = "No owners"


class SidebarAPI(APIClient):
    def __init__(self, dbt_runner: DbtRunner):
        super().__init__(dbt_runner)
        self.models_api = ModelsAPI(dbt_runner=self.dbt_runner)
        self.tests_api = TestsAPI(dbt_runner=self.dbt_runner)

    def get_sidebars(
        self,
        models: Dict[str, NormalizedModelSchema],
        sources: Dict[str, NormalizedModelSchema],
    ) -> SidebarsSchema:
        dbt_sidebar = self.get_dbt_sidebar(models, sources)
        tags_sidebar = self.get_tags_sidebar(models, sources)
        owners_sidebar = self.get_owners_sidebar(models, sources)
        return SidebarsSchema(dbt=dbt_sidebar, tags=tags_sidebar, owners=owners_sidebar)

    def get_dbt_sidebar(
        self,
        models: Dict[str, NormalizedModelSchema],
        sources: Dict[str, NormalizedModelSchema],
    ) -> DbtSidebarSchema:
        sidebar = dict()
        for model in [*models.values(), *sources.values()]:
            self._update_dbt_sidebar(
                dbt_sidebar=sidebar,
                model_unique_id=model.unique_id,
                model_full_path=model.normalized_full_path,
            )
        return sidebar

    @classmethod
    def _update_dbt_sidebar(
        cls, dbt_sidebar: dict, model_unique_id: str, model_full_path: str
    ) -> None:
        if model_unique_id is None or model_full_path is None:
            return
        model_full_path_split = model_full_path.split(os.path.sep)
        for part in model_full_path_split:
            if part.endswith(".sql"):
                if SIDEBAR_FILES_KEYWORD in dbt_sidebar:
                    if model_unique_id not in dbt_sidebar[SIDEBAR_FILES_KEYWORD]:
                        dbt_sidebar[SIDEBAR_FILES_KEYWORD].append(model_unique_id)
                else:
                    dbt_sidebar[SIDEBAR_FILES_KEYWORD] = [model_unique_id]
            else:
                if part not in dbt_sidebar:
                    dbt_sidebar[part] = {}
                dbt_sidebar = dbt_sidebar[part]

    def get_tags_sidebar(
        self,
        models: Dict[str, NormalizedModelSchema],
        sources: Dict[str, NormalizedModelSchema],
    ) -> TagsSidebarSchema:
        sidebar = defaultdict(list)
        for artifact in [*models.values(), *sources.values()]:
            unique_id = artifact.unique_id
            if artifact.tags:
                for tag in artifact.tags:
                    sidebar[tag].append(unique_id)
            else:
                sidebar[NO_TAGS_DEFAULT_TREE].append(unique_id)
        return sidebar

    def get_owners_sidebar(
        self,
        models: Dict[str, NormalizedModelSchema],
        sources: Dict[str, NormalizedModelSchema],
    ) -> OwnersSidebarSchema:
        sidebar = defaultdict(list)
        for artifact in [*models.values(), *sources.values()]:
            unique_id = artifact.unique_id
            if artifact.owners:
                for owner in artifact.owners:
                    sidebar[owner].append(unique_id)
            else:
                sidebar[NO_OWNERS_DEFAULT_TREE].append(unique_id)
        return sidebar
