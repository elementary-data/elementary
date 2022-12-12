import os
from typing import Dict

from elementary.clients.api.api import APIClient
from elementary.monitor.api.models.schema import (
    NormalizedModelSchema,
    NormalizedSourceSchema,
)

SIDEBAR_FILES_KEYWORD = "__files__"


class SidebarAPI(APIClient):
    def get_sidebar(
        self,
        models: Dict[str, NormalizedModelSchema],
        sources: Dict[str, NormalizedSourceSchema],
    ) -> dict:
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
