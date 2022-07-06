import copy
import os
from typing import Dict, Optional

from clients.api.api import APIClient
from clients.dbt.dbt_runner import DbtRunner
from monitor.api.models.models import ModelsAPI
from monitor.api.models.schema import NormalizedModelSchema
from monitor.api.tests.tests import TestsAPI


class SidebarAPI(APIClient):
    def __init__(self, dbt_runner: DbtRunner):
        super().__init__(dbt_runner)
        self.models_api = ModelsAPI(dbt_runner=self.dbt_runner)
        self.tests_api = TestsAPI(dbt_runner=self.dbt_runner)

    def get_sidebar(
        self,
        models: Optional[Dict[str, NormalizedModelSchema]] = None,
        sources: Optional[Dict[str, NormalizedModelSchema]] = None
    ) -> dict:
        sidebar = dict()
        models = models if models is not None else self.models_api.get_models()
        sources = sources if sources is not None else self.models_api.get_sources()
        for model in [*models.values(), *sources.values()]:
            SidebarAPI._update_dbt_sidebar(
                dbt_sidebar=sidebar,
                model_unique_id=model.unique_id,
                model_full_path=model.normalized_full_path
            )
        return sidebar

    @classmethod
    def _update_dbt_sidebar(cls, dbt_sidebar: dict, model_unique_id: str, model_full_path: str) -> None:
        if model_unique_id is None or model_full_path is None:
            return
        model_full_path_split = model_full_path.split(os.path.sep)
        for part in model_full_path_split:
            if part.endswith('.sql'):
                if 'files' in dbt_sidebar:
                    if model_unique_id not in dbt_sidebar['files']:
                        dbt_sidebar['files'].append(model_unique_id)
                else:
                    dbt_sidebar['files'] = [model_unique_id]
            else:
                if part not in dbt_sidebar:
                    dbt_sidebar[part] = {}
                dbt_sidebar = dbt_sidebar[part]
