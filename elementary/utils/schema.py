import json
from typing import Union

from elementary.utils.json_utils import try_load_json
from elementary.utils.pydantic_shim import BaseModel


class ExtendedBaseModel(BaseModel):
    @staticmethod
    def _load_var_to_dict(var: Union[str, dict]) -> dict:
        if not var:
            return {}
        elif isinstance(var, dict):
            return var
        elif isinstance(var, str):
            return json.loads(var)

    @staticmethod
    def _load_var_to_list(var: Union[str, list]) -> list:
        if not var:
            return []
        elif isinstance(var, list):
            return var
        elif isinstance(var, str):
            loaded_var = try_load_json(var)
            if isinstance(loaded_var, dict):
                loaded_var = [json.dumps(loaded_var)]
            if loaded_var is None:
                loaded_var = [var]
            return loaded_var
