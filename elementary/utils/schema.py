import json
from typing import Union

from pydantic import BaseModel


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
            return []
        elif isinstance(var, str):
            return json.loads(var)
