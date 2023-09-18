from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class FilterSchema(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    name: str
    display_name: str
    model_unique_ids: List[Optional[str]] = []

    def add_model_unique_id(self, model_unique_id: Optional[str]):
        new_model_unique_ids = list({*self.model_unique_ids, model_unique_id})
        self.model_unique_ids = new_model_unique_ids


class FiltersSchema(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    test_results: List[FilterSchema] = list()
    test_runs: List[FilterSchema] = list()
    model_runs: List[FilterSchema] = list()
