from typing import List, Optional

from elementary.utils.pydantic_shim import BaseModel


class FilterSchema(BaseModel):
    name: str
    display_name: str
    model_unique_ids: List[Optional[str]] = []

    def add_model_unique_id(self, model_unique_id: Optional[str]):
        new_model_unique_ids = list({*self.model_unique_ids, model_unique_id})
        self.model_unique_ids = new_model_unique_ids


class FiltersSchema(BaseModel):
    test_results: List[FilterSchema] = list()
    test_runs: List[FilterSchema] = list()
    model_runs: List[FilterSchema] = list()
