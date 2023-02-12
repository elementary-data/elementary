from typing import List, Optional

from pydantic import BaseModel


class FilterSchema(BaseModel):
    name: str
    display_name: str
    model_unique_ids: List[Optional[str]] = []

    def add_model_unique_id(self, model_unique_id: str):
        new_model_unique_ids = list(set([*self.model_unique_ids, model_unique_id]))
        self.model_unique_ids = new_model_unique_ids


class FiltersSchema(BaseModel):
    test_results: List[FilterSchema]
    test_runs: List[FilterSchema]
    model_runs: List[FilterSchema]
