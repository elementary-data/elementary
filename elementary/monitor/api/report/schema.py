from typing import Optional

from pydantic import BaseModel


class ReportDataSchema(BaseModel):
    creation_time: str
    days_back: int
    models: dict
    sidebars: dict
    invocations: dict
    test_results: dict
    test_results_totals: dict
    test_runs: dict
    test_runs_totals: dict
    coverages: dict
    model_runs: list
    model_runs_totals: dict
    filters: dict
    lineage: dict
    env: Optional[dict]
    tracking: Optional[dict]
