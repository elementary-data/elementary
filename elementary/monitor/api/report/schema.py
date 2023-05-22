from typing import Optional

from pydantic import BaseModel


class ReportDataSchema(BaseModel):
    creation_time: Optional[str] = None
    days_back: Optional[int] = None
    models: dict = dict()
    sidebars: dict = dict()
    invocation: dict = dict()
    test_results: dict = dict()
    test_results_totals: dict = dict()
    test_runs: dict = dict()
    test_runs_totals: dict = dict()
    coverages: dict = dict()
    model_runs: list = list()
    model_runs_totals: dict = dict()
    filters: dict = dict()
    lineage: dict = dict()
    invocations: list = list()
    resources_latest_invocation: dict = dict()
    env: dict = dict()
    tracking: Optional[dict] = None
