from typing import Optional

from elementary.utils.pydantic_shim import BaseModel


class ReportDataEnvSchema(BaseModel):
    project_name: Optional[str] = None
    env: Optional[str] = None
    warehouse_type: Optional[str] = None


class ReportDataSchema(BaseModel):
    creation_time: Optional[str] = None
    days_back: Optional[int] = None
    models: dict = dict()
    groups: dict = dict()
    invocation: dict = dict()
    tests: dict = dict()
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
    invocations_job_identification: dict = dict()
    env: ReportDataEnvSchema = ReportDataEnvSchema()
    tracking: Optional[dict] = None
