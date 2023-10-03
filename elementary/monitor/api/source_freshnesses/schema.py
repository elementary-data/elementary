from typing import Optional

from pydantic import BaseModel


class DbtSourceFreshnessResultSchema(BaseModel):
    status: str
    error_message: Optional[str] = None
    max_loaded_at_time_ago_in_s: Optional[float] = None
    max_loaded_at: Optional[str] = None


class SourceFreshnessMetadataSchema(BaseModel):
    test_unique_id: str
    database_name: Optional[str] = None
    schema_name: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    test_name: str
    latest_run_time: str
    latest_run_status: str
    model_unique_id: Optional[str] = None
    test_type: str
    test_sub_type: str
    description: Optional[str] = None
    configuration: dict


class SourceFreshnessResultSchema(BaseModel):
    metadata: SourceFreshnessMetadataSchema
    test_results: DbtSourceFreshnessResultSchema
