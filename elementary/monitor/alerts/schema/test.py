from typing import Optional

from pydantic import BaseModel


class TestResultSchema(BaseModel):
    result_description: Optional[str] = None
    result_query: Optional[str] = None


class DbtTestConfigurationSchema(BaseModel):
    test_name: str
    test_params: Optional[dict] = None


class AnomalyTestConfigurationSchema(BaseModel):
    test_name: str
    timestamp_column: Optional[str] = None
    testing_timeframe: Optional[str] = None
    anomaly_threshold: Optional[str] = None
