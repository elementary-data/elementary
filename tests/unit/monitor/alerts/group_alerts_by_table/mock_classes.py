from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union


@dataclass
class MockAlert:
    status: Optional[str]  # should be in "warn", "error" , "fail[ure]"
    slack_group_alerts_by: Optional[str]
    model_unique_id: Optional[str]
    slack_channel: Optional[str]
    detected_at: Optional[Union[str, datetime]]
    model_meta: Optional[dict]
    owners: Optional[List[str]]
    subscribers: Optional[List[str]]
    tags: Optional[List[str]]
    database_name: str = "elementary_test_db"
    schema_name: str = "master_elementary"
    concise_name: str = "Alert"

    def __repr__(self):
        return f"{self.status} {self.model_unique_id} {self.detected_at}"


@dataclass
class MockConfig:
    slack_group_alerts_by: Optional[str]
    slack_channel_name: Optional[str]
    env: str = "dev"


@dataclass
class MockDataMonitoringAlerts:
    config: MockConfig
    execution_properties: Dict
