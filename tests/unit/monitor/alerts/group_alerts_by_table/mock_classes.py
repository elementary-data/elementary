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
    model_meta: Optional[
        str
    ]  # this string should be a json of a dict that has or has not the key "channel"
    owners: Optional[List[str]]
    subscribers: Optional[List[str]]
    tags: Optional[List[str]]
    database_name: str = "elementary_test_db"
    schema_name: str = "master_elementary"
    concise_name: str = "Alert"


@dataclass
class MockConfig:
    slack_group_alerts_by: Optional[str]
    slack_channel_name: Optional[str]
    env: str = "dev"


@dataclass
class MockDataMonitoringAlerts:
    config: MockConfig
    execution_properties: Dict
