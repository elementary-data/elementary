from typing import Optional

from pydantic import BaseModel

from elementary.clients.slack.schema import SlackBlocksType


class AlertDetailsPartSlackMessageSchema(BaseModel):
    result: Optional[SlackBlocksType] = None
    configuration: Optional[SlackBlocksType] = None


class SlackAlertMessageSchema(BaseModel):
    title: Optional[SlackBlocksType] = None
    preview: Optional[SlackBlocksType] = None
    details: Optional[AlertDetailsPartSlackMessageSchema] = None
