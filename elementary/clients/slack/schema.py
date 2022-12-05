from typing import List, Optional
from pydantic import BaseModel

SlackBlockType = dict
SlackBlocksType = List[Optional[SlackBlockType]]


class SlackMessageSchema(BaseModel):
    text: Optional[str] = None
    attachments: Optional[list] = None
    blocks: Optional[list] = None


class AlertDetailsPartSlackMessageSchema(BaseModel):
    result: Optional[SlackBlocksType] = None
    configuration: Optional[SlackBlocksType] = None


class AlertSlackMessageSchema(BaseModel):
    title: Optional[SlackBlocksType] = None
    preview: Optional[SlackBlocksType] = None
    details: Optional[AlertDetailsPartSlackMessageSchema] = None
