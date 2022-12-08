from typing import List, Optional

from pydantic import BaseModel

SlackBlockType = dict
SlackBlocksType = List[Optional[SlackBlockType]]


class SlackMessageSchema(BaseModel):
    text: Optional[str] = None
    attachments: Optional[list] = None
    blocks: Optional[list] = None
