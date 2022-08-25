from typing import Optional
from pydantic import BaseModel


class SlackMessageSchema(BaseModel):
    text: Optional[str] = None
    attachments: Optional[list] = None
    blocks: Optional[list] = None
