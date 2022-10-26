from typing import Optional
from pydantic import BaseModel


class SlackMessageSchema(BaseModel):
    text: Optional[str] = None
    blocks: Optional[list] = None
