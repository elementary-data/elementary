from typing import List, Optional

from pydantic import BaseModel, validator

from elementary.utils.log import get_logger

logger = get_logger(__name__)

SlackBlockType = dict
SlackBlocksType = List[Optional[SlackBlockType]]


class SlackMessageSchema(BaseModel):
    text: Optional[str] = None
    attachments: Optional[list] = None
    blocks: Optional[list] = None

    @validator("attachments", pre=True)
    def validate_attachments(cls, attachments):
        if isinstance(attachments, list) and len(attachments) > 50:
            logger.error(
                f"Slack message attachments limit is 50. You have {len(attachments)} attachments. Attachments were removed fro the message"
            )
            return None
        return attachments
