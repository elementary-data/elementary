from typing import List, Optional

from elementary.utils.log import get_logger
from elementary.utils.pydantic_shim import BaseModel, validator

logger = get_logger(__name__)

SLACK_MESSAGE_ATTACHMENTS_LIMIT = 50

SlackBlockType = dict
SlackBlocksType = List[SlackBlockType]


class SlackMessageSchema(BaseModel):
    text: Optional[str] = None
    attachments: Optional[list] = None
    blocks: Optional[list] = None

    @validator("attachments", pre=True)
    def validate_attachments(cls, attachments):
        if (
            isinstance(attachments, list)
            and len(attachments) > SLACK_MESSAGE_ATTACHMENTS_LIMIT
        ):
            logger.error(
                f"Slack message attachments limit is {SLACK_MESSAGE_ATTACHMENTS_LIMIT}, but {len(attachments)} attachments were provided. Attachments were removed from the message.\nhis shouldn't happen, please notify Elementary's support channel"
            )
            return None
        return attachments
