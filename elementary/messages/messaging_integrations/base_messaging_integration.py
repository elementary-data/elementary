from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Generic, Optional, TypeVar

from elementary.messages.blocks import Icon
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.exceptions import (
    MessageIntegrationReactionNotSupportedError,
    MessageIntegrationReplyNotSupportedError,
)
from elementary.utils.log import get_logger
from elementary.utils.pydantic_shim import BaseModel

logger = get_logger(__name__)


T = TypeVar("T")


class MessageSendResult(BaseModel, Generic[T]):
    timestamp: datetime
    message_format: str
    message_context: Optional[T] = None


class ReactionAction(Enum):
    ADD = "add"
    REMOVE = "remove"


DestinationType = TypeVar("DestinationType")
MessageContextType = TypeVar("MessageContextType", bound=BaseModel)


class BaseMessagingIntegration(ABC, Generic[DestinationType, MessageContextType]):
    @abstractmethod
    def parse_message_context(self, context: dict[str, Any]) -> MessageContextType:
        raise NotImplementedError

    @abstractmethod
    def send_message(
        self,
        destination: DestinationType,
        body: MessageBody,
    ) -> MessageSendResult[MessageContextType]:
        raise NotImplementedError

    @abstractmethod
    def supports_reply(self) -> bool:
        raise NotImplementedError

    def supports_actions(self) -> bool:
        return False

    def supports_reactions(self) -> bool:
        return False

    def reply_to_message(
        self,
        destination: DestinationType,
        message_context: MessageContextType,
        body: MessageBody,
    ) -> MessageSendResult[MessageContextType]:
        if not self.supports_reply():
            raise MessageIntegrationReplyNotSupportedError(type(self).__name__)
        raise NotImplementedError

    def react_to_message(
        self,
        destination: DestinationType,
        message_context: MessageContextType,
        icon: Icon,
        action: ReactionAction = ReactionAction.ADD,
    ) -> None:
        if not self.supports_reactions():
            raise MessageIntegrationReactionNotSupportedError(type(self).__name__)
        raise NotImplementedError
