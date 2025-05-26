from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel

from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.exceptions import (
    MessageIntegrationReplyNotSupportedError,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


T = TypeVar("T")


class MessageSendResult(BaseModel, Generic[T]):
    timestamp: datetime
    message_format: str
    message_context: Optional[T] = None


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

    def reply_to_message(
        self,
        destination: DestinationType,
        message_context: MessageContextType,
        body: MessageBody,
    ) -> MessageSendResult[MessageContextType]:
        if not self.supports_reply():
            raise MessageIntegrationReplyNotSupportedError(type(self).__name__)
        raise NotImplementedError
