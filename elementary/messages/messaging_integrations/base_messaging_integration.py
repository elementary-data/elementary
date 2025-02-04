from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, Optional, TypeVar

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
    message_context: Optional[T] = None


DestinationType = TypeVar("DestinationType")
MessageContextType = TypeVar("MessageContextType")


class BaseMessagingIntegration(ABC, Generic[DestinationType, MessageContextType]):
    @abstractmethod
    def send_message(
        self,
        destination: DestinationType,
        message_body: MessageBody,
    ) -> MessageSendResult[MessageContextType]:
        raise NotImplementedError

    @abstractmethod
    def supports_reply(self) -> bool:
        raise NotImplementedError

    def reply_to_message(
        self,
        destination: DestinationType,
        message_context: MessageContextType,
        message_body: MessageBody,
    ) -> MessageSendResult[MessageContextType]:
        if not self.supports_reply():
            raise MessageIntegrationReplyNotSupportedError
        raise NotImplementedError
