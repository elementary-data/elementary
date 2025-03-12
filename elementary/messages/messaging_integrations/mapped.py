from typing import Generic, Mapping

from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.base_messaging_integration import (
    BaseMessagingIntegration,
    MessageContextType,
    MessageSendResult,
)
from elementary.messages.messaging_integrations.exceptions import (
    MessagingIntegrationError,
)


class MappedMessagingIntegration(
    Generic[MessageContextType],
    BaseMessagingIntegration[str, MessageContextType],
):
    def __init__(
        self, mapping: Mapping[str, BaseMessagingIntegration[None, MessageContextType]]
    ):
        self._mapping = mapping

    def send_message(
        self, destination: str, body: MessageBody
    ) -> MessageSendResult[MessageContextType]:
        if destination not in self._mapping:
            raise MessagingIntegrationError(f"Invalid destination: {destination}")
        return self._mapping[destination].send_message(None, body)

    def supports_reply(self) -> bool:
        return all(
            integration.supports_reply() for integration in self._mapping.values()
        )

    def supports_actions(self) -> bool:
        return all(
            integration.supports_actions() for integration in self._mapping.values()
        )

    def reply_to_message(
        self, destination: str, message_context: MessageContextType, body: MessageBody
    ) -> MessageSendResult[MessageContextType]:
        if destination not in self._mapping:
            raise MessagingIntegrationError(f"Invalid destination: {destination}")
        return self._mapping[destination].reply_to_message(None, message_context, body)
