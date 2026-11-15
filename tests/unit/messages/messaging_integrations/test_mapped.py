from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from elementary.messages.blocks import HeaderBlock, Icon
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.base_messaging_integration import (
    BaseMessagingIntegration,
    MessageSendResult,
    ReactionAction,
)
from elementary.messages.messaging_integrations.exceptions import (
    MessageIntegrationReactionNotSupportedError,
    MessagingIntegrationError,
)
from elementary.messages.messaging_integrations.mapped import MappedMessagingIntegration
from elementary.utils.pydantic_shim import BaseModel


class MockMessageContext(BaseModel):
    id: str


class MockMessagingIntegration(BaseMessagingIntegration[None, MockMessageContext]):
    def __init__(self, supports_reply: bool = True, supports_actions: bool = False):
        self.supports_reply_value = supports_reply
        self.supports_actions_value = supports_actions
        self.send_message_mock = MagicMock()
        self.send_message_mock.return_value = MessageSendResult(
            timestamp=datetime.now(),
            message_format="test_format",
            message_context=MockMessageContext(id="test_id"),
        )
        self.reply_to_message_mock = MagicMock()
        self.reply_to_message_mock.return_value = MessageSendResult(
            timestamp=datetime.now(),
            message_format="test_format",
            message_context=MockMessageContext(id="test_id"),
        )

    def parse_message_context(self, context: dict[str, Any]) -> MockMessageContext:
        return MockMessageContext(**context)

    def send_message(
        self, destination: None, body: MessageBody
    ) -> MessageSendResult[MockMessageContext]:
        return self.send_message_mock(destination, body)

    def supports_reply(self) -> bool:
        return self.supports_reply_value

    def supports_actions(self) -> bool:
        return self.supports_actions_value

    def reply_to_message(
        self,
        destination: None,
        message_context: MockMessageContext,
        body: MessageBody,
    ) -> MessageSendResult[MockMessageContext]:
        return self.reply_to_message_mock(destination, message_context, body)


@pytest.fixture
def mock_integration() -> MockMessagingIntegration:
    return MockMessagingIntegration()


@pytest.fixture
def mapped_integration(
    mock_integration: MockMessagingIntegration,
) -> MappedMessagingIntegration:
    return MappedMessagingIntegration({"test_destination": mock_integration})


def test_send_message_success(
    mapped_integration: MappedMessagingIntegration,
    mock_integration: MockMessagingIntegration,
) -> None:
    destination = "test_destination"
    body = MessageBody(blocks=[HeaderBlock(text="test message")])
    expected_result: MessageSendResult[MockMessageContext] = MessageSendResult(
        timestamp=datetime.now(),
        message_format="test_format",
        message_context=None,
    )
    mock_integration.send_message_mock.return_value = expected_result

    result = mapped_integration.send_message(destination, body)

    assert result == expected_result
    mock_integration.send_message_mock.assert_called_once_with(None, body)


def test_send_message_invalid_destination(
    mapped_integration: MappedMessagingIntegration,
) -> None:
    destination = "invalid_destination"
    body = MessageBody(blocks=[HeaderBlock(text="test message")])

    with pytest.raises(MessagingIntegrationError) as exc_info:
        mapped_integration.send_message(destination, body)
    assert str(exc_info.value) == "Invalid destination: invalid_destination"


@pytest.mark.parametrize(
    "integrations_support_reply,expected_support",
    [
        ([True, True], True),
        ([True, False], False),
        ([False, True], False),
        ([False, False], False),
    ],
)
def test_supports_reply(
    integrations_support_reply: List[bool], expected_support: bool
) -> None:
    integrations: Dict[str, BaseMessagingIntegration[None, MockMessageContext]] = {
        f"dest_{i}": MockMessagingIntegration(supports_reply=supports_reply)
        for i, supports_reply in enumerate(integrations_support_reply)
    }
    mapped_integration = MappedMessagingIntegration(integrations)

    result = mapped_integration.supports_reply()

    assert result == expected_support


@pytest.mark.parametrize(
    "integrations_support_actions,expected_support",
    [
        ([True, True], True),
        ([True, False], False),
        ([False, True], False),
        ([False, False], False),
    ],
)
def test_supports_actions(
    integrations_support_actions: List[bool], expected_support: bool
) -> None:
    integrations: Dict[str, BaseMessagingIntegration[None, MockMessageContext]] = {
        f"dest_{i}": MockMessagingIntegration(supports_actions=supports_actions)
        for i, supports_actions in enumerate(integrations_support_actions)
    }
    mapped_integration = MappedMessagingIntegration(integrations)

    result = mapped_integration.supports_actions()

    assert result == expected_support


def test_reply_to_message_success(
    mapped_integration: MappedMessagingIntegration,
    mock_integration: MockMessagingIntegration,
) -> None:
    destination = "test_destination"
    message_context = MagicMock()
    body = MessageBody(blocks=[HeaderBlock(text="test reply")])
    expected_result: MessageSendResult[MockMessageContext] = MessageSendResult(
        timestamp=datetime.now(),
        message_format="test_format",
        message_context=message_context,
    )
    mock_integration.reply_to_message_mock.return_value = expected_result

    result = mapped_integration.reply_to_message(destination, message_context, body)

    assert result == expected_result
    mock_integration.reply_to_message_mock.assert_called_once_with(
        None, message_context, body
    )


def test_reply_to_message_invalid_destination(
    mapped_integration: MappedMessagingIntegration,
) -> None:
    destination = "invalid_destination"
    message_context = MagicMock()
    body = MessageBody(blocks=[HeaderBlock(text="test reply")])

    with pytest.raises(MessagingIntegrationError) as exc_info:
        mapped_integration.reply_to_message(destination, message_context, body)
    assert str(exc_info.value) == "Invalid destination: invalid_destination"


def test_react_to_message_not_supported_by_default(
    mock_integration: MockMessagingIntegration,
) -> None:
    assert not mock_integration.supports_reactions()
    with pytest.raises(MessageIntegrationReactionNotSupportedError):
        mock_integration.react_to_message(
            None, MockMessageContext(id="test_id"), Icon.CHECK
        )


def test_supports_reactions_requires_every_integration(
    mapped_integration: MappedMessagingIntegration,
    mock_integration: MockMessagingIntegration,
) -> None:
    assert not mapped_integration.supports_reactions()
    mock_integration.supports_reactions = lambda: True  # type: ignore[method-assign]
    assert mapped_integration.supports_reactions()


@pytest.mark.parametrize("action", [ReactionAction.ADD, ReactionAction.REMOVE])
def test_react_to_message_delegates_to_destination_integration(
    mapped_integration: MappedMessagingIntegration,
    mock_integration: MockMessagingIntegration,
    action: ReactionAction,
) -> None:
    react_mock = MagicMock()
    mock_integration.react_to_message = react_mock  # type: ignore[method-assign]
    message_context = MockMessageContext(id="test_id")

    mapped_integration.react_to_message(
        "test_destination", message_context, Icon.CHECK, action
    )

    react_mock.assert_called_once_with(None, message_context, Icon.CHECK, action)


def test_react_to_message_invalid_destination(
    mapped_integration: MappedMessagingIntegration,
) -> None:
    with pytest.raises(MessagingIntegrationError):
        mapped_integration.react_to_message(
            "invalid_destination", MockMessageContext(id="test_id"), Icon.CHECK
        )
