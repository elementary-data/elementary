import json
from pathlib import Path

import pytest

from elementary.messages.blocks import LineBlock, LinesBlock, TextBlock
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.exceptions import (
    MessagingIntegrationError,
)
from elementary.messages.messaging_integrations.file_system import (
    FileSystemMessagingIntegration,
)


def _build_body() -> MessageBody:
    return MessageBody(
        blocks=[LinesBlock(lines=[LineBlock(inlines=[TextBlock(text="hello")])])]
    )


def test_send_message_creates_file_and_appends(tmp_path: Path) -> None:
    directory = tmp_path / "alerts"
    integ = FileSystemMessagingIntegration(directory=str(directory))
    body = _build_body()

    integ.send_message("channel.json", body)

    target_file = directory / "channel.json"
    assert target_file.exists()

    with target_file.open() as fp:
        lines = fp.readlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == json.loads(body.json())


def test_send_multiple_messages(tmp_path: Path) -> None:
    directory = tmp_path / "alerts"
    integ = FileSystemMessagingIntegration(directory=str(directory))
    body1 = _build_body()
    body2 = _build_body()

    integ.send_message("channel.json", body1)
    integ.send_message("channel.json", body2)

    target_file = directory / "channel.json"
    with target_file.open() as fp:
        lines = fp.readlines()

    assert len(lines) == 2
    assert json.loads(lines[0]) == json.loads(body1.json())
    assert json.loads(lines[1]) == json.loads(body2.json())


def test_send_message_no_create_flag(tmp_path: Path) -> None:
    directory = tmp_path / "alerts-no-create"
    with pytest.raises(MessagingIntegrationError):
        FileSystemMessagingIntegration(
            directory=str(directory), create_if_missing=False
        )
