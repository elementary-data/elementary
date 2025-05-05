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


def test_send_message_creates_file_in_channel_dir(tmp_path: Path) -> None:
    root_dir = tmp_path / "alerts"
    integ = FileSystemMessagingIntegration(directory=str(root_dir))
    body = _build_body()

    integ.send_message("channel", body)

    channel_dir = root_dir / "channel"
    files = list(channel_dir.glob("*.json"))
    assert len(files) == 1, "Expected exactly one file in the channel directory"

    message_json = files[0].read_text()
    assert json.loads(message_json) == json.loads(body.json())


def test_send_multiple_messages_creates_multiple_files(tmp_path: Path) -> None:
    root_dir = tmp_path / "alerts"
    integ = FileSystemMessagingIntegration(directory=str(root_dir))
    body1 = _build_body()
    body2 = _build_body()

    integ.send_message("channel", body1)
    integ.send_message("channel", body2)

    channel_dir = root_dir / "channel"
    files = sorted(channel_dir.glob("*.json"))

    assert len(files) == 2
    assert json.loads(files[0].read_text()) == json.loads(body1.json())
    assert json.loads(files[1].read_text()) == json.loads(body2.json())


def test_send_message_no_create_flag(tmp_path: Path) -> None:
    directory = tmp_path / "alerts-no-create"
    with pytest.raises(MessagingIntegrationError):
        FileSystemMessagingIntegration(
            directory=str(directory), create_if_missing=False
        )
