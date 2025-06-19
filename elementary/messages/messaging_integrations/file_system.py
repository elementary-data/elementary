import json
from datetime import datetime
from pathlib import Path
from typing import Any

from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.base_messaging_integration import (
    BaseMessagingIntegration,
    MessageSendResult,
)
from elementary.messages.messaging_integrations.empty_message_context import (
    EmptyMessageContext,
)
from elementary.messages.messaging_integrations.exceptions import (
    MessagingIntegrationError,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class FileSystemMessagingIntegration(
    BaseMessagingIntegration[str, EmptyMessageContext]
):
    def __init__(self, directory: str, create_if_missing: bool = True) -> None:
        self.directory = Path(directory).expanduser().resolve()
        self._create_if_missing = create_if_missing

        if not self.directory.exists():
            if self._create_if_missing:
                logger.info(
                    f"Creating directory for FileSystemMessagingIntegration: {self.directory}"
                )
                self.directory.mkdir(parents=True, exist_ok=True)
            else:
                raise MessagingIntegrationError(
                    f"Directory {self.directory} does not exist and create_if_missing is False"
                )

    def parse_message_context(self, context: dict[str, Any]) -> EmptyMessageContext:
        return EmptyMessageContext(**context)

    def supports_reply(self) -> bool:
        return False

    def send_message(
        self, destination: str, body: MessageBody
    ) -> MessageSendResult[EmptyMessageContext]:
        channel_dir = self.directory / destination
        if not channel_dir.exists():
            if self._create_if_missing:
                channel_dir.mkdir(parents=True, exist_ok=True)
            else:
                raise MessagingIntegrationError(
                    f"Channel directory {channel_dir} does not exist and create_if_missing is False"
                )

        filename = datetime.utcnow().strftime("%Y%m%dT%H%M%S_%fZ.json")
        file_path = channel_dir / filename

        try:
            json_str = json.dumps(body.dict(), indent=2)
            file_path.write_text(json_str, encoding="utf-8")
        except Exception as exc:
            logger.error(
                f"Failed to write alert message to file {file_path}: {exc}",
                exc_info=True,
            )
            raise MessagingIntegrationError(
                f"Failed writing alert message to file {file_path}"
            ) from exc

        return MessageSendResult(
            timestamp=datetime.utcnow(),
            message_format="json",
            message_context=EmptyMessageContext(),
        )
