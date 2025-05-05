import os
from datetime import datetime

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
        self.directory = os.path.abspath(directory)
        self._create_if_missing = create_if_missing

        if not os.path.exists(self.directory):
            if self._create_if_missing:
                logger.info(
                    "Creating directory for FileSystemMessagingIntegration: %s",
                    self.directory,
                )
                os.makedirs(self.directory, exist_ok=True)
            else:
                raise MessagingIntegrationError(
                    f"Directory {self.directory} does not exist and create_if_missing is False"
                )

    def supports_reply(self) -> bool:
        return False

    def send_message(
        self, destination: str, body: MessageBody
    ) -> MessageSendResult[EmptyMessageContext]:
        file_path = os.path.join(self.directory, destination)

        if not os.path.exists(file_path) and not self._create_if_missing:
            raise MessagingIntegrationError(
                f"File {file_path} does not exist and create_if_missing is False"
            )

        try:
            logger.info("Writing alert message to file %s", file_path)
            with open(file_path, "a", encoding="utf-8") as fp:
                fp.write(body.json())
                fp.write("\n")
        except Exception as exc:
            logger.error(
                "Failed to write alert message to file %s: %s",
                file_path,
                exc,
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
