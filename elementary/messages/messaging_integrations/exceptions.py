class MessagingIntegrationError(Exception):
    pass


class MessageIntegrationReplyNotSupportedError(MessagingIntegrationError):
    def __init__(self, integration_name: str):
        self.integration_name = integration_name
        super().__init__(f"{integration_name} does not support replying to messages")
