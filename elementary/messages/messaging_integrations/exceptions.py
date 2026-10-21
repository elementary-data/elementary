class MessagingIntegrationError(Exception):
    pass


class MessageIntegrationReplyNotSupportedError(MessagingIntegrationError):
    def __init__(self, integration_name: str):
        self.integration_name = integration_name
        super().__init__(f"{integration_name} does not support replying to messages")


class MessagingIntegrationDestinationNotFoundError(MessagingIntegrationError):
    def __init__(self, channel_name: str):
        self.channel_name = channel_name
        super().__init__(
            f"Channel {channel_name} was not found by the Elementary app. Please add the app to the channel."
        )
