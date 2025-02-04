# Elementary Messaging Integration System

## Overview

The Elementary Messaging Integration system provides a flexible and extensible framework for sending alerts and messages to various messaging platforms (e.g., Slack, Teams). The system is designed to support a gradual migration from the legacy integration system to a more generic messaging-based approach.

## Architecture

### BaseMessagingIntegration

The core of the new messaging system is the `BaseMessagingIntegration` abstract class. This class defines the contract that all messaging integrations must follow:

- `send_message()`: Send a message to a specific destination
- `supports_reply()`: Check if the integration supports message threading/replies
- `reply_to_message()`: Reply to an existing message (if supported)

### Key Components

1. **MessageBody**: A platform-agnostic representation of a message
2. **MessageSendResult**: Contains information about a sent message, including timestamp and platform-specific context
3. **DestinationType**: Generic type representing the destination for a message (e.g., webhook URL, channel)
4. **MessageContextType**: Generic type for platform-specific message context

## Migration Strategy

The system currently supports both:

- Legacy `BaseIntegration` implementations (e.g., Slack)
- New `BaseMessagingIntegration` implementations (e.g., Teams)

This dual support allows for a gradual migration path where:

1. New integrations are implemented using `BaseMessagingIntegration`
2. Existing integrations can be migrated one at a time
3. The legacy `BaseIntegration` will eventually be deprecated

## Implementing a New Integration

To add a new messaging platform integration:

1. Create a new class that extends `BaseMessagingIntegration`
2. Implement the required abstract methods:
   ```python
   def send_message(self, destination: DestinationType, message_body: MessageBody) -> MessageSendResult
   def supports_reply(self) -> bool
   def reply_to_message(self, destination, message_context, message_body) -> MessageSendResult  # if supported
   ```
3. Update the `Integrations` factory class to support the new integration

## Current Implementations

- **Teams**: Uses the new `BaseMessagingIntegration` system with webhook support
- **Slack**: Currently uses the legacy `BaseIntegration` system (planned for migration)

## Future Improvements

1. Complete migration of Slack to `BaseMessagingIntegration`
2. Add support for more messaging platforms
