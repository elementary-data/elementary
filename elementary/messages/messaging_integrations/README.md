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

1. **MessageBody**: A platform-agnostic representation of a message, containing:

   - `blocks`: List of message blocks (headers, code, dividers, lines, facts, expandable sections)
   - `color`: Optional color theme (red, yellow, green)

2. **MessageSendResult**: Contains information about a sent message, including timestamp and platform-specific context
3. **DestinationType**: Generic type representing the destination for a message (e.g., webhook URL, channel)
4. **MessageContextType**: Generic type for platform-specific message context

## Contributing a New Integration

### Prerequisites

1. Understand Elementary's message blocks (see `message_body.py`)
2. Check if your platform's message format is already supported in `elementary/messages/formats/`
3. Review existing implementations (e.g., Teams with Adaptive Cards) for reference

### Step 1: Message Format

If your platform's message format is not yet supported:

1. Create a new format module in `elementary/messages/formats/{format_name}.py`
2. Implement conversion of all Elementary message blocks:
   ```python
   # Required message blocks to support:
   - HeaderBlock: Title/heading formatting
   - CodeBlock: Code snippets with optional syntax highlighting
   - DividerBlock: Visual separator
   - LinesBlock: Plain text content
   - FactListBlock: Key-value pairs
   - ExpandableBlock: Collapsible sections
   - MentionBlock: Mention a user
   - TableBlock: Table of data
   - WhitespaceBlock: Whitespace for indentation
   ```
3. Add tests in `tests/unit/messages/formats/`

See existing implementations for reference:

- `adaptive_cards.py` - Microsoft Teams Adaptive Cards
- `block_kit.py` - Slack Block Kit

### Step 2: Messaging Integration

Once the message format is ready:

1. Create `{platform_name}.py` in this directory

2. Define your destination and context types:

   - **Destination**: Where to send the message (e.g., webhook URL, channel ID, user ID)
   - **MessageContext**: Information needed to identify a sent message for replies (e.g., message ID, thread ID)
   - Both should be Pydantic models with appropriate fields for your platform

3. Implement the integration class:

   - Extend `BaseMessagingIntegration[YourDestination, YourContext]`
   - Implement `send_message()`: Convert message format and send to platform
   - Implement `supports_reply()`: Return True only if your platform supports replies
   - Implement `reply_to_message()` if supported: Use message context to reply

4. Add error handling:

   - Use exceptions from `exceptions.py`
   - Handle platform-specific errors
   - Provide clear error messages

5. Add support in the integrations factory:

   - Update `elementary/monitor/data_monitoring/alerts/integrations/integrations.py`
   - Add your integration to `get_integration()` method
   - Add destination creation to `get_destination()` method

6. Add configuration support:
   - Add your platform's configuration to `Config` class
   - Support both CLI arguments and config file input
   - Make sure to get all required information from users to create your destination type
   - See Teams implementation for reference (webhook URL configuration)

## Implementing a New Integration

To add a new messaging platform integration:

1. Create a new class that extends `BaseMessagingIntegration`
2. Implement the required abstract methods:
   ```python
   def send_message(self, destination: DestinationType, body: MessageBody) -> MessageSendResult
   def supports_reply(self) -> bool
   def reply_to_message(self, destination, message_context, message_body) -> MessageSendResult  # if supported
   ```
3. Update the `Integrations` factory class to support the new integration

## Current Implementations

- **Teams**: Webhook support, Adaptive Cards format
- **Slack**: Webhook and token support, Block Kit format

## Future Improvements

1. Add support for more messaging platforms
