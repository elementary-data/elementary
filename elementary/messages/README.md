# Elementary Messages

This package provides a flexible system for building structured messages that can be rendered across different platforms (like Slack, Teams, etc.).
The system defines a minimal and simple format that can be formatted into different output formats.However, some blocks, styles, or icons might not be supported across all platforms.

## Core Components

- `MessageBody`: The root container for a message
- `blocks`: Building blocks for message content (headers, code blocks, text, etc.)
- `block_builders`: Helper functions to easily construct common block patterns

## Message Structure

A `MessageBody` can contain the following blocks directly:

- `HeaderBlock`
- `CodeBlock`
- `DividerBlock`
- `LinesBlock`
- `FactListBlock`
- `ExpandableBlock`

## Basic Usage

```python
from elementary.messages.message_body import MessageBody, Color
from elementary.messages.blocks import HeaderBlock, LinesBlock, TextBlock
from elementary.messages.block_builders import TextLineBlock, BoldTextLineBlock

# Create a simple message
message = MessageBody(
    blocks=[
        HeaderBlock(text="My Message Title"),
        TextLineBlock(text="This is a simple text line"),
        BoldTextLineBlock(text="This is bold text"),
    ],
    color=Color.GREEN
)
```

## Available Blocks

### Text and Layout

- `HeaderBlock`: Section headers
- `TextBlock`: Basic text content with optional style (BOLD/ITALIC)
- `LineBlock`: A line that can contain multiple inline elements:
  - `TextBlock`: Text with optional styling
  - `LinkBlock`: Clickable URL with display text
  - `IconBlock`: Visual indicator (like ✓, ⚠️, etc.)
- `LinesBlock`: Multiple lines of text
- `DividerBlock`: Visual separator
- `CodeBlock`: Code snippets or formatted text

### Interactive Elements

- `ExpandableBlock`: Collapsible sections
- `LinkBlock`: Clickable URLs

### Data Display

- `FactBlock`: Key-value pairs
- `FactListBlock`: Collection of facts

### Styling

- `TextStyle`: Text formatting (BOLD, ITALIC)
- `Icon`: Various icons for visual indicators
- `Color`: Message theme colors (RED, YELLOW, GREEN)

## Block Builders

The `block_builders` module provides convenient functions for creating common block patterns. These builders are wrappers around the basic blocks defined in the `blocks` module and help with constructing commonly used block combinations:

```python
from elementary.messages.block_builders import (
    BulletListBlock,
    FactsBlock,
    JsonCodeBlock,
    TitledParagraphBlock
)
from elementary.messages.blocks import Icon

# Create a bullet list
bullet_list = BulletListBlock(
    icon=Icon.CHECK,
    lines=[TextLineBlock(text="Item 1"), TextLineBlock(text="Item 2")]
)

# Create facts list
facts = FactsBlock(
    facts=[
        ("Status", "Passed"),
        ("Duration", "2.5s"),
    ]
)

# Create JSON code block
json_block = JsonCodeBlock(
    content={"key": "value"}
)

# Create titled paragraph
paragraph = TitledParagraphBlock(
    title="Section Title",
    lines=[TextLineBlock(text="Paragraph content")]
)

# Example of a line with multiple inline elements
from elementary.messages.blocks import LineBlock, TextBlock, LinkBlock, IconBlock, Icon, TextStyle

complex_line = LineBlock(
    inlines=[
        IconBlock(icon=Icon.CHECK),
        TextBlock(text="Test passed - ", style=TextStyle.BOLD),
        TextBlock(text="View details at "),
        LinkBlock(text="dashboard", url="https://example.com"),
    ]
)
```

## Message Formatting

To format messages into different output formats (like Slack or Teams), a formatter needs to support all the basic blocks defined in the `blocks` module:

- Core blocks: `HeaderBlock`, `CodeBlock`, `DividerBlock`, `LinesBlock`, `FactListBlock`, `ExpandableBlock`
- Inline blocks: `TextBlock`, `LinkBlock`, `IconBlock`
- Styling: `TextStyle`, `Color`

The block builders are convenience wrappers that ultimately create these basic blocks, so formatters only need to handle the core block types.
