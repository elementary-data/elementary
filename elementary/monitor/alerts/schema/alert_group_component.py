from pydantic.dataclasses import dataclass


@dataclass(
    frozen=True, eq=True
)  # frozen+eq defined, so we can use it as a dict key. Also, it's all Strings
class NotificationComponent:
    order: int
    name_in_summary: str
    empty_section_content: str


@dataclass(frozen=True, eq=True)
class AlertGroupComponent:
    name_in_summary: str
    emoji_in_summary: str
    name_in_full: str
    emoji_in_full: str
