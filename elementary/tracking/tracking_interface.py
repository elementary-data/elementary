import hashlib
from typing import Any, Dict, Optional

import posthog

from elementary.config.config import Config


class Tracking:
    POSTHOG_PROJECT_API_KEY = "phc_56XBEzZmh02mGkadqLiYW51eECyYKWPyecVwkGdGUfg"

    def __init__(self, config: Config):
        posthog.project_api_key = self.POSTHOG_PROJECT_API_KEY
        self._config = config
        self._props: Dict[str, Any] = {}

    @staticmethod
    def _set_events_group(
        group_type: str, group_identifier: str, group_props: Optional[dict] = None
    ) -> None:
        posthog.group_identify(group_type, group_identifier, group_props)

    @staticmethod
    def _send_event(
        distinct_id: str,
        event_name: str,
        properties: Optional[dict] = None,
        groups: Optional[dict] = None,
    ) -> None:
        posthog.capture(
            distinct_id=distinct_id,
            event=event_name,
            properties=properties,
            groups=groups,
        )

    @staticmethod
    def _hash(content: str):
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def record_internal_exception(self, exc: Exception):
        pass

    def set_env(self, key: str, value):
        self._props[key] = value
