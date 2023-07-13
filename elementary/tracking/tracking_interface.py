from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import posthog

from elementary.config.config import Config
from elementary.utils.hash import hash
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class BaseTracking(ABC):
    POSTHOG_PROJECT_API_KEY = "phc_56XBEzZmh02mGkadqLiYW51eECyYKWPyecVwkGdGUfg"
    POSTHOG_API_HOST = "https://app.posthog.com"

    def __init__(self, config: Config):
        self._config = config
        self._props: Dict[str, Any] = {}
        self.groups: Dict[str, str] = {}

    @staticmethod
    def _hash(content: str):
        return hash(content)

    def record_internal_exception(self, exc: Exception):
        pass

    def set_env(self, key: str, value):
        self._props[key] = value

    @abstractmethod
    def register_group(
        self, group_type: str, group_identifier: str, group_props: Optional[dict] = None
    ):
        raise NotImplementedError

    @abstractmethod
    def _send_event(
        self,
        distinct_id: str,
        event_name: str,
        properties: Optional[dict] = None,
    ):
        raise NotImplementedError


class Tracking(BaseTracking):
    def __init__(self, config: Config):
        super().__init__(config)
        posthog.project_api_key = self.POSTHOG_PROJECT_API_KEY

    def _send_event(
        self,
        distinct_id: str,
        event_name: str,
        properties: Optional[dict] = None,
    ):
        posthog.capture(
            distinct_id=distinct_id,
            event=event_name,
            properties=properties,
            groups=self.groups,
        )

    def register_group(
        self, group_type: str, group_identifier: str, group_props: Optional[dict] = None
    ):
        posthog.group_identify(group_type, group_identifier, group_props)
        self.groups[group_type] = group_identifier
