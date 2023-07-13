from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import posthog
import requests

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


class TrackingAPI(BaseTracking):
    def __init__(self, config: Config):
        super().__init__(config)
        self.client = self._init_client()

    @staticmethod
    def _init_client() -> requests.Session:
        session = requests.Session()
        return session

    def _group_identify(self, group_type, group_identifier, group_props):
        response = self.client.post(
            f"{self.POSTHOG_API_HOST}/capture/",
            json=dict(
                api_key=self.POSTHOG_PROJECT_API_KEY,
                event="$groupidentify",
                properties={
                    "distinct_id": f"{group_type} {group_identifier}",
                    "$group_type": group_type,
                    "$group_key": group_identifier,
                    "$group_set": group_props,
                },
            ),
        )
        if not response.ok:
            logger.warning(
                f"Failed to register group in Posthog - {group_type} {group_identifier}"
            )

    def _capture(
        self,
        distinct_id: str,
        event_name: str,
        properties: Optional[dict] = None,
        groups: Optional[dict] = None,
    ):
        response = self.client.post(
            f"{self.POSTHOG_API_HOST}/capture/",
            json=dict(
                api_key=self.POSTHOG_PROJECT_API_KEY,
                event=event_name,
                distinct_id=distinct_id,
                properties={**(properties or {}), "$groups": groups},
            ),
        )
        if response.status_code != requests.codes.ok:
            logger.debug(f"Failed to capture event - {event_name} {distinct_id}")

    def register_group(
        self, group_type: str, group_identifier: str, group_props: Optional[dict] = None
    ):
        self._group_identify(group_type, group_identifier, group_props)
        self.groups[group_type] = group_identifier

    def _send_event(
        self,
        distinct_id: str,
        event_name: str,
        properties: Optional[dict] = None,
    ):
        self._capture(
            distinct_id=distinct_id,
            event_name=event_name,
            properties=properties,
            groups=self.groups,
        )
