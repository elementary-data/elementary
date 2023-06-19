from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import posthog
import requests

from elementary.config.config import Config
from elementary.utils.hash import hash


class BaseTracking(ABC):
    POSTHOG_PROJECT_API_KEY = "phc_56XBEzZmh02mGkadqLiYW51eECyYKWPyecVwkGdGUfg"
    POSTHOG_API_HOST = "https://app.posthog.com"

    def __init__(self, config: Config):
        self._config = config
        self._props: Dict[str, Any] = {}

    @staticmethod
    def _hash(content: str):
        return hash(content)

    def record_internal_exception(self, exc: Exception):
        pass

    def set_env(self, key: str, value):
        self._props[key] = value

    @abstractmethod
    def _set_events_group(
        group_type: str, group_identifier: str, group_props: Optional[dict] = None
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def _send_event(
        distinct_id: str,
        event_name: str,
        properties: Optional[dict] = None,
        groups: Optional[dict] = None,
    ) -> None:
        raise NotImplementedError


class Tracking(BaseTracking):
    def __init__(self, config: Config):
        super().__init__(config)
        posthog.project_api_key = self.POSTHOG_PROJECT_API_KEY

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


class TrackingAPI(BaseTracking):
    def __init__(self, config: Config):
        super().__init__(config)
        self.client = self._init_client()

    def _init_client(self) -> requests.Session:
        session = requests.Session()
        return session

    def _group_identify(
        self, group_type, group_identifier, group_props
    ) -> requests.Response:
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
        return response

    def _capture(
        self,
        distinct_id: str,
        event_name: str,
        properties: Optional[dict] = None,
        groups: Optional[dict] = None,
    ) -> requests.Response:
        response = self.client.post(
            f"{self.POSTHOG_API_HOST}/capture/",
            json=dict(
                api_key=self.POSTHOG_PROJECT_API_KEY,
                event=event_name,
                distinct_id=distinct_id,
                properties={**properties, "$groups": groups},
            ),
        )
        return response

    def _set_events_group(
        self, group_type: str, group_identifier: str, group_props: Optional[dict] = None
    ) -> requests.Response:
        return self._group_identify(group_type, group_identifier, group_props)

    def _send_event(
        self,
        distinct_id: str,
        event_name: str,
        properties: Optional[dict] = None,
        groups: Optional[dict] = None,
    ) -> requests.Response:
        return self._capture(
            distinct_id=distinct_id,
            event_name=event_name,
            properties=properties,
            groups=groups,
        )
