import posthog

from elementary.config.config import Config


class Tracking:
    POSTHOG_PROJECT_API_KEY = "phc_56XBEzZmh02mGkadqLiYW51eECyYKWPyecVwkGdGUfg"

    def __init__(self, config: Config):
        posthog.project_api_key = self.POSTHOG_PROJECT_API_KEY
        self._config = config
        self._props = {}

    @staticmethod
    def _send_event(
        distinct_id: str, event_name: str, properties: dict = None, groups: dict = None
    ) -> None:
        posthog.capture(
            distinct_id=distinct_id,
            event=event_name,
            properties=properties,
            groups=groups,
        )

    def record_internal_exception(self, exc: Exception):
        pass

    def set_env(self, key: str, value):
        self._props[key] = value
