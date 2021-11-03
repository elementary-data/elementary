import uuid
import os
from typing import Union, Optional
import requests
from bs4 import BeautifulSoup
import posthog
from lineage.utils import get_run_properties


class AnonymousTracking(object):
    ANONYMOUS_USER_ID_FILE = '.user_id'
    FETCH_API_KEY_AND_URL = 'https://www.elementary-data.com/telemetry'

    def __init__(self, profiles_dir: str, anonymous_usage_tracking: bool = True) -> None:
        self.profiles_dir = profiles_dir
        self.anonymous_user_id = None
        self.api_key = None
        self.url = None
        self.do_not_track = anonymous_usage_tracking is False
        self.run_id = str(uuid.uuid4())

    def init(self):
        anonymous_user_id_file_name = os.path.join(self.profiles_dir, self.ANONYMOUS_USER_ID_FILE)
        if os.path.exists(anonymous_user_id_file_name):
            with open(anonymous_user_id_file_name, 'r') as anonymous_user_id_file:
                self.anonymous_user_id = anonymous_user_id_file.read()
        else:
            self.anonymous_user_id = str(uuid.uuid4())
            with open(anonymous_user_id_file_name, 'w') as anonymous_user_id_file:
                anonymous_user_id_file.write(self.anonymous_user_id)

        self.api_key, self.url = self._fetch_api_key_and_url()

    @classmethod
    def _fetch_api_key_and_url(cls) -> (Union[str, None], Union[str, None]):
        result = requests.get(url=cls.FETCH_API_KEY_AND_URL)
        if result.status_code != 200:
            return None, None
        soup = BeautifulSoup(result.content, 'html.parser')
        h5_tag = soup.find('h5')
        if h5_tag is None:
            return None, None

        api_key_and_url = h5_tag.text.split('\n')
        if len(api_key_and_url) != 2:
            return None, None

        return api_key_and_url[0], api_key_and_url[1]

    def send_event(self, name: str, properties: dict = None) -> None:
        if self.do_not_track:
            return

        if self.api_key is None or self.url is None or self.anonymous_user_id is None:
            return

        if properties is None:
            properties = dict()

        properties['run_id'] = self.run_id

        posthog.api_key = self.api_key
        posthog.host = self.url
        posthog.capture(distinct_id=self.anonymous_user_id, event=name, properties=properties)


def track_cli_start(profiles_dir: str, profile_data: dict) -> Optional['AnonymousTracking']:
    try:
        anonymous_tracking = AnonymousTracking(profiles_dir, profile_data.get('anonymous_usage_tracking'))
        anonymous_tracking.init()
        cli_start_properties = {'platform_type': profile_data.get('type')}
        cli_start_properties.update(get_run_properties())
        anonymous_tracking.send_event('cli-start', properties=cli_start_properties)
        return anonymous_tracking
    except Exception:
        pass
    return None


def track_cli_end(anonymous_tracking: AnonymousTracking, lineage_properties: dict, query_history_properties: dict) \
        -> None:
    try:
        if anonymous_tracking is None:
            return

        cli_end_properties = dict()
        cli_end_properties.update(lineage_properties)
        cli_end_properties.update(query_history_properties)
        anonymous_tracking.send_event('cli-end', properties=cli_end_properties)
    except Exception:
        pass


def track_cli_exception(anonymous_tracking: AnonymousTracking, exc: Exception) \
        -> None:
    try:
        if anonymous_tracking is None:
            return

        cli_exception_properties = dict()
        cli_exception_properties['exception_type'] = str(type(exc))
        cli_exception_properties['exception_content'] = str(exc)
        anonymous_tracking.send_event('cli-exception', properties=cli_exception_properties)
    except Exception:
        pass
