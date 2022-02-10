import os
import uuid
from typing import Union

import posthog
import requests
from bs4 import BeautifulSoup

from config.config import Config


class AnonymousTracking(object):
    ANONYMOUS_USER_ID_FILE = '.user_id'
    FETCH_API_KEY_AND_URL = 'https://www.elementary-data.com/telemetry'

    def __init__(self, config: Config) -> None:
        self.anonymous_user_id = None
        self.api_key = None
        self.url = None
        self.config = config
        self.do_not_track = config.anonymous_tracking_enabled is False
        self.run_id = str(uuid.uuid4())
        self.init()

    def init(self):
        legacy_anonymous_user_id_file_name = os.path.join(self.config.profiles_dir, self.ANONYMOUS_USER_ID_FILE)
        anonymous_user_id_file_name = os.path.join(self.config.config_dir, self.ANONYMOUS_USER_ID_FILE)
        # First check legacy file path
        if os.path.exists(legacy_anonymous_user_id_file_name):
            with open(legacy_anonymous_user_id_file_name, 'r') as anonymous_user_id_file:
                self.anonymous_user_id = anonymous_user_id_file.read()
        elif os.path.exists(anonymous_user_id_file_name):
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
        properties['platform'] = self.config.platform

        posthog.api_key = self.api_key
        posthog.host = self.url
        posthog.capture(distinct_id=self.anonymous_user_id, event=name, properties=properties)


def track_cli_start(anonymous_tracking: AnonymousTracking, module_name: str, cli_properties: dict,
                    command: str = None) -> None:
    try:
        cli_start_properties = {'cli_properties': cli_properties,
                                'module_name': module_name,
                                'command': command}
        anonymous_tracking.send_event('cli-start', properties=cli_start_properties)
    except Exception:
        pass


def track_cli_end(anonymous_tracking: AnonymousTracking, module_name: str, execution_properties: dict,
                  command: str = None) -> None:
    try:
        if anonymous_tracking is None:
            return

        cli_end_properties = {'execution_properties': execution_properties,
                              'module_name': module_name,
                              'command': command}
        anonymous_tracking.send_event('cli-end', properties=cli_end_properties)
    except Exception:
        pass


def track_cli_exception(anonymous_tracking: AnonymousTracking, module_name: str, exc: Exception,
                        command: str = None) -> None:
    try:
        if anonymous_tracking is None:
            return

        cli_exception_properties = {'exception_properties': {'exception_type': str(type(exc)),
                                                             'exception_content': str(exc)},
                                    'module_name': module_name,
                                    'command': command}
        anonymous_tracking.send_event('cli-exception', properties=cli_exception_properties)
    except Exception:
        pass


def track_cli_help(anonymous_tracking: AnonymousTracking):
    try:
        anonymous_tracking.send_event('cli-help')
    except Exception:
        pass