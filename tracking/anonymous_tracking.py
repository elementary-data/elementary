import logging
import uuid
from pathlib import Path
from typing import Optional, Tuple

import posthog
import requests
from bs4 import BeautifulSoup

from config.config import Config
from utils.package import get_package_version

logging.getLogger('posthog').disabled = True


class AnonymousTracking:
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
        self.anonymous_user_id = self.init_user_id()
        self.api_key, self.url = self._fetch_api_key_and_url()

    def init_user_id(self):
        legacy_user_id_path = Path().joinpath(self.config.profiles_dir, self.ANONYMOUS_USER_ID_FILE)
        user_id_path = Path().joinpath(self.config.config_dir, self.ANONYMOUS_USER_ID_FILE)
        # First check legacy file path
        try:
            return legacy_user_id_path.read_text()
        except FileNotFoundError:
            pass

        try:
            return user_id_path.read_text()
        except FileNotFoundError:
            pass

        user_id = str(uuid.uuid4())
        user_id_path.write_text(user_id)
        return user_id

    @classmethod
    def _fetch_api_key_and_url(cls) -> Tuple[Optional[str], Optional[str]]:
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

        cli_exception_properties = {
            'exception_properties': {
                'exception_type': str(type(exc)),
                'exception_content': str(exc)
            },
            'module_name': module_name,
            'command': command,
            'version': get_package_version()
        }
        anonymous_tracking.send_event('cli-exception', properties=cli_exception_properties)
    except Exception:
        pass


def track_cli_help(anonymous_tracking: AnonymousTracking):
    try:
        anonymous_tracking.send_event('cli-help')
    except Exception:
        pass
