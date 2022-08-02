import hashlib
import logging
import uuid
from pathlib import Path
from typing import Optional, Tuple

import posthog
import requests
from bs4 import BeautifulSoup

import monitor.paths
import tracking.user
from clients.dbt.dbt_runner import DbtRunner
from config.config import Config
from utils.package import get_package_version

logging.getLogger('posthog').disabled = True


class AnonymousTracking:
    ANONYMOUS_USER_ID_FILE = '.user_id'
    FETCH_API_KEY_AND_URL = 'https://www.elementary-data.com/telemetry'

    def __init__(self, config: Config) -> None:
        self.anonymous_user_id = None
        self.hashed_adapter_unique_id = None
        self.api_key = None
        self.url = None
        self.config = config
        self.do_not_track = config.anonymous_tracking_enabled is False
        self.run_id = str(uuid.uuid4())
        self.init()

    def init(self):
        self.anonymous_user_id = self.init_user_id()
        self.hashed_adapter_unique_id = self._get_hashed_adapter_unique_id()
        self.api_key, self.url = self._fetch_api_key_and_url()
        posthog.api_key, posthog.host = self.api_key, self.url

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

        if properties is None:
            properties = dict()

        properties['run_id'] = self.run_id
        posthog.capture(distinct_id=self.anonymous_user_id, event=name, properties=properties,
                        groups={'warehouse': self.hashed_adapter_unique_id})

    def track_cli_start(self, module_name: str, cli_properties: dict, command: str = None):
        try:
            user_props = tracking.user.get_props()
            props = {'cli_properties': cli_properties, 'module_name': module_name, 'command': command}
            self.send_event('cli-start', properties={'user': user_props, **props})
        except Exception:
            pass

    def track_cli_end(self, module_name: str, execution_properties: dict, command: str = None):
        try:
            props = {'execution_properties': execution_properties, 'module_name': module_name, 'command': command}
            self.send_event('cli-end', properties=props)
        except Exception:
            pass

    def track_cli_exception(self, module_name: str, exc: Exception, command: str = None) -> None:
        try:
            props = {
                'exception_properties': {
                    'exception_type': str(type(exc)),
                    'exception_content': str(exc)
                },
                'module_name': module_name,
                'command': command,
                'version': get_package_version()
            }
            self.send_event('cli-exception', properties=props)
        except Exception:
            pass

    def track_cli_help(self):
        try:
            self.send_event('cli-help')
        except Exception:
            pass

    def _get_hashed_adapter_unique_id(self):
        try:
            dbt_runner = DbtRunner(monitor.paths.DBT_PROJECT_PATH, self.config.profiles_dir, self.config.profile_target)
            adapter_unique_id = dbt_runner.run_operation('get_adapter_unique_id', should_log=False)[0]
            hashed_adapter_unique_id = hashlib.sha256(adapter_unique_id.encode('utf-8')).hexdigest()
            return hashed_adapter_unique_id
        except Exception:
            return None
